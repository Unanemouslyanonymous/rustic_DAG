//! The primary: builds batches, proposes headers, votes, assembles
//! certificates, keeps the local DAG and drives consensus.
//!
//! All protocol state lives in this one struct and is only touched from a
//! single task, so there are no locks anywhere on the critical path. The
//! network layer feeds it `Inbound` events and a timer ticks it.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::config::{Committee, Parameters, Stake};
use crate::consensus::Consensus;
use crate::crypto::{Digest, KeyPair, PublicKey, Signature};
use crate::error::{DagError, DagResult};
use crate::messages::{PeerMessage, MAX_REQUEST_ITEMS};
use crate::network::{Inbound, Network};
use crate::types::{Batch, Certificate, CommittedCertificate, Header, Round, Transaction, Vote};

const TICK: Duration = Duration::from_millis(5);
const STATS_EVERY: Duration = Duration::from_secs(5);
/// Batches we created that are not yet part of a certificate are tagged with
/// this round so garbage collection leaves them alone.
const OWN_UNCERTIFIED: Round = Round::MAX;

#[derive(Debug, Default, Clone)]
pub struct CoreStats {
    pub headers_proposed: u64,
    pub votes_cast: u64,
    pub certificates_formed: u64,
    pub certificates_accepted: u64,
    pub committed_certificates: u64,
    pub committed_transactions: u64,
    pub equivocations_seen: u64,
    pub invalid_messages: u64,
}

/// One of our own headers that has not gathered a quorum yet.
struct InFlight {
    header: Header,
    votes: Vec<(PublicKey, Signature)>,
    stake: Stake,
    sent_at: Instant,
}

pub struct Core {
    keypair: KeyPair,
    name: PublicKey,
    committee: Committee,
    params: Parameters,
    network: Network,
    consensus: Consensus,
    output: mpsc::UnboundedSender<CommittedCertificate>,
    commit_index: u64,

    // Local view of the DAG.
    certificates: HashMap<Digest, Certificate>,
    by_round: BTreeMap<Round, HashMap<PublicKey, Digest>>,
    batches: HashMap<Digest, Batch>,
    batch_rounds: HashMap<Digest, Round>,
    gc_round: Round,

    // Batch maker.
    tx_buffer: Vec<Transaction>,
    tx_bytes: usize,
    batch_started: Option<Instant>,
    batch_nonce: u64,

    // Proposer.
    round: Round,
    payload_queue: VecDeque<Digest>,
    last_proposal: Instant,
    in_flight: BTreeMap<Round, InFlight>,

    // Voter: the header we voted for, per round and author.
    last_voted: BTreeMap<Round, HashMap<PublicKey, Digest>>,

    // Synchroniser: objects waiting for parents or payload.
    pending_headers: HashMap<Digest, Header>,
    pending_certificates: HashMap<Digest, Certificate>,
    dirty: bool,
    last_sync: Instant,

    last_stats: Instant,
    pub stats: CoreStats,
}

impl Core {
    pub fn new(
        keypair: KeyPair,
        committee: Committee,
        params: Parameters,
        network: Network,
        output: mpsc::UnboundedSender<CommittedCertificate>,
    ) -> Self {
        let name = keypair.name;
        let mut certificates = HashMap::new();
        let mut genesis_round = HashMap::new();
        for c in Certificate::genesis(&committee) {
            genesis_round.insert(c.origin(), c.digest());
            certificates.insert(c.digest(), c);
        }
        let mut by_round = BTreeMap::new();
        by_round.insert(0, genesis_round);
        let now = Instant::now();
        Core {
            consensus: Consensus::new(committee.clone(), params.gc_depth),
            keypair,
            name,
            committee,
            params,
            network,
            output,
            commit_index: 0,
            certificates,
            by_round,
            batches: HashMap::new(),
            batch_rounds: HashMap::new(),
            gc_round: 0,
            tx_buffer: Vec::new(),
            tx_bytes: 0,
            batch_started: None,
            batch_nonce: 0,
            round: 0,
            payload_queue: VecDeque::new(),
            last_proposal: now,
            in_flight: BTreeMap::new(),
            last_voted: BTreeMap::new(),
            pending_headers: HashMap::new(),
            pending_certificates: HashMap::new(),
            dirty: false,
            last_sync: now,
            last_stats: now,
            stats: CoreStats::default(),
        }
    }

    pub fn round(&self) -> Round {
        self.round
    }

    pub fn last_committed_round(&self) -> Round {
        self.consensus.last_committed_round()
    }

    pub async fn run(mut self, mut rx: mpsc::Receiver<Inbound>) {
        let mut tick = tokio::time::interval(TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                msg = rx.recv() => match msg {
                    Some(msg) => self.handle(msg),
                    None => break,
                },
                _ = tick.tick() => self.on_tick(),
            }
        }
    }

    /// Processes one inbound event. Public so tests can drive the core
    /// synchronously without any sockets.
    pub fn handle(&mut self, msg: Inbound) {
        match msg {
            Inbound::Client(txs) => self.add_transactions(txs),
            Inbound::Peer(from, msg) => self.handle_peer(from, msg),
        }
        self.drain_pending();
        self.try_propose();
    }

    pub fn on_tick(&mut self) {
        let now = Instant::now();
        if self.batch_started.is_some_and(|t| {
            now.duration_since(t) >= Duration::from_millis(self.params.max_batch_delay_ms)
        }) {
            self.seal_batch();
        }
        self.try_propose();
        if now.duration_since(self.last_sync) >= Duration::from_millis(self.params.sync_retry_ms) {
            self.last_sync = now;
            self.retry_sync();
        }
        if now.duration_since(self.last_stats) >= STATS_EVERY {
            self.last_stats = now;
            info!(
                "round {} | committed round {} | dag {} certs | pending {}h/{}c | {:?}",
                self.round,
                self.consensus.last_committed_round(),
                self.certificates.len(),
                self.pending_headers.len(),
                self.pending_certificates.len(),
                self.stats
            );
        }
    }

    fn handle_peer(&mut self, from: PublicKey, msg: PeerMessage) {
        match msg {
            PeerMessage::Batch(batch) => self.store_batch(batch),
            PeerMessage::Header(header) => self.process_header(header, from),
            PeerMessage::Vote(vote) => self.process_vote(vote),
            PeerMessage::Certificate(cert) => self.process_certificate(cert, from),
            PeerMessage::CertificatesRequest(digests) => {
                for d in digests.iter().take(MAX_REQUEST_ITEMS) {
                    if let Some(c) = self.certificates.get(d).filter(|c| c.round() > 0) {
                        self.network
                            .send(&from, &PeerMessage::Certificate(c.clone()));
                    }
                }
            }
            PeerMessage::BatchesRequest(digests) => {
                for d in digests.iter().take(MAX_REQUEST_ITEMS) {
                    if let Some(b) = self.batches.get(d) {
                        self.network.send(&from, &PeerMessage::Batch(b.clone()));
                    }
                }
            }
        }
    }

    // ----- batch maker -------------------------------------------------

    fn add_transactions(&mut self, txs: Vec<Transaction>) {
        for tx in txs {
            if tx.is_empty() || tx.len() > self.params.batch_size {
                continue;
            }
            self.tx_bytes += tx.len();
            self.tx_buffer.push(tx);
            self.batch_started.get_or_insert_with(Instant::now);
            if self.tx_bytes >= self.params.batch_size {
                self.seal_batch();
            }
        }
    }

    fn seal_batch(&mut self) {
        if self.tx_buffer.is_empty() {
            return;
        }
        let batch = Batch {
            author: self.name,
            nonce: self.batch_nonce,
            transactions: std::mem::take(&mut self.tx_buffer),
        };
        self.batch_nonce += 1;
        self.tx_bytes = 0;
        self.batch_started = None;
        let digest = batch.digest();
        debug!(
            "sealed batch {digest} with {} txs",
            batch.transactions.len()
        );
        self.network.broadcast(&PeerMessage::Batch(batch.clone()));
        self.batches.insert(digest, batch);
        self.batch_rounds.insert(digest, OWN_UNCERTIFIED);
        self.payload_queue.push_back(digest);
    }

    fn store_batch(&mut self, batch: Batch) {
        if !self.committee.contains(&batch.author) || batch.author == self.name {
            return;
        }
        let digest = batch.digest();
        if self.batches.contains_key(&digest) {
            return;
        }
        self.batches.insert(digest, batch);
        self.batch_rounds.entry(digest).or_insert(self.round);
        self.dirty = true;
    }

    // ----- proposer ----------------------------------------------------

    /// Highest round for which we hold a quorum of certificates.
    fn ready_parents(&self) -> Option<(Round, BTreeSet<Digest>)> {
        let quorum = self.committee.quorum_threshold();
        self.by_round.iter().rev().find_map(|(round, certs)| {
            let stake: Stake = certs.keys().map(|a| self.committee.stake(a)).sum();
            (stake >= quorum).then(|| (*round, certs.values().copied().collect()))
        })
    }

    fn try_propose(&mut self) {
        let Some((parent_round, parents)) = self.ready_parents() else {
            return;
        };
        if parent_round < self.round {
            return;
        }
        let enough = self.payload_queue.len() >= self.params.header_size;
        let timed_out =
            self.last_proposal.elapsed() >= Duration::from_millis(self.params.max_header_delay_ms);
        if !enough && !timed_out {
            return;
        }

        let round = parent_round + 1;
        let n = self.payload_queue.len().min(self.params.max_header_batches);
        let payload: Vec<Digest> = self.payload_queue.drain(..n).collect();
        let header = Header::new(&self.keypair, round, payload, parents);
        debug!(
            "proposing {} at round {round} with {} batches",
            header.id,
            header.payload.len()
        );

        self.round = round;
        self.last_proposal = Instant::now();
        self.stats.headers_proposed += 1;
        self.in_flight.insert(
            round,
            InFlight {
                header: header.clone(),
                votes: Vec::new(),
                stake: 0,
                sent_at: Instant::now(),
            },
        );
        self.network.broadcast(&PeerMessage::Header(header.clone()));
        self.process_header(header, self.name);
    }

    // ----- voter -------------------------------------------------------

    fn process_header(&mut self, header: Header, from: PublicKey) {
        if header.round <= self.gc_round {
            return;
        }
        if let Err(e) = header.verify(&self.committee) {
            warn!("invalid header from {from}: {e}");
            self.stats.invalid_messages += 1;
            return;
        }
        let (certs, batches) = self.missing(&header);
        if !certs.is_empty() || !batches.is_empty() {
            debug!(
                "header {} waits for {} certs, {} batches",
                header.id,
                certs.len(),
                batches.len()
            );
            self.request(&from, &header.author, certs, batches);
            self.pending_headers.insert(header.id, header);
            return;
        }
        self.vote_on(header);
    }

    fn vote_on(&mut self, header: Header) {
        if let Err(e) = self.check_parents(&header) {
            warn!("{e}");
            self.stats.invalid_messages += 1;
            return;
        }
        let voted = self.last_voted.entry(header.round).or_default();
        match voted.get(&header.author) {
            Some(d) if *d != header.id => {
                let e = DagError::Equivocation {
                    author: header.author,
                    round: header.round,
                };
                warn!("{e}: {d} vs {}", header.id);
                self.stats.equivocations_seen += 1;
                return;
            }
            // Same header again: the author probably did not get our vote,
            // so send it again. Signing is deterministic, nothing new leaks.
            Some(_) => {}
            None => {
                voted.insert(header.author, header.id);
                self.stats.votes_cast += 1;
            }
        }
        let vote = Vote::new(&header, &self.keypair);
        if header.author == self.name {
            self.process_vote(vote);
        } else {
            self.network.send(&header.author, &PeerMessage::Vote(vote));
        }
    }

    // ----- certificate aggregation ------------------------------------

    fn process_vote(&mut self, vote: Vote) {
        if vote.origin != self.name {
            return;
        }
        let Some(pending) = self.in_flight.get_mut(&vote.round) else {
            return;
        };
        if pending.header.id != vote.header_id
            || pending.votes.iter().any(|(v, _)| *v == vote.voter)
        {
            return;
        }
        if let Err(e) = vote.verify(&self.committee) {
            warn!("bad vote from {}: {e}", vote.voter);
            self.stats.invalid_messages += 1;
            return;
        }
        pending.votes.push((vote.voter, vote.signature));
        pending.stake += self.committee.stake(&vote.voter);
        if pending.stake < self.committee.quorum_threshold() {
            return;
        }
        let InFlight { header, votes, .. } = self.in_flight.remove(&vote.round).expect("present");
        let certificate = Certificate { header, votes };
        debug!(
            "formed certificate {} for round {}",
            certificate.digest(),
            certificate.round()
        );
        self.stats.certificates_formed += 1;
        self.network
            .broadcast(&PeerMessage::Certificate(certificate.clone()));
        self.accept_certificate(certificate);
    }

    // ----- DAG -----------------------------------------------------------

    fn process_certificate(&mut self, certificate: Certificate, from: PublicKey) {
        let digest = certificate.digest();
        if certificate.round() <= self.gc_round
            || self.certificates.contains_key(&digest)
            || self.pending_certificates.contains_key(&digest)
        {
            return;
        }
        if let Err(e) = certificate.verify(&self.committee) {
            warn!("invalid certificate from {from}: {e}");
            self.stats.invalid_messages += 1;
            return;
        }
        let (certs, batches) = self.missing(&certificate.header);
        if !certs.is_empty() || !batches.is_empty() {
            self.request(&from, &certificate.origin(), certs, batches);
            self.pending_certificates.insert(digest, certificate);
            return;
        }
        self.accept_certificate(certificate);
    }

    /// Inserts a certificate whose parents and payload are all local.
    fn accept_certificate(&mut self, certificate: Certificate) {
        let digest = certificate.digest();
        if self.certificates.contains_key(&digest) {
            return;
        }
        if let Err(e) = self.check_parents(&certificate.header) {
            // A quorum signed it, so at least one honest node checked this.
            // Seeing it means more than f nodes are faulty.
            warn!("certified header with bad parents: {e}");
            self.stats.invalid_messages += 1;
            return;
        }
        let round = certificate.round();
        let slot = self.by_round.entry(round).or_default();
        if let Some(existing) = slot.get(&certificate.origin()) {
            // Impossible with at most f faults: two quorums intersect in an
            // honest node, who votes once per slot.
            warn!(
                "two certificates for {} at round {round}: {existing} and {digest}",
                certificate.origin()
            );
            self.stats.equivocations_seen += 1;
            return;
        }
        slot.insert(certificate.origin(), digest);
        for b in &certificate.header.payload {
            self.batch_rounds.insert(*b, round);
        }
        self.pending_headers.remove(&digest);
        self.certificates.insert(digest, certificate.clone());
        self.stats.certificates_accepted += 1;
        self.dirty = true;

        let committed = self.consensus.process_certificate(certificate);
        if !committed.is_empty() {
            self.deliver(committed);
            self.garbage_collect();
        }
    }

    fn deliver(&mut self, committed: Vec<(Round, Certificate)>) {
        let last = committed.last().map(|(r, _)| *r).unwrap_or_default();
        for (leader_round, certificate) in committed {
            let batches: Vec<Batch> = certificate
                .header
                .payload
                .iter()
                .map(|d| {
                    self.batches
                        .get(d)
                        .cloned()
                        .expect("payload is local before insertion")
                })
                .collect();
            self.stats.committed_certificates += 1;
            self.stats.committed_transactions += batches
                .iter()
                .map(|b| b.transactions.len() as u64)
                .sum::<u64>();
            let _ = self.output.send(CommittedCertificate {
                index: self.commit_index,
                leader_round,
                certificate,
                batches,
            });
            self.commit_index += 1;
        }
        debug!("committed up to leader round {last}");
    }

    fn check_parents(&self, header: &Header) -> DagResult<()> {
        let bad = |reason| {
            Err(DagError::BadParents {
                digest: header.id,
                reason,
            })
        };
        let mut authors = HashSet::new();
        let mut stake = 0;
        for p in &header.parents {
            let Some(parent) = self.certificates.get(p) else {
                return bad("unknown parent");
            };
            if parent.round() + 1 != header.round {
                return bad("parent from the wrong round");
            }
            if !authors.insert(parent.origin()) {
                return bad("two parents by the same author");
            }
            stake += self.committee.stake(&parent.origin());
        }
        if stake < self.committee.quorum_threshold() {
            return bad("parents do not form a quorum");
        }
        Ok(())
    }

    fn missing(&self, header: &Header) -> (Vec<Digest>, Vec<Digest>) {
        let certs = header
            .parents
            .iter()
            .filter(|d| !self.certificates.contains_key(d))
            .copied()
            .collect();
        let batches = header
            .payload
            .iter()
            .filter(|d| !self.batches.contains_key(d))
            .copied()
            .collect();
        (certs, batches)
    }

    // ----- synchroniser ------------------------------------------------

    fn request(
        &self,
        from: &PublicKey,
        author: &PublicKey,
        certs: Vec<Digest>,
        batches: Vec<Digest>,
    ) {
        let mut targets = vec![*from];
        if author != from {
            targets.push(*author);
        }
        for t in targets.iter().filter(|t| **t != self.name) {
            if !certs.is_empty() {
                self.network
                    .send(t, &PeerMessage::CertificatesRequest(certs.clone()));
            }
            if !batches.is_empty() {
                self.network
                    .send(t, &PeerMessage::BatchesRequest(batches.clone()));
            }
        }
    }

    /// Re-examines suspended objects after something new was stored.
    fn drain_pending(&mut self) {
        while std::mem::take(&mut self.dirty) {
            let ready: Vec<Digest> = self
                .pending_certificates
                .iter()
                .filter(|(_, c)| self.is_complete(&c.header))
                .map(|(d, _)| *d)
                .collect();
            for d in ready {
                if let Some(c) = self.pending_certificates.remove(&d) {
                    self.accept_certificate(c);
                }
            }
            let ready: Vec<Digest> = self
                .pending_headers
                .iter()
                .filter(|(_, h)| self.is_complete(h))
                .map(|(d, _)| *d)
                .collect();
            for d in ready {
                if let Some(h) = self.pending_headers.remove(&d) {
                    self.vote_on(h);
                }
            }
        }
    }

    fn is_complete(&self, header: &Header) -> bool {
        header
            .parents
            .iter()
            .all(|d| self.certificates.contains_key(d))
            && header.payload.iter().all(|d| self.batches.contains_key(d))
    }

    /// Periodic catch-up: ask everyone for what we are still missing and
    /// re-broadcast our own headers that are still collecting votes.
    fn retry_sync(&mut self) {
        let mut certs = BTreeSet::new();
        let mut batches = BTreeSet::new();
        let headers = self
            .pending_certificates
            .values()
            .map(|c| &c.header)
            .chain(self.pending_headers.values());
        for h in headers {
            let (c, b) = self.missing(h);
            certs.extend(c);
            batches.extend(b);
        }
        if !certs.is_empty() {
            debug!("sync: still missing {} certificates", certs.len());
            let v: Vec<_> = certs.into_iter().take(MAX_REQUEST_ITEMS).collect();
            self.network.broadcast(&PeerMessage::CertificatesRequest(v));
        }
        if !batches.is_empty() {
            let v: Vec<_> = batches.into_iter().take(MAX_REQUEST_ITEMS).collect();
            self.network.broadcast(&PeerMessage::BatchesRequest(v));
        }
        let retry = Duration::from_millis(self.params.sync_retry_ms);
        for pending in self.in_flight.values_mut().rev().take(3) {
            if pending.sent_at.elapsed() >= retry {
                pending.sent_at = Instant::now();
                self.network
                    .broadcast(&PeerMessage::Header(pending.header.clone()));
            }
        }
    }

    fn garbage_collect(&mut self) {
        let gc_round = self
            .consensus
            .last_committed_round()
            .saturating_sub(self.params.gc_depth);
        if gc_round <= self.gc_round {
            return;
        }
        self.gc_round = gc_round;

        let keep = self.by_round.split_off(&gc_round);
        for (_, certs) in std::mem::replace(&mut self.by_round, keep) {
            for d in certs.values() {
                self.certificates.remove(d);
            }
        }
        self.last_voted = self.last_voted.split_off(&(gc_round + 1));
        self.pending_headers.retain(|_, h| h.round > gc_round);
        self.pending_certificates
            .retain(|_, c| c.round() > gc_round);

        // Our own headers that never got certified: put their batches back
        // in the queue so the transactions are not lost.
        let alive = self.in_flight.split_off(&(gc_round + 1));
        for (_, stale) in std::mem::replace(&mut self.in_flight, alive) {
            for d in stale.header.payload.into_iter().rev() {
                self.payload_queue.push_front(d);
            }
        }

        let batches = &mut self.batches;
        self.batch_rounds.retain(|d, r| {
            let keep = *r > gc_round;
            if !keep {
                batches.remove(d);
            }
            keep
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::decode;
    use crate::testing::{committee_with_keys, genesis_digests, make_certificate};
    use bytes::Bytes;

    struct Harness {
        core: Core,
        keys: Vec<KeyPair>,
        outbox: HashMap<PublicKey, mpsc::Receiver<Bytes>>,
        _commits: mpsc::UnboundedReceiver<CommittedCertificate>,
    }

    impl Harness {
        fn new() -> Self {
            let (keys, committee) = committee_with_keys(4);
            let mut senders = HashMap::new();
            let mut outbox = HashMap::new();
            for k in &keys[1..] {
                let (tx, rx) = mpsc::channel(1000);
                senders.insert(k.name, tx);
                outbox.insert(k.name, rx);
            }
            let (out_tx, out_rx) = mpsc::unbounded_channel();
            let params = Parameters {
                max_header_delay_ms: 60_000,
                ..Parameters::default()
            };
            let core = Core::new(
                keys[0].clone(),
                committee,
                params,
                Network::from_channels(senders),
                out_tx,
            );
            Harness {
                core,
                keys,
                outbox,
                _commits: out_rx,
            }
        }

        fn sent_to(&mut self, i: usize) -> Vec<PeerMessage> {
            let rx = self.outbox.get_mut(&self.keys[i].name).unwrap();
            let mut out = Vec::new();
            while let Ok(b) = rx.try_recv() {
                out.push(decode(&b).unwrap());
            }
            out
        }

        fn from(&mut self, i: usize, msg: PeerMessage) {
            let from = self.keys[i].name;
            self.core.handle(Inbound::Peer(from, msg));
        }
    }

    fn votes(msgs: &[PeerMessage]) -> Vec<&Vote> {
        msgs.iter()
            .filter_map(|m| {
                if let PeerMessage::Vote(v) = m {
                    Some(v)
                } else {
                    None
                }
            })
            .collect()
    }

    #[test]
    fn votes_at_most_once_per_slot() {
        let mut h = Harness::new();
        let parents = genesis_digests(&h.core.committee);
        let a = Header::new(&h.keys[1], 1, vec![], parents.clone());
        let mut b_parents = parents.clone();
        b_parents.pop_first();
        let b = Header::new(&h.keys[1], 1, vec![], b_parents);

        h.from(1, PeerMessage::Header(a.clone()));
        h.from(1, PeerMessage::Header(b));
        let sent = h.sent_to(1);
        let v = votes(&sent);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].header_id, a.id);
        assert_eq!(h.core.stats.equivocations_seen, 1);

        // Re-delivery of the header we voted for re-sends the same vote.
        h.from(1, PeerMessage::Header(a.clone()));
        assert_eq!(votes(&h.sent_to(1)).len(), 1);
        assert_eq!(h.core.stats.votes_cast, 1);
    }

    #[test]
    fn header_with_unknown_parents_is_synced_then_voted() {
        let mut h = Harness::new();
        let genesis = genesis_digests(&h.core.committee);
        let r1: Vec<_> = h
            .keys
            .iter()
            .map(|k| make_certificate(k, &h.keys, 1, genesis.clone()))
            .collect();
        let parents = r1.iter().map(|c| c.digest()).collect();
        let header = Header::new(&h.keys[2], 2, vec![], parents);

        h.from(2, PeerMessage::Header(header.clone()));
        let sent = h.sent_to(2);
        assert!(votes(&sent).is_empty());
        assert!(sent
            .iter()
            .any(|m| matches!(m, PeerMessage::CertificatesRequest(d) if d.len() == 4)));

        for c in &r1 {
            h.from(2, PeerMessage::Certificate(c.clone()));
        }
        let v = votes(&h.sent_to(2))
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].header_id, header.id);
    }

    #[test]
    fn header_with_missing_payload_waits_for_batch() {
        let mut h = Harness::new();
        let batch = Batch {
            author: h.keys[1].name,
            nonce: 0,
            transactions: vec![vec![1, 2, 3]],
        };
        let header = Header::new(
            &h.keys[1],
            1,
            vec![batch.digest()],
            genesis_digests(&h.core.committee),
        );
        h.from(1, PeerMessage::Header(header));
        let sent = h.sent_to(1);
        assert!(votes(&sent).is_empty());
        assert!(sent
            .iter()
            .any(|m| matches!(m, PeerMessage::BatchesRequest(_))));
        h.from(1, PeerMessage::Batch(batch));
        assert_eq!(votes(&h.sent_to(1)).len(), 1);
    }

    #[test]
    fn rejects_certificate_without_quorum() {
        let mut h = Harness::new();
        let genesis = genesis_digests(&h.core.committee);
        let mut cert = make_certificate(&h.keys[1], &h.keys, 1, genesis);
        cert.votes.truncate(2);
        h.from(1, PeerMessage::Certificate(cert.clone()));
        assert!(!h.core.certificates.contains_key(&cert.digest()));
        assert_eq!(h.core.stats.invalid_messages, 1);
    }

    #[test]
    fn rejects_header_with_too_few_parents() {
        let mut h = Harness::new();
        let mut parents = genesis_digests(&h.core.committee);
        parents.pop_first();
        parents.pop_first();
        let header = Header::new(&h.keys[1], 1, vec![], parents);
        h.from(1, PeerMessage::Header(header));
        assert!(votes(&h.sent_to(1)).is_empty());
        assert_eq!(h.core.stats.invalid_messages, 1);
    }

    #[test]
    fn transactions_become_a_header_and_then_a_certificate() {
        let mut h = Harness::new();
        let params = h.core.params.clone();
        let tx = vec![7u8; params.batch_size];
        h.core.handle(Inbound::Client(vec![tx]));

        let sent = h.sent_to(1);
        assert!(matches!(sent[0], PeerMessage::Batch(_)));
        let header = sent
            .iter()
            .find_map(|m| {
                if let PeerMessage::Header(x) = m {
                    Some(x.clone())
                } else {
                    None
                }
            })
            .expect("header proposed");
        assert_eq!(header.round, 1);
        assert_eq!(header.payload.len(), 1);

        for i in 1..3 {
            let v = Vote::new(&header, &h.keys[i]);
            h.from(i, PeerMessage::Vote(v));
        }
        assert_eq!(h.core.stats.certificates_formed, 1);
        assert!(h
            .sent_to(1)
            .iter()
            .any(|m| matches!(m, PeerMessage::Certificate(c) if c.digest() == header.id)));
    }

    #[test]
    fn forged_vote_is_ignored() {
        let mut h = Harness::new();
        h.core
            .handle(Inbound::Client(vec![vec![1u8; h.core.params.batch_size]]));
        let header = h
            .sent_to(1)
            .into_iter()
            .find_map(|m| {
                if let PeerMessage::Header(x) = m {
                    Some(x)
                } else {
                    None
                }
            })
            .unwrap();
        let mut v = Vote::new(&header, &h.keys[1]);
        v.voter = h.keys[2].name;
        h.from(1, PeerMessage::Vote(v));
        let v = Vote::new(&header, &h.keys[3]);
        h.from(3, PeerMessage::Vote(v));
        assert_eq!(h.core.stats.certificates_formed, 0);
        assert_eq!(h.core.stats.invalid_messages, 1);
    }
}
