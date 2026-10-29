//! Tusk: zero message overhead ordering on top of the Narwhal DAG.
//!
//! Every node interprets its local copy of the DAG. Even rounds have a
//! leader. A leader at round `r` is committed once its certificate is
//! referenced by at least f+1 (validity threshold) certificates of round
//! `r + 1`; we check this when the first certificate of round `r + 3`
//! shows up, i.e. once a quorum of round `r + 2` exists locally. Before
//! committing a leader we walk back and commit, oldest first, every earlier
//! uncommitted leader that it has a path to. Committing a leader means
//! flattening its causal history (minus everything already committed) into
//! a deterministic sequence.
//!
//! Since certificates are immutable and the core only inserts a certificate
//! once all of its parents are present, the causal history of a leader is
//! identical on every honest node, so all honest nodes output the same
//! sequence even though they decide at different times.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::config::Committee;
use crate::crypto::{Digest, PublicKey};
use crate::types::{Certificate, Round};

type Dag = BTreeMap<Round, HashMap<PublicKey, Certificate>>;

pub struct Consensus {
    committee: Committee,
    gc_depth: Round,
    dag: Dag,
    /// Digest -> round of everything committed and not yet garbage collected.
    committed: HashMap<Digest, Round>,
    last_committed_round: Round,
}

impl Consensus {
    pub fn new(committee: Committee, gc_depth: Round) -> Self {
        let mut dag = Dag::new();
        let genesis = Certificate::genesis(&committee)
            .into_iter()
            .map(|c| (c.origin(), c))
            .collect();
        dag.insert(0, genesis);
        Consensus {
            committee,
            gc_depth,
            dag,
            committed: HashMap::new(),
            last_committed_round: 0,
        }
    }

    pub fn last_committed_round(&self) -> Round {
        self.last_committed_round
    }

    /// Adds a certificate (whose parents must already have been added) and
    /// returns whatever became committed as a result, in commit order, each
    /// tagged with the round of the leader that committed it.
    pub fn process_certificate(&mut self, certificate: Certificate) -> Vec<(Round, Certificate)> {
        let round = certificate.round();
        if round + self.gc_depth < self.last_committed_round {
            return Vec::new();
        }
        self.dag
            .entry(round)
            .or_default()
            .insert(certificate.origin(), certificate);

        // A certificate at `round` means its parents, a quorum at `round - 1`,
        // are here. The candidate leader is two rounds below that.
        let Some(r) = round.checked_sub(1) else {
            return Vec::new();
        };
        if r % 2 != 0 || r < 4 {
            return Vec::new();
        }
        let leader_round = r - 2;
        if leader_round <= self.last_committed_round {
            return Vec::new();
        }
        let Some(leader) = self.leader(leader_round).cloned() else {
            return Vec::new();
        };

        let support: u64 = self
            .dag
            .get(&(leader_round + 1))
            .map(|certs| {
                certs
                    .values()
                    .filter(|c| c.header.parents.contains(&leader.digest()))
                    .map(|c| self.committee.stake(&c.origin()))
                    .sum()
            })
            .unwrap_or(0);
        if support < self.committee.validity_threshold() {
            return Vec::new();
        }

        let mut sequence = Vec::new();
        for leader in self.order_leaders(&leader).into_iter().rev() {
            let leader_round = leader.round();
            for c in self.order_dag(&leader) {
                self.committed.insert(c.digest(), c.round());
                sequence.push((leader_round, c));
            }
            self.last_committed_round = leader_round;
        }
        self.garbage_collect();
        sequence
    }

    fn leader(&self, round: Round) -> Option<&Certificate> {
        let name = self.committee.leader(round);
        self.dag.get(&round).and_then(|certs| certs.get(&name))
    }

    /// `leader` followed by every earlier uncommitted leader reachable from
    /// it through a chain of leaders, newest first.
    fn order_leaders(&self, leader: &Certificate) -> Vec<Certificate> {
        let mut chain = vec![leader.clone()];
        let mut current = leader.clone();
        let mut r = leader.round();
        while r >= self.last_committed_round + 4 {
            r -= 2;
            let Some(prev) = self.leader(r) else { continue };
            if self.linked(&current, prev) {
                chain.push(prev.clone());
                current = prev.clone();
            }
        }
        chain
    }

    /// Is there a path in the DAG from `from` down to `to`?
    fn linked(&self, from: &Certificate, to: &Certificate) -> bool {
        let mut frontier: HashSet<Digest> = from.header.parents.iter().copied().collect();
        for r in (to.round()..from.round()).rev() {
            if r == to.round() {
                return frontier.contains(&to.digest());
            }
            let Some(certs) = self.dag.get(&r) else {
                return false;
            };
            frontier = certs
                .values()
                .filter(|c| frontier.contains(&c.digest()))
                .flat_map(|c| c.header.parents.iter().copied())
                .collect();
        }
        false
    }

    /// The not yet committed causal history of `leader`, sorted by round and
    /// then author so every node lays it out identically.
    fn order_dag(&self, leader: &Certificate) -> Vec<Certificate> {
        let mut ordered = Vec::new();
        let mut visited = HashSet::new();
        let mut stack = vec![leader.clone()];
        visited.insert(leader.digest());
        while let Some(c) = stack.pop() {
            let parent_round = c.round().saturating_sub(1);
            if c.round() > 0 {
                if let Some(parents) = self.dag.get(&parent_round) {
                    for p in parents.values() {
                        let d = p.digest();
                        if c.header.parents.contains(&d)
                            && !self.committed.contains_key(&d)
                            && visited.insert(d)
                        {
                            stack.push(p.clone());
                        }
                    }
                }
            }
            ordered.push(c);
        }
        // Genesis carries no payload and anything at or below the GC horizon
        // may be partially pruned on some nodes, so leave both out.
        let horizon = self.last_committed_round.saturating_sub(self.gc_depth);
        ordered.retain(|c| c.round() > 0 && c.round() > horizon);
        ordered.sort_by_key(|c| (c.round(), c.origin()));
        ordered
    }

    fn garbage_collect(&mut self) {
        let horizon = self.last_committed_round.saturating_sub(self.gc_depth);
        self.dag.retain(|r, _| *r >= horizon);
        self.committed.retain(|_, r| *r >= horizon);
    }

    #[cfg(test)]
    fn dag_rounds(&self) -> Vec<Round> {
        self.dag.keys().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{committee_with_keys, genesis_digests, make_certificate, make_rounds};
    use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
    use std::collections::BTreeSet;

    fn feed(consensus: &mut Consensus, certs: &[Certificate]) -> Vec<(Round, Certificate)> {
        certs
            .iter()
            .flat_map(|c| consensus.process_certificate(c.clone()))
            .collect()
    }

    #[test]
    fn commits_fully_connected_dag() {
        let (keys, committee) = committee_with_keys(4);
        let (certs, _) = make_rounds(&keys, &[0, 1, 2, 3], genesis_digests(&committee), 1, 5);
        let mut consensus = Consensus::new(committee.clone(), 50);
        let out = feed(&mut consensus, &certs);

        // Round 5 triggers the leader of round 2 which pulls in rounds 1 and 2.
        assert_eq!(consensus.last_committed_round(), 2);
        assert_eq!(out.len(), 5);
        let leader = out.last().unwrap();
        assert_eq!(leader.1.round(), 2);
        assert_eq!(leader.1.origin(), committee.leader(2));
        assert!(out[..4].iter().all(|(_, c)| c.round() == 1));
    }

    #[test]
    fn each_certificate_committed_once() {
        let (keys, committee) = committee_with_keys(4);
        let (certs, _) = make_rounds(&keys, &[0, 1, 2, 3], genesis_digests(&committee), 1, 40);
        let mut consensus = Consensus::new(committee, 50);
        let out = feed(&mut consensus, &certs);
        let unique: HashSet<_> = out.iter().map(|(_, c)| c.digest()).collect();
        assert_eq!(unique.len(), out.len());
        // The last certificates are at round 40 (odd parent round), so the
        // last trigger is round 39 which decides the leader of round 36.
        let last = consensus.last_committed_round();
        assert_eq!(last, 36);
        assert_eq!(out.len() as u64, 4 * (last - 1) + 1);
    }

    #[test]
    fn progress_with_one_silent_node() {
        let (keys, committee) = committee_with_keys(4);
        let (certs, _) = make_rounds(&keys, &[0, 1, 2], genesis_digests(&committee), 1, 30);
        let mut consensus = Consensus::new(committee.clone(), 50);
        let out = feed(&mut consensus, &certs);
        assert!(consensus.last_committed_round() >= 20);
        assert!(out.iter().all(|(_, c)| c.origin() != keys[3].name));
    }

    #[test]
    fn same_sequence_regardless_of_delivery_order() {
        let (keys, committee) = committee_with_keys(4);
        let (certs, _) = make_rounds(&keys, &[0, 1, 2, 3], genesis_digests(&committee), 1, 30);

        let mut a = Consensus::new(committee.clone(), 50);
        let seq_a: Vec<_> = feed(&mut a, &certs)
            .into_iter()
            .map(|(_, c)| c.digest())
            .collect();

        // Shuffle within each round: any order that respects causality is a
        // legitimate delivery order.
        let mut rng = StdRng::seed_from_u64(42);
        for _ in 0..10 {
            let mut shuffled = Vec::new();
            for chunk in certs.chunks(4) {
                let mut chunk = chunk.to_vec();
                chunk.shuffle(&mut rng);
                shuffled.extend(chunk);
            }
            let mut b = Consensus::new(committee.clone(), 50);
            let seq_b: Vec<_> = feed(&mut b, &shuffled)
                .into_iter()
                .map(|(_, c)| c.digest())
                .collect();
            let n = seq_a.len().min(seq_b.len());
            assert!(n > 0);
            assert_eq!(seq_a[..n], seq_b[..n]);
        }
    }

    /// The leader of round 2 lacks support at round 3 so it is skipped at
    /// first; the leader of round 4 has a path to it, so it must be committed
    /// before the round 4 leader.
    #[test]
    fn skipped_leader_committed_through_link() {
        let (keys, committee) = committee_with_keys(4);
        let idx = |name| keys.iter().position(|k| k.name == name).unwrap();
        let l2 = idx(committee.leader(2));

        let (mut certs, r1) = make_rounds(&keys, &[0, 1, 2, 3], genesis_digests(&committee), 1, 2);
        let leader2 = certs
            .iter()
            .find(|c| c.round() == 2 && c.origin() == keys[l2].name)
            .unwrap()
            .digest();

        // Round 3: only one certificate (the leader's own) references leader 2.
        let mut r3 = BTreeSet::new();
        let others: Vec<Digest> = r1.iter().copied().filter(|d| *d != leader2).collect();
        for (i, k) in keys.iter().enumerate() {
            let parents: BTreeSet<_> = if i == l2 {
                r1.clone()
            } else {
                others.iter().copied().collect()
            };
            let c = make_certificate(k, &keys, 3, parents);
            r3.insert(c.digest());
            certs.push(c);
        }
        let (more, _) = make_rounds(&keys, &[0, 1, 2, 3], r3, 4, 4);
        certs.extend(more);

        let mut consensus = Consensus::new(committee.clone(), 50);
        let mut out = Vec::new();
        let mut leader_rounds = Vec::new();
        for c in &certs {
            for (lr, c) in consensus.process_certificate(c.clone()) {
                if leader_rounds.last() != Some(&lr) {
                    leader_rounds.push(lr);
                }
                out.push(c);
            }
        }
        // Leader 2 was not committed directly (not enough support) but is
        // reachable from leader 4 through the leader's own round 3 cert.
        assert_eq!(leader_rounds, vec![2, 4]);
        let pos = out.iter().position(|c| c.digest() == leader2).unwrap();
        let pos4 = out
            .iter()
            .position(|c| c.round() == 4 && c.origin() == committee.leader(4))
            .unwrap();
        assert!(pos < pos4);
    }

    #[test]
    fn leader_without_path_is_skipped_for_good() {
        let (keys, committee) = committee_with_keys(4);
        let idx = |name| keys.iter().position(|k| k.name == name).unwrap();
        let l2 = idx(committee.leader(2));
        let (mut certs, r1) = make_rounds(&keys, &[0, 1, 2, 3], genesis_digests(&committee), 1, 2);
        let leader2 = certs
            .iter()
            .find(|c| c.round() == 2 && c.origin() == keys[l2].name)
            .unwrap()
            .digest();

        // Nobody references leader 2, not even its author.
        let others: BTreeSet<Digest> = r1.iter().copied().filter(|d| *d != leader2).collect();
        let (more, _) = make_rounds(&keys, &[0, 1, 2, 3], others, 3, 6);
        certs.extend(more);

        let mut consensus = Consensus::new(committee.clone(), 50);
        let out = feed(&mut consensus, &certs);
        assert!(consensus.last_committed_round() >= 4);
        assert!(out.iter().all(|(_, c)| c.digest() != leader2));
    }

    #[test]
    fn garbage_collection_bounds_memory() {
        let (keys, committee) = committee_with_keys(4);
        let (certs, _) = make_rounds(&keys, &[0, 1, 2, 3], genesis_digests(&committee), 1, 100);
        let mut consensus = Consensus::new(committee, 10);
        feed(&mut consensus, &certs);
        let rounds = consensus.dag_rounds();
        assert!(*rounds.first().unwrap() >= consensus.last_committed_round() - 10);
        assert!(consensus.committed.len() <= 4 * 12);
    }
}
