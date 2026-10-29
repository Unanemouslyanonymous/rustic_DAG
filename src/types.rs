//! The objects that make up the DAG: batches, headers, votes, certificates.

use std::collections::{BTreeSet, HashSet};

use serde::{Deserialize, Serialize};

use crate::config::Committee;
use crate::crypto::{Digest, KeyPair, PublicKey, Signature, DOMAIN_HEADER, DOMAIN_VOTE};
use crate::error::{DagError, DagResult};

pub type Round = u64;
pub type Transaction = Vec<u8>;

/// A bag of client transactions. Batches are disseminated on their own and
/// headers only reference them by digest, so the consensus path never moves
/// transaction bytes (the main idea behind Narwhal).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Batch {
    pub author: PublicKey,
    /// Per author sequence number, keeps digests unique even if two batches
    /// happen to carry identical transactions.
    pub nonce: u64,
    pub transactions: Vec<Transaction>,
}

impl Batch {
    pub fn digest(&self) -> Digest {
        Digest::of(&bincode::serialize(self).expect("batch serialises"))
    }

    pub fn size_bytes(&self) -> usize {
        self.transactions.iter().map(|t| t.len()).sum()
    }
}

/// A DAG vertex proposal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Header {
    pub author: PublicKey,
    pub round: Round,
    /// Digests of batches, in the order their transactions should be executed.
    pub payload: Vec<Digest>,
    /// Certificates of round `round - 1`.
    pub parents: BTreeSet<Digest>,
    pub id: Digest,
    pub signature: Signature,
}

impl Header {
    pub fn new(
        keypair: &KeyPair,
        round: Round,
        payload: Vec<Digest>,
        parents: BTreeSet<Digest>,
    ) -> Self {
        let id = Self::compute_id(&keypair.name, round, &payload, &parents);
        let signature = keypair.sign(DOMAIN_HEADER, id.as_bytes());
        Header { author: keypair.name, round, payload, parents, id, signature }
    }

    fn compute_id(
        author: &PublicKey,
        round: Round,
        payload: &[Digest],
        parents: &BTreeSet<Digest>,
    ) -> Digest {
        let bytes = bincode::serialize(&(author, round, payload, parents)).expect("header serialises");
        Digest::of(&bytes)
    }

    /// Checks everything that can be checked without looking at the DAG.
    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        if !committee.contains(&self.author) {
            return Err(DagError::UnknownAuthority(self.author));
        }
        let id = Self::compute_id(&self.author, self.round, &self.payload, &self.parents);
        if id != self.id {
            return Err(DagError::BadDigest(self.id));
        }
        if self.round == 0 {
            // Only genesis lives at round zero and it is never sent around.
            return Err(DagError::BadParents { digest: self.id, reason: "round 0 header" });
        }
        let distinct: HashSet<_> = self.payload.iter().collect();
        if distinct.len() != self.payload.len() {
            return Err(DagError::BadDigest(self.id));
        }
        self.signature.verify(DOMAIN_HEADER, self.id.as_bytes(), &self.author)
    }
}

/// A signed acknowledgement that the voter stores the header's payload and
/// has not voted for a different header of the same author and round.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Vote {
    pub header_id: Digest,
    pub round: Round,
    pub origin: PublicKey,
    pub voter: PublicKey,
    pub signature: Signature,
}

impl Vote {
    pub fn new(header: &Header, keypair: &KeyPair) -> Self {
        Vote {
            header_id: header.id,
            round: header.round,
            origin: header.author,
            voter: keypair.name,
            signature: keypair.sign(DOMAIN_VOTE, header.id.as_bytes()),
        }
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        if !committee.contains(&self.voter) {
            return Err(DagError::UnknownAuthority(self.voter));
        }
        self.signature.verify(DOMAIN_VOTE, self.header_id.as_bytes(), &self.voter)
    }
}

/// A header together with a quorum of votes. Holding one proves that at
/// least f+1 honest nodes store the payload (availability) and that no other
/// header by the same author for that round can be certified (non
/// equivocation), because two quorums always share an honest node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Certificate {
    pub header: Header,
    pub votes: Vec<(PublicKey, Signature)>,
}

impl Certificate {
    /// One empty certificate per authority at round 0. They are never
    /// transmitted: everyone derives the same set from the committee.
    pub fn genesis(committee: &Committee) -> Vec<Certificate> {
        committee
            .authorities
            .keys()
            .map(|name| {
                let parents = BTreeSet::new();
                let id = Header::compute_id(name, 0, &[], &parents);
                Certificate {
                    header: Header {
                        author: *name,
                        round: 0,
                        payload: Vec::new(),
                        parents,
                        id,
                        signature: Signature::default(),
                    },
                    votes: Vec::new(),
                }
            })
            .collect()
    }

    pub fn digest(&self) -> Digest {
        self.header.id
    }

    pub fn round(&self) -> Round {
        self.header.round
    }

    pub fn origin(&self) -> PublicKey {
        self.header.author
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        self.header.verify(committee)?;

        let mut seen = HashSet::new();
        let mut stake = 0;
        for (voter, _) in &self.votes {
            if !seen.insert(*voter) {
                return Err(DagError::DuplicateVote(*voter));
            }
            let s = committee.stake(voter);
            if s == 0 {
                return Err(DagError::UnknownAuthority(*voter));
            }
            stake += s;
        }
        if stake < committee.quorum_threshold() {
            return Err(DagError::NoQuorum(self.digest()));
        }
        Signature::verify_batch(DOMAIN_VOTE, self.header.id.as_bytes(), &self.votes)
    }
}

/// What a node hands to the application: a certificate in commit order along
/// with the batches it references.
#[derive(Clone, Debug)]
pub struct CommittedCertificate {
    /// Position of this certificate in the global sequence (starting at 0).
    pub index: u64,
    /// Round of the leader whose commit pulled this certificate in.
    pub leader_round: Round,
    pub certificate: Certificate,
    pub batches: Vec<Batch>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::committee_with_keys;

    #[test]
    fn header_roundtrip_and_tamper() {
        let (keys, committee) = committee_with_keys(4);
        let parents = Certificate::genesis(&committee).iter().map(|c| c.digest()).collect();
        let h = Header::new(&keys[0], 1, vec![Digest::of(b"b")], parents);
        assert!(h.verify(&committee).is_ok());

        let mut bad = h.clone();
        bad.round = 2;
        assert_eq!(bad.verify(&committee), Err(DagError::BadDigest(h.id)));

        let mut forged = h.clone();
        forged.signature = keys[1].sign(DOMAIN_HEADER, h.id.as_bytes());
        assert_eq!(forged.verify(&committee), Err(DagError::InvalidSignature));
    }

    #[test]
    fn header_from_outsider_rejected() {
        let (_, committee) = committee_with_keys(4);
        let (outsiders, _) = committee_with_keys(1);
        let h = Header::new(&outsiders[0], 1, vec![], BTreeSet::new());
        assert!(matches!(h.verify(&committee), Err(DagError::UnknownAuthority(_))));
    }

    #[test]
    fn certificate_needs_quorum() {
        let (keys, committee) = committee_with_keys(4);
        let h = Header::new(&keys[0], 1, vec![], BTreeSet::new());
        let votes: Vec<_> = keys.iter().map(|k| Vote::new(&h, k)).map(|v| (v.voter, v.signature)).collect();

        let ok = Certificate { header: h.clone(), votes: votes[..3].to_vec() };
        assert!(ok.verify(&committee).is_ok());

        let short = Certificate { header: h.clone(), votes: votes[..2].to_vec() };
        assert_eq!(short.verify(&committee), Err(DagError::NoQuorum(h.id)));

        let dup = Certificate { header: h.clone(), votes: vec![votes[0], votes[1], votes[1]] };
        assert_eq!(dup.verify(&committee), Err(DagError::DuplicateVote(keys[1].name)));

        let mut forged = votes[..3].to_vec();
        forged[2].1 = keys[3].sign(DOMAIN_VOTE, h.id.as_bytes());
        let bad = Certificate { header: h, votes: forged };
        assert_eq!(bad.verify(&committee), Err(DagError::InvalidSignature));
    }

    #[test]
    fn header_signature_is_not_a_vote() {
        // The author's header signature must not be usable as a vote.
        let (keys, committee) = committee_with_keys(4);
        let h = Header::new(&keys[0], 1, vec![], BTreeSet::new());
        let mut votes: Vec<_> = keys[1..3].iter().map(|k| (k.name, Vote::new(&h, k).signature)).collect();
        votes.push((keys[0].name, h.signature));
        let c = Certificate { header: h, votes };
        assert_eq!(c.verify(&committee), Err(DagError::InvalidSignature));
    }

    #[test]
    fn genesis_is_stable() {
        let (_, committee) = committee_with_keys(4);
        let a: Vec<_> = Certificate::genesis(&committee).iter().map(|c| c.digest()).collect();
        let b: Vec<_> = Certificate::genesis(&committee).iter().map(|c| c.digest()).collect();
        assert_eq!(a, b);
        assert_eq!(a.iter().collect::<HashSet<_>>().len(), 4);
    }
}
