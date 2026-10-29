//! Helpers for building committees and hand crafted DAGs in tests.

use std::collections::BTreeSet;

use rand::{rngs::StdRng, SeedableRng};

use crate::config::{local_committee, Committee};
use crate::crypto::{Digest, KeyPair};
use crate::types::{Certificate, Header, Round, Vote};

/// A deterministic committee of `n` nodes listening on 127.0.0.1:0.
pub fn committee_with_keys(n: usize) -> (Vec<KeyPair>, Committee) {
    local_committee(
        &mut StdRng::seed_from_u64(n as u64 * 7919),
        n,
        "127.0.0.1",
        0,
    )
}

/// A fully signed certificate for `author` at `round` with the given parents,
/// voted for by every key in `voters`.
pub fn make_certificate(
    author: &KeyPair,
    voters: &[KeyPair],
    round: Round,
    parents: BTreeSet<Digest>,
) -> Certificate {
    let header = Header::new(author, round, Vec::new(), parents);
    let votes = voters
        .iter()
        .map(|k| {
            let v = Vote::new(&header, k);
            (v.voter, v.signature)
        })
        .collect();
    Certificate { header, votes }
}

/// Builds rounds `1..=rounds` where each of `authors` proposes once per round
/// and references every certificate of the previous round it can see.
/// `authors` are indices into `keys`. Returns certificates in causal order.
pub fn make_rounds(
    keys: &[KeyPair],
    authors: &[usize],
    start: BTreeSet<Digest>,
    first_round: Round,
    rounds: Round,
) -> (Vec<Certificate>, BTreeSet<Digest>) {
    let mut out = Vec::new();
    let mut parents = start;
    for round in first_round..first_round + rounds {
        let mut next = BTreeSet::new();
        for &i in authors {
            let c = make_certificate(&keys[i], keys, round, parents.clone());
            next.insert(c.digest());
            out.push(c);
        }
        parents = next;
    }
    (out, parents)
}

pub fn genesis_digests(committee: &Committee) -> BTreeSet<Digest> {
    Certificate::genesis(committee)
        .iter()
        .map(|c| c.digest())
        .collect()
}
