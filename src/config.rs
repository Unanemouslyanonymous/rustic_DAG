//! Committee membership and protocol parameters.

use std::collections::BTreeMap;
use std::path::Path;

use rand::{CryptoRng, RngCore};
use serde::{Deserialize, Serialize};

use crate::crypto::{Digest, KeyPair, PublicKey};
use crate::types::Round;

pub type Stake = u64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Authority {
    pub stake: Stake,
    /// `host:port` the authority listens on for WebSocket connections.
    pub address: String,
}

/// The static set of validators. Keyed by public key so iteration order is
/// the same everywhere, which several deterministic choices rely on.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Committee {
    pub authorities: BTreeMap<PublicKey, Authority>,
}

impl Committee {
    pub fn new(authorities: BTreeMap<PublicKey, Authority>) -> Self {
        Committee { authorities }
    }

    pub fn size(&self) -> usize {
        self.authorities.len()
    }

    pub fn stake(&self, name: &PublicKey) -> Stake {
        self.authorities.get(name).map_or(0, |a| a.stake)
    }

    pub fn contains(&self, name: &PublicKey) -> bool {
        self.authorities.contains_key(name)
    }

    pub fn total_stake(&self) -> Stake {
        self.authorities.values().map(|a| a.stake).sum()
    }

    /// 2f+1 out of 3f+1: any two quorums intersect in at least one honest node.
    pub fn quorum_threshold(&self) -> Stake {
        2 * self.total_stake() / 3 + 1
    }

    /// f+1: any set this large contains at least one honest node.
    pub fn validity_threshold(&self) -> Stake {
        self.total_stake().div_ceil(3)
    }

    pub fn address(&self, name: &PublicKey) -> Option<&str> {
        self.authorities.get(name).map(|a| a.address.as_str())
    }

    pub fn others(&self, me: &PublicKey) -> Vec<(PublicKey, String)> {
        self.authorities
            .iter()
            .filter(|(k, _)| *k != me)
            .map(|(k, a)| (*k, a.address.clone()))
            .collect()
    }

    /// Leader of the given round, weighted by stake.
    ///
    /// Tusk wants an unpredictable common coin (threshold signatures in the
    /// paper). Here the coin is a hash of the round number, which every node
    /// computes locally. That keeps the commit rule and all of its safety
    /// arguments intact but gives an adaptive network adversary the ability
    /// to know leaders in advance. See docs/REPORT.md for discussion.
    pub fn leader(&self, round: Round) -> PublicKey {
        let mut seed = b"rustic-dag/coin/v1".to_vec();
        seed.extend_from_slice(&round.to_be_bytes());
        let total = self.total_stake();
        let mut ticket = Digest::of(&seed).prefix_u64() % total;
        for (name, a) in &self.authorities {
            if ticket < a.stake {
                return *name;
            }
            ticket -= a.stake;
        }
        unreachable!("ticket is below the total stake")
    }

    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(&std::fs::read(path)?)?)
    }

    pub fn save(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        std::fs::write(path, serde_json::to_vec_pretty(self)?)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Parameters {
    /// Seal a batch once this many bytes of transactions are buffered.
    pub batch_size: usize,
    /// ... or once the oldest buffered transaction is this old.
    pub max_batch_delay_ms: u64,
    /// Propose a header as soon as this many batches are waiting.
    pub header_size: usize,
    /// Upper bound on batches referenced by a single header.
    pub max_header_batches: usize,
    /// Propose (possibly empty) headers at least this often once parents are ready.
    pub max_header_delay_ms: u64,
    /// How many rounds behind the last committed leader we keep around.
    pub gc_depth: Round,
    /// Re-request missing data and re-broadcast our own header this often.
    pub sync_retry_ms: u64,
    /// Per peer outbound queue length.
    pub channel_capacity: usize,
}

impl Default for Parameters {
    fn default() -> Self {
        Parameters {
            batch_size: 500_000,
            max_batch_delay_ms: 100,
            header_size: 1,
            max_header_batches: 64,
            max_header_delay_ms: 200,
            gc_depth: 50,
            sync_retry_ms: 1_000,
            channel_capacity: 2_000,
        }
    }
}

impl Parameters {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(&std::fs::read(path)?)?)
    }
}

/// On disk representation of a node's secret key.
#[derive(Serialize, Deserialize)]
pub struct KeyFile {
    pub name: PublicKey,
    pub secret: String,
}

impl KeyFile {
    pub fn from_keypair(kp: &KeyPair) -> Self {
        KeyFile { name: kp.name, secret: hex::encode(kp.secret_bytes()) }
    }

    pub fn into_keypair(self) -> anyhow::Result<KeyPair> {
        let bytes: [u8; 32] = hex::decode(&self.secret)?
            .try_into()
            .map_err(|_| anyhow::anyhow!("secret key must be 32 bytes"))?;
        let kp = KeyPair::from_secret(bytes);
        anyhow::ensure!(kp.name == self.name, "secret key does not match the public key");
        Ok(kp)
    }

    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<KeyPair> {
        let kf: KeyFile = serde_json::from_slice(&std::fs::read(path)?)?;
        kf.into_keypair()
    }

    pub fn save(kp: &KeyPair, path: impl AsRef<Path>) -> anyhow::Result<()> {
        std::fs::write(path, serde_json::to_vec_pretty(&KeyFile::from_keypair(kp))?)?;
        Ok(())
    }
}

/// Builds a local committee of `n` equally weighted nodes on consecutive ports.
pub fn local_committee<R: RngCore + CryptoRng>(
    rng: &mut R,
    n: usize,
    host: &str,
    base_port: u16,
) -> (Vec<KeyPair>, Committee) {
    let keys: Vec<KeyPair> = (0..n).map(|_| KeyPair::generate(rng)).collect();
    let authorities = keys
        .iter()
        .enumerate()
        .map(|(i, k)| {
            let a = Authority { stake: 1, address: format!("{host}:{}", base_port + i as u16) };
            (k.name, a)
        })
        .collect();
    (keys, Committee::new(authorities))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng};

    #[test]
    fn thresholds() {
        for (n, q, v) in [(4, 3, 2), (7, 5, 3), (10, 7, 4), (13, 9, 5)] {
            let (_, c) = local_committee(&mut StdRng::seed_from_u64(0), n, "127.0.0.1", 0);
            assert_eq!(c.quorum_threshold(), q, "n={n}");
            assert_eq!(c.validity_threshold(), v, "n={n}");
        }
    }

    #[test]
    fn leader_is_deterministic_and_spread() {
        let (_, c) = local_committee(&mut StdRng::seed_from_u64(0), 4, "127.0.0.1", 0);
        let mut seen = std::collections::HashSet::new();
        for r in 0..200 {
            assert_eq!(c.leader(r), c.leader(r));
            seen.insert(c.leader(r));
        }
        assert_eq!(seen.len(), 4);
    }

    #[test]
    fn committee_json_roundtrip() {
        let (_, c) = local_committee(&mut StdRng::seed_from_u64(0), 4, "127.0.0.1", 9000);
        let json = serde_json::to_string(&c).unwrap();
        let back: Committee = serde_json::from_str(&json).unwrap();
        assert_eq!(back.authorities.len(), 4);
        assert_eq!(back.total_stake(), 4);
    }
}
