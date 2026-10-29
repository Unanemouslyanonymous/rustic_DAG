//! Wire format. Every WebSocket frame is a single bincode encoded message.

use serde::{Deserialize, Serialize};

use crate::crypto::{Digest, KeyPair, PublicKey, Signature, DOMAIN_HELLO};
use crate::error::DagResult;
use crate::types::{Batch, Certificate, Header, Transaction, Vote};

/// Upper bound on digests in a single sync request.
pub const MAX_REQUEST_ITEMS: usize = 1_000;

/// Primary to primary traffic.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PeerMessage {
    Batch(Batch),
    Header(Header),
    Vote(Vote),
    Certificate(Certificate),
    CertificatesRequest(Vec<Digest>),
    BatchesRequest(Vec<Digest>),
}

/// First frame the server sends on every new connection.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Challenge {
    pub server: PublicKey,
    pub nonce: [u8; 32],
}

/// First frame the connecting side sends back.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Handshake {
    /// A committee member proving it owns `from` by signing the challenge.
    Peer(Hello),
    /// An unauthenticated client that only submits transactions.
    Client,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Hello {
    pub from: PublicKey,
    pub to: PublicKey,
    pub signature: Signature,
}

impl Hello {
    fn signed_bytes(from: &PublicKey, to: &PublicKey, nonce: &[u8; 32]) -> Vec<u8> {
        let mut m = Vec::with_capacity(96);
        m.extend_from_slice(&from.0);
        m.extend_from_slice(&to.0);
        m.extend_from_slice(nonce);
        m
    }

    pub fn new(keypair: &KeyPair, challenge: &Challenge) -> Self {
        let msg = Self::signed_bytes(&keypair.name, &challenge.server, &challenge.nonce);
        Hello {
            from: keypair.name,
            to: challenge.server,
            signature: keypair.sign(DOMAIN_HELLO, &msg),
        }
    }

    pub fn verify(&self, challenge: &Challenge) -> DagResult<()> {
        let msg = Self::signed_bytes(&self.from, &challenge.server, &challenge.nonce);
        self.signature.verify(DOMAIN_HELLO, &msg, &self.from)
    }
}

/// Client to node traffic.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientMessage {
    Transactions(Vec<Transaction>),
}

pub fn encode<T: Serialize>(msg: &T) -> bytes::Bytes {
    bincode::serialize(msg)
        .expect("in-memory serialisation cannot fail")
        .into()
}

pub fn decode<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> DagResult<T> {
    use bincode::Options;
    // Fixed int encoding to match `bincode::serialize`, plus a size limit so
    // a hostile length prefix cannot make us allocate gigabytes.
    Ok(bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .with_limit(64 * 1024 * 1024)
        .deserialize(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::committee_with_keys;

    #[test]
    fn hello_binds_server_and_nonce() {
        let (keys, _) = committee_with_keys(2);
        let ch = Challenge {
            server: keys[1].name,
            nonce: [7; 32],
        };
        let hello = Hello::new(&keys[0], &ch);
        assert!(hello.verify(&ch).is_ok());

        let replay = Challenge {
            server: keys[1].name,
            nonce: [8; 32],
        };
        assert!(hello.verify(&replay).is_err());
    }

    #[test]
    fn hostile_length_prefix_rejected() {
        // A Vec length of u64::MAX right after the enum tag.
        let mut bytes = 4u32.to_le_bytes().to_vec();
        bytes.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(decode::<PeerMessage>(&bytes).is_err());
    }
}
