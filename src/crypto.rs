//! Hashing and signatures.
//!
//! Everything that goes over the wire and needs to be authenticated is
//! reduced to a 32 byte BLAKE3 digest and signed with Ed25519. Signed
//! messages always carry a domain tag so a signature produced for one
//! purpose (say, a vote) can never be replayed as another (say, a header).

use std::fmt;

use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use rand::{CryptoRng, RngCore};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{DagError, DagResult};

/// Domain tags. Bump the suffix if the encoding of a signed object changes.
pub const DOMAIN_HEADER: &[u8] = b"rustic-dag/header/v1";
pub const DOMAIN_VOTE: &[u8] = b"rustic-dag/vote/v1";
pub const DOMAIN_HELLO: &[u8] = b"rustic-dag/hello/v1";

/// Serialises fixed size byte arrays as hex for human readable formats
/// (JSON config files) and as raw bytes for bincode.
macro_rules! bytes_newtype_serde {
    ($ty:ident, $len:expr) => {
        impl Serialize for $ty {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                if s.is_human_readable() {
                    s.serialize_str(&hex::encode(self.0))
                } else {
                    s.serialize_bytes(&self.0)
                }
            }
        }

        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let bytes: Vec<u8> = if d.is_human_readable() {
                    let s = String::deserialize(d)?;
                    hex::decode(s).map_err(de::Error::custom)?
                } else {
                    serde_bytes_compat::deserialize(d)?
                };
                let arr: [u8; $len] = bytes
                    .try_into()
                    .map_err(|_| de::Error::custom(concat!("expected ", $len, " bytes")))?;
                Ok($ty(arr))
            }
        }
    };
}

/// bincode encodes `serialize_bytes` as a length prefixed byte string; this
/// is the matching visitor (avoids pulling in serde_bytes for one function).
mod serde_bytes_compat {
    use serde::de::{self, Deserializer, SeqAccess, Visitor};
    use std::fmt;

    struct BytesVisitor;

    impl<'de> Visitor<'de> for BytesVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a byte string")
        }

        fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Vec<u8>, E> {
            Ok(v.to_vec())
        }

        fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Vec<u8>, E> {
            Ok(v)
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<u8>, A::Error> {
            let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0));
            while let Some(b) = seq.next_element()? {
                out.push(b);
            }
            Ok(out)
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        d.deserialize_bytes(BytesVisitor)
    }
}

/// A BLAKE3 digest.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Digest(pub [u8; 32]);

bytes_newtype_serde!(Digest, 32);

impl Digest {
    pub fn of(bytes: &[u8]) -> Self {
        Digest(*blake3::hash(bytes).as_bytes())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// First 8 bytes interpreted as a big endian integer. Used by the coin.
    pub fn prefix_u64(&self) -> u64 {
        u64::from_be_bytes(self.0[..8].try_into().unwrap())
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &hex::encode(self.0)[..12])
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// An Ed25519 public key; doubles as the identity of an authority.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PublicKey(pub [u8; 32]);

bytes_newtype_serde!(PublicKey, 32);

impl PublicKey {
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn from_hex(s: &str) -> DagResult<Self> {
        let bytes = hex::decode(s).map_err(|_| DagError::MalformedKey)?;
        let arr: [u8; 32] = bytes.try_into().map_err(|_| DagError::MalformedKey)?;
        Ok(PublicKey(arr))
    }

    fn verifying_key(&self) -> DagResult<VerifyingKey> {
        VerifyingKey::from_bytes(&self.0).map_err(|_| DagError::MalformedKey)
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.to_hex()[..8])
    }
}

impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// An Ed25519 signature.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Signature(pub [u8; 64]);

bytes_newtype_serde!(Signature, 64);

impl Default for Signature {
    fn default() -> Self {
        Signature([0u8; 64])
    }
}

impl fmt::Debug for Signature {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "sig:{}", &hex::encode(self.0)[..12])
    }
}

impl Signature {
    /// Verifies a signature over `domain || msg` using strict verification
    /// (rejects small order keys and non-canonical encodings).
    pub fn verify(&self, domain: &[u8], msg: &[u8], key: &PublicKey) -> DagResult<()> {
        let vk = key.verifying_key()?;
        let sig = ed25519_dalek::Signature::from_bytes(&self.0);
        vk.verify_strict(&tagged(domain, msg), &sig)
            .map_err(|_| DagError::InvalidSignature)
    }

    /// Verifies many signatures over the same message at once. Considerably
    /// faster than one by one, which matters since every certificate carries
    /// a quorum of them.
    pub fn verify_batch(
        domain: &[u8],
        msg: &[u8],
        votes: &[(PublicKey, Signature)],
    ) -> DagResult<()> {
        let m = tagged(domain, msg);
        let messages: Vec<&[u8]> = votes.iter().map(|_| m.as_slice()).collect();
        let signatures: Vec<ed25519_dalek::Signature> = votes
            .iter()
            .map(|(_, s)| ed25519_dalek::Signature::from_bytes(&s.0))
            .collect();
        let keys = votes
            .iter()
            .map(|(k, _)| k.verifying_key())
            .collect::<DagResult<Vec<_>>>()?;
        // The batch equation accepts some weak keys that strict verification
        // refuses, so screen those out first.
        if keys.iter().any(|k| k.is_weak()) {
            return Err(DagError::MalformedKey);
        }
        ed25519_dalek::verify_batch(&messages, &signatures, &keys)
            .map_err(|_| DagError::InvalidSignature)
    }
}

fn tagged(domain: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(domain.len() + 1 + msg.len());
    out.extend_from_slice(domain);
    out.push(0);
    out.extend_from_slice(msg);
    out
}

/// Long term signing key of an authority.
pub struct KeyPair {
    pub name: PublicKey,
    secret: SigningKey,
}

impl KeyPair {
    pub fn generate<R: RngCore + CryptoRng>(rng: &mut R) -> Self {
        Self::from_signing_key(SigningKey::generate(rng))
    }

    pub fn from_secret(secret: [u8; 32]) -> Self {
        Self::from_signing_key(SigningKey::from_bytes(&secret))
    }

    fn from_signing_key(secret: SigningKey) -> Self {
        let name = PublicKey(secret.verifying_key().to_bytes());
        KeyPair { name, secret }
    }

    pub fn secret_bytes(&self) -> [u8; 32] {
        self.secret.to_bytes()
    }

    pub fn sign(&self, domain: &[u8], msg: &[u8]) -> Signature {
        Signature(self.secret.sign(&tagged(domain, msg)).to_bytes())
    }
}

impl Clone for KeyPair {
    fn clone(&self) -> Self {
        KeyPair::from_secret(self.secret_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng};

    #[test]
    fn sign_and_verify() {
        let kp = KeyPair::generate(&mut StdRng::seed_from_u64(1));
        let sig = kp.sign(DOMAIN_VOTE, b"hello");
        assert!(sig.verify(DOMAIN_VOTE, b"hello", &kp.name).is_ok());
        assert!(sig.verify(DOMAIN_VOTE, b"hellO", &kp.name).is_err());
    }

    #[test]
    fn domains_are_separated() {
        let kp = KeyPair::generate(&mut StdRng::seed_from_u64(2));
        let sig = kp.sign(DOMAIN_VOTE, b"payload");
        assert!(sig.verify(DOMAIN_HEADER, b"payload", &kp.name).is_err());
    }

    #[test]
    fn batch_verification() {
        let mut rng = StdRng::seed_from_u64(3);
        let keys: Vec<_> = (0..5).map(|_| KeyPair::generate(&mut rng)).collect();
        let mut votes: Vec<_> = keys
            .iter()
            .map(|k| (k.name, k.sign(DOMAIN_VOTE, b"m")))
            .collect();
        assert!(Signature::verify_batch(DOMAIN_VOTE, b"m", &votes).is_ok());
        votes[2].1 = keys[2].sign(DOMAIN_VOTE, b"other");
        assert!(Signature::verify_batch(DOMAIN_VOTE, b"m", &votes).is_err());
    }

    #[test]
    fn serde_roundtrip() {
        let kp = KeyPair::generate(&mut StdRng::seed_from_u64(4));
        let json = serde_json::to_string(&kp.name).unwrap();
        assert_eq!(json.len(), 66);
        let back: PublicKey = serde_json::from_str(&json).unwrap();
        assert_eq!(back, kp.name);
        let sig = kp.sign(DOMAIN_VOTE, b"x");
        let bin = bincode::serialize(&sig).unwrap();
        let back: Signature = bincode::deserialize(&bin).unwrap();
        assert_eq!(back, sig);
    }
}
