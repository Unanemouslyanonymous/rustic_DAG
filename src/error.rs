use thiserror::Error;

use crate::crypto::{Digest, PublicKey};
use crate::types::Round;

pub type DagResult<T> = Result<T, DagError>;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DagError {
    #[error("malformed public key")]
    MalformedKey,

    #[error("invalid signature")]
    InvalidSignature,

    #[error("unknown authority {0}")]
    UnknownAuthority(PublicKey),

    #[error("digest mismatch: object does not hash to {0}")]
    BadDigest(Digest),

    #[error("authority {0} voted more than once")]
    DuplicateVote(PublicKey),

    #[error("certificate {0} does not have a quorum of votes")]
    NoQuorum(Digest),

    #[error("header {digest} has invalid parents: {reason}")]
    BadParents { digest: Digest, reason: &'static str },

    #[error("authority {author} equivocated at round {round}")]
    Equivocation { author: PublicKey, round: Round },

    #[error("message too old (round {0})")]
    TooOld(Round),

    #[error("serialisation error: {0}")]
    Serialisation(String),
}

impl From<bincode::Error> for DagError {
    fn from(e: bincode::Error) -> Self {
        DagError::Serialisation(e.to_string())
    }
}
