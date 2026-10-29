//! A DAG based BFT consensus engine in the style of Narwhal and Tusk
//! (Danezis, Kokoris-Kogias, Sonnino, Spiegelman; EuroSys 2022).
//!
//! * `types`     – batches, headers, votes and certificates
//! * `core`      – the primary: dissemination, voting, certificate
//!   aggregation, DAG maintenance and synchronisation
//! * `consensus` – Tusk commit rule over the local DAG
//! * `network`   – authenticated WebSocket mesh between committee members
//! * `node`      – glue that runs all of the above

pub mod cluster;
pub mod config;
pub mod consensus;
pub mod core;
pub mod crypto;
pub mod error;
pub mod messages;
pub mod network;
pub mod node;
pub mod testing;
pub mod types;

pub use config::{Committee, Parameters};
pub use crypto::{Digest, KeyPair, PublicKey};
pub use types::{Batch, Certificate, CommittedCertificate, Header, Round, Vote};
