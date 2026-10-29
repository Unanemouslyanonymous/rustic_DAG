//! Wires the network and the core together.

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::info;

use crate::config::{Committee, Parameters};
use crate::core::Core;
use crate::crypto::{KeyPair, PublicKey};
use crate::network::{listen, spawn_dialers};
use crate::types::CommittedCertificate;

/// A running node. Dropping the handle (or calling `shutdown`) stops every
/// task belonging to it and closes all of its sockets.
pub struct NodeHandle {
    pub name: PublicKey,
    pub commits: mpsc::UnboundedReceiver<CommittedCertificate>,
    tasks: JoinSet<()>,
}

impl NodeHandle {
    pub fn shutdown(mut self) {
        self.tasks.abort_all();
    }

    /// Next committed certificate, or `None` if one of the node's tasks
    /// stopped (which should only ever happen through a panic).
    pub async fn next_commit(&mut self) -> Option<CommittedCertificate> {
        tokio::select! {
            c = self.commits.recv() => c,
            _ = self.tasks.join_next() => None,
        }
    }
}

pub async fn spawn(keypair: KeyPair, committee: Committee, params: Parameters) -> anyhow::Result<NodeHandle> {
    let name = keypair.name;
    let address = committee
        .address(&name)
        .ok_or_else(|| anyhow::anyhow!("{name} is not in the committee"))?
        .to_string();
    let listener = TcpListener::bind(&address).await?;
    info!("node {name} listening on {address}");

    let mut tasks = JoinSet::new();
    let (inbound_tx, inbound_rx) = mpsc::channel(params.channel_capacity * 4);
    let (commit_tx, commit_rx) = mpsc::unbounded_channel();

    let network = spawn_dialers(&keypair, &committee, params.channel_capacity, &mut tasks);
    tasks.spawn(listen(listener, name, Arc::new(committee.clone()), inbound_tx));
    let core = Core::new(keypair, committee, params, network, commit_tx);
    tasks.spawn(core.run(inbound_rx));

    Ok(NodeHandle { name, commits: commit_rx, tasks })
}
