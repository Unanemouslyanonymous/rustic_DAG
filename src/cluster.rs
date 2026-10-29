//! A whole committee inside one process, talking over real localhost
//! WebSockets. Used by the integration tests and the benchmark.

use std::sync::{Arc, Mutex};

use rand::{rngs::StdRng, SeedableRng};
use tokio::task::JoinHandle;

use crate::config::{local_committee, Committee, Parameters};
use crate::crypto::{Digest, KeyPair};
use crate::node::{self, NodeHandle};
use crate::types::{CommittedCertificate, Round};

/// Called with (node index, certificate) for every commit on every node.
pub type CommitHook = Arc<dyn Fn(usize, &CommittedCertificate) + Send + Sync>;

/// What a node has committed so far, in order.
#[derive(Default)]
pub struct CommitLog {
    pub certificates: Vec<(Digest, Round)>,
    /// First 8 bytes of every committed transaction (clients put a unique
    /// id there), in commit order.
    pub transaction_ids: Vec<u64>,
}

pub struct ClusterNode {
    pub index: usize,
    pub keypair: KeyPair,
    pub address: String,
    pub log: Arc<Mutex<CommitLog>>,
    handle: Option<(JoinHandle<()>, tokio::sync::oneshot::Sender<()>)>,
}

impl ClusterNode {
    pub fn is_running(&self) -> bool {
        self.handle.is_some()
    }

    /// Stops the node abruptly (all sockets closed, state discarded).
    pub fn crash(&mut self) {
        if let Some((task, stop)) = self.handle.take() {
            let _ = stop.send(());
            task.abort();
        }
    }
}

pub struct LocalCluster {
    pub committee: Committee,
    pub nodes: Vec<ClusterNode>,
}

/// Grabs `n` currently free TCP ports on localhost.
pub fn free_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<_> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect()
}

impl LocalCluster {
    /// Starts the nodes whose index is in `live`; the others exist in the
    /// committee but never come up (crash faults from the start).
    /// `on_commit` runs inside each node's collector task.
    pub async fn start(
        n: usize,
        live: &[usize],
        params: Parameters,
        seed: u64,
        on_commit: Option<CommitHook>,
    ) -> anyhow::Result<Self> {
        let (keys, mut committee) =
            local_committee(&mut StdRng::seed_from_u64(seed), n, "127.0.0.1", 0);
        let ports = free_ports(n);
        for (k, port) in keys.iter().zip(&ports) {
            committee.authorities.get_mut(&k.name).unwrap().address = format!("127.0.0.1:{port}");
        }

        let mut nodes = Vec::new();
        for (index, keypair) in keys.into_iter().enumerate() {
            let address = committee.address(&keypair.name).unwrap().to_string();
            let log = Arc::new(Mutex::new(CommitLog::default()));
            let handle = if live.contains(&index) {
                let h = node::spawn(keypair.clone(), committee.clone(), params.clone()).await?;
                Some(collect(index, h, log.clone(), on_commit.clone()))
            } else {
                None
            };
            nodes.push(ClusterNode {
                index,
                keypair,
                address,
                log,
                handle,
            });
        }
        Ok(LocalCluster { committee, nodes })
    }

    pub fn running(&self) -> impl Iterator<Item = &ClusterNode> {
        self.nodes.iter().filter(|n| n.is_running())
    }

    /// Checks that every node's committed sequence is a prefix of the
    /// longest one. Returns the length of the shortest sequence among
    /// `nodes` (the part everybody agrees on).
    pub fn check_consistency(&self, nodes: &[usize]) -> Result<usize, String> {
        let logs: Vec<Vec<(Digest, Round)>> = nodes
            .iter()
            .map(|i| self.nodes[*i].log.lock().unwrap().certificates.clone())
            .collect();
        let longest = logs
            .iter()
            .max_by_key(|l| l.len())
            .cloned()
            .unwrap_or_default();
        for (i, log) in nodes.iter().zip(&logs) {
            if let Some(pos) = log.iter().zip(&longest).position(|(a, b)| a != b) {
                return Err(format!("node {i} diverges at position {pos}"));
            }
        }
        Ok(logs.iter().map(|l| l.len()).min().unwrap_or(0))
    }
}

impl Drop for LocalCluster {
    fn drop(&mut self) {
        for n in &mut self.nodes {
            n.crash();
        }
    }
}

fn collect(
    index: usize,
    mut handle: NodeHandle,
    log: Arc<Mutex<CommitLog>>,
    on_commit: Option<CommitHook>,
) -> (JoinHandle<()>, tokio::sync::oneshot::Sender<()>) {
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                c = handle.next_commit() => {
                    let Some(c) = c else { break };
                    if let Some(f) = &on_commit {
                        f(index, &c);
                    }
                    let mut log = log.lock().unwrap();
                    log.certificates.push((c.certificate.digest(), c.certificate.round()));
                    for b in &c.batches {
                        log.transaction_ids.extend(b.transactions.iter().map(|t| tx_id(t)));
                    }
                }
                _ = &mut stop_rx => break,
            }
        }
        handle.shutdown();
    });
    (task, stop_tx)
}

/// Reads the id clients put in front of every transaction.
pub fn tx_id(tx: &[u8]) -> u64 {
    let mut id = [0u8; 8];
    let n = tx.len().min(8);
    id[..n].copy_from_slice(&tx[..n]);
    u64::from_be_bytes(id)
}
