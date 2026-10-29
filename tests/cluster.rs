//! End to end tests: real nodes, real WebSockets on localhost.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use rustic_dag::cluster::LocalCluster;
use rustic_dag::network::ClientConnection;
use rustic_dag::Parameters;

fn fast_params() -> Parameters {
    Parameters {
        batch_size: 16_000,
        max_batch_delay_ms: 20,
        max_header_delay_ms: 50,
        sync_retry_ms: 300,
        gc_depth: 50,
        ..Parameters::default()
    }
}

fn make_tx(id: u64) -> Vec<u8> {
    let mut tx = vec![0u8; 64];
    tx[..8].copy_from_slice(&id.to_be_bytes());
    tx
}

async fn submit(address: &str, ids: std::ops::Range<u64>) {
    let mut c = ClientConnection::connect(address).await.unwrap();
    let ids: Vec<u64> = ids.collect();
    for chunk in ids.chunks(50) {
        c.submit(chunk.iter().map(|i| make_tx(*i)).collect())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Waits until every node in `nodes` has committed all of `expected`.
async fn wait_for(
    cluster: &LocalCluster,
    nodes: &[usize],
    expected: &HashSet<u64>,
    timeout: Duration,
) {
    let start = Instant::now();
    loop {
        let done = nodes.iter().all(|i| {
            let log = cluster.nodes[*i].log.lock().unwrap();
            let seen: HashSet<u64> = log.transaction_ids.iter().copied().collect();
            expected.is_subset(&seen)
        });
        if done {
            return;
        }
        if start.elapsed() > timeout {
            for i in nodes {
                let log = cluster.nodes[*i].log.lock().unwrap();
                eprintln!(
                    "node {i}: {} certs, {} txs",
                    log.certificates.len(),
                    log.transaction_ids.len()
                );
            }
            panic!("timed out waiting for commits");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn assert_no_duplicates(cluster: &LocalCluster, nodes: &[usize]) {
    for i in nodes {
        let log = cluster.nodes[*i].log.lock().unwrap();
        let unique: HashSet<_> = log.transaction_ids.iter().collect();
        assert_eq!(
            unique.len(),
            log.transaction_ids.len(),
            "node {i} committed a transaction twice"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_nodes_commit_same_sequence() {
    let all = [0, 1, 2, 3];
    let cluster = LocalCluster::start(4, &all, fast_params(), 1, None)
        .await
        .unwrap();

    let mut expected = HashSet::new();
    let mut clients = Vec::new();
    for (i, node) in cluster.nodes.iter().enumerate() {
        let range = (i as u64 * 10_000)..(i as u64 * 10_000 + 500);
        expected.extend(range.clone());
        clients.push(tokio::spawn(submit_owned(node.address.clone(), range)));
    }
    for c in clients {
        c.await.unwrap();
    }

    wait_for(&cluster, &all, &expected, Duration::from_secs(30)).await;
    let agreed = cluster.check_consistency(&all).expect("logs diverged");
    assert!(agreed > 0);
    assert_no_duplicates(&cluster, &all);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tolerates_one_node_down_from_the_start() {
    let live = [0, 1, 3];
    let cluster = LocalCluster::start(4, &live, fast_params(), 2, None)
        .await
        .unwrap();

    let mut expected = HashSet::new();
    for i in live {
        let range = (i as u64 * 10_000)..(i as u64 * 10_000 + 300);
        expected.extend(range.clone());
        submit(&cluster.nodes[i].address, range).await;
    }
    wait_for(&cluster, &live, &expected, Duration::from_secs(30)).await;
    cluster.check_consistency(&live).expect("logs diverged");
    assert_no_duplicates(&cluster, &live);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keeps_going_after_a_crash() {
    let all = [0, 1, 2, 3];
    let mut cluster = LocalCluster::start(4, &all, fast_params(), 3, None)
        .await
        .unwrap();

    let first: HashSet<u64> = (0..200).collect();
    submit(&cluster.nodes[0].address, 0..200).await;
    wait_for(&cluster, &all, &first, Duration::from_secs(30)).await;

    cluster.nodes[2].crash();
    let survivors = [0, 1, 3];
    let second: HashSet<u64> = (1_000..1_200).collect();
    submit(&cluster.nodes[1].address, 1_000..1_200).await;
    wait_for(&cluster, &survivors, &second, Duration::from_secs(30)).await;

    cluster
        .check_consistency(&survivors)
        .expect("logs diverged");
    // The crashed node's log must still be a prefix of everyone else's.
    cluster
        .check_consistency(&all)
        .expect("crashed node diverged");
    assert_no_duplicates(&cluster, &survivors);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seven_nodes_two_down() {
    let live = [0, 1, 2, 4, 6];
    let cluster = LocalCluster::start(7, &live, fast_params(), 4, None)
        .await
        .unwrap();
    let expected: HashSet<u64> = (0..300).collect();
    submit(&cluster.nodes[4].address, 0..300).await;
    wait_for(&cluster, &live, &expected, Duration::from_secs(40)).await;
    cluster.check_consistency(&live).expect("logs diverged");
}

async fn submit_owned(address: String, ids: std::ops::Range<u64>) {
    submit(&address, ids).await
}
