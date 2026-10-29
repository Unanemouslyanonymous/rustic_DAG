//! A hand driven Byzantine committee member that equivocates: it sends one
//! header to some honest nodes and a conflicting header for the same round
//! to the others, then tries to gather a certificate for both.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use rustic_dag::cluster::LocalCluster;
use rustic_dag::messages::PeerMessage;
use rustic_dag::network::{listen, spawn_dialers, ClientConnection, Inbound};
use rustic_dag::testing::genesis_digests;
use rustic_dag::{Certificate, Header, Parameters, Vote};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn equivocating_author_cannot_get_two_certificates() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("rustic_dag::core=warn")
        .with_test_writer()
        .try_init();
    let params = Parameters {
        max_header_delay_ms: 50,
        max_batch_delay_ms: 20,
        ..Parameters::default()
    };
    // Node 3 is ours; the cluster only starts 0..3.
    let honest = [0, 1, 2];
    let cluster = LocalCluster::start(4, &honest, params, 99, None)
        .await
        .unwrap();
    let evil = cluster.nodes[3].keypair.clone();
    let committee = cluster.committee.clone();

    // Bring up the Byzantine node's own endpoint so honest nodes can send
    // it votes.
    let listener = TcpListener::bind(&cluster.nodes[3].address).await.unwrap();
    let (tx, mut rx) = mpsc::channel(10_000);
    let mut tasks = JoinSet::new();
    tasks.spawn(listen(listener, evil.name, Arc::new(committee.clone()), tx));
    let network = spawn_dialers(&evil, &committee, 1000, &mut tasks);

    // Two different headers for round 1: different parent sets.
    let all = genesis_digests(&committee);
    let mut fewer = all.clone();
    fewer.pop_last();
    let a = Header::new(&evil, 1, vec![], all);
    let b = Header::new(&evil, 1, vec![], fewer);
    assert_ne!(a.id, b.id);

    // Tell everybody both stories, in different orders.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let names: Vec<_> = honest
        .iter()
        .map(|i| cluster.nodes[*i].keypair.name)
        .collect();
    network.send(&names[0], &PeerMessage::Header(a.clone()));
    network.send(&names[1], &PeerMessage::Header(a.clone()));
    network.send(&names[2], &PeerMessage::Header(b.clone()));
    tokio::time::sleep(Duration::from_millis(100)).await;
    network.send(&names[0], &PeerMessage::Header(b.clone()));
    network.send(&names[1], &PeerMessage::Header(b.clone()));
    network.send(&names[2], &PeerMessage::Header(a.clone()));

    let mut voters: HashMap<_, HashSet<_>> = HashMap::new();
    let mut signatures = HashMap::new();
    let deadline = Instant::now() + Duration::from_secs(3);
    while let Ok(Some(msg)) = tokio::time::timeout_at(deadline.into(), rx.recv()).await {
        if let Inbound::Peer(_, PeerMessage::Vote(v)) = msg {
            if v.round == 1 && v.verify(&committee).is_ok() {
                voters.entry(v.header_id).or_default().insert(v.voter);
                signatures.insert((v.header_id, v.voter), v.signature);
            }
        }
    }
    let for_a = voters.get(&a.id).map_or(0, |s| s.len());
    let for_b = voters.get(&b.id).map_or(0, |s| s.len());
    println!(
        "byzantine: sent header A {} and conflicting header B {} for round 1",
        a.id, b.id
    );
    println!(
        "byzantine: honest votes received: A={for_a} B={for_b} (quorum is {})",
        committee.quorum_threshold()
    );
    // Every honest node voted for exactly one of the two.
    assert_eq!(for_a + for_b, 3, "a={for_a} b={for_b}");
    let both: HashSet<_> = voters.values().flatten().collect();
    assert_eq!(both.len(), 3);
    // With its own vote the attacker can certify at most one of them
    // (quorum is 3): 2 + 1 for A, but then only 1 + 1 for B.
    assert!(for_a.min(for_b) + 1 < committee.quorum_threshold() as usize);

    // Splicing the votes of one header onto the other does not work either:
    // votes sign the header digest.
    let (winner, loser) = if for_a >= for_b { (&a, &b) } else { (&b, &a) };
    let mut votes: Vec<_> = signatures
        .iter()
        .filter(|((h, _), _)| *h == winner.id)
        .map(|((_, voter), sig)| (*voter, *sig))
        .collect();
    votes.push((evil.name, Vote::new(winner, &evil).signature));
    let genuine = Certificate {
        header: winner.clone(),
        votes: votes.clone(),
    };
    assert!(genuine.verify(&committee).is_ok());
    let forged = Certificate {
        header: loser.clone(),
        votes,
    };
    let err = forged.verify(&committee).unwrap_err();
    println!(
        "byzantine: certificate for {} carrying the votes for {} rejected: {err}",
        loser.id, winner.id
    );

    // Honest nodes keep making progress and agree with each other.
    let mut c = ClientConnection::connect(&cluster.nodes[0].address)
        .await
        .unwrap();
    for i in 0..10u64 {
        let mut t = vec![0u8; 32];
        t[..8].copy_from_slice(&i.to_be_bytes());
        c.submit(vec![t]).await.unwrap();
    }
    let start = Instant::now();
    loop {
        let done = honest
            .iter()
            .all(|i| cluster.nodes[*i].log.lock().unwrap().transaction_ids.len() >= 10);
        if done || start.elapsed() > Duration::from_secs(20) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(honest
        .iter()
        .all(|i| cluster.nodes[*i].log.lock().unwrap().transaction_ids.len() >= 10));
    let agreed = cluster
        .check_consistency(&honest)
        .expect("honest nodes diverged");
    println!("byzantine: honest nodes keep committing and agree on all {agreed} certificates");

    // Whichever round 1 header of ours (if any) got committed, there is only one.
    for i in honest {
        let log = cluster.nodes[i].log.lock().unwrap();
        let ours: BTreeSet<_> = log
            .certificates
            .iter()
            .filter(|(d, _)| *d == a.id || *d == b.id)
            .collect();
        assert!(ours.len() <= 1);
    }
}
