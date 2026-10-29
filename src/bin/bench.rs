//! Local benchmark: runs a full committee in one process over localhost
//! WebSockets, drives it with open-loop clients and reports throughput,
//! end-to-end latency and whether all nodes agreed on the same sequence.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::Parser;
use serde::Serialize;

use rustic_dag::cluster::{tx_id, CommitHook, LocalCluster};
use rustic_dag::network::ClientConnection;
use rustic_dag::{CommittedCertificate, Parameters};

#[derive(Parser, Clone, Serialize)]
#[command(name = "rdag-bench", about = "Benchmark a local committee")]
struct Args {
    /// Committee size.
    #[arg(long, default_value_t = 4)]
    nodes: usize,
    /// Nodes that never start (crash faults from the beginning).
    #[arg(long, default_value_t = 0)]
    faults: usize,
    /// Nodes that crash `crash_at` seconds into the run.
    #[arg(long, default_value_t = 0)]
    crash: usize,
    #[arg(long, default_value_t = 10)]
    crash_at: u64,
    /// Total offered load in transactions per second.
    #[arg(long, default_value_t = 20_000)]
    rate: u64,
    #[arg(long, default_value_t = 512)]
    tx_size: usize,
    /// Measurement window in seconds (after warm-up).
    #[arg(long, default_value_t = 20)]
    duration: u64,
    #[arg(long, default_value_t = 3)]
    warmup: u64,
    #[arg(long, default_value_t = 500_000)]
    batch_size: usize,
    #[arg(long, default_value_t = 100)]
    max_batch_delay_ms: u64,
    #[arg(long, default_value_t = 200)]
    max_header_delay_ms: u64,
    #[arg(long, default_value = "")]
    label: String,
    /// Append the result as one JSON line to this file.
    #[arg(long)]
    out: Option<std::path::PathBuf>,
}

#[derive(Serialize)]
struct Report {
    config: Args,
    offered_tps: u64,
    committed_tps: f64,
    committed_mb_per_s: f64,
    latency_ms_avg: f64,
    latency_ms_p50: f64,
    latency_ms_p90: f64,
    latency_ms_p99: f64,
    latency_samples: usize,
    committed_certificates: usize,
    last_leader_round: u64,
    leader_rounds_per_s: f64,
    consistent: bool,
    agreed_prefix: usize,
}

const BURST_MS: u64 = 10;

struct Window {
    start: Instant,
    from: Duration,
    to: Duration,
}

impl Window {
    fn contains(&self, t: Duration) -> bool {
        t >= self.from && t < self.to
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();
    let args = Args::parse();
    anyhow::ensure!(
        args.tx_size >= 16,
        "transactions need 16 bytes for id and timestamp"
    );
    anyhow::ensure!(
        args.faults + args.crash < args.nodes,
        "need at least one live node"
    );

    let params = Parameters {
        batch_size: args.batch_size,
        max_batch_delay_ms: args.max_batch_delay_ms,
        max_header_delay_ms: args.max_header_delay_ms,
        ..Parameters::default()
    };

    let live: Vec<usize> = (0..args.nodes - args.faults).collect();
    let observer = 0usize;
    let window = Arc::new(Window {
        start: Instant::now(),
        from: Duration::from_secs(args.warmup),
        to: Duration::from_secs(args.warmup + args.duration),
    });

    let latencies = Arc::new(Mutex::new(Vec::<u64>::new()));
    let committed_in_window = Arc::new(AtomicU64::new(0));
    let last_leader = Arc::new(AtomicU64::new(0));
    let on_commit = {
        let (window, latencies, committed, last_leader) = (
            window.clone(),
            latencies.clone(),
            committed_in_window.clone(),
            last_leader.clone(),
        );
        Arc::new(move |node: usize, c: &CommittedCertificate| {
            if node != observer {
                return;
            }
            let now = window.start.elapsed();
            last_leader.store(c.leader_round, Ordering::Relaxed);
            if !window.contains(now) {
                return;
            }
            let mut lat = latencies.lock().unwrap();
            for b in &c.batches {
                committed.fetch_add(b.transactions.len() as u64, Ordering::Relaxed);
                for tx in &b.transactions {
                    let sent = Duration::from_micros(tx_id(&tx[8..16]));
                    if window.contains(sent) {
                        lat.push((now - sent).as_micros() as u64);
                    }
                }
            }
        }) as CommitHook
    };

    let mut cluster =
        LocalCluster::start(args.nodes, &live, params, 0xbe9c, Some(on_commit)).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let per_node = args.rate / live.len() as u64;
    let per_burst = (per_node * BURST_MS / 1000).max(1);
    let end = window.to;
    let mut clients = Vec::new();
    for (client, &i) in live.iter().enumerate() {
        let address = cluster.nodes[i].address.clone();
        let window = window.clone();
        let size = args.tx_size;
        clients.push(tokio::spawn(async move {
            let Ok(mut conn) = ClientConnection::connect(&address).await else {
                return;
            };
            let mut tick = tokio::time::interval(Duration::from_millis(BURST_MS));
            let mut seq = 0u64;
            while window.start.elapsed() < end {
                tick.tick().await;
                let ts = window.start.elapsed().as_micros() as u64;
                let txs = (0..per_burst)
                    .map(|_| {
                        seq += 1;
                        let mut tx = vec![0u8; size];
                        tx[..8].copy_from_slice(&(((client as u64) << 48) | seq).to_be_bytes());
                        tx[8..16].copy_from_slice(&ts.to_be_bytes());
                        tx
                    })
                    .collect();
                if conn.submit(txs).await.is_err() {
                    return;
                }
            }
        }));
    }

    if args.crash > 0 {
        let at = Duration::from_secs(args.crash_at);
        tokio::time::sleep(at.saturating_sub(window.start.elapsed())).await;
        // Crash the highest numbered live nodes, never the observer.
        for i in live.iter().rev().take(args.crash) {
            eprintln!("crashing node {i}");
            cluster.nodes[*i].crash();
        }
    }

    tokio::time::sleep(end.saturating_sub(window.start.elapsed())).await;
    // Give in-flight transactions a moment to land before we stop measuring.
    tokio::time::sleep(Duration::from_secs(2)).await;
    for c in clients {
        c.abort();
    }

    let still_up: Vec<usize> = cluster.running().map(|n| n.index).collect();
    let consistency = cluster.check_consistency(&live);
    let committed_certificates = cluster.nodes[observer]
        .log
        .lock()
        .unwrap()
        .certificates
        .len();

    let mut lat = latencies.lock().unwrap().clone();
    lat.sort_unstable();
    let pct = |p: f64| -> f64 {
        if lat.is_empty() {
            return f64::NAN;
        }
        let i = ((lat.len() as f64 - 1.0) * p).round() as usize;
        lat[i] as f64 / 1000.0
    };
    let avg = if lat.is_empty() {
        f64::NAN
    } else {
        lat.iter().sum::<u64>() as f64 / lat.len() as f64 / 1000.0
    };
    let secs = args.duration as f64;
    let committed = committed_in_window.load(Ordering::Relaxed) as f64;
    let total_secs = window.start.elapsed().as_secs_f64();
    let last_leader_round = last_leader.load(Ordering::Relaxed);

    let report = Report {
        offered_tps: per_burst * (1000 / BURST_MS) * live.len() as u64,
        committed_tps: committed / secs,
        committed_mb_per_s: committed * args.tx_size as f64 / secs / 1e6,
        latency_ms_avg: avg,
        latency_ms_p50: pct(0.50),
        latency_ms_p90: pct(0.90),
        latency_ms_p99: pct(0.99),
        latency_samples: lat.len(),
        committed_certificates,
        last_leader_round,
        leader_rounds_per_s: last_leader_round as f64 / total_secs,
        consistent: consistency.is_ok(),
        agreed_prefix: consistency.clone().unwrap_or(0),
        config: args.clone(),
    };
    if let Err(e) = &consistency {
        eprintln!("SAFETY VIOLATION: {e}");
    }

    println!(
        "n={} faults={} crash={} rate={} -> {:.0} tx/s ({:.2} MB/s), latency avg {:.0} ms p50 {:.0} p99 {:.0}, \
         {} certs, consistent={} (prefix {} across {:?}, running {:?})",
        args.nodes,
        args.faults,
        args.crash,
        report.offered_tps,
        report.committed_tps,
        report.committed_mb_per_s,
        report.latency_ms_avg,
        report.latency_ms_p50,
        report.latency_ms_p99,
        report.committed_certificates,
        report.consistent,
        report.agreed_prefix,
        live,
        still_up,
    );
    if let Some(path) = &args.out {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        writeln!(f, "{}", serde_json::to_string(&report)?)?;
    }
    drop(cluster);
    std::process::exit(if report.consistent { 0 } else { 1 });
}
