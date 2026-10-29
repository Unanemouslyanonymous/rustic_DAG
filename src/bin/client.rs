//! Load generator: submits fixed size transactions to a node at a fixed rate.

use std::time::{Duration, Instant};

use clap::Parser;
use rustic_dag::network::ClientConnection;

#[derive(Parser)]
#[command(name = "rdag-client", about = "Submit transactions to a node")]
struct Cli {
    /// Node address (host:port).
    #[arg(long)]
    node: String,
    /// Transactions per second.
    #[arg(long, default_value_t = 1_000)]
    rate: u64,
    /// Transaction size in bytes (at least 16).
    #[arg(long, default_value_t = 512)]
    size: usize,
    /// Stop after this many seconds (0 = run forever).
    #[arg(long, default_value_t = 0)]
    duration: u64,
}

const BURSTS_PER_SEC: u64 = 50;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    anyhow::ensure!(cli.size >= 16, "transactions must be at least 16 bytes");
    let mut conn = ClientConnection::connect(&cli.node).await?;
    let per_burst = (cli.rate / BURSTS_PER_SEC).max(1);
    let mut tick = tokio::time::interval(Duration::from_millis(1000 / BURSTS_PER_SEC));
    let start = Instant::now();
    let mut counter: u64 = rand::random::<u32>() as u64 * 1_000_000_000;
    let mut sent = 0u64;
    loop {
        tick.tick().await;
        if cli.duration > 0 && start.elapsed() >= Duration::from_secs(cli.duration) {
            break;
        }
        let txs = (0..per_burst)
            .map(|_| {
                counter += 1;
                let mut tx = vec![0u8; cli.size];
                tx[..8].copy_from_slice(&counter.to_be_bytes());
                tx
            })
            .collect();
        conn.submit(txs).await?;
        sent += per_burst;
    }
    println!("sent {sent} transactions in {:.1}s", start.elapsed().as_secs_f64());
    Ok(())
}
