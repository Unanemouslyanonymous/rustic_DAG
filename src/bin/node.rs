use std::io::Write;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use rand::rngs::OsRng;
use tracing::info;

use rustic_dag::config::{local_committee, KeyFile};
use rustic_dag::{node, Committee, Parameters};

#[derive(Parser)]
#[command(name = "rdag-node", about = "Narwhal/Tusk style DAG consensus node")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Generate keys, a committee file and default parameters for a local testbed.
    Generate {
        #[arg(long, default_value_t = 4)]
        nodes: usize,
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 9000)]
        base_port: u16,
        #[arg(long, default_value = "config")]
        dir: PathBuf,
    },
    /// Run a node.
    Run {
        #[arg(long)]
        key: PathBuf,
        #[arg(long)]
        committee: PathBuf,
        #[arg(long)]
        parameters: Option<PathBuf>,
        /// Append one line per committed certificate to this file.
        #[arg(long)]
        commit_log: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    match Cli::parse().cmd {
        Cmd::Generate {
            nodes,
            host,
            base_port,
            dir,
        } => {
            std::fs::create_dir_all(&dir)?;
            let (keys, committee) = local_committee(&mut OsRng, nodes, &host, base_port);
            for (i, k) in keys.iter().enumerate() {
                KeyFile::save(k, dir.join(format!("node-{i}.json")))?;
            }
            committee.save(dir.join("committee.json"))?;
            std::fs::write(
                dir.join("parameters.json"),
                serde_json::to_vec_pretty(&Parameters::default())?,
            )?;
            println!(
                "wrote {nodes} keys, committee.json and parameters.json to {}",
                dir.display()
            );
        }
        Cmd::Run {
            key,
            committee,
            parameters,
            commit_log,
        } => {
            let keypair = KeyFile::load(key)?;
            let committee = Committee::load(committee)?;
            let params = match parameters {
                Some(p) => Parameters::load(p)?,
                None => Parameters::default(),
            };
            let mut handle = node::spawn(keypair, committee, params).await?;
            let mut log = match commit_log {
                Some(p) => Some(
                    std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(p)?,
                ),
                None => None,
            };
            loop {
                tokio::select! {
                    c = handle.next_commit() => {
                        let Some(c) = c else { anyhow::bail!("node task stopped unexpectedly") };
                        let txs: usize = c.batches.iter().map(|b| b.transactions.len()).sum();
                        info!(
                            "commit #{} leader {} cert {} round {} author {} txs {}",
                            c.index, c.leader_round, c.certificate.digest(), c.certificate.round(),
                            c.certificate.origin(), txs
                        );
                        if let Some(f) = log.as_mut() {
                            writeln!(f, "{} {} {} {} {}", c.index, c.certificate.round(),
                                c.certificate.origin(), c.certificate.digest(), txs)?;
                        }
                    }
                    _ = tokio::signal::ctrl_c() => break,
                }
            }
        }
    }
    Ok(())
}
