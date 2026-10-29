# rustic_DAG

A DAG based Byzantine fault tolerant consensus engine in Rust, modelled on
[Narwhal and Tusk](https://arxiv.org/abs/2105.11827). Nodes talk to each
other over an authenticated peer to peer WebSocket mesh.

- **Narwhal mempool / DAG**: transactions go into batches, batches are
  disseminated separately, and every round each validator proposes a
  header that points to its batches and to a quorum of the previous round's
  certificates. A header with 2f+1 votes is a *certificate*: proof that the
  data is available and that its author did not equivocate.
- **Tusk ordering**: every node reads a total order out of its local
  DAG with no extra messages. Even rounds have a leader, a leader with f+1
  support from the next round is committed, and earlier leaders it links
  to are committed first.
- **Transport**: `tokio-tungstenite` WebSockets, a challenge-response
  handshake signed with the node's committee key, reconnects with
  backoff, and size-limited decoding.
- **Crypto**: Ed25519 (strict verification, batch verification for
  certificates), BLAKE3 digests, and a domain tag on every signed message.

The design, the security argument and the evaluation are in
[`docs/REPORT.md`](docs/REPORT.md). Raw benchmark results are in
[`results/`](results/).

## Layout

```
src/
  crypto.rs      digests, keys, signatures, domain separation
  types.rs       Batch, Header, Vote, Certificate (+ verification)
  config.rs      committee, thresholds, leader coin, parameters, key files
  messages.rs    wire messages, handshake, bounded bincode decoding
  network.rs     WebSocket server, per-peer dialers, client connection
  core.rs        the primary: batching, proposing, voting, certificates, sync, GC
  consensus.rs   Tusk commit rule
  node.rs        wiring
  cluster.rs     in-process multi-node cluster (tests and benchmark)
  bin/node.rs    rdag-node: generate configs / run a validator
  bin/client.rs  rdag-client: load generator
  bin/bench.rs   rdag-bench: local benchmark harness
tests/
  cluster.rs     end-to-end runs over real sockets (crashes included)
  byzantine.rs   equivocating validator
scripts/
  local_testnet.sh   N separate processes + clients, checks the commit logs agree
  run_eval.sh        full benchmark matrix -> results/
```

## Building and testing

Needs a recent stable Rust toolchain (tested with 1.97).

```sh
cargo build --release
cargo test --release
```

The unit tests cover signature and digest checks, the certificate quorum
rules, voting once per slot, sync of missing parents and payload, and the
commit rule: delivery order does not change the result, a skipped leader
gets committed through a later one, and GC keeps memory bounded. The
integration tests start real committees on localhost: 4 nodes, 4 with one
node down, 4 with a crash mid-run, 7 with two down, plus a Byzantine node
that sends conflicting headers.

## Running a local testnet

```sh
# 4 processes on ports 9100-9103, 30 s of load at 5k tx/s per node
scripts/local_testnet.sh 4 30 5000
```

Or by hand:

```sh
rdag-node generate --nodes 4 --base-port 9000 --dir config
rdag-node run --key config/node-0.json --committee config/committee.json \
              --parameters config/parameters.json --commit-log commits-0.txt
# ... same for node-1 .. node-3, then
rdag-client --node 127.0.0.1:9000 --rate 10000 --size 512
```

Each node logs every commit, and `--commit-log` also writes them to a
file, one line per certificate (`index round author digest tx_count`).

## Benchmarking

```sh
cargo run --release --bin rdag-bench -- --nodes 4 --rate 100000 --duration 20
scripts/run_eval.sh          # full matrix, writes results/bench.jsonl and results/summary.md
```

`rdag-bench` runs the whole committee in one process over localhost
WebSockets, with one open-loop client per live node. It reports committed
throughput, end-to-end latency (client submit to commit at node 0) and
whether every node's commit sequence is a prefix of the others'. The run
fails with a non-zero exit code if they are not.

## Parameters

| field | default | meaning |
|---|---|---|
| `batch_size` | 500000 | seal a batch at this many bytes |
| `max_batch_delay_ms` | 100 | ... or when the oldest transaction is this old |
| `header_size` | 1 | propose as soon as this many batches are waiting |
| `max_header_batches` | 64 | cap on batches per header |
| `max_header_delay_ms` | 200 | propose (possibly empty) headers at least this often |
| `gc_depth` | 50 | rounds kept below the last committed leader |
| `sync_retry_ms` | 1000 | re-request missing data / re-broadcast own header |
| `channel_capacity` | 2000 | per-peer outbound queue |

## Limitations

This is a research implementation. State is in memory only, so a
restarted node does not recover. The leader "coin" is a public hash of the
round, not a threshold signature. Links are not encrypted (every protocol
message is signed, but batches can be read in transit). See the report for
the full list.

## License

MIT
