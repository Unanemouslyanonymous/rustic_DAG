#!/usr/bin/env bash
# Runs the evaluation matrix and writes results/bench.jsonl + results/summary.md.
# Usage: scripts/run_eval.sh [duration-seconds]
set -euo pipefail
cd "$(dirname "$0")/.."

DUR=${1:-20}
OUT=results/bench.jsonl
cargo build --release --bin rdag-bench
rm -f "$OUT"
B=./target/release/rdag-bench
run() { echo ">> $*"; "$B" --duration "$DUR" --out "$OUT" "$@"; }

# 1. Throughput vs latency, 4 nodes, 512 byte transactions.
for rate in 10000 50000 100000 200000 300000; do
    run --label load --nodes 4 --rate "$rate"
done

# 2. Committee size at a fixed load.
for n in 4 7 10; do
    run --label scale --nodes "$n" --rate 100000
done

# 3. Crash faults present from the start (f = (n-1)/3).
run --label faults --nodes 4 --faults 1 --rate 50000
run --label faults --nodes 7 --faults 2 --rate 50000
run --label faults --nodes 10 --faults 3 --rate 50000

# 4. A node crashes mid-run.
run --label crash --nodes 4 --crash 1 --crash-at 10 --rate 50000
run --label crash --nodes 7 --crash 2 --crash-at 10 --rate 50000

# 5. Small batches / short timeouts: latency oriented configuration.
run --label lowlat --nodes 4 --rate 20000 --batch-size 50000 --max-batch-delay-ms 10 --max-header-delay-ms 20
run --label lowlat --nodes 4 --rate 100000 --batch-size 50000 --max-batch-delay-ms 10 --max-header-delay-ms 20

python3 scripts/summarize.py "$OUT" > results/summary.md
cat results/summary.md
