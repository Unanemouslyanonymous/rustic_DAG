#!/usr/bin/env bash
# Starts a local committee as separate processes plus one load client per node.
# Usage: scripts/local_testnet.sh [nodes] [seconds] [rate-per-node]
set -euo pipefail
cd "$(dirname "$0")/.."

N=${1:-4}
SECS=${2:-30}
RATE=${3:-5000}
cargo build --release --bins
BIN=./target/release
rm -rf config logs && mkdir -p logs
$BIN/rdag-node generate --nodes "$N" --base-port 9100 --dir config

pids=()
for i in $(seq 0 $((N - 1))); do
    $BIN/rdag-node run --key config/node-$i.json --committee config/committee.json \
        --parameters config/parameters.json --commit-log logs/commits-$i.txt \
        > logs/node-$i.log 2>&1 &
    pids+=($!)
done
trap 'kill "${pids[@]}" 2>/dev/null || true' EXIT
sleep 1
clients=()
for i in $(seq 0 $((N - 1))); do
    $BIN/rdag-client --node 127.0.0.1:$((9100 + i)) --rate "$RATE" --duration "$SECS" > logs/client-$i.log 2>&1 &
    clients+=($!)
done
wait "${clients[@]}"
sleep 3

echo "committed certificates per node:"
wc -l logs/commits-*.txt
# Every node's commit log must be a prefix of the longest one.
longest=$(wc -l logs/commits-*.txt | grep -v total | sort -n | tail -1 | awk '{print $2}')
for f in logs/commits-*.txt; do
    n=$(wc -l < "$f")
    if ! cmp -s <(cut -d' ' -f1-4 "$f") <(head -n "$n" "$longest" | cut -d' ' -f1-4); then
        echo "DIVERGENCE in $f"; exit 1
    fi
done
echo "all commit logs agree"
