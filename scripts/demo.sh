#!/usr/bin/env bash
# End-to-end demonstration for reviewers.
#
#   1. builds everything and runs the full test suite
#   2. starts a 4 validator committee as 4 separate OS processes talking
#      over WebSockets, with a load client per node
#   3. kills one validator half way through (crash fault, f = 1)
#   4. checks that the three survivors kept committing and that every
#      node's commit log, the crashed one included, is a prefix of the others
#   5. runs the Byzantine scenario (a validator sending conflicting headers)
#
# Usage: scripts/demo.sh [seconds-per-phase] [tx-per-second-per-node]
# Everything it produces goes to demo-output/.
set -euo pipefail
cd "$(dirname "$0")/.."

PHASE=${1:-10}
RATE=${2:-5000}
N=4
PORT=9300
OUT=demo-output
BIN=./target/release

bold() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok() { printf '   \033[32mOK\033[0m %s\n' "$*"; }
fail() { printf '   \033[31mFAIL\033[0m %s\n' "$*"; exit 1; }

bold "1/5 build and test"
cargo build --release --bins 2>&1 | tail -1
cargo test --release 2>&1 | grep -E "^test result" | sed 's/^/   /'
cargo test --release 2>&1 | grep -q "FAILED" && fail "test failures" || ok "all tests pass"

bold "2/5 start a committee of $N validators (separate processes)"
rm -rf "$OUT" && mkdir -p "$OUT"
$BIN/rdag-node generate --nodes $N --base-port $PORT --dir "$OUT/config" | sed 's/^/   /'
pids=()
for i in $(seq 0 $((N - 1))); do
    RUST_LOG=info $BIN/rdag-node run --key "$OUT/config/node-$i.json" \
        --committee "$OUT/config/committee.json" --parameters "$OUT/config/parameters.json" \
        --commit-log "$OUT/commits-$i.txt" > "$OUT/node-$i.log" 2>&1 &
    pids+=($!)
    echo "   node $i  pid ${pids[$i]}  ws://127.0.0.1:$((PORT + i))  key $(grep -o '"name": "[0-9a-f]*' "$OUT/config/node-$i.json" | cut -c10-17)"
done
cleanup() { kill "${pids[@]}" "${clients[@]:-}" 2>/dev/null || true; }
trap cleanup EXIT
sleep 1

clients=()
for i in $(seq 0 $((N - 1))); do
    $BIN/rdag-client --node 127.0.0.1:$((PORT + i)) --rate "$RATE" --size 512 \
        --duration $((PHASE * 2)) > "$OUT/client-$i.log" 2>&1 &
    clients+=($!)
done
echo "   $N clients sending $RATE tx/s each (512 B) for $((PHASE * 2)) s"

count() { [ -f "$1" ] && wc -l < "$1" || echo 0; }
txs() { [ -f "$1" ] && awk '{s += $5} END {print s + 0}' "$1" || echo 0; }

sleep "$PHASE"
bold "3/5 after ${PHASE}s: crash node 3 (kill -9)"
for i in $(seq 0 $((N - 1))); do
    echo "   node $i: $(count "$OUT/commits-$i.txt") certificates, $(txs "$OUT/commits-$i.txt") transactions committed"
done
kill -9 "${pids[3]}"
wait "${pids[3]}" 2>/dev/null || true
crashed_at=$(count "$OUT/commits-0.txt")
ok "node 3 killed"

sleep "$PHASE"
wait "${clients[@]}" 2>/dev/null || true
sleep 3
bold "4/5 after another ${PHASE}s with only 3 of 4 validators"
for i in $(seq 0 $((N - 1))); do
    echo "   node $i: $(count "$OUT/commits-$i.txt") certificates, $(txs "$OUT/commits-$i.txt") transactions committed"
done
now=$(count "$OUT/commits-0.txt")
[ "$now" -gt "$crashed_at" ] && ok "survivors kept committing ($crashed_at -> $now certificates)" \
    || fail "no progress after the crash"

# Agreement: compare (index, round, author, digest) line by line.
longest=$(for i in 0 1 2 3; do echo "$(count "$OUT/commits-$i.txt") $OUT/commits-$i.txt"; done | sort -n | tail -1 | cut -d' ' -f2)
for i in $(seq 0 $((N - 1))); do
    f="$OUT/commits-$i.txt"
    n=$(count "$f")
    if cmp -s <(cut -d' ' -f1-4 "$f") <(head -n "$n" "$longest" | cut -d' ' -f1-4); then
        ok "node $i log ($n entries) is a prefix of the longest log"
    else
        fail "node $i diverges from $longest"
    fi
done
echo
echo "   first commits, side by side (index round author digest txs):"
paste -d'|' <(head -5 "$OUT/commits-0.txt") <(head -5 "$OUT/commits-1.txt") | sed 's/|/   |   /; s/^/   /'
echo "   ..."
echo
echo "   final counters of node 0:"
grep "CoreStats" "$OUT/node-0.log" | tail -1 | sed 's/.*CoreStats { /   /; s/ }$//; s/, /\n   /g'

cleanup
bold "5/5 Byzantine validator: conflicting headers for the same round"
cargo test --release --test byzantine -- --nocapture 2>&1 | grep -E "equivocated at|^byzantine:|test equivocating|test result" | sed -E 's/^.*(WARN)/\1/' | sed 's/^/   /'

bold "done"
echo "   logs, commit logs and configs are in $OUT/"
