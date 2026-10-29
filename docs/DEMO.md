# Demonstration

Everything below runs on one machine with a stable Rust toolchain (1.85
or newer). Nothing else is needed. There is also a Dockerfile if you prefer
not to install Rust.

## One command

```sh
scripts/demo.sh            # ~1 minute after the build, 10 s per phase
scripts/demo.sh 20 10000   # longer phases, heavier load
```

The script

1. builds the binaries and runs the whole test suite (unit, end-to-end
   and Byzantine tests);
2. generates keys and a committee for 4 validators and starts them as **4
   separate processes**, each listening on its own WebSocket port, plus
   one load client per validator;
3. after the first phase, **kills validator 3 with `kill -9`** (one crash
   fault, which is the most `n = 4` tolerates);
4. after the second phase, checks that
   * the three survivors kept committing after the crash, and
   * every node's commit log is a prefix of the longest one, the dead
     node included. That is the safety property: nobody ever commits
     something different at the same position;
5. runs the Byzantine scenario: a validator sends header A to some nodes
   and a conflicting header B for the same round to others. Honest nodes
   vote for only one, so at most one can be certified, and they keep
   agreeing.

All output (configs, per-node logs, commit logs) lands in `demo-output/`.
Each line in `demo-output/commits-<i>.txt` is

```
<position> <round> <author> <certificate digest> <transactions in it>
```

so agreement can be checked by hand too:

```sh
diff <(cut -d' ' -f1-4 demo-output/commits-0.txt) \
     <(cut -d' ' -f1-4 demo-output/commits-1.txt | head -n $(wc -l < demo-output/commits-0.txt))
```

## Expected output (abridged)

From an actual run on a 4 vCPU machine (`scripts/demo.sh 8 5000`). A full
transcript is in [`results/demo_run.txt`](../results/demo_run.txt).

```
== 3/5 after 8s: crash node 3 (kill -9)
   node 0: 273 certificates, 150800 transactions committed
   ...
   OK node 3 killed

== 4/5 after another 8s with only 3 of 4 validators
   node 0: 520 certificates, 279600 transactions committed
   node 1: 520 certificates, 279600 transactions committed
   node 2: 520 certificates, 279600 transactions committed
   node 3: 273 certificates, 150800 transactions committed
   OK survivors kept committing (273 -> 520 certificates)
   OK node 0 log (520 entries) is a prefix of the longest log
   ...
   OK node 3 log (273 entries) is a prefix of the longest log

== 5/5 Byzantine validator: conflicting headers for the same round
   WARN rustic_dag::core: authority 01f3369f equivocated at round 1: 71922cea3bb6 vs f8d0e85e75bd
   byzantine: sent header A 71922cea3bb6 and conflicting header B f8d0e85e75bd for round 1
   byzantine: honest votes received: A=2 B=1 (quorum is 3)
   byzantine: certificate for f8d0e85e75bd carrying the votes for 71922cea3bb6 rejected: invalid signature
   byzantine: honest nodes keep committing and agree on all 196 certificates
   test equivocating_author_cannot_get_two_certificates ... ok
```

Exact numbers, keys and digests depend on the machine and the run.

## Doing it by hand

```sh
cargo build --release
./target/release/rdag-node generate --nodes 4 --base-port 9000 --dir config
for i in 0 1 2 3; do
  ./target/release/rdag-node run --key config/node-$i.json \
      --committee config/committee.json --commit-log commits-$i.txt &
done
./target/release/rdag-client --node 127.0.0.1:9000 --rate 5000 --duration 30
```

Kill any one node at any time and watch the others keep logging
`commit #...` lines. Kill two and progress stops (2 of 4 is not a quorum).
Restart one of them and progress resumes, though the restarted node itself
does not catch up from scratch (see the limitations in the report).

## Benchmarks

```sh
cargo run --release --bin rdag-bench -- --nodes 4 --rate 100000 --duration 20
scripts/run_eval.sh    # the full matrix reported in docs/REPORT.md
```

## Docker

```sh
docker build -t rustic-dag .
docker run --rm rustic-dag              # runs scripts/demo.sh
```
