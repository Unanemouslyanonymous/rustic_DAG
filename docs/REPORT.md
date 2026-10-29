# rustic_DAG: a Narwhal/Tusk style DAG consensus engine over WebSockets

## 1. Goal

Build a working Byzantine fault tolerant consensus engine in Rust that

1. separates data dissemination from ordering the way Narwhal does, by
   building a round-based DAG of certified blocks;
2. reads a total order from that DAG with the Tusk commit rule, which
   needs no messages beyond the DAG itself;
3. runs over a real peer to peer network: every validator is a WebSocket
   server and a client of every other validator;
4. comes with tests and measurements that show the safety properties hold
   and what performance to expect.

The committee is static: `n = 3f + 1` validators with equal stake (the code
supports arbitrary stake), of which up to `f` may be Byzantine.

## 2. Background

**Narwhal** (Danezis et al., EuroSys 2022) points out that most of the
cost in leader-based BFT is moving transaction data, and that this work can
be split from ordering. Validators advance in rounds. In round `r`, each
validator broadcasts a *header* that names some batches of transactions and
at least `2f+1` certificates from round `r-1`. Other validators sign
(vote for) the header if they hold its data and have not signed another
header from the same author for the same round. `2f+1` votes make a
*certificate*. Certificates form a DAG in which every vertex references a
quorum of the previous round.

**Tusk** is the ordering layer from the same paper. Each validator looks
at its local DAG and, at fixed rounds, picks a leader vertex using a shared
random coin. If enough vertices of the next round reference the leader, the
leader is committed together with its whole uncommitted causal history,
flattened deterministically. Because the DAG is the same everywhere (only
which parts have arrived differs), all honest validators output the same
sequence. Tusk adds no messages of its own.

## 3. Architecture

```
            clients (WebSocket, unauthenticated, tx submission only)
                 |
                 v
 +--------------------------------------------------------------+
 |  network.rs                                                  |
 |  WS server: challenge -> Hello(sig) | Client                 |
 |  one dialer per peer: reconnect + backoff, in-flight replay  |
 +-----------------------------+--------------------------------+
                               |  Inbound events (mpsc)
                               v
 +--------------------------------------------------------------+
 |  core.rs (single task, no locks)                             |
 |   batch maker -> payload queue -> proposer                   |
 |   voter (one vote per author/round)                          |
 |   vote aggregator -> certificates                            |
 |   synchroniser (suspend on missing parents/batches, fetch)   |
 |   local DAG store, garbage collection                        |
 |                 |                                            |
 |                 v                                            |
 |   consensus.rs (Tusk) -> ordered certificates + batches      |
 +-----------------------------+--------------------------------+
                               v
                   CommittedCertificate stream (application)
```

The whole protocol state machine lives in one struct (`Core`) driven by a
single tokio task. Network tasks only decode frames and pass them on. This
removes every lock from the critical path, makes the core fully
deterministic for a given input sequence, and makes it easy to unit test:
the tests call `Core::handle` directly and inspect the outbound queues.
The trade-off is that signature checks are serialised on one core; §8
comes back to this.

### 3.1 Data structures (`types.rs`)

| object | contents | identity |
|---|---|---|
| `Batch` | author, nonce, transactions | BLAKE3(bincode(batch)) |
| `Header` | author, round, payload (batch digests, ordered), parents (set of certificate digests) | `id` = BLAKE3(bincode(author, round, payload, parents)), signed by the author |
| `Vote` | header id, round, origin, voter, signature | Ed25519 over `DOMAIN_VOTE ‖ header id` |
| `Certificate` | header + list of (voter, signature) | the header id |

Round 0 holds one *genesis* certificate per authority. Every node derives
the same genesis set from the committee, so it is never sent.

### 3.2 Protocol flow (`core.rs`)

1. **Batching.** Client transactions are buffered and sealed into a batch
   once `batch_size` bytes have built up or the oldest one is
   `max_batch_delay_ms` old. The batch is broadcast at once and its digest
   goes into the payload queue.
2. **Proposing.** Let `r` be the highest round for which we hold
   certificates worth a quorum of stake. If we have not proposed for `r+1`
   yet, and either `header_size` batches are queued or
   `max_header_delay_ms` has passed, we create a header for `r+1` that
   references *all* certificates of round `r` we hold, broadcast it, and
   vote for it ourselves.
3. **Voting.** On a header: check it on its own (author in the committee,
   id recomputes, signature, no duplicate batches), then check that all
   parents and batches are local. If any are missing, the header is
   suspended and the missing items are requested from the sender and the
   author. Otherwise check the parents (all from round `r-1`, distinct
   authors, quorum stake) and vote, **at most once per (round, author)**.
   A second, different header for the same slot counts as equivocation and
   is refused. Seeing the same header again re-sends the same vote, which
   is harmless (Ed25519 signing is deterministic) and covers lost votes.
4. **Certifying.** The author collects votes for each of its in-flight
   headers. It keeps collecting for older rounds after moving on, so a
   slow certificate still forms. At quorum it broadcasts the certificate.
5. **Accepting certificates.** A certificate is checked (header checks,
   distinct known voters, quorum stake, batch signature check). It is
   inserted into the DAG only once all of its parents and batches are
   local, so the local DAG is always *causally closed*. Tusk depends on
   this.
6. **Ordering.** Every inserted certificate is passed to Tusk, which may
   return a run of newly committed certificates. The core attaches their
   batches and sends them to the application.

### 3.3 Tusk commit rule (`consensus.rs`)

This follows the structure of the reference Narwhal implementation:

* Even rounds `≥ 2` have a leader, `committee.leader(round)`.
* When a certificate of round `R` arrives, set `r = R-1`. If `r` is even
  and `r ≥ 4`, the candidate is the leader of round `r-2`. At that point a
  quorum of round `r` is local (the parents of the new certificate), so
  the decision is never made too early.
* The candidate is committed if certificates of round `r-1` holding at
  least `f+1` stake reference it.
* Before committing it, walk back two rounds at a time to the last
  committed leader. Each earlier leader that the current one has a DAG
  path to joins the chain, and the walk continues from it. Commit the
  chain oldest first.
* Committing a leader means a DFS over its causal history that skips
  already-committed certificates and anything at or below the GC horizon,
  then a sort by `(round, author)`. Every node therefore lays out the same
  sub-DAG in the same order.

One deliberate change from the reference code: the reference marks an
authority's history as committed up to the highest of its rounds that has
been committed. That can silently drop a certificate whose author had a
later round committed through a different path. We track committed
*digests* instead (pruned by GC), so any certificate in a leader's history
is output exactly once.

### 3.4 Networking (`network.rs`)

* Each node listens on its committee address. For every peer it runs a
  *dialer* task that owns the outbound link to that peer. Each pair of
  nodes is therefore joined by two one-way WebSocket connections. This
  avoids the who-dials-whom tie-breaking that a single shared connection
  needs.
* **Handshake.** The server opens with `Challenge{server, nonce}` (32
  random bytes). A validator answers `Hello{from, to, sig}` where `sig` is
  Ed25519 over `DOMAIN_HELLO ‖ from ‖ to ‖ nonce`. The server checks that
  `from` is a committee member other than itself, that `to` is itself,
  and that the signature is valid. A captured Hello cannot be replayed:
  the nonce is fresh per connection and the server key is bound into the
  signature. The dialer also checks the challenge names the peer it meant
  to reach. Clients answer `Client` and can only submit transactions.
* **Reliability.** Dialers reconnect with exponential backoff (50 ms up
  to 2 s). The message whose send failed is kept and sent first after
  reconnecting. Sends from the core never block. If a peer's queue is
  full the message is dropped, and the synchroniser's periodic retries
  (re-request missing items, re-broadcast our own uncertified headers)
  recover it.
* **Framing.** One bincode-encoded message per binary frame. Decoding
  uses a 64 MiB limit, so a hostile length prefix cannot trigger a huge
  allocation (unit test `hostile_length_prefix_rejected`). A peer that
  sends an undecodable frame is disconnected.

### 3.5 Synchronisation and garbage collection

Suspended headers and certificates are kept in maps. Whenever something
new is stored (a batch or a certificate), the core re-examines the
suspended items until nothing more can be processed. Every
`sync_retry_ms` the core broadcasts a request for everything still
missing. Because requests are answered from any node's store, a lagging
node can pull a whole chain of certificates back to the point where it
diverged.

After every commit the GC horizon becomes `last_committed_round -
gc_depth`. Everything at or below it is dropped: certificates, vote
records, suspended items, and batches tagged with a round at or below it.
If our own header expires uncertified, its batches go back into the
payload queue so no client transaction is lost. Our own batches that are
not yet certified are exempt from GC.

## 4. Cryptography

| primitive | use | notes |
|---|---|---|
| Ed25519 (`ed25519-dalek` 2) | header signatures, votes, peer handshake | `verify_strict` for single signatures (rejects non-canonical encodings and small-order keys) |
| Ed25519 batch verification | the `2f+1` votes in each certificate | much faster than one by one; weak keys are screened out first, since the batch equation alone does not reject them |
| BLAKE3 | batch, header and certificate digests; coin | 256-bit output |
| Domain separation | `rustic-dag/{header,vote,hello}/v1` + `0x00` + message | a header signature can never count as a vote (test `header_signature_is_not_a_vote`), and a handshake can never be replayed as either |

Votes sign the header id, which is a hash of every header field except the
signature, so a vote cannot be moved to a different header (shown
end-to-end in `tests/byzantine.rs`). Certificates are identified by their
header id, so two certificates with different vote subsets for the same
header are the same DAG vertex.

**Coin.** Tusk needs a common coin that the adversary cannot predict
(threshold signatures in the paper). `Committee::leader` instead picks the
leader by stake-weighted lot from `BLAKE3("rustic-dag/coin/v1" ‖ round)`.
Every node computes it locally, and with equal stakes it spreads leaders
uniformly (unit test `leader_is_deterministic_and_spread`). This does not
affect safety, but it does weaken liveness: an adversary that controls
message scheduling and knows future leaders can delay exactly the leaders'
certificates. The reference Narwhal code makes the same simplification
(round-robin leaders). A real threshold coin, for example BLS threshold
signatures on the round number carried in headers, would plug in at
`Committee::leader` and the round-`r` header.

## 5. Security argument

Assumptions: at most `f` of `3f+1` validators are Byzantine, signatures
are unforgeable, hashes are collision resistant, and the network
eventually delivers messages between honest nodes.

* **Integrity.** Every header, vote and certificate is verified before
  use. Batches are only accepted from authenticated committee peers and
  are bound to headers by digest.
* **Non-equivocation (one certificate per slot).** A certificate needs
  `2f+1` votes. Two certificates for different headers in the same
  `(author, round)` would need two quorums, which share at least `f+1`
  voters, of whom at least one is honest. An honest node votes once per
  slot (`last_voted`). So two certificates for one slot cannot both
  exist. The core also refuses to insert a second certificate for an
  occupied slot.
* **Availability.** A certificate proves that `2f+1` validators, `f+1`
  of them honest, stored the payload before voting. Any node can
  therefore fetch it.
* **Causal closure.** Certificates enter the local DAG only after their
  parents and batches do, so a certificate means the same causal history
  on every honest node.
* **Total order safety.** If an honest node commits leader `L` at round
  `r` (`≥ f+1` support at `r+1`), then any round `r+2` certificate
  references `2f+1` round `r+1` certificates. These must overlap the
  `f+1` supporters, so every round `r+2` certificate, and every later
  leader, has a path to `L`. Any honest node that commits a later leader
  first walks back and finds `L` through `order_leaders`. Together with
  deterministic flattening, this makes every node's output a prefix of
  every other's. The tests check this property directly (§7).
* **Liveness** holds under eventual delivery with the caveat about the
  predictable coin in §4. Proposers never wait on a particular leader,
  so the DAG keeps growing even when leaders are slow or missing (shown
  in `progress_with_one_silent_node` and the fault experiments).

**Denial of service measures:** bounded decode size; sync requests capped
at 1000 digests; non-blocking outbound queues (a slow peer cannot stall
the core); only committee members can open peer links; malformed frames
end the connection. Not addressed: rate limiting of clients or peers
(an authenticated Byzantine peer can still send valid but useless batches)
and memory caps on suspended items (they are bounded by GC, not by size).

## 6. Implementation notes

* About 3,700 lines of Rust including tests; there is no `unsafe` and
  the protocol code takes no locks.
* Determinism: committee iteration uses `BTreeMap`, header parents are a
  `BTreeSet`, and the commit flattening sorts by `(round, author)`, so
  output does not depend on hash map iteration order.
* The in-process `LocalCluster` uses real TCP sockets and the same code
  path as separate processes. `scripts/local_testnet.sh` runs separate
  processes and compares their commit logs line by line.

## 7. Testing

`cargo test` runs 28 unit tests and 5 networked integration tests. All of
them pass.

| area | tests |
|---|---|
| crypto | sign/verify, domain separation, batch verification catches one bad signature, serde round trips |
| objects | tampered or forged headers, headers from outsiders, quorum / duplicate voter / forged vote checks on certificates, header signature not usable as a vote, stable genesis |
| wire | handshake bound to nonce and server, hostile length prefix |
| core | one vote per slot (equivocation counted, re-delivery re-sends the same vote), header waits for unknown parents and missing batch and then votes, rejects certificates without quorum and headers with too few parents, forged votes ignored, transactions go all the way to a certificate |
| Tusk | fully connected DAG commits the right leader and history; every certificate committed once; progress with a silent node; **same sequence for 10 random causal delivery orders**; leader with no support committed later through a link; unreachable leader skipped for good; GC bounds memory |
| end-to-end | 4 nodes agree, every transaction committed exactly once; 1 of 4 down from the start; crash of 1 of 4 mid-run (crashed node's log is still a prefix); 7 nodes with 2 down; Byzantine validator equivocating to different nodes |

The Byzantine test runs a hand-driven validator on the real network that
sends header A to two honest nodes and a conflicting header B to the third,
then swaps. Every honest node votes for exactly one of the two. The attacker
can certify one of them but not both, votes for one cannot be attached to
the other, and the honest nodes go on committing identical sequences.

## 8. Evaluation

### 8.1 Setup

All runs use one machine with **4 vCPUs** and 15 GB RAM. The whole
committee *and* the load generators share those 4 cores in one process and
talk over localhost WebSockets. The numbers therefore measure the
software's overhead with zero network latency. They say nothing about
WAN performance and understate what each node could do on its own machine.
Transactions are 512 bytes. Clients are open-loop (fixed rate, bursts
every 10 ms) and send to every live node equally. Latency runs from
client submission to commit at node 0. Each run has 3 s of warm-up, a 20 s
measurement window and a 2 s drain. Unless stated otherwise:
`batch_size` = 500 KB, `max_batch_delay` = 100 ms, `max_header_delay` =
200 ms. Every run also checks that each node's commit sequence is a prefix
of the others' (`consistent`).

Reproduce with `scripts/run_eval.sh`. Raw data is in
`results/bench.jsonl` and the table below is `results/summary.md`.

### 8.2 Results

RESULTS_TABLE

### 8.3 Discussion

DISCUSSION

## 9. Limitations and future work

* **No persistence or crash recovery.** State is in memory. A restarted
  node starts from genesis and can only catch up if the others have not
  garbage collected past that point. Adding a store (RocksDB/sled) for
  certificates, batches and `last_voted` is required for production;
  persisting `last_voted` is what stops a restarted honest node from
  accidentally equivocating.
* **Predictable coin** (§4). Replace it with a threshold BLS coin.
* **No transport encryption.** Messages are authenticated at the
  application layer, but batches can be read in transit. Running the
  mesh over `wss://` (rustls) would fix that without protocol changes.
* **Single worker per primary.** Narwhal scales throughput by giving each
  validator many worker machines. Here batching runs inside the primary.
  The interfaces (batches referenced by digest, separate batch requests)
  already have the right shape to split out.
* **Single-threaded verification.** Certificate and vote verification
  run on the core task. At saturation that is the bottleneck. Moving
  verification to a pool before messages reach the core is the obvious
  next step.
* **Overload behaviour.** There is no admission control. Past
  saturation, clients keep pushing and latency grows without bound.
  Client backpressure (bounded per-connection buffers, rejecting
  transactions when the payload queue is long) would make degradation
  graceful.
* Tusk's commit latency could be cut by moving to Bullshark-style
  partially synchronous leaders, which commit in 2 rounds instead of 3+.

## References

1. G. Danezis, L. Kokoris-Kogias, A. Sonnino, A. Spiegelman. *Narwhal and
   Tusk: A DAG-based Mempool and Efficient BFT Consensus.* EuroSys 2022.
2. A. Spiegelman, N. Giridharan, A. Sonnino, L. Kokoris-Kogias.
   *Bullshark: DAG BFT Protocols Made Practical.* CCS 2022.
3. I. Keidar, E. Kokoris-Kogias, O. Naor, A. Spiegelman. *All You Need is
   DAG.* PODC 2021.
4. D. J. Bernstein et al. *High-speed high-security signatures* (Ed25519).
   2012. H. de Valence, *It's 255:19AM* (on Ed25519 validation criteria).
5. J. O'Connor et al. *BLAKE3: one function, fast everywhere.* 2020.
