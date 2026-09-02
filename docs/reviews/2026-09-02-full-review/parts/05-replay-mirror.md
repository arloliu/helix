# 05 — Replay and Mirror Subsystems: Data Consistency and Durability Review

Fresh, independent review from source (commit `7e93857`, 2026-09-02). Prior review
documents in `tmp/` were deliberately not consulted. Every claim is marked
**CONFIRMED** (read in code and/or reproduced with a throwaway test run via
`go test -overlay`, no repo files touched) or **SUSPECTED** (inferred, not executed).

---

## (a) Subsystem summary and data flow

Helix dual-writes every `Query.Exec` / `Batch.Exec`. When exactly one leg fails, the
caller gets `nil` and the failed leg is turned into a `types.ReplayPayload` and handed to
the configured `Replayer`. A `Worker` later re-executes the payload against the target
cluster with the *original client timestamp* so that Cassandra LWW makes the replay
idempotent for plain column writes.

```
 caller  Query(...).Exec()
   |
   |  ts = q.timestamp ?? TimestampProvider()          (cql_client.go:3085-3090, 3110)
   v
 executeWriteWithReplay (cql_client.go:1745)
   |-- drainA||drainB --> executeWriteWithDrain (1801): write healthy leg, enqueue draining leg
   `-- else            --> executeDualWrite   (1890): write A & B concurrently
                               |
                               | classify: nil | ErrWriteAsync | ErrWriteDropped | real error
                               | both real errors -> DualClusterError, NO replay (2000-2003)
                               | any other non-nil leg -> enqueueReplayIfNeeded (2026)
                               v
                     Replayer.Enqueue(context.WithoutCancel(ctx), payload)
                       |                        |
        +--------------+                        +------------------------+
        v                                                                v
 MemoryReplayer (replay/memory.go)                          NATSReplayer (replay/nats.go)
  two chan[cap] high/low, shared cap 10k                     msgp encode args -> js.Publish (sync)
  ErrReplayQueueFull when full                               subject {prefix}.{high|low}.{A|B}
  lost on Close()/restart                                    stream: WorkQueue, File, MaxAge 24h,
        |                                                    MaxMsgs 1M, MaxBytes 1GB, DiscardOld
        v                                                                v
 memoryBackend (memory_worker.go)                           natsBackend (nats_worker.go)
  1 goroutine, attempt 1 inline,                             2 goroutines (A,B), durable pull
  attempts 2..5 in retry pool (cap 100)                      consumers helix-worker-{high,low}-{A,B}
  backoff 100ms*2^n cap 30s                                  Ack on success / Nak (no delay) on
  drop -> OnDrop + IncReplayDropped                          error / Term at MaxDeliver=5
        |                                                                |
        +------------------------------+---------------------------------+
                                       v
                       ExecuteFunc = client.DefaultExecuteFunc() (cql_client.go:1436)
                       session := c.getSession(payload.TargetCluster)   <- resolves CURRENT session
                       session.Query(q, args...).WithTimestamp(ts).ExecContext(ctx)
                       (no drain check, no consistency, no closed check)
```

Mirror (opt-in `Query.Mirror()` / `Batch.Mirror()`):

```
 Exec returns err == nil  (includes partial success where one primary leg was replayed)
   -> dispatchMirrorQuery/Batch (mirror_dispatch.go:325,343): TargetCluster = ClusterA (constant)
   -> mirror.Engine.TryEnqueue: non-blocking chan[8192], drop on full/disabled/stopped
   -> 4 workers: execute(payload)
        target mode   : target.Query(...).WithTimestamp(ts).Exec  (full dual-write on the mirror pair)
                        on error -> WithMirrorReplayer -> Replayer.Enqueue -> auto worker
        publisher mode: execute == MirrorPublisher.Enqueue (NATS publish); consumer binary
                        runs helix.NewMirrorWorker(natsReplayer, mirrorTarget)
```

---

## (b) Invariants

| # | Invariant | Held? | Evidence |
|---|-----------|-------|----------|
| I1 | Every write that reaches the dual-write path carries a client-side timestamp, and the replay reuses exactly that timestamp | **Yes** | `getTimestamp()` always returns provider value when unset (cql_client.go:3085, 3518); `query.WithTimestamp(ts)` applied on both legs (3145, 3166); payload.Timestamp = wc.timestamp (2044); `DefaultExecuteFunc` applies `WithTimestamp(payload.Timestamp)` (1446, 1450). |
| I2 | A partially failed write is either replayed or the caller is told | **Conditional** | Enqueue failure returns `nil` to the caller; only metric `replay_dropped_total`, log, `OnReplayDropped`, `EventReplayDropped` (2066-2073). No replayer configured -> warning at construction only (871). |
| I3 | A replayed statement is semantically identical to the original | **No** | Not captured: consistency, serial consistency (`ReplayPayload` has no field, types/types.go:176-201); replay runs at the worker session's default consistency. Args are lossy for some Go types (F5, F9). `USING TTL n` re-starts the TTL clock at replay time. |
| I4 | Replay never applies a non-idempotent statement twice | **No** | Counter updates, list append/prepend, `CounterBatch` are enqueued like any other write (no detection). `ErrWriteAsync` enqueues a replay *while the background write is still in flight* (2007-2016) -> guaranteed double apply for counters even without a timeout. Documented in docs/strict-write.md and replay-system.md §Best Practices, not enforced. |
| I5 | An admitted replay message survives until the target cluster accepts it (bounded only by explicit stream limits) | **No** | NATS: `Nak()` with no delay + `MaxDeliver=5` -> a message is `Term`'d after ~5 s of cluster outage (F1, reproduced). Memory: `MaxAttempts=5` with backoff 100/200/400/800 ms -> dropped after 1.5 s of outage (F2, reproduced). |
| I6 | Replay respects drain mode | **No** | `DefaultExecuteFunc` calls `c.getSession()` directly; no `drainA/drainB` check (1436-1452). The drain path itself enqueues to the draining cluster (1856-1869), so the worker immediately hammers the drained cluster and burns its delivery budget (F3). |
| I7 | Stream/queue evictions are observable | **No** | JetStream `MaxAge` expiry and `DiscardOld` evictions emit nothing in Helix (no metric, event, or callback). `SetReplayQueueDepth` is never called by production code (F6). |
| I8 | Replay payload is immutable after enqueue | **Conditional** | NATS encodes at enqueue time (safe). MemoryReplayer stores `wc.args` = `q.values` by reference, no clone (2040, 1861) — caller buffer reuse after `Exec` corrupts the queued replay (F8). Mirror path clones `[]byte` only (mirror_dispatch.go:293). |
| I9 | In-flight replay is not lost or double-applied on graceful shutdown | **Conditional** | NATS: message mid-execute finishes then Acks; remaining batch is Nak'd (nats_worker.go:213-225). Memory: everything still queued or in backoff is dropped via `OnDrop("shutdown")` — lost unless the app persists in `OnDrop` (memory_worker.go:70-78, 138-142). Documented. |
| I10 | Replay targets the cluster the write was meant for | **Conditional** | `getSession(TargetCluster)` resolves the *current* holder, so after `SwapSession(B, newB)` pending B replays go to `newB`. Correct if `newB` is the same logical cluster; if the slot is repointed to a different cluster (migration by swap) the backlog lands on the new cluster. Any non-`A` `TargetCluster` string (e.g. a corrupted/foreign message) maps to B (1467-1470). |
| I11 | Mirror captures are bounded and drops are counted | **Yes** | `TryEnqueue` non-blocking, `dropped` counter + `IncMirrorEnqueueDropped` + rate-limited log + `OnDrop` (mirror/engine.go:279-307). |
| I12 | Mirror ordering preserved | **No (by design)** | 4 workers pulling from one channel; publisher mode adds cross-instance interleaving. Safe under LWW because timestamp is preserved; unsafe for counters/list ops (documented in docs/mirror.md:148). |
| I13 | An operator can quantify the data gap per cluster | **No** | Only counters (`enqueued`, `success`, `errors`, `dropped`). `replay_queue_depth` is permanently 0; no oldest-message age; `NATSReplayer.Pending()` is stream-wide, not per cluster/priority (F6). |

---

## (c) Findings, ranked by severity

### HIGH

#### F1 — NATS worker: immediate `Nak` + `MaxDeliver=5` turns a short outage into permanent data loss — **CONFIRMED (reproduced)**

- `replay/nats_worker.go:261` `b.nakMessage(msg, "retry")` -> `msg.Nak()` (nats.go:929). No `NakWithDelay`, no consumer `BackOff`, and `calculateBackoff` / `RetryDelay` / `MaxRetryDelay` are never referenced by `natsBackend`.
- `replay/nats.go:622-631` consumer config: `MaxDeliver: 5`, no `BackOff`.
- `replay/nats_worker.go:236-258`: at `DeliveryCount >= MaxDeliver` the message is `Term`'d (removed from the stream for good) and counted as `replay_dropped`.
- Reproduction (embedded nats-server, execute always fails, `WithRetryDelay(2s)` explicitly set): **dropped after 4.5 s with 5 attempts**; the configured 2 s delay had no effect. The ~1 s spacing is the Fetch window, not a backoff.
- docs/replay-system.md:572 claims backoff is "Server-side, controlled by `AckWait`" — false: `AckWait` only applies to *un-acked* messages, and the worker Naks explicitly.

**Scenario:** cluster B goes down for 10 minutes. Every write during that window succeeds on A and is enqueued for B (correct). The NATS worker fetches each message, gets "no hosts available" in milliseconds, Naks it, gets it back immediately, and `Term`s it after the 5th failure — a few seconds after enqueue. When B returns, the stream is empty. Every write from the outage is missing on B, and the only trace is `replay_dropped_total` and `OnDrop`. This defeats the purpose of the durable backend.

**Direction:** Nak with delay (`msg.NakWithDelay(calculateBackoff(deliveryCount, RetryDelay, MaxRetryDelay))`) and/or set `ConsumerConfig.BackOff`; make `MaxDeliver` default much larger (or `-1`) with a *time-based* budget; distinguish "target unreachable" (never terminate, keep redelivering) from "statement rejected" (terminate as poison). At minimum fix the doc.

#### F2 — Memory worker: default retry budget is 1.5 s of wall time; anything longer is dropped — **CONFIRMED (reproduced)**

- `replay/worker.go:113-116` defaults `RetryDelay=100ms`, `MaxAttempts=5`; `calculateBackoff` in `worker.go:417-430` (no jitter despite `worker.go:31` doc). Waits: 100+200+400+800 ms = 1.5 s.
- Reproduction: execute fails for 3 s -> **dropped after 1.50 s, 5 attempts**; the write never reaches the target although it was reachable 1.5 s later.
- `memory_worker.go:118-124`: once 100 retries are in flight, further failures drop *immediately* ("retry pool saturated") — under an outage this is reached in well under a second at modest write rates, so effectively **all** writes during a memory-backed outage are dropped after the first 100.
- `WithAutoMemoryWorker` (config.go:751) wires this configuration for users who want "simple in-process replay"; the option's doc does not mention the 1.5 s budget.

**Scenario:** ScyllaDB rolling restart takes a node set out for 30 s; every partial write in that window is lost on the restarting cluster. Nothing prompts the operator except `replay_dropped_total`.

**Direction:** derive `MaxAttempts` from a wall-clock budget (e.g. default `MaxRetryDelay=30s`, retry until N minutes elapsed), re-enqueue to the tail instead of dropping when the pool is saturated, and document that MemoryReplayer is not an outage-survival mechanism.

#### F3 — Drain mode enqueues replays that the worker immediately executes against the draining cluster — **CONFIRMED (code)**

- `cql_client.go:1856-1869` (`executeWriteWithDrain`): writes are enqueued with `TargetCluster = drainingCluster`.
- `cql_client.go:1436-1452` (`DefaultExecuteFunc`): no drain check; it dereferences the session and executes.
- The replay package has no knowledge of topology; the worker keeps running while a cluster is drained (nothing pauses per-cluster consumption).

**Scenario:** operator drains B for a 20-minute maintenance. Writes keep flowing; each partial write goes to the queue and is replayed *right now* against B. If B is actually unreachable the F1/F2 budgets are exhausted within seconds and the backlog is terminated — the drain window becomes a data-loss window. If B is reachable but the drain was meant to keep writes off it (e.g. schema migration, repair), replay violates the drain contract.

**Direction:** worker-side per-cluster pause (`Worker.PauseCluster(id)` driven by drain transitions, or `DefaultExecuteFunc` returning a sentinel `ErrClusterDraining` that the backends treat as "Nak with long delay, do not count toward MaxDeliver").

#### F4 — Silent stream-level loss: `MaxAge=24h` expiry and `DiscardOld` evictions are invisible — **CONFIRMED (code)**

- `replay/nats.go:355-365` stream config: `MaxAge 24h`, `MaxMsgs 1M`, `MaxBytes 1GB`, `Discard: DiscardOld`, `Retention: WorkQueuePolicy`.
- Nothing in Helix observes expiry/eviction: no metric, no event, no callback. `Enqueue` keeps returning `nil` while the oldest messages are being evicted.
- Default `Replicas: 1` on `FileStorage` — a single-node JetStream disk loss removes the entire backlog; documented as "use 3 for production" but the default is the unsafe value.

**Scenario:** cluster B down for 26 h, or a 2 M-write backlog. The first 1 M (or the first 2 h) of writes are gone before the worker ever sees them. `replay_enqueued_total - replay_success_total - replay_dropped_total` stays large forever with no explanation.

**Direction:** default to `DiscardNew` (fail loud), or at least emit `EventReplayEvicted` by polling `stream.Info()` (`FirstSeq` movement / `State.Msgs` drop without consumer acks); require explicit `MaxAge`; default `Replicas` to 3 or warn.

### MEDIUM

#### F5 — Args encoding is lossy or rejects common CQL Go types (NATS path) — **CONFIRMED (reproduced)**

Observed round-trip through `encodeArgs`/`decodeArgs` (replay/nats.go:745-895):

| Go input | Result | Consequence |
|---|---|---|
| `*big.Int` (varint) | **encode error** | Enqueue fails -> `replay_dropped`, write lost on the failed cluster |
| `*inf.Dec` (decimal) | **encode error** | same |
| struct / `*struct` (UDT) | **encode error** | same |
| `map[int]string` | **encode error** | same |
| `net.IP` (inet) | `[]any{uint64...}` | Enqueue succeeds; every replay attempt fails in the driver -> poison, dropped after MaxDeliver |
| `[]byte{}` (empty blob) | decoded as **`nil`** | Replay writes NULL (tombstone) where the original wrote an empty value |
| `int`/`int8`/`int16` | `int64` | fine for gocql marshalling |
| `time.Duration` | `int64` | gocql accepts int64 ns for `duration`; ok |
| `time.Time` with zone | UTC instant preserved | fine |
| `[16]byte` / gocql.UUID | `[]byte(16)` | fine |
| `[]string`, `[]int`, `map[string]int` | `[]any` / `map[string]any` | fine via gocql reflection |

Note: `gocql.UUID`'s `MarshalBinary` isn't relied on; reflection path handles it. Google UUID goes through `encoding.BinaryMarshaler`.

**Direction:** explicit encoders for `big.Int`, `inf.Dec`, `net.IP`, `gocql.Duration`, and a documented "unsupported types" list that is checked at **Enqueue time on both backends** (MemoryReplayer currently accepts anything and only NATS surfaces the problem). Encode empty `[]byte` distinctly from nil (msgp `bin` of length 0 already exists on the wire — the decoder returns nil; wrap with a length check).

#### F6 — Operators cannot measure the per-cluster data gap; `replay_queue_depth` is a dead gauge — **CONFIRMED (code)**

- `types/metrics.go:117-119` declares `SetReplayQueueDepth`; grep shows **no production caller** (only `internal/metrics/nop.go`, `test/testutil`, and the vm collector's setter). `contrib/metrics/vm/doc.go:77` and docs advertise the gauge; it always reads 0.
- No oldest-message age, no per-cluster/priority pending count (`NATSReplayer.Pending()` is stream-wide, nats.go:975), no in-flight-retry gauge for the memory backend (`Len()` explicitly excludes in-flight work, memory.go:377).
- `replay_dropped_total` conflates enqueue failures and worker exhaustion (vm/doc.go:73-76).

**Direction:** have workers call `SetReplayQueueDepth` (memory: `Len()`; NATS: `consumer.Info().NumPending` per cluster/priority on each poll); add `replay_oldest_age_seconds{cluster}`; split dropped reason label.

#### F7 — `ErrWriteAsync` enqueues a replay while the background write is in flight — double apply for non-idempotent statements — **CONFIRMED (code)**

- `cql_client.go:2007-2016`: `ErrWriteAsync` legs are enqueued as a "safety net". `policy/adaptive_write.go:728` returns `ErrWriteAsync` after launching the real write in a goroutine.
- For plain writes this is idempotent by timestamp. For `UPDATE ... SET c = c + 1`, `list = list + [x]`, `CounterBatch`, the background write and the replay both apply. This occurs on *every* write to a degraded cluster under `AdaptiveDualWrite`, not only on timeouts. Docs (strict-write.md) tell users to use `Strict()` for such statements; nothing detects the pattern.

**Direction:** at minimum a cheap statement classifier (`CounterBatch`, `+ [`, `+ {`, `= .* + ` on `UPDATE`) that refuses replay (or forces Strict) and emits an event; longer term a `NonIdempotent()` marker on Query/Batch.

#### F8 — MemoryReplayer retains caller arg slices by reference — **CONFIRMED (code)**

- `cql_client.go:2040` / `1861`: `Args: wc.args` where `wc.args = q.values` (3157). No copy. The queued payload aliases the caller's `[]byte` buffers and slices.
- The mirror path recognised this and clones `[]byte` (mirror_dispatch.go:281-303); the primary replay path did not.

**Scenario:** app uses a `sync.Pool` of `[]byte` for blob columns. Exec returns nil (A ok, B failed), buffer is returned to the pool and reused for the next row; the queued replay for B is executed 200 ms later with the *next row's* bytes under the *original* key — silent corruption on B only.

**Direction:** reuse `cloneArgs`/`cloneBatchEntries` in `enqueueReplayIfNeeded` (only on the failure path, so hot-path cost is nil).

#### F9 — Replay does not preserve consistency level, serial consistency, or TTL semantics — **CONFIRMED (code)**

- `ReplayPayload` (types/types.go:176) has no consistency field; `DefaultExecuteFunc` uses session defaults. A write issued at `ALL`/`EACH_QUORUM` replays at the worker session's default (often `LOCAL_ONE`/`LOCAL_QUORUM`).
- `USING TTL n` text is preserved but the TTL clock restarts at replay time (Cassandra computes expiry from server apply time, not the `USING TIMESTAMP`). Rows expire later on the replayed cluster; with long outages the divergence equals the outage duration.
- Documented for mirror (docs/mirror.md:184-192), not for the primary replay path.

**Direction:** add `Consistency *Consistency` to the payload/msgp message; document TTL drift.

#### F10 — Sequential batch processing vs `AckWait`: duplicates and wasted delivery budget — **SUSPECTED**

- `nats_worker.go:210-232`: a batch of up to 100 messages is executed sequentially, each with `ExecuteTimeout=30s`, while the consumer `AckWait=30s` starts at fetch time (nats.go:625-629). With a slow (not down) target, messages late in the batch pass `AckWait` before their turn, are redelivered to the sibling consumer/process, executed twice, and each redelivery consumes one of the 5 deliveries. Also a successful execute followed by a late `Ack` may be silently ignored.

**Direction:** `msg.InProgress()` heartbeats, or `AckWait >= BatchSize * ExecuteTimeout`, or fetch smaller batches.

### LOW

#### F11 — NATS `Enqueue` is a synchronous publish on the write path with a 5 s timeout and an uncancellable context — **CONFIRMED (code)**
`nats.go:428-436`; `context.WithoutCancel` at the call site. When JetStream is slow every partially failed write blocks up to 5 s. No `Nats-Msg-Id` dedup, so a publish that times out *after* the server stored it is counted as `replay_dropped` while the replay still happens (benign duplicate, misleading metric).

#### F12 — Poison messages burn the full delivery budget and are invisible in metrics — **CONFIRMED (code)**
Schema drift (table exists on A, not on B) or F5's `net.IP` case: every such write fails 5 times and is `Term`'d. Corrupt-decode messages are `Term`'d with only `OnCorruptMessage` (nats.go:1001-1009) — no metric, no event. If `Term()` itself fails, `IncReplayDropped`/`OnDrop` are skipped (nats_worker.go:241-243), the loss is uncounted.

#### F13 — Memory backend head-of-line blocking on the first attempt — **CONFIRMED (code)**
`memory_worker.go:82-99`: attempt 1 runs inline with `ExecuteTimeout=30s`. A target that hangs (rather than fast-fails) reduces throughput to 1 payload / 30 s for *both* clusters; the queue then fills and `Enqueue` returns `ErrReplayQueueFull`.

#### F14 — `SwapSession` re-targets the pending backlog — **CONFIRMED (code)**
`DefaultExecuteFunc` resolves `getSession()` at execute time (cql_client.go:1438). Pending replays for slot B follow the swap. Acceptable when the swap replaces a session to the same cluster (its documented purpose); surprising when the slot is repointed. Also any unknown `TargetCluster` string routes to B (1467-1470).

#### F15 — Mirror `Stop` drains the whole 8192-slot queue synchronously inside `client.Close()` — **CONFIRMED (code)**
`mirror/engine.go:243-251` closes the channel and waits; workers keep executing until the channel is empty. With a slow mirror target this can hold `Close()` for minutes; in publisher mode each item is a NATS publish with a 5 s timeout. Not a loss issue but an operational trap; not documented on `Close`.

#### F16 — Docs/code drift in the replay area — **CONFIRMED**
`worker.go:31` "exponential backoff with jitter" (no jitter); docs/replay-system.md:572 NATS backoff "controlled by AckWait" (false, see F1); `WorkerConfig.RetryDelay/MaxRetryDelay` accepted and validated for `NewNATSWorkerChecked` but unused; vm/doc.go:77 advertises a gauge that is never set.

---

## (d) What is already solid

- **Timestamp discipline is correct end-to-end.** Timestamp is always client-generated, applied to *both* original legs and to the replay; `WithTimestamp` override flows through. LWW idempotency for column writes genuinely holds. (`cql_client.go:3085-3166`, `1446-1451`)
- **Enqueue is insulated from caller cancellation** (`context.WithoutCancel`) and from panics in a leg (`safeCQLWrite`), so a partial failure is never silently forgotten because the request context died.
- **Classification of outcomes is sound**: `ErrWriteAsync`/`ErrWriteDropped` are not health failures, both-real-errors returns `DualClusterError` without a phantom replay, one-sided failure always attempts enqueue.
- **Enqueue failure is loudly reported**: metric + log + `OnReplayDropped` + `EventReplayDropped` (`cql_client.go:2066-2073`, `737-748`).
- **NATS wire format is compact and validated**: msgp-generated codec, UUID extension with length checking (`uuid_ext.go:70-78`), corrupt messages are `Term`'d immediately rather than looping, stale consumer references are evicted and rebuilt (`nats.go:700-730`).
- **JetStream usage is durable-by-construction**: `FileStorage`, `WorkQueuePolicy`, durable explicit-ack pull consumers, synchronous publish waits for the server ack, `MaxAckPending` backpressure.
- **Memory worker design** avoids head-of-line blocking on retries (retry pool), bounds goroutines, and its shutdown drop behaviour is explicitly documented with `OnDrop("shutdown")`.
- **Graceful shutdown ordering** in `Close()` is right: mirror engine, then replay worker, then probes, then sessions — no replay runs against a closed session.
- **Mirror engine** is a clean bounded, non-blocking capture with honest drop accounting, `[]byte` cloning, and correct fencing of `TryEnqueue` against `Stop`'s `close(queue)`.
- **Documentation is candid** about counters, LWT, list append, MemoryReplayer volatility, and `DiscardOld` semantics.

---

## (e) Open questions

1. Is the intended contract for `MaxDeliver`/`MaxAttempts` a *poison-message* cap or an *outage-survival* budget? The code implements the former with defaults that make the latter impossible. If both are needed, error classification (unreachable vs rejected) is required.
2. Should the replay worker be topology-aware (pause consumption for a drained cluster), or is drain expected to be short enough that replay exhaustion is acceptable? Today nothing enforces either.
3. For `SwapSession`/`RefreshSession`: is "the backlog follows the slot" the intended semantics, or should pending replays be fenced by a session generation?
4. Is there a reason `SetReplayQueueDepth` was never wired — was it meant to be the worker's responsibility or the client's?
5. For NATS deployments with more than one Helix application sharing a stream/prefix, is it guaranteed that `ClusterA`/`ClusterB` map to the same physical clusters in every worker binary? Nothing in the message identifies the cluster beyond the letter.
6. Is `Replicas: 1` as the default deliberate? It contradicts "recommended replayer for production".
7. Should `MemoryReplayer.Enqueue` reject arg types that the NATS encoder cannot serialise, so that switching backends does not change which writes are reconcilable?

---

### Reproduction notes (throwaway, not committed)

`go test -overlay <scratch>/overlay.json ./replay/ -run TestReview` with three tests placed
virtually at `replay/zz_review_test.go`:

- `TestReviewArgRoundtrip` — table above (F5).
- `TestReviewNATSNakStorm` — embedded nats-server, always-failing execute, `WithRetryDelay(2s)`:
  `dropped after 4.504s with 5 attempts`.
- `TestReviewMemoryOutageWindow` — execute fails for 3 s, default worker config:
  `dropped after 1.502s with 5 attempts`.
