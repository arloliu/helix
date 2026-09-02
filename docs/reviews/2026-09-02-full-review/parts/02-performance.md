# Helix — Fresh Performance Review (hot paths)

Scope: write path, read path, replay path, mirror, adapters, background goroutines.
Method: source read of every hot-path file + existing benchmarks + a scratch
module (outside the repo) with parallel/contention benchmarks, an escape-analysis
pass (`go build -gcflags=-m`), a `-memprofilerate=1` allocation profile of the
dual-write benchmarks, and an embedded NATS JetStream server for `Enqueue`.

Environment: AMD Ryzen 9 9950X3D (32 threads), linux/amd64, go1.26.7
(`go.mod` says 1.26.0), `-benchmem -count 1`. All numbers use
in-process mock sessions unless noted, so they isolate Helix's own overhead
(a real CQL round trip is ~0.5–5 ms; Helix's per-op cost is ~0.1–0.7 µs).

Tags: **CONFIRMED** = measured or verified by reading the exact code path.
**SUSPECTED** = reasoned from source, not measured.

Scratch benchmark sources (not in the repo):
`/tmp/claude-1000/-home-arlo-projects-helix/bca19f30-1dd3-4e0c-b506-6561606e930b/scratchpad/perfbench/`
Raw output: `.../scratchpad/bench/root.txt`, `.../scratchpad/bench/sub.txt`,
`.../scratchpad/perfbench/results.txt`.

---

## (a) Hot-path summary — measured

### Root package (`benchmark_test.go`, mock sessions, serial unless noted)

| Path | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| Direct mock `Exec` (baseline) | 8.2 | 0 | 0 | floor |
| Single-cluster `Query().Exec()` | 155 | 208 | 3 | cqlQuery + values slice + driver query |
| Dual-cluster `Query().Exec()` (nil strategy) | 698 | 464 | 12 | 1 goroutine spawn + 7 heap objects in `executeDualWrite` |
| Dual-cluster `Strict().Exec()` | 710 | 464 | 12 | same shape |
| Dual-cluster `Exec()` + Replayer | 603 | 464 | 12 | replayer not touched on success |
| Dual-cluster `Exec()` + ConcurrentDualWrite/StickyRead/CB | 616 | 480 | 13 | |
| Partial failure + replay enqueue | 826 | 636 | 17 | payload + error objects |
| Single-cluster `Scan` | 165 | 80 | 3 | |
| Dual-cluster `Scan` | 171 | 80 | 3 | closure does NOT escape (good) |
| Dual `Exec` parallel (32 thr) | 237 | 464 | 12 | |
| Dual `Scan` parallel (32 thr) | 36 | 80 | 3 | |
| Single batch `Exec` (5 stmts) | 436 | 896 | 9 | entries slice growth |
| Dual batch `Exec` | 1230 | 1568 | 19 | |
| `Iter()` single/dual | 134 | 144 | 6 | cqlIter + adapter iter + driver |
| `resolveReadTarget` no override | 7.4 | 0 | 0 | |
| `resolveReadTarget` override active | 26.7 | 32 | 1 | the slice the user fn returns |
| `SliceScanAs` 1000 rows | 26 µs | 49.5 KB | 2020 | ≈2 allocs/row, 1 is the `[]any{dst}` in the user callback |
| `drainIterToSliceMapWithLimit` 1000 rows | 75 µs | 344 KB | 2003 | 1 map/row — inherent to SliceMap |
| `MemoryReplayer.Enqueue` | 3.8 | 0 | 0 | |
| `MemoryReplayer.Enqueue` parallel | 86 | 0 | 0 | CAS on `pending` |
| StickyRead / RoundRobin / PrimaryOnly `Select` | 0.7 / 4.0 / 1.0 | 0 | 0 | |
| `CircuitBreaker.ShouldFailover` | 1.2 | 0 | 0 | |
| `sync.WaitGroup.Go` (spawn floor) | 378 | 64 | 3 | the dominant cost of a dual write |

### Policy / replay / adapter packages

| Path | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `ConcurrentDualWrite.Execute` (noop writes) | 286 | 104 | 4 |
| `AdaptiveDualWrite.Execute` both healthy | 431 | 136 | 6 |
| `AdaptiveDualWrite.ExecuteStrict` both healthy | 428 | 136 | 6 |
| msgp `MarshalMsg(natsReplayMessage)` | 31 | 128 | 1 |
| msgp `Unmarshal(natsReplayMessage)` | 38 | 0 | 0 |
| `encodeArgs` (prealloc, small) | 116 | 256 | 7 |
| `encodeArgs` (prealloc, large) | 371 | 832 | 21 |
| adapter v1 `Batch.Query` ×100 | 1.7 µs | 16.2 KB | 8 |
| adapter v2 `Batch.Query` ×100 | 2.2 µs | 16.4 KB | 9 |

### Scratch module — contention and configuration variants (32 threads)

| Benchmark | ns/op | B/op | allocs/op | Read as |
|---|---:|---:|---:|---|
| `CircuitBreaker.RecordSuccess` serial | 16 | 0 | 0 | |
| `CircuitBreaker.RecordSuccess` parallel | 61 | 0 | 0 | mutex per successful read |
| `LatencyCircuitBreaker.RecordLatency` parallel | 66 | 0 | 0 | same lock |
| `AdaptiveDualWrite.Execute` parallel | 230 | 136 | 6 | vs Concurrent 53 → 4.4× |
| `ConcurrentDualWrite.Execute` parallel | 53 | 104 | 4 | |
| Client dual `Exec`, NopMetrics, serial | 621 | 384 | 12 | |
| Client dual `Exec`, vm collector, serial | 641 | 384 | 12 | +20 ns intrinsic |
| Client dual `Exec`, NopMetrics, parallel | 204 | 384 | 12 | |
| Client dual `Exec`, vm collector, parallel | 447 | 384 | 12 | **+243 ns → contention** |
| Client dual `Exec`, AdaptiveDualWrite, parallel | 322 | 432 | 15 | |
| Client dual `Exec` + 3 option setters | 621 | 408 | 15 | +3 allocs for `Consistency/WithTimestamp/WithPriority` |
| Client `Scan`, StickyRead + ActiveFailover, parallel | 26 | 40 | 3 | |
| Client `Scan`, StickyRead + CircuitBreaker, parallel | 145 | 40 | 3 | **+120 ns from the CB mutex** |
| Client `Scan`, StickyRead + LatencyCB, parallel | 173 | 40 | 3 | |
| Client `Scan`, 10-deep ctx chain (fallback key lookup) | 160 | 40 | 3 | +9 ns vs Background — negligible |
| Client `Exec` with mirror engine present, not `.Mirror()` | 144 | 416 | 14 | mirror off-path is free |
| Client `Exec().Mirror()` enabled | 359 | 552 | 19 | +215 ns, +5 allocs |
| Closure-based dual write (current shape) | 384 / 62 par | 136 | 6 | |
| Single-struct dual write job (prototype) | 338 / 52 par | 112 | 2 | −12 % / −16 %, 6→2 allocs |
| `NATSReplayer.Enqueue` serial (loopback JetStream) | **15.1 µs** | 3.0 KB | 43 | sync `js.Publish` |
| `NATSReplayer.Enqueue` parallel | 3.9 µs | 3.1 KB | 40 | |
| raw `js.PublishAsync` reference | 1.6 µs | 2.1 KB | 31 | |
| `Enqueue` with `[16]byte` arg | 15.1 µs | 2.9 KB | 42 | |
| `Enqueue` with `gocql.UUID` arg | 15.7 µs | 2.9 KB | **57** | +15 allocs: reflect path |
| Memory replay worker drain, 2000 payloads @1 ms exec | 2.11 s → **949/s** | | | serial bound is 1000/s |

Driver struct sizes (`unsafe.Sizeof`): gocql v1 `Query` = 296 B, v2 `Query` = 312 B, v1 `Batch` = 216 B.

---

## (b) Findings, ranked by impact

### HIGH

#### H1. Replay drain is single-threaded per backend — throughput is bounded by 1 / execute latency  — CONFIRMED (measured 949/s @ 1 ms)

- `replay/memory_worker.go:60-89` — one dequeue goroutine; `handleFirstAttempt` (`:87`, `:96-127`) runs the first attempt **inline** on that goroutine (`runAttempt` → `executeOnce` `:168-170`, `:237-242`). Only retries fan out (`retryAsync` `:126`, capped by `retrySem` = 100).
- `replay/nats_worker.go:51-112` — one goroutine per cluster; `processMessages` (`:212-286`) executes the fetched batch **sequentially**, one `executeOnce` at a time. `BatchSize=100` and `MaxAckPending=1000` therefore buy nothing for throughput.
- Evidence: scratch test drained 2000 payloads with a 1 ms execute in 2.108 s = 949/s, i.e. exactly the serial bound. With a realistic 2–5 ms CQL write, replay drains at 200–500/s per cluster. A 5-minute outage at 5k writes/s leaves ~1.5M payloads → 50–125 minutes to reconcile, during which `FallbackRead` divergence and stale reads persist.
- Impact: this is the library's core availability promise ("asynchronous reconciliation"); it's the one place where throughput matters more than ns/op.
- Direction: a bounded executor pool inside both backends (e.g. `WithReplayConcurrency(n)`, default 8–16): the dequeue loop hands payloads to N executors via a channel; NATS backend processes each fetched batch with a `WaitGroup` fan-out and acks per message as today. The memory backend can also drop the `TryDequeue + time.After` poll (`:77-85`) for the blocking `MemoryReplayer.Dequeue(ctx)` that already exists (`replay/memory.go`), with `ctx` cancelled on `stopCh`.
- Trade-offs: ordering across payloads is lost. That is already safe for regular writes because every replay carries the original client timestamp (LWW), and counter batches are already documented as non-idempotent. Per-payload retry goroutines already break ordering today, so this is not a new semantic. Ack pressure: keep `MaxAckPending ≥ BatchSize × concurrency`.

#### H2. `NATSReplayer.Enqueue` is a synchronous JetStream publish on the caller's write path, and every write to a degraded/drained cluster pays it — CONFIRMED (measured 15 µs loopback; real RTT ≫)

- `replay/nats.go:401-467`: encode args (`:411`), build `msgp` message (`:445`, 1 alloc), `context.WithTimeout(ctx, PublishTimeout=5s)` (`:458`), then `n.js.Publish(pubCtx, subject, data)` (`:461`) which waits for the stream ack.
- Called from `cql_client.go:1869` (drain path) and `:2049` (`enqueueReplayIfNeeded`) with `context.WithoutCancel(ctx)` — so the caller's deadline does not bound it; only `PublishTimeout` (5 s) does.
- Amplification: `executeDualWrite` enqueues a replay for **every** `ErrWriteAsync` and `ErrWriteDropped` result (`cql_client.go:2006-2016`, `:2026-2075`). With `AdaptiveDualWrite`, a degraded cluster turns 100 % of writes into a NATS publish; with drain mode (`:1856-1879`), likewise. So exactly when one cluster is slow, every write acquires an extra JetStream round trip (0.2–2 ms on a real network, up to 5 s on a slow JetStream) — the opposite of what the adaptive strategy is trying to achieve (not blocking on the slow leg).
- Evidence: 15.1 µs/op serial vs 1.6 µs/op for `PublishAsync` on loopback, i.e. ~10× even in-process; 43 allocs / 3 KB per enqueue.
- Direction: (1) `js.PublishAsync` with `jetstream.WithPublishAsyncMaxPending(n)` and a background goroutine consuming the ack futures — failures go to `OnReplayDropped`/`IncReplayDropped` and `EventReplayDropped`; (2) or a small in-memory staging ring in front of NATS with a batching publisher (also coalesces the per-message `WithTimeout` timer + subject string). (3) Cheap wins regardless: reuse an encode buffer (`sync.Pool` of `[]byte` for `MarshalMsg`), avoid `context.WithTimeout` per publish when `ctx` already carries a deadline.
- Trade-offs: async publish weakens "enqueue succeeded ⇒ durably queued" at the moment `Exec` returns; the drop callback becomes asynchronous. Offer it as an option (`WithAsyncPublish`) with the sync path as default if durability-at-return is a documented contract.

### MEDIUM

#### M1. `CircuitBreaker.RecordSuccess` takes a mutex on every successful read — CONFIRMED (+120 ns/read at 32 threads)

- `policy/failover_policy.go:578-627`: `muA.Lock()` → 3 atomic stores → `Unlock()` on every success; `LatencyCircuitBreaker.RecordLatency` (`policy/latency_circuit_breaker.go`) routes to the same. Called from `recordReadSuccess` (`cql_client.go:2206-2218`) on every read.
- Evidence: client `Scan` parallel = 26 ns with `ActiveFailover` vs 145 ns with `CircuitBreaker` / 173 ns with `LatencyCircuitBreaker`; the breaker alone: 16 ns serial → 61 ns parallel. `ShouldFailover` (`:432-463`) is already lock-free — only the success path locks.
- Direction: lock-free fast path at the top of `RecordSuccess`: `if failures.Load() == 0 { return }`. Invariant verified: every `failuresX.Store(0)` (`:589`, `:604`) is paired with `trippedX = false` (`:591`, `:606`), and a trip requires `newFailures ≥ threshold ≥ 1` (`:525`, `:549`), so `failures == 0 ⇒ !tripped` and there is nothing to reset.
- Trade-offs: a `RecordFailure` that lands between the load and the return is simply ordered after the success — the same outcome as the lock ordering would give. `lastFailure` stays at its old value while `failures==0`; it's only consulted when `failures > 0` (`:451`, `:501`), so no behavior change.

#### M2. `AdaptiveDualWrite` hot path locks both cluster mutexes per write and escapes 2 extra objects — CONFIRMED (4.4× slower than ConcurrentDualWrite under parallel load)

- `policy/adaptive_write.go:558-626`: after `wg.Wait`, `updateHealthState` → `processLatencies` → `recordFastIfNoViolation` ×2 → `recordFast` (`:987-1023`) which does `state.mu.Lock()` on **both** clusters for every healthy write just to store `slowStrikes = 0`.
- The `wg.Go` closures at `:574-578` capture `errB`/`latencyB` (and in the mixed branch `errA`/`latencyA`) by reference from a goroutine → "moved to heap"; 6 allocs vs 4 for `ConcurrentDualWrite`.
- Evidence: 431 ns serial / 230 ns parallel vs 286 / 53 for `ConcurrentDualWrite`; through the client 322 ns vs 204 ns parallel, 15 vs 12 allocs.
- Direction: make `slowStrikes`/`fastStrikes` `atomic.Int32` and add a fast path in `recordFast`: `if !state.isDegraded.Load() && state.slowStrikes.Load() == 0 { return }`; keep `mu` for the compound transitions. Move per-call state into one stack/heap struct (see M4) so only one object escapes.
- Trade-offs: a strike racing the fast-path load is retained rather than reset — equivalent to the strike being ordered after the fast write, which is within the existing "one-call lag" tolerance already documented at `:95-103`.

#### M3. `contrib/metrics/vm` histograms serialize on a mutex; counters share cache lines — CONFIRMED (+243 ns per dual write at 32 threads, +20 ns serial)

- `contrib/metrics/vm/vm.go:448-454` (`ObserveReadDuration`) and the write equivalent call `metrics.PrometheusHistogram.Update`, which is `h.mu.Lock(); h.sum += v; h.count++; h.buckets[i]++; Unlock()` (VictoriaMetrics `prometheus_histogram.go:69-91`). A dual write does 2 histogram updates + 2 counter increments; a read does 1 + 1. `Counter.Inc` is a single `atomic.AddUint64` — cheap serially, but one word shared by every core.
- Evidence: serial 621 → 641 ns (intrinsic +20 ns); parallel 204 → 447 ns. The delta is contention, not work.
- Direction: keep the Prometheus exposition format but make the storage lock-free: each bucket as a `metrics.Counter` named `..._bucket{cluster="A",le="0.005"}` (atomic add, cumulative bucket semantics computed at scrape via `metrics.Set` callbacks or by adding to all buckets ≥ idx), `_count` as a counter, `_sum` as a float gauge backed by `atomic.Int64` nanoseconds and exposed via `NewGauge(name, func() float64)`. Optionally shard by `runtime` P / per-goroutine hash (e.g. 8 shards merged at scrape) to remove the shared-line ping-pong on the counters too.
- Trade-offs: more code in `vm.go`, and exposition ordering must stay Prometheus-valid (`le` ascending, `+Inf` last). Metrics is off by default (`NopMetrics`), so this only matters for users who opt in — but those are the production users.

#### M4. Dual-write orchestration allocates 7 objects per call in `executeDualWrite` — CONFIRMED (profile + escape analysis); prototype shows 6 → 2 allocs, −12 % serial / −16 % parallel

- `cql_client.go:1904` (`startA`, `startB` → heap), `:1911` / `:1915` (`writeA`, `writeB` closures escape), `:1920` (`errB` → heap because the goroutine writes it), `:1935` (`wg` → heap), `:1937` (`wg.Go` closure) plus `sync.WaitGroup.Go`'s own wrapper and the `writeFunc` closure at `:3161` / `:3586`. `-memprofilerate=1` attributes 2.4 M of the 7.4 M objects in the dual benchmarks to `executeDualWrite` itself. `executeStrictDualWrite` (`:2090-2119`) duplicates the same shape.
- Evidence: 12 allocs / 464 B per dual `Exec` vs 3 / 208 single; `BenchmarkWaitGroupGo` shows the goroutine spawn floor is 378 ns of the 698.
- Direction: one heap-allocated `dualWriteJob{ctx, writeFunc, startA, startB, errA, errB, wg}` with methods `runA()/runB()` so the goroutine closure captures one pointer. Prototype measured 6 → 2 allocs, 384 → 338 ns serial, 62 → 52 ns parallel. Do **not** `sync.Pool` it: `AdaptiveDualWrite.fireAndForget` (`policy/adaptive_write.go:668-726`) retains `writeB` in a goroutine after `Execute` returns, so the job can outlive the call.
- Trade-offs: slightly less readable than closures; ~50 ns/op and ~150 B/op saved — real but small against a 1 ms CQL round trip. Worth doing together with M2 since both touch the same shape.

#### M5. Every fluent option setter allocates — CONFIRMED (`-gcflags=-m`: "moved to heap")

- `cql_client.go:2957` (`Consistency`), `:2970` (`SerialConsistency`), `:2975` (`PageSize`), `:2985` (`WithTimestamp`), `:2990` (`WithPriority`), `:3010` (`MaxRows`) store `&c` / `&n` / `&ts` / `&p` into pointer fields; the same pattern in `cqlBatch` (`:3463-3498`). A query that sets `Consistency` + `WithTimestamp` costs 2 extra 8-byte allocations per call; measured +3 allocs/op for three setters.
- Direction: value fields + a `set` bitmask (or `hasConsistency bool` etc.). `applyConfig` (`:3098-3113`) and `getTimestamp/getPriority` read the flag instead of nil-checking. Zero semantic change.
- Trade-offs: none; `cqlQuery` grows by a few bytes.

#### M6. gocql v1 adapter: a 296-byte `Query` is allocated twice per operation — CONFIRMED by reading driver source; savings SUSPECTED (needs a live session to measure)

- `adapter/cql/v1/adapter.go:77-83`: `s.session.Query(...)` pulls from gocql's `queryPool` (`gocql/session.go:454-461`). Helix never calls `Release()` on the hot path (`cqlQuery.ExecContext` `:3141-3168` does not), so the pool is perpetually empty and every `Query()` allocates a fresh 296 B struct.
- `:213`, `:219`, `:224`, `:229`, `:239`, `:249`: every `*Context` method calls `q.query.WithContext(ctx)`, which is `q2 := *q; return &q2` (`gocql/session.go:1099-1103`) — a second 296 B copy per op. Batch `WithContext` does the same 216 B copy (`:303`, `:314`, `:329`, `:347`; driver `:1894-1898`).
- v2 (`adapter/cql/v2/adapter.go`) allocates once (`&Query{}`, 312 B) and its `ExecContext` takes `ctx` directly — no copy.
- Direction: the `WithContext` copy is unavoidable with gocql v1's API, but the pool miss is not: after `q2 := q.query.WithContext(ctx)`, call `q.query.Release()` on the original so the next `Query()` hits the pool (gocql's `reset()` zeroes only the original; the copy is independent). Alternatively keep the adapter as-is and document v2 as the performance-preferred adapter.
- Trade-offs: `Release` semantics must be re-verified against the pinned gocql version each upgrade; the adapter `Query` also caches `values` (`:81`) so `Values()` keeps working after release.

### LOW

#### L1. Three clock reads per read and per write — CONFIRMED by reading
`runPrimaryRead` (`cql_client.go:2294-2296`) reads `time.Now()` twice, then `recordOpOutcome` (`:472` → `:487`) calls `NowProvider()` a third time; `tryFallbackCluster`/`executeFallbackRead` repeat the pattern. Writes already thread `nowNano` (`:1991-1992`). ~20 ns each on Linux vDSO. Direction: pass `start.UnixNano()+elapsed` into `recordOpOutcomeAt`.

#### L2. Memory replay worker polls with `time.After(100 ms)` instead of blocking — CONFIRMED
`replay/memory_worker.go:77-85`: on an empty queue it allocates a timer every 100 ms and adds up to 100 ms of latency before the first replay attempt. `MemoryReplayer.Dequeue(ctx)` (`replay/memory.go`) already blocks on the channels. Direction: use it with a `stopCh`-cancelled context (folds into H1).

#### L3. NATS worker allocates a `context.WithTimeout` per loop iteration and per message — CONFIRMED
`replay/nats_worker.go:72` (5 s per poll) and `:333` (`ExecuteTimeout` per message) each create a timer; `fetchReplayMessages` adds another 1 s timer (`replay/nats.go:596`). Idle cost is ~4 fetch RPCs/s per cluster (high+low consumers) — fine; under load it's 2 timer allocs per replayed message. Direction: derive one deadline per batch.

#### L4. `Columns()` is copied twice per call — CONFIRMED
Adapter `Iter.Columns()` (`adapter/cql/v1/adapter.go:440-457`, v2 equivalent) builds a `[]cql.ColumnInfo`, then `cqlIter.Columns()` (`cql_client.go:3786-3799`) copies it again into `[]helix.ColumnInfo` with identical fields. Direction: `type ColumnInfo = cql.ColumnInfo` and return the adapter slice; or cache per iterator.

#### L5. Batch entries grow by append from zero capacity — CONFIRMED
`Batch()` (`cql_client.go:1059-1065`) does `make([]batchEntry, 0)`; each `Query()` (`:3455-3461`, "append escapes to heap") reallocates at 1, 2, 4, 8, 16… A 10-statement batch pays 4 growth allocs before the driver batch is built once per cluster (`:3586-3600`, unavoidable). Direction: start at cap 8, or accept a size hint.

#### L6. UUID args take a 16×`reflect.Index().Convert().Interface()` path in `encodeArgs` — CONFIRMED (+15 allocs per UUID arg)
`replay/nats.go:789-852`: `gocql.UUID` (named `[16]byte`) matches neither the `[16]byte` case nor `encoding.BinaryMarshaler` (gocql's `UUID` has only `MarshalText`/`MarshalJSON`, verified in `gocql/uuid.go:318-341`), so it falls through to the per-byte reflection loop at `:839-845`. Measured: `Enqueue` 42 allocs with `[16]byte` vs 57 with `gocql.UUID`. Direction: `reflect.Copy(reflect.ValueOf(&u).Elem(), rv)` (one call, no per-byte boxing), or check for `interface{ Bytes() []byte }` first. Only on the replay-enqueue path, but UUID keys are the common case in Cassandra schemas and H2 makes every degraded write go through here.

#### L7. `Mirror()` enabled costs +215 ns / +5 allocs and an `RWMutex.RLock` per capture — CONFIRMED
`mirror_dispatch.go:334-347` (`cloneArgs`, payload by value) → `mirror/engine.go:262-284` (`enqueueMu.RLock`, `len(queue)` gauge). The off-path is free (144 ns with and without an engine). The RWMutex reader count is one shared word — at high core counts it ping-pongs like the metrics counters. Direction: never `close(queue)`; stop via a `quit` channel that workers `select` on, and drain what's left after `wg.Wait` — removes the lock entirely. Minor.

#### L8. `hasFallbackRead` walks the context chain on every read — CONFIRMED negligible
`client.go:145` via `resolveReadOptions` (`cql_client.go:2193-2201`). Measured +9 ns for a 10-deep chain. Leave as-is.

#### L9. `EmitClusterEvent` uses `defer` and a shared `emitting` counter — CONFIRMED, failure-path only
`events.go:139-167` — only reached on failover/divergence/transitions; the buffered channel (128) and drop counting keep it non-blocking. No action.

---

## (c) Already good (confirmed)

- Session swapping is lock-free on the hot path: `atomic.Pointer[sessionHolder]` (`cql_client.go:78-79`, `:168-181`); no RWMutex anywhere on read/write dispatch.
- `resolveReadTarget` is zero-alloc without an override and returns everything by value (`:1508-1523`, `:1548-1647`): 7.4 ns.
- Read strategies are atomic-only in `Select` (`policy/read_strategy.go`): 0.7–4 ns, 0 allocs; `StickyRead.OnFailure` only takes its RWMutex on the failure path.
- `CircuitBreaker.ShouldFailover` and `ActiveFailover` are lock-free (`policy/failover_policy.go:432-463`).
- The `Scan`/`MapScan` closures do not escape (`-gcflags=-m`: "func literal does not escape" at `:3180`, `:3232`) — a dual-cluster read is 3 allocs / 80 B, identical to single-cluster.
- `FallbackRead` for `SliceMap`/`SliceScan` does not double-buffer: the alt leg only runs when the primary drained zero rows (`:3265`, `:3362`), and `drainIterToSliceMapWithLimit` nils partial results on error (`:2901-2908`).
- `drainIterScanWithLimit` hoists and pre-boxes the `RowScanner` adapter (`:2849-2856`) — one allocation per drain, not per row.
- Only one goroutine per dual write (A inline, B spawned) in both the nil-strategy path (`:1937-1943`) and every built-in strategy.
- Batch replay conversion is lazy (`writeContext.toBatchStatements`, `:1721-1735`) — the success path never builds `[]types.BatchStatement`.
- Metrics default to `NopMetrics`; the VM collector pre-creates every series (no label formatting, no map lookup per call).
- Cluster events cost nothing without a handler (nil dispatcher no-ops) and policies' outbox `enqueue` is reached only on real transitions (`policy/event_outbox.go`).
- Mirror off-path is free; drop logging is rate-limited with an atomic CAS (`mirror/engine.go:286-303`).
- Replay serialization is tight: msgp 12–70 ns, 0–1 alloc; subjects/consumer names use concatenation, not `Sprintf` (the repo's own `nats_perf_internal_test.go` shows the 5–8× win); `encodeArgs` preallocates.
- `MemoryReplayer.Enqueue` is 3.8 ns / 0 allocs with a CAS-based capacity guard.
- Background goroutines are idle-cheap: recovery probe checks `IsDegraded` atomically before doing anything (`cql_client.go:392`), auto-refresh is three atomic loads per tick, the NATS topology watcher uses KV `Watch` and only polls (5 s) as a fallback, the event dispatcher blocks on a channel. No `time.After` leaks in long-lived loops except L2 (bounded, GC-collected).

---

## (d) Open questions

1. **Replay ordering contract.** Is per-partition ordering of replays required anywhere? If LWW timestamps are the only correctness mechanism (as the docs imply), H1's parallel executor is safe for everything except counter batches, which are already documented as unsafe.
2. **Enqueue durability contract.** Must `Exec` return only after the replay payload is durably in JetStream? If yes, H2's async publish must be opt-in; if "best-effort with drop accounting" is acceptable, it can be the default.
3. **`SetReplayQueueDepth` has no producer.** `types/metrics.go:119` defines it and `vm.go:626` implements it, but no non-test code calls it — the `replay_queue_depth` gauge is always 0. Not a perf issue, but it is the metric an operator would use to see H1's backlog.
4. **Fire-and-forget budget.** `fireForgetLimit=100` with `fireForgetTimeout=30 s` means a degraded, slow cluster can pin all 100 slots for 30 s; everything beyond that is `ErrWriteDropped` → replay. Combined with H2 that is one NATS publish per write for the outage's duration. Is the intent that the replay system, not the semaphore, is the real throttle?
5. **SliceMap preallocation hint.** `drainIterToSliceMapWithLimit` uses `min(limit, 1024)`; `iter.NumRows()` (current page size) is available and would be a better first-page hint. Worth checking whether both drivers report it before the first `MapScan`.
6. **gocql v1 `Release()` after `WithContext`** (M6): needs a live-session benchmark to confirm the pool actually refills and that `reset()` never touches the copy's shared slices.
