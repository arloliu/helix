# Fresh Review 04 — Auto-Recovery and Self-Healing Under Extreme Conditions

Reviewer stance: independent SRE / chaos read of the source at commit `7e93857` (v1.6.0 line).
Everything below was derived from the code first; `docs/*.md` were consulted only to check
whether a behaviour is documented. Five hypotheses were reproduced with throwaway tests
against the public API (scratch module, minimal `cql.Session` mock, `policy`, `replay`);
those are marked **CONFIRMED (measured)**. Code-only conclusions are **CONFIRMED (code)**.
Things I could not execute (NATS, real drivers) are **SUSPECTED**.

Defaults assumed unless stated: `AdaptiveDualWrite` (delta 300 ms, absMax 2 s, strike 3,
recover 5, fire-forget timeout 30 s, limit 100), recovery probe (2 s / 1 s), `StickyRead`
(5 min cooldown), `CircuitBreaker` (3 / 30 s), `MemoryReplayer` (10 000) + `MemoryWorker`
(MaxAttempts 5, backoff 100 ms→30 s, retry pool 100, poll 100 ms), `NATSReplayer`
(MaxDeliver 5, AckWait 30 s, PublishTimeout 5 s, DiscardOld, MaxAge 24 h), auto-refresh
(threshold 10, window 5 min, retry 1 min, check 30 s).

---

## (a) Scenario matrix

| # | Scenario | Components | Converges automatically? | Time-to-heal | Data risk | Stuck states |
|---|----------|-----------|--------------------------|--------------|-----------|--------------|
| S1 | A hard-down 6 h, returns | AdaptiveDualWrite, recovery probe, replay worker, CB, StickyRead/PrimaryOnlyRead | **Partial** — writes yes, reads strategy-dependent, data **no** | Writes: ~10 s after A returns (5 probe ticks), sub-second if fire-and-forget traffic succeeds first. Reads: never (StickyRead) / `recoveryTimeout` (PrimaryOnlyRead) | **High**: replay entries for A are exhausted and dropped ~1.5 s after enqueue (memory) or after 5 immediate Naks (NATS); when A returns there is no backlog to drain — 6 h of writes are missing on A unless `OnDrop` persisted them | StickyRead preferred=B forever; CB state gauge for A stays "open" until a read touches A; no replay-backlog-aware read gating |
| S2 | Both down 2 min, both return | AdaptiveDualWrite, replay, CB, read strategies | **Partial** — state recovers symmetrically; data does not | Writes: 3 failed writes → both degraded; after return ~10 s per cluster. Reads: first successful read | **High**: after both are degraded every write returns `nil` backed only by the replay queue (or by nothing when no Replayer); those entries are dropped ~1.5 s later | PrimaryOnlyRead without `recoveryTimeout` stays on B; CB gauges stale |
| S3 | A fails 1 s every 10 s for 1 h | AdaptiveDualWrite, probe, replay, CB, StickyRead, auto-refresh | **Yes** (state), with oscillation | Degrade within 3 writes of each blip; recover 5 fast background writes later (ms under load, ≤10 s via probe) | Low–Medium: every write during degraded windows is duplicated through replay (idempotent except counters); replay entries from the 1 s blip retry within the same ~1.5 s window — some may still land on a down A and be dropped | ~360 degrade/recover pairs per hour; no time-based hysteresis; auto-refresh correctly never fires |
| S4 | A alive but 5 s latency, no errors | AdaptiveDualWrite, LatencyCB, StickyRead, replay worker, Concurrent/Sync writes | **Partial** | Adaptive: 3 slow writes (15 s of caller-visible latency) → degraded; LatencyCB trips after 3 slow reads but **does not move reads** | Medium: replay worker executes serially → throughput 0.2/s; queue fills; drops | Concurrent/SyncDualWrite callers block for the full 5 s per write (no per-cluster timeout); probe (fast `system.local`) can re-recover a write-slow cluster → 15 s-latency oscillation |
| S5 | Replay saturation / NATS unreachable / poison | replay enqueue path, MemoryReplayer, NATSReplayer, workers, events | **Partial** | n/a | **High**: write acknowledged `nil` while replay entry is lost — surfaced via `IncReplayDropped` + `EventReplayDropped` + `OnReplayDropped` (good). Silent cases: JetStream `DiscardOld` eviction, `MaxAge` expiry, replay ignores original consistency level, TTL drift | NATS outage adds up to `PublishTimeout` (5 s) synchronously to every partially-failed write |
| S6 | Permanently dead session (DNS/port change) | auto-refresh detector, `RefreshSession`, `SwapSession` | **Partial** — works with `ConcurrentDualWrite`; **starved** with `AdaptiveDualWrite` + StickyRead (default recommended stack) | ≥5 min + ≤30 s tick; retry every 1 min forever (no backoff, bounded) | High under starvation: A stays degraded forever, replay to the dead session drops everything | consecutiveFailures frozen at 3 (< threshold 10) once A is degraded |
| S7 | NatsKV topology loses NATS / both draining | topology/nats, watchTopology | **Yes** (drain state) | ≤ PollInterval (5 s) after NATS returns | Low: fail-closed (last-known drain kept); both draining → `ErrBothClustersDraining` until operator changes KV | Watch mode never re-established (poll forever); startup without NATS is fail-open |
| S8 | Process restart | MemoryReplayer, NATS durable consumers | Memory: **No** (documented); NATS: **Yes** | NATS: immediate (work-queue, durable per priority×cluster) | Memory: loss measured only on graceful `Close` (OnDrop "shutdown"); crash = unmeasured. NATS: batch of 100 × slow executes > AckWait 30 s → redelivery + duplicate execution | — |
| S9 | Recovery ordering / operator overrides | probe, ForceDegrade, AllowedClusters, PrimaryOnlyRead, CB | **No** for backlog ordering (by design, operator-driven) | — | Stale reads if reads return before backfill (documented) | **Recovery probe undoes `ForceDegrade` within ~10 s**; CB × PrimaryOnlyRead probe cycle costs 2 caller-visible errors |
| S10 | Goroutine/timer hygiene under long degradation | fire-forget semaphore, retry pool, outbox, dispatcher, metrics | **Yes** | — | Low | Nothing unbounded found; only caller-driven blocking (no ctx deadline) can pile goroutines |

---

## (b) Per-scenario traces

### S1 — Cluster A hard-down for 6 hours, then returns

**Write side.** `executeDualWrite` (`cql_client.go:1893-2020`) calls `AdaptiveDualWrite.Execute`
(`policy/adaptive_write.go:558-626`). Each real error on A → `recordStrike` (`:927-975`); the
third strike flips `isDegraded` and emits `EventWriteDegraded`. From then on A's leg goes through
`fireAndForget` (`:642-729`): non-blocking semaphore (`:659-666`, **one semaphore shared by both
clusters**), `context.Background()`+30 s timeout (`:671`), and the caller gets `ErrWriteAsync`.
If A is black-holed rather than refusing connections, each goroutine holds a slot for 30 s, so only
~3 writes/s are dispatched async; the rest return `ErrWriteDropped`. Both are classified as
"operational" (`cql_client.go:1949-1952`), a replay payload is enqueued for A
(`:2013-2016`), and **the caller receives `nil`** (`:2019`).

**Replay side (the important part).** `MemoryWorker` dequeues within 100 ms
(`replay/memory_worker.go:77-87`), runs attempt 1 inline, then attempts 2–5 in a retry goroutine
with backoff 100/200/400/800 ms (`:135-161`, `replay/worker.go:421-434`) and drops via `OnDrop`
after ~1.5 s (`:160`). Once 100 retries are in flight, every further first-attempt failure is
dropped **immediately** with reason `retry pool saturated` (`:118-123`).
**CONFIRMED (measured):** 500 payloads enqueued against a hard-down execute func with default
config → 500 dropped, last drop at 1.50 s, queue length 0, 900 total attempts (100 got 5 tries,
400 got 1). The "replay backlog" for a 6-hour outage therefore never exists — it is a ~1.5 s
retry buffer. NATS path: `processMessages` Naks immediately (`replay/nats_worker.go:256`,
`msg.Nak()` without delay), the loop re-fetches without pause while messages exist
(`:65-111`), so `MaxDeliver=5` is consumed in seconds and the message is `Term`'d and counted
dropped (`:239-253`). **CONFIRMED (code)**; `MaxAge=24h` never gets a chance to matter.
There is no "target cluster is down, pause this consumer" gate anywhere in the worker.

**Recovery of writes.** `recoveryProbeLoop` (`cql_client.go:380-412`) ticks every 2 s, runs
`SELECT release_version FROM system.local` (`config.go:61-64`) only while `IsDegraded`, and on
success calls `RecordProbeSuccess` → `recordFast` (`adaptive_write.go:1288-1290, 987-1023`).
Five successes → healthy → `EventWriteRecovered`. Concurrent fire-and-forget successes that are
fast and within `deltaThreshold` of B also credit (`:697-725`), so under load recovery is
sub-second; worst case ~10 s. Failed probes are only `Debug`-logged (`:408`).

**Reads.** `StickyRead.OnFailure` (`policy/read_strategy.go:142-185`) swaps preferred to B once
(cooldown 5 min gates *further* swaps). Nothing ever swaps it back; the type doc says so
(`:22-28`). With `CircuitBreaker`, the first two read failures are returned to the caller
without failover (`cql_client.go:2478-2484`, `failover_policy.go:451-453`). After A returns,
no read touches A, so `RecordSuccess(A)` is never called and the CB gauge
`SetCircuitBreakerState(A, 2)` stays "open" indefinitely (the timed close only runs inside
`RecordFailure`, `:501-520`). `PrimaryOnlyRead` with `WithPrimaryOnlyRecoveryTimeout` does
return to A (`read_strategy.go:268-281`) — with no knowledge of the replay backlog. There is
no replay-backlog-aware gating; `WithAllowedClusters` is the manual lever (`config.go:99-117`).

### S2 — Both clusters down 2 minutes, then return

`ConcurrentDualWrite`/default: both legs real errors → `DualClusterError`, no replay
(`cql_client.go:2000-2004`). Caller knows; no silent loss.

`AdaptiveDualWrite`: writes 1–3 return `DualClusterError` and strike both clusters; from write 4
both legs are fire-and-forget → both `ErrWriteAsync` → `realErrA/B` false → replay enqueued for
both → **return `nil`**. With no Replayer configured, `nil` is returned with zero copies and
nothing queued (`:2013`). **CONFIRMED (measured):** writes 0–2 → `DualClusterError`, writes 3+ →
`nil` with both clusters degraded and no replayer. With `MemoryReplayer` the queued entries die
within ~1.5 s (see S1), so effectively every write acknowledged during the outage is lost.
`ExecuteStrict` correctly returns `ErrClusterDegraded` for both (`adaptive_write.go:1220-1222`).

Recovery is symmetric: one probe goroutine per cluster (`cql_client.go:371-373`), ~10 s each.
The `fireAndForget` recovery credit uses the sibling's stale pre-outage `lastLatency` (not
reset on degrade) for the delta check — harmless. Reads: StickyRead is on whichever cluster
it switched to; both back → immediate. `PrimaryOnlyRead` without `recoveryTimeout` stays on B
(documented). CB: the cluster that receives the first read closes; the other's gauge is stale.

### S3 — Flapping A: 1 s failure every 10 s for an hour

Per blip: 3 strikes in the first ~3 writes → degraded (`Warn` + event). A returns 1 s later;
background writes succeed fast → `recordFast` ×5 → recovered within milliseconds under load
(or ≤10 s via probe at low QPS). Next blip repeats. Nothing time-gates the transition
(`recordStrike`/`recordFast` are pure counters), so ~360 degrade/recover pairs/hour, each
producing a `Warn`/`Info` line, metric increments and two events (outbox cap 64,
`policy/event_outbox.go:15`; dispatcher buffer 128, `events.go:21` — not overrun at this rate).
Replay churn: every write in a degraded window is enqueued *in addition to* the fire-and-forget
attempt; both succeed on A (same client timestamp → idempotent, `DefaultExecuteFunc`
`cql_client.go:1449-1451`), doubling A's write load in those windows. Entries enqueued during
the 1 s blip retry at 100/200/400/800 ms, so most land after A returns; some are dropped.
CB + StickyRead: after the first blip trips CB and swaps preferred to B, reads stay on B and stop
flapping. `PrimaryOnlyRead(recoveryTimeout)` re-probes A once per timeout and may land in a
blip → flips at timeout cadence. Auto-refresh: successes reset `consecutiveFailures`
(`cql_client.go:485-490`) so it correctly never fires.

### S4 — Slow-but-alive A (5 s latency, no errors)

`ConcurrentDualWrite`: `wg.Wait()` on both legs (`policy/write_strategy.go:60-76`); shared caller
ctx; no per-cluster timeout anywhere in Helix — the caller blocks 5 s per write unless it sets a
ctx deadline, in which case A's leg fails with `DeadlineExceeded` → replay (fine) but throughput
is still bounded by the deadline. `SyncDualWrite` (A first): after A's 5 s, `ctx.Err()` short-
circuits B (`:163-167`) → both errors → `DualClusterError`, no replay, and A's actual outcome is
unknown to the caller.

`AdaptiveDualWrite`: `checkAbsoluteCap` strikes per write > 2 s (`adaptive_write.go:787-799`);
three writes (15 s of caller-visible latency) → degraded → callers stop blocking. Recovery: the
probe reads `system.local` with a 1 s timeout. If slowness affects all queries the probe fails
and A stays degraded (correct). If only writes are slow (commitlog/disk stall) the read probe
passes, `RecordProbeSuccess` credits regardless of the probe's own latency (`cql_client.go:
398-399`), A recovers after 10 s, the next three writes block callers 5 s each, and A degrades
again — a 25 s oscillation with 15 s of caller pain per cycle. **CONFIRMED (code)**; the probe
has no latency criterion and no minimum stable period.

Fire-and-forget at 5 s per write: 100 shared slots / 5 s → 20 async writes/s, the rest
`ErrWriteDropped` → replay. `MemoryWorker` is one goroutine with the first attempt inline
(`memory_worker.go:43-45, 96-102`), so replay throughput ≈ 1/latency = 0.2/s; queue reaches
10 000 within minutes at moderate QPS → `ErrReplayQueueFull` → drops. NATS backend: two
goroutines (one per cluster) but each processes its batch sequentially (`nats_worker.go:213`);
same serialization.

`LatencyCircuitBreaker`: `recordReadSuccess` → `RecordLatency` → `RecordFailure` after slow reads
(`cql_client.go:2206-2218`, `latency_circuit_breaker.go:244-250`); after 3 it is "open". But
`ShouldFailover` is consulted only in the failure branch (`cql_client.go:2478-2484`); the read
strategy's `Select` does not consult the policy. **CONFIRMED (measured):** 10 slow successful
reads with `absoluteMax=1ms`, threshold 3, StickyRead preferred A → LCB open, failures=10,
**10/10 reads hit A, 0 hit B**. The LCB open state only pre-arms failover for the first *real*
error; a slow-but-successful cluster keeps serving every read.

### S5 — Replay queue saturation, NATS unreachable, poison messages

*Memory full:* `Enqueue` returns `ErrReplayQueueFull` non-blocking (`replay/memory.go:228-249`);
`enqueueReplayIfNeeded` counts `IncReplayDropped`, logs `Error`, calls `OnReplayDropped` and
emits `EventReplayDropped` (`cql_client.go:2066-2074, 737-746`), then `executeDualWrite` still
returns `nil` (`:2019`). So yes: success to the caller, entry lost, but loudly surfaced.

*NATS unreachable / publish timeout:* `NATSReplayer.Enqueue` publishes synchronously with
`PublishTimeout` 5 s (`replay/nats.go:458-461`) and the client wraps the caller ctx in
`context.WithoutCancel` (`cql_client.go:2049`), so a NATS outage adds up to 5 s to **every**
write that has a failing/degraded leg — i.e. every write during a cluster outage — and then the
entry is dropped (surfaced as above). *Stream full:* default `DiscardOld` (`nats.go:99, 374`)
makes JetStream evict the oldest replay entries with a successful publish → **silent loss** with
no Helix event/metric; `WithRejectNewOnLimit` converts it into the loud path. `MaxAge` 24 h expiry
is likewise silent. Documented in `docs/replay-system.md:96-101, 678-690`.

*Worker failure mid-drain (NATS):* explicit ack after success (`nats_worker.go:270`), so a crash
between execute and ack → redelivery after `AckWait` → duplicate execution; idempotent for
timestamped INSERT/UPDATE/DELETE, **not** for counters (documented,
`docs/replay-system.md:633-640`). Poison/corrupt: `Term` immediately on decode failure
(`nats.go:629-641`); persistent execution failure → `Term` after `MaxDeliver` + `OnDrop`.
Memory: `MaxAttempts` then `OnDrop`.

*Ordering vs newer direct writes:* every replay re-applies the original client timestamp
(`cql_client.go:1446, 1450`), so LWW resolves stale replays correctly. Gaps: (1) `ReplayPayload`
carries no consistency level (`types/types.go:176-195`); replay runs at the session default,
which may differ from the original query's `Consistency()`; (2) `USING TTL` in the query text is
re-applied relative to replay time, so a row replayed hours later expires later on the lagging
cluster than on the healthy one (not documented; only `docs/mirror.md:145` mentions TTL);
(3) a tombstone replayed after `gc_grace_seconds` may be purgeable immediately.

### S6 — Permanently dead session

Detector: `autoRefreshLoop` ticks every 30 s (`cql_client.go:213-228`); `maybeAutoRefresh`
requires `consecutiveFailures ≥ 10`, `now-lastSuccess ≥ 5 min`, `now-lastRefresh ≥ 1 min`
(`:266-275`). Throttle stamp is written before the refresher runs (`:280`), so a hung refresher
cannot double-fire. Refresher failing forever → one attempt per `MinRetryInterval` per cluster,
constant rate, no exponential backoff or jitter (acceptable at 1/min; `docs/session-refresh.md:76`).
`RefreshSession` swaps atomically and closes the old session immediately (`:1372-1387`) —
in-flight ops on the old session abort (documented). New session built but swap fails (client
closed) → new session closed (`:1373-1378`); no leak. Both clusters are refreshed sequentially
on the same tick with `RefreshTimeout` 30 s each, so B's refresh can lag A's by 30 s.

**Starvation under `AdaptiveDualWrite`.** `recordOpOutcomeAt` deliberately skips `ErrWriteAsync`
/`ErrWriteDropped` (`:495-502`). After 3 strikes every write to A is async and no longer counts;
the fire-and-forget goroutine's real failures only emit `IncWriteError` + `Warn`
(`adaptive_write.go:679-691`) and never reach `clusterStats`; the recovery probe's failures also
do not (`cql_client.go:405-409`); with StickyRead on B no read touches A. `consecutiveFailures`
freezes at 3 < 10. **CONFIRMED (measured):** 100 failing writes to A with tiny windows:
`ConcurrentDualWrite` → 8 refresher invocations; `AdaptiveDualWrite` → **0**. Consequences: A stays
degraded forever (probe keeps failing on the dead session), `DefaultExecuteFunc` replays against
the same dead session (`:1438`), every replay entry is dropped → S1-style permanent divergence,
with no refresh ever attempted. Works only if `FailureThreshold ≤ strikeThreshold`, or a read
strategy keeps probing A (`PrimaryOnlyRead` recovery timeout), or `ConcurrentDualWrite` is used.

### S7 — Topology drain via NATS KV

`watchLoop` (`topology/nats.go:193-240`): initial `fetchAndEmit`, then `kv.Watch`; on watch
error or a closed `Updates()` channel it falls into `pollLoop` **for the rest of its life**
(`:211-217, 228-231`) at 5 s (`topology/config.go:63-65`) — logged once. `fetchAndEmit` keeps
last-known drain state on any non-`ErrKeyNotFound` error (`:274-291`) and `processEntry` keeps it
on malformed JSON (`:310-317`) — fail-closed (fixed per CHANGELOG). So: NATS loss → drain state
frozen; NATS return → next poll (≤5 s) or watcher update converges. Operator setting both clusters
→ `drainA && drainB` → non-strict writes fail with `ErrBothClustersDraining`
(`cql_client.go:1769-1782`), strict → `DualClusterError`, reads proceed best-effort on the
selected cluster (`:2279-2288`). Only KV changes clear it. Startup with NATS down: initial fetch
fails (10 s timeout) → no drain state (fail-open until poll succeeds). The goroutine has no panic
recovery but no obvious panic sources; the jetstream KV watcher's own reconnect handling is
**SUSPECTED** adequate (ordered consumer), and the fallback covers channel closure anyway.
`sendUpdate` never blocks the watcher (overflow goroutine, `topology/local.go:171-182`).

### S8 — Process restart

Memory: documented volatile (`replay/memory.go:37-44`). `Close` → `Worker.Stop` → `drainAndDrop`
→ `OnDrop(reason="shutdown")` + `IncReplayDropped` for every queued item (`memory_worker.go:226-234`)
— measured on graceful shutdown only; SIGKILL leaves no trace. NATS: durable consumers
`helix-worker-{high|low}-{A|B}` (`nats.go:536, 569-579`) on a `WorkQueuePolicy` stream — any
number of processes compete safely; unacked messages redeliver after `AckWait`. Hazard: a batch
of up to 100 is fetched then executed serially with `ExecuteTimeout` 30 s each and no
`InProgress()` heartbeat; if `BatchSize × latency > AckWait` (30 s) the tail of the batch is
redelivered while still being processed → duplicate execution, and each redelivery counts toward
`MaxDeliver` (**CONFIRMED (code)**, `nats_worker.go:212-286`). With a slow recovering cluster
(1 s/replay) this is ~3× amplification of replay load onto the cluster you are trying to nurse.

### S9 — Recovery ordering and operator overrides

Reads can return to A before backfill via `PrimaryOnlyRead` recovery timeout or `CircuitBreaker`
half-open when a strategy still routes to A; `docs/auto-recovery.md:67-80` documents this and
prescribes `WithAllowedClusters`. There is no automatic gate; `MemoryReplayer.Len()` exists but
is not consulted by any routing code.

**Probe vs `ForceDegrade`.** `ForceDegrade` only sets `isDegraded` (`adaptive_write.go:1116-1152`);
`recoveryProbeLoop` gates on `IsDegraded` alone (`cql_client.go:392`) and the probe's success
feeds the same counter. **CONFIRMED (measured):** `ForceDegrade(A)` + healthy A + 5 ms probe
interval → `IsDegraded(A) == false` after 200 ms. The documented Phase-1 workflow
(`docs/auto-recovery.md:91-102`, "ForceDegrade(A)") is silently reverted by the default-on probe
within ~10 s in production unless `WithRecoveryProbeDisabled()` is set. `docs/auto-recovery.md`
does not mention this interaction. Fire-and-forget successes also credit recovery
(`adaptive_write.go:721-725`), so even with the probe off a healthy-but-backfilling A recovers
by itself. `ForceRecover` is not fought by anything (next slow writes simply re-degrade).
`AllowedClusters` freezes the read strategy but the failover policy still records health
(`cql_client.go:2206-2217`) — fine.

**CB × PrimaryOnlyRead probe.** When `recoveryTimeout > resetTimeout` (e.g. 5 min vs 30 s), the
probe read on a still-dead A hits `RecordFailure` → stale-gap reset to 1 → `ShouldFailover` false
→ error returned to caller and `OnFailure` never called (`cql_client.go:2478-2484`), so
`failoverTime` is not refreshed and the next read goes to A again. **CONFIRMED (measured):** 2
caller-visible read errors per probe cycle before the third failure re-trips CB and fails over.

### S10 — Goroutine/timer hygiene under prolonged degradation

Bounded: fire-and-forget semaphore 100 shared across clusters (`adaptive_write.go:460, 659-666`);
memory retry pool 100 (`memory_worker.go:20`); event outbox 64/policy; dispatcher buffer 128 with
exact drop accounting (`events.go:129-159`); probes/auto-refresh/topology are single goroutines
with tickers stopped on `Close` (`cql_client.go:1184-1229`, probes joined via WaitGroup).
`MemoryReplayer` pre-allocates two channels of full capacity (`memory.go:196-199`) — 2× struct
buffers, bounded; payload `Args` are retained until drop. `ConcurrentDualWrite` spawns one
goroutine per write that lives as long as the slower leg — unbounded only if callers omit ctx
deadlines against a black-holed cluster. Metrics are labelled by cluster only
(`contrib/metrics/vm`) — no cardinality growth. `time.After` in poll loops is fine at 100 ms.
No leaks found.

---

## (c) Findings ranked by severity

### High

**H1. Replay is a seconds-long retry buffer, not an outage backlog — a hard-down cluster loses
every replayed write.** CONFIRMED (measured, memory) / CONFIRMED (code, NATS).
Evidence: `memory_worker.go:135-161` + `worker.go:421-434` (5 attempts, 1.5 s total, then
`OnDrop`); `:118-123` (pool saturated → drop instantly); `nats_worker.go:256` (`Nak` with no
delay) + `:239-253` (`Term` after `MaxDeliver`). Scratch run: 500/500 dropped by t=1.5 s.
Failure scenario: S1/S2/S6 — any outage longer than ~2 s (memory) or a few fetch cycles (NATS)
converts "eventually consistent" into "permanently divergent"; the only signal is
`IncReplayDropped`/`OnDrop`, and `docs/auto-recovery.md` still talks about draining a backlog
when the cluster returns. The CHANGELOG records the infinite→bounded change as a behaviour change
but the defaults were not sized for outages.
Direction: gate replay execution on target-cluster health (pause the consumer for cluster X while
`IsDegraded(X)`/probe failing, or treat "cluster unreachable" errors as *not* consuming an
attempt); for NATS use `NakWithDelay` with backoff and consider `MaxDeliver=-1` with
`MaxAge` as the real bound; for memory keep the payload parked (not retried) while the target is
down; document the effective survival window per backend.

**H2. Both clusters degraded ⇒ writes acknowledged with zero synchronous copies.** CONFIRMED
(measured). Evidence: `cql_client.go:1949-1952, 2000-2004, 2013-2019`; `adaptive_write.go:
583-605`. After three failed writes every write returns `nil` backed only by the replay queue —
which per H1 drops it 1.5 s later — or by nothing when no Replayer is configured (construction-
time `Warn` only, `:871-873`). Failure scenario: S2 — minutes of "successful" writes that exist
nowhere. Direction: require at least one synchronous acknowledgement unless the replayer is
durable (option, default on): when both legs return `ErrWriteAsync`/`ErrWriteDropped`, return
`DualClusterError` (or a new `ErrNoSyncAck`) instead of `nil`; when no Replayer is configured,
any async-only result must be an error.

**H3. Auto-refresh detector is starved by `AdaptiveDualWrite` (default production stack).**
CONFIRMED (measured: 8 refreshes vs 0). Evidence: `cql_client.go:495-502` skip list;
`adaptive_write.go:679-691` background failures never reach `clusterStats`; probe failures
`cql_client.go:405-409` likewise. Failure scenario: S6 — dead session on A is never refreshed;
A stays degraded and every replay against it drops (H1). Direction: record fire-and-forget
outcomes and recovery-probe outcomes into `clusterStats` (a strategy→client callback, or have the
client wrap `writeA/writeB` so the *inner* result is observed regardless of the strategy's
sentinel), and/or let the probe loop count consecutive probe failures as the refresh trigger.

**H4. Recovery probe silently reverts operator `ForceDegrade`.** CONFIRMED (measured).
Evidence: `cql_client.go:392-399`; `adaptive_write.go:1116-1152, 1288-1290`. Failure scenario:
S9 — the documented isolation workflow (`docs/auto-recovery.md:91-102`) is undone in ~10 s;
writes start blocking on a backfilling cluster. Direction: add a "manual" latch in
`clusterWriteState` set by `ForceDegrade` and cleared only by `ForceRecover`/`Reset`; the probe
and `recordFast` must not clear it (skip probing while latched); document the interaction.

### Medium

**M1. `LatencyCircuitBreaker` never moves traffic off a slow-but-successful cluster.** CONFIRMED
(measured: 10/10 reads to A after trip). Evidence: `cql_client.go:2337-2347` (success path never
consults `ShouldFailover`), `:2478-2484`. The docs (`docs/strategy-policy.md:508-580`) imply slow
reads are "treated as degraded". Direction: consult `ShouldFailover(selected)` in
`runPrimaryRead` before issuing the read when the policy is open (route to the alternative), or
document clearly that LCB only pre-arms failover.

**M2. No per-cluster write timeout; `SyncDualWrite`/`ConcurrentDualWrite` block for the slow
leg and `SyncDualWrite` turns one slow leg into `DualClusterError` without replay.** CONFIRMED
(code). Evidence: `write_strategy.go:60-76, 158-178`. Direction: optional per-leg timeout
(`WithClusterWriteTimeout`) applied inside `executeDualWrite`'s `writeA/writeB` closures so a slow
leg becomes a replayable error while the fast leg's ack stands.

**M3. Probe has no latency/stability criterion → degrade/recover oscillation on a write-slow
cluster (S4) and per-blip flapping (S3).** CONFIRMED (code). Evidence: `cql_client.go:398-399`;
`adaptive_write.go:987-1023` (pure counters). Direction: require probe latency ≤ `absoluteMax`
(or ≤ sibling+delta) to credit; add a minimum degraded dwell time / exponential re-degrade
backoff (hysteresis) before the strategy may recover; emit a "flapping" event/metric.

**M4. Replay worker throughput is serialized (one inline attempt at a time per goroutine).**
CONFIRMED (code). Evidence: `memory_worker.go:43-45, 77-87`; `nats_worker.go:212-228`. On a
recovering cluster with elevated latency the queue cannot drain and saturates (S4). Direction:
bounded concurrent execution per cluster (worker pool of N) with per-cluster queues so a slow A
does not stall B.

**M5. NATS `BatchSize × latency > AckWait` ⇒ redelivery storm and `MaxDeliver` burn while a
message is still being processed.** CONFIRMED (code). Evidence: `nats_worker.go:212-286` (no
`InProgress()`), defaults `nats.go:102-105`, `worker.go:109`. Direction: call `msg.InProgress()`
periodically or before each execute in the batch, or cap `BatchSize` by `AckWait/ExecuteTimeout`,
or fetch smaller batches when the previous batch took > AckWait/2.

**M6. Silent replay loss paths on NATS: `DiscardOld` eviction and `MaxAge` expiry produce no
Helix signal.** CONFIRMED (code) — documented but easy to miss (`docs/replay-system.md:678-690`).
Direction: default to `DiscardNew` (loud) or expose stream `discarded`/`purged` counters via the
metrics collector; at minimum emit a startup `Warn` when `DiscardOld` is in effect.

**M7. NATS outage adds up to `PublishTimeout` (5 s) synchronously to every write with a failed
leg.** CONFIRMED (code). Evidence: `nats.go:458-461`; `cql_client.go:2049`. Direction: bound the
enqueue by the caller's deadline *or* a short client-side enqueue budget, spill to a small
in-memory side buffer with a background publisher, and surface publish latency as a metric.

**M8. Replay does not preserve the original consistency level or TTL semantics.** CONFIRMED
(code). Evidence: `types/types.go:176-195`; `cql_client.go:1436-1453`. Direction: capture
consistency/serial consistency in `ReplayPayload` (and the mirror payload) and apply on replay;
document TTL drift and that TTL'd rows may outlive their sibling on a replayed cluster.

### Low

**L1. `CircuitBreaker` state gauge for the failed cluster stays "open" forever once reads stop
touching it (StickyRead).** CONFIRMED (code): `failover_policy.go:501-520` only closes inside
`RecordFailure`. Direction: evaluate the reset timeout lazily in `ShouldFailover`/a periodic sweep
and report the transition, or feed `RecordSuccess` from the recovery probe.

**L2. CB × `PrimaryOnlyRead` recovery probe costs 2 caller-visible errors per cycle when
`recoveryTimeout > resetTimeout`.** CONFIRMED (measured). Direction: when `ShouldFailover` denies
failover, still inform the strategy (`OnFailure`) so `PrimaryOnlyRead` can restart its timer; or
let the half-open probe bypass the threshold for exactly one request.

**L3. Failed recovery probes are only `Debug`-logged.** CONFIRMED (code) `cql_client.go:408`. A
6-hour degraded cluster produces 10 800 probe failures with no `Warn`. Direction: log at `Warn`
on first failure and on power-of-two counts (the pattern already used for override errors).

**L4. `StickyRead` has no automatic way home; the CB reset never re-tests A.** Documented
(`read_strategy.go:22-28`, `docs/auto-recovery.md`). Direction: optional `WithStickyReadProbeBack
(d)` that, when preferred was swapped by failover, re-probes the original after `d` only if
the failover policy reports it closed — and (ideally) only if the replay backlog for it is empty.

**L5. Topology watcher never returns from poll mode to watch mode.** CONFIRMED (code)
`topology/nats.go:211-231`. Poll at 5 s is adequate; direction: periodic re-attempt of `Watch`.

**L6. Simulation coverage gap.** The `complete-failure` (15 s outage) and `replay-saturation`
scenarios track `nil`/`ErrWriteAsync`-acked keys and assert presence on both clusters
(`test/simulation/simulation.go:479-486`, `workload/tracker.go:156-186`) with the default worker
(`cmd/main.go:226`, `simulation.go:343`). Given H1 they should fail; either they do today or the
chaos path masks it. Not run here (docker). See open questions.

---

## (d) What is already solid

- **Every replay-drop path on the write side is loud and exact**: `IncReplayDropped`, `Error`
  log, `OnReplayDropped`, `EventReplayDropped` (`cql_client.go:2066-2074, 1871-1878`).
- **Client-generated timestamps on every write and replay** make duplicates and reordering
  harmless for regular tables (`cql_client.go:3145-3151, 1446-1451`); counters are documented as
  unsafe.
- **Enqueue is decoupled from caller cancellation** (`context.WithoutCancel`), so a cancelled
  request still leaves a replay record.
- **Fail-closed drain state** in the NATS topology watcher, non-blocking update delivery, and an
  independent ctx exit in `watchTopology`.
- **Bounded everything**: fire-and-forget semaphore, retry pool, outbox, dispatcher, with drop
  counters instead of blocking or unbounded growth; `Close` joins probe goroutines before closing
  sessions.
- **Auto-refresh is conservative by design** (5 min window, throttle stamped before the call,
  `lastSuccess` left stale after refresh so a still-dead replacement re-qualifies honestly).
- **Circuit-breaker and adaptive-write transitions are race-safe** (per-cluster mutex, sequence-
  checked gauge writes, events last, panic-safe emitters).
- **StickyRead cooldown still returns the alternative for the current request**, so reads do not
  fail outright during cooldown.
- **`docs/auto-recovery.md` is honest** about the backlog/stale-read problem and prescribes the
  `AllowedClusters` workflow.

---

## (e) Open questions

1. Does the simulation `complete-failure` scenario pass today? If yes, what masks the ~1.5 s
   replay lifetime (H1) — chaos session semantics, or verification timing? A docker run of
   `-profile quick` with `OnDrop` instrumented would settle it.
2. Is "replay = short retry buffer" the intended contract post-`MaxAttempts` change, or should the
   defaults be re-sized (e.g. `MaxAttempts` unbounded with `MaxRetryDelay` 30 s and a target-
   health gate)? The docs still describe backlog draining after prolonged outages.
3. Is `nil` on both-async (H2) deliberate for the NATS-durable case? If so, should it be
   conditional on `Replayer` durability (an interface marker) rather than unconditional?
4. Should the recovery probe be allowed to override any manual state (H4), or should
   `ForceDegrade` be sticky? The docs currently prescribe `ForceDegrade` as the isolation step.
5. Does the jetstream KV watcher (ordered consumer) transparently survive a NATS reconnect, or
   does `Updates()` close (SUSPECTED)? Either way the poll fallback covers it, but watch-mode
   latency is lost permanently.
6. Is there an intended per-cluster write deadline (M2), or is the guidance "always pass a ctx
   deadline"? If the latter, `SyncDualWrite`'s no-replay `DualClusterError` on a slow leg should
   be documented as a data-consistency hazard.

---

### Reproduction notes (scratch tests, not committed)

Scratch module at `scratchpad/recov` with a 60-line `cql.Session` mock:
`TestProbeUndoesForceDegrade` (H4), `TestAutoRefreshStarvedByAdaptive` (H3),
`TestBothDegradedAckWithoutReplayer` (H2), `TestLatencyCBDoesNotReroute` (M1),
`TestCBPrimaryOnlyProbeCost` (L2), `TestMemoryReplayDropsDuringOutage` (H1). All ran green
with `go test` against the working tree; outputs quoted inline above.
