# Helix — Fresh Architecture Review (Part 01: Overall Architecture)

Reviewed at commit `7e93857` (main), 2026-09-02. Read-only review of source under
`/home/arlo/projects/helix`. Prior review documents in `./tmp` were deliberately ignored.
Every finding is tagged **CONFIRMED** (I read the code path end-to-end) or
**SUSPECTED** (inferred; not exercised or not fully traced).

---

## (a) Architecture summary

### Package map and dependency graph (from `go list -f '{{.Imports}}'`)

```
                          +--------------------+
                          |  types (leaf)      |  ClusterID, errors, ReplayPayload,
                          |  imports: stdlib   |  MetricsCollector, Logger,
                          +---------^----------+  ClusterEvent(+Emitter), 7 optional
                                    |             metrics extension interfaces
     +------------+------------+----+----+-------------+-------------+
     |            |            |         |             |             |
+----+-----+ +----+----+ +-----+----+ +--+---------+ +-+----------+ +--+---------+
| adapter/ | | policy  | | replay   | | internal/  | | contrib/   | | mirror     |
| cql      | |         | |          | | logging    | | metrics/vm | |            |
| (+v1,v2) | | read/   | | Memory,  | | metrics    | | Prometheus | | Engine     |
| Session, | | write/  | | NATS,    | | typeutil   | | -style     | | (imports   |
| Query,   | | failover| | Worker   | |            | | collector  | |  replay)   |
| Batch,   | | strate- | | (msgp)   | |            | |            | |            |
| Iter     | | gies    | |          | |            | |            | |            |
+----+-----+ +---------+ +----^-----+ +-----^------+ +------------+ +-----^------+
     |                        |              |                            |
     |    (policy is NOT      |              |                            |
     |     imported by root;  |              |                            |
     |     structural typing) |              |                            |
     +-----------+------------+--------------+----------------------------+
                 |
        +--------v-----------------------------------------------------------+
        |  helix (root)                                                      |
        |  strategy.go   : ReadStrategy, WriteStrategy, FailoverPolicy,      |
        |                  Replayer, ReplayWorker, TopologyWatcher/Operator, |
        |                  StrictWriter, LatencyRecorder, AllowedClustersFunc|
        |  config.go     : ClientConfig + 30 WithXxx options                 |
        |  cql_client.go : CQLClient, read/write orchestration, auto-refresh,|
        |                  recovery probe, topology watcher, query/batch/iter|
        |  cql_session.go: CQLSession/Query/Batch/Iter public interfaces     |
        |  events.go     : eventDispatcher                                   |
        |  mirror_dispatch.go : mirror wiring                                |
        +--------^-----------------------------------------------------------+
                 |
        +--------+---------+
        | topology         |   imports ROOT (for helix.TopologyUpdate /
        | Local, NATS(KV)  |   helix.TopologyWatcher assertions)
        +------------------+
```

Key structural facts (CONFIRMED):

- `types` is a genuine leaf: it imports only `errors`, `fmt`, `regexp`, `time`. The
  rule stated in `.agents/rules/100-overview.md` ("All shared interfaces ... live
  there") is only half true: the *strategy* interfaces (`ReadStrategy`,
  `WriteStrategy`, `FailoverPolicy`, `Replayer`, `ReplayWorker`, `TopologyWatcher`,
  `StrictWriter`, `LatencyRecorder`) live in the **root** package (`strategy.go`),
  not in `types`. `policy/` satisfies them by structural typing without importing
  root; `topology/` imports root and therefore root can never import `topology`.
- Root imports `adapter/cql`, `mirror`, `replay`, `types`, `internal/*`. Root does
  **not** import `policy`. All policy-specific behaviour is reached via type
  assertions to small, mostly unexported interfaces (see finding F-06).
- `mirror` imports `replay` (shares `replay.ExecuteFunc`); root wires the two.

### Lifecycle of a write: `client.Query(stmt, args...).Exec()`

```
cqlQuery.Exec() -> ExecContext(ctx)                          cql_client.go:3115-3172
  |- Strict()+Mirror() combination rejected (ErrStrictMirrorUnsupported)
  |- defer: if err==nil && mirror -> dispatchMirrorQuery (mirror_dispatch.go:346)
  |- single-cluster fast path: sessionA.Query().WithTimestamp(ts).ExecContext
  |     -> recordOpOutcome(ClusterA)                          (auto-refresh stats)
  '- executeWriteWithReplay(ctx, writeContext, writeFunc)     cql_client.go:1745
       |- closed? -> ErrSessionClosed
       |- drainA && drainB -> ErrBothClustersDraining (or DualClusterError if strict)
       |- drainA XOR drainB -> executeWriteWithDrain          cql_client.go:1795
       |     (bypasses WriteStrategy entirely; writes healthy leg, enqueues replay
       |      for the draining leg, or returns PartialWriteError if strict)
       '- executeDualWrite                                    cql_client.go:1893
            |- strict -> executeStrictDualWrite               cql_client.go:2086
            |     WriteStrategy.(StrictWriter).ExecuteStrict or inline concurrent
            |     -> PartialWriteError / DualClusterError, NO replay
            |- WriteStrategy.Execute(ctx, writeA, writeB)  [policy/*]
            |     nil strategy -> inline concurrent (writeA inline, writeB goroutine)
            |     AdaptiveDualWrite: degraded leg -> fire-and-forget goroutine,
            |       returns ErrWriteAsync / ErrWriteDropped
            |- classify per leg: nil | ErrWriteAsync | ErrWriteDropped | real error
            |- metrics (IncWriteTotal/Error/Async/Dropped, ObserveWriteDuration)
            |- recordOpOutcomeAt(A), recordOpOutcomeAt(B)  (auto-refresh stats)
            |- both real errors -> DualClusterError
            '- any non-nil leg -> enqueueReplayIfNeeded (if Replayer != nil)
                 Replayer.Enqueue(context.WithoutCancel(ctx), payload)
                 on enqueue failure: IncReplayDropped + OnReplayDropped + event
                 -> return nil (partial success is success to the caller)
```

### Lifecycle of a read: `client.Query(...).Scan()` / `.Iter()`

```
cqlQuery.Scan -> ScanContext                                  cql_client.go:3178
  |- resolveReadOptions: per-query FallbackRead > ctx WithFallbackRead > client default
  '- executeRead(ctx, opts, readFunc)                         cql_client.go:2327
       |- runPrimaryRead                                      cql_client.go:2249
       |    |- closed? -> ErrSessionClosed
       |    |- resolveReadTarget                              cql_client.go:1548
       |    |    AllowedClustersFunc (panic-recovered) -> override snapshot with
       |    |    drain filtering, fail-closed; else ReadStrategy.Select(ctx)
       |    |- drain-aware re-selection (non-override, non-paged only)
       |    '- readFunc(session) + IncReadTotal + ObserveReadDuration
       |- err == nil -> recordReadSuccess (ReadStrategy.OnSuccess unless override;
       |               FailoverPolicy.RecordLatency if LatencyRecorder else RecordSuccess)
       |- ErrNotFound / ErrRowLimitExceeded -> not a health signal;
       |    ErrNotFound + fallbackRead -> executeFallbackRead (one-shot on alt cluster,
       |    suppresses alt errors to ErrNotFound by default; slice paths tune this via
       |    fallbackReadOptions predicates)
       '- real error -> IncReadError + recordOpOutcome, then
            override active -> executeOverrideFailover (FailoverPolicy gates,
                               fallback from override list)
            else -> executeNormalFailover: FailoverPolicy.RecordFailure +
                    ShouldFailover, ReadStrategy.OnFailure -> alternative, drain
                    veto, tryFallbackCluster (IncFailoverTotal, EventFailover,
                    DualClusterError if alt also fails)

cqlQuery.Iter -> IterContext                                  cql_client.go:3204
  |- resolveReadTarget(ctx, readOptions{})   (override+drain filter only in override
  |    branch; NO drain-aware re-selection in the normal branch)
  '- cqlIter{...}; Close() -> recordOpOutcome + ReadStrategy.OnSuccess. No failover.

SliceMap/SliceScan -> executeRead or executeReadNoFailover with preserveSelectedCluster
  when PageState is set; empty drain synthesised as ErrNotFound to reuse fallback path.
CAS (ScanCAS, batch ExecCAS) -> selectClusterForCAS (ReadStrategy.Select; ignores
  override and drain), single cluster, no failover, no health recording.
```

### Background goroutines owned by a client (dual-cluster, fully configured)

| Goroutine | Started | Stopped in `Close()` | Joined? |
|---|---|---|---|
| `watchTopology` | `NewCQLClient` | `topologyClose()` (ctx cancel) | **No** |
| replay `Worker` (memory: 1 + retry pool; NATS: 2) | `ReplayWorker.Start()` | `ReplayWorker.Stop()` | Yes (`wg.Wait`) |
| mirror `Engine` workers (default 4) | `setupMirror` | `stopMirrorComponents` | Yes |
| mirror replay worker | `setupMirror` | `stopMirrorComponents` | Yes |
| `eventDispatcher.run` | `startEventDelivery` | `events.stop()` | Yes (+ spin on `emitting`) |
| `autoRefreshLoop` | after event delivery | `autoRefreshClose()` (ctx cancel) | **No** |
| `recoveryProbeLoop` ×2 | `startRecoveryProbes` | `recoveryProbeClose()` | Yes (`recoveryProbeWG`) |
| AdaptiveDualWrite fire-and-forget writes (≤100) | per degraded write | never | No (documented) |
| default dual-write leg B goroutine | per write | n/a | Yes (`wg.Wait`) |

Close order (cql_client.go:1184-1229): topology cancel → auto-refresh cancel → mirror
engine + mirror worker stop → replay worker stop → probes cancel+join → event
dispatcher stop → close session A, close session B.

---

## (b) Strengths

1. **Clean leaf/adapter layering.** `types` is a true leaf; drivers are isolated
   behind `adapter/cql.Session`; root never imports a gocql driver. Session refresh
   is delegated to a caller factory precisely because of this (`config.go:212-227`).
2. **Lock-free hot paths with careful state sequencing.** `atomic.Pointer[sessionHolder]`
   for live sessions; `clusterStats` all-atomic; the circuit breaker and adaptive
   strategy use a per-cluster state mutex plus a *separate* report mutex and a
   transition sequence so a superseded transition never overwrites a newer gauge
   (`policy/failover_policy.go:676-725`, `policy/adaptive_write.go:857-892`). This
   is unusually disciplined.
3. **Event delivery never holds policy locks.** `eventOutbox` (policy) → non-blocking
   `eventDispatcher` (root) with exact drop accounting; construction order ensures
   no goroutine leaks on constructor error paths (`cql_client.go:875-943`).
4. **Explicit sentinel taxonomy.** `ErrWriteAsync`, `ErrWriteDropped`,
   `ErrClusterDegraded`, `ErrClusterDraining`, `ErrNotFound`, `ErrRowLimitExceeded`
   are consistently excluded from health accounting in root and policy
   (`cql_client.go:449-514`, `policy/adaptive_write.go:779-784`).
5. **Fail-closed operator override.** `AllowedClustersFunc` panics, unknown IDs and
   drain conflicts all fail the read rather than silently re-routing
   (`cql_client.go:1527-1692`), with log storm protection.
6. **Panic containment on every foreign callback**: write legs, probes, override
   function, event handlers, emitters.
7. **Replay design.** Client-generated timestamps make replay idempotent for
   overwrites; NATS subjects are partitioned by priority and cluster; memory worker
   runs retries off the dequeue loop with a bounded pool; corrupt messages are
   `Term`'d rather than redelivered.
8. **Test pyramid is real.** 339 root unit tests, 150 policy, 101 replay, 134
   integration (testcontainers), 52 e2e (container kill, network partition,
   cascading failure), and a simulation harness with a chaos `cql.Session` wrapper
   and a `WriteTracker` consistency oracle. `make test` runs `-race`.

---

## (c) Findings (ranked)

Severity key: **High** = correctness/availability impact in realistic operation or a
structural problem that will keep producing bugs; **Medium** = surprising behaviour,
observability gap, or maintainability cost; **Low** = polish/doc.

### F-01 [High] CONFIRMED — Caller-side context cancellation is treated as cluster failure on non-slice reads and on writes

**Evidence.** `executeRead` (`cql_client.go:2360-2376`) classifies any error that is
not `ErrNotFound`/`ErrRowLimitExceeded` as a real cluster error: it calls
`IncReadError`, `recordOpOutcome` (auto-refresh `consecutiveFailures++`), then
`executeNormalFailover` → `FailoverPolicy.RecordFailure(selected)` and
`ShouldFailover`. With the context already cancelled, `tryFallbackCluster`
(`cql_client.go:2516-2571`) immediately fails on the alternative too →
`RecordFailure(fallback)` + `IncReadError(fallback)` + `EventFailover` +
`DualClusterError`. `isCtxErr` exists (`cql_client.go:2579`) but is wired only into
the slice-read `fallbackReadOptions` (`cql_client.go:3302-3306`, `3369-3379`).
On the write side `executeDualWrite` (`cql_client.go:1999-2004`) treats
`context.DeadlineExceeded` on both legs as `DualClusterError` and
`recordOpOutcomeAt` counts a failure for both clusters;
`AdaptiveDualWrite.handleErrors` (`policy/adaptive_write.go:768-775`) records a
slow-strike for each leg.

**Why it matters.** One request with a short deadline (or an HTTP client that
disconnects) counts as *two* cluster failures in the circuit breaker and in the
auto-refresh detector. Three consecutive short-deadline requests degrade **both**
clusters in `AdaptiveDualWrite` (strike threshold 3) into fire-and-forget mode, and
ten of them satisfy the auto-refresh `FailureThreshold` (10). The authors clearly
recognised the problem — the slice-path predicates say "long-running drains see a
disproportionate rate of caller-driven cancellation that would otherwise poison the
failover-policy view of cluster health" — but the fix was applied to only one of
the four read paths and to neither write path.

**Direction.** Add `isCtxErr(err)` to the "not a health signal" branch of
`recordOpOutcomeAt`, `executeRead`, `executeReadNoFailover`, `tryFallbackCluster`,
and to `isSkippedErr` in `policy/adaptive_write.go` (or gate `handleErrors` on
`ctx.Err() == nil`). Skip failover entirely when `ctx.Err() != nil` (there is
nothing to gain by retrying with a dead context). This is a small, local change
consistent with the "minimum change" principle and with the existing slice-path
precedent. A deliberate exception may be warranted for `DeadlineExceeded` in the
*latency*-aware policies if the deadline is the cluster SLO — document either way.

### F-02 [High] CONFIRMED — Five independent per-cluster health state machines with no shared view

| State machine | Owner | Fed by | Consumed by |
|---|---|---|---|
| `CircuitBreaker.failures/tripped` | policy | reads only (`RecordFailure/Success/Latency`) | read failover gating |
| `AdaptiveDualWrite.isDegraded/strikes` | policy | writes only + recovery probe | write leg mode, `ExecuteStrict` |
| `clusterStats.consecutiveFailures/lastSuccess` | root | reads **and** writes | auto-refresh detector only |
| `drainA/drainB` | root | topology watcher | read re-route, write skip |
| `StickyRead.preferred/cooldown`, `PrimaryOnlyRead.failedOver` | policy | read failures | read selection |

**Evidence.** Writes never call `FailoverPolicy.RecordFailure` (grep: all
`RecordFailure` call sites are in the read path, `cql_client.go:2437,2454,2479,2563,2721`).
Reads never consult `AdaptiveDualWrite.IsDegraded`. The recovery probe
(`cql_client.go:355-412`) credits only `probeReporter.RecordProbeSuccess`, i.e. the
write strategy; it does not close a tripped breaker or reset `clusterStats`. The
auto-refresh detector fires `RefreshSession`, which resets `clusterStats` but leaves
the breaker open and the adaptive state degraded.

**Why it matters.** Operators get contradictory signals: a cluster can be
`circuit_breaker_state=2` (reads avoid it) while writes are still synchronous to it,
and `write_degraded=1` while sticky reads keep hitting it. Each new feature
(auto-refresh, recovery probe, strict, drain) has added another state machine and
another set of exclusions rather than composing on one. This is the root cause of
the `isCtxErr` inconsistency in F-01 and of the strict/drain duplication in F-05.

**Direction (staged, minimum-change first).**
1. Document the matrix above in `docs/strategy-policy.md` so the independence is a
   stated contract, not an accident.
2. Introduce an internal `clusterHealth` hub in root that owns `clusterStats` and
   `drain*` and exposes `observe(cluster, op, err, latency)`; route every
   `recordOpOutcome`/`RecordFailure`/`RecordLatency` call through it. No public API
   change; policies remain the *decision* layer.
3. Only later, and opt-in: let the hub forward breaker-open → `ForceDegrade` and
   probe-success → `RecordSuccess` so recovery signals are shared.

### F-03 [High] CONFIRMED — `cql_client.go` is a god file; `CQLClient` entangles eight responsibilities

**Evidence.** 3,858 lines; 123 `c.config.` accesses; the file contains: client
struct + lifecycle (65-230, 1184-1409), auto-refresh detector (207-336), recovery
probe (338-426), health bookkeeping (446-514), DI wiring by type assertion
(519-754), constructor (756-944), topology watcher (946-1023), read-target/override
resolver (1508-1705), three write orchestrators (1737-2171), read orchestrators and
fallback (2173-2744), slice drain helpers (2746-2933), `cqlQuery` (2935-3434),
`cqlBatch` (3436-3718), `cqlIter`/`errorIter`/scanner (3720-3825). `config.go`
(1,158 lines) mixes user options with runtime registry fields (`MirrorEngine`,
`MirrorReplayWorker`, `events`, auto-created `Replayer`).

**Why it matters.** Every behavioural change must be reasoned about against all
eight concerns in one 4k-line file; review diffs are hard to scope; the read
pipeline alone has six entry points (`executeRead`, `executeReadNoFailover`,
`IterContext`, CAS, `executeFallbackRead`, slice) with subtly different
classification tables (see F-04, F-01).

**Direction — respecting "minimum change".** Two tiers:

*Tier 1 (pure file moves, zero behaviour or API change, ~1 PR):*
`session_lifecycle.go` (holders, Swap/Refresh, `clusterStats`, auto-refresh),
`recovery_probe.go`, `wiring.go` (auto-inject*, event-kind reachability),
`read_path.go`, `write_path.go`, `slice_read.go`, `query.go`, `batch.go`, `iter.go`.
The package stays `helix`; nothing exported moves.

*Tier 2 (small internal types, still no public API change):*
- `readRouter` struct owning `resolveReadTarget`, drain re-selection and the
  failover branch, with one classification function
  `classifyReadErr(err) (kind)` returning {ok, notFound, rowLimit, ctxErr,
  clusterErr}. All six read entry points call it → F-01/F-04 become impossible to
  regress.
- `writeOrchestrator` where drain is expressed as a per-leg sentinel
  (`ErrClusterDraining`) fed through the same classification switch as
  `ErrClusterDegraded`, deleting `executeWriteWithDrain` (F-05).
- Split `ClientConfig` into the user-facing options struct and an unexported
  `runtime` struct (engine, workers, dispatcher) so `Config()` returns something
  immutable (F-12).

Do **not** split into sub-packages: `topology` already imports root, and the
strategy interfaces live in root, so package extraction would ripple into public
import paths for little gain.

### F-04 [Medium] CONFIRMED — `Iter()`, batch `IterContext()`, and CAS ignore drain state (docs say reads are routed away from draining clusters)

**Evidence.** Drain-aware re-selection lives only in `runPrimaryRead`
(`cql_client.go:2279-2289`). `IterContext` (`3204-3224`) and batch `IterContext`
(`3610-3641`) call `resolveReadTarget` whose normal (non-override) branch returns
`normalSelect` with no drain check (`1548-1568`, `1695-1700`). `selectClusterForCAS`
(`1501-1506`) has none either. `NewCQLClient` doc (`cql_client.go:768-770`) and
`IsDraining` doc (`1005-1007`) state "Reads are failed over away from draining
clusters". No unit or integration test pairs `Iter` with `SetDrain` (grep of
`cql_client_test.go` and `test/integration/topology_integration_test.go`).

**Why it matters.** During a planned drain, streaming reads and LWTs continue to
hit the cluster being taken down. Iterators are the common path for range scans.

**Direction.** Move the drain re-selection into `resolveReadTarget` (it already has
`readOptions`; honour `preserveSelectedCluster` there), so every entry point gets it.
Decide explicitly for CAS (arguably should also avoid a draining cluster) and
document. Add a test per entry point.

### F-05 [Medium] CONFIRMED — Drain-mode writes bypass the `WriteStrategy` and its metrics

**Evidence.** `executeWriteWithDrain` (`cql_client.go:1795-1882`) writes the healthy
leg directly; `WriteStrategy.Execute` is never called; `AdaptiveDualWrite` sees no
latency sample; no `IncWriteTotal`/`IncWriteSkipped` is emitted for the draining
cluster (contrast with the strict path `2129-2147` which emits `IncWriteSkipped`).
The strict semantics are re-implemented inline three times in this function.

**Direction.** Treat drain as a per-leg pre-check that returns
`types.ErrClusterDraining` from `writeA`/`writeB` closures and let
`executeDualWrite`/`executeStrictDualWrite` classify it exactly like
`ErrClusterDegraded` (both already skip it in health accounting). Deletes ~90
lines and one code path.

### F-06 [Medium] CONFIRMED — Optional-interface duck typing is the primary extension mechanism, and half of it is unexported

**Evidence.** Root type-asserts against: `probeReporter` (unexported,
`cql_client.go:342`), `eventAware` (unexported, `595`), `metricsAware`,
`loggerAware` (function-local, `529-535`), `StrictWriter`, `LatencyRecorder`
(exported), `types.ClusterNamer`, plus seven optional metrics extension interfaces
(`SessionRefreshMetrics`, `RecoveryProbeMetrics`, `StrictMetrics`, `MirrorMetrics`,
`MirrorReplayMetrics`, `ClusterEventMetrics`, `AdaptiveWriteMetrics`). `policy`
contains no `var _ helix.WriteStrategy = ...` assertions (only `examples/` and
`test/testutil` do), so a signature drift in policy is caught only by root tests.
`mirror_dispatch.go:236-243` additionally switches on concrete `*replay.MemoryReplayer`
/ `*replay.NATSReplayer` types.

**Why it matters.** A custom `WriteStrategy` author cannot discover from godoc that
implementing `IsDegraded`+`RecordProbeSuccess` enables the recovery probe, or that
`SetEventEmitter` enables events; the `WithOnClusterEvent` docs mention it only
indirectly. Each optional interface is another silent no-op path.

**Direction.** Export the three capability interfaces (`ProbeReporter`,
`EventEmitterSetter`, `Instrumentable`) in `strategy.go` next to `StrictWriter`, and
add compile-time assertions in `policy/` for every built-in. Consider collapsing the
seven metrics extension interfaces into one `types.ExtendedMetricsCollector` for
v2 — the current split makes `contrib/metrics/vm` implement 40 methods across 8
interfaces.

### F-07 [Medium] CONFIRMED — `LatencyRecorder` silently replaces `RecordSuccess`

**Evidence.** `recordReadSuccess` (`cql_client.go:2206-2218`): if the policy
implements `LatencyRecorder`, only `RecordLatency` is called; `RecordSuccess` is
**not**. `LatencyCircuitBreaker.RecordLatency` happens to call `RecordSuccess`/
`RecordFailure` internally (`policy/latency_circuit_breaker.go:244-250`), so the
built-in works, but the `FailoverPolicy` contract ("RecordSuccess records a success
to reset failure counters") is violated for any custom `LatencyRecorder`.

**Direction.** Call both (`RecordSuccess` then `RecordLatency`), or document on
`LatencyRecorder` that it *replaces* `RecordSuccess`. The former is safer and costs
one extra atomic store for the built-in.

### F-08 [Medium] CONFIRMED — `Close()` does not join the auto-refresh goroutine; `Close` vs `RefreshSession` can leak a session

**Evidence.** `Close` cancels `autoRefreshCtx` (`cql_client.go:1195-1197`) but has
no `WaitGroup` for `autoRefreshLoop`. `maybeAutoRefresh` → `RefreshSession` →
`SwapSession` uses check-then-act on `closed` (`1268`, `1339`) with no lock.
Interleaving: `SwapSession` passes the `closed` check → `Close` sets `closed`,
loads session A and closes it → `SwapSession` swaps in the freshly built session →
`RefreshSession` closes `old` (double close, tolerated) → the new session is never
closed. The `Close` doc calls concurrent Swap/Close "undefined", but here the racing
caller is Helix's own goroutine, not the user. The topology watcher is likewise
unjoined (harmless: it only flips atomics and emits counted-dropped events).

**Direction.** Track `autoRefreshLoop` (and `watchTopology`) in a `sync.WaitGroup`
and wait before closing sessions — the same pattern already used for probes. The
`RefreshTimeout` bounds the wait. Alternatively guard `SwapSession`/`Close` with a
small `sync.Mutex`; the swap is off the hot path.

### F-09 [Medium] CONFIRMED — With no `Replayer` (the default), partial write failures are silently declared success

**Evidence.** `DefaultConfig` leaves `Replayer` nil; `executeDualWrite` returns
`nil` on partial failure and skips `enqueueReplayIfNeeded` when `Replayer == nil`
(`cql_client.go:2013-2019`); the only per-occurrence signal is `IncWriteError`. The
drain path (`1857-1879`) likewise drops the draining leg with no metric at all.
`EventReplayDropped` is marked unreachable in this configuration
(`eventKindUnreachable`, `639-640`), so the event stream is silent too. The
constructor logs one `Warn` at startup (`871-873`).

**Why it matters.** The library's headline guarantee ("failed writes are enqueued
for replay") is opt-in; the default silently diverges the two clusters.

**Direction.** Minimum: when `Replayer == nil` and a leg failed, still call
`IncReplayDropped` and emit `EventReplayDropped` with a distinct `Reason`
("no replayer configured") so divergence is observable per write. Stronger option
for a major version: default to an auto memory replayer or require an explicit
`WithNoReplayer()` opt-out.

### F-10 [Medium] CONFIRMED — `NATSReplayer.Enqueue` is a synchronous JetStream publish on the write hot path, uncancellable by the caller

**Evidence.** `replay/nats.go:401-467`: msgp encode + `js.Publish` (waits for
stream ack) bounded by `PublishTimeout` (default 5s). Root calls it with
`context.WithoutCancel(ctx)` (`cql_client.go:1869`, `2049`), so the caller's
deadline cannot shorten it. In `AdaptiveDualWrite` degraded mode every write
returns `ErrWriteAsync` and therefore enqueues (`2009-2016`).

**Why it matters.** During a NATS partition, every partially failing write — and,
in degraded mode, *every* write — blocks up to 5s. Replay-transport availability
becomes part of the write-path latency budget, which contradicts the reason
fire-and-forget exists. Mirror publisher mode has the same coupling but through the
engine's worker pool (documented in `config.go:546-550`).

**Direction.** Either `js.PublishAsync` with a bounded pending window and drop-on-
full accounting (`IncReplayDropped`), or front the NATS replayer with a small
in-memory ring drained by a goroutine. Keep the synchronous variant available for
callers who prefer back-pressure. Document the current blocking behaviour on
`WithReplayer`.

### F-11 [Medium] CONFIRMED — `WithAutoMemoryWorker` silently overwrites a user-supplied `Replayer`/`ReplayWorker`

**Evidence.** `cql_client.go:844-862`: when `AutoMemoryWorker` is set,
`config.Replayer` and `config.ReplayWorker` are unconditionally replaced. A caller
who passes `WithReplayer(natsReplayer)` plus `WithAutoMemoryWorker(0)` gets a
memory replayer and a worker they never see; their NATS replayer is never used and
their own `WithReplayWorker` worker is never started. `root_validation.go` has no
check for this pair. Neither does `WithMirrorReplayer` without `WithMirror`
("has no effect", `config.go:557`) nor `WithRecoveryProbe` with a non-adaptive
strategy (silently no probe, `cql_client.go:359-362`).

**Direction.** Add `validateRootReplayWiring` rejecting `AutoMemoryWorker &&
(Replayer != nil || ReplayWorker != nil)` with a `types.OptionError`, and warn (or
reject) the other two no-effect combinations. All three are five-line additions to
`root_validation.go`.

### F-12 [Medium] CONFIRMED — `ClientConfig` is simultaneously user options, runtime registry, and a live mutable pointer returned by `Config()`

**Evidence.** `config.go:84-247`: exported fields `MirrorEngine`,
`MirrorReplayWorker`, auto-created `Replayer`/`ReplayWorker`, unexported `events`,
`mirrorTargetSet`. `Config()` (`cql_client.go:3856`) returns `c.config`; hot paths
read `c.config.ReadStrategy`, `c.config.AllowedClusters`, etc. with no
synchronisation, so any post-construction mutation through `Config()` is a data
race. `CLAUDE.md` says the root package is the entry point and `policy` etc. are
public, yet the runtime wiring is visible through a "config" object.

**Direction.** Return a copy (or a read-only view) from `Config()`, and move
runtime-populated fields into an unexported struct on `CQLClient` (Tier 2 of F-03).

### F-13 [Medium] CONFIRMED — Auto-refresh `SustainedFailureWindow` is vacuous until the first successful op

**Evidence.** `clusterStats.lastSuccessNanos` is never initialised at construction
(only stored in `recordOpOutcomeAt`, `cql_client.go:490`), so
`now - 0 >= SustainedFailureWindow` is true from boot. `maybeAutoRefresh`
(`267-275`) therefore fires as soon as `FailureThreshold` failures accumulate,
contradicting the "ALL of" contract in `AutoRefreshConfig` docs
(`config.go:1015-1019`). Combined with F-01, ten cancelled requests at startup
against a healthy-but-slow cluster trigger a refresh.

**Direction.** Stamp `lastSuccessNanos = NowProvider()` for both clusters in
`NewCQLClient`, or document "the window is measured from construction". One line
either way.

### F-14 [Medium] CONFIRMED — Policy `eventOutbox` drops are counted but never surfaced

**Evidence.** `policy/event_outbox.go:40,76,129,145`: overflow past `outboxCap`
(64), emitter-removed, and emitter-panic drops increment `o.dropped`. No code reads
it (grep `dropped.Load` in `policy/` returns nothing); it is not exposed via
`ClusterEventMetrics.AddClusterEventsDropped`, which counts only the root
dispatcher's drops. The comment at `:140` says "surfacing it is left to whatever
consumes the drop count" — nothing does.

**Direction.** Add `Dropped() uint64` on the policies (or have `drain()` forward the
delta to the emitter as a synthetic count) and fold it into the existing
`cluster_events_dropped_total` metric.

### F-15 [Medium] SUSPECTED — Two-level buffering makes the event ordering guarantee weaker than documented under load

**Evidence.** Policy outbox (64, ordered per policy instance) → dispatcher channel
(128, non-blocking send, drop-newest) → single handler goroutine. `EventFailover`
and `EventReadDivergence` fire per read (`types/cluster_event.go:101-103`) and
compete for the same 128-slot buffer as the rare state-transition events. During a
read outage the buffer is saturated by failover events, so an `EventCircuitBreakerOpen`
arriving one microsecond later is dropped while the noise is delivered. Docs say
drops can "remove either end of an open/closed pair" but do not say that high-rate
kinds preferentially evict the important ones.

**Direction.** Per-kind priority: reserve a few slots (or a second small channel)
for transition kinds, or coalesce per-read kinds with a counter. Not exercised by
me; worth a targeted test that floods failovers while tripping a breaker.

### F-16 [Low] CONFIRMED — Dual constructors (`New*` vs `New*Checked`) across policy and replay create an inconsistent validation story

**Evidence.** `NewCircuitBreaker` silently normalises invalid options
(`policy/failover_policy.go:386-397`), `NewCircuitBreakerChecked` rejects them; same
for `AdaptiveDualWrite`, `LatencyCircuitBreaker`, `MemoryReplayer`, memory/NATS
workers. Root `NewCQLClient` always validates (`root_validation.go`). Policy
`With*` setters also pre-normalise (e.g. `WithThreshold(n > maxInt32)` sets 0). The
README recommends the unchecked forms.

**Direction.** For v2 collapse to one constructor returning `error`. Until then,
make the unchecked constructors log a `Warn` per substituted value (as
`sanitizeAutoRefreshConfig` already does) so silent normalisation is at least
visible.

### F-17 [Low] CONFIRMED — Dead/duplicated validation paths in root

- `validateRootAutoRefresh` rejects `<= 0` values, then `sanitizeAutoRefreshConfig`
  re-checks the same five fields and warns (`cql_client.go:820-825`); the second
  can no longer trigger through public options.
- `setupMirror` re-checks `mirrorTargetSet && mirrorPublisherSet` and nil
  target/publisher (`mirror_dispatch.go:63-79,118-120`) after
  `validateRootMirrorMode` already rejected them.
- `NewCQLClient` line 887 checks `client.topologyClose != nil` before the topology
  watcher has been started (always nil).

**Direction.** Delete the redundant branches or convert them to `panic`-free
assertions with a comment; keeps the constructor readable.

### F-18 [Low] CONFIRMED — `NowProvider` is a public config field with no `With*` option, unlike every sibling knob

`config.go:155-161` documents it for tests; there is no `WithNowProvider`. Tests set
it via `Config()`-style mutation or the exported field. Either add the option or
make it unexported with an `export_test.go` hook.

### F-19 [Low] CONFIRMED — Documentation drift in the architecture contract and godoc

- `DefaultConfig` doc says cluster names default to `"ClusterA"`/`"ClusterB"`
  (`config.go:270`); `types.DefaultClusterNames()` returns `"A"`/`"B"`.
- `mirror/engine.go:21,34` and `mirror/doc.go:30` reference `[types.Replayer]`, a
  symbol that does not exist (`Replayer` lives in root).
- `.knowledges/root/index.md` lists `client.go → NewClient`; no such function.
- `.agents/rules/100-overview.md` says all shared interfaces live in `types`; the
  strategy interfaces live in root (see summary).
- `types/metrics.go:84-88` documents circuit-breaker state 1 (half-open) as
  "reserved; no policy emits it" — the gauge therefore never reflects the half-open
  probe window that `ShouldFailover` implements.

### F-20 [Low] CONFIRMED — `Close()` from inside replay/mirror callbacks deadlocks (undocumented)

`WithOnClusterEvent` documents the handler-must-not-Close rule
(`config.go:677-681`). The same holds for `replay.WithOnDrop`/`OnError` (called from
the worker goroutine; `Worker.Stop` waits on `wg`, `replay/worker.go:343-351`) and
`mirror.WithOnError` (called from engine workers; `Engine.Stop` waits on `wg`,
`mirror/engine.go:244-253`), and for `OnReplayDropped` when invoked from the mirror
error handler. Add the same sentence to those option docs.

### F-21 [Low] CONFIRMED — `AdaptiveDualWrite` fire-and-forget writes and `MemoryReplayer` contents are lost at `Close()` by design, but the two documents disagree on visibility

`CQLClient` doc: "enqueued replays are lost if using MemoryReplayer"
(`cql_client.go:59`). `memoryBackend.drainAndDrop` (`replay/memory_worker.go:226-234`)
actually invokes `OnDrop` for each with reason "shutdown", so they are *reported*,
not silently lost — but only if the caller set `replay.WithOnDrop`, which
`WithAutoMemoryWorker` does not do by default. Consider defaulting the auto worker's
`OnDrop` to the client's `OnReplayDropped` handler so one callback covers both.

### F-22 [Low] SUSPECTED — Testing architecture: coverage of failure surfaces is structural for policies and replay, incidental for root read-path classification

**Evidence.** Policy and replay packages have focused table tests and race tests.
The simulation harness (`test/simulation`) has 14 scenario files covering
degradation, drain, CB, LCB, fallback, replay saturation, fire-and-forget,
failover-back, with a chaos session wrapper and a write-consistency oracle — this is
genuinely structural. However, `test/simulation` and `test/e2e` are not part of
`make ci` (`Makefile:163`), so they run only on demand. In root, the read-pipeline
classification (six entry points × {nil, notFound, rowLimit, ctxErr, clusterErr} ×
{override, drain, fallback}) is tested by many hand-written cases
(`cql_fallback_test.go`, `cql_fallback_slice_test.go`, `cql_two_layer_read_test.go`)
rather than by a single matrix, which is why F-01 and F-04 could persist. No test
found for `Iter`+drain, `Close` racing `RefreshSession` (only
`TestSwapSession_ConcurrentSwapAndQuery`), or auto-refresh firing before any
success (F-13).

**Direction.** Add one table-driven test over the read classification matrix that
runs each entry point through the same expectations (fits naturally once
`classifyReadErr` from F-03 exists); add a Close-vs-autoRefresh race test using the
existing `NowProvider` hook and a blocking `SessionRefresher`; wire `make test-e2e`
into a nightly CI job.

### F-23 [Low] CONFIRMED — `topology.Local.Watch` / `NATS.Watch` honour only the first caller's context

Documented (`topology/local.go:52-53`, `nats.go:104-105`), and root calls `Watch`
once, so this is fine today; but `Watch` returning the same channel to a second
subscriber means two clients sharing one watcher would compete for updates (each
update delivered to exactly one). Document that a watcher instance is per-client,
or fan out.

---

## (d) Open questions I could not resolve

1. **Intended semantics of caller cancellation vs cluster health (F-01).** Is
   `DeadlineExceeded` deliberately treated as a latency signal for
   `AdaptiveDualWrite`/`LatencyCircuitBreaker`? The slice-path exception suggests
   not, but the write path may have been left intentionally.
2. **Is `Iter()` ignoring drain intentional (F-04)?** There may be a reasoning that
   paging cursors must stay on one cluster (as for slice reads with `PageState`),
   but that would only justify skipping re-selection when `PageState` is set.
3. **Should CAS honour drain and `AllowedClusters`?** Currently documented as "not
   affected" for override; drain is unmentioned.
4. **Why does `topology` import root rather than `types`?** Moving
   `TopologyUpdate`/`TopologyWatcher`/`TopologyOperator` to `types` would make
   `topology` a sibling of `policy` and free root to reference topology types. I
   could not find a stated reason.
5. **Are the `New*` (unchecked) constructors still the recommended path?** The
   README and examples use them; the `Checked` variants look like the intended
   production path. Which one is canonical affects F-16.
6. **Replay outcome for `ErrWriteAsync` writes that later succeed (double
   application for non-idempotent statements)** — acknowledged in the `Batch` docs
   for counters; I did not verify whether the NATS worker de-duplicates by
   timestamp or relies solely on Cassandra LWW.
7. **What consumes `mirror.Engine.Stats()` and the outbox drop counters in
   production?** No metrics path for outbox drops exists (F-14); if an external
   dashboard scrapes `Stats()`, that would change the F-14 recommendation.
