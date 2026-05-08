# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **TestS1 e2e flake — `MemoryReplayer.Len()==0` is not a "drain done"
  signal**. The S1 happy-path drain test was using `memReplayer.Len()==0`
  to decide when replay was complete; under sustained suite pressure
  AdaptiveDualWrite did not always degrade and every Exec ran sync
  against paused A (~2s gocql timeout each), producing a long tail of
  retries. The worker holds payloads in flight between attempts —
  outside the queue but still actively retrying — so Len() can
  transiently report 0 while ~70 items remain to retry. The drain
  check passed prematurely, the test exited, and only ~28 of 100 rows
  reached cluster A. Diagnostic worker callbacks confirmed
  `success=8, error=98, dropped=0` with MaxAttempts=1000 — proving
  the issue was check semantics, not retry exhaustion. The test now
  uses row-count convergence (the authoritative downstream signal),
  and `MemoryReplayer.Len()` carries a Godoc note explaining the gotcha
  so future users avoid the same trap.

- **FailoverPolicy metrics and logger auto-injection** — surfaced while
  writing the plain-`CircuitBreaker` e2e test. `NewCQLClient`'s
  `autoInjectMetricsAndLogger` walked `ReplayWorker` and `WriteStrategy`
  but not `FailoverPolicy`, so users wiring `helix.WithMetrics(collector)`
  + `helix.WithFailoverPolicy(policy.NewCircuitBreaker(...))` saw zero
  `IncCircuitBreakerTrip` events in their dashboard — they had to also
  pass `policy.WithCircuitBreakerMetrics(collector)` redundantly.
  `CircuitBreaker` (and `LatencyCircuitBreaker` via embedding) now
  satisfy the same `metricsAware` / `loggerAware` interfaces as
  `AdaptiveDualWrite` and `replay.Worker`, and the auto-inject pass
  walks `FailoverPolicy` too. Caller-supplied `WithCircuitBreakerMetrics`
  / `WithLatencyMetrics` / `WithCircuitBreakerLogger` /
  `WithLatencyLogger` still win on conflict (last-write-wins via
  the `metricsExplicit` / `loggerExplicit` guard).

### Added

- **e2e final-pass: plain CB, FallbackRead, Mirror+drain (v1.4.0)**:
  three more combinations from the audit gap list:
  - `TestS_PlainCircuitBreaker_TripAndClose` — counter-based
    `CircuitBreaker` (vs LCB which already had S3 coverage). Pause A,
    accumulate failures past threshold, verify trip + reset-timeout
    half-open + close after Unpause + successful probe. Wires
    `WithCircuitBreakerMetrics` explicitly because FailoverPolicy is
    not auto-injected by helix.
  - `TestS_FallbackRead_RowMissingOnPreferredButPresentOnOther` and
    `TestS_FallbackRead_AlternativeUnreachable_ReturnsNotFound` —
    real-cluster coverage for `FallbackRead`. Verifies the lag-recovery
    path (row only on B, FallbackRead recovers it) and the
    "alternative unreachable returns ErrNotFound, not network error"
    contract.
  - `TestMirror_PrimaryClusterDraining_MirrorStillFires` — mirror +
    topology drain mode. With cluster A in drain, writes are skipped
    on A, B succeeds, primary returns nil via any-cluster-ack, mirror
    must still fire. Real migration scenario.

- **e2e suite hardening (v1.4.0 follow-up)**: combination-coverage audit
  flagged five real production scenarios with zero e2e coverage. Added
  five new e2e tests, each parameterized over v1 and v2 drivers:
  - `TestMirror_WithAdaptiveDualWritePrimary` — proves the v1.4.0 plan's
    central orthogonality claim under real outage. Pause primary cluster
    A, AdaptiveDualWrite degrades, primary returns nil via any-cluster-ack,
    mirror dispatch fires regardless of write strategy state.
  - `TestS_NetworkDisconnect_StickyReadFailover` — first test in the
    repo to exercise `testutil.NetworkDisconnect`. Real network partition
    via Docker network detach, StickyRead fails over from A to B, failover
    metric increments. Closest reproducible analog to a real cloud blip
    (different from `Pause` which keeps TCP open).
  - `TestS1b_PauseA_NATSReplayerDrain` — companion to S1 exercising the
    production `NATSReplayer` + `NATSWorker` durability backend rather
    than `MemoryReplayer`. Pause A, drive writes, replays land in NATS
    JetStream, Unpause, NATSWorker drains and both clusters converge.
  - `TestS2b_PauseB_StickyReadFailover` — symmetry counterpart to S2
    (which always paused cluster A). Verifies different gocql session,
    different driver state, different ports behave symmetrically when B
    is the failing cluster.
  - `TestS_SequentialFailures_AThenB` — cascading failure path. Pause A
    (failover to B), Unpause A, Pause B (failover back to A). Proves
    helix's per-cluster health state is reset on recovery and the
    failover metric records every transition, not just the first.

- **Mirror e2e Stop+Start and Kill scenarios (v1.4.0 follow-up)**:
  combination-coverage audit found the original mirror e2e suite covered
  only `Pause` (graceful TCP-open hang). Real migrations also see
  graceful container halts and process crashes — different gocql code
  paths. Added two more e2e tests pairing the mirror engine with helix's
  existing `AutoRefresh` + `SessionRefresher` machinery:
  - `TestMirror_DestinationStopAndStart_AutoRefreshRecovers` — `Stop`
    the mirror cluster mid-write, `Start` it back. AutoRefresh on the
    mirror client detects the dead session, refreshes via the
    SessionRefresher, and the auto-built replay worker drains the
    backlog. Scylla-only.
  - `TestMirror_DestinationKilled_AutoRefreshRecovers` — `Kill`
    (SIGKILL) the mirror destination, `Start` it back. Same recovery
    path, exercising the harder RST-on-existing-connection failure mode.
    Scylla-only.

  Both run against v1 and v2 adapters. Total mirror e2e: 5 tests × 2
  drivers = 10 subtests, ~55s wall-clock.

- **Mirror e2e tests with real cluster lifecycle (v1.4.0 follow-up)**: the
  Phase 5 integration tests fake destination outages with mock-session
  `execErr` fields. The repo's `test/e2e/cql/` harness already supports
  real container lifecycle (`Pause`, `Unpause`, `Stop`, `Start`, `Kill`,
  `NetworkDisconnect`) but no mirror test used it. Added `mirror_test.go`
  under that build tag covering:
  - `TestMirror_DestinationPausedAndRecovered` — pause the mirror
    destination cluster while the app keeps writing, verify failures land
    in the auto-built replay worker and catch up after `Unpause`. Uses
    real gocql session timeouts and reconnects.
  - `TestMirror_BothPrimaryClustersPaused_NoMirrorFire` — answers the
    "did we test A and B both down, not just A or B?" question. With both
    primary clusters paused, `Mirror()` returns `DualClusterError` and
    the engine queue stays empty (no `IncMirrorEnqueueSuccess` call).
  - `TestMirror_PrimaryPartialOutage_MirrorStillFires` — pause one
    primary cluster, verify any-cluster-ack returns nil and mirror
    dispatch fires against the live destination (real CQL roundtrip,
    not mocked).

  Each test runs against both v1 and v2 adapters (6 subtests total). All
  green against ScyllaDB.

- **Mirror test gap closures (v1.4.0 follow-up)**: a critical pass through
  the mirror feature surface flagged real coverage holes. Closing them:
  - Dual-cluster primary + Mirror() trigger conditions now tested:
    both-success fires, partial-success (any-cluster-ack returns nil)
    fires, total-failure (DualClusterError) suppresses.
  - `WithMirror(nil)` and `WithMirrorPublisher(nil)` now reject at
    construction with `types.ErrNilMirrorTarget` /
    `types.ErrNilMirrorPublisher` instead of silently disabling
    mirroring (added internal `mirrorTargetSet` / `mirrorPublisherSet`
    config flags to distinguish "passed nil" from "never called").
  - Caller-supplied `Query.WithTimestamp()` is preserved through mirror
    dispatch (regression test against future refactors).
  - `client.Close()` drains the in-flight mirror queue before returning.
  - Auto-built NATS replay worker path (`WithMirrorReplayer` +
    `*replay.NATSReplayer`) end-to-end test against embedded NATS +
    real CQL — previously only the `MemoryReplayer` branch was covered.
  - `docs/mirror.md`: added "Per-query consistency is not preserved on
    mirror exec" to the Known Parity Gaps section. Matches the existing
    primary-replay omission.

- **Mirror docs, examples, and integration tests (v1.4.0 Phase 5)**:
  - `docs/mirror.md` — user guide covering target / publisher modes,
    semantics, runtime control, observability, known parity gaps, and
    failure observability paths.
  - `examples/mirror/` — runnable wiring example for both modes
    (target via `WithMirror` + optional `WithMirrorReplayer`, and the
    publisher / consumer split via `WithMirrorPublisher` + `NewMirrorWorker`).
  - `test/integration/mirror_integration_test.go` — end-to-end integration
    tests against shared CQL clusters (and an embedded NATS JetStream
    server for publisher mode) covering target-mode dispatch, durable
    retry via `WithMirrorReplayer`, publisher / consumer split, and
    server-side timestamp parity (`WRITETIME` matches across
    primary / mirror).

### Changed

- **Mirror payload routing**: dispatched mirror payloads now carry
  `TargetCluster = ClusterA` so transports that route by cluster (NATS
  subjects use `{prefix}.{priority}.{cluster}`) deliver mirror messages
  to the consumer. Mirror writes target the mirror destination as a
  single logical sink — the destination's own write strategy handles
  per-cluster fan-out internally — so the conventional ClusterA tag has
  no real per-cluster meaning at the source.

- **Mirror metrics (v1.4.0 Phase 4)**: dedicated `helix_mirror_*` metric
  surface for the async mirror engine.
  - New optional interface `types.MirrorMetrics` (paralleling
    `types.SessionRefreshMetrics`). Implementations may opt in by adding
    the methods; bundled collectors (`internal/metrics.NopMetrics` and
    `contrib/metrics/vm.Collector`) implement it directly.
  - Metric set: `mirror_enqueue_success_total`,
    `mirror_enqueue_dropped_total`, `mirror_exec_success_total`,
    `mirror_exec_errors_total`, `mirror_exec_duration_seconds`
    (histogram), `mirror_queue_depth` (gauge), `mirror_enabled` (gauge).
    Mirror metrics are not cluster-scoped — per-cluster routing on the
    mirror destination is recorded against that destination's own
    metrics namespace.
  - `mirror.WithMetrics(types.MirrorMetrics)` option. Helix's CQLClient
    auto-wires the mirror engine when the configured `MetricsCollector`
    also satisfies `MirrorMetrics`, so passing a bundled collector to
    `helix.WithMetrics` enables mirror observability without extra
    plumbing.

- **Mirror publisher mode (v1.4.0 Phase 3)**: out-of-process mirroring for
  production cluster migrations. The app publishes captured mirror writes
  to a [Replayer] (typically [replay.NATSReplayer]) instead of writing
  them in-process; a separate consumer binary performs the actual writes
  against the mirror clusters.
  - `helix.WithMirrorPublisher(publisher Replayer, opts ...mirror.Option)`:
    configures publisher mode. Captures pass through helix's bounded
    in-memory ring buffer (the same engine queue used in target mode)
    before reaching the publisher; queue overflow is dropped per the
    engine's drop-on-full policy. Mutually exclusive with `WithMirror` —
    NewCQLClient returns `types.ErrMirrorModeConflict` if both are set.
    Pair with `mirror.WithOnError` to observe `publisher.Enqueue`
    failures (e.g., NATS publish errors).
  - `helix.NewMirrorWorker(replayer, target, opts ...replay.WorkerOption)`:
    constructor for the consumer-side worker. Type-switches on the
    replayer's concrete type (`*replay.MemoryReplayer` or
    `*replay.NATSReplayer`) and binds the worker to the same execute
    path the in-process mirror engine uses, so timestamps and dual-write
    behavior on the mirror destination are preserved across modes. No
    metrics auto-injection — the consumer binary owns its observability
    stack and passes options explicitly.
  - New sentinel errors: `types.ErrMirrorModeConflict`,
    `types.ErrNilMirrorTarget`.

- **Mirror replay durability (v1.4.0 Phase 2)**: failed mirror writes are
  now durably retried via the existing replay system.
  - `helix.WithMirrorReplayer(replayer, workerOpts...)`: configures a
    [Replayer] that receives any mirror write whose execute returned an
    error. When the replayer's concrete type is `*replay.MemoryReplayer`
    or `*replay.NATSReplayer` an appropriate `ReplayWorker` is auto-built
    and bound to the same execute function the mirror engine uses, so
    timestamps, dual-write strategy, and per-cluster routing on the
    mirror destination are preserved on retry. The worker is started
    during `NewCQLClient` and stopped during `Close`.
  - `mirror.WithOnError(handler)`: generic per-execute-error hook on the
    mirror engine. Helix uses it internally to route failures to the
    configured `MirrorReplayer`; custom integrations (alerting, alternate
    durability stores) can install their own.
  - `mirror.ExecuteFunc` is now a type alias for `replay.ExecuteFunc` so
    the same function is used for the mirror engine's initial dispatch
    and the auto-built replay worker.
  - When a mirror enqueue itself fails (e.g., replayer queue saturated),
    the existing `WithOnReplayDropped` callback fires — mirror and primary
    replay share one alerting path.

- **Async mirror writes (v1.4.0 Phase 1)**: per-statement opt-in mirroring of
  writes to a second helix dual-cluster pair, designed for seamless Cassandra
  cluster migrations where the application needs N days of write history on
  the new clusters before cutover.
  - `Query.Mirror()` / `Batch.Mirror()`: fluent opt-in on a single statement
    or batch. Always async, fire-and-forget; the mirror leg never surfaces
    an error to the caller. The original client-generated timestamp is
    preserved on the mirror exec so server-side `WRITETIME`, LWW, TTL, and
    tombstone semantics match the primary cluster.
  - `helix.WithMirror(target, opts...)`: configures the mirror destination
    (a second helix `*CQLClient`) and engine options. Mirroring fires only
    after the primary write succeeds (any-cluster ack); total primary
    failure suppresses the mirror.
  - `mirror.Engine`: bounded in-memory queue + worker pool with non-blocking
    enqueue and drop-on-full policy. Drop logs are rate-limited; an optional
    `mirror.WithOnDrop` callback receives every dropped capture.
  - `*CQLClient.Mirror()`: runtime control surface — `Enable`, `Disable`
    (drains in-flight queue), `Enabled`, and a `Stats` snapshot
    (Enqueued / Dropped / Success / Error / QueueDepth).
  - Bound `[]any` args (and batch statement args) are deep-copied
    synchronously before Exec returns, so caller-side buffer reuse / pooling
    cannot corrupt mirror payloads.

  Phase 1 ships in-memory dispatch only; failed mirror writes are logged and
  counted but not retried. Phase 2 will integrate the existing replay system
  for durability; Phase 3 adds an out-of-process NATS publisher mode. See
  `docs/plans/v1.4.0-async-mirror.md` for the full plan.

## [1.3.0] — 2026-05-08

### Added

- **Session refresh — manual API**: `*CQLClient.SwapSession(cluster, newSession)`
  and `*CQLClient.RefreshSession(ctx, cluster)` recover from a permanently-dead
  underlying `cql.Session` (cluster restart with port reassignment, DNS
  rotation, host migration) without rebuilding the client. The topology
  watcher, replay worker, and any application-side references stay alive
  across the swap.
  - `SwapSession`: lowest-level escape hatch. Caller passes a fresh session
    they built; receives the old session back and decides when to close it.
    Lock-free on the read path via `atomic.Pointer[sessionHolder]`.
  - `RefreshSession`: high-level entry point. Invokes the registered
    `SessionRefresher`, atomically swaps the result in, and closes the old
    session.
  - `WithSessionRefresher(fn SessionRefresher)`: caller-supplied factory.
    Helix never imports a specific gocql driver — only the caller knows
    how to construct a session. The refresher receives the most recently
    observed failure error against this cluster (or nil if none) so it can
    tailor reconnection strategy.
  - New sentinels: `types.ErrInvalidCluster` (cluster B on a single-cluster
    client; unknown ClusterID), `types.ErrNoSessionRefresher` (RefreshSession
    called without a registered refresher).
- **Session refresh — automatic detection (`WithAutoRefresh`)**: Helix
  observes per-cluster op outcomes via a background goroutine and invokes
  the registered `SessionRefresher` automatically when a cluster's session
  is observed to be permanently dead. The decision is policy-independent
  (works with any `FailoverPolicy`) and references no driver-specific
  error types.
  - Trigger condition (all three required): `consecutiveFailures >=
    FailureThreshold` AND `time.Since(lastSuccess) >= SustainedFailureWindow`
    AND `time.Since(lastRefresh) >= MinRetryInterval` (throttle).
  - Conservative defaults (10 / 5 min / 1 min / 30 s / 30 s) — refresh
    storms are operationally far worse than slow recovery. Tunable via
    per-knob options: `WithAutoRefreshFailureThreshold`,
    `WithAutoRefreshSustainedFailureWindow`,
    `WithAutoRefreshMinRetryInterval`, `WithAutoRefreshCheckInterval`,
    `WithAutoRefreshRefreshTimeout`.
  - Throttle stamp set BEFORE invoking the refresher so a hung refresher
    cannot cause re-entrant double-fire.
  - Optional metrics interface `types.SessionRefreshMetrics` (Inc
    SessionRefreshAttempt / Success / Error). The CQLClient
    type-asserts on this interface and silently no-ops if the configured
    collector does not implement it, so by-hand `MetricsCollector`
    implementations stay source-compatible. The bundled
    `contrib/metrics/vm` collector implements it.
- **`NowProvider`** abstraction in `config.go`, mirroring `TimestampProvider`.
  Lets tests substitute a deterministic clock for the auto-refresh detector.
- **Session Refresh Guide** (`docs/session-refresh.md`): when you need this,
  quick start, decision logic, throttling, operational-state filtering,
  manual SwapSession/RefreshSession semantics, concurrency rules,
  observability, documented non-goals, production-shaped refresher example.
- **e2e/cql test suite** (`test/e2e/cql/`, build tag `e2e`): real-container
  failure-mode scenarios on Cassandra/ScyllaDB. Tests S1–S11 cover read/write
  failover, the LCB half-open transition, container Pause/Unpause and
  Stop/Start, hard SIGKILL, manual + auto session refresh, and the
  replay-queue conservation law under deliberate sustained-load overflow.
  Runs via `make test-e2e`; gated to opt-in via the build tag.
- **`testutil.CQLCluster` lifecycle methods**: `Stop`, `Start`, `Pause`,
  `Unpause`, `Kill`, `NetworkDisconnect`, `NetworkReconnect`, `Reconnect`,
  `RefreshHost`. Supports the e2e suite's container-level chaos.
- **Bounded retry for the memory replay worker**: New
  `WorkerConfig.MaxAttempts` field (default `5`) and `WithMaxAttempts(n)`
  option. The memory backend runs the first attempt synchronously on the
  dequeue loop and dispatches attempts 2..MaxAttempts to a bounded
  retry-goroutine pool, then drops via `OnDrop`. Splitting attempt 1
  from the rest keeps unrelated traffic flowing while a single payload
  retries; the bounded pool prevents goroutine fan-out under sustained
  failure storms. Mirrors the bounded-retry contract NATS already
  provides via JetStream's `MaxDeliver`.
- **Observability for `AdaptiveDualWrite` background writes**:
  `WithAdaptiveMetrics(m)`, `WithAdaptiveLogger(l)`,
  `WithAdaptiveClusterNames(names)` options on the strategy.
  `helix.NewCQLClient` auto-injects the client-level metrics collector and
  logger via `MetricsConfigured`/`SetMetrics` and
  `LoggerConfigured`/`SetLogger` interfaces, so the new visibility lights
  up by default with no caller change. Explicit `WithAdaptiveMetrics` /
  `WithAdaptiveLogger` configuration wins over auto-injection — callers
  who deliberately route background-write logs or metrics to a separate
  sink keep that routing.

### Bug Fixes

- **`CircuitBreaker.ShouldFailover` had no half-open probe path**: once the
  breaker tripped on a cluster, it stayed open indefinitely if traffic
  routed away (e.g., `StickyRead` to the survivor) — no `RecordSuccess`
  ever fired against the failed cluster, so the breaker had no path back
  to closed. Fix is two-part: (a) `ShouldFailover` now returns false when
  `time.Since(lastFailure) > resetTimeout`, allowing a probe; (b)
  `RecordFailure` clears the trip latch on the timeout-reset branch so
  multi-cycle outages emit `IncCircuitBreakerTrip` once per trip rather
  than once per the entire outage. "Leaky" half-open semantics are
  documented (concurrent callers may all probe the same cycle).
- **Auto-injection of metrics into replay worker**: when `WithMetrics(mc)`
  is set on the client but the worker is built without
  `WithWorkerMetrics(mc)`, worker-side `IncReplaySuccess` / `IncReplayDropped`
  / `IncReplayError` previously went into the worker's internal `NopMetrics`
  and were silently invisible to the client's collector. `NewCQLClient`
  now detects this via type-assertion (`MetricsConfigured() bool` +
  `SetMetrics(types.MetricsCollector)` on `*replay.Worker`) and injects
  the client's mc. Caller-supplied `WithWorkerMetrics(otherMc)` is NOT
  overwritten. The auto-memory worker (created internally when
  `WithAutoMemoryWorker` is set) inherits the client's mc by default.
- **Integration test `TestAllowedClusters_Integration_CAS_NotOverridden`
  fails on Scylla**: Scylla's `IF NOT EXISTS` returns 3 columns
  (`[applied]`, key, value) regardless of applied=true vs false, while
  Cassandra returns just `[applied]` when applied=true. gocql's `ScanCAS`
  errors on column-count mismatch. Fixed by passing two destination
  values to `ScanCAS` to match Scylla's wider response — works on both
  backends.
- **`CircuitBreaker.RecordFailure` mishandled `resetTimeout == 0`**: The
  half-open work above (a) made `ShouldFailover` skip the timed
  transition when `resetTimeout=0`, and (b) reset the failure counter to
  1 on the timeout-reset branch in `RecordFailure`. The (b) branch was
  missing a matching `resetTimeout > 0` guard, so with `resetTimeout=0`
  every failure after the first matched
  `time.Duration(now-lastFailure) > 0` and reset the counter to 1 — the
  breaker could never accumulate to threshold and silently failed open.
  `RecordFailure` now mirrors `ShouldFailover`: with `resetTimeout=0`,
  failures keep accumulating until an explicit `RecordSuccess` closes
  the breaker.
- **Memory replay worker had inert exponential backoff and no
  max-attempts cap**: `executeWithRetry` was always invoked with
  `attempt=1` from the polling loop, so `calculateBackoff` always
  returned the base `RetryDelay` regardless of attempt number — the
  documented "exponential backoff" was effectively a fixed delay. The
  re-enqueue retry strategy also had no upper bound: a permanently
  broken cluster bounced the same payload through the queue forever
  (until `ErrReplayQueueFull` happened to drop it via the re-enqueue
  path, with the misleading log line "queue full"). Replaced with a
  synchronous-first attempt + bounded asynchronous retries: attempt 1
  runs on the dequeue loop, attempts 2..MaxAttempts run in a dedicated
  retry goroutine pool (default 100). The dequeue loop is therefore
  never blocked behind a permanently-failing payload, including
  payloads targeting a different cluster. After MaxAttempts the
  payload drops via `OnDrop` with the actual failure error.
  **Behavior change**: callers that relied on infinite retry should
  set `WithMaxAttempts` higher (or use `NATSReplayer` whose
  `MaxDeliver` semantics are unchanged). When the retry pool is full
  under sustained failure, further failures drop immediately via
  `OnDrop` (reason `retry pool saturated`) rather than queue behind
  in-flight retries.
- **`topology/nats` watcher silently undrained clusters on transient
  errors**: Both `fetchAndEmit` (any `kv.Get` error) and `processEntry`
  (any `json.Unmarshal` error) called `handleNoDrain`, clearing drain
  state for both clusters. A NATS KV blip during the pollLoop fallback
  path or a malformed config push from operator tooling could silently
  undrain a cluster the operator had marked offline for maintenance.
  Fail closed instead: only `jetstream.ErrKeyNotFound` and explicit
  Delete/Purge operations clear drain state. All other errors preserve
  the last-known-good drain state, forcing an authoritative valid
  config (or an explicit Delete) to actually undrain.
- **`AdaptiveDualWrite` swallowed background-write errors**: When a
  degraded cluster's fire-and-forget goroutine returned a real error
  (not `ErrWriteAsync`/`ErrWriteDropped`), it returned silently — no
  metric, no log, no strike. The replay safety net handled eventual
  consistency, but operators couldn't tell a degraded cluster had
  progressed to permanently broken until they noticed replay backlog
  growing. The goroutine now emits `IncWriteError(cluster)` and a Warn
  log on real errors, so the transition from "merely degraded" to
  "actively erroring" is visible in dashboards.
- **NATS replay worker delayed redelivery for the unprocessed batch
  tail on shutdown**: When `stopCh` fired mid-batch, only the current
  message was Nak'd. Remaining messages in the batch stayed
  unacknowledged and relied on `AckWait` (default 30 s) before the
  broker re-delivered them — inserting a 30-second visible delay
  during graceful restarts before a fresh worker could see those
  messages again. `processMessages` now Naks every unprocessed message
  in the batch on shutdown so the broker re-delivers immediately. Each
  Nak is independent; an error on one does not block the others.
- **Adapter `Session.Close()` idempotency depended on driver internals**:
  Both `adapter/cql/v1.Session` and `adapter/cql/v2.Session` delegated
  Close directly to `gocql.Session.Close`. The `CQLClient` docstring
  promises Close on bundled adapters is idempotent, but in practice
  that guarantee leaned on each driver's own internal sync.Once — a
  future driver release that drops its internal idempotency would
  silently break the Helix promise. Added an explicit `sync.Once` in
  each adapter so idempotency is a Helix-level contract independent of
  any specific driver release.

### Documentation

- **`WithAllowedClusters` scope clarified**: The override applies to
  reads ONLY. Writes (`Exec`/`ExecContext`, batch `Exec`) and CAS
  operations always go through normal routing. To fence a cluster from
  writes, drain it via `TopologyWatcher`/`TopologyOperator` — drain
  skips writes to the affected cluster and enqueues them for replay.
  The previous godoc only flagged CAS as exempt, leaving dual-write
  behavior under override ambiguous.
- **`WorkerConfig.HighPriorityRatio` unit clarified**: The ratio is
  per-message for the memory backend but per-batch for the NATS
  backend. With `BatchSize=100` and `HighPriorityRatio=10`, NATS
  effectively processes ~1000 high-priority messages : ~100
  low-priority messages per scheduling cycle. Memory worker users who
  expected per-message semantics on NATS will see different operational
  behavior and can plan accordingly.
- **`NewMemoryWorker` shutdown burst documented**: `Stop` drains every
  pending payload via `OnDrop`. High-throughput systems can see a
  sudden burst of `OnDrop` callbacks at shutdown proportional to queue
  depth — size the handler (and any synchronous fallback persistence)
  accordingly.

### Internal

- **`*CQLClient.sessionA` / `sessionB`** migrated from `cql.Session` fields
  to `atomic.Pointer[sessionHolder]`. Pure refactor; existing tests pass
  unchanged. The wrapper struct is required because `cql.Session` is an
  interface, `atomic.Pointer[T]` requires a concrete type, and
  `atomic.Value` would panic when successive Stores have different
  dynamic types — which the upcoming `SwapSession` feature explicitly
  supports.
- **Per-cluster op-outcome tracking** (`recordOpOutcome` helper) wired
  into every hot-path success/failure site, including the dual-write
  per-cluster paths. Operational/data states (`ErrWriteAsync`,
  `ErrWriteDropped`, `ErrNotFound`) are filtered structurally — they
  don't accumulate as failures. The dual-write wiring is per-cluster
  (not gated on "all clusters succeeded") so a partial-success outage
  on B doesn't starve A's `lastSuccess`, preventing false-positive
  auto-refresh on the healthy cluster.
- **testcontainers-go** upgraded `v0.40.0 → v0.42.0`. Docker SDK API
  breakage in `ContainerPause` / `ContainerUnpause` (now require options
  structs and return result values) absorbed in the testutil shim.
- **`policy.safeWrite` panic recovery captures `runtime.Stack`**: The
  recovered panic error now embeds the goroutine stack, mirroring the
  `callAllowedClusters` recovery pattern in `cql_client.go`. Without
  the stack, "panic: ..." with no trace was useless for debugging
  panics that originated several frames deep in driver or caller code.
- **`NewCQLClient` auto-injection helper extracted**: Metrics and
  logger auto-injection for `ReplayWorker` and `WriteStrategy` moved
  into `autoInjectMetricsAndLogger` to keep cyclomatic complexity
  within the project's lint cap as new auto-inject targets land.

## [1.2.0] — 2026-04-15

### Added

- **`WithAllowedClusters`**: Operator-driven read routing override. When the
  provided `AllowedClustersFunc` returns a non-empty cluster list, the read
  strategy is bypassed and the list directly controls routing with optional
  failover. Strategy state (OnSuccess/OnFailure) is frozen during the override
  to prevent drift, and resumes cleanly when the override is removed. Key
  behaviors:
  - **Fail-closed**: unknown cluster IDs, drain conflicts, and panics all
    return errors (`ErrInvalidClusterOverride`, `ErrNoValidClusters`,
    `ErrClusterOverridePanic`) rather than falling through silently.
  - **FallbackRead fencing**: FallbackRead only probes the alternative cluster
    if it appears in the allowed list.
  - **CAS bypass**: CAS operations (ScanCAS, MapScanCAS, batch ExecCAS) are
    unaffected — they are write-like, single-cluster operations.
  - **Power-of-2 log backoff**: misconfiguration errors log on the 1st, 2nd,
    4th, 8th, … occurrence to prevent log storms at high QPS.
  - **Iterator paths**: override errors are deferred to `Close()`. Always call
    `Close()` and check its error.
- **`AllowedClustersFunc`** type: function signature for the override provider.
- **`ErrNoValidClusters`**, **`ErrInvalidClusterOverride`**,
  **`ErrClusterOverridePanic`**: New sentinel errors in `types/` for
  AllowedClusters fail-closed conditions.
- **Auto-recovery guide** (`docs/auto-recovery.md`): End-to-end recovery
  lifecycle documentation — when auto-recovery suffices, when operator
  intervention is needed, the coordinated 4-phase workflow, and common mistakes.
- **AllowedClusters section in `docs/strategy-policy.md`**: Override semantics,
  strategy freezing, drain intersection, FallbackRead fencing, CAS bypass,
  fail-closed behavior, and operator workflow.

### Bug Fixes

- **`PrimaryOnlyRead.OnFailure` drops reads when ClusterB fails in failed-over
  state**: When ClusterA failed and reads moved to ClusterB, a subsequent
  ClusterB failure returned `("", false)` — the read failed entirely even if
  ClusterA had recovered. `OnFailure` now returns ClusterA as a probe target.
  If the probe succeeds, `OnSuccess(ClusterA)` clears the failover state. If
  ClusterA is also down, both attempts fail (DualClusterError) and the next
  `Select()` still returns ClusterB — avoiding request-level A/B flipping.
- **`StickyRead.OnFailure` drops reads during cooldown**: When the preferred
  cluster failed and cooldown was still active, `OnFailure` returned
  `("", false)` — the read failed entirely. Now returns the alternative cluster
  for the current request without changing preferred. Reads succeed via retry,
  but each request during the cooldown window pays the cost of trying the
  preferred cluster first (elevated latency and error counts until cooldown
  expires and preferred can switch).

### Internal

- **`tryFallbackCluster` extraction**: The ~35 lines of shared failover tail
  logic (metrics, logging, session read, not-found handling, DualClusterError
  construction) duplicated between `executeOverrideFailover` and
  `executeNormalFailover` were extracted into a single helper.

### Tests

- 32 unit tests for AllowedClusters: single-cluster routing, failover within
  override, nil/empty return (normal behavior), toggle on/off, failover policy
  denial, drain filtering, drain conflict, strategy state freezing/resumption,
  iterator paths, CAS bypass, FallbackRead fencing, unknown cluster IDs, panic
  recovery, duplicate deduplication, single-cluster mode edge cases, snapshot
  consistency, and batch iterator paths.
- 17 integration tests for AllowedClusters against real Cassandra clusters:
  override routing, failover within override, drain interaction, single-cluster
  mode, CAS bypass, panic recovery, FallbackRead fencing, ForceDegrade
  coordination, and DefaultFallbackRead fencing.
- 7 unit tests for failover-back fixes: `PrimaryOnlyRead` ClusterB failure
  probes ClusterA, non-failed-over B failure does not probe, dual failure
  (both down), recovery timeout timer preservation; `StickyRead` cooldown
  still fails over (per-request), cooldown expired switches preferred,
  non-preferred failure with no cooldown bypass.
- 3 integration tests for failover-back behavior against real Cassandra
  clusters.
- 8 simulation soak-loop tests covering duration-bounded randomized scenario
  replay with inter-iteration queue drains and pre-verify drains.

---

## [1.1.0] — 2026-04-12

### Breaking Changes

#### Interface additions (compile-time)

The following methods were added to exported interfaces. **Custom implementations
must be updated to compile.**

| Interface | New method | Purpose |
|---|---|---|
| `helix.Query` / `adapter/cql.Query` | `FallbackRead() Query` | Enables best-effort read from both clusters |
| `types.MetricsCollector` | `IncReadDivergence(cluster ClusterID)` | Tracks fallback-read divergence events |

**Migration for `helix.Query` implementors**: Add a no-op `FallbackRead()` method
that returns the receiver. The testutil `MockQuery` already implements this.

**Migration for `MetricsCollector` implementors**: Add a no-op
`IncReadDivergence` method. See `internal/metrics/NopMetrics` for reference.

#### Not-found error contract change (silent behavior change)

The v1 and v2 CQL adapters now map `gocql.ErrNotFound` to `types.ErrNotFound`
in `Scan`, `ScanContext`, `MapScan`, and `MapScanContext`. This is a **silent
breaking change** for callers that check `errors.Is(err, gocql.ErrNotFound)`
directly — those checks will stop matching.

**Migration**: Replace:

```go
// Before — stops working after this release
if errors.Is(err, gocql.ErrNotFound) { ... }

// After — works with both old and new releases
if helix.IsNotFound(err) { ... }
// or
if errors.Is(err, helix.ErrNotFound) { ... }
```

### Removed

- **SQL client**: `SQLClient`, `NewSQLClient`, `NewSQLClientFromDB`, and the
  `adapter/sql` package have been deleted. The feature was incomplete and is not
  being carried forward. Helix is CQL-only going forward.

### Added

- **`FallbackRead`**: Best-effort read from both clusters for critical
  read-after-write scenarios. When the selected cluster returns not-found,
  Helix silently tries the other cluster before returning not-found to the
  caller. Activated per-query (`.FallbackRead()`), per-context
  (`helix.WithFallbackRead(ctx)`), or per-client
  (`helix.WithDefaultFallbackRead(true)`). Precedence:
  per-query > context > client default.
- **`types.ErrNotFound`** / **`helix.IsNotFound`**: Canonical not-found
  sentinel and helper. Adapters map driver-specific not-found errors to this
  sentinel at the boundary.
- **`IncReadDivergence` metric**: Fired when FallbackRead finds data on the
  alternative cluster, labeled with the stale cluster for replay-lag
  correlation.
- **`helix.ErrNotFound`** / **`helix.IsNotFound`**: Root-package re-exports
  for convenience.

### Bug Fixes

- **Not-found incorrectly treated as cluster failure**: `gocql.ErrNotFound`
  was flowing through the read path as a real error, triggering
  `IncReadError`, `RecordFailure`, and `OnFailure`. Not-found is now
  classified as a successful cluster response with no data — it never
  poisons health state or triggers failover.
- **Failover not-found leaking into `DualClusterError`**: When the primary
  cluster had a real error and the failover cluster returned not-found,
  the combined `DualClusterError` satisfied `helix.IsNotFound()` — callers
  would treat an inconclusive partial outage as "row definitively absent."
  Now returns the healthy cluster's `ErrNotFound` directly (no
  `DualClusterError`), preserving availability during single-cluster outages.
- **Failover not-found poisoning healthy cluster**: In the same path,
  `IncReadError` and `RecordFailure` were called on the failover cluster even
  when it correctly returned not-found. Repeated reads for missing rows during
  a partial outage could trip the healthy cluster's circuit breaker.
- **FallbackRead returning error when alternative is unreachable**: When the
  primary (healthy) cluster returned not-found and the alternative was down,
  `executeFallbackRead` returned the alternative's connection error. This
  made FallbackRead *decrease* availability versus not using it — the
  primary's not-found would have been returned cleanly without FallbackRead.
  Now returns `ErrNotFound` (the primary's healthy answer) while still
  recording health metrics on the unreachable cluster.
- **`WithTimestampProvider(nil)` panics on first write**: Passing `nil` to
  `WithTimestampProvider` stored a nil function pointer with no validation.
  Any subsequent write calling `getTimestamp()` would panic with a nil
  function dereference. `NewCQLClient` now resets a nil provider to
  `DefaultTimestampProvider` after options are applied, matching the existing
  nil-guard pattern for `Metrics` and `Logger`.
- **`MemoryReplayer.Dequeue` blocks forever after `Close`**: After `Close()`
  was called with both queues empty, `Dequeue` fell into a blocking `select`
  with no escape path — the underlying channels are intentionally never
  closed to prevent Enqueue panics. The documented contract ("returns false
  when closed and empty") was not implemented. `Dequeue` now checks
  `closed && Len() == 0` before entering the blocking select; remaining
  queued items are still drained before returning false.
- **Phantom `replay.NewWorker` / `WithExecutor` / `WithMaxRetries` in docs**:
  The README "Minimal Production Example", `docs/adaptive-dual-write.md`, and
  `replay/doc.go` all referenced a non-existent `replay.NewWorker` constructor
  and phantom `WithExecutor` / `WithMaxRetries` options. The actual
  constructors are `replay.NewMemoryWorker` and `replay.NewNATSWorker`.
  All three files have been corrected to use real API symbols.

### Tests

- 29 unit tests for FallbackRead behavior, error classification, metrics,
  drain bypass, chaining, precedence, and single-cluster mode.
- 16 integration tests against real Cassandra clusters:
  - 4 adapter round-trip tests proving `gocql.ErrNotFound` → `types.ErrNotFound`
    mapping through v1 and v2 adapters (Scan + MapScan).
  - 5 FallbackRead end-to-end tests: write-to-one-cluster read-back, MapScan
    variant, both-not-found, primary-has-data short-circuit, v2 adapter.
  - 1 partial-write + replay convergence scenario.
  - 1 primary-error + failover-not-found returns real error (not false
    not-found).
  - 3 metrics verification: divergence on stale cluster, not-found never trips
    failover, ReadTotal accounting for both clusters.
  - 2 activation-level tests: `WithDefaultFallbackRead(true)`,
    `WithFallbackRead(ctx)`.

---

## [1.0.0] — 2026-04-09

This release stabilizes the public API for long-term support. All interfaces
are now minimal and free of deprecated methods; the concrete adapter types
retain full backward compatibility for callers who used the deprecated API.

### Breaking Changes

The following methods have been **removed from the `CQLSession`, `Query`, and
`Batch` interfaces** in the root `helix` package and `adapter/cql` package.
They remain available on the concrete types in `adapter/cql/v1` and
`adapter/cql/v2` for callers who depend on them directly.

#### `CQLSession` / `adapter/cql.Session`

| Removed method | Replacement |
|---|---|
| `NewBatch(kind BatchType) Batch` | `Batch(kind BatchType) Batch` |
| `ExecuteBatch(batch Batch) error` | Use `Batch(...).Exec()` |
| `ExecuteBatchCAS(batch Batch, dest ...any) (bool, Iter, error)` | Use `Batch(...).ExecCAS(dest...)` |
| `MapExecuteBatchCAS(batch Batch, dest map[string]any) (bool, Iter, error)` | Use `Batch(...).MapExecCAS(dest)` |

#### `Query` / `adapter/cql.Query`

| Removed method | Replacement |
|---|---|
| `WithContext(ctx context.Context) Query` | Use `ExecContext`, `ScanContext`, `IterContext`, `MapScanContext`, `ScanCASContext`, `MapScanCASContext` |
| `SetConsistency(c Consistency) Query` | `Consistency(c Consistency) Query` |

#### `Batch` / `adapter/cql.Batch`

| Removed method | Replacement |
|---|---|
| `WithContext(ctx context.Context) Batch` | Use `ExecContext`, `ExecCASContext`, `MapExecCASContext` |
| `SetConsistency(c Consistency) Batch` | `Consistency(c Consistency) Batch` |

**Migration**: Replace `WithContext(ctx).Exec()` chains with the direct
`*Context(ctx)` variant (e.g. `.ExecContext(ctx)`, `.ScanContext(ctx)`).
These methods have been the recommended pattern since v0.4.0 and are
thread-safe, whereas `WithContext` mutated shared state without synchronization.

### Bug Fixes

#### `adapter/cql` — context propagation and nil-safety (v1 and v2 adapters)

- **v1 `Batch.ExecCASContext` / `MapExecCASContext`**: `batch.WithContext(ctx)`
  was called but its return value was discarded (gocql v1 returns a copy).
  Context was never applied; canceled contexts were silently ignored.
- **v2 `Query.ScanCAS` / `MapScanCAS` / `Batch.ExecCAS` / `MapExecCAS`**: The
  context stored by the deprecated `WithContext` call was not forwarded to the
  underlying driver, so a pre-canceled context had no effect.
- **v1 / v2 `NewSession(nil)`**: Passing a nil driver session silently created
  a deferred nil-dereference trap that bypassed `NewCQLClient`'s nil guard.
  `NewSession` now panics immediately with a descriptive message.
- **v2 doc.go**: Corrected wrong import path for `cassandra-gocql-driver`.

#### `policy` — data races, TOCTOU, and resilience gaps

- **`CircuitBreaker`**: Added per-cluster mutexes to serialize the
  load→check→store compound in `RecordFailure`/`RecordSuccess`, eliminating a
  TOCTOU race that could lose failure counts or emit duplicate metrics.
  Introduced one-shot trip flag so the `circuit-opened` metric fires exactly
  once per trip. Changed `clusterNames` to `atomic.Pointer` so
  `SetClusterNames` is race-free with concurrent record calls.
- **`LatencyCircuitBreaker`**: Added `WithLatencyMetrics` and
  `WithLatencyLogger` options; previously the embedded `CircuitBreaker` was
  always wired to no-op implementations, silently dropping all circuit-trip
  metrics and log events.
- **`SyncDualWrite.Execute`**: Added `ctx.Err()` check between the two
  sequential writes; skips the second write and returns `ctx.Err()` when the
  context is already canceled.
- **`AdaptiveDualWrite.ForceDegrade`**: Acquires mutex and resets `fastStrikes`
  atomically with the `isDegraded` transition, preventing stale fast-strike
  credit from immediately recovering a force-degraded cluster.

#### `policy` — `AdaptiveDualWrite` correctness (6 fixes)

- **Both-async returns `DualClusterError` (P0)**: When both clusters were in
  degraded (fire-and-forget) state, `executeDualWrite` returned
  `DualClusterError` and enqueued no replay. Now correctly returns
  `ErrWriteAsync` and enqueues replay for the non-nil result.
- **`recordFast` TOCTOU with `ForceRecover`**: Mutex acquired before
  `isDegraded` check so `slowStrikes` is always reset on a healthy cluster.
- **Counter compound transitions**: Replaced separate `atomic.Int32` fields
  with `sync.Mutex` + plain `int32` to make strike counter transitions
  race-free; `isDegraded` remains `atomic.Bool` for fast-path reads.
- **`ErrWriteDropped` recorded as slow strike**: Excluded `ErrWriteDropped`
  from `recordStrike` to avoid penalizing already-degraded clusters.
- **Duplicate delta strike**: `processLatencies` now skips the delta strike
  when the absolute-cap already fired in the same cycle.
- **Metric classification**: `ErrWriteAsync`/`ErrWriteDropped` are now
  classified as operational sentinels (emit `IncWriteAsync`/`IncWriteDropped`)
  rather than errors (previously emitted `IncWriteError`). Added
  `IncWriteAsync` and `IncWriteDropped` to `MetricsCollector`.

### Tests

- Added 13 new integration tests covering adapter/cql fix verification and
  previously untested code paths:
  - 6 fix-verification tests in `context_test.go` proving Fix #1 (v1 CAS
    context drop) and Fix #4 (v2 CAS `WithContext` ignored) are corrected
  - 3 v1 gap tests: `ScanCAS`, `ScanCASContext`, `Batch.ExecCAS`,
    `Batch.ExecCASContext`
  - 4 v2 comprehensive tests: context methods, CAS operations, batch CAS,
    batch context operations
- Documented Cassandra LWT column ordering in test code: partition key first,
  then non-key columns alphabetically (not in `CREATE TABLE` order).

---

## [0.5.0] — prior release

See git history for changes prior to v1.0.0.
