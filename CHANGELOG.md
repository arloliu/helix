#

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `types.ReadRouteMetrics` (`SetReadPreferred`) and the
  `{prefix}_read_preferred{cluster}` gauge report which cluster
  `policy.StickyRead` or `policy.PrimaryOnlyRead` currently prefers, and
  the new `read_route_changed` cluster event reports every move with its
  reason (`failover`, `alternative known good`, `manual`, `recovered`).
  The client installs its collector and event dispatcher on a read strategy
  that implements `helix.Instrumentable` / `helix.EventEmitterSetter`.
- `types.ReplayStreamMetrics` lets a collector count the NATS worker's
  stream-level losses: `{prefix}_replay_corrupt_total{cluster}` (a fetched
  message that did not decode was terminated, also logged at `Error`) and
  `{prefix}_replay_term_failed_total{cluster}` (a `Term` the server
  refused). Under `RetryBounded` a refused `Term` on the last permitted
  delivery now records the `max_attempts` drop as well; previously neither
  was counted. See `docs/replay-system.md`.
- The cluster event buffer grows from 128 to 160 slots and the four
  per-operation kinds (`failover`, `read_divergence`, `replay_dropped`,
  `mirror_replay_dropped`) may hold at most 128 of them, so a storm of
  per-operation events can no longer evict a state transition; delivery
  stays in enqueue order. The policies' outbox drops (overflow, an emitter
  removed or panicking mid-delivery) now reach the dispatcher through the
  optional `types.ClusterEventDropReporter` interface and count in
  `{prefix}_cluster_events_dropped_total`; previously they were counted
  internally and never reported.
- `policy.StickyRead.SetPreferred(cluster)` and `policy.StickyRead.Reset()`
  let an operator move the read preference by hand.
- `replay.WithClusterGate(func(types.ClusterID) bool)` lets a replay worker
  hold execution back per cluster on both backends: a gated payload is
  parked without consuming an attempt or its retry window, a fetched NATS
  batch is kept in progress rather than NAK'd, and repeated gates compose by
  AND. `WithReplayGate(func(ClusterID) bool)` is the client's operator
  control; the client composes it with drain and installs the result on the
  worker it builds for `WithAutoMemoryWorker`, so replay never runs against
  a draining cluster. A worker supplied through `WithReplayWorker` must
  carry its own gate; the client warns at startup when drain or a replay
  gate is configured with one. Mirror workers are never gated by the source
  client. A payload the gate refuses after dequeue keeps its capacity slot
  while it is put back; the new drop reason `requeue_failed` guards that
  accounting invariant and is not expected in practice.
- `WithBehaviorProfile(Safe)` selects the defaults a future major version
  will adopt for the client-owned options kept at their v1 value for
  compatibility; today that is `WithRouteVeto(true)`. It is pure option
  expansion, so a later option in the same call overrides it. Options owned
  by a policy or replayer constructor (`WithFailoverBelowThreshold`,
  `WithLatencyFailoverBelowThreshold`, the replay stream settings) keep
  their own defaults and their startup warnings.
- `policy.WithFailoverBelowThreshold(bool)` and
  `policy.WithLatencyFailoverBelowThreshold(bool)` let a failed read retry on
  the other cluster while the breaker is still closed. Default `false` keeps
  the v1 behaviour of returning the first `threshold-1` errors to the caller;
  a client logs a startup warning while the default is in effect. The
  breakers implement the new `FailoverBelowThresholdReporter` and
  `FailoverProbeReporter` interfaces, and `types.ProbeOutcome` carries a
  probe's result.
- `WithAutoRefreshFailureClassifier(fn)` and
  `DefaultAutoRefreshFailureClassifier` select which errors count toward
  auto-refresh; `types.ErrClusterTimeout` marks a Helix-owned write-leg or
  probe deadline that expired while the caller was still waiting.
- `WithRouteVeto(true)` lets a failover policy that implements the new
  `RouteVeto` interface steer ordinary reads away from a cluster.
  `policy.LatencyCircuitBreaker` implements it: while its breaker is open,
  reads that are not pinned to a paging cursor, not under an
  `AllowedClusters` override, and not CAS move to the other cluster when
  that cluster is neither draining nor vetoed, and a FallbackRead probe
  skips a vetoed alternative. Previously an open latency breaker never moved
  the next read away from the slow cluster. Off by default in v1; a client
  whose failover policy can veto logs a startup warning while it is off.
- `ExcludeWhileReplayBacklog(depth, threshold)` builds an `AllowedClustersFunc`
  that keeps reads away from a cluster while its replay backlog exceeds the
  threshold, so reads return to a recovered cluster only after its backlog
  has drained. `replay.MemoryReplayer.PendingByCluster` can be passed
  directly; sample `replay.NATSReplayer.PendingByCluster` into atomics first.
- `policy.WithAdaptiveMinDegradedDwell(d)` keeps a degraded cluster degraded
  for at least `d`, and `policy.WithAdaptiveRedegradeBackoff(window, maxDwell)`
  doubles that dwell on every strike-driven degrade that follows a recovery
  within `window`, up to `maxDwell`. Reaching the cap emits the new
  `write_flapping` cluster event (`types.EventWriteFlapping`). Both default
  to off.
- `WithClusterWriteTimeout(d)` bounds each cluster's leg of a dual write
  independently of the caller's context. A leg that exceeds `d` is replayed
  like any other failed leg and counts as that cluster's health failure,
  while the other leg's acknowledgement stands. It applies to normal and
  strict dual writes, including the background legs a degraded
  `AdaptiveDualWrite` dispatches; it does not apply to single-cluster
  writes, reads, or mirror writes. Default 0 (disabled).

### Behavior change

- `policy.StickyRead` moves its preference during the cooldown when the
  current preferred cluster fails and the other cluster is known good (it
  reported a success since its own last failure). Previously the cooldown
  pinned reads to a dead preferred for up to five minutes after a single
  blip on the other cluster; two clusters that keep failing in turn still
  do not oscillate.
- `policy.PrimaryOnlyRead` hands cluster A to exactly one caller as the
  recovery probe once the recovery timeout has elapsed; the other callers
  keep reading cluster B until that probe reports. Previously every
  concurrent caller was sent to A at once. A probe whose caller never
  reports expires after another recovery timeout.
- `policy.CircuitBreaker` and `policy.LatencyCircuitBreaker` no longer turn
  a caller's read into the recovery probe. Previously, once `resetTimeout`
  had elapsed since the last failure, `ShouldFailover` returned false so the
  next failed read reached the caller, and the following `RecordFailure`
  discarded the stale count and closed the open span with the reason
  `"reset timeout elapsed"`. Now the breaker stays open (`ShouldFailover`
  true) until a successful read on that cluster closes it or until the
  client's recovery probe, which reserves the breaker once the timeout has
  elapsed, succeeds; that close carries the new reason `"probe succeeded"`,
  and the `circuit_breaker_state` gauge reports 1 (half-open) while the
  probe is in flight. Failure counts are no longer reset by a quiet gap.
  A breaker that is never probed (`resetTimeout` 0, `WithRecoveryProbeDisabled`,
  single-cluster mode) stays open until a successful read.
- The recovery probe now runs whenever the write strategy implements
  `ProbeReporter` or the failover policy implements the new
  `FailoverProbeReporter`; a client with a built-in breaker and a plain
  write strategy therefore starts the probe loop. One probe per tick serves
  both.
- Auto-refresh counts only connectivity failures toward its failure
  threshold: errors that wrap `types.ErrClusterUnreachable` or the new
  `types.ErrClusterTimeout` (a `WithClusterWriteTimeout` leg or a recovery
  probe that exceeded its Helix-owned deadline). A schema or query error
  proves the session is reachable and no longer counts, so it cannot
  replace a healthy session. Restore the previous behaviour with one line:
  `helix.WithAutoRefresh(helix.WithAutoRefreshFailureClassifier(func(error) bool { return true }))`.
  A custom adapter that does not normalise driver errors needs its own
  classifier to trigger auto-refresh.
- The auto-refresh sustained-failure window is armed when a session is
  installed (construction, `SwapSession`, `RefreshSession`) instead of
  being satisfied by an empty last-success timestamp, so a burst of
  failures on a fresh client cannot trigger a refresh before the window
  has elapsed.
- Failures on `AdaptiveDualWrite` fire-and-forget legs and recovery-probe
  outcomes now reach the auto-refresh counters, so a degraded cluster whose
  session is dead is refreshed even when no synchronous traffic reaches it.
- `RefreshSession` closes the replaced session after a grace period of
  `AutoRefreshConfig.RefreshTimeout` instead of immediately, so in-flight
  operations that captured it can finish; `Close` closes it at once.
  Without `WithAutoRefresh` the grace period is zero and the previous
  behaviour is unchanged.
- The recovery probe now credits `AdaptiveDualWrite` recovery only when the
  probe itself was fast: under `WithAdaptiveAbsoluteMax` and within
  `WithAdaptiveDeltaThreshold` of the other cluster's last write (or under
  `WithAdaptiveMinFloor` when the other cluster has no baseline). A cluster
  whose probe query is cheap but whose writes are still slow is no longer
  restored by the probe. The new `helix.ProbeLatencyReporter` interface
  carries the probe's latency; strategies that only implement
  `ProbeReporter` keep receiving `RecordProbeSuccess`.
- `AdaptiveDualWrite.ForceDegrade` is now a sticky operator latch: fast
  background writes and successful recovery probes no longer restore
  synchronous writes on a cluster degraded by hand, and the client skips the
  recovery probe for a latched cluster. Only `ForceRecover` or `Reset`
  clears the latch. Previously a healthy cluster was restored by the probe
  within a few ticks of `ForceDegrade`, silently undoing the operator's
  isolation. The new `helix.LatchReporter` interface and
  `AdaptiveDualWrite.IsLatched` expose the latch.
- `replay.NATSReplayer.PendingByCluster` counts the stream's messages for
  the cluster instead of the consumers this replayer has created, so a
  freshly started process (or one whose worker is gated and has fetched
  nothing yet) reports the durable backlog. Previously such a replayer
  reported 0, which let `helix.AllowedClusters` re-admit reads to a cluster
  whose backlog had not drained.
- `topology.NewNATS` rejects a zero or negative `WithPollInterval` with an
  error. Previously the value was accepted and the watcher panicked when it
  first fell back to polling.

### Changed

- Session-liveness stats (the auto-refresh detector's consecutive failure
  count, last success, and last error) now live on the installed session
  rather than on the client: `SwapSession` and `RefreshSession` install a
  session with fresh stats, and an operation that started on the replaced
  session reports to that session, never to the new one. All health
  observations now enter through one internal observation hub with a fixed
  authority order; the read strategy, failover policy, and latency recorder
  receive exactly the calls they received before.
- The recovery probe logs a failing probe at `Warn` on the first failure and
  on every power-of-two count of consecutive failures, and at `Debug`
  otherwise; the first success after failures is logged at `Info`.
  Previously every failure was a `Debug` line.
- `topology.NATS` retries the KV watch on every poll tick after falling
  back to polling, so a transient watch failure no longer leaves the watcher
  polling for the rest of its life.
- `replay.MemoryReplayer` keeps a separate queue pair per target cluster and
  alternates between clusters on dequeue, so a backlog for one cluster no
  longer delays the other cluster's replays. Each channel is still sized to
  the full capacity, so the reserved buffer is now about four times
  `WithQueueCapacity` instead of two; the enforced pending limit is unchanged.

## [1.7.0] — 2026-09-03

### Behavior change

- `replay.RetryWhileRetained` is now the default retry policy on both
  replay backends: a payload is retried, with exponential backoff, for as
  long as it is retained (the memory worker's `WithRetryWindow`, default
  24 h; the NATS stream's `MaxAge`), and only attempts the classifier marks
  dead-letter consume the `MaxAttempts` poison budget. Previously the
  default budget was five attempts, about 1.5 s on the memory backend, so
  any longer outage lost every replayed write. The NATS consumer is now
  created with unlimited deliveries and delayed redelivery. Restore the
  old policy with one line: `replay.WithRetryPolicy(replay.RetryBounded)`.
- A FallbackRead probe no longer contacts a draining alternative: drain is
  the operator's signal that the cluster should not serve reads, and a
  draining cluster may hold stale rows. The probe returns not-found
  without asking, as the slice methods already did. Restore the previous
  `Scan` / `MapScan` behaviour with one line:
  `helix.WithFallbackReadOnDrainingCluster(true)`.
- A dual-cluster write that no cluster acknowledged synchronously now
  returns `*types.NoSynchronousAckError` instead of `nil`. This covers every
  leg pair without an acknowledgement: fire-and-forget plus
  fire-and-forget (both clusters degraded under `AdaptiveDualWrite`),
  fire-and-forget or dropped plus a failure, dropped plus dropped, and a
  draining leg beside any of them. The error names each leg's result and
  whether the write was admitted to the replay queue, and matches
  `errors.Is(err, types.ErrNoSynchronousAck)`. A write with one
  acknowledgement still returns `nil`. A leg still running in the
  background counts as admitted provisionally: its failure is enqueued
  when it completes, and a failure to enqueue it then is reported only
  through `WithOnReplayDropped` and `EventReplayDropped`. Restore the
  previous result for queued writes with one line:
  `helix.WithAckMode(helix.AckOnReplayAdmission)`; a value outside the two
  declared modes is rejected by `NewCQLClient`.
- Without a `Replayer`, a leg that needed replay is now counted in
  `IncReplayDropped` and reported through `WithOnReplayDropped` and
  `EventReplayDropped` with `types.ErrNoReplayer`; previously it was
  silently lost.

### Fixed

- Replay carries every common CQL argument type through the NATS wire
  format: `*big.Int` (varint), `*inf.Dec` (decimal), `net.IP` (inet), and
  CQL durations (a driver duration or the new `types.Duration`) now
  round-trip as equal values, and an empty `[]byte` stays an empty blob
  instead of becoming NULL. Previously varint, decimal, and duration
  arguments failed at enqueue, an inet argument failed on every replay
  attempt, and an empty blob replayed as a tombstone. Both replayers now
  reject an argument no backend can carry (a struct or user-defined type,
  or a map with non-string keys, or an invalid `net.IP`) at enqueue with
  `types.ErrUnsupportedReplayArg`, so the write is reported as a dropped
  replay on the memory backend as well. The extension types are carried
  inside slices, arrays, and string-keyed maps as well as at the top
  level, a nil slice or map replays as NULL, and the bundled adapters bind
  `types.Duration` inside a `[]types.Duration` and inside the decoded
  `[]any` / `map[string]any` collections.
- A write to a cluster that `AdaptiveDualWrite` has degraded is no longer
  replayed beside a background attempt that succeeds (an ambiguous failure
  such as a timeout can still lead to a replay of a statement the cluster
  applied). The fire-and-forget leg's result now implements the new
  `helix.DeferredWriteResult` interface, and the client enqueues replay
  for that leg only if the background write reports a failure, instead of
  eagerly as a safety net beside a write that then succeeds. `Close`
  waits for replaying dual writes in progress and for such legs before
  stopping the replay worker, so a failure reported during shutdown is
  still enqueued, and `AdaptiveDualWrite` releases its background slot
  before reporting the result. A `WithOnReplayDropped` handler must not
  call `Close`. A custom
  strategy that returns a plain `types.ErrWriteAsync` keeps the immediate
  enqueue.
- `Close` waits for the auto-refresh detector and topology watcher
  goroutines to exit before closing the sessions, so a refresh or drain
  update of theirs can no longer run after `Close` returns.
- `RefreshSession` only replaces the session it set out to replace. If a
  `SwapSession` or another refresh installed a different session while the
  refresher ran, it closes the refresher's session, keeps the newer one,
  and returns `types.ErrSessionReplaced` instead of closing the newer
  session.
- `Iter`, batch `IterContext`, and the CAS operations now avoid a draining
  cluster like every other read, because drain-aware re-selection moved
  into the shared read-target resolver. Previously only Scan, MapScan, and
  the slice reads honoured drain state.
- A paged read is sent to the cluster that issued its paging token.
  `Iter.PageState` now wraps the driver's token with the issuing cluster,
  and `Query.PageState` routes `Iter`, `SliceMap`, and `SliceScan` to that
  cluster regardless of the read strategy or drain state; an
  `AllowedClusters` override that excludes it fails the read with
  `types.ErrNoValidClusters`. Previously `Iter` re-resolved the cluster on
  every page and could ship one cluster's cursor to the other once the
  sticky preference moved. The token is Helix-specific and must be handed
  back to a Helix client; a driver token still works and resumes on the
  cluster the strategy selects.
- A write timestamp of zero is rejected: `WithTimestamp(0)` on a query or
  batch fails at execution with `types.ErrInvalidTimestamp`, and
  `NewCQLClient` rejects a `TimestampProvider` that returns zero with a
  `types.OptionError`. The drivers treat zero as "use the current time", so
  a replayed write would have carried a newer timestamp than the original
  and overwritten later data. `DefaultExecuteFunc` and the built-in mirror
  executor reject a payload with a zero timestamp (one enqueued before this
  release) with `types.ErrInvalidTimestamp`, which
  `replay.DefaultReplayClassifier` dead-letters.
- `Iter.Close` now reports its outcome to the failover policy and the read
  strategy like every other read: a clean close is a `RecordSuccess`, and a
  cluster error is a `RecordFailure` plus `OnFailure` (the suggested
  alternative is ignored because an iterator cannot be retried). Only the
  first `Close` reports; a repeated `Close` returns the same error.
  Previously an iterator-heavy workload never tripped a circuit breaker,
  never moved the sticky preference, and a clean close never reset a
  breaker.
- `LatencyRecorder` documents that `RecordLatency` stands in for
  `RecordSuccess` on the read path and must reset the failure counter for a
  fast sample. Calling both would let `RecordSuccess` erase the slow-read
  count a latency breaker accumulates, which is why the client calls only
  `RecordLatency` for such policies.
- An error observed after the caller's context was cancelled or expired is
  no longer counted as a cluster failure. On every read entry point it is
  returned as-is without `IncReadError`, `RecordFailure`, `OnFailure`, the
  auto-refresh failure counter, or a failover attempt with the dead context,
  and a `FallbackRead` probe is not started with a dead context (the
  caller's context error is returned instead); on writes the leg is
  replayed exactly when a failed leg would be (a pair of unacknowledged
  hard failures still returns `DualClusterError` without replay) but
  neither `IncWriteError`, the auto-refresh counter, nor an
  `AdaptiveDualWrite` strike records it; a failover attempt is skipped
  once the caller's context has ended. A
  context error the driver reports while the caller's context is still
  live is a cluster error like any other. Previously one cancelled request
  counted as a failure on both clusters and could flip the sticky read
  preference.
- The NATS worker sends one negative acknowledgement per failed attempt
  under `RetryWhileRetained`. Previously a plain `Nak` preceded the delayed
  one, so JetStream redelivered immediately and logged the delayed call as
  already acknowledged; the documented backoff never applied.
- The built-in mirror executor applies the captured consistency and serial
  consistency levels, as `DefaultExecuteFunc` does; previously a mirrored
  write ran at the destination's session defaults.
- A paging token from `Iter.PageState` carries a checksum, so a driver
  token that happens to start with the Helix header is passed through
  unchanged instead of being misread.
- A concurrent `Close` returns only after the first one finished shutting
  the client down; previously it returned immediately.

### Added

- Replay preserves the consistency and serial consistency a write set.
  `types.ReplayPayload` gains `Consistency` and `SerialConsistency`
  (nil for a session-default write), the client records them on every
  replay and mirror payload, and `DefaultExecuteFunc` applies them. The
  NATS envelope is now versioned: version 2 messages carry the levels and
  the non-idempotent marker, and workers keep reading version 1 messages.
  Upgrade every worker before any publisher: an older worker replays a
  version 2 message at its session default, as it did before, but cannot
  bind the varint, decimal, inet, and duration encodings that are new in
  this release and drops such a message after its retries. `USING TTL`
  drift on replay is documented.
- `Query.NonIdempotent` and `Batch.NonIdempotent` mark a statement that
  must not be applied twice, such as a counter update or a collection
  append. The write takes the strict path (synchronous on both clusters,
  no fire-and-forget, no replay) and a partial failure surfaces as
  `*types.PartialWriteError`; unlike `Strict`, it may be combined with
  `Mirror`: `types.ReplayPayload.NonIdempotent` carries the marker, so
  the destination executes the statement on its strict path and never
  replays it within its own pair (mirror delivery itself may still be
  retried, as before). A `CounterBatch` is non-idempotent automatically,
  so it is no longer replayed.
- `helix.DeferredWriteResult`: the optional interface on a write strategy's
  fire-and-forget leg result through which the client learns the leg's
  final outcome.
- `helix.ProbeReporter`, `helix.EventEmitterSetter`, `helix.Instrumentable`,
  and `helix.LoggerSetter` name the optional capabilities the client
  discovers on a write strategy, failover policy, or replay worker by type
  assertion: recovery probing, cluster-event emission, and metrics / logger
  injection. They were previously unexported, so a custom strategy could
  not tell from the documentation how to opt in.

### Changed

- `CQLClient.Config` returns a copy of the effective configuration instead
  of the client's live pointer, so assigning to its fields after
  construction no longer changes the client. Callers that set
  `Config().ReplayWorker` after building a worker with `DefaultExecuteFunc`
  must instead stop that worker themselves before `Close`, as
  [docs/replay-system.md](docs/replay-system.md) shows.
- `NewCQLClient` rejects `WithAutoMemoryWorker` combined with `WithReplayer`
  or `WithReplayWorker` with a `types.OptionError`; previously the
  auto-built replayer and worker silently replaced the caller's. It also
  logs a warning for `WithMirrorReplayer` without `WithMirror` and for
  `WithRecoveryProbe` with a write strategy that does not report degraded
  clusters, both of which have no effect.
- `ClientConfig` no longer exposes the components `NewCQLClient` builds:
  the `MirrorEngine` and `MirrorReplayWorker` fields are gone. The mirror
  engine is still available through `CQLClient.Mirror`. This is a
  source-incompatible change for code that read those fields; it is the
  only exported removal in this release.

- A write to a dual cluster with one cluster draining now runs through the
  configured `WriteStrategy` like every other write: the draining cluster's
  leg returns `types.ErrClusterDraining` without contacting the session and
  is enqueued for replay, while the other leg is executed by the strategy.
  Previously the healthy cluster was written directly, bypassing the
  strategy. Observable differences: the draining cluster now counts in
  `IncWriteTotal` and, when the collector implements `types.StrictMetrics`,
  in `IncWriteSkipped` (previously only strict writes did); the replay log
  line reads "write skipped on draining cluster, enqueued for replay".
  `AdaptiveDualWrite` no longer counts a draining leg as a background write
  error.

## [1.6.1] — 2026-09-03

### Added

- `replay.WithRetryPolicy(replay.RetryWhileRetained)`: an opt-in worker
  policy that keeps retrying a payload for as long as it is retained (the
  memory worker's `WithRetryWindow`, default 24 h; the NATS stream's
  `MaxAge`) instead of dropping it after a fixed number of attempts.
  Failed attempts are classified by a `replay.ReplayClassifier`
  (`DefaultReplayClassifier`, override with `WithReplayClassifier`) into
  `DispositionDefer`, `DispositionRetry`, or `DispositionDeadLetter`; only
  dead-letter attempts consume the poison budget (`MaxAttempts` on both
  backends). Under this policy the memory worker holds a
  payload's queue slot until it succeeds or is dropped, so `Len()` counts
  waiting payloads and new enqueues fail loudly at capacity; the NATS
  worker creates its consumers with unlimited deliveries and requests
  redelivery with `NakWithDelay` on the existing `RetryDelay` /
  `MaxRetryDelay` schedule. The default policy, `RetryBounded`, is
  unchanged. See [docs/replay-system.md](docs/replay-system.md#retry-policies).
- `types.ErrClusterUnreachable`: both CQL adapters wrap driver errors that
  mean the cluster could not be reached (no connections, closed session,
  dropped connection, coordinator unavailable) in this sentinel while
  keeping the driver error reachable through `errors.Is` / `errors.As`.
- `types.ReplayBacklogMetrics` (optional `MetricsCollector` interface):
  `{prefix}_replay_oldest_age_seconds{cluster}` and
  `{prefix}_replay_worker_dropped_total{cluster,reason}`, implemented by
  `contrib/metrics/vm`.
- `replay.MemoryReplayer.PendingByCluster` and
  `replay.NATSReplayer.PendingByCluster` report the per-cluster backlog;
  `replay.NATSReplayer.Config` returns the effective configuration;
  `replay.ReplayMessage` gains `NakWithDelay`, `InProgress`, and
  `StreamSequence`.

### Changed

- Replay payloads copy byte-slice arguments on the failure path, so a
  caller buffer reused after `Exec` returns can no longer be replayed with
  the wrong bytes.
- A replay payload whose `TargetCluster` is neither `A` nor `B` is rejected
  at enqueue on both backends, terminated at NATS decode like a corrupt
  message, and refused by `DefaultExecuteFunc` with
  `types.ErrInvalidCluster`, instead of falling through to cluster B.
- The bundled workers now publish `{prefix}_replay_queue_depth{cluster}`
  (previously always 0): slots held per cluster on the memory backend,
  undelivered plus unacknowledged messages per cluster on the NATS backend.
- Under `RetryWhileRetained` the NATS worker marks each message in progress
  before executing it, so a batch that takes longer than `AckWait` is not
  redelivered while it is still being worked through.
- `NewCQLClient` logs a warning when a NATS replayer keeps a single stream
  replica or the `DiscardOld` policy.
- Memory worker drop reasons in the log are now `max_attempts`,
  `retry_pool_saturated`, and `shutdown` (previously free-form text), and
  the worker option docs no longer claim retry jitter, which was never
  implemented.

### Fixed

- `docs/replay-system.md` stated that NATS retry backoff is controlled by
  `AckWait`; the bounded policy requests immediate redelivery. The guide
  now documents each backend's effective survival window, stream isolation
  per deployment, and how the backlog follows a cluster slot across
  `SwapSession`.

## [1.6.0] — 2026-07-30

### Added

- `helix.WithOnClusterEvent`: register a handler for typed cluster-health
  events (`types.ClusterEvent`) — failover, read divergence, circuit
  breaker open/close, adaptive-write degrade/recover, drain enter/exit,
  replay drops, mirror replay drops (unless a caller-supplied
  `mirror.WithOnError` replaces the internal mirror error handler), and
  session refresh attempt/success/error. Delivery is asynchronous and
  best-effort on a dedicated goroutine; circuit-breaker and adaptive-write
  events are delivered in per-cluster transition order, per policy instance.
  Registering the handler is not sufficient on its own: most kinds are
  produced by an optional component (a circuit-breaker failover policy, an
  adaptive write strategy, a replayer, a topology watcher, auto-refresh) and
  stay silent without it — the constructor logs one Info line listing the
  kinds left unreachable by the configuration. See
  [docs/cluster-events.md](docs/cluster-events.md) for the per-kind
  prerequisites.
- `policy`: `SetEventEmitter` on `CircuitBreaker`, `LatencyCircuitBreaker`,
  and `AdaptiveDualWrite` for standalone (non-`CQLClient`) usage; the
  emitter is always invoked outside policy state locks.
- `policy.AdaptiveDualWrite` now logs degrade/recover transitions
  (previously silent) and emits a recovery event from `Reset`.
- `types.AdaptiveWriteMetrics` (optional `MetricsCollector` interface):
  `AdaptiveDualWrite` records a degraded-state gauge and per-direction
  transition counters on collectors that implement it. `contrib/metrics/vm`
  exposes them as `{prefix}_write_degraded{cluster}` (1=degraded, 0=healthy),
  `{prefix}_write_degraded_total{cluster}`, and
  `{prefix}_write_recovered_total{cluster}`.
- `types.ClusterEventMetrics` (optional `MetricsCollector` interface): the
  cluster event dispatcher reconciles its drop total into collectors that
  implement it — from the dispatcher goroutine, never from the read/write
  hot path — so applications can alert on event loss.
  `contrib/metrics/vm` exposes it as
  `{prefix}_cluster_events_dropped_total`.
- `types.MirrorReplayMetrics` (optional `MetricsCollector` interface): the
  internal mirror error handler counts mirror captures that could not be
  enqueued for mirror replay. `contrib/metrics/vm` exposes it as
  `{prefix}_mirror_replay_dropped_total` (no cluster label — mirror targets
  a logical sink). A caller-supplied `mirror.WithOnError` replaces the
  internal handler and suppresses this metric along with the event.
  With these three interfaces every cluster event kind has a metric
  counterpart; existing by-hand `MetricsCollector` implementations remain
  source-compatible and opt in by adding the methods.
- `contrib/metrics/vm`: `WithDurationBuckets` option (validated: strictly
  increasing, finite, positive values) and `DefaultDurationBuckets()`
  accessor for the default bucket bounds.

### Changed

- **`contrib/metrics/vm` (dashboard migration required):** all
  `*_duration_seconds` histograms (read, write, replay, mirror exec) are
  now classic Prometheus histograms (`_bucket{le=...}`, `_sum`, `_count`)
  instead of VictoriaMetrics-native `vmrange` histograms.
  `histogram_quantile()` over `le` buckets now works with vanilla
  Prometheus, but existing `vmrange`-based dashboard queries against these
  metrics will no longer return data and must be rewritten against `le`
  buckets:

  ```promql
  histogram_quantile(0.99, sum(rate(helix_read_duration_seconds_bucket[5m])) by (le))
  ```

  Queries built on the `_sum` and `_count` series — average latency,
  throughput — are unaffected; only quantile queries break.

### Fixed

- **`policy.CircuitBreaker` / `policy.LatencyCircuitBreaker`: circuit
  breaker state gauge stuck at open.** When a tripped breaker went quiet
  for longer than its reset timeout, the next `RecordFailure` cleared the
  trip internally but never wrote `SetCircuitBreakerState(cluster, 0)`, and
  the following `RecordSuccess` saw an already-cleared flag and wrote
  nothing either. Any metrics collector kept reporting the breaker as open
  (`2`) for a cluster that was closed and serving, so alerts built on that
  gauge never cleared. The reset now records the close: it writes the gauge,
  logs the transition, and emits
  `types.EventCircuitBreakerClosed` with Reason `"reset timeout elapsed"`.
  A breaker whose stale failure count merely aged out without ever tripping
  is unaffected and still emits nothing. With threshold 1 the same call
  closes and immediately re-opens, so the gauge correctly ends at open.
- **`policy.CircuitBreaker` / `policy.LatencyCircuitBreaker`: circuit
  breaker state gauge could report closed for a cluster whose breaker was
  actually open.** A transition was latched under the cluster's state
  mutex but its gauge write and log line happened after that mutex was
  released, so two transitions on the same cluster could report out of
  order: a call that closed the breaker, if descheduled before writing the
  gauge, could still write `SetCircuitBreakerState(cluster, 0)` after a
  concurrent re-trip had already written `2` — the inverse of the stuck-open
  bug above. Each cluster's transitions are now sequenced, and a call writes
  the gauge and the log line only while its transition is still the newest
  one latched for that cluster; an older call that lost the race skips both
  writes rather than overwriting a state the breaker has already left. Its
  `types.ClusterEvent` is still delivered, in order. One consequence worth
  knowing: because a superseded transition no longer logs, a count of
  "circuit breaker tripped" log lines can now run below
  `{prefix}_circuit_breaker_trips_total`, which counts every trip whether or
  not it was later superseded.

## [1.5.3] — 2026-07-12

### Security

- **Dependency upgrade resolving `GO-2026-5052`**:
  `software.sslmate.com/src/go-pkcs12` 0.6.0 → 0.7.3 — reachable via
  testcontainers' Cassandra module from `test/testutil`, not from any path a
  library consumer's binary pulls in.
- **Minimum Go version raised `1.25.0` → `1.26.0`**. Helix does not pin a
  `toolchain` directive, so consumers build with whatever Go 1.26.x patch
  they have installed — picking up upstream `crypto/tls`, `crypto/x509`,
  `net`, and `net/http` CVE fixes shipped in later 1.26 patch releases is
  the build environment's responsibility, not something this library forces.

### Changed

- **Dependency bumps** (direct): `VictoriaMetrics/metrics` 1.40.2 → 1.44.0,
  `apache/cassandra-gocql-driver/v2` 2.0.0 → 2.1.2, `nats-server/v2` 2.12.6
  → 2.14.3, `nats.go` 1.49.0 → 1.52.0, `testcontainers-go` (+ `cassandra` /
  `scylladb` modules) 0.42.0 → 0.43.0, `moby/moby/client` 0.4.0 → 0.5.0,
  `tinylib/msgp` 1.6.1 → 1.6.4.
  - Transitive deps pulled forward by `go mod tidy`: `golang.org/x/crypto`,
    `golang.org/x/sys`, `nats-io/jwt/v2`, `nats-io/nkeys`, `otel`,
    `otel/metric`, `otel/trace`, `otel/contrib/otelhttp`,
    `antithesishq/antithesis-sdk-go`, and others.
  - No source changes — `go.mod`/`go.sum` only. Wire behavior and public
    API are unaffected. Verified with `go build`, `make lint`, `make
    vet`, `make test-unit` (race detector), and `make test-integration`
    (real Cassandra/ScyllaDB containers via testcontainers).

## [1.5.2] — 2026-07-11

### Security

- **Dependency bumps resolving 29 Dependabot alerts** (7 critical, 10 high,
  12 moderate):
  - `golang.org/x/crypto` 0.48.0 → 0.52.0 (12 alerts, up to critical —
    SSH auth bypass, deadlocks, panics).
  - `github.com/nats-io/nats-server/v2` 2.12.2 → 2.12.6 (15 alerts, up to
    high — pre-auth DoS, ACL/auth bypasses, credential exposure). Only
    reachable from `test/testutil` (embedded NATS for tests) and
    `examples/replay`, not from any path a library consumer's binary pulls
    in.
  - `go.opentelemetry.io/otel/sdk` / `otel/sdk/metric` 1.37.0 → 1.43.0 (2
    alerts, high — PATH hijacking / arbitrary code execution).
  - Transitive deps pulled forward by `go mod tidy` to satisfy the new
    requirements: `nats.go`, `nats-io/jwt/v2`, `nats-io/nkeys`, `otel`,
    `otel/metric`, `otel/trace`, `golang.org/x/sys`, `golang.org/x/time`,
    `antithesishq/antithesis-sdk-go`, `google/go-tpm`.
  - No source changes — `go.mod`/`go.sum` only. Wire behavior and public
    API are unaffected.

## [1.5.1] — 2026-07-11

### Performance

- **`adapter/cql/v2.Batch` allocation reduction**: The adapter kept its own
  `[]cql.BatchEntry` slice, appended in lockstep with the underlying gocql
  batch's `Entries` on every `Query` — a redundant second growing slice that
  only fed `Size()`/`Statements()`. Removed it; both now derive from the
  gocql batch. 100-statement batch: 17 → 9 allocs/op, 28352 → 16416 B/op.
- **`adapter/cql/v1.Batch` allocation reduction**: Same fix ported to the
  gocql v1 adapter. 100-statement batch: 17 → 8 allocs/op, 28368 → 16208
  B/op.
- Wire behavior, public API, and `Statements()`/`Size()` return values are
  unchanged — this is a build-time bookkeeping change only.

### Documentation

- **`cql.Batch.Statements()` contract pinned**: The interface godoc now
  states the returned slice is a fresh copy the caller may keep, and that
  the per-entry `Args` are shared with the batch and must not be mutated —
  matching what both adapters have always done.

### Tests

- New `TestBatchSizeAndStatementsDeriveFromGocql` and
  `BenchmarkAdapterBatchQuery` in both `adapter/cql/v1` and
  `adapter/cql/v2`, proving `Size`/`Statements` correctness and guarding
  the allocation win against regression.
- New `BenchmarkCQLBatchBuildExec` in the root package's benchmark suite,
  measuring the full single-cluster batch build/exec path.

## [1.5.0] — 2026-05-11

### Breaking Changes

#### Interface additions (compile-time)

The following methods were added to exported interfaces. **Custom implementations
must be updated to compile.**

| Interface | New methods | Purpose |
|---|---|---|
| `helix.Query` / `adapter/cql.Query` | `MaxRows(n int) Query` | Per-query row cap for slice methods |
| `helix.Query` / `adapter/cql.Query` | `SliceMap() ([]map[string]any, error)` | Bounded multi-row read into maps |
| `helix.Query` / `adapter/cql.Query` | `SliceMapContext(ctx) ([]map[string]any, error)` | Context-aware variant of `SliceMap` |
| `helix.Query` / `adapter/cql.Query` | `SliceScan(fn func(RowScanner) error) (int, error)` | Bounded multi-row read via callback |
| `helix.Query` / `adapter/cql.Query` | `SliceScanContext(ctx, fn func(RowScanner) error) (int, error)` | Context-aware variant of `SliceScan` |

**Migration for `helix.Query` / `adapter/cql.Query` implementors**: Add the
five new methods. The `testutil.MockQuery` already implements them. For custom
mocks or decorators, wire through the delegate. Temporary compile-only stubs
must match each method signature: `MaxRows` returns the receiver/delegate,
`SliceMap` / `SliceMapContext` return `nil, nil`, and `SliceScan` /
`SliceScanContext` return `0, nil` while migrating.

### Added

- **Slice read methods** (`SliceMap`, `SliceMapContext`, `SliceScan`,
  `SliceScanContext`) on `Query` and `adapter/cql.Query`: bounded multi-row
  reads that materialize results into memory before returning. Unlike `Iter`
  (streaming cursor), slice methods drain the full result set — up to the
  configured row cap — and support `FallbackRead` empty-retry for recovering
  partitions lagging behind on one cluster.
  - `SliceMap` / `SliceMapContext`: returns `[]map[string]any`, one map per row
    keyed by column name. Zero-row result: `(nil, nil)`.
  - `SliceScan` / `SliceScanContext`: invokes a callback once per row with a
    `RowScanner`; returns `(rowCount int, err error)`. Zero-row result:
    `(0, nil)`. Does not participate in the standard failover path
    (primary-error → retry secondary); `FallbackRead` empty-retry still applies.
- **`RowScanner` interface** (`helix`): narrow scan surface passed to
  `SliceScan` callbacks. Exposes only `Scan(dest ...any) error` — callbacks
  cannot accidentally advance or close the underlying iterator.
- **`MaxRows(n int) Query`** method on `Query`: per-query row cap for slice
  methods. When the (N+1)th row is read, the method aborts with
  `ErrRowLimitExceeded` and discards the partial accumulator. `MaxRows(0)` on a
  query clears the per-query override and falls back to `Config.DefaultMaxRows`.
  Has no effect on `Scan`, `MapScan`, `Iter`, `Exec`, or CAS operations.
- **`Config.DefaultMaxRows`** and **`WithDefaultMaxRows(n)`**: client-wide
  default row cap for all slice methods. Unset (0) means no cap — drain is
  unbounded. Per-query `MaxRows(n>0)` always wins over the client default.
- **`ErrRowLimitExceeded`** sentinel (root package and `types`) and
  **`IsRowLimitExceeded`** helper (root package): application-level cap signal,
  not a cluster fault. Helix never records it as a read error, never advances
  circuit-breaker / auto-refresh state, and never triggers `FallbackRead`
  empty-retry. Check with `helix.IsRowLimitExceeded(err)` or
  `errors.Is(err, helix.ErrRowLimitExceeded)`.
- **`SliceScanAs[T]`** generic free function (root package): typed helper
  layered over `SliceScanContext` that returns `[]T` instead of using a
  side-effecting accumulator. Returns `(nil, nil)` on empty drain; returns
  `(nil, err)` on any error without exposing the partial accumulator. All
  `Query` options — `FallbackRead`, `MaxRows`, `PageState`, etc. — apply
  transparently through the underlying `SliceScanContext` call. Use `SliceScan`
  directly when partial results on error are required.
- **FallbackRead extended to slice methods**: `FallbackRead()` on
  `SliceMap`/`SliceScan` retries the query against the alternative cluster when
  the primary returns zero rows. Slice-specific behaviors differ from
  `Scan`/`MapScan`: zero-row results return `(nil, nil)` / `(0, nil)` rather
  than `ErrNotFound`; when the alternative cluster is draining, the fallback
  attempt is skipped rather than reading from a cluster in transition.
- **Page-size clamp**: when `MaxRows` is active, Helix clamps the gocql page
  size to `min(pageSize, maxRows+1)` before issuing the first request. The
  `+1` lets Helix detect the overflow row without fetching a second page,
  preventing over-fetching on large partitions with small caps.

### Bug Fixes

- **User-callback `ErrNotFound` incorrectly treated as slice empty-signal**: A
  `SliceScan` callback that deliberately returns `types.ErrNotFound` (e.g., to
  signal a malformed row) was misinterpreted by the FallbackRead pipeline as an
  empty-drain signal, triggering a spurious retry against the alternative
  cluster. The callback's `ErrNotFound` is now propagated as a real error and
  never treated as an empty-row indicator.
- **Single-cluster success-path policy gating regression**: A pre-refactor guard
  that invoked `OnSuccess` / `OnFailure` on the read strategy only for
  dual-cluster reads was accidentally dropped during the slice-read
  restructuring. Single-cluster reads now correctly advance strategy state on
  both success and failure paths.

### Documentation

- `docs/slice-read.md`: new guide covering all four slice methods, `MaxRows`
  semantics, page-size clamping, `FallbackRead` integration, `SliceScanAs[T]`,
  error handling table, and a method-selection guide.
- `docs/fallback-read.md`: updated to document slice-method integration —
  empty-result shape, drain-aware skip, and the full availability table for all
  affected read methods.
- Stale API references in `docs/mirror.md`, `docs/adaptive-dual-write.md`, and
  `docs/replay-system.md` corrected to match current exported symbols.

### Tests

- 94 unit tests across 6 test files covering slice method correctness, `MaxRows`
  bounding, page-size clamping, FallbackRead empty-retry on slice paths,
  drain-skip, `ErrRowLimitExceeded` classification (no failover, no health
  impact), `SliceScanAs[T]`, and nil-callback guards.
- 7 integration tests in `test/integration/cql_slice_read_integration_test.go`
  against real Cassandra clusters: `SliceMap`, `SliceScan`, `SliceScanAs`,
  `MaxRows` cap enforcement, `FallbackRead` multi-row recovery, `DefaultMaxRows`
  inheritance, and both-empty short-circuit.
- 5 e2e tests in `test/e2e/cql/slice_read_test.go`: container-level chaos
  covering slice read under partial cluster failure, `FallbackRead` divergence
  recovery, and `MaxRows` propagation through real gocql page boundaries.

### Internal

- **Two-layer read helper extracted**: the shared coordinator for primary-read
  then optional FallbackRead empty-retry was factored out of the single-row
  (`Scan`/`MapScan`) path to serve both the single-row and slice paths without
  duplication.
- **`go fix` modernizations**: applied `go fix` across the entire codebase to
  adopt current Go idioms (loop-variable capture, `any` aliases, etc.).
- **golangci-lint v2.12.2 compatibility**: lint errors for the updated linter
  version resolved; additional valuable rules enabled post-audit.

## [1.4.0] — 2026-05-10

### Added

- **Strict writes** (`Strict()` per-query/batch option): bypasses replay and
  fire-and-forget. Partial failure returns `*types.PartialWriteError` immediately;
  full failure returns `*types.DualClusterError`. Draining clusters are skipped
  and reported as unacknowledged. `Strict().Mirror()` is rejected before any
  write. No-op in single-cluster and CAS/LWT mode.
- **`StrictWriter` interface** (`helix`): optional interface for write strategies
  supporting strict semantics. Implemented by `ConcurrentDualWrite`,
  `SyncDualWrite`, and `AdaptiveDualWrite`. `AdaptiveDualWrite.ExecuteStrict`
  fast-fails degraded clusters with `ErrClusterDegraded`; `RecordProbeSuccess`
  advances the recovery counter.
- **Background recovery probe** for `AdaptiveDualWrite`: background goroutine
  probes degraded clusters (default 2 s interval) and restores them without
  operator action. Configure with `WithRecoveryProbe(cfg)` or disable with
  `WithRecoveryProbeDisabled()`. Stops cleanly on `Close()`.
- **Async mirror writes** (`Mirror()` per-query/batch option): fire-and-forget
  writes to a second `*CQLClient` for zero-downtime Cassandra migrations. Fires
  only after primary write success; preserves original timestamps for
  WRITETIME/LWW/TTL correctness; deep-copies args before return. Two modes:
  in-process (`helix.WithMirror(target)`) and out-of-process
  (`helix.WithMirrorPublisher(publisher)` + `helix.NewMirrorWorker`). Mutually
  exclusive — `NewCQLClient` returns `types.ErrMirrorModeConflict` if both are
  set. Failed mirror writes retry via `helix.WithMirrorReplayer`. Runtime
  control via `*CQLClient.Mirror()` (`Enable`, `Disable`, `Enabled`, `Stats`).
  Optional `types.MirrorMetrics` interface auto-wired from the client's
  `MetricsCollector`.
- **New optional metrics interfaces** (`types`): `StrictMetrics` adds
  `IncWriteSkipped(cluster)`; `RecoveryProbeMetrics` adds
  `IncRecoveryProbeSuccess` / `IncRecoveryProbeFailure`. Neither breaks existing
  `MetricsCollector` implementations.
- **Root re-exports** (`helix`): `PartialWriteError`, `DualClusterError`,
  `ErrClusterDegraded`, `ErrClusterDraining`, `ErrStrictUnsupported`,
  `ErrStrictMirrorUnsupported`, `AsPartialWriteError`, `IsPartialWrite` —
  accessible without importing `types`.
- **e2e coverage expansion**: new parameterized e2e tests covering network
  disconnect failover, NATSReplayer drain, cascading A→B→A failures,
  `CircuitBreaker` trip/half-open/close cycle, `FallbackRead` real-cluster paths,
  and mirror scenarios with `Pause`/`Unpause`, `AdaptiveDualWrite`, `Stop`/`Start`,
  and SIGKILL + `AutoRefresh` recovery.
- **Strict option validation**: `types.OptionError` and `types.IsOptionError` for
  structured constructor error handling. Checked constructors —
  `policy.NewAdaptiveDualWriteChecked`, `policy.NewCircuitBreakerChecked`,
  `policy.NewLatencyCircuitBreakerChecked` — return joined `*types.OptionError`
  on invalid inputs. `NewNATSReplayer` validates all option fields at
  construction. `NewCQLClient` validates root options (auto-refresh knobs,
  recovery probe, mirror mode conflicts) before any side effects on
  caller-supplied strategies, policies, or workers; invalid `WithAutoMemoryWorker`
  options also surface as construction errors.

### Bug Fixes

- **`FailoverPolicy` metrics/logger not auto-injected**: `CircuitBreaker` and
  `LatencyCircuitBreaker` now implement `metricsAware`/`loggerAware` so
  `helix.WithMetrics(collector)` auto-wires failover policy observability.
  Explicit `WithCircuitBreakerMetrics` / `WithLatencyMetrics` calls still win.
- **`MemoryReplayer.Len()` is not a drain-done signal**: payloads in-flight
  between retry attempts are outside the queue, so `Len()` can transiently
  report 0 while work remains. Added Godoc note; the S1 e2e test now uses
  row-count convergence as the authoritative drain signal.
- **NATS replay worker hardening**: `Stop` is now terminal (restarting returns an
  error). Invalid startup options are rejected rather than silently normalized.
  Dequeue correctly honours context cancellation. Broker ack, nak, and term
  failures are surfaced instead of being swallowed as uncertain success or silent
  drop. NATS stream names and subject prefixes with surrounding whitespace are
  rejected (`strings.TrimSpace(input) != input`).
- **`StickyRead` / `RoundRobinRead` input validation**: `StickyRead` rejects
  negative cooldown durations and unknown preferred-cluster IDs. `RoundRobinRead`
  fails closed for unknown cluster IDs.
- **`NewCQLClient` side-effect atomicity**: `propagateClusterNames` and the
  missing-`Replayer` warning now run only after all validation passes — rejected
  configurations no longer mutate caller-supplied components.

### Documentation

- `docs/strict-write.md`: usage, behavior table, error types, `PartialWriteError`
  helpers, recovery probe configuration, drain interaction, and `StrictWriter`
  implementation guide.
- `docs/mirror.md`: target/publisher modes, semantics, runtime control,
  observability, and known parity gaps. `examples/mirror/` runnable examples.

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
