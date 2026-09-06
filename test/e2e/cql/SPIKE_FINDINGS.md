# e2e/cql spike findings (2026-05-06)

Run via `go test -tags spike -v -run TestSpike ./test/e2e/cql/...` against
Cassandra (test host has no AIO; ScyllaDB module fell back). Captured before
implementing the e2e scenario suite.

## 1. Stop / Start with the testcontainers Cassandra module — root cause identified, still not viable

Verified after upgrading testcontainers-go `v0.40.0 → v0.42.0`. The exact
JVM error captured from the restart logs:

```
ERROR [main] CassandraDaemon.java:900 - Fatal configuration error
org.apache.cassandra.exceptions.ConfigurationException:
  Cannot change the number of tokens from 1 to 16
```

Root cause: the testcontainers Cassandra module writes
`num_tokens=1` directly into `cassandra.yaml` during first-boot
initialization. On restart, the image default `num_tokens=16` conflicts
with the persisted `system` keyspace, and Cassandra refuses to boot.
Setting `CASSANDRA_NUM_TOKENS=16` via env var does NOT override the
module's direct yaml edit — the conflict persists either way (you'd
just get the inverse mismatch).

This is a fundamental incompatibility between the testcontainers
Cassandra module's first-boot config and Cassandra's restart
requirements. Fixing it in our codebase would require either a
custom container build or post-init yaml surgery — out of scope.

**Implication for the e2e suite**: Stop/Start scenarios MUST run on
Scylla, which survives the cycle cleanly. Pause/Unpause works on both
backends. Decision unchanged:

- Pause/Unpause is the primary failure mode for all e2e scenarios.
- Stop/Start scenarios are gated on `cluster.Type == ScyllaDB`.
- For real "process death" semantics that work on both backends, use
  `docker kill -SIGKILL` (see §9 below).

## 2. Pause / Unpause works reliably on both v1 and v2

Both drivers honored a `cluster.Timeout=3s`:

```
v1 paused-query wall=3.001854s err=*errors.errorString: gocql: no response received from cassandra within timeout period
v2 paused-query wall=3.002143s err=*errors.errorString: gocql: no response received from cassandra within timeout period
```

Post-unpause recovery is sub-10 ms on both:

```
v1 recovered after 1 attempts in 5.3ms
v2 recovered after 1 attempts in 3.4ms
```

So a Pause-based failure scenario can run a full cycle (pause → ops fail →
unpause → ops succeed) inside a few seconds.

**Amendment (2026-09-04):** that holds for a pause shorter than the v2
driver's heartbeat budget, and both the budget and what happens once it is
exceeded depend on the driver version.

Against upstream v2.1.2 and the fork through `v2.3.0-otter`, a pause long
enough to empty a host's pool left the v2 session answering `gocql: no hosts
available in the pool` for more than 30 s after the unpause even with
`ReconnectInterval = 500 ms`, while the v1 session recovered in
milliseconds: the driver marked a host down through an address lookup that
missed wherever the connect and broadcast addresses differ — which is every
port-mapped container — so `ReconnectInterval` never refilled the pool.
On the CI runner, whose pauses routinely last 10-12 s, the breaker and probe
scenarios hit exactly that and configured a `SessionRefresher` to rebuild the
session instead of waiting on the driver.

`v2.3.1-otter` fixes it: hosts are marked down by identity, a query against
an empty pool waits for the fill already in flight rather than failing on
it, and the heartbeat runs at one 5-second interval on every connection
regardless of the connection's age, so the worst case before a dead
connection is closed is 32.5-35 s at any `Session.Timeout` at or below the
5-second heartbeat-timeout floor — longer than any pause this suite takes.
The breaker and probe scenarios therefore no longer configure a session
rebuild, and assert native v1/v2 recovery parity again.

`ensureReachable` stays (see `setup_test.go`): it rebuilds the harness
sessions when a driver cannot answer a `system.local` read, which keeps a
scenario from starting against a session an earlier one left unusable.

For Helix users, `WithAutoRefresh` with a `SessionRefresher` remains the
answer whenever a session's driver cannot recover on its own — an address
change, or an older driver — rather than something this suite has to
configure to pass.

**Amendment (2026-09-06):** the fork's `v2.4.2-otter` changes what the driver
does *during* a pause, without changing what this suite observes after one.
Every `ReconnectInterval` tick that finds a host down now also re-reads the
peers tables, so a node that rejoined at a different address under the same
host id is rediscovered without a server event. At the suite's
`ReconnectInterval = 500 ms` (`setup_test.go`) a paused host therefore draws
two extra control queries per session every 500 ms for as long as it stays
down, and none once every host is up. Three defects on the same paths were
fixed with it: a control-connection reconnect that could wait on its own ring
refresh and hang `Session.Close`, pool admission and host removal that could
act on a host object the ring had already replaced, and a ring snapshot read
across two control connections that could evict the healthy new control host.
The last two are reachable precisely where the connect and broadcast addresses
differ, which is every port-mapped container this suite runs.

The full suite passes on `v2.4.2-otter` locally: 50 tests, 0 failures, 562 s.
The scenarios that hold a host down longest are the ones to watch; recorded
here so a later run has something to compare against —
`TestS3_PauseA_LatencyCircuitBreaker` 24.7 s,
`TestS_PlainCircuitBreaker_TripAndClose` 20.6 s,
`TestStrict_RecoveryProbe_StopAndStart_RestoresCluster` 14.4 s,
`TestStrict_RecoveryProbe_DefaultProbeRestoresCluster` 11.3 s. No per-test
baseline was captured on `v2.3.1-otter`, so these establish one rather than
confirm the timings are unchanged; every scenario passed its own recovery
assertions, which is the property that matters. Local green is
necessary but not sufficient here: the pause failure this section records was
only ever reproduced on the CI runner, so the CI `e2e` job stays the gate for
a driver bump.

**Amendment 2026-09-06 (`v2.5.0-otter`):** the fork halves default detection of
a silent node — the heartbeat timeout no longer derives from
`ClusterConfig.Timeout` and is now the exported `ClusterConfig.HeartbeatTimeout`
at 5 s, so a paused host's connections close in 30-35 s instead of 66-71 s.
The paused-host scenarios recorded above do not move, and that is the expected
result rather than a missing effect: every one of them is gated on a leg
timeout, a breaker threshold or a probe interval well under 30 s, so none was
ever waiting on heartbeat detection.
`TestS3_PauseA_LatencyCircuitBreaker` 24.65 s (was 24.7),
`TestS_PlainCircuitBreaker_TripAndClose` 20.62 s (was 20.6),
`TestStrict_RecoveryProbe_StopAndStart_RestoresCluster` 14.52 s (was 14.4),
`TestStrict_RecoveryProbe_DefaultProbeRestoresCluster` 10.21 s (was 11.3).
The full suite passes locally: 52 tests, 0 failures, 598 s (was 50 tests, 562 s;
the two additions are `TestS_PauseA_IterFirstPageMovesToTheOtherCluster` and the
new `TestS_PauseA_CloseReturnsDuringFault`).

`TestS_PauseA_CloseReturnsDuringFault` is where the fork's `Session.Close` work
is observed, and it is the suite's longest scenario at 82.7 s because it holds A
paused for 40 s per driver — long enough to outlast detection, so the driver's
own reconnect is running when the shutdown starts.
It is the only scenario built over dedicated driver sessions rather than the
shared ones, since `noCloseSession` would otherwise swallow the `Close` under
test.
Measured with A paused: `client.Close` 1.98 s on v1 and 86.9 µs on v2, a second
direct `Close` of each driver session in microseconds, and goroutines settled
within +5 of the pre-test count on v1 and at or below it on v2 across three
runs, against a tolerance of 10 that sits under one session's 12-13 goroutines.

One confound for a later comparison: the new file sorts between
`circuit_breaker_test.go` and `doc_test.go`, so it inserts a paused-A scenario
ahead of `dual_fail`, `fallback_read`, `iter_failover` and `lcb`.
If one of those shifts, test ordering is a candidate cause alongside the driver.
The CI `e2e` job stays the gate for a driver bump, as above.

## 3. v1/v2 errors.Is sentinel mismatch — INVALIDATED, spike test artifact

Original framing: "v1 `gocql.ErrTimeoutNoResponse` does NOT match v2 errors
via `errors.Is`, even though message text is identical." Suspected adapter
parity hazard.

Verification:

- v1 adapter (`adapter/cql/v1/adapter.go:14`) uses `errors.Is(err,
  gocql.ErrNotFound)` where `gocql` is `github.com/gocql/gocql`.
- v2 adapter (`adapter/cql/v2/adapter.go:14`) uses `errors.Is(err,
  gocql.ErrNotFound)` where `gocql` is `github.com/apache/cassandra-gocql-driver/v2`.
- Each adapter only checks its **own** driver's sentinels. There is no
  cross-package compare anywhere in Helix.
- Both adapters normalize to the unified `types.ErrNotFound`. Callers use
  `types.IsNotFound(err)` and don't need to know the driver.

Integration tests already confirm parity end-to-end:
`test/integration/cql_fallback_integration_test.go:36`
`TestV1Adapter_Scan_ReturnsErrNotFound` and `TestV2Adapter_Scan_ReturnsErrNotFound`.

The original spike finding came from a test that ran
`errors.Is(v2err, gocqlv1.ErrTimeoutNoResponse)` — a cross-package compare
that Helix never performs. The mismatch is real for that specific (and
incorrect) usage, but does not affect Helix's contract. No code fix
needed.

Worth noting for *future* design: only `ErrNotFound` is normalized today.
If callers ever need to distinguish e.g. timeouts from other errors at
the Helix level, a normalized `types.ErrTimeout` would be a clean
addition — but that is a feature, not a bug fix, and out of scope here.

## 4. v1 reconnect attempts during outage are **chatty and instant**

During the Cassandra outage window, gocql v1 emits a steady stream of
`gocql: unable to dial control conn ...` messages — multiple per second.
This means the driver is trying to reconnect aggressively. Helix's degradation
detection (`AdaptiveDualWrite` strikes) should be informed by these
reconnect failures, but only insofar as the Query/Exec call returns an
error. The control-conn chatter is logged but not raised to the API.

## 5. Container backend in CI / dev hosts

`testutil.IsAIOAvailable()` checks `aio-nr < aio-max-nr` and returns false
if AIO slots are exhausted; `StartCQLCluster` then falls back to Cassandra.

**Common gotcha: a long-running peer Scylla without
`--reactor-backend=epoll` consumes most of the AIO slots** (default
1,048,576 system-wide). With `aio-nr == aio-max-nr`, no further Scylla
container can register an AIO ring, so `IsAIOAvailable()` correctly
declines. Verified by stopping a 3-week-old peer Scylla and seeing
`aio-nr` drop from 1,048,576 to 0.

Remediations (in order of preference):

1. Stop or terminate any peer Scylla container hogging the AIO pool.
2. Configure peers to use `--reactor-backend=epoll` (skips AIO entirely).
3. Raise the system limit: `sudo sysctl -w fs.aio-max-nr=4194304`.

The testutil-managed Scylla already passes `--reactor-backend=epoll`
itself, so it doesn't perpetuate the problem — but it still needs a
non-empty headroom of unused AIO slots at startup time on some Scylla
versions.

## 5a. Stop/Start on Scylla — supported, but with port reassignment

Verified directly via the spike (`TestSpike_StopStartPortPreserved`):

- Scylla survives a clean Stop/Start cycle (Cassandra in this
  testcontainers configuration does not — see §1).
- **Docker reassigns the host port**: `localhost:32849 → localhost:32851`
  across one cycle. The original gocql session cannot auto-recover
  because its connect string points at the dead port.
- `cluster.Reconnect(ctx)` rebuilds both v1 and v2 sessions against
  the new mapping; queries succeed immediately on both drivers.

So real Stop/Start scenarios are viable on Scylla, but require the
caller to call `Reconnect` after `Start`. The `withRestoredCluster`
helper in setup_test.go already does this.

Caveat for Helix client testing: the helix CQLClient holds the gocql
sessions by value at construction time. After `Reconnect`, the cluster's
new sessions are accessible via `cluster.Session` / `cluster.SessionV2`,
but any helix.CQLClient built before Stop is still referencing the
old (now-dead) sessions. Tests must rebuild the helix client too.

---

## Findings from running the suite (2026-05-06)

### 6. Replay queue overflow under sustained load — REVISED to surface a real metric-undercounting gotcha

The original v1 unit reproducer (`cql_client_replay_drop_test.go:
TestReplayDrop_QueueOverflow_IsObservable`) asserted `enqueued +
dropped == writes` and passed. Based on that, F6 was marked
invalidated. But the unit test never started a real replay worker —
items just sat in the queue.

The deliberate sustained-load probe with a real worker
(`test/e2e/cql/replay_overflow_test.go:TestS11_ReplayOverflow_ConservationLaw`)
surfaces the real issue. With cluster A paused and 1000 writes
against a 50-capacity queue:

```
writes accepted:        1000     rows on A: 25     rows on B: 1000
ReplayEnqueued(A):        36     ReplaySuccess(A): 25
ReplayDropped(A):        975     OnReplayDropped (client cb): 964
                                 OnDrop          (worker cb):  11
```

Six conservation invariants all hold:

  1. Client-side:  writes_accepted = enqueued + client_drops    (1000 = 36 + 964)
  2. Worker-side:  enqueued = successes + worker_drops          (36 = 25 + 11)
  3. Metric:       droppedMetric = client_drops + worker_drops  (975 = 964 + 11)
  4. Data:         rows_on_A = ReplaySuccess                    (25 = 25)
  5. Survivor:     rows_on_B = writes_accepted                  (1000 = 1000)
  6. Operator:     writes_accepted = rows_on_A + droppedMetric  (1000 = 25 + 975)

**BUT:** the metric counter only reflects all of this if the worker
was constructed with `replay.WithWorkerMetrics(yourMetricsCollector)`.
Without that option, `NewMemoryWorker` falls back to an internal
`NopMetrics` and:

- `IncReplaySuccess` from the worker is silently discarded
- `IncReplayDropped` from the worker is silently discarded
- `IncReplayError` from the worker is silently discarded
- The `OnDrop` callback fires regardless (separate path)

Concretely, in the same scenario WITHOUT `WithWorkerMetrics`:

```
ReplaySuccess(A):         0    ← was actually 25
ReplayDropped(A):       964    ← was actually 975 (worker drops invisible)
```

The user who instruments via metrics-only (typical Prometheus setup)
sees undercounted successes and drops — and may (as we did originally)
conclude there's a "silent loss" when in fact the count is correct,
just hidden in the worker's separate metrics collector.

This is **a real Helix usability gotcha** (not a correctness bug —
the data and callbacks are right, just the metric path is bifurcated):

- `replay.NewMemoryWorker(replayer, executeFn, replay.WithWorkerMetrics(mc), …)`
  remains the safe pattern.
- **Resolved**: `NewCQLClient` now auto-injects the client's metrics
  collector into a worker that doesn't have one explicitly set, via
  type-assertion on `MetricsConfigured() bool` + `SetMetrics(...)`
  (added to `*replay.Worker`). Auto-memory worker gets the same
  injection. Callers who explicitly pass `WithWorkerMetrics(otherMc)`
  are NOT overwritten — their choice wins. See
  `cql_client_worker_metrics_test.go` for the unit guards.

Two design observations from the v1 doc retained:

a) `WithQueueCapacity(N)` allocates N/2 per priority queue (high + low),
   not N total slots. Documented at `replay/memory.go:122-125` but easy
   to miss; the parameter name suggests N total.
b) `client.Query().Exec()` returns nil on partial success even when the
   replay was dropped (cql_client.go:860 — "partial success is still
   success from the caller's perspective"). Users who care about
   replay drops must subscribe to `OnReplayDropped` or watch
   `IncReplayDropped`.

The e2e S1 test (which uses a slow workload and doesn't trigger
overflow) keeps testing the happy-path drain. The e2e S11 test now
covers the deliberate-overflow conservation law and serves as the
guard against future instrumentation regressions.

### 8. CircuitBreaker had no half-open probe path — FIXED

Original symptom: LCB on A, once tripped, stayed open indefinitely under
read-only workloads. Root cause: `CircuitBreaker.ShouldFailover` returned
`failures >= threshold` without consulting the reset timeout — so once
`StickyRead` routed traffic away from A, no `RecordLatency`/`RecordSuccess`
ever fired against A, and the breaker had no path back to closed.

Verified with `policy/circuit_breaker_probe_test.go:
TestCircuitBreaker_ShouldFailover_StaysTrueAfterResetTimeout` (failing
before the fix; passing after).

Fix in two parts (`policy/failover_policy.go`):

1. `ShouldFailover` is now time-aware — after `resetTimeout` elapses
   since the last failure, it returns false to allow a probe attempt.
2. `RecordFailure` clears the `tripped*` latch on the timeout-reset
   branch, so multi-cycle outages (trip → half-open → probe-fail →
   re-trip) emit `IncCircuitBreakerTrip` once per cycle rather than
   once per the entire outage. Without this, observability undercounts
   trips across recurring failures. Verified by
   `TestCircuitBreaker_TripMetric_FiresOncePerTripCycle`.

This is "leaky" half-open: concurrent callers may all see `false` during
the probe window and route to the failed cluster simultaneously. That is
intentional and bounded — the per-cluster mutex serializes outcomes, and
at most `threshold` operations can fail before the breaker re-trips. A
strict single-probe variant would need an additional "probe in flight"
atomic; the leaky version is adequate for Helix's failure-recovery
purpose and avoids the extra synchronization.

**Scope of the fix.** It corrects the `ShouldFailover` *API contract*
and therefore the trip metric across multi-cycle outages. The fix is
load-bearing for **external monitoring** that polls `ShouldFailover`
directly (which is what the e2e LCB test does). It does **not** by
itself restore read routing in a `StickyRead + LCB` scenario — read
routing under StickyRead is governed by StickyRead's own preferred-
cluster latch and cooldown, which are independent of the breaker
state. Any caller using `ShouldFailover` as a health gate will now see
correct half-open / re-trip semantics.

Existing tests still pass (full `./policy/...` race-detected suite, plus
`TestCircuitBreakerResetTimeout`, `TestCircuitBreaker_MetricEmittedOnce`
single-trip dedup, and `TestLatencyCircuitBreaker_ResetTimeout`). The
S3 e2e test now asserts the breaker closes after `Unpause + resetTimeout`
rather than logging an observation.

### 7. RoundRobinRead failover metric — INVALIDATED, test artifact

Original symptom: under `RoundRobinRead + ActiveFailover` with cluster A
paused, `mc.GetTotalFailovers()` stayed at 0 while other strategies
incremented. Suspected observability gap.

Verification (`cql_client_rr_failover_metric_test.go:
TestRoundRobinRead_FailoverMetric_FiresOnFailedClusterRead`):

- 20 reads against an always-failing A + always-healthy B → 10 failovers,
  exactly half (RR alternation).

The original e2e test only did ONE read. RoundRobinRead's first `Select`
returns ClusterB (counter starts at 0, first call increments to 1, 1%2==1
→ B). B was healthy, so the read succeeded immediately and no failover
was needed — `IncFailoverTotal` correctly stayed at 0.

The S2 test was updated to drive 4 reads per sub-test, ensuring at least
one Select hits the paused cluster regardless of the initial counter
parity. With multiple reads, RR fires `IncFailoverTotal` exactly as
expected. No code fix needed.

---

## Plan deltas

1. **Pause/Unpause is the primary failure mode** for S1–S5. Stop becomes
   optional and gated on `cluster.Type == ScyllaDB`.
2. **S6 (parity) explicitly probes the v1/v2 sentinel mismatch** as its
   first assertion — a regression in error classification is the highest-
   value bug this suite can find.
3. **Spike file kept as a diagnostic.** `spike_test.go` (build tag
   `//go:build spike`) stays in place — re-running it on a new host
   confirms whether Stop/Start/Pause/Unpause work for the chosen
   backend before investing in a full e2e run. It is not part of
   `make test-e2e`; invoke explicitly with `go test -tags spike ...`.
