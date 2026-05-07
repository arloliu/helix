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

### 6. Replay queue overflow under sustained load — INVALIDATED, observable via instrumentation

Original symptom: S1 driving 4898 writes flat-out against a paused A
ended with `A=600` and `B=4898`. The "silent loss" framing was wrong.

Verification (`cql_client_replay_drop_test.go:TestReplayDrop_QueueOverflow_IsObservable`):

- `WithQueueCapacity(10) + 100 writes against failing A`:
  `enqueued=5  dropped(metric)=95  dropped(callback)=95`
- Full accounting holds: `enqueued + dropped == writes` (5 + 95 = 100).
- `OnReplayDropped` callback count == `IncReplayDropped` metric — symmetric.

So drops are **fully observable**, just opt-in. The original e2e test had
neither subscribed to `OnReplayDropped` nor read `IncReplayDropped`, so
the loss appeared silent from the test's perspective only.

Two design observations worth noting:

a) `WithQueueCapacity(N)` allocates N/2 per priority queue (high + low),
   not N total slots. Documented at `replay/memory.go:122-125` but easy
   to miss; the parameter name suggests N total.
b) `client.Query().Exec()` returns nil on partial success even when the
   replay was dropped (cql_client.go:860 — "partial success is still
   success from the caller's perspective"). Users who care about
   replay drops must subscribe to `OnReplayDropped` or watch
   `IncReplayDropped`.

No code fix needed. The e2e S1 test was updated to demonstrate the
visibility pattern (subscribe to OnReplayDropped, read the metric).

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
