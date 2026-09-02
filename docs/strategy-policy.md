# Strategy and Policy Reference

This document describes every `ReadStrategy`, `WriteStrategy`, and `FailoverPolicy` implementation in Helix, how they interact, and guidance on choosing the right combination for your workload.

## Overview

Helix separates concerns into three distinct interfaces:

| Interface | Responsibility | Implementations |
|-----------|----------------|-----------------|
| **ReadStrategy** | Where to route reads and how to failover | `StickyRead`, `RoundRobinRead`, `PrimaryOnlyRead` |
| **WriteStrategy** | How to execute writes across both clusters | `ConcurrentDualWrite`, `SyncDualWrite`, `AdaptiveDualWrite` |
| **FailoverPolicy** | Whether failover is allowed on a given error | `ActiveFailover`, `CircuitBreaker`, `LatencyCircuitBreaker` |

Each interface has a single responsibility. Compose them to express your exact resilience requirements.

---

## Read Path Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CQLClient.executeRead()                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  0. resolveReadTarget(ctx) — resolved once, snapshot passed through │
│     ├─ AllowedClusters func not set (or returns nil/[]) →           │
│     │   normal path: ReadStrategy.Select(ctx) → selectedCluster     │
│     └─ AllowedClusters func set and returns cluster list →          │
│         override active: list[0] → selectedCluster (strategy FROZEN)│
│         drain state intersected; fail-closed on conflict or panic   │
│                                                                     │
│  1. Execute read on selectedCluster                                 │
│                                                                     │
│  2. On SUCCESS:                                                     │
│     ├─ [if !overrideActive] ReadStrategy.OnSuccess(cluster)         │
│     ├─ FailoverPolicy.RecordSuccess(cluster)                        │
│     └─ [If LatencyRecorder] FailoverPolicy.RecordLatency(cluster, d)│
│                                                                     │
│  3. On FAILURE (real error):                                        │
│     ├─ FailoverPolicy.RecordFailure(cluster)                        │
│     │                                                               │
│     ├─ FailoverPolicy.ShouldFailover(cluster, err)  ← GATEKEEPER   │
│     │   └─ If FALSE → return error immediately                      │
│     │                                                               │
│     ├─ [if overrideActive] → use snap.fallback (strategy NOT called)│
│     └─ [if normal]  → ReadStrategy.OnFailure(cluster, err)         │
│                          → alternative, ok                          │
│         └─ If ok → retry on alternative cluster                     │
│                                                                     │
│  4. On NOT-FOUND:                                                   │
│     └─ [if FallbackRead enabled]                                    │
│         ├─ [if overrideActive && alt not in allowed set] → skip alt │
│         └─ Otherwise → try alternative cluster once                  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

**Key design decisions:**

- **FailoverPolicy is a gatekeeper.** It must approve failover before `ReadStrategy.OnFailure` is even consulted. This enforces circuit breaker semantics uniformly.
- **Both layers can deny failover.** `FailoverPolicy.ShouldFailover()` returning `false` stops immediately. `ReadStrategy.OnFailure()` returning `false` stops as well (e.g., failure on a non-preferred cluster in `StickyRead`). Note: `StickyRead` cooldown does **not** deny failover — it returns the alternative cluster for the current request without changing the preferred cluster, so the read can still succeed on the other cluster.
- **Latency is recorded automatically.** If the configured `FailoverPolicy` implements `LatencyRecorder` (e.g., `LatencyCircuitBreaker`), the client calls `RecordLatency()` after each successful read with no extra wiring.
- **Not-found is not a failure.** A cluster that responds with "row absent" is healthy. Not-found results never trigger `RecordFailure`, `OnFailure`, or `IncReadError`. This classification is independent of FallbackRead.
- **FallbackRead is orthogonal to failover.** FallbackRead activates when a healthy cluster returns not-found; failover activates when a cluster returns a real error. They handle different failure modes and do not interfere with each other. See [FallbackRead Guide](fallback-read.md) for details.
- **`WithAllowedClusters` overrides the read path at the resolution layer.** When active, it bypasses `ReadStrategy.Select()` and freezes strategy state. See [External Cluster Control](#external-cluster-control-withallowedclusters) for the full operational model.

---

## Write Path Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                  CQLClient.executeWriteWithReplay()                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1. WriteStrategy.Execute(ctx, writeA, writeB)                      │
│     ├─ ConcurrentDualWrite: both clusters in parallel               │
│     ├─ SyncDualWrite:       clusters sequentially (A→B or B→A)     │
│     └─ AdaptiveDualWrite:   healthy clusters wait; degraded async   │
│                                                                     │
│  2. Results (errA, errB):                                           │
│     ├─ Both succeed       → return nil                              │
│     ├─ Both fail          → return DualClusterError                 │
│     └─ One fails          → enqueue to Replayer, return nil         │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

`FailoverPolicy` does not participate in the write path. Write resilience comes from the write strategy itself plus the replay system.

---

## Write Strategy Reference

### ConcurrentDualWrite

Executes writes to both clusters in parallel using two goroutines, then waits for both to complete.

```go
strategy := policy.NewConcurrentDualWrite()
```

No configuration options. The strategy has no per-instance state.

**Behavior:**
- Spawns one goroutine per cluster; waits via `sync.WaitGroup`
- Each goroutine runs independently — a slow cluster does not delay the other's goroutine launch
- Both goroutines are always launched and the caller always blocks until **both** complete — there is no short-circuit between them
- Context cancellation is honored by the underlying driver (gocql aborts in-flight queries when the context is done), but `ConcurrentDualWrite` itself does not check `ctx.Err()` — a canceled context does not prevent both goroutines from being started
- Returns `(errA, errB)` to the caller; if a `Replayer` is configured, the client enqueues partial failures for replay

**When to use:** General-purpose default. Gives the lowest write latency on healthy clusters because both writes race in parallel.

**Trade-off:** Both writes must finish (or fail) before the caller unblocks. A transiently slow cluster increases tail latency.

---

### SyncDualWrite

Executes writes sequentially, one cluster at a time.

```go
// Write A first, then B (default)
strategy := policy.NewSyncDualWrite()

// Write B first, then A
strategy := policy.NewSyncDualWrite(
    policy.WithSecondaryFirst(),
)
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `WithPrimaryFirst()` | ✓ default | Write cluster A before cluster B |
| `WithSecondaryFirst()` | — | Write cluster B before cluster A |

**Behavior:**
- Writes cluster A (or B) first, records the result, then writes the other
- Context cancellation is honored by the underlying driver during each write
- After the first write completes, explicitly checks `ctx.Err()`: if the context is already canceled or deadline-exceeded at that point, returns `ctx.Err()` for the second cluster without executing it — preventing wasted work on an already-dead context
- Returns `(errA, errB)`; if a `Replayer` is configured, the client enqueues partial failures for replay

**When to use:**
- Debugging — sequential execution makes it easy to tell which cluster failed first
- Tests requiring strict ordering guarantees
- Scenarios where you need to guarantee write A precedes write B (e.g., cross-cluster dependency ordering)

**Trade-off:** Total write latency is `latency(A) + latency(B)`. Never use in latency-sensitive production paths unless ordering semantics are required.

---

### AdaptiveDualWrite

Monitors per-cluster write latency and automatically degrades slow clusters to fire-and-forget, then recovers them when performance improves.

```go
strategy := policy.NewAdaptiveDualWrite(
    policy.WithAdaptiveDeltaThreshold(300 * time.Millisecond), // relative slowness threshold
    policy.WithAdaptiveAbsoluteMax(2 * time.Second),           // hard latency cap
    policy.WithAdaptiveMinFloor(100 * time.Millisecond),       // noise floor for delta
    policy.WithAdaptiveStrikeThreshold(3),                     // slow writes before degraded
    policy.WithAdaptiveRecoveryThreshold(5),                   // fast writes to recover
    policy.WithAdaptiveFireForgetTimeout(30 * time.Second),    // background write timeout
    policy.WithAdaptiveFireForgetLimit(100),                   // max concurrent background writes
)
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `WithAdaptiveDeltaThreshold` | 300ms | Relative latency difference before counting a strike |
| `WithAdaptiveAbsoluteMax` | 2s | Latency above this always counts as a strike, regardless of delta |
| `WithAdaptiveMinFloor` | 100ms | If both clusters are faster than this, delta comparison is skipped (noise filter) |
| `WithAdaptiveStrikeThreshold` | 3 | Consecutive slow writes to transition HEALTHY → DEGRADED |
| `WithAdaptiveRecoveryThreshold` | 5 | Consecutive fast writes to transition DEGRADED → HEALTHY |
| `WithAdaptiveFireForgetTimeout` | 30s | Timeout applied to each background (fire-and-forget) write |
| `WithAdaptiveFireForgetLimit` | 100 | Max concurrent background writes; excess returns `ErrWriteDropped` |

**Cluster events:** degrade and recover transitions emit
`types.EventWriteDegraded` and `types.EventWriteRecovered`, and record the
`{prefix}_write_degraded{cluster}` gauge plus the
`{prefix}_write_degraded_total` / `{prefix}_write_recovered_total` transition
counters on collectors that implement the optional
`types.AdaptiveWriteMetrics` interface (the bundled `contrib/metrics/vm`
collector does). Inside a client, register `helix.WithOnClusterEvent`;
outside one, call `SetEventEmitter` on the strategy. See the
[Cluster Events Guide](cluster-events.md).

**Configuration validation:**

- `NewAdaptiveDualWrite` is the compatibility constructor. Invalid values fall back to defaults.
- `NewAdaptiveDualWriteChecked` returns `error` (joined `*types.OptionError`) when any option value is invalid.

```go
strategy, err := policy.NewAdaptiveDualWriteChecked(
    policy.WithAdaptiveStrikeThreshold(3),
    policy.WithAdaptiveFireForgetTimeout(30 * time.Second),
)
if err != nil {
    return fmt.Errorf("configure adaptive strategy: %w", err)
}
```

**State machine (per cluster):**

```
┌─────────────────────────────────────────────────────────────────────┐
│                     AdaptiveDualWrite per-cluster state             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  HEALTHY (default)                                                  │
│  ├─ Write is executed synchronously; caller blocks until done       │
│  ├─ Caller's context is passed through; driver honors cancellation  │
│  ├─ Both healthy-cluster goroutines always run (no short-circuit)   │
│  ├─ Latency recorded for delta comparison on next write             │
│  └─ Strike recorded when:                                           │
│       (a) latency > absoluteMax, OR                                 │
│       (b) latency - sibling_latency > deltaThreshold                │
│           AND both clusters > minFloor (real degradation, not noise)│
│                                                                     │
│  Transition HEALTHY → DEGRADED: strikeThreshold consecutive strikes │
│                                                                     │
│  DEGRADED                                                           │
│  ├─ Write runs in a background goroutine (fire-and-forget)          │
│  ├─ Caller receives ErrWriteAsync immediately (non-blocking)        │
│  ├─ Background goroutine uses context.Background() + fireForgetTimeout│
│  │   — the caller's context is NOT propagated; canceling the        │
│  │   caller's context has no effect on an already-scheduled write   │
│  ├─ Semaphore limits concurrent background writes to fireForgetLimit│
│  └─ If semaphore full: returns ErrWriteDropped                      │
│                                                                     │
│  Transition DEGRADED → HEALTHY: recoveryThreshold consecutive fast  │
│  writes observed (including from background goroutines)             │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

**Error semantics:**
- `ErrWriteAsync` — write accepted for background execution (not a failure)
- `ErrWriteDropped` — semaphore full; background write could not be scheduled (rare)
- Actual write errors from a healthy cluster count as strikes toward degradation
- `ErrWriteAsync` and `ErrWriteDropped` are excluded from strike counting

**Testing helpers:**

```go
strategy.IsDegraded(helix.ClusterA)        // check current state
strategy.ForceDegrade(helix.ClusterA)      // force DEGRADED for testing
strategy.ForceRecover(helix.ClusterA)      // force HEALTHY for testing
strategy.RecordFastWrite(helix.ClusterA)   // credit a fast write (external health probe)
strategy.Reset()                           // clear all state, both clusters → HEALTHY
```

**When to use:** Production environments with strict write-path latency SLAs. AdaptiveDualWrite ensures a slow or GC-pausing cluster never extends tail latency for the caller — the degraded cluster catches up asynchronously via replay.

**Trade-off:** Degraded writes are not immediately durable on the slow cluster. You must have a `Replayer` configured to ensure eventual consistency.

---

## Read Strategy Reference

### StickyRead

Randomly selects an initial preferred cluster at construction and routes all reads there. Switches to the other cluster on failure (subject to cooldown).

```go
// Random initial cluster (default)
strategy := policy.NewStickyRead()

// Pin initial preference to cluster A
strategy := policy.NewStickyRead(
    policy.WithPreferredCluster(helix.ClusterA),
)

// Custom cooldown (default: 5 minutes)
strategy := policy.NewStickyRead(
    policy.WithStickyReadCooldown(10 * time.Minute),
)
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `WithPreferredCluster` | random | Which cluster to prefer on startup |
| `WithStickyReadCooldown` | 5m | Minimum time between cluster switches |

**Cooldown behavior:**

```
Time →
├────────────────────────────────────────────────────────────────────┤
│ Preferred: Cluster A            │ Preferred: Cluster B             │
├─────────────────────────────────┼──────────────────────────────────┤
│ Normal reads → A                │ Reads → B                        │
│                                 │                                  │
│ Error occurs → failover to B ───┤ Cooldown starts (5 min)          │
│                                 │                                  │
│                                 │ If B errors during cooldown:     │
│                                 │   Preferred stays on B           │
│                                 │   Read retries on A (per-request)│
│                                 │                                  │
│                                 │ After cooldown: can switch to A  │
└─────────────────────────────────┴──────────────────────────────────┘
```

During the cooldown window, if the current preferred cluster (B) fails, `OnFailure` returns the alternative (A) as a failover target for that individual request without changing preferred. Reads succeed via the retry, but each request during this window pays the cost of trying B first — resulting in elevated latency, error counts, and failover log entries until a later failure occurs after cooldown expiry and preferred can switch.

Cooldown expiry by itself does **not** trigger a probe back to the original cluster. If B stays healthy after A recovers, reads continue on B indefinitely. StickyRead only changes preferred in response to a failure on the current preferred cluster.

> **Oscillation risk.** If both clusters are intermittently failing, `StickyRead` can flip-flop between them — once the cooldown expires, a failure on cluster B causes a switch back to A, and vice versa. The cooldown is the only brake. Set it long enough that a single blip does not trigger rapid back-and-forth switching; 2–10 minutes is typical. Pairing `StickyRead` with `CircuitBreaker` instead of `ActiveFailover` provides an additional layer of protection: the circuit breaker absorbs transient errors and only allows failover after repeated failures.

**When to use:** The default choice for most workloads. Sticking to one cluster maximizes row-cache and OS page-cache hit rates.

**Trade-off:** One cluster handles all reads by default; the other is idle for reads. If read load must be distributed, use `RoundRobinRead` instead.

---

### RoundRobinRead

Alternates between clusters on every read using an atomic counter.

```go
strategy := policy.NewRoundRobinRead()
```

No configuration options.

**Behavior:**
- Reads go A, B, A, B, … in strict alternation
- On failure, tries the other cluster regardless of state
- Stateless — no history, no cooldown

**When to use:** Even load distribution is the primary concern, or when both clusters are equally close to clients (e.g., two datacenters equidistant from the application). Also useful when data is not cached at the application layer, making cache affinity irrelevant.

**Trade-off:** No cache affinity. The same row may be read from both clusters alternately, halving the effective cache utilization compared to `StickyRead`.

---

### PrimaryOnlyRead

Always reads from cluster A. Fails over to cluster B on error. Reads return to cluster A when one of three things happens: `Reset()` is called manually, the recovery timeout elapses and a probe succeeds, or cluster B itself fails while failed-over (triggering a probe back to A).

```go
// Default: permanent failover until Reset() is called
strategy := policy.NewPrimaryOnlyRead()

// Auto-recovery: probe cluster A again after 2 minutes of being failed-over
strategy := policy.NewPrimaryOnlyRead(
    policy.WithPrimaryOnlyRecoveryTimeout(2 * time.Minute),
)

// Manual reset — use when you observe cluster A recovery externally
strategy.Reset()
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `WithPrimaryOnlyRecoveryTimeout` | disabled | After this duration in failed-over state, `Select` returns cluster A as a probe |

**Behavior:**
- All reads go to cluster A
- On cluster A failure: sets `failedOver`, returns cluster B for failover
- On cluster B failure while `failedOver`: returns cluster A as a probe for that request without resetting state; if the probe succeeds, `OnSuccess` clears `failedOver` and reads return to A; if A is also down, the caller receives `DualClusterError` and subsequent reads stay on B
- While `failedOver` (no recovery timeout): all reads route to cluster B permanently (unless B itself fails — see above)
- While `failedOver` (with recovery timeout): after the timeout elapses, `Select` returns cluster A as a probe; if that read succeeds (`OnSuccess`), the strategy resets to cluster A; if it fails again (`OnFailure`), the timer restarts
- `Reset()`: clears the `failedOver` flag immediately regardless of timeout

**When to use:** Read-after-write consistency requirements where writes always go to cluster A and you need to read your own writes. Also suitable for primary-secondary setups where cluster B is a warm standby.

**Trade-off:** Cluster B is idle for reads until A fails. Does not distribute load. Without `WithPrimaryOnlyRecoveryTimeout`, if cluster A recovers the client stays on cluster B indefinitely unless `Reset()` is called externally or cluster B also fails.

---

## Failover Policy Reference

### ActiveFailover

Always allows failover. Every error results in an attempt to use the alternative cluster.

```go
fp := policy.NewActiveFailover()
```

No configuration options. All methods are no-ops except `ShouldFailover`, which always returns `true`.

**When to use:** Read-heavy workloads where availability is more important than stability. Suitable when transient errors are rare, short-lived, and each retry is cheap (e.g., simple key lookups against a stable cluster pair).

**Trade-off — Oscillation risk:** `ActiveFailover` has no circuit breaker, no threshold, and no cooldown of its own. If both clusters are intermittently failing, the client will flip-flop between them on every request:

```
Request 1: read A → fails → failover to B → B returns error
Request 2: next Select() picks A again (StickyRead switched back after prior success on B)
           read A → fails → failover to B → B errors again
Request 3: … same cycle repeats
```

In this scenario failover doubles latency on every request while providing no benefit. The effect is amplified when the `ReadStrategy` has no cooldown (e.g., `RoundRobinRead`) — then every alternating request hits a different cluster, generating a constant stream of failover attempts.

**Mitigations:**
- Pair with `StickyRead` and a long cooldown (≥ 2 minutes) so that even if both clusters are degraded, the client at least stays on one cluster for the duration of the cooldown.
- Prefer `CircuitBreaker` in production — it absorbs transient errors silently and only gates failover after repeated failures, which prevents thrashing at the cost of surfacing the first `threshold - 1` errors directly to the caller.

---

### CircuitBreaker

Tracks consecutive failures **per cluster independently**. Blocks failover until the failure count reaches a threshold (CLOSED state), then allows failover (OPEN state). Cluster A's circuit state has no effect on cluster B's counter and vice versa.

```go
breaker := policy.NewCircuitBreaker(
    policy.WithThreshold(3),                      // failures before opening (default: 3)
    policy.WithResetTimeout(30*time.Second),      // stale-failure window (default: 30s)
    policy.WithCircuitBreakerLogger(logger),      // optional structured logging
    policy.WithCircuitBreakerMetrics(metrics),    // optional metrics
    policy.WithCircuitBreakerClusterNames(names), // cluster labels for logs/metrics
)
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `WithThreshold` | 3 | Consecutive failures before circuit opens |
| `WithResetTimeout` | 30s | If the gap between two consecutive failures exceeds this, the counter resets to 1 on the next failure rather than incrementing — stale failures are discarded, not the live counter |
| `WithCircuitBreakerLogger` | no-op | Structured logger for open/close events |
| `WithCircuitBreakerMetrics` | no-op | Metrics collector for trip events and state changes |
| `WithCircuitBreakerClusterNames` | "A"/"B" | Display names used in log and metric labels |

**Cluster events:** open and close transitions also emit
`types.EventCircuitBreakerOpen` and `types.EventCircuitBreakerClosed`. Inside a
client, register `helix.WithOnClusterEvent` and Helix installs the emitter into
the configured failover policy for you. Outside a client, call
`SetEventEmitter` on the breaker directly. See the
[Cluster Events Guide](cluster-events.md).

**Configuration validation:**

- `NewCircuitBreaker` is the compatibility constructor. Invalid values fall back to defaults.
- `NewCircuitBreakerChecked` returns `error` (joined `*types.OptionError`) when any option value is invalid.

```go
breaker, err := policy.NewCircuitBreakerChecked(
    policy.WithThreshold(3),
    policy.WithResetTimeout(30*time.Second),
)
if err != nil {
    return fmt.Errorf("configure circuit breaker: %w", err)
}
```

> **`resetTimeout` semantics:** this is **not** "close the circuit after N seconds of silence." The counter does not reset automatically over time — it resets to 1 only when the *next* failure arrives after a `resetTimeout`-long gap. If a cluster stays broken, the counter keeps incrementing; `resetTimeout` only protects against counting a failure from *last week* against today's blip. An open circuit closes two ways: a successful read (`RecordSuccess`) closes it immediately, or that delayed failure closes it first — ending the stale open span — before it starts the fresh count at 1. With `threshold` set to 1, the fresh count reaches threshold immediately, so the same call closes and reopens the circuit.

**What triggers `RecordFailure` vs `RecordSuccess`:**
- `RecordFailure`: every hard error returned by the driver (network failure, timeout, unavailable) — all errors except Helix-internal sentinels (`ErrWriteAsync`, `ErrWriteDropped`) which never appear on the read path
- `RecordSuccess`: every successful read response, regardless of latency

**State machine (per cluster):**

```
CLOSED (failures < threshold)
  └─ ShouldFailover() → false   ← absorbs transient errors; error returned to caller
  └─ On RecordFailure():
       ├─ Gap since last failure > resetTimeout → reset counter to 1 (stale)
       └─ Otherwise → increment; if counter >= threshold: transition to OPEN, emit metric

OPEN (failures >= threshold)
  └─ ShouldFailover() → true    ← permits failover to alternative cluster
  └─ On RecordSuccess():
       └─ Reset counter to 0, transition to CLOSED, emit metric (Reason: "operation succeeded")
  └─ On RecordFailure():
       ├─ Gap since last failure > resetTimeout → close (reset counter to 1, transition to CLOSED,
       │    emit metric, Reason: "reset timeout elapsed"); threshold=1 re-opens in the same call
       └─ Otherwise → increment (stay OPEN)
```

**When OPEN: what the caller observes:**
- `ShouldFailover(A) = true` → `ReadStrategy.OnFailure(A)` is called → returns cluster B
- Read is retried on cluster B
- If B succeeds → caller gets result; B's `RecordSuccess` is called (B's counter resets)
- If B also fails → **both circuits now track failures independently; caller gets the B error** — there is no further retry. If B's circuit was already OPEN, the caller immediately receives the error without attempting A; Helix does not return `DualClusterError` for reads (only writes).

**Example timeline** (threshold = 3, resetTimeout = 30s):

```
 t=0s   Failure 1 on A → A.failures=1, ShouldFailover(A)=false  (error returned to caller)
 t=5s   Failure 2 on A → A.failures=2, ShouldFailover(A)=false  (error returned)
 t=10s  Failure 3 on A → A.failures=3, ShouldFailover(A)=true   (circuit OPENS; failover to B)
           B read succeeds → B.failures=0, A stays OPEN
 t=15s  Next read → ShouldFailover(A)=true → retry on B → B succeeds
 t=20s  A read attempted directly (ShouldFailover still true) → success → A.failures=0 (CLOSES)
 t=25s  Failure 1 on A → A.failures=1, ShouldFailover(A)=false  (fresh start)
 t=90s  Failure 1 on A → gap > 30s → A.failures reset to 1 (stale failure discarded)
```

**When to use:** Workloads that can tolerate a few transient errors per cluster without failing over. The threshold absorbs brief blips (single-node restarts, brief GC pauses) without switching clusters unnecessarily.

**Trade-off:** The first `threshold - 1` errors are surfaced to the caller without failover. If your SLA requires failover on the very first error, use `ActiveFailover` instead.

---

### LatencyCircuitBreaker

Extends `CircuitBreaker` by treating slow responses as soft failures. Latency above `absoluteMax` counts toward the failure threshold even when no hard error occurs.

```go
breaker := policy.NewLatencyCircuitBreaker(
    policy.WithLatencyAbsoluteMax(2 * time.Second), // slow threshold (default: 2s)
    policy.WithLatencyThreshold(3),                 // failures before opening (default: 3)
    policy.WithLatencyResetTimeout(30*time.Second), // reset timeout (default: 30s)
    policy.WithLatencyLogger(logger),
    policy.WithLatencyMetrics(metrics),
)
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `WithLatencyAbsoluteMax` | 2s | Responses slower than this count as failures |
| `WithLatencyThreshold` | 3 | Inherited from `CircuitBreaker` |
| `WithLatencyResetTimeout` | 30s | Inherited from `CircuitBreaker` |
| `WithLatencyLogger` | no-op | Structured logger |
| `WithLatencyMetrics` | no-op | Metrics collector |

**Cluster events:** same as `CircuitBreaker` — open and close transitions emit
`types.EventCircuitBreakerOpen` and `types.EventCircuitBreakerClosed`, and
`SetEventEmitter` is available for standalone use. See the
[Cluster Events Guide](cluster-events.md).

**Configuration validation:**

- `NewLatencyCircuitBreaker` is the compatibility constructor. Invalid values fall back to defaults.
- `NewLatencyCircuitBreakerChecked` returns `error` (joined `*types.OptionError`) when any option value is invalid.

```go
breaker, err := policy.NewLatencyCircuitBreakerChecked(
    policy.WithLatencyAbsoluteMax(2*time.Second),
    policy.WithLatencyThreshold(3),
)
if err != nil {
    return fmt.Errorf("configure latency circuit breaker: %w", err)
}
```

**What triggers `RecordFailure` vs `RecordSuccess`:**
- Hard error from driver → `RecordFailure()` directly (same as `CircuitBreaker`)
- Successful read, `elapsed ≤ absoluteMax` → `RecordSuccess()` (resets counter)
- Successful read, `elapsed > absoluteMax` → `RecordLatency()` → internally calls `RecordFailure()` (soft failure counted toward threshold)

The client calls `RecordLatency()` automatically after each successful read if the configured policy implements `LatencyRecorder` — no manual wiring required.

**Example timeline** (absoluteMax = 2s, threshold = 3, resetTimeout = 30s):

```
 t=0s   Read A in 800ms  → RecordSuccess(A) → A.failures=0
 t=1s   Read A in 2.3s   → RecordLatency(A, 2.3s) → soft failure → A.failures=1, ShouldFailover(A)=false
 t=2s   Read A in 2.1s   → soft failure → A.failures=2, ShouldFailover(A)=false
 t=3s   Read A in 2.5s   → soft failure → A.failures=3, ShouldFailover(A)=true  (circuit OPENS)
           failover to B → B succeeds in 900ms → RecordSuccess(B)
 t=4s   Read A → circuit still OPEN → failover to B → B in 600ms → RecordSuccess(B)
 t=5s   Read A → hard error → RecordFailure(A) directly → A.failures=4 (stays OPEN)
 t=10s  Read A → circuit OPEN → failover to B → B succeeds → RecordSuccess(B)
           Meanwhile: A closes on its next recorded call — a RecordSuccess
           (Reason "operation succeeded"), or a RecordFailure arriving more
           than resetTimeout after A's last failure (Reason "reset timeout
           elapsed"). Until one of those arrives, A stays OPEN.
```

> **Note:** Because every fast successful read calls `RecordSuccess()`, the `resetTimeout` is less significant in `LatencyCircuitBreaker` than in `CircuitBreaker` — successful reads continuously reset the counter, so stale failure accumulation is rare. The dominant closure mechanism is fast responses, not idle timeouts.

**When to use:** Latency-sensitive production environments where a technically-healthy but slow cluster (e.g., compaction, GC pressure) should be treated as degraded. Combines hard-failure and latency-based degradation in one policy.

**Trade-off:** Adds per-read overhead of a latency measurement and policy call. Tune `absoluteMax` carefully: too low and healthy clusters trip on normal jitter; too high and degraded clusters stay open too long. A useful starting point is your p99 read SLA measured at the driver level — not at the application level, since Helix overhead adds a small constant.

---

## External Cluster Control (`WithAllowedClusters`)

### Problem: Stale Reads After Cluster Recovery

When a cluster fails for an extended period, the replay queue accumulates writes destined for it. When the cluster comes back online, reads may resume on it without operator coordination: `PrimaryOnlyRead` automatically probes the original cluster once its recovery timeout elapses, and any strategy whose preferred was never swapped to the alternative will route reads back as soon as the failover policy (e.g., `CircuitBreaker`) closes. The replay worker is still backfilling, so the recovering cluster has incomplete data.

(`StickyRead` has no symmetric path: once preferred has been swapped to the alternative, only a failure of the new preferred — not cooldown expiry — moves it back. But if the original preferred recovers before its first `OnFailure` swap, reads simply resume on it as the failover policy resets.)

```
+-- A fails --- failover to B --- A comes back ---  strategy auto-recovers to A --+
|                                                                                  |
|  Writes land on B only    Replay queue grows    Reads hit A BUT replay worker    |
|                                                 is still backfilling             |
|                                                 → stale or missing data          |
```

The read strategies have no visibility into the replay backlog. They see "A is responding" and switch back.

### Solution: `WithAllowedClusters`

`WithAllowedClusters` provides an operator-driven function that controls which clusters are eligible for reads. While the function returns a non-empty list, the read strategy is bypassed entirely and the override list directly controls routing.

```go
client, _ := helix.NewCQLClient(sessionA, sessionB,
    helix.WithReadStrategy(policy.NewStickyRead(...)),
    helix.WithAllowedClusters(func() []helix.ClusterID {
        if featureFlag.IsClusterExcluded("A") {
            return []helix.ClusterID{helix.ClusterB}
        }
        return nil // no override, normal strategy behavior
    }),
)
```

The function is called on every read operation. It must be non-blocking, goroutine-safe, and cheap to call.

### List Ordering Is the Routing Decision

When the override is active:

| Return value | Behavior |
|---|---|
| `[]ClusterID{ClusterB}` | Only B; A excluded, no failover |
| `[]ClusterID{ClusterB, ClusterA}` | B primary, A as failover |
| `nil` or `[]ClusterID{}` | No override; normal strategy + drain behavior |

The first element is the primary read target; subsequent elements are failover candidates in priority order. Duplicate entries are deduplicated while preserving order: `[B, B, A]` resolves to primary=B, fallback=A. `[B, B]` resolves to primary=B, no fallback.

### Strategy State Is Frozen During Override

When the override is active, the `ReadStrategy` is completely bypassed:

- `Select()` is not called — the override list controls routing
- `OnSuccess()` is not called — strategy internal state does not advance
- `OnFailure()` is not called — strategy does not flip preferred cluster

This is intentional. If `OnFailure(B)` were called during override, `StickyRead` might flip its preferred cluster to A — the cluster the operator excluded. When the override is later removed, reads would route to A (stale data).

**When override is removed**, strategy state resumes from where it was frozen. For example:
- `StickyRead`: preferred stays at its last value (likely B, from the original failover). Reads go to B, normal cooldown mechanics resume.
- `PrimaryOnlyRead`: `failoverTime` stays at its last value. If the recovery timeout elapsed while frozen, the next `Select()` probes A — which is correct if the operator removed the override because A is now consistent.
- If you need to force strategy state after removing the override, call `PrimaryOnlyRead.Reset()` or use `WithPreferredCluster`.

**`FailoverPolicy` still receives health signals.** Even during override, `RecordSuccess`, `RecordFailure`, and `RecordLatency` are called so that circuit breaker state reflects real cluster health. The circuit breaker still gates failover: if it says no, the read fails even if the override list has a fallback entry.

### Drain and Override Intersect

Override and drain serve different purposes — neither supersedes the other. The valid candidate set for reads is the **intersection** of override-allowed and non-draining clusters.

| Override | Drain state | Effective candidates | Behavior |
|---|---|---|---|
| `[B]` | neither draining | `[B]` | Reads go to B only |
| `[B, A]` | A draining | `[B]` | A filtered by drain; reads go to B only |
| `[A]` | A draining | `[]` — **conflict** | `ErrNoValidClusters` |
| `[A, B]` | A draining | `[B]` | A filtered; B is the sole candidate |
| `nil` | A draining | `[B]` | Normal drain behavior (unchanged) |

When the intersection is empty the read **fails** with `ErrNoValidClusters`. This is fail-closed: conflicting constraints are an operator error that must be resolved.

### FallbackRead Respects the Override Fence

When the override is active and a cluster is excluded, FallbackRead will not probe it. If the alternative cluster is not in the allowed set, FallbackRead returns `ErrNotFound` immediately without probing.

This is critical: the excluded cluster may have stale or missing rows. A FallbackRead that probed it could return old data or falsely confirm existence of a row that was deleted while the cluster was down.

### Fail-Closed on Broken Provider Output

| Provider output | Behavior |
|---|---|
| `nil` or empty slice | No override, normal behavior |
| Valid cluster list | Override active |
| Only unknown `ClusterID`s | `ErrInvalidClusterOverride` |
| `ClusterB` in single-cluster mode | `ErrInvalidClusterOverride` |
| Function panics | `ErrClusterOverridePanic` (panic recovered, stack logged) |

### CAS Operations Are Not Overridden

CAS operations (`ScanCAS`, `MapScanCAS`, batch `ExecCAS`, `MapExecCAS`) are single-cluster, non-replicated conditional writes. The override is a read-safety mechanism — it prevents reading stale data from a recovering cluster. CAS operations are write-like: they apply conditional mutations and are never replicated to the other cluster.

If the override rerouted a CAS to a different cluster, it would silently move conditional writes, potentially increasing divergence. CAS paths use `ReadStrategy.Select()` and, like every read, avoid a draining cluster when the other one is not draining; a read failure that moves the sticky preference therefore also moves subsequent CAS operations. To control which cluster CAS operations target, use `ForceDegrade`/`ForceRecover` on the write side.

### Iterator Paths Defer Errors to `Close()`

`IterContext()` returns `Iter`, not `(Iter, error)`. If `resolveReadTarget` fails (e.g., all unknown cluster IDs, panic), an `errorIter` is returned that defers the error:

- `Scan()` returns `false`
- `Close()` returns the error
- `SliceMap()` returns the error
- All other methods return zero values

**Always call `Close()` and check its error.** Callers that iterate with `for iter.Scan(&x) { ... }` and never call `Close()` will observe an empty result set with no error indication — this is a constraint of the `Iter` API shape.

### Operator Workflow

```
1. Cluster A fails → automatic failover (read strategies handle this normally)

2. Operator detects prolonged outage:
   strategy.ForceDegrade(ClusterA)         → writes: fire-and-forget A, replay on failure
   featureFlag.Set("cluster_A_excluded")   → AllowedClusters returns [B]
                                           → reads: only from B

3. A comes back online
   → Replay worker backfills A from queue
   → Operator monitors replay queue depth
   → Strategies would auto-recover, but override blocks them
   → FallbackRead fence prevents probing A

4. Operator confirms A's data is consistent:
   featureFlag.Clear()                     → AllowedClusters returns nil
                                           → reads: strategy resumes from frozen state
   strategy.ForceRecover(ClusterA)         → writes: resume dual-write to A
   strategy.Reset() (optional)             → force strategy to a known state
```

### Compose with `ForceDegrade`/`ForceRecover` for Full Control

Reads and writes are controlled separately:

| Concern | Read-side | Write-side |
|---|---|---|
| Exclude cluster | `WithAllowedClusters` returning `[B]` | `ForceDegrade(A)` |
| Re-include cluster | Return `nil` from func | `ForceRecover(A)` |

Composing separate primitives is cleaner than one mechanism that attempts to control both paths.

---

## Decision Guide

### Choosing a Write Strategy

```
Is write-path latency a primary concern?
│
├─ Yes → Does a single slow cluster extend tail latency unacceptably?
│        │
│        ├─ Yes → AdaptiveDualWrite
│        │        (degraded clusters go fire-and-forget; healthy clusters
│        │         determine tail latency; requires Replayer for consistency)
│        │
│        └─ No  → ConcurrentDualWrite
│                 (both writes race in parallel; caller unblocks when
│                  both complete; simplest, no extra state)
│
└─ No  → Is write order observable or important for debugging?
         │
         ├─ Yes → SyncDualWrite
         │        (sequential A→B or B→A; easy to trace failures;
         │         worst-case latency = sum of both)
         │
         └─ No  → ConcurrentDualWrite
                  (still the default choice when latency is not critical)
```

**Summary table:**

| Strategy | Latency | Consistency | Requires Replayer | Best for |
|----------|---------|-------------|-------------------|----------|
| `ConcurrentDualWrite` | Low (parallel) | Strong — waits for both | No¹ | Default general use |
| `SyncDualWrite` | Highest (sequential) | Strong — waits for both | No¹ | Debugging, ordered writes |
| `AdaptiveDualWrite` | Lowest tail (async degraded) | Eventual on degraded cluster | **Yes** | Latency-SLA-bound production |

> ¹ A `Replayer` is optional for `ConcurrentDualWrite` and `SyncDualWrite`. Both strategies wait for both writes to complete synchronously, so a partial failure is immediately visible as an error. Without a replayer the failed write is not retried, but the caller at least knows about it and can handle it. `AdaptiveDualWrite` is different: writes to a degraded cluster are fire-and-forget, so **without a replayer a fire-and-forget leg that fails is lost**; the client counts each such leg as a dropped replay and reports it with `types.ErrNoReplayer`, and a write with no synchronous acknowledgement at all returns `*types.NoSynchronousAckError`.

---

### Choosing a Read Strategy

```
Do you need cache locality (same rows read from the same cluster)?
│
├─ Yes → StickyRead (default)
│        Picks one cluster, sticks with it; switches on failure
│        with cooldown to prevent flapping.
│
└─ No  → Do you need to distribute read load evenly?
         │
         ├─ Yes → RoundRobinRead
         │        Alternates A/B; even throughput distribution;
         │        no affinity, lower cache hit rate.
         │
         └─ No  → Do you need read-after-write or a primary preference?
                  │
                  └─ Yes → PrimaryOnlyRead
                           Always prefers A; single failover to B
                           on hard failure; call Reset() on recovery.
```

**Summary table:**

| Strategy | Cache affinity | Load distribution | Failover behavior |
|----------|---------------|-------------------|-------------------|
| `StickyRead` | High — single cluster | Uneven | Switch on failure + cooldown |
| `RoundRobinRead` | None | Even | Switch on failure, no cooldown |
| `PrimaryOnlyRead` | Always A | None | Single failover to B |

---

### Choosing a Failover Policy

```
Can the workload tolerate a few errors without failover?
│
├─ No  → Are both clusters guaranteed stable (rarely fail simultaneously)?
│        │
│        ├─ Yes → ActiveFailover
│        │        Failover on every error; maximum availability;
│        │        MUST pair with StickyRead + long cooldown (≥ 2m)
│        │        to prevent oscillation if both clusters degrade.
│        │
│        └─ No  → CircuitBreaker (lower threshold, e.g. 2)
│                 Absorbs single blips; opens fast enough that
│                 first-error failover happens within 2 requests.
│
└─ Yes → Do slow (but successful) reads cause SLA violations?
         │
         ├─ Yes → LatencyCircuitBreaker
         │        Treats high-latency reads as failures;
         │        requires tuning absoluteMax to match SLA.
         │
         └─ No  → CircuitBreaker
                  Absorbs transient errors; only allows failover
                  after threshold consecutive failures.
```

**Summary table:**

| Policy | Failover trigger | Circuit break | Latency-aware | Oscillation risk | Best for |
|--------|-----------------|---------------|---------------|-----------------|----------|
| `ActiveFailover` | Every error | No | No | **High** — no damping | Stable clusters, max availability |
| `CircuitBreaker` | After N errors | Yes | No | Low | General production use |
| `LatencyCircuitBreaker` | After N errors or N slow reads | Yes | Yes | Low | Latency-SLA production |

---

### Common Combinations

**General-purpose HA (recommended starting point):**
```go
client, _ := helix.NewCQLClient(sessionA, sessionB,
    helix.WithReadStrategy(policy.NewStickyRead(
        policy.WithStickyReadCooldown(5*time.Minute),
    )),
    helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
    helix.WithFailoverPolicy(policy.NewCircuitBreaker(
        policy.WithThreshold(3),
        policy.WithResetTimeout(30*time.Second),
    )),
)
```

**Latency-SLA production (lowest tail latency):**
```go
client, _ := helix.NewCQLClient(sessionA, sessionB,
    helix.WithReadStrategy(policy.NewStickyRead()),
    helix.WithWriteStrategy(policy.NewAdaptiveDualWrite(
        policy.WithAdaptiveDeltaThreshold(300*time.Millisecond),
        policy.WithAdaptiveAbsoluteMax(2*time.Second),
    )),
    helix.WithFailoverPolicy(policy.NewLatencyCircuitBreaker(
        policy.WithLatencyAbsoluteMax(2*time.Second),
        policy.WithLatencyThreshold(3),
    )),
    helix.WithReplayer(replayer), // required for AdaptiveDualWrite consistency
)
```

**Maximum availability (failover on every error):**
```go
// NOTE: ActiveFailover with StickyRead requires a long cooldown.
// Without it, both-cluster degradation causes rapid oscillation —
// every request flips between A and B. The cooldown ensures the client
// stays on one cluster for at least the duration of a blip.
client, _ := helix.NewCQLClient(sessionA, sessionB,
    helix.WithReadStrategy(policy.NewStickyRead(
        policy.WithStickyReadCooldown(5*time.Minute), // essential damping
    )),
    helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
    helix.WithFailoverPolicy(policy.NewActiveFailover()),
)
```

**Even load distribution:**
```go
// RoundRobinRead has no cooldown, so pair it with CircuitBreaker — not
// ActiveFailover. Without a circuit breaker, every alternating request hitting
// a degraded cluster triggers a failover attempt, generating constant noise.
client, _ := helix.NewCQLClient(sessionA, sessionB,
    helix.WithReadStrategy(policy.NewRoundRobinRead()),
    helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
    helix.WithFailoverPolicy(policy.NewCircuitBreaker(
        policy.WithThreshold(3),
        policy.WithResetTimeout(30*time.Second),
    )),
)
```

**Read-after-write consistency:**
```go
client, _ := helix.NewCQLClient(sessionA, sessionB,
    helix.WithReadStrategy(policy.NewPrimaryOnlyRead(
        // Auto-probe cluster A after 2 minutes of being failed-over,
        // so the client self-heals when cluster A recovers.
        policy.WithPrimaryOnlyRecoveryTimeout(2*time.Minute),
    )),
    helix.WithWriteStrategy(policy.NewSyncDualWrite(
        policy.WithPrimaryFirst(), // ensure A is durable before B
    )),
    helix.WithFailoverPolicy(policy.NewCircuitBreaker(
        policy.WithThreshold(5), // tolerate more transient errors
    )),
)
```

**Mixed workload — critical data with FallbackRead, bulk data without:**
```go
// Critical data client: FallbackRead on every query
criticalClient, _ := helix.NewCQLClient(sessionA, sessionB,
    helix.WithReadStrategy(policy.NewStickyRead()),
    helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
    helix.WithFailoverPolicy(policy.NewCircuitBreaker(
        policy.WithThreshold(3),
    )),
    helix.WithDefaultFallbackRead(true), // check both clusters on not-found
    helix.WithReplayer(replayer),
)

// Bulk data client: no FallbackRead, accept eventual consistency
bulkClient, _ := helix.NewCQLClient(sessionA, sessionB,
    helix.WithReadStrategy(policy.NewStickyRead()),
    helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
    helix.WithFailoverPolicy(policy.NewCircuitBreaker(
        policy.WithThreshold(3),
    )),
    helix.WithReplayer(replayer),
)
```

See [FallbackRead Guide](fallback-read.md) for detailed behavior, activation levels, and best practices.

**Controlled recovery after prolonged cluster failure:**
```go
// Use a feature flag or atomic to control the override at runtime.
// ForceDegrade/ForceRecover control the write side separately.
var excludeA atomic.Bool

writeStrategy := policy.NewAdaptiveDualWrite()

client, _ := helix.NewCQLClient(sessionA, sessionB,
    helix.WithReadStrategy(policy.NewStickyRead()),
    helix.WithWriteStrategy(writeStrategy),
    helix.WithFailoverPolicy(policy.NewCircuitBreaker(policy.WithThreshold(3))),
    helix.WithReplayer(replayer),
    helix.WithAllowedClusters(func() []helix.ClusterID {
        if excludeA.Load() {
            return []helix.ClusterID{helix.ClusterB}
        }
        return nil // normal strategy routing
    }),
)

// When cluster A has an outage:
writeStrategy.ForceDegrade(helix.ClusterA) // writes: fire-and-forget A, replay on failure
excludeA.Store(true)                        // reads: only from B

// When A is back and replay queue is drained:
excludeA.Store(false)                       // reads: strategy resumes (frozen state)
writeStrategy.ForceRecover(helix.ClusterA) // writes: resume dual-write to A
```

See [External Cluster Control](#external-cluster-control-withallowedclusters) for full semantics, fail-closed behavior, and the operator workflow.

---

## Operation Timeouts

Timeouts for CQL operations should be configured on the underlying driver session, not at the Helix strategy level.

```go
// gocql
cluster := gocql.NewCluster("host1", "host2")
cluster.Timeout = 5 * time.Second // per-query timeout

// The AdaptiveDualWrite fireForgetTimeout is a separate budget for background writes
strategy := policy.NewAdaptiveDualWrite(
    policy.WithAdaptiveFireForgetTimeout(30 * time.Second),
)
```

Driver-level timeouts apply uniformly to all query types (reads, writes, batches) and are handled with proper internal cleanup by the driver.

---

## Best Practices

1. **Always pair a write strategy with a replayer when using AdaptiveDualWrite.** Fire-and-forget writes on degraded clusters are not immediately durable; the replayer is the consistency guarantee.

2. **Set a cooldown on StickyRead to prevent flapping.** A cooldown of 2–10 minutes is typical. Too short risks alternating between clusters on intermittent errors; too long delays recovery.

3. **Tune CircuitBreaker threshold to your error budget.** A threshold of 3 means the first 2 errors per cluster are absorbed. If your SLA cannot tolerate even one error without failover, use `ActiveFailover`.

4. **Use LatencyCircuitBreaker's `absoluteMax` conservatively.** Set it to your p99 read SLA, not your p50. Cassandra/ScyllaDB p99 can be 2–5× p50 under compaction; a too-tight threshold will cause spurious failovers.

5. **Configure `PrimaryOnlyRead` with a recovery timeout in production.** Without `WithPrimaryOnlyRecoveryTimeout`, a single cluster-A failure leaves the client permanently on cluster B until `Reset()` is called externally. Use the timeout to auto-probe cluster A after a reasonable recovery window (e.g., 2–5 minutes), so the client self-heals without operator intervention.

6. **Add metrics and logging to CircuitBreaker in production.** The policy emits trip/close events that are the primary signal for cluster health degradation. Without a `MetricsCollector`, these events are silent.

7. **Do not use `ActiveFailover` when both clusters can fail simultaneously.** `ActiveFailover` has no threshold, no cooldown of its own, and no circuit-breaking. When both clusters are degraded it causes request-level oscillation — each request flips to the other cluster, doubling latency with no benefit. Preferred alternatives:
   - Use `CircuitBreaker` (or `LatencyCircuitBreaker`) in all production environments. The circuit breaker absorbs transient errors and dampens oscillation by requiring repeated failures before allowing failover.
   - If you require first-error failover semantics, keep `ActiveFailover` but mandate `StickyRead` with a cooldown of at least 2 minutes. The cooldown prevents the read strategy from switching clusters again until the blip has passed.
   - Never pair `ActiveFailover` with `RoundRobinRead`. `RoundRobinRead` has no cooldown at all; combined with `ActiveFailover`, every other request will hit a different cluster, generating constant failover noise during any dual-cluster degradation.

8. **Use `WithAllowedClusters` to gate reads during replay backfill.** When a cluster recovers after prolonged downtime, read strategies may auto-recover before the replay worker has finished backfilling. Use `WithAllowedClusters` to hold reads on the healthy cluster until you confirm data consistency externally. Compose it with `ForceDegrade`/`ForceRecover` on the write side. Return `nil` (not an empty slice, though both work) to signal "no constraint" so the pattern is explicit in code review. See the [Auto-Recovery Guide](auto-recovery.md) for the full coordinated workflow.

9. **Understand the two layers of failover damping.** Oscillation protection requires both layers working together:
   - `FailoverPolicy` (`CircuitBreaker`) decides **whether** to failover. Without a threshold it cannot absorb blips.
   - `ReadStrategy` (`StickyRead` + cooldown) decides **where** to failover and **how long to stay there**. Without a cooldown the strategy can switch back immediately.

   Using `ActiveFailover` removes the first layer entirely. Using `RoundRobinRead` removes the second. Removing both is the worst-case combination for oscillation.
