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
│  1. ReadStrategy.Select(ctx) → selectedCluster                      │
│                                                                     │
│  2. Execute read on selectedCluster                                 │
│                                                                     │
│  3. On SUCCESS:                                                     │
│     ├─ ReadStrategy.OnSuccess(cluster)                              │
│     ├─ FailoverPolicy.RecordSuccess(cluster)                        │
│     └─ [If LatencyRecorder] FailoverPolicy.RecordLatency(cluster, d)│
│                                                                     │
│  4. On FAILURE:                                                     │
│     ├─ FailoverPolicy.RecordFailure(cluster)                        │
│     │                                                               │
│     ├─ FailoverPolicy.ShouldFailover(cluster, err)  ← GATEKEEPER   │
│     │   └─ If FALSE → return error immediately                      │
│     │                                                               │
│     └─ ReadStrategy.OnFailure(cluster, err) → alternative, ok      │
│         └─ If ok → retry on alternative cluster                     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

**Key design decisions:**

- **FailoverPolicy is a gatekeeper.** It must approve failover before `ReadStrategy.OnFailure` is even consulted. This enforces circuit breaker semantics uniformly.
- **Both layers can deny failover.** `FailoverPolicy.ShouldFailover()` returning `false` stops immediately. `ReadStrategy.OnFailure()` returning `false` stops as well (e.g., cooldown active).
- **Latency is recorded automatically.** If the configured `FailoverPolicy` implements `LatencyRecorder` (e.g., `LatencyCircuitBreaker`), the client calls `RecordLatency()` after each successful read with no extra wiring.

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
│                                 │ Even if B errors: stays on B     │
│                                 │ (cooldown prevents flapping)     │
│                                 │                                  │
│                                 │ After cooldown: can switch to A  │
└─────────────────────────────────┴──────────────────────────────────┘
```

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

Always reads from cluster A. Fails over to cluster B once on error, then continues preferring A.

```go
strategy := policy.NewPrimaryOnlyRead()

// After cluster A recovers, reset to prefer A again
strategy.Reset()
```

No configuration options.

**Behavior:**
- All reads go to cluster A
- First failure on A: fails over to B for that request only; sets a `failedOver` flag
- While `failedOver`: subsequent reads still attempt A first (reads do not permanently stay on B)
- `Reset()`: clears the `failedOver` flag — call this after observing cluster A recovery

**When to use:** Read-after-write consistency requirements where writes always go to cluster A and you need to read your own writes. Also suitable for primary-secondary setups where cluster B is a warm standby.

**Trade-off:** Cluster B is idle for reads until A fails. Does not distribute load.

---

## Failover Policy Reference

### ActiveFailover

Always allows failover. Every error results in an attempt to use the alternative cluster.

```go
fp := policy.NewActiveFailover()
```

No configuration options. All methods are no-ops except `ShouldFailover`, which always returns `true`.

**When to use:** Read-heavy workloads where availability is more important than stability. Suitable when transient errors are common and each retry is cheap (e.g., simple key lookups).

**Trade-off:** No circuit breaker logic. If both clusters are degraded simultaneously, the client will attempt failover on every request, doubling latency without benefit.

---

### CircuitBreaker

Tracks consecutive failures per cluster. Blocks failover until the failure count reaches a threshold (CLOSED state), then allows failover (OPEN state).

```go
breaker := policy.NewCircuitBreaker(
    policy.WithThreshold(3),                    // failures before opening (default: 3)
    policy.WithResetTimeout(30*time.Second),    // reset counter after idle (default: 30s)
    policy.WithCircuitBreakerLogger(logger),    // optional structured logging
    policy.WithCircuitBreakerMetrics(metrics),  // optional metrics
    policy.WithCircuitBreakerClusterNames(names), // cluster labels for logs/metrics
)
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `WithThreshold` | 3 | Consecutive failures before circuit opens |
| `WithResetTimeout` | 30s | Reset failure counter after this idle period |
| `WithCircuitBreakerLogger` | no-op | Structured logger for open/close events |
| `WithCircuitBreakerMetrics` | no-op | Metrics collector for trip events and state changes |
| `WithCircuitBreakerClusterNames` | "A"/"B" | Display names used in log and metric labels |

**State machine:**

```
CLOSED (failures < threshold)
  └─ ShouldFailover() → false   ← absorbs transient errors
  └─ On RecordFailure():
       - Increment counter; if counter < threshold: stay CLOSED
       - If counter >= threshold: transition to OPEN, emit metric once

OPEN (failures >= threshold)
  └─ ShouldFailover() → true    ← permits failover to alternative cluster
  └─ On RecordSuccess():
       - Reset counter to 0, transition to CLOSED, emit metric
  └─ On RecordFailure() after resetTimeout idle:
       - Reset counter to 1 (stale failures discarded), stay OPEN
```

**Example timeline** (threshold = 3, resetTimeout = 30s):

```
 t=0s   Failure 1 → failures=1, ShouldFailover()=false
 t=5s   Failure 2 → failures=2, ShouldFailover()=false
 t=10s  Failure 3 → failures=3, ShouldFailover()=true  (circuit OPENS)
 t=15s  Failure 4 → failures=4, ShouldFailover()=true
 t=20s  Success   → failures=0, circuit CLOSES
 t=25s  Failure 1 → failures=1, ShouldFailover()=false  (fresh start)
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

**Behavior beyond CircuitBreaker:**
- The client automatically detects that the configured policy implements `LatencyRecorder` and calls `RecordLatency(cluster, elapsed)` after each successful read — no manual wiring required
- `RecordLatency`: if `elapsed > absoluteMax` → internally calls `RecordFailure()` (soft failure); otherwise calls `RecordSuccess()` (resets counter)
- Hard errors (network failures) still call `RecordFailure()` directly, as in `CircuitBreaker`

**When to use:** Latency-sensitive production environments where a technically-healthy but slow cluster (e.g., compaction, GC pressure) should be treated as degraded. Combines hard-failure and latency-based degradation in one policy.

**Trade-off:** Adds per-read overhead of a latency measurement and policy call. Tune `absoluteMax` carefully: too low and healthy clusters trip on normal jitter; too high and degraded clusters stay open too long.

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

> ¹ A `Replayer` is optional for `ConcurrentDualWrite` and `SyncDualWrite`. Both strategies wait for both writes to complete synchronously, so a partial failure is immediately visible as an error. Without a replayer the failed write is not retried, but the caller at least knows about it and can handle it. `AdaptiveDualWrite` is different: writes to a degraded cluster are fire-and-forget and the caller receives `ErrWriteAsync` instead of a real error, so **without a replayer those writes are silently lost**.

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
├─ No  → ActiveFailover
│        Failover on every error; maximum availability;
│        use with StickyRead cooldown to prevent flapping.
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

| Policy | Failover trigger | Circuit break | Latency-aware | Best for |
|--------|-----------------|---------------|---------------|----------|
| `ActiveFailover` | Every error | No | No | Max availability, simple |
| `CircuitBreaker` | After N errors | Yes | No | Tolerate transient errors |
| `LatencyCircuitBreaker` | After N errors or N slow reads | Yes | Yes | Latency-SLA production |

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
client, _ := helix.NewCQLClient(sessionA, sessionB,
    helix.WithReadStrategy(policy.NewStickyRead(
        policy.WithStickyReadCooldown(5*time.Minute),
    )),
    helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
    helix.WithFailoverPolicy(policy.NewActiveFailover()),
)
```

**Even load distribution:**
```go
client, _ := helix.NewCQLClient(sessionA, sessionB,
    helix.WithReadStrategy(policy.NewRoundRobinRead()),
    helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
    helix.WithFailoverPolicy(policy.NewActiveFailover()),
)
```

**Read-after-write consistency:**
```go
client, _ := helix.NewCQLClient(sessionA, sessionB,
    helix.WithReadStrategy(policy.NewPrimaryOnlyRead()),
    helix.WithWriteStrategy(policy.NewSyncDualWrite(
        policy.WithPrimaryFirst(), // ensure A is durable before B
    )),
    helix.WithFailoverPolicy(policy.NewCircuitBreaker(
        policy.WithThreshold(5), // tolerate more transient errors
    )),
)
```

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

5. **Call `PrimaryOnlyRead.Reset()` after cluster A recovers.** Without it, the `failedOver` flag persists across process restarts (if you reuse the same instance) and the primary cluster remains unused.

6. **Add metrics and logging to CircuitBreaker in production.** The policy emits trip/close events that are the primary signal for cluster health degradation. Without a `MetricsCollector`, these events are silent.
