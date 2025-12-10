# AdaptiveDualWrite Strategy Guide

AdaptiveDualWrite is a latency-aware write strategy that automatically adapts to cluster health, providing the best balance between consistency and latency.

## Overview

Unlike `ConcurrentDualWrite` which always waits for both clusters to complete, `AdaptiveDualWrite` monitors cluster performance and can switch slow clusters to "fire-and-forget" mode to maintain low latency while relying on the replay system for eventual consistency.

```
┌─────────────────────────────────────────────────────────────────────┐
│                   AdaptiveDualWrite Behavior                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  HEALTHY Cluster          │  DEGRADED Cluster                       │
│  ─────────────────        │  ─────────────────                      │
│  • Wait for completion    │  • Fire-and-forget (async)              │
│  • Record latency         │  • Bounded goroutine pool               │
│  • Update health metrics  │  • Background retry with timeout        │
│                           │  • Rely on replay for failures          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## When to Use AdaptiveDualWrite

| Scenario | Recommendation |
|----------|----------------|
| Latency-sensitive applications (< 100ms p99) | ✅ Use AdaptiveDualWrite |
| Clusters with frequent GC pauses | ✅ Use AdaptiveDualWrite |
| Cross-region replication with variable latency | ✅ Use AdaptiveDualWrite |
| Maximum write consistency required | ❌ Use ConcurrentDualWrite |
| Simple setup, no latency concerns | ❌ Use ConcurrentDualWrite |

## Quick Start

```go
import (
    "github.com/arloliu/helix"
    "github.com/arloliu/helix/policy"
    "github.com/arloliu/helix/replay"
)

// Basic usage with defaults
client, _ := helix.NewCQLClient(sessionA, sessionB,
    helix.WithWriteStrategy(policy.NewAdaptiveDualWrite()),
    helix.WithReplayer(replay.NewNATSReplayer(nc, js)), // Required for fire-and-forget
)
```

## How Degradation Works

A cluster transitions from **HEALTHY** to **DEGRADED** when either condition is met:

### 1. Absolute Latency Cap (absoluteMax)

If a single write takes longer than `absoluteMax` (default: 2s), the cluster immediately accumulates a strike.

```
Write latency: 2.5s  → Strike! (exceeds 2s absolute cap)
```

### 2. Relative Delta Threshold (deltaThreshold)

If one cluster is consistently slower than its sibling by more than `deltaThreshold` (default: 300ms), it accumulates strikes.

```
Cluster A: 50ms   │ Cluster B: 400ms  │ Delta: 350ms > 300ms
                  │                   │ → Strike for Cluster B
```

### 3. Strike Accumulation (strikeThreshold)

After `strikeThreshold` (default: 3) consecutive strikes, the cluster becomes DEGRADED.

```
Write 1: A=50ms, B=400ms → Strike B (1/3)
Write 2: A=60ms, B=450ms → Strike B (2/3)
Write 3: A=55ms, B=380ms → Strike B (3/3) → B is now DEGRADED
```

## The MinFloor Filter

To prevent false positives when both clusters are healthy and fast, delta comparison is **skipped** when both latencies are below `minFloor` (default: 100ms).

```
┌─────────────────────────────────────────────────────────────────────┐
│                      MinFloor Filter Logic                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Cluster A: 20ms   │  Cluster B: 80ms  │  Delta: 60ms               │
│                    │                   │                            │
│  Both < 100ms (minFloor)                                            │
│  → Delta comparison SKIPPED                                         │
│  → Both recorded as FAST (no strikes)                               │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Cluster A: 50ms   │  Cluster B: 450ms │  Delta: 400ms              │
│                    │                   │                            │
│  B > 100ms (minFloor)                                               │
│  → Delta comparison APPLIES                                         │
│  → Strike for B (slower by 400ms > 300ms threshold)                 │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

**Why this matters:** Minor latency variations (20ms vs 80ms) are normal and don't indicate cluster degradation. The minFloor prevents these natural variations from triggering unnecessary mode switches.

## Recovery Mechanism

A DEGRADED cluster can recover to HEALTHY after `recoveryThreshold` (default: 5) consecutive fast writes:

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Recovery Process                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Cluster B is DEGRADED (fire-and-forget mode)                       │
│                                                                     │
│  Background write 1: 80ms, within delta  → Fast (1/5)               │
│  Background write 2: 90ms, within delta  → Fast (2/5)               │
│  Background write 3: 85ms, within delta  → Fast (3/5)               │
│  Background write 4: 95ms, within delta  → Fast (4/5)               │
│  Background write 5: 70ms, within delta  → Fast (5/5)               │
│                                                                     │
│  → Cluster B is now HEALTHY (waits for completion again)            │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

Recovery requires:
1. Write succeeds (no error)
2. Latency < absoluteMax
3. Latency within deltaThreshold of sibling (if sibling has valid latency)

## Configuration Options

### Default Values

```go
strategy := policy.NewAdaptiveDualWrite()
// Equivalent to:
strategy := policy.NewAdaptiveDualWrite(
    policy.WithAdaptiveDeltaThreshold(300 * time.Millisecond),
    policy.WithAdaptiveAbsoluteMax(2 * time.Second),
    policy.WithAdaptiveMinFloor(100 * time.Millisecond),
    policy.WithAdaptiveStrikeThreshold(3),
    policy.WithAdaptiveRecoveryThreshold(5),
    policy.WithAdaptiveFireForgetTimeout(30 * time.Second),
    policy.WithAdaptiveFireForgetLimit(100),
)
```

### Option Reference

| Option | Default | Description |
|--------|---------|-------------|
| `WithAdaptiveDeltaThreshold` | 300ms | Max acceptable latency difference between clusters |
| `WithAdaptiveAbsoluteMax` | 2s | Single write timeout that triggers immediate strike |
| `WithAdaptiveMinFloor` | 100ms | Skip delta comparison if both clusters faster than this |
| `WithAdaptiveStrikeThreshold` | 3 | Consecutive slow writes before degradation |
| `WithAdaptiveRecoveryThreshold` | 5 | Consecutive fast writes to recover |
| `WithAdaptiveFireForgetTimeout` | 30s | Timeout for fire-and-forget background writes |
| `WithAdaptiveFireForgetLimit` | 100 | Max concurrent fire-and-forget goroutines |

### Tuning for Different Environments

#### Low-Latency Local Clusters (same datacenter)

```go
strategy := policy.NewAdaptiveDualWrite(
    policy.WithAdaptiveDeltaThreshold(50 * time.Millisecond),   // Tighter tolerance
    policy.WithAdaptiveAbsoluteMax(500 * time.Millisecond),     // Lower cap
    policy.WithAdaptiveMinFloor(20 * time.Millisecond),         // Lower floor
    policy.WithAdaptiveStrikeThreshold(5),                      // More tolerant
)
```

#### Cross-Region Clusters (higher baseline latency)

```go
strategy := policy.NewAdaptiveDualWrite(
    policy.WithAdaptiveDeltaThreshold(500 * time.Millisecond),  // Higher tolerance
    policy.WithAdaptiveAbsoluteMax(5 * time.Second),            // Higher cap
    policy.WithAdaptiveMinFloor(200 * time.Millisecond),        // Higher floor
    policy.WithAdaptiveStrikeThreshold(3),                      // Standard
)
```

#### High-Throughput Systems (more concurrent fire-and-forget)

```go
strategy := policy.NewAdaptiveDualWrite(
    policy.WithAdaptiveFireForgetLimit(500),                    // More goroutines
    policy.WithAdaptiveFireForgetTimeout(60 * time.Second),     // Longer timeout
)
```

## Fire-and-Forget Behavior

When a cluster is DEGRADED, writes are executed asynchronously:

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Fire-and-Forget Write Flow                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1. Try to acquire semaphore (non-blocking)                         │
│     ├─ Success → Launch background goroutine                        │
│     └─ Failure → Return ErrWriteDropped (replay handles it)         │
│                                                                     │
│  2. Background goroutine:                                           │
│     ├─ Create timeout context (fireForgetTimeout)                   │
│     ├─ Execute write                                                │
│     ├─ Track latency for recovery                                   │
│     └─ Release semaphore                                            │
│                                                                     │
│  3. Main thread returns immediately with:                           │
│     └─ ErrWriteAsync (not a real error, indicates async execution)  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Handling ErrWriteAsync

The client automatically handles `ErrWriteAsync` as a partial success:

```go
// This is handled internally by CQLClient
resultA, resultB := strategy.Execute(ctx, writeA, writeB)

if errors.Is(resultB, types.ErrWriteAsync) {
    // B is degraded, write is running in background
    // Enqueue to replayer as backup
}
```

### Concurrency Limit (fireForgetLimit)

To prevent resource exhaustion, fire-and-forget writes are bounded by `fireForgetLimit`:

- If limit reached → `ErrWriteDropped` returned
- Dropped writes are handled by the replay system
- Default: 100 concurrent fire-and-forget goroutines

## Monitoring Cluster State

### Programmatic Inspection

```go
strategy := policy.NewAdaptiveDualWrite()

// Check if a cluster is degraded
if strategy.IsDegraded(helix.ClusterB) {
    log.Warn("Cluster B is in fire-and-forget mode")
}
```

### Manual Intervention

```go
// Force a cluster into degraded mode (e.g., during maintenance)
strategy.ForceDegrade(helix.ClusterB)

// Force a cluster back to healthy (e.g., after maintenance)
strategy.ForceRecover(helix.ClusterB)

// Reset all state (e.g., after deployment)
strategy.Reset()

// Record a fast write manually (e.g., from health probe)
strategy.RecordFastWrite(helix.ClusterB)
```

## Integration with Replay System

AdaptiveDualWrite **requires** a Replayer for production use:

```go
// REQUIRED: Configure replayer for fire-and-forget reliability
client, _ := helix.NewCQLClient(sessionA, sessionB,
    helix.WithWriteStrategy(policy.NewAdaptiveDualWrite()),
    helix.WithReplayer(replay.NewNATSReplayer(nc, js)),
    helix.WithReplayWorker(replay.NewWorker(replayer)),
)
```

When a fire-and-forget write fails:
1. Background goroutine detects failure
2. CQLClient enqueues the write to Replayer
3. ReplayWorker retries the write later

**Without a Replayer**, fire-and-forget failures are lost permanently.

## Decision Flowchart

```
                         ┌─────────────────┐
                         │ Incoming Write  │
                         └────────┬────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    ▼                           ▼
            ┌──────────────┐            ┌──────────────┐
            │  Cluster A   │            │  Cluster B   │
            │   HEALTHY?   │            │   HEALTHY?   │
            └──────┬───────┘            └──────┬───────┘
                   │                           │
         ┌─────────┴─────────┐       ┌─────────┴─────────┐
         ▼                   ▼       ▼                   ▼
    ┌─────────┐         ┌─────────┐ ┌─────────┐     ┌─────────┐
    │  Wait   │         │Fire&    │ │  Wait   │     │Fire&    │
    │ for     │         │Forget   │ │ for     │     │Forget   │
    │ result  │         │(async)  │ │ result  │     │(async)  │
    └────┬────┘         └────┬────┘ └────┬────┘     └────┬────┘
         │                   │           │               │
         └───────────────────┴───────────┴───────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────┐
                    │   Compare latencies     │
                    │   Update health state   │
                    └─────────────────────────┘
```

## Common Patterns

### Graceful Degradation During Maintenance

```go
strategy := policy.NewAdaptiveDualWrite()

// Before maintenance: force cluster B to degraded
strategy.ForceDegrade(helix.ClusterB)

// Perform maintenance on Cluster B...

// After maintenance: allow automatic recovery
strategy.ForceRecover(helix.ClusterB)
```

### Health Check Integration

```go
func healthCheck(strategy *policy.AdaptiveDualWrite) {
    // Perform lightweight probe to Cluster B
    start := time.Now()
    err := clusterB.Query("SELECT now() FROM system.local").Exec()
    latency := time.Since(start)

    if err == nil && latency < 100*time.Millisecond {
        // Record fast write to help recovery
        strategy.RecordFastWrite(helix.ClusterB)
    }
}
```

### Monitoring Dashboard Metrics

```go
// Export for monitoring
func exportMetrics(strategy *policy.AdaptiveDualWrite) {
    metrics.Gauge("cluster_a_degraded").Set(boolToFloat(strategy.IsDegraded(helix.ClusterA)))
    metrics.Gauge("cluster_b_degraded").Set(boolToFloat(strategy.IsDegraded(helix.ClusterB)))
}
```

## Troubleshooting

### Cluster Keeps Getting Degraded

**Symptoms:** Cluster frequently switches between HEALTHY and DEGRADED

**Causes:**
1. `deltaThreshold` too tight for your latency variance
2. `strikeThreshold` too low
3. Actual cluster performance issues

**Solutions:**
```go
// Increase tolerance
policy.WithAdaptiveDeltaThreshold(500 * time.Millisecond)
policy.WithAdaptiveStrikeThreshold(5)
```

### Cluster Never Recovers

**Symptoms:** Cluster stays DEGRADED even after issues resolved

**Causes:**
1. Fire-and-forget writes still slow
2. `recoveryThreshold` too high
3. Delta still exceeded vs healthy sibling

**Solutions:**
```go
// Manual recovery
strategy.ForceRecover(helix.ClusterB)

// Or reduce recovery threshold
policy.WithAdaptiveRecoveryThreshold(3)
```

### Too Many Fire-and-Forget Goroutines

**Symptoms:** High memory usage, many pending goroutines

**Causes:**
1. `fireForgetLimit` too high
2. `fireForgetTimeout` too long
3. Cluster completely unreachable

**Solutions:**
```go
// Reduce limits
policy.WithAdaptiveFireForgetLimit(50)
policy.WithAdaptiveFireForgetTimeout(10 * time.Second)
```

## See Also

- [Strategy & Policy Overview](strategy-policy.md) - How read/write strategies interact
- [Replay System](replay-system.md) - How failed writes are reconciled
