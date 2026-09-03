# Auto-Recovery Guide

Helix has multiple recovery mechanisms that work at different levels. Short blips heal automatically. Prolonged outages need coordinated operator intervention. This guide explains when each layer activates, how they interact, and the operator workflow for safe recovery.

## Recovery Layers at a Glance

| Layer | Scope | Automatic? | Recovers When |
|-------|-------|------------|---------------|
| **FailoverPolicy** (read) | Per-read routing | Yes | Single successful read resets CircuitBreaker |
| **ReadStrategy** (read) | Sticky cluster preference | Partial | `PrimaryOnlyRead`: recovery timeout probes succeed. `StickyRead`: does NOT auto-recover — preferred only changes on a new failure of the current preferred. |
| **AdaptiveDualWrite** (write) | Per-write mode | Yes | `recoveryThreshold` consecutive fast background writes |
| **Recovery probe** (write) | Degraded-cluster healing for strict workloads | Yes (default-on with AdaptiveDualWrite) | Periodic `system.local` probe advances recovery counter when live strict writes cannot |
| **Replay** (write) | Data consistency | Yes (queue processing) | Worker drains the backlog |
| **AllowedClusters** (read) | Operator override | No — manual | Operator removes the override |
| **ForceDegrade / ForceRecover** (write) | Operator override | No — manual | Operator calls `ForceRecover` |

---

## When Auto-Recovery Is Enough

For short blips (seconds to a few minutes), no operator action is required. Helix self-heals:

```
Cluster A slow for 30s
  → AdaptiveDualWrite: 3 slow writes → A enters fire-and-forget
  → CircuitBreaker: 3 failures → trips, reads failover to B
  → Replay: failed writes enqueued for A

A recovers
  → Background writes to A succeed → fastStrikes accumulate
  → 5 consecutive fast writes → A exits fire-and-forget
  → Next successful read → CircuitBreaker resets
  → Reads route per the configured strategy (see below)
  → Replay worker drains remaining queue
```

For workloads that use only `Strict()` writes, live background writes to the degraded cluster
are not dispatched, so the recovery counter does not advance naturally. The **background recovery
probe** (default-on for `AdaptiveDualWrite`) compensates: it periodically checks the degraded
cluster and calls `RecordProbeSuccess` on each success, driving the same counter until the
cluster is restored. See the [Strict Write Guide](strict-write.md#recovery-probe-and-adaptivedualwrite)
for probe configuration options.

> **Read-side recovery depends on the strategy.** With `PrimaryOnlyRead`,
> reads probe back to A once the recovery timeout elapses. With
> `StickyRead`, if `OnFailure` already swapped preferred to B during the
> outage, reads stay on B until B itself fails — cooldown expiry alone
> does not move preferred back. Use [`PrimaryOnlyRead`](strategy-policy.md#primaryonlyread)
> for a strategy that probes the original cluster automatically, or
> coordinate read recovery via [`WithAllowedClusters`](#phase-4-re-enable-in-the-correct-order).

**Indicators that auto-recovery is handling it:**
- Replay queue depth stays low (tens to low hundreds)
- `AdaptiveDualWrite.IsDegraded(cluster)` flips back to `false` within minutes
- Circuit breaker close events appear in logs
- No operator alerts fire

**No intervention needed when:**
- GC pauses cause transient latency spikes
- Network hiccups are short and the configured failover policy resets after a successful read
- A deployment restart briefly disrupts one cluster

---

## When Operator Intervention Is Needed

The dangerous case is a **prolonged outage** where the replay queue grows significantly. The problem: read strategies recover based on cluster responsiveness ("A is answering queries"), but they have **no visibility into the replay backlog**. A cluster that is "up" but has a 2-hour data gap will serve stale or missing data.

```
                    ┌── danger zone ──┐
A is down           A comes back      reads auto-recover to A
  writes → B only   replay queue:     but replay is still
  reads  → B only   50K messages      backfilling
                                      → stale data
```

**Intervention is needed when:**
- Outage exceeds your `PrimaryOnlyRead` recovery timeout, or you use `StickyRead` (which has no automatic path back to the original preferred)
- Replay queue depth is in the thousands or higher
- The recovering cluster missed a meaningful volume of writes
- You need to guarantee read consistency before switching back

**Replay holds back for a draining or quarantined cluster.** The replay worker a client builds with
`WithAutoMemoryWorker` never executes against a draining cluster, and `helix.WithReplayGate` lets
the operator hold replay back for any reason; queued writes wait, without consuming retries, until
the gate opens. See [Hold Replay Back per Cluster](replay-system.md#9-hold-replay-back-per-cluster).

**Automatic backlog gating.** `helix.ExcludeWhileReplayBacklog` builds an `AllowedClusters`
function that keeps reads away from a cluster while its replay backlog is above a threshold,
so reads return to a recovered cluster only after its backlog has drained:

```go
replayer := replay.NewMemoryReplayer()
client, _ := helix.NewCQLClient(sessionA, sessionB,
    helix.WithReplayer(replayer),
    helix.WithAllowedClusters(helix.ExcludeWhileReplayBacklog(replayer.PendingByCluster, 100)),
)
```

The depth function runs on every read. `MemoryReplayer.PendingByCluster` is an atomic read and
can be passed directly; `NATSReplayer.PendingByCluster` queries JetStream, so sample it from a
background goroutine into atomics and pass a function that reads them. A manual `AllowedClusters`
flag can still wrap the helper for the cases below.

---

## The Coordinated Recovery Workflow

Recovery requires coordinating **reads** (AllowedClusters) and **writes** (ForceDegrade/ForceRecover) as separate concerns. The order matters.

### Phase 1: Isolate the Failed Cluster

When you detect a prolonged outage, explicitly exclude the cluster from both reads and writes:

```go
// Write-side: degrade A to fire-and-forget, replayed on failure
writeStrategy.ForceDegrade(helix.ClusterA)

// Read-side: override routing to B only
featureFlag.Set("exclude_cluster_A", true)
// (AllowedClusters func returns []ClusterID{ClusterB})
```

**Why both?** ForceDegrade alone still lets read strategies route to A. AllowedClusters alone still lets writes wait on A. Both are needed for full isolation.

`ForceDegrade` is a latch: fast background writes and successful recovery probes cannot restore
synchronous writes while it is set, and the client skips the recovery probe for a latched cluster.
Only `ForceRecover` or `Reset` clears it, so the isolation holds for as long as you need it.

### Phase 2: Wait for Cluster Recovery

The cluster comes back online. Helix's background fire-and-forget writes start succeeding. The replay worker begins draining the queue.

**What to monitor:**
- Replay queue depth (NATS: `nats stream info helix-replay --json | jq '.state.messages'`)
- Replay success/error callbacks
- Background write success rate on the recovering cluster

```
A comes back online
  → Background fire-and-forget writes start succeeding
  → Replay worker processes backlog
  → AllowedClusters override keeps reads on B
  → FallbackRead fence prevents probing A
```

**Do NOT remove the override yet.** The cluster is responsive but its data is incomplete.

### Phase 3: Verify Data Consistency

Wait until the replay queue is fully drained (or acceptably low), then verify:

1. **Replay queue depth is zero** (or near-zero with only new entries)
2. **No replay errors** in the error callback for the recovering cluster
3. **Application-level spot checks** if your domain requires it

### Phase 4: Re-enable in the Correct Order

**Reads first, then writes — or both together.** Never recover writes before reads.

```go
// Step 1: Remove read override — strategy resumes from frozen state
featureFlag.Set("exclude_cluster_A", false)
// (AllowedClusters func returns nil → normal strategy routing)

// Step 2: Recover writes — resume synchronous dual-write
writeStrategy.ForceRecover(helix.ClusterA)

// Step 3 (optional): force reads back to A
// primaryOnly.Reset()
// stickyRead.SetPreferred(helix.ClusterA) // or stickyRead.Reset()
```

**Why this order?** If you call `ForceRecover` first (writes resume to both clusters) but reads are still overridden to B, new writes land on A but no reads go there — this is safe, just redundant. If you remove the read override first, reads may go to A which now has consistent data — also safe. The dangerous order would be recovering writes while reads are already going to A with stale data, which can't happen if you kept the override active during the outage.

### Complete Example

```go
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
        return nil
    }),
)

// --- Outage detected ---
writeStrategy.ForceDegrade(helix.ClusterA)
excludeA.Store(true)

// --- A recovers, replay drains ---
// Monitor: replay queue depth → 0

// --- Re-enable ---
excludeA.Store(false)
writeStrategy.ForceRecover(helix.ClusterA)
```

---

## How Each Component Recovers

### AdaptiveDualWrite

Clusters move between HEALTHY and DEGRADED based on write latency:

- **Degrades** after `strikeThreshold` (default: 3) consecutive slow writes
- **Recovers** after `recoveryThreshold` (default: 5) consecutive fast background writes
- Background fire-and-forget writes credit recovery even while the cluster is degraded

Manual control: `ForceDegrade(cluster)` (a latch that only `ForceRecover(cluster)` or `Reset()` clears), `RecordFastWrite(cluster)`, `Reset()`

See [AdaptiveDualWrite Guide](adaptive-dual-write.md) for thresholds, tuning, and fire-and-forget details.

### CircuitBreaker

Trips after consecutive read failures and recovers through the client's recovery probe:

- **Trips** after `threshold` (default: 3) consecutive failures
- **Is probed** once `resetTimeout` (default: 30s) has elapsed since the last failure: the next tick of the client's recovery probe (`WithRecoveryProbe`, default on for dual-cluster clients) reserves the breaker (half-open) and runs one probe against the cluster. A successful probe closes the breaker; a failed one returns it to open and restarts the timeout. No caller's read is sacrificed to test the cluster
- **Also closes** on any successful read against that cluster
- Stays open until such a read when `resetTimeout` is 0 or the probe is disabled
- `LatencyCircuitBreaker` also treats slow reads (above `absoluteMax`) as soft failures, and with `helix.WithRouteVeto(true)` keeps ordinary reads away from the cluster while open or half-open

No manual intervention needed. Both transitions emit cluster events; see the
[Cluster Events Guide](cluster-events.md).

### Read Strategies

| Strategy | Auto-Recovery Behavior |
|----------|----------------------|
| **StickyRead** | Cooldown controls when a later failure on the current preferred cluster may change preferred again, with one exception: within the cooldown the preference still moves when the alternative is known good (it succeeded since its own last failure), so a hard-down preferred is abandoned on the next failed read while two flapping clusters do not oscillate. It does **not** cause `Select()` to probe the original cluster passively after expiry; if the current preferred stays healthy, reads stay there until `SetPreferred()` or `Reset()`. |
| **PrimaryOnlyRead** | After recovery timeout (if configured), `Select()` hands cluster A to exactly one caller as the probe while the others keep reading B; success resets to A, failure restarts the timer, and a probe whose caller never reports expires after another recovery timeout. If cluster B fails while in the failed-over state, cluster A is returned as a probe even without an elapsed recovery timeout; if A succeeds, `OnSuccess` clears the failed-over flag and reads return to A. Without a timeout and without B failing, stays on B until `Reset()`. |
| **RoundRobinRead** | No state to recover — alternates regardless. Failover policy governs whether to try the failed cluster. |

### Replay System

The replay worker continuously processes the queue; only a cluster whose replay gate is closed
(`replay.WithClusterGate`, or the client's drain state and `WithReplayGate`) is held back:

- **MemoryReplayer**: Volatile, in-process. Fast but lost on crash.
- **NATSReplayer**: Durable, supports dedicated replay service. Production recommended.

The worker routes replays to the correct cluster using `DefaultExecuteFunc()` and preserves the original write timestamp for idempotency.

See [Replay System](replay-system.md) for deployment patterns and configuration.

### AllowedClusters Override

The override is fully manual — it never activates or deactivates on its own. The provider function is called on every read, so changes take effect immediately (no propagation delay).

**Strategy state is frozen** during the override: `OnSuccess()` and `OnFailure()` are not called, so the strategy resumes from its pre-override state when the override is removed.

**FallbackRead is fenced by the override.** When a cluster is excluded from the allowed set, FallbackRead will not probe it — even if the selected cluster returns not-found. This is critical during recovery: the excluded cluster may have stale or missing rows, and a FallbackRead probe could return old data or falsely confirm existence of a row that was deleted while the cluster was down. The fence ensures the recovering cluster is completely shielded from not-found probes until the operator explicitly re-enables it.

See [External Cluster Control](strategy-policy.md#external-cluster-control-withallowedclusters) for semantics, drain interaction, and fail-closed behavior.

---

## Best Practices

1. **Let short blips self-heal.** Don't reach for ForceDegrade on every alert. AdaptiveDualWrite and CircuitBreaker handle transient issues automatically. Intervene only when the replay queue is growing unboundedly or the outage exceeds your `PrimaryOnlyRead` recovery timeout (or, for `StickyRead`, when you need preferred forced back to the original cluster).

2. **Always use AllowedClusters with ForceDegrade.** ForceDegrade alone doesn't prevent reads from the failing cluster. AllowedClusters alone doesn't prevent write-side latency impact. Use both together for full isolation.

3. **Monitor replay queue depth during recovery.** The queue depth is the single most important metric. Remove the override only after the queue is drained. A cluster that is "up" but has a data gap is worse than one that is explicitly excluded — the gap is invisible to callers.

4. **Remove the override before or with ForceRecover, never after.** If ForceRecover runs while reads are already going to the recovering cluster (override already removed), there's a brief window where new writes land on both clusters but old writes are still replaying. This is safe due to timestamp-based idempotency, but the ordering in [Phase 4](#phase-4-re-enable-in-the-correct-order) avoids any ambiguity.

5. **Configure PrimaryOnlyRead with a recovery timeout in production.** Without `WithPrimaryOnlyRecoveryTimeout`, a single cluster-A failure leaves the client permanently on B until `Reset()` is called. Use the timeout (e.g., 2-5 minutes) for automatic probing.

6. **Use ForceRecover, not Reset, for targeted recovery.** `Reset()` clears all state on the write strategy, including the other cluster's health data. `ForceRecover(cluster)` only affects the specified cluster.

7. **Drain mode and AllowedClusters interact.** The valid read candidate set is the intersection of override-allowed and non-draining clusters. If AllowedClusters returns `[A]` but A is draining, reads fail with `ErrNoValidClusters`. Resolve the conflict before it reaches production.

8. **Counter operations are not idempotent.** The replay system uses timestamps for idempotency, but Cassandra counter updates are additive. A replayed counter increment that already succeeded in the background will double-count. Avoid counter tables in dual-cluster mode, or use application-level deduplication.

---

## Common Mistakes

### Removing the override before replay finishes

```
❌ A comes back → remove override → reads hit A with stale data
✅ A comes back → wait for replay drain → verify → remove override
```

The override exists specifically to prevent this race. Don't remove it early because the cluster "looks healthy."

### Not using AllowedClusters with ForceDegrade

```
❌ ForceDegrade(A) only → writes fire-and-forget, but the read strategy is
   unaware (e.g., PrimaryOnlyRead probes A after recovery timeout, or
   StickyRead leaves preferred on A if it never failed) → reads hit A → stale data

✅ ForceDegrade(A) + AllowedClusters → both reads and writes isolated
```

### Recovering writes before reads are safe

```
❌ ForceRecover(A) while reads still going to A with incomplete data
   → new dual-writes AND stale reads simultaneously

✅ Drain replay → verify consistency → remove read override → ForceRecover
```

### Reading NoSynchronousAckError

`ErrWriteAsync` never reaches the caller on its own: a write with one
acknowledged cluster returns `nil`, and a write with none returns
`*types.NoSynchronousAckError`. That error means the write is at best in the
replay queue or still running in the background (a fire-and-forget leg is
enqueued only if it fails, and that later enqueue failure is reported only
through the replay-dropped callback and event), so retrying it is safe only
for idempotent statements; with a durable replayer,
`helix.WithAckMode(helix.AckOnReplayAdmission)` makes the queued case `nil`
again.

```go
err := client.Query("INSERT ...").Exec()
var noAck *types.NoSynchronousAckError
if errors.As(err, &noAck) && noAck.Replay == nil {
    // Both clusters degraded; the write is queued for replay, or still
    // running in the background and queued only if that attempt fails.
}
```

---

## Monitoring Checklist

| Metric / Signal | What It Tells You | Action |
|-----------------|-------------------|--------|
| `AdaptiveDualWrite.IsDegraded(cluster)` | Cluster in fire-and-forget mode | Investigate if persistent |
| Replay queue depth | Volume of unprocessed failed writes | Intervene if growing unboundedly |
| Replay error callbacks | Target cluster rejecting replays | Check cluster health |
| CircuitBreaker trip/close events | Read-path health transitions | Usually informational |
| `helix_failover_total` metric | Frequency of read failovers | High rate = instability |
| `helix_read_errors_total` metric | Read failures per cluster | Correlate with circuit breaker |

---

## See Also

- [AdaptiveDualWrite Guide](adaptive-dual-write.md) — Degradation thresholds, fire-and-forget details, tuning
- [Replay System](replay-system.md) — Queue implementations, deployment patterns, worker configuration
- [Strategy & Policy](strategy-policy.md) — Read/write strategies, failover policies, AllowedClusters semantics
- [FallbackRead Guide](fallback-read.md) — Best-effort dual-cluster reads during convergence gaps
