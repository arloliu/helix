# FallbackRead Guide

FallbackRead is a best-effort mechanism for recovering data that exists on only one cluster due to partial write failures or replay lag. When the selected cluster returns "not found," Helix silently tries the other cluster before returning the result to the caller.

> FallbackRead is available only on Helix CQL reads through `helix.CQLClient` and `helix.Query`.

## When to Use

FallbackRead is designed for **critical read-after-write scenarios** where a recent dual-write partially failed and replay has not yet converged.

| Use case | FallbackRead? | Why |
|----------|---------------|-----|
| User profile lookup after registration | Yes | Must find the user even if one write failed |
| Order status after placement | Yes | Read-after-write must succeed for the user |
| Session token validation | Yes | Token must be found regardless of which cluster received the write |
| Time-series event ingestion | No | Missing a few events during an outage is acceptable |
| Analytics aggregation queries | No | Eventual consistency is sufficient |
| Batch data pipeline reads | No | Retries will catch up naturally |

**Rule of thumb:** Use FallbackRead when a false "not found" would cause a user-visible error or an incorrect business decision. Leave it off when missing data is tolerable.

## How It Works

```
           ┌──────────────────────────┐
           │  Query.FallbackRead()    │
           │  .Scan(&dest)            │
           └────────────┬─────────────┘
                        │
                        ▼
           ┌──────────────────────────┐
           │  ReadStrategy.Select()   │
           │  → selectedCluster (A)   │
           └────────────┬─────────────┘
                        │
                        ▼
           ┌──────────────────────────┐
           │  Read from cluster A     │
           └────────────┬─────────────┘
                        │
              ┌─────────┼─────────┐
              │         │         │
           success   not-found   error
              │         │         │
              ▼         ▼         ▼
           return    try B     normal failover
           nil       (below)   path (no change)
                        │
                        ▼
           ┌──────────────────────────┐
           │  Read from cluster B     │
           │  (bypasses drain state)  │
           └────────────┬─────────────┘
                        │
              ┌─────────┼─────────┐
              │         │         │
           success   not-found   error
              │         │         │
              ▼         ▼         ▼
           return    return    return
           nil +     ErrNot-   ErrNotFound
           diverge   Found     (primary's
           metric              healthy answer)
```

**Key behaviors:**

- FallbackRead is CQL-only. It is exposed on `helix.Query` returned by `helix.CQLClient`; raw adapter sessions do not support it.
- FallbackRead only applies to `Scan`, `ScanContext`, `MapScan`, and `MapScanContext`. It has no effect on `Iter` (streaming cursors have no not-found signal) or `Exec` (write operations).
- Not-found is never treated as a cluster failure. It never triggers `IncReadError`, `RecordFailure`, or failover — regardless of whether FallbackRead is enabled.
- When the primary returns a real error (timeout, connection refused), the normal failover path handles it. FallbackRead only activates on not-found.
- FallbackRead bypasses drain state: the caller opted in to checking both clusters, and a draining cluster may still hold the data.
- The fallback attempt reuses the same statement, bound values, and query options on the alternative cluster.
- Both attempts share the same context and deadline. FallbackRead does not create a fresh timeout for the second read.

## Activation Levels

FallbackRead can be activated at three levels, with strict precedence: per-query > context > client default.

### Per-Query (most granular)

```go
// Critical: check both clusters
var user User
err := client.Query("SELECT * FROM users WHERE id = ?", userID).
    FallbackRead().Scan(&user.Name, &user.Email)

// Bulk: no fallback, accept eventual consistency
iter := client.Query("SELECT * FROM events WHERE ts > ?", since).Iter()
```

### Per-Context (request-scoped)

```go
// Enable FallbackRead for all queries in this request handler
ctx := helix.WithFallbackRead(r.Context())

// Both queries below use FallbackRead
err := client.Query("SELECT ...").ScanContext(ctx, &dest)
err = client.Query("SELECT ...").MapScanContext(ctx, result)
```

### Per-Client (global default)

```go
// All queries on this client use FallbackRead by default
criticalClient, _ := helix.NewCQLClient(sessionA, sessionB,
    helix.WithDefaultFallbackRead(true),
)

// Bulk data uses a separate client without FallbackRead
bulkClient, _ := helix.NewCQLClient(sessionA, sessionB)
```

## Availability Semantics

Helix is an AP system. FallbackRead favors availability over strict consistency:

| Scenario | Primary | Alternative | Result | Health impact |
|----------|---------|-------------|--------|---------------|
| Both have data | success | not tried | return data | OnSuccess(primary) |
| Primary missing, alt has data | not-found | success | return data + divergence metric | OnSuccess(alt), IncReadDivergence(primary) |
| Both missing | not-found | not-found | ErrNotFound | none |
| Primary missing, alt down | not-found | error | **ErrNotFound** | IncReadError(alt), RecordFailure(alt) |
| Primary down, alt missing | error | not-found | **ErrNotFound** | IncReadError(primary), RecordFailure(primary) |
| Primary down, alt has data | error | success | return data via normal failover | IncReadError(primary), RecordFailure(primary), OnSuccess(alt) |
| Both have real failures | error | error | DualClusterError | failure recorded on both |

The bold rows are the AP availability guarantee: **a healthy cluster's "not found" is always returned to the caller**, even when the other cluster is unreachable. The alternative — returning a cluster error — would make every read for genuinely nonexistent rows fail during single-cluster outages. FallbackRead must not decrease availability compared to not using it.

> Health recording is unaffected by the return value: `IncReadError` and `RecordFailure` still fire on the cluster that actually failed. Operators see the degradation in dashboards; callers see a usable response.

## Divergence Metric

When FallbackRead finds data on the alternative cluster that the primary was missing, `IncReadDivergence` is emitted with the stale cluster's label. This metric directly correlates with replay lag:

```
helix_read_divergence_total{cluster="us_east"} 42
```

A rising divergence count on a specific cluster means replay is falling behind for that cluster. Alert on the rate of change, not the absolute value.

## Error Handling

Use `helix.IsNotFound()` to check for not-found results:

```go
err := client.Query("SELECT * FROM users WHERE id = ?", userID).
    FallbackRead().Scan(&name)

if helix.IsNotFound(err) {
    // Row absent on all reachable clusters (see Availability Semantics
    // for behavior when one cluster is unreachable)
    return nil, ErrUserNotFound
}
if err != nil {
    // Both clusters had real failures — DualClusterError
    return nil, fmt.Errorf("read failed: %w", err)
}
```

**Important:** Always use `helix.IsNotFound(err)` or `errors.Is(err, helix.ErrNotFound)` — not `errors.Is(err, gocql.ErrNotFound)`. The adapter layer maps driver-specific errors to `types.ErrNotFound` at the boundary.

With FallbackRead, a non-nil, non-NotFound error means **both clusters had real failures** (a `DualClusterError`). This only happens when neither cluster could respond at all.

## Single-Cluster Mode

When `sessionB` is nil (single-cluster mode), FallbackRead is a safe no-op. There is no alternative cluster to try, so the primary's result is returned directly. You can safely enable `WithDefaultFallbackRead(true)` on a single-cluster client — it won't panic or change behavior. This allows code to be written once and work in both single-cluster (development) and dual-cluster (production) deployments without conditional logic.

## Performance & Latency Overhead

FallbackRead is **sequential, not parallel**. It issues the query to the primary cluster first. Only if the primary cluster responds with `ErrNotFound` will it issue a second query to the alternative cluster.

* **Latency Overhead**: Successful reads (data found on the primary) incur zero extra latency. Queries for definitively missing rows (e.g., queries for a non-existent UUID) will incur the latency of *two* sequential database round-trips.
* **Cluster Load**: FallbackRead does not double your base read load. However, the throughput cost for queries that yield zero rows will double since both clusters must confirm the row is absent.
* **Timeout Budgeting**: The fallback attempt runs under the same context deadline as the first attempt. If your timeout budget is very tight, the second read may have little or no time left to complete.

## Missing Data vs. Stale Data

FallbackRead specifically resolves **missing rows** (where the primary cluster has no record of the row, but the alternative cluster does). **It does not resolve stale data.**

If the primary cluster has an older version of the row (e.g., `status = 'pending'`) while the alternative cluster has the updated version (`status = 'completed'`), FallbackRead will **not** activate. Because the primary cluster successfully returned a row (no `ErrNotFound` was generated), Helix returns that row immediately and short-circuits. FallbackRead won't compare timestamps across clusters; it strictly activates when zero rows are returned.

FallbackRead also does **not** repair the stale or missing cluster on read. A fallback hit emits a divergence metric, but convergence still depends on the replay system.

## Best Practices

1. **Separate clients for different data tiers.** Create one client with `WithDefaultFallbackRead(true)` for critical data (user profiles, orders, sessions) and another without it for bulk data (events, metrics, logs). This is cleaner than per-query opt-in when entire services operate on one data tier.

2. **Monitor `read_divergence_total`.** A sustained rate above zero means replay is lagging. Investigate the replay queue depth and worker throughput for the stale cluster.

3. **FallbackRead does not replace the replay system.** FallbackRead is a read-time safety net; replay is the write-time convergence mechanism. Without replay, FallbackRead may recover reads temporarily but the clusters will not converge.

4. **Do not use FallbackRead with `Iter`.** Iterators return a streaming cursor where "no rows" is an empty iteration with nil error — there is no not-found signal to trigger a fallback. For multi-row queries where completeness matters, query both clusters explicitly.

5. **Understand the partial-outage trade-off.** When one cluster is down and the other says not-found, FallbackRead returns `ErrNotFound` to preserve availability. In the common case the row genuinely doesn't exist. In the rare case where the row exists only on the unreachable cluster, this is a false negative. If this is unacceptable for your use case, implement application-level retry with backoff — the down cluster may recover between attempts.

6. **Use context-level activation for request-scoped critical reads.** When a single request handler needs FallbackRead for some queries but not others, `helix.WithFallbackRead(ctx)` is more readable than per-query chaining.

7. **Budget for two round-trips when you set deadlines.** If you want FallbackRead to be effective under load, size your read timeout for a normal read plus one extra sequential fallback attempt.
