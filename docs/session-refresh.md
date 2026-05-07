# Session Refresh Guide

Helix's session refresh feature recovers from situations where a cluster's underlying `cql.Session` becomes permanently dead — typically when the cluster's network endpoint changes (cluster restart with port reassignment, DNS rotation, host migration). Without it, the only recovery is to tear down the entire `helix.CQLClient` and rebuild it, losing the topology watcher, the running replay worker, and any references the application holds.

This guide covers two layers:

1. **Manual refresh** — `SwapSession` and `RefreshSession` exposed for callers who detect dead sessions themselves.
2. **Automatic refresh** — `WithAutoRefresh` enables a background detector that invokes the refresher when Helix observes a cluster's session is permanently dead.

The decoupling principle is preserved at every layer: Helix never imports a specific gocql driver and so cannot construct a `cql.Session` itself. The caller supplies a `SessionRefresher` factory; Helix calls it.

---

## When You Need This

You need session refresh if **any** of these can happen in your environment:

- Cluster restart that reassigns its host:port (Kubernetes pod cycling, container restart with dynamic ports, blue/green migration).
- DNS rotation against a stable cluster name where the underlying IPs change and the driver's host filter rejects the new ones.
- Long network partition exhausts the driver's internal retry budget; the session is "alive" but every operation fails.

If your cluster endpoints are stable and the driver's reconnect logic always recovers in your environment, you may not need this. The Pause/Unpause failure mode (TCP frozen but endpoint stable) is handled entirely by the existing failover policy + driver reconnect — session refresh adds nothing there.

---

## Quick Start (Auto-refresh)

For most callers, this is all you need:

```go
client, err := helix.NewCQLClient(sessionA, sessionB,
    // … usual options …

    // Tell Helix HOW to build a fresh session for a cluster.
    helix.WithSessionRefresher(func(ctx context.Context, cluster helix.ClusterID, lastErr error) (cql.Session, error) {
        // Caller code: rebuild the underlying gocql session against the
        // cluster's current endpoint, then wrap it with the v1/v2 adapter.
        gocqlSession, err := buildGocqlSession(cluster) // your code
        if err != nil {
            return nil, err
        }
        return cqlv1.NewSession(gocqlSession), nil
    }),

    // Tell Helix WHEN to invoke the refresher (defaults shown for clarity).
    helix.WithAutoRefresh(),
)
```

Helix now monitors per-cluster op outcomes. When a cluster's session looks permanently dead, Helix invokes your refresher in a background goroutine and atomically swaps the result in. Existing reads and writes start succeeding against the new session. **You don't have to detect when, or call anything yourself.**

---

## How Auto-refresh Decides

A cluster is considered "session permanently dead" when **all three** are true:

1. `consecutiveFailures >= FailureThreshold` (default 10)
2. `time.Since(lastSuccess) >= SustainedFailureWindow` (default 5 minutes)
3. `time.Since(lastRefresh) >= MinRetryInterval` (default 1 minute, throttle)

The detector goroutine ticks every `CheckInterval` (default 30s) and evaluates each cluster independently.

Defaults are intentionally conservative — refresh storms (the detector firing repeatedly) are operationally far worse than slow recovery. Tune per-knob if you need faster reaction:

```go
helix.WithAutoRefresh(
    helix.WithAutoRefreshFailureThreshold(5),               // fewer failures before suspecting
    helix.WithAutoRefreshSustainedFailureWindow(time.Minute), // recover faster from blips
    helix.WithAutoRefreshMinRetryInterval(30*time.Second),  // tighter retry floor
    helix.WithAutoRefreshCheckInterval(5*time.Second),      // detector polls more often
    helix.WithAutoRefreshRefreshTimeout(10*time.Second),    // your refresher's budget
)
```

The throttle (`MinRetryInterval`) bounds the rate at which auto-refresh can fire. Even with a refresher that always errors, attempts are capped at one per `MinRetryInterval` per cluster.

---

## Operational States the Detector Filters

`recordOpOutcome` (the helper that updates the per-cluster failure counter) filters the following non-failure states so they never trigger auto-refresh:

| State | Why it's not counted |
|---|---|
| `nil` (success) | Resets `consecutiveFailures` to 0; advances `lastSuccess` |
| `types.ErrNotFound` | Cluster responded correctly with "no row" — health signal is positive |
| `types.ErrWriteAsync` | `AdaptiveDualWrite` dispatched async; not a connection failure |
| `types.ErrWriteDropped` | Concurrency limit reached; not a connection failure |

This depends on Helix's adapter normalization contract: gocql v1 and v2 `ErrNotFound` are translated to `types.ErrNotFound` at the adapter boundary. If you build a custom adapter that surfaces driver-native not-found errors, revisit this contract.

---

## Manual Path: `SwapSession` and `RefreshSession`

If you detect a dead session yourself (e.g., your application has more context than Helix's per-op stats), use `RefreshSession`:

```go
if err := client.RefreshSession(ctx, helix.ClusterA); err != nil {
    log.Printf("refresh failed: %v", err)
}
```

This invokes the registered refresher synchronously, swaps the new session in atomically, and **closes the old session** because the refresh contract implies the old one is dead.

For full caller control over old-session lifecycle, use `SwapSession`:

```go
freshSession := buildFreshSessionYourself()
oldSession, err := client.SwapSession(helix.ClusterA, freshSession)
if err != nil {
    return err
}
// In-flight operations may still be using the old session. Close it
// only after you're confident they've drained.
oldSession.Close()
```

`SwapSession` does **not** close the returned old session. The caller decides when in-flight ops have drained.

---

## Concurrency and Lifecycle Semantics

- The swap is **lock-free on the read path**. Concurrent `Query`/`Batch`/`Iter` callers see either the old or the new session, never a partially-swapped state.
- **In-flight ops on the old session — `SwapSession` only.** Operations that have already resolved their session (in-flight `Iter` or CAS, mid-execution synchronous calls, fire-and-forget writes captured into a goroutine) continue against the session they captured. Only operations that resolve the session AFTER the swap observe the new one. This preserves "the write was dispatched to cluster X" semantics — but it relies on the caller deferring `oldSession.Close()` until in-flights have drained, which is `SwapSession`'s contract.
- **`RefreshSession` does not preserve in-flights.** The old session is closed immediately after the atomic swap (the refresh contract implies the old one is dead). Drivers that abort outstanding work on `Close()` will fail any in-flight ops that captured the old session reference. Only call `RefreshSession` when the old session is already non-functional, or use `SwapSession` if you need to drain.
- `SwapSession` and `RefreshSession` reject calls on a closed client (`types.ErrSessionClosed`).
- `SwapSession` rejects nil sessions (`types.ErrNilSession`) and `ClusterB` on a single-cluster client (`types.ErrInvalidCluster`).
- `RefreshSession` returns `types.ErrNoSessionRefresher` if no refresher was registered.
- `client.Close()` cancels the auto-refresh detector goroutine before closing sessions.
- Concurrent `Close` and `Swap`/`Refresh` are documented as undefined: synchronize externally if you need a deterministic order.

---

## Observability

Three optional metrics are exposed via the `types.SessionRefreshMetrics` interface. `MetricsCollector` implementations that embed `internal/metrics.NopMetrics` get them as no-ops automatically; implementers by hand may opt in by adding three methods.

| Metric | Increments when |
|---|---|
| `IncSessionRefreshAttempt(cluster)` | Detector decided to fire and is about to invoke the refresher |
| `IncSessionRefreshSuccess(cluster)` | Refresher returned a non-nil session and the swap installed it |
| `IncSessionRefreshError(cluster)` | Refresher errored, returned nil, or swap failed (e.g., client closed mid-call) |

`Attempt = Success + Error`. The detector also logs at `Info` on attempt and success, `Warn` on error.

---

## What Auto-refresh Does NOT Do

Documented non-goals:

1. **Promote single-cluster to dual.** A client constructed with `sessionB == nil` stays single-cluster; `SwapSession(ClusterB, …)` returns `types.ErrInvalidCluster`.
2. **Atomic dual swap.** Two clusters are evaluated independently. If both die simultaneously, both refresh independently; ordering is arbitrary.
3. **Drain in-flights inside the swap call.** The caller of `SwapSession` is the right party to know when in-flights are quiet (they have the contexts and counters); RefreshSession's own lifecycle ownership is documented above.
4. **Exponential backoff on repeated failures.** v1 uses the linear `MinRetryInterval` floor.

---

## Example: Production-shaped Refresher

A real refresher rebuilds the gocql cluster + session and wraps it with the existing adapter. Your DNS / discovery logic goes inside.

```go
helix.WithSessionRefresher(func(ctx context.Context, cluster helix.ClusterID, lastErr error) (cql.Session, error) {
    contactPoints, err := resolveCluster(ctx, cluster) // your discovery
    if err != nil {
        return nil, fmt.Errorf("resolve %s: %w", cluster, err)
    }

    cfg := gocql.NewCluster(contactPoints...)
    cfg.Keyspace = "myapp"
    cfg.Timeout = 5 * time.Second
    cfg.ConnectTimeout = 5 * time.Second

    session, err := cfg.CreateSession()
    if err != nil {
        return nil, fmt.Errorf("create session for %s: %w", cluster, err)
    }
    return cqlv1.NewSession(session), nil
}),
helix.WithAutoRefresh(),
```

The `lastErr` parameter is the most recently observed failure error against this cluster at the time the refresher is invoked, or nil if no failure has been recorded (typical for caller-driven `RefreshSession` invoked before any op has failed). Refreshers may inspect it to tailor reconnection strategy — for example, a "no hosts available" pattern suggests a hard reachability change (DNS, port reassignment) while a timeout suggests a slow but reachable cluster.

---

## See Also

- [Auto-Recovery Guide](auto-recovery.md) — how the layered recovery story fits together (failover policy, replay, drain mode, session refresh).
- [Replay System](replay-system.md) — the durability story for writes that didn't reach a degraded cluster while the session was being refreshed.
