# Strict Write Guide

`Strict()` is a per-statement opt-in on `Query` and `Batch` that changes how Helix handles partial write failures. Instead of silently enqueueing a failed cluster's write for async replay, Helix surfaces the failure to the caller immediately via `*PartialWriteError`.

> **Quick rule of thumb:** use `Strict()` only for write operations where replay is unsafe —
> counters, list/set append, and tombstone-race-sensitive flows. Normal column overwrites are
> already replay-safe via client-generated timestamps; adding `Strict()` to those writes is
> unnecessary overhead for the caller.

---

## When to Use Strict Writes

Three classes of writes break under the default replay-based reconciliation:

| Write type                              | Why replay is unsafe                                                                       |
| --------------------------------------- | ------------------------------------------------------------------------------------------ |
| **Counters** (`UPDATE … SET c = c + 1`) | Replay double-counts; the counter is incremented a second time on the catching-up cluster. |
| **List/set append/prepend**             | Replay duplicates the appended elements.                                                   |
| **Tombstone-race-sensitive flows**      | A delayed replay arriving after `gc_grace_seconds` can resurrect a deleted row.            |

For all other writes (ordinary column overwrites), the default behaviour — replay with eventual consistency — is the correct choice. Helix's client-generated timestamps make those writes idempotent under Cassandra LWW. Do not use `Strict()` for them.

---

## Guarantees

### What Strict() guarantees

- **No Helix-side replay.** A partial failure is not enqueued. The caller decides what to do.
- **No fire-and-forget.** Under `AdaptiveDualWrite`, degraded clusters are skipped rather than
  receiving a background goroutine. The healthy cluster still receives the write at normal latency.
- **Explicit partial-failure surface.** The caller receives `*PartialWriteError` naming the
  unacknowledged cluster and the underlying cause.
- **Auto-recovery preserved.** The background recovery probe continues running against degraded
  clusters so strict-only workloads still self-heal without operator intervention (see
  [Recovery probe and AdaptiveDualWrite](#recovery-probe-and-adaptivedualwrite) below).

### What Strict() does NOT guarantee

- **Atomicity.** `*PartialWriteError` means "the unacknowledged cluster did not respond OK before
  the deadline." It does **not** prove the mutation was not applied — the write may have committed
  and the response was lost. Caller retries on `PartialWriteError` can still double-apply for
  non-idempotent operations. This is a fundamental Cassandra/network property.
- **Exactly-once delivery.** Helix cannot, and does not claim to, deliver exactly-once across two
  independent clusters.
- **Divergence detection.** If both clusters have the row but at different values, `Strict()` does
  not detect that — neither at write time nor at read time.

---

## Behavior Table

| Scenario                                   | Result                                                                                                         |
| ------------------------------------------ | -------------------------------------------------------------------------------------------------------------- |
| Both clusters acknowledge                  | `nil`                                                                                                          |
| One cluster fails or times out             | `*PartialWriteError{Acknowledged, Unacknowledged, Cause}` — no replay                                          |
| One cluster degraded (`AdaptiveDualWrite`) | `*PartialWriteError{Cause: ErrClusterDegraded}` — degraded cluster skipped, healthy cluster written, no replay |
| One cluster draining (topology)            | `*PartialWriteError{Cause: ErrClusterDraining}` — draining cluster skipped, healthy cluster written, no replay |
| Both clusters fail / degraded / draining   | `*DualClusterError` — no replay                                                                                |
| Cluster slow but not flagged               | Bounded by caller's context deadline; on timeout → `*PartialWriteError`                                        |
| Single-cluster mode                        | No-op — the single write returns its error directly                                                            |
| CAS / LWT (`ScanCAS`, `ExecCAS`)           | No-op — CAS is always single-cluster internally                                                                |

---

## Usage

### Query

```go
import (
    "errors"

    "github.com/arloliu/helix"
    "github.com/arloliu/helix/types"
)

err := client.Query(
    "UPDATE counters SET total = total + 1 WHERE partition = ?", partitionKey,
).Strict().ExecContext(ctx)

switch {
case err == nil:
    // Both clusters acknowledged.

case helix.IsPartialWrite(err):
    pwe, _ := helix.AsPartialWriteError(err)
    // pwe.Acknowledged names the cluster that acked.
    // pwe.Unacknowledged names the cluster that did not.
    // pwe.Cause explains why: a driver error, ErrClusterDegraded, or ErrClusterDraining.
    log.Printf("partial write: acked=%v unacked=%v cause=%v",
        pwe.Acknowledged, pwe.Unacknowledged, pwe.Cause)
    // Compensate, retry, or alert — the caller decides.

case errors.Is(err, helix.ErrStrictUnsupported):
    // The configured WriteStrategy does not implement StrictWriter.
    // All built-in strategies (ConcurrentDualWrite, SyncDualWrite,
    // AdaptiveDualWrite) support Strict(). A custom strategy must
    // implement ExecuteStrict to use this feature.
    log.Printf("strict not supported by current write strategy: %v", err)

default:
    // Both clusters failed → *DualClusterError, or a non-strict error.
    log.Printf("write error: %v", err)
}
```

### Batch

`Strict()` on a `Batch` applies the same semantics to all statements in the batch:

```go
err := client.Batch(helix.LoggedBatch).
    Query("UPDATE counters SET views = views + 1 WHERE id = ?", id).
    Query("UPDATE counters SET recent = recent + 1 WHERE bucket = ?", bucket).
    Strict().
    ExecContext(ctx)
```

---

## Error Types

### PartialWriteError

`*helix.PartialWriteError` (also available as `*types.PartialWriteError`) is returned when
exactly one cluster acknowledged the write.

```go
type PartialWriteError struct {
    Acknowledged   types.ClusterID // cluster that returned OK
    Unacknowledged types.ClusterID // cluster that did not ack
    Cause          error           // underlying cause
}
```

The `Cause` field may be:

| Cause                                           | Meaning                                                                        |
| ----------------------------------------------- | ------------------------------------------------------------------------------ |
| A driver error (timeout, connection refused, …) | The cluster was reachable but the write failed                                 |
| `helix.ErrClusterDegraded`                      | `AdaptiveDualWrite` had already marked the cluster degraded; write was skipped |
| `helix.ErrClusterDraining`                      | The cluster is in topology drain mode; write was skipped                       |

Use the provided helpers:

```go
// Check whether any error is a PartialWriteError
if helix.IsPartialWrite(err) { … }

// Extract the PartialWriteError for field access
if pwe, ok := helix.AsPartialWriteError(err); ok {
    // pwe.Acknowledged, pwe.Unacknowledged, pwe.Cause
}

// Inspect specific skip causes
if errors.Is(err, helix.ErrClusterDegraded) { … }
if errors.Is(err, helix.ErrClusterDraining) { … }
```

### DualClusterError

When both clusters fail, Helix returns `*helix.DualClusterError` (also `*types.DualClusterError`):

```go
var dce *helix.DualClusterError
if errors.As(err, &dce) {
    log.Printf("both clusters failed: A=%v B=%v", dce.ErrorA, dce.ErrorB)
}
```

Both `ErrorA` and `ErrorB` may be `ErrClusterDegraded` or `ErrClusterDraining` if both clusters
were in a skipped state.

---

## Strict and Mirror Are Incompatible

`Strict().Mirror()` is rejected before any write attempt with `helix.ErrStrictMirrorUnsupported`.
Mirror writes are fire-and-forget by design — the mirror destination may have its own replay path
and cannot provide the acknowledgement guarantee that `Strict()` requires:

```go
err := client.Query("INSERT INTO t (k) VALUES (?)", key).
    Strict().Mirror().ExecContext(ctx)
// err == helix.ErrStrictMirrorUnsupported; no write was attempted.
```

---

## Recovery Probe and AdaptiveDualWrite

When `AdaptiveDualWrite` is the write strategy, strict writes skip degraded clusters entirely
rather than dispatching a fire-and-forget goroutine. This means strict-only workloads do not
generate the live dual-writes that normally advance `AdaptiveDualWrite`'s recovery counter.

The **background recovery probe** compensates. By default, `CQLClient` starts one probe goroutine
per cluster when the write strategy implements `helix.ProbeReporter` (`IsDegraded` plus
`RecordProbeSuccess`), which `AdaptiveDualWrite` does. While a cluster is degraded, the probe executes
a lightweight read of `system.local` at a configurable interval and calls
`AdaptiveDualWrite.RecordProbeSuccess` on each success. After `recoveryThreshold` consecutive
successes the cluster is restored to healthy and subsequent strict writes resume dual-cluster
behaviour — no operator action required.

The probe is default-on. To customise or disable it:

```go
// Custom interval and timeout (zero values fall back to defaults)
client, _ := helix.NewCQLClient(sessionA, sessionB,
    helix.WithWriteStrategy(policy.NewAdaptiveDualWrite()),
    helix.WithRecoveryProbe(helix.RecoveryProbe{
        Interval: 5 * time.Second,
        Timeout:  2 * time.Second,
    }),
)

// Custom write-path probe (for environments with write-only failure modes)
client, _ := helix.NewCQLClient(sessionA, sessionB,
    helix.WithWriteStrategy(policy.NewAdaptiveDualWrite()),
    helix.WithRecoveryProbe(helix.RecoveryProbe{
        Probe: func(ctx context.Context, session cql.Session) error {
            return session.Query(
                "INSERT INTO probe_table (id, ts) VALUES (?, ?)",
                "probe", time.Now().UnixMicro(),
            ).ExecContext(ctx)
        },
        Interval: 5 * time.Second,
        Timeout:  2 * time.Second,
    }),
)

// Opt out of probe — manual ForceRecover() only
client, _ := helix.NewCQLClient(sessionA, sessionB,
    helix.WithWriteStrategy(policy.NewAdaptiveDualWrite()),
    helix.WithRecoveryProbeDisabled(),
)
// To recover manually:
//   adaptive := policy.NewAdaptiveDualWrite()
//   adaptive.ForceRecover(helix.ClusterA)
```

The probe has no effect when the `WriteStrategy` does not implement `helix.ProbeReporter`. It is
idle when both clusters are healthy (the ticker still fires, but the probe is not called unless
`IsDegraded` is true).

**Default probe:** reads `release_version` from `system.local`. This is intentionally read-only:
it proves the driver/connection path without schema dependencies or write traffic. If the cluster
still has a write-path-only problem after recovering from the probe's perspective, the next strict
write will fail visibly and `AdaptiveDualWrite` will mark it degraded again. Operators with
known write-path-specific failure modes may override the probe as shown above.

**Probe failure behaviour:** a failing probe (unreachable cluster, bad query, missing schema)
never advances the recovery counter. Consecutive failures are logged at `Warn` on the first
failure and on every power-of-two count (1, 2, 4, 8, …) and at `Debug` otherwise, so a long
outage stays visible without a log line per tick; the first success after failures is logged at
`Info`. `ForceRecover` remains available as an escape valve. Probe failures never degrade a
healthy cluster — probes are recovery-only signals.

**Probe counters:** a `MetricsCollector` that also implements
`types.RecoveryProbeMetrics` receives `IncRecoveryProbeSuccess(cluster)` and
`IncRecoveryProbeFailure(cluster)` for every probe against a degraded cluster.
A healthy cluster is not probed, so it produces neither counter.

---

## FallbackRead After a Strict Partial Write

A `*PartialWriteError` means the unacknowledged cluster may not have the newly written row.
If you read back immediately after a partial write, the selected cluster may return not-found
even though the row exists on the acknowledged cluster.

`FallbackRead()` handles this case: when the selected cluster returns not-found, Helix silently
tries the other cluster before returning `ErrNotFound` to the caller.

```go
// Write: strict, may partially fail
writeErr := client.Query(
    "INSERT INTO orders (id, status) VALUES (?, ?)", orderID, "placed",
).Strict().ExecContext(ctx)

// Read back: FallbackRead guards against partial-write not-found
var status string
readErr := client.Query(
    "SELECT status FROM orders WHERE id = ?", orderID,
).FallbackRead().ScanContext(ctx, &status)
```

> **Note:** `FallbackRead()` activates only on not-found (zero rows returned). It does **not**
> detect divergence — if both clusters have the row but with different values, the read returns
> whichever value the selected cluster holds. See the [FallbackRead Guide](fallback-read.md) for
> full semantics.

---

## Drain-Mode Interaction

When a cluster is in topology drain mode, strict writes treat it the same as a degraded cluster:
the write is skipped for the draining cluster and the caller receives
`*PartialWriteError{Cause: ErrClusterDraining}`. No replay is enqueued regardless of whether a
`Replayer` is configured.

```go
err := client.Query("INSERT INTO t (k) VALUES (?)", key).Strict().ExecContext(ctx)

if errors.Is(err, helix.ErrClusterDraining) {
    // A topology drain is in progress; one cluster was skipped.
    pwe, _ := helix.AsPartialWriteError(err)
    log.Printf("cluster %v is draining — write skipped for it", pwe.Unacknowledged)
}
```

---

## NonIdempotent Writes

`Query.NonIdempotent()` and `Batch.NonIdempotent()` take the same write path
as `Strict()` — synchronous on both clusters, no fire-and-forget, no replay,
`*types.PartialWriteError` on partial failure — but express a different
intent: the statement must not be applied twice. Use it for counter updates
and collection appends. A `CounterBatch` is marked automatically. The one
difference from `Strict()` is that `NonIdempotent()` may be combined with
`Mirror()`: the marker travels with the mirror payload, so the destination
executes the statement on the same strict path and never replays it within
its own pair. Mirror delivery itself may still be retried after an
ambiguous failure, as [docs/mirror.md](mirror.md) describes under
idempotence.

## Custom Write Strategy Support

All three built-in write strategies (`ConcurrentDualWrite`, `SyncDualWrite`,
`AdaptiveDualWrite`) implement `StrictWriter`. If you have a custom strategy:

| Strategy type                                       | Result                                                                              |
| --------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `nil` (no strategy configured)                      | Falls back to inline concurrent write — same as `ConcurrentDualWrite.ExecuteStrict` |
| Built-in strategy                                   | `ExecuteStrict` called — no fire-and-forget, no replay                              |
| Custom strategy implementing `StrictWriter`         | `ExecuteStrict` called                                                              |
| Custom strategy **not** implementing `StrictWriter` | Fails immediately with `ErrStrictUnsupported`; no write attempted                   |

The client discovers every optional capability of a custom strategy by
interface: `helix.StrictWriter` for `Strict()` writes, `helix.ProbeReporter`
for the recovery probe, `helix.EventEmitterSetter` for cluster events, and
`helix.Instrumentable` / `helix.LoggerSetter` for metrics and logger
injection. A strategy that implements none of them still works as a plain
`WriteStrategy`.

To add strict support to a custom strategy, implement `ExecuteStrict`:

```go
// ExecuteStrict must NOT fire-and-forget or enqueue replay.
// Return each cluster's error directly; the caller surfaces partial
// failure as *PartialWriteError.
func (s *myStrategy) ExecuteStrict(
    ctx context.Context,
    writeA func(context.Context) error,
    writeB func(context.Context) error,
) (errA, errB error) {
    var wg sync.WaitGroup
    wg.Go(func() { errA = writeA(ctx) })
    wg.Go(func() { errB = writeB(ctx) })
    wg.Wait()
    return errA, errB
}
```

---

## Single-Cluster Mode and CAS

`Strict()` is a documented no-op in two cases:

- **Single-cluster mode** (`NewCQLClient(sessionA, nil)`): the write runs normally and returns
  its error directly. There is no second cluster to acknowledge or fail.
- **CAS / LWT operations** (`ScanCAS`, `ExecCAS`, and similar): Helix routes CAS operations to a
  single cluster internally. `Strict()` has no additional effect.

---

## Metrics

`StrictMetrics` is an optional extension of `MetricsCollector`. Implement it to receive
strict-write-specific counters:

```go
type StrictMetrics interface {
    // IncWriteSkipped is called when a Strict() write skips a cluster due
    // to ErrClusterDegraded or ErrClusterDraining.
    IncWriteSkipped(cluster types.ClusterID)
}
```

Skipped writes (`ErrClusterDegraded`, `ErrClusterDraining`) do **not** increment `IncWriteError`.
They are operational state, not failures.

---

## See Also

- [Replay System](replay-system.md) — default partial-failure handling via async reconciliation
- [AdaptiveDualWrite Guide](adaptive-dual-write.md) — fire-and-forget mode and recovery probe details
- [FallbackRead Guide](fallback-read.md) — guarding reads after strict partial writes
- [Auto-Recovery Guide](auto-recovery.md) — full recovery lifecycle and operator workflow
