# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
