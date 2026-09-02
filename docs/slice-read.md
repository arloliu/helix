# Slice Read Guide

The four bounded multi-row read methods on `helix.Query` — `SliceMap`, `SliceMapContext`, `SliceScan`, and `SliceScanContext` — drain a query into a concrete in-memory result. Unlike `Iter` (which returns a streaming cursor), slice methods materialize all rows up to the configured limit before returning.

> Use slice methods when you need the full result set in memory and want [FallbackRead](fallback-read.md) recovery for missing partitions. Use `Iter` for streaming or unbounded scans where latency matters more than completeness.

---

## Methods

### `SliceMap` / `SliceMapContext`

```go
func (q Query) SliceMap() ([]map[string]any, error)
func (q Query) SliceMapContext(ctx context.Context) ([]map[string]any, error)
```

Drains the query into a `[]map[string]any` — one map per row, keyed by column name. Semantics match `Iter().SliceMap()` with the addition of `MaxRows` bounding and `FallbackRead` support.

| Return | Meaning |
|--------|---------|
| `(rows, nil)` | N > 0 rows |
| `(nil, nil)` | Zero rows on all attempted clusters |
| `(nil, ErrRowLimitExceeded)` | Row cap exceeded before drain completed |
| `(nil, err)` | Cluster error, or the caller's own context error (never counted against the cluster) |

### `SliceScan` / `SliceScanContext`

```go
func (q Query) SliceScan(scanFn func(r RowScanner) error) (rowCount int, err error)
func (q Query) SliceScanContext(ctx context.Context, scanFn func(r RowScanner) error) (rowCount int, err error)
```

Drains the query and invokes `scanFn` once per row. `RowScanner` exposes only `Scan(dest ...any) error` — the callback cannot advance or close the underlying iterator.

| Return | Meaning |
|--------|---------|
| `(N, nil)` | N rows scanned successfully |
| `(0, nil)` | Zero rows |
| `(N, ErrRowLimitExceeded)` | N rows scanned before the limit was hit |
| `(N, err)` | `scanFn` returned `err` on row N+1, or cluster/context error |

**No standard failover.** `SliceScan` does not participate in the normal `executeRead` failover path (primary error → retry on secondary). [FallbackRead](fallback-read.md) empty-retry still applies when `FallbackRead()` is set.

---

## MaxRows

`MaxRows(n int) Query` sets a per-query row cap. `Config.DefaultMaxRows` sets the client-wide default. When the (N+1)th row is read, the method aborts with [`ErrRowLimitExceeded`](#errrowlimitexceeded) and discards the partial accumulator.

```go
rows, err := client.Query("SELECT * FROM events WHERE org = ?", orgID).
    MaxRows(50_000).
    SliceMapContext(ctx)
if helix.IsRowLimitExceeded(err) {
    // Partition has more than 50,000 rows — paginate or narrow the query.
}
```

### Precedence

| Setting | Wins when |
|---------|-----------|
| `q.MaxRows(n)` where n > 0 | Always beats the client default |
| `q.MaxRows(0)` | Clears the per-query override; client default applies |
| `Config.DefaultMaxRows` | Used when no per-query override is set |
| Neither set | No cap; drain is unbounded |

### Page-size clamp

When `MaxRows` is active, Helix clamps the gocql page size to `min(pageSize, maxRows+1)` before issuing the first request. The `+1` lets Helix detect the overflow row without fetching a second page while still preventing large over-fetches on very large partitions.

---

## Error Handling

### `ErrRowLimitExceeded`

`ErrRowLimitExceeded` is an application-level cap signal, not a cluster fault. It never triggers `IncReadError`, `RecordFailure`, auto-refresh, or a FallbackRead retry. Check for it with `helix.IsRowLimitExceeded` or `errors.Is`:

```go
if helix.IsRowLimitExceeded(err) {
    return nil, ErrPartitionTooLarge
}
```

### Empty results

Slice methods return a clean empty — not `ErrNotFound` — when the query matches zero rows:

| Method | Zero-row result |
|--------|----------------|
| `SliceMap` / `SliceMapContext` | `(nil, nil)` |
| `SliceScan` / `SliceScanContext` | `(0, nil)` |

Check for zero length or zero count rather than `errors.Is(err, ErrNotFound)`:

```go
rows, err := client.Query("SELECT * FROM orders WHERE user = ?", userID).
    SliceMapContext(ctx)
if err != nil {
    return nil, fmt.Errorf("read failed: %w", err)
}
if len(rows) == 0 {
    return nil, ErrOrderNotFound
}
```

---

## `SliceScanAs[T]`

`SliceScanAs` is a generic free function layered over `SliceScanContext` that returns a `[]T` instead of using a side-effecting accumulator:

```go
func SliceScanAs[T any](
    ctx context.Context,
    q Query,
    decode func(r RowScanner, dst *T) error,
) ([]T, error)
```

```go
type Order struct {
    ID     string
    Status string
}

orders, err := helix.SliceScanAs(ctx,
    client.Query("SELECT id, status FROM orders WHERE user = ?", userID).
        FallbackRead().MaxRows(1_000),
    func(r helix.RowScanner, dst *Order) error {
        return r.Scan(&dst.ID, &dst.Status)
    },
)
```

All `Query` options — `FallbackRead`, `MaxRows`, `PageState`, etc. — apply transparently through the underlying `SliceScanContext` call.

On any error, `SliceScanAs` returns `(nil, err)`. The partial accumulator is not exposed. Use `SliceScan` directly when partial results on error are required.

---

## FallbackRead Integration

When `FallbackRead()` is set on a slice query, Helix retries the query against the alternative cluster if the primary returns zero rows. This recovers partitions that exist on only one cluster due to partial write failures or replay lag.

Slice methods have two FallbackRead-specific behaviors that differ from `Scan`/`MapScan`:

- **Empty-result shape** — a both-empty result returns `(nil, nil)` or `(0, nil)`, not `ErrNotFound`.
- **Drain-aware skip** — when the alternative cluster is draining, slice methods skip the fallback attempt rather than reading from a cluster in transition.

See the [FallbackRead Guide — Slice Methods](fallback-read.md#slice-methods) for the full availability table and semantics.

---

## When to Use Which Method

| Scenario | Method |
|----------|--------|
| Untyped multi-row result | `SliceMap` / `SliceMapContext` |
| Typed struct per row; custom accumulation | `SliceScan` / `SliceScanContext` |
| Typed slice; no partial-state needed on error | `SliceScanAs[T]` |
| Single-row lookup | `Scan` / `MapScan` |
| Streaming or unbounded; missing rows acceptable | `Iter` (no FallbackRead) |

---

## Performance Notes

**Materialization.** All rows up to `MaxRows` are held in memory before the method returns. For large partitions, size `MaxRows` conservatively and paginate via `PageState` if needed. A token from `Iter.PageState` names the cluster that issued it, and every paged read follows it there.

**Page-size clamp.** When `MaxRows` is set, the effective gocql page size is clamped to `maxRows+1` unless the caller already set a smaller page size. For very small caps (e.g., `MaxRows(10)`), this avoids fetching a full page just to discard all but the first few rows while still allowing Helix to detect overflow immediately.

**FallbackRead latency.** When FallbackRead is enabled and the primary returns zero rows, a second sequential read is issued against the alternative. Budget for two round-trips in your deadline for queries on partitions that may be absent on the primary.
