package helix

import "context"

// CQLSession mirrors gocql.Session and provides a drop-in replacement interface.
//
// This interface abstracts over different gocql versions (v1 and v2) and allows
// Helix to perform dual-cluster operations transparently.
type CQLSession interface {
	// Query creates a new Query instance for the given statement.
	//
	// The method called on the returned Query determines the strategy:
	//   - Exec/ExecContext: Triggers Write Strategy (Dual Write)
	//   - Scan/Iter/MapScan: Triggers Read Strategy (Sticky Read)
	//
	// Parameters:
	//   - stmt: CQL statement with ? placeholders
	//   - values: Values to bind to placeholders
	//
	// Returns:
	//   - Query: A query builder for further configuration
	Query(stmt string, values ...any) Query

	// Batch creates a new Batch instance for grouping multiple mutations.
	//
	// All statements in a batch are executed atomically against both clusters
	// using the Write Strategy.
	//
	// Parameters:
	//   - kind: Type of batch (Logged, Unlogged, or Counter)
	//
	// Returns:
	//   - Batch: A batch builder for adding statements
	Batch(kind BatchType) Batch

	// Close terminates connections to both clusters.
	//
	// After Close is called, the session cannot be reused. Implementations
	// must be idempotent: a second Close on an already-closed session must
	// not panic and must not block indefinitely. [CQLClient] satisfies this
	// contract; alternative implementations (mocks, decorators) must do the
	// same.
	Close()
}

// Query mirrors gocql.Query and provides chainable configuration.
//
// The method called to execute the query determines whether it's a read or write:
//   - Exec/ExecContext → Write Strategy (Dual Write)
//   - Scan/Iter/MapScan → Read Strategy (Sticky Read)
//
// # Context Usage
//
// Pass context directly to the *Context method variants:
//
//	err := query.ExecContext(ctx)
//	err = query.ScanContext(ctx, &dest)
//	iter := query.IterContext(ctx)
type Query interface {
	// Consistency sets the consistency level for the query.
	//
	// Parameters:
	//   - c: Consistency level
	//
	// Returns:
	//   - Query: The same query for chaining
	Consistency(c Consistency) Query

	// PageSize sets the number of rows to fetch per page.
	//
	// Parameters:
	//   - n: Number of rows per page
	//
	// Returns:
	//   - Query: The same query for chaining
	PageSize(n int) Query

	// PageState sets the pagination state for resuming iteration.
	//
	// Parameters:
	//   - state: Opaque pagination token from a previous Iter.PageState()
	//
	// Returns:
	//   - Query: The same query for chaining
	PageState(state []byte) Query

	// WithTimestamp sets a specific timestamp for the write operation.
	//
	// This is critical for idempotency in dual-write scenarios.
	// If not set, the client's TimestampProvider is used.
	//
	// Parameters:
	//   - ts: Timestamp in microseconds since Unix epoch
	//
	// Returns:
	//   - Query: The same query for chaining
	WithTimestamp(ts int64) Query

	// WithPriority sets the replay priority level for this write operation.
	//
	// Priority affects replay processing order when a write fails on one cluster.
	// High priority writes are processed before low priority writes.
	// If not set, defaults to PriorityHigh.
	//
	// Parameters:
	//   - p: Priority level (PriorityHigh or PriorityLow)
	//
	// Returns:
	//   - Query: The same query for chaining
	WithPriority(p PriorityLevel) Query

	// SerialConsistency sets the consistency level for the serial phase of CAS operations.
	//
	// This only applies to conditional updates (INSERT/UPDATE with IF clause).
	// Valid values are Serial or LocalSerial.
	//
	// Parameters:
	//   - c: Serial consistency level (Serial or LocalSerial)
	//
	// Returns:
	//   - Query: The same query for chaining
	SerialConsistency(c Consistency) Query

	// Mirror marks this write to be asynchronously mirrored to the helix
	// mirror destination configured via [WithMirror].
	//
	// Mirroring is fire-and-forget: the mirror write is dispatched on a
	// background worker pool after the primary write completes successfully
	// (at least one current-pair cluster acked). Errors on the mirror leg
	// are never surfaced to the caller. Captures are dropped silently (with
	// metrics and a log entry) when the mirror queue is full or the engine
	// is disabled.
	//
	// Mirror has no effect on read or CAS operations and no effect when
	// [WithMirror] was not configured. The original write timestamp is
	// preserved on the mirror exec so server-side WRITETIME, LWW, TTL, and
	// tombstone semantics match the primary cluster.
	//
	// Returns:
	//   - Query: The same query for chaining
	Mirror() Query

	// Strict marks the statement for strong-consistency dual-write semantics.
	//
	// On partial failure (one cluster fails, times out, is degraded, or is
	// draining), Strict returns [*types.PartialWriteError] instead of silently
	// enqueueing for replay. On [policy.AdaptiveDualWrite], Strict fast-fails to
	// degraded clusters rather than fire-and-forgetting; the recovery probe
	// continues running in the background so degraded clusters still self-heal.
	// Pair with [Query.FallbackRead] on reads when the row may be absent on one cluster.
	//
	// What Strict guarantees: no Helix-side replay; no fire-and-forget;
	// explicit partial-failure surface to the caller.
	//
	// What Strict does NOT guarantee: atomic dual-cluster writes,
	// exactly-once delivery, or divergence detection. A [*types.PartialWriteError]
	// means the unacknowledged cluster did not respond OK before the deadline;
	// the mutation MAY have applied. Caller retries on PartialWriteError can
	// still double-apply non-idempotent ops.
	//
	// Use only for writes whose replay is unsafe (counters, list/set append,
	// tombstone-race-sensitive flows). Normal column overwrites are already
	// replay-safe via timestamps; using Strict() for those is unnecessary.
	//
	// Strict and Mirror are mutually exclusive: combining them returns
	// [types.ErrStrictMirrorUnsupported] before any write is attempted.
	//
	// In single-cluster mode or CAS operations, Strict is a documented no-op.
	//
	// Returns:
	//   - Query: The same query for chaining
	Strict() Query

	// FallbackRead enables best-effort read from both clusters for this query.
	//
	// When the selected cluster returns not-found (zero rows), Helix silently
	// tries the other cluster before returning not-found to the caller.
	//
	// This is a best-effort check, not a guaranteed dual-cluster read.
	// If the alternative cluster is unreachable (timeout, connection refused,
	// etc.), FallbackRead returns [ErrNotFound] from the healthy primary
	// cluster — it does NOT propagate the alternative's network error to the
	// caller. Health metrics ([MetricsCollector.IncReadError]) and failure
	// tracking ([FailoverPolicy.RecordFailure]) are still recorded on the
	// unreachable cluster, but the caller sees only a clean not-found result.
	// This preserves availability: without this behavior, enabling FallbackRead
	// would cause reads to fail during single-cluster outages for rows that
	// genuinely do not exist.
	//
	// Not-found results are never recorded as cluster health failures regardless
	// of whether FallbackRead is set. FallbackRead only controls whether
	// a second cluster is attempted after a not-found.
	//
	// FallbackRead only applies to Scan, ScanContext, MapScan, and MapScanContext.
	// It has no effect on Iter, Exec, or CAS operations. Iter returns a streaming
	// cursor where "no rows" is an empty iteration with nil error — there is no
	// not-found signal to trigger a fallback.
	//
	// Circuit-breaker note: a cluster that consistently lags behind (replay
	// backlog) will produce many not-found results that are silently recovered
	// via FallbackRead. Not-found is not treated as a health failure, so the
	// lagging cluster will NOT trip a [CircuitBreaker] or [LatencyCircuitBreaker]
	// through FallbackRead reads alone. Monitor [MetricsCollector.IncReadDivergence]
	// to detect persistent replay lag and alert before it becomes a write-path issue.
	//
	// Returns:
	//   - Query: The same query for chaining
	FallbackRead() Query

	// Exec executes a write query using the Write Strategy.
	//
	// This triggers concurrent dual-write to both clusters.
	// Returns nil if at least one cluster succeeds (partial writes
	// are handled via the Replayer).
	//
	// Returns:
	//   - error: nil on success (at least one cluster), error if both fail
	Exec() error

	// ExecContext executes a write query with the given context.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//
	// Returns:
	//   - error: nil on success, error if both clusters fail
	ExecContext(ctx context.Context) error

	// Scan executes a read query and scans a single row into dest.
	//
	// This triggers the Read Strategy (Sticky Read with failover).
	//
	// Parameters:
	//   - dest: Pointers to variables to receive column values
	//
	// Returns:
	//   - error: nil on success, error if read fails on all clusters
	Scan(dest ...any) error

	// ScanContext executes a read query with context.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - dest: Pointers to variables to receive column values
	//
	// Returns:
	//   - error: nil on success, error if read fails
	ScanContext(ctx context.Context, dest ...any) error

	// Iter returns an iterator for reading multiple rows.
	//
	// This triggers the Read Strategy. The iterator reads from
	// the selected cluster based on sticky routing and failover.
	//
	// Returns:
	//   - Iter: Iterator for scanning rows
	Iter() Iter

	// IterContext returns an iterator with the given context.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//
	// Returns:
	//   - Iter: Iterator for scanning rows
	IterContext(ctx context.Context) Iter

	// MapScan executes a read and scans a single row into the map.
	//
	// Parameters:
	//   - m: Map to receive column name → value pairs
	//
	// Returns:
	//   - error: nil on success, error if read fails
	MapScan(m map[string]any) error

	// MapScanContext executes a read with context and scans a single row into the map.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - m: Map to receive column name → value pairs
	//
	// Returns:
	//   - error: nil on success, error if read fails
	MapScanContext(ctx context.Context, m map[string]any) error

	// ScanCAS executes a lightweight transaction (INSERT/UPDATE with IF clause).
	//
	// If the transaction fails because conditions weren't met, the previous
	// values are stored in dest.
	//
	// NOTE: CAS operations are executed on a single cluster and are NOT replicated.
	//
	// Parameters:
	//   - dest: Pointers to variables to receive previous values if not applied
	//
	// Returns:
	//   - applied: true if the CAS operation was applied
	//   - err: error if the operation failed
	ScanCAS(dest ...any) (applied bool, err error)

	// ScanCASContext executes a lightweight transaction with context.
	//
	// NOTE: CAS operations are executed on a single cluster and are NOT replicated.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - dest: Pointers to variables to receive previous values if not applied
	//
	// Returns:
	//   - applied: true if the CAS operation was applied
	//   - err: error if the operation failed
	ScanCASContext(ctx context.Context, dest ...any) (applied bool, err error)

	// MapScanCAS executes a lightweight transaction and returns previous values as a map.
	//
	// NOTE: CAS operations are executed on a single cluster and are NOT replicated.
	//
	// Parameters:
	//   - dest: Map to receive previous values if not applied
	//
	// Returns:
	//   - applied: true if the CAS operation was applied
	//   - err: error if the operation failed
	MapScanCAS(dest map[string]any) (applied bool, err error)

	// MapScanCASContext executes a lightweight transaction with context.
	//
	// NOTE: CAS operations are executed on a single cluster and are NOT replicated.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - dest: Map to receive previous values if not applied
	//
	// Returns:
	//   - applied: true if the CAS operation was applied
	//   - err: error if the operation failed
	MapScanCASContext(ctx context.Context, dest map[string]any) (applied bool, err error)

	// MaxRows caps the number of rows admitted by [Query.SliceMap],
	// [Query.SliceMapContext], [Query.SliceScan], and [Query.SliceScanContext].
	//
	// The (N+1)th row aborts the read with [ErrRowLimitExceeded]. The bound
	// applies independently per cluster: if both clusters yield more than
	// N rows, both reads abort with the same sentinel.
	//
	// Passing 0 clears the per-query override. A negative value is
	// programmer error; see the implementation for the panic boundary
	// introduced alongside Config.DefaultMaxRows.
	//
	// MaxRows has no effect on [Query.Scan], [Query.MapScan], [Query.Iter],
	// or CAS methods.
	//
	// Returns:
	//   - Query: The same query for chaining
	MaxRows(n int) Query

	// SliceMap drains the query into a slice of column-name → value maps.
	//
	// Semantics match Iter().SliceMap() except:
	//   - Bounded by [Query.MaxRows] / Config.DefaultMaxRows. Exceeding the
	//     bound returns (nil, [ErrRowLimitExceeded]) and discards the
	//     partially materialized buffer.
	//   - When [Query.FallbackRead] is enabled and the selected cluster
	//     returns zero rows, Helix silently retries the drain on the
	//     alternative cluster (best-effort; the alternative is skipped if
	//     draining). Empty result on the alternative is returned as
	//     (nil, nil).
	//
	// The full result set is materialized in memory. Use [Query.Iter] for
	// unbounded or large result sets.
	//
	// Returns:
	//   - []map[string]any: All rows up to the configured cap; nil on empty or error
	//   - error: nil on success, [ErrRowLimitExceeded] on overflow, or a
	//     cluster error
	SliceMap() ([]map[string]any, error)

	// SliceMapContext is the context-aware variant of [Query.SliceMap].
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//
	// Returns:
	//   - []map[string]any: All rows up to the configured cap; nil on empty or error
	//   - error: nil on success, [ErrRowLimitExceeded] on overflow, ctx error
	//     on cancellation, or a cluster error
	SliceMapContext(ctx context.Context) ([]map[string]any, error)

	// SliceScan drains the query and invokes scanFn once per row.
	//
	// scanFn receives a [RowScanner] positioned at the current row; the
	// caller uses RowScanner.Scan(dest...) to copy column values via the
	// driver's destination-driven unmarshalling path. Returning a non-nil
	// error from scanFn aborts iteration and propagates the error. The
	// RowScanner is valid only for the duration of the callback — do not
	// retain it after scanFn returns.
	//
	// If scanFn is nil, SliceScan returns an error before any cluster
	// contact — neither the primary nor the alternative cluster is queried.
	//
	// SliceScan does NOT participate in standard executeRead failover for
	// selected-cluster errors. Any error from the selected cluster is
	// returned to the caller with the number of completed scanFn
	// invocations; the caller is expected to retry with a fresh
	// accumulator. The conservative all-error rule prevents post-callback
	// silent corruption: re-running on the alternative would invoke scanFn
	// a second time on rows already mutated into the caller's accumulator
	// from the failed primary. [Query.FallbackRead] empty-retry still
	// applies — a selected cluster that returns zero rows triggers a
	// single drain attempt on the alternative.
	//
	// Bounded by [Query.MaxRows] / Config.DefaultMaxRows. On overflow,
	// SliceScan returns (N, [ErrRowLimitExceeded]) where N is the number
	// of scanFn invocations that returned nil before the (N+1)th row was
	// detected. The caller's accumulator state on err != nil is undefined
	// per the failover contract above; rowCount is diagnostic-only.
	//
	// Parameters:
	//   - scanFn: Callback invoked once per row with a narrow RowScanner
	//
	// Returns:
	//   - rowCount: Number of successful scanFn invocations before return
	//   - err: nil on success, [ErrRowLimitExceeded] on overflow, the
	//     scanFn-returned error if non-nil, or a cluster error
	SliceScan(scanFn func(r RowScanner) error) (rowCount int, err error)

	// SliceScanContext is the context-aware variant of [Query.SliceScan].
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - scanFn: Callback invoked once per row with a narrow RowScanner
	//
	// Returns:
	//   - rowCount: Number of successful scanFn invocations before return
	//   - err: nil on success, [ErrRowLimitExceeded] on overflow, the
	//     scanFn-returned error if non-nil, ctx error on cancellation,
	//     or a cluster error
	SliceScanContext(ctx context.Context, scanFn func(r RowScanner) error) (rowCount int, err error)
}

// RowScanner is the narrow scan surface presented to a [Query.SliceScan]
// callback. It exposes only [RowScanner.Scan] so a callback cannot accidentally
// advance or close the underlying iterator — both are owned by Helix's
// counted-drain loop.
//
// The pointer types passed to Scan follow the same destination-driven
// convention as [Query.Scan]: custom UnmarshalCQL, blob coercion, and any
// other driver unmarshalling rules apply unchanged. The RowScanner is valid
// only for the duration of a single scanFn invocation.
type RowScanner interface {
	// Scan reads the current row's columns into the destination pointers.
	//
	// Parameters:
	//   - dest: Pointers to variables to receive column values
	//
	// Returns:
	//   - error: Any error from the underlying driver scan
	Scan(dest ...any) error
}

// Batch mirrors gocql.Batch for grouping multiple mutations.
//
// All statements in a batch are executed atomically and use the Write Strategy.
type Batch interface {
	// Query adds a statement to the batch.
	//
	// Parameters:
	//   - stmt: CQL statement with ? placeholders
	//   - args: Values to bind to placeholders
	//
	// Returns:
	//   - Batch: The same batch for chaining
	Query(stmt string, args ...any) Batch

	// Consistency sets the consistency level for the batch.
	//
	// Parameters:
	//   - c: Consistency level
	//
	// Returns:
	//   - Batch: The same batch for chaining
	Consistency(c Consistency) Batch

	// WithTimestamp sets a specific timestamp for all statements in the batch.
	//
	// Parameters:
	//   - ts: Timestamp in microseconds since Unix epoch
	//
	// Returns:
	//   - Batch: The same batch for chaining
	WithTimestamp(ts int64) Batch

	// WithPriority sets the replay priority level for this batch operation.
	//
	// Priority affects replay processing order when a write fails on one cluster.
	// High priority writes are processed before low priority writes.
	// If not set, defaults to PriorityHigh.
	//
	// Parameters:
	//   - p: Priority level (PriorityHigh or PriorityLow)
	//
	// Returns:
	//   - Batch: The same batch for chaining
	WithPriority(p PriorityLevel) Batch

	// SerialConsistency sets the consistency level for the serial phase of CAS operations.
	//
	// This only applies to batch CAS operations.
	// Valid values are Serial or LocalSerial.
	//
	// Parameters:
	//   - c: Serial consistency level (Serial or LocalSerial)
	//
	// Returns:
	//   - Batch: The same batch for chaining
	SerialConsistency(c Consistency) Batch

	// Mirror marks this batch to be asynchronously mirrored to the helix
	// mirror destination configured via [WithMirror]. The whole batch is
	// mirrored atomically; mixed mirror/non-mirror statements within a
	// single batch are not supported.
	//
	// See [Query.Mirror] for full semantics.
	//
	// Returns:
	//   - Batch: The same batch for chaining
	Mirror() Batch

	// Strict marks the batch for strong-consistency dual-write semantics.
	//
	// See [Query.Strict] for full semantics and guarantees.
	//
	// Returns:
	//   - Batch: The same batch for chaining
	Strict() Batch

	// Size returns the number of statements in the batch.
	//
	// Returns:
	//   - int: Number of statements added to the batch
	Size() int

	// Exec executes the batch using the Write Strategy.
	//
	// This triggers concurrent dual-write to both clusters.
	//
	// Returns:
	//   - error: nil on success, error if both clusters fail
	Exec() error

	// ExecContext executes the batch with the given context.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//
	// Returns:
	//   - error: nil on success, error if both clusters fail
	ExecContext(ctx context.Context) error

	// IterContext executes the batch with context and returns an iterator.
	//
	// This is useful for accessing execution metadata like latency and attempts.
	// The iterator reads from the selected cluster based on sticky routing.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//
	// Returns:
	//   - Iter: Iterator for accessing batch execution results
	IterContext(ctx context.Context) Iter

	// ExecCAS executes a batch with lightweight transactions.
	//
	// NOTE: CAS operations are executed on a single cluster and are NOT replicated.
	//
	// Parameters:
	//   - dest: Pointers to variables to receive previous values if not applied
	//
	// Returns:
	//   - applied: true if the CAS operation was applied
	//   - iter: Iterator for scanning additional CAS result rows
	//   - err: error if the operation failed
	ExecCAS(dest ...any) (applied bool, iter Iter, err error)

	// ExecCASContext executes a batch with lightweight transactions and context.
	//
	// NOTE: CAS operations are executed on a single cluster and are NOT replicated.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - dest: Pointers to variables to receive previous values if not applied
	//
	// Returns:
	//   - applied: true if the CAS operation was applied
	//   - iter: Iterator for scanning additional CAS result rows
	//   - err: error if the operation failed
	ExecCASContext(ctx context.Context, dest ...any) (applied bool, iter Iter, err error)

	// MapExecCAS executes a batch with lightweight transactions and returns previous values as a map.
	//
	// NOTE: CAS operations are executed on a single cluster and are NOT replicated.
	//
	// Parameters:
	//   - dest: Map to receive previous values if not applied
	//
	// Returns:
	//   - applied: true if the CAS operation was applied
	//   - iter: Iterator for scanning additional CAS result rows
	//   - err: error if the operation failed
	MapExecCAS(dest map[string]any) (applied bool, iter Iter, err error)

	// MapExecCASContext executes a batch with lightweight transactions, context, and map result.
	//
	// NOTE: CAS operations are executed on a single cluster and are NOT replicated.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - dest: Map to receive previous values if not applied
	//
	// Returns:
	//   - applied: true if the CAS operation was applied
	//   - iter: Iterator for scanning additional CAS result rows
	//   - err: error if the operation failed
	MapExecCASContext(ctx context.Context, dest map[string]any) (applied bool, iter Iter, err error)
}

// ColumnInfo holds metadata about a column in query results.
type ColumnInfo struct {
	// Keyspace is the keyspace containing the table.
	Keyspace string

	// Table is the table name.
	Table string

	// Name is the column name.
	Name string

	// TypeInfo describes the CQL type (implementation-specific).
	TypeInfo any
}

// Scanner provides database/sql-style row scanning.
type Scanner interface {
	// Next advances to the next row, returning true if a row is available.
	Next() bool

	// Scan reads the current row into dest.
	Scan(dest ...any) error

	// Err returns any error from iteration and releases resources.
	Err() error
}

// Iter mirrors gocql.Iter for iterating over query results.
type Iter interface {
	// Scan reads the next row into dest.
	//
	// Parameters:
	//   - dest: Pointers to variables to receive column values
	//
	// Returns:
	//   - bool: true if a row was read, false if no more rows
	Scan(dest ...any) bool

	// Close closes the iterator and returns any error.
	//
	// Returns:
	//   - error: Any error that occurred during iteration
	Close() error

	// MapScan reads the next row into the map.
	//
	// Parameters:
	//   - m: Map to receive column name → value pairs
	//
	// Returns:
	//   - bool: true if a row was read, false if no more rows
	MapScan(m map[string]any) bool

	// SliceMap reads all remaining rows into a slice of maps.
	//
	// Returns:
	//   - []map[string]any: All remaining rows
	//   - error: Any error that occurred
	SliceMap() ([]map[string]any, error)

	// PageState returns the pagination token for resuming iteration.
	//
	// Returns:
	//   - []byte: Opaque pagination token
	PageState() []byte

	// NumRows returns the number of rows in the current page.
	//
	// Note: This only reflects rows in the current page, not total results.
	// The value updates when new pages are fetched.
	//
	// Returns:
	//   - int: Number of rows in current page
	NumRows() int

	// Columns returns metadata about the columns in the result set.
	//
	// Returns:
	//   - []ColumnInfo: Slice of column metadata
	Columns() []ColumnInfo

	// Scanner returns a database/sql-style scanner for the iterator.
	//
	// After calling Scanner, the iterator should not be used directly.
	//
	// Returns:
	//   - Scanner: Scanner for row-by-row processing
	Scanner() Scanner

	// Warnings returns any warnings from the Cassandra server.
	//
	// Available in CQL protocol v4 and above.
	//
	// Returns:
	//   - []string: Server warning messages
	Warnings() []string
}
