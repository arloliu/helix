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

	// NewBatch creates a new Batch instance for grouping multiple mutations.
	//
	// Deprecated: Use Batch() instead. This method exists for gocql v1 compatibility.
	//
	// Parameters:
	//   - kind: Type of batch (Logged, Unlogged, or Counter)
	//
	// Returns:
	//   - Batch: A batch builder for adding statements
	NewBatch(kind BatchType) Batch

	// ExecuteBatch executes a batch of statements.
	//
	// Deprecated: Use Batch().Exec() instead. This method exists for gocql v1 compatibility.
	//
	// Parameters:
	//   - batch: The batch to execute
	//
	// Returns:
	//   - error: nil on success, error if execution fails
	ExecuteBatch(batch Batch) error

	// ExecuteBatchCAS executes a batch lightweight transaction.
	//
	// Deprecated: Use Batch().ExecCAS() instead. This method exists for gocql v1 compatibility.
	//
	// Parameters:
	//   - batch: The batch to execute
	//   - dest: Pointers to variables to receive previous values if not applied
	//
	// Returns:
	//   - applied: true if the CAS operation was applied
	//   - iter: Iterator for scanning additional CAS result rows
	//   - err: error if the operation failed
	ExecuteBatchCAS(batch Batch, dest ...any) (applied bool, iter Iter, err error)

	// MapExecuteBatchCAS executes a batch lightweight transaction and scans into a map.
	//
	// Deprecated: Use Batch().MapExecCAS() instead. This method exists for gocql v1 compatibility.
	//
	// Parameters:
	//   - batch: The batch to execute
	//   - dest: Map to receive previous values if not applied
	//
	// Returns:
	//   - applied: true if the CAS operation was applied
	//   - iter: Iterator for scanning additional CAS result rows
	//   - err: error if the operation failed
	MapExecuteBatchCAS(batch Batch, dest map[string]any) (applied bool, iter Iter, err error)

	// Close terminates connections to both clusters.
	//
	// After Close is called, the session cannot be reused.
	Close()
}

// Query mirrors gocql.Query and provides chainable configuration.
//
// The method called to execute the query determines whether it's a read or write:
//   - Exec/ExecContext → Write Strategy (Dual Write)
//   - Scan/Iter/MapScan → Read Strategy (Sticky Read)
//
// # Context Usage Patterns
//
// Two equivalent patterns are supported for context handling:
//
//	// Pattern 1: Direct context method (recommended for simple cases)
//	err := query.ExecContext(ctx)
//	result := query.ScanContext(ctx, &dest)
//
//	// Pattern 2: Method chaining (useful with multiple options)
//	err := query.Consistency(helix.Quorum).WithContext(ctx).Exec()
//
// Both patterns respect context cancellation and timeouts. Choose Pattern 1
// when you only need to set a context. Choose Pattern 2 when chaining multiple
// configuration calls (consistency, page size, timestamps, etc.).
type Query interface {
	// WithContext associates a context with the query.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//
	// Returns:
	//   - Query: The same query for chaining
	WithContext(ctx context.Context) Query

	// Consistency sets the consistency level for the query.
	//
	// Parameters:
	//   - c: Consistency level
	//
	// Returns:
	//   - Query: The same query for chaining
	Consistency(c Consistency) Query

	// SetConsistency sets the consistency level for the query.
	//
	// Deprecated: Use Consistency() instead. This method exists for gocql v1 compatibility.
	//
	// Parameters:
	//   - c: Consistency level
	SetConsistency(c Consistency)

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

	// SetConsistency sets the consistency level for the batch.
	//
	// Deprecated: Use Consistency() instead. This method exists for gocql v1 compatibility.
	//
	// Parameters:
	//   - c: Consistency level
	SetConsistency(c Consistency)

	// WithContext associates a context with the batch.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//
	// Returns:
	//   - Batch: The same batch for chaining
	WithContext(ctx context.Context) Batch

	// WithTimestamp sets a specific timestamp for all statements in the batch.
	//
	// Parameters:
	//   - ts: Timestamp in microseconds since Unix epoch
	//
	// Returns:
	//   - Batch: The same batch for chaining
	WithTimestamp(ts int64) Batch

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
