// Package cql provides CQL-specific adapter interfaces for different gocql versions.
package cql

import (
	"context"

	"github.com/arloliu/helix/types"
)

// Type aliases for convenience - re-export from types package.
type (
	BatchType   = types.BatchType
	Consistency = types.Consistency
)

// Re-export batch type constants for convenience.
const (
	LoggedBatch   = types.LoggedBatch
	UnloggedBatch = types.UnloggedBatch
	CounterBatch  = types.CounterBatch
)

// Re-export consistency level constants for convenience.
const (
	Any         = types.Any
	One         = types.One
	Two         = types.Two
	Three       = types.Three
	Quorum      = types.Quorum
	All         = types.All
	LocalQuorum = types.LocalQuorum
	EachQuorum  = types.EachQuorum
	Serial      = types.Serial
	LocalSerial = types.LocalSerial
	LocalOne    = types.LocalOne
)

// Session represents a raw CQL session from the underlying driver.
//
// This interface is implemented by adapters for gocql v1 and v2.
// It provides the low-level operations that Helix orchestrates.
type Session interface {
	// Query creates a new query for the given statement.
	//
	// Parameters:
	//   - stmt: CQL statement with ? placeholders
	//   - values: Values to bind to placeholders
	//
	// Returns:
	//   - Query: A query builder
	Query(stmt string, values ...any) Query

	// Batch creates a new batch of the given type.
	//
	// Parameters:
	//   - kind: Type of batch
	//
	// Returns:
	//   - Batch: A batch builder
	Batch(kind BatchType) Batch

	// NewBatch creates a new batch of the given type.
	//
	// Deprecated: Use Batch() instead. This method exists for gocql v1 compatibility.
	//
	// Parameters:
	//   - kind: Type of batch
	//
	// Returns:
	//   - Batch: A batch builder
	NewBatch(kind BatchType) Batch

	// ExecuteBatch executes a batch.
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
	ExecuteBatchCAS(batch Batch, dest ...any) (applied bool, iter Iter, err error)

	// MapExecuteBatchCAS executes a batch lightweight transaction and scans into a map.
	//
	// Deprecated: Use Batch().MapExecCAS() instead. This method exists for gocql v1 compatibility.
	MapExecuteBatchCAS(batch Batch, dest map[string]any) (applied bool, iter Iter, err error)

	// Close terminates the session.
	Close()
}

// Query represents a raw CQL query from the underlying driver.
type Query interface {
	// WithContext associates a context with the query.
	// Deprecated: Use ExecContext, ScanContext, IterContext, or MapScanContext instead.
	WithContext(ctx context.Context) Query

	// Consistency sets the consistency level.
	Consistency(c Consistency) Query

	// SetConsistency sets the consistency level.
	//
	// Deprecated: Use Consistency() instead. This method exists for gocql v1 compatibility.
	SetConsistency(c Consistency)

	// PageSize sets the page size.
	PageSize(n int) Query

	// PageState sets the pagination state.
	PageState(state []byte) Query

	// WithTimestamp sets the write timestamp.
	WithTimestamp(ts int64) Query

	// Exec executes the query.
	Exec() error

	// ExecContext executes the query with context.
	ExecContext(ctx context.Context) error

	// Scan executes and scans a single row.
	Scan(dest ...any) error

	// ScanContext executes and scans a single row with context.
	ScanContext(ctx context.Context, dest ...any) error

	// Iter returns an iterator for results.
	Iter() Iter

	// IterContext returns an iterator for results with context.
	IterContext(ctx context.Context) Iter

	// MapScan executes and scans into a map.
	MapScan(m map[string]any) error

	// MapScanContext executes and scans into a map with context.
	MapScanContext(ctx context.Context, m map[string]any) error

	// ScanCAS executes a lightweight transaction and scans the result.
	// Returns applied=true if the transaction succeeded.
	ScanCAS(dest ...any) (applied bool, err error)

	// ScanCASContext executes a lightweight transaction with context.
	ScanCASContext(ctx context.Context, dest ...any) (applied bool, err error)

	// MapScanCAS executes a lightweight transaction and scans into a map.
	// Returns applied=true if the transaction succeeded.
	MapScanCAS(dest map[string]any) (applied bool, err error)

	// MapScanCASContext executes a lightweight transaction with context.
	MapScanCASContext(ctx context.Context, dest map[string]any) (applied bool, err error)

	// SerialConsistency sets the consistency level for the serial phase of CAS operations.
	// Valid values are Serial or LocalSerial.
	SerialConsistency(c Consistency) Query

	// Statement returns the CQL statement.
	Statement() string

	// Values returns the bound values.
	Values() []any

	// Release returns the query to a pool (if applicable).
	Release()
}

// Batch represents a raw CQL batch from the underlying driver.
type Batch interface {
	// Query adds a statement to the batch.
	Query(stmt string, args ...any) Batch

	// Consistency sets the consistency level.
	Consistency(c Consistency) Batch

	// SetConsistency sets the consistency level.
	//
	// Deprecated: Use Consistency() instead. This method exists for gocql v1 compatibility.
	SetConsistency(c Consistency)

	// WithContext associates a context with the batch.
	// Deprecated: Use ExecContext or IterContext instead.
	WithContext(ctx context.Context) Batch

	// WithTimestamp sets the write timestamp for all statements.
	WithTimestamp(ts int64) Batch

	// Exec executes the batch.
	Exec() error

	// ExecContext executes the batch with context.
	ExecContext(ctx context.Context) error

	// IterContext executes the batch with context and returns an iterator.
	IterContext(ctx context.Context) Iter

	// ExecCAS executes a batch lightweight transaction.
	// Returns applied=true if the transaction succeeded, and an iterator for results.
	ExecCAS(dest ...any) (applied bool, iter Iter, err error)

	// ExecCASContext executes a batch lightweight transaction with context.
	ExecCASContext(ctx context.Context, dest ...any) (applied bool, iter Iter, err error)

	// MapExecCAS executes a batch lightweight transaction and scans into a map.
	MapExecCAS(dest map[string]any) (applied bool, iter Iter, err error)

	// MapExecCASContext executes a batch lightweight transaction with context.
	MapExecCASContext(ctx context.Context, dest map[string]any) (applied bool, iter Iter, err error)

	// SerialConsistency sets the consistency level for the serial phase of CAS operations.
	// Valid values are Serial or LocalSerial.
	SerialConsistency(c Consistency) Batch

	// Size returns the number of statements in the batch.
	Size() int

	// Statements returns all statements in the batch.
	Statements() []BatchEntry
}

// BatchEntry represents a single statement in a batch.
type BatchEntry struct {
	Statement string
	Args      []any
}

// Iter represents a raw CQL iterator from the underlying driver.
type Iter interface {
	// Scan reads the next row.
	Scan(dest ...any) bool

	// Close closes the iterator.
	Close() error

	// MapScan reads the next row into a map.
	MapScan(m map[string]any) bool

	// SliceMap reads all rows into a slice of maps.
	SliceMap() ([]map[string]any, error)

	// PageState returns the pagination token.
	PageState() []byte

	// NumRows returns the number of rows in the current page.
	NumRows() int

	// Columns returns metadata about the columns in the result set.
	Columns() []ColumnInfo

	// Scanner returns a database/sql-style scanner for the iterator.
	Scanner() Scanner

	// Warnings returns any warnings from the Cassandra server.
	Warnings() []string
}

// ColumnInfo holds metadata about a column in query results.
type ColumnInfo struct {
	Keyspace string
	Table    string
	Name     string
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
