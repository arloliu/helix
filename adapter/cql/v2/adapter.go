// Package v2 provides an adapter for gocql v2 (github.com/apache/cassandra-gocql-driver).
package v2

import (
	"context"
	"errors"
	"fmt"
	"sync"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/types"
)

// mapNotFound translates gocql.ErrNotFound to types.ErrNotFound at the adapter boundary.
func mapNotFound(err error) error {
	if errors.Is(err, gocql.ErrNotFound) {
		return types.ErrNotFound
	}
	return err
}

// mapUnreachable wraps driver errors that mean the cluster could not be
// reached in types.ErrClusterUnreachable.
// The driver error stays in the chain for errors.Is and errors.As.
func mapUnreachable(err error) error {
	if err == nil {
		return nil
	}
	var unavailable *gocql.RequestErrUnavailable
	switch {
	case errors.Is(err, gocql.ErrNoConnections),
		errors.Is(err, gocql.ErrNoConnectionsStarted),
		errors.Is(err, gocql.ErrConnectionClosed),
		errors.Is(err, gocql.ErrSessionClosed),
		errors.Is(err, gocql.ErrNoHosts),
		errors.As(err, &unavailable):
		return fmt.Errorf("%w: %w", types.ErrClusterUnreachable, err)
	default:
		return err
	}
}

// mapErr applies every adapter-boundary translation to a driver result.
func mapErr(err error) error {
	if err = mapNotFound(err); err == nil || err == types.ErrNotFound { //nolint:errorlint // identity check on the sentinel mapNotFound just returned
		return err
	}

	return mapUnreachable(err)
}

// mapCAS applies mapErr to a lightweight-transaction result.
func mapCAS(applied bool, err error) (bool, error) {
	return applied, mapErr(err)
}

// Session wraps a gocql v2 session.
//
// Close is idempotent: a sync.Once guards the underlying gocql.Session.Close
// so the adapter remains safe to double-Close even if a future gocql release
// drops its own internal idempotency guarantee. Helix relies on this contract
// — the CQLClient docstring promises Close-on-bundled-adapter is safe.
type Session struct {
	session   *gocql.Session
	closeOnce sync.Once
}

// NewSession creates a new v2 adapter from a gocql session.
//
// NewSession panics if session is nil. Always pass a fully initialized
// [gocql.Session] obtained from [gocql.ClusterConfig.CreateSession].
//
// Parameters:
//   - session: A gocql.Session instance from the Apache driver (must not be nil)
//
// Returns:
//   - *Session: An adapter implementing cql.Session
func NewSession(session *gocql.Session) *Session {
	if session == nil {
		panic("cql/v2: NewSession called with nil gocql.Session")
	}
	return &Session{session: session}
}

// WrapSession is an alias for NewSession that wraps a gocql v2 session.
//
// This is useful for migrating existing gocql code to use Helix.
//
// Example:
//
//	cluster := gocql.NewCluster("127.0.0.1")
//	session, _ := cluster.CreateSession()
//	helix.NewCQLClient(v2.WrapSession(session), nil) // single-cluster mode
//
// Parameters:
//   - session: A gocql.Session instance from the Apache driver
//
// Returns:
//   - cql.Session: An adapter implementing cql.Session interface
func WrapSession(session *gocql.Session) cql.Session {
	return NewSession(session)
}

// Query creates a new query for the given statement.
//
// Parameters:
//   - stmt: CQL statement with ? placeholders
//   - values: Values to bind to placeholders
//
// Returns:
//   - cql.Query: A query builder
func (s *Session) Query(stmt string, values ...any) cql.Query {
	return &Query{
		query:     s.session.Query(stmt, values...),
		statement: stmt,
		values:    values,
	}
}

// Batch creates a new batch of the given type.
//
// Parameters:
//   - kind: Type of batch
//
// Returns:
//   - cql.Batch: A batch builder
func (s *Session) Batch(kind cql.BatchType) cql.Batch {
	return &Batch{
		batch: s.session.Batch(gocql.BatchType(kind)),
	}
}

// NewBatch creates a new batch of the given type.
//
// Deprecated: Use Batch() instead. This method exists for gocql v1 compatibility.
func (s *Session) NewBatch(kind cql.BatchType) cql.Batch {
	return s.Batch(kind)
}

// ExecuteBatch executes a batch.
//
// Deprecated: Use Batch().Exec() instead. This method exists for gocql v1 compatibility.
func (s *Session) ExecuteBatch(batch cql.Batch) error {
	return batch.Exec()
}

// ExecuteBatchCAS executes a batch lightweight transaction.
//
// Deprecated: Use Batch().ExecCAS() instead. This method exists for gocql v1 compatibility.
func (s *Session) ExecuteBatchCAS(batch cql.Batch, dest ...any) (applied bool, iter cql.Iter, err error) {
	return batch.ExecCAS(dest...)
}

// MapExecuteBatchCAS executes a batch lightweight transaction and scans into a map.
//
// Deprecated: Use Batch().MapExecCAS() instead. This method exists for gocql v1 compatibility.
func (s *Session) MapExecuteBatchCAS(batch cql.Batch, dest map[string]any) (applied bool, iter cql.Iter, err error) {
	return batch.MapExecCAS(dest)
}

// Close terminates the session. Safe to call multiple times; the underlying
// gocql.Session.Close is invoked at most once.
func (s *Session) Close() {
	s.closeOnce.Do(s.session.Close)
}

// Query wraps a gocql v2 query.
type Query struct {
	query     *gocql.Query
	statement string
	values    []any
	ctx       context.Context
}

// WithContext associates a context with the query.
//
// Deprecated: Use the *Context variants (ExecContext, ScanContext, IterContext,
// ScanCASContext, MapScanCASContext) instead. Those methods are safe for
// concurrent use; WithContext mutates internal state without synchronization.
func (q *Query) WithContext(ctx context.Context) cql.Query {
	q.ctx = ctx

	return q
}

// Consistency sets the consistency level.
func (q *Query) Consistency(c cql.Consistency) cql.Query {
	q.query = q.query.Consistency(gocql.Consistency(c))

	return q
}

// SetConsistency sets the consistency level.
//
// Deprecated: Use Consistency() instead. This method exists for gocql v1 compatibility.
func (q *Query) SetConsistency(c cql.Consistency) {
	q.query = q.query.Consistency(gocql.Consistency(c))
}

// PageSize sets the page size.
func (q *Query) PageSize(n int) cql.Query {
	q.query = q.query.PageSize(n)

	return q
}

// PageState sets the pagination state.
func (q *Query) PageState(state []byte) cql.Query {
	q.query = q.query.PageState(state)

	return q
}

// WithTimestamp sets the write timestamp.
func (q *Query) WithTimestamp(ts int64) cql.Query {
	q.query = q.query.WithTimestamp(ts)

	return q
}

// Exec executes the query.
func (q *Query) Exec() error {
	if q.ctx != nil {
		return mapErr(q.query.ExecContext(q.ctx))
	}
	return mapErr(q.query.Exec())
}

// Scan executes and scans a single row.
func (q *Query) Scan(dest ...any) error {
	if q.ctx != nil {
		return mapErr(q.query.ScanContext(q.ctx, dest...))
	}
	return mapErr(q.query.Scan(dest...))
}

// Iter returns an iterator for results.
func (q *Query) Iter() cql.Iter {
	if q.ctx != nil {
		return &Iter{iter: q.query.IterContext(q.ctx)}
	}
	return &Iter{iter: q.query.Iter()}
}

// MapScan executes and scans into a map.
func (q *Query) MapScan(m map[string]any) error {
	if q.ctx != nil {
		return mapErr(q.query.MapScanContext(q.ctx, m))
	}
	return mapErr(q.query.MapScan(m))
}

// Statement returns the CQL statement.
func (q *Query) Statement() string {
	return q.statement
}

// Values returns the bound values.
func (q *Query) Values() []any {
	return q.values
}

// Release is a no-op for v2 as it doesn't have query pooling.
func (q *Query) Release() {
	// v2 driver doesn't have Release method - no-op
}

// ExecContext executes the query with context.
func (q *Query) ExecContext(ctx context.Context) error {
	return mapErr(q.query.ExecContext(ctx))
}

// ScanContext executes and scans a single row with context.
func (q *Query) ScanContext(ctx context.Context, dest ...any) error {
	return mapErr(q.query.ScanContext(ctx, dest...))
}

// IterContext returns an iterator for results with context.
func (q *Query) IterContext(ctx context.Context) cql.Iter {
	return &Iter{iter: q.query.IterContext(ctx)}
}

// MapScanContext executes and scans into a map with context.
func (q *Query) MapScanContext(ctx context.Context, m map[string]any) error {
	return mapErr(q.query.MapScanContext(ctx, m))
}

// ScanCAS executes a lightweight transaction and scans the result.
func (q *Query) ScanCAS(dest ...any) (applied bool, err error) {
	if q.ctx != nil {
		return mapCAS(q.query.ScanCASContext(q.ctx, dest...))
	}
	return mapCAS(q.query.ScanCAS(dest...))
}

// ScanCASContext executes a lightweight transaction with context.
func (q *Query) ScanCASContext(ctx context.Context, dest ...any) (applied bool, err error) {
	return mapCAS(q.query.ScanCASContext(ctx, dest...))
}

// MapScanCAS executes a lightweight transaction and scans into a map.
func (q *Query) MapScanCAS(dest map[string]any) (applied bool, err error) {
	if q.ctx != nil {
		return mapCAS(q.query.MapScanCASContext(q.ctx, dest))
	}
	return mapCAS(q.query.MapScanCAS(dest))
}

// MapScanCASContext executes a lightweight transaction with context.
func (q *Query) MapScanCASContext(ctx context.Context, dest map[string]any) (applied bool, err error) {
	return mapCAS(q.query.MapScanCASContext(ctx, dest))
}

// SerialConsistency sets the consistency level for the serial phase of CAS operations.
func (q *Query) SerialConsistency(c cql.Consistency) cql.Query {
	q.query = q.query.SerialConsistency(gocql.Consistency(c))

	return q
}

// Batch wraps a gocql v2 batch. The gocql batch is the single source of truth for
// batch entries; Size and Statements derive from it.
type Batch struct {
	batch *gocql.Batch
	ctx   context.Context
}

// Query adds a statement to the batch.
func (b *Batch) Query(stmt string, args ...any) cql.Batch {
	b.batch = b.batch.Query(stmt, args...)

	return b
}

// Consistency sets the consistency level.
func (b *Batch) Consistency(c cql.Consistency) cql.Batch {
	b.batch = b.batch.Consistency(gocql.Consistency(c))

	return b
}

// SetConsistency sets the consistency level.
//
// Deprecated: Use Consistency() instead. This method exists for gocql v1 compatibility.
func (b *Batch) SetConsistency(c cql.Consistency) {
	b.batch = b.batch.Consistency(gocql.Consistency(c))
}

// WithContext associates a context with the batch.
//
// Deprecated: Use the *Context variants (ExecContext, ExecCASContext,
// MapExecCASContext, IterContext) instead. Those methods are safe for
// concurrent use; WithContext mutates internal state without synchronization.
func (b *Batch) WithContext(ctx context.Context) cql.Batch {
	b.ctx = ctx

	return b
}

// WithTimestamp sets the write timestamp for all statements.
func (b *Batch) WithTimestamp(ts int64) cql.Batch {
	b.batch = b.batch.WithTimestamp(ts)

	return b
}

// Exec executes the batch.
func (b *Batch) Exec() error {
	if b.ctx != nil {
		return mapErr(b.batch.ExecContext(b.ctx))
	}
	return mapErr(b.batch.Exec())
}

// ExecContext executes the batch with context.
func (b *Batch) ExecContext(ctx context.Context) error {
	return mapErr(b.batch.ExecContext(ctx))
}

// IterContext executes the batch with context and returns an iterator.
func (b *Batch) IterContext(ctx context.Context) cql.Iter {
	return &Iter{iter: b.batch.IterContext(ctx)}
}

// ExecCAS executes a batch lightweight transaction.
func (b *Batch) ExecCAS(dest ...any) (applied bool, iter cql.Iter, err error) {
	if b.ctx != nil {
		return b.ExecCASContext(b.ctx, dest...)
	}
	applied, gocqlIter, err := b.batch.ExecCAS(dest...)
	return applied, &Iter{iter: gocqlIter}, mapErr(err)
}

// ExecCASContext executes a batch lightweight transaction with context.
func (b *Batch) ExecCASContext(ctx context.Context, dest ...any) (applied bool, iter cql.Iter, err error) {
	applied, gocqlIter, err := b.batch.ExecCASContext(ctx, dest...)
	return applied, &Iter{iter: gocqlIter}, mapErr(err)
}

// MapExecCAS executes a batch lightweight transaction and scans into a map.
func (b *Batch) MapExecCAS(dest map[string]any) (applied bool, iter cql.Iter, err error) {
	if b.ctx != nil {
		return b.MapExecCASContext(b.ctx, dest)
	}
	applied, gocqlIter, err := b.batch.MapExecCAS(dest)
	return applied, &Iter{iter: gocqlIter}, mapErr(err)
}

// MapExecCASContext executes a batch lightweight transaction with context.
func (b *Batch) MapExecCASContext(ctx context.Context, dest map[string]any) (applied bool, iter cql.Iter, err error) {
	applied, gocqlIter, err := b.batch.MapExecCASContext(ctx, dest)
	return applied, &Iter{iter: gocqlIter}, mapErr(err)
}

// Statements returns all statements in the batch. The slice is built on demand from
// the underlying gocql batch (Args are shared, not copied), so callers that never call
// Statements pay nothing for it.
func (b *Batch) Statements() []cql.BatchEntry {
	entries := make([]cql.BatchEntry, len(b.batch.Entries))
	for i := range b.batch.Entries {
		entries[i] = cql.BatchEntry{
			Statement: b.batch.Entries[i].Stmt,
			Args:      b.batch.Entries[i].Args,
		}
	}

	return entries
}

// SerialConsistency sets the consistency level for the serial phase of CAS operations.
func (b *Batch) SerialConsistency(c cql.Consistency) cql.Batch {
	b.batch = b.batch.SerialConsistency(gocql.Consistency(c))

	return b
}

// Size returns the number of statements in the batch.
func (b *Batch) Size() int {
	return len(b.batch.Entries)
}

// Iter wraps a gocql v2 iterator.
type Iter struct {
	iter *gocql.Iter
}

// Scan reads the next row.
func (i *Iter) Scan(dest ...any) bool {
	if i.iter == nil {
		return false
	}
	return i.iter.Scan(dest...)
}

// Close closes the iterator.
func (i *Iter) Close() error {
	if i.iter == nil {
		return nil
	}
	return mapErr(i.iter.Close())
}

// MapScan reads the next row into a map.
func (i *Iter) MapScan(m map[string]any) bool {
	if i.iter == nil {
		return false
	}
	return i.iter.MapScan(m)
}

// SliceMap reads all rows into a slice of maps.
func (i *Iter) SliceMap() ([]map[string]any, error) {
	if i.iter == nil {
		return nil, nil
	}
	rows, err := i.iter.SliceMap()

	return rows, mapErr(err)
}

// PageState returns the pagination token.
func (i *Iter) PageState() []byte {
	if i.iter == nil {
		return nil
	}
	return i.iter.PageState()
}

// NumRows returns the number of rows in the current page.
func (i *Iter) NumRows() int {
	if i.iter == nil {
		return 0
	}
	return i.iter.NumRows()
}

// Columns returns metadata about the columns in the result set.
func (i *Iter) Columns() []cql.ColumnInfo {
	if i.iter == nil {
		return nil
	}
	gocqlCols := i.iter.Columns()
	result := make([]cql.ColumnInfo, len(gocqlCols))
	for idx, col := range gocqlCols {
		result[idx] = cql.ColumnInfo{
			Keyspace: col.Keyspace,
			Table:    col.Table,
			Name:     col.Name,
			TypeInfo: col.TypeInfo,
		}
	}

	return result
}

// Scanner returns a database/sql-style scanner for the iterator.
func (i *Iter) Scanner() cql.Scanner {
	if i.iter == nil {
		return &scanner{scanner: nil}
	}
	return &scanner{scanner: i.iter.Scanner()}
}

// Warnings returns any warnings from the Cassandra server.
func (i *Iter) Warnings() []string {
	if i.iter == nil {
		return nil
	}
	return i.iter.Warnings()
}

// scanner wraps gocql.Scanner to implement cql.Scanner.
type scanner struct {
	scanner gocql.Scanner
}

func (s *scanner) Next() bool {
	if s.scanner == nil {
		return false
	}
	return s.scanner.Next()
}

func (s *scanner) Scan(dest ...any) error {
	if s.scanner == nil {
		return nil
	}
	return mapErr(s.scanner.Scan(dest...))
}

func (s *scanner) Err() error {
	if s.scanner == nil {
		return nil
	}
	return mapErr(s.scanner.Err())
}
