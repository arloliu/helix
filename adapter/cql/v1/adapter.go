// Package v1 provides an adapter for gocql v1 (github.com/gocql/gocql).
package v1

import (
	"context"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/gocql/gocql"
)

// Session wraps a gocql v1 session.
type Session struct {
	session *gocql.Session
}

// NewSession creates a new v1 adapter from a gocql session.
//
// Parameters:
//   - session: A gocql.Session instance
//
// Returns:
//   - *Session: An adapter implementing cql.Session
func NewSession(session *gocql.Session) *Session {
	return &Session{session: session}
}

// WrapSession is an alias for NewSession that wraps a gocql v1 session.
//
// This is useful for migrating existing gocql code to use Helix.
//
// Example:
//
//	cluster := gocql.NewCluster("127.0.0.1")
//	session, _ := cluster.CreateSession()
//	helix.NewCQLClient(v1.WrapSession(session), nil) // single-cluster mode
//
// Parameters:
//   - session: A gocql.Session instance
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
		batch:   s.session.NewBatch(gocql.BatchType(kind)),
		session: s.session,
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

// Close terminates the session.
func (s *Session) Close() {
	s.session.Close()
}

// Query wraps a gocql v1 query.
type Query struct {
	query     *gocql.Query
	statement string
	values    []any
}

// WithContext associates a context with the query.
func (q *Query) WithContext(ctx context.Context) cql.Query {
	q.query = q.query.WithContext(ctx)
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
	return q.query.Exec()
}

// Scan executes and scans a single row.
func (q *Query) Scan(dest ...any) error {
	return q.query.Scan(dest...)
}

// Iter returns an iterator for results.
func (q *Query) Iter() cql.Iter {
	return &Iter{iter: q.query.Iter()}
}

// MapScan executes and scans into a map.
func (q *Query) MapScan(m map[string]any) error {
	return q.query.MapScan(m)
}

// Statement returns the CQL statement.
func (q *Query) Statement() string {
	return q.statement
}

// Values returns the bound values.
func (q *Query) Values() []any {
	return q.values
}

// Release returns the query to the pool.
func (q *Query) Release() {
	q.query.Release()
}

// ExecContext executes the query with context.
func (q *Query) ExecContext(ctx context.Context) error {
	return q.query.WithContext(ctx).Exec()
}

// ScanContext executes and scans a single row with context.
func (q *Query) ScanContext(ctx context.Context, dest ...any) error {
	return q.query.WithContext(ctx).Scan(dest...)
}

// IterContext returns an iterator for results with context.
func (q *Query) IterContext(ctx context.Context) cql.Iter {
	return &Iter{iter: q.query.WithContext(ctx).Iter()}
}

// MapScanContext executes and scans into a map with context.
func (q *Query) MapScanContext(ctx context.Context, m map[string]any) error {
	return q.query.WithContext(ctx).MapScan(m)
}

// ScanCAS executes a lightweight transaction and scans the result.
func (q *Query) ScanCAS(dest ...any) (applied bool, err error) {
	return q.query.ScanCAS(dest...)
}

// ScanCASContext executes a lightweight transaction with context.
func (q *Query) ScanCASContext(ctx context.Context, dest ...any) (applied bool, err error) {
	return q.query.WithContext(ctx).ScanCAS(dest...)
}

// MapScanCAS executes a lightweight transaction and scans into a map.
func (q *Query) MapScanCAS(dest map[string]any) (applied bool, err error) {
	return q.query.MapScanCAS(dest)
}

// MapScanCASContext executes a lightweight transaction with context.
func (q *Query) MapScanCASContext(ctx context.Context, dest map[string]any) (applied bool, err error) {
	return q.query.WithContext(ctx).MapScanCAS(dest)
}

// SerialConsistency sets the consistency level for the serial phase of CAS operations.
func (q *Query) SerialConsistency(c cql.Consistency) cql.Query {
	q.query = q.query.SerialConsistency(gocql.SerialConsistency(c))
	return q
}

// Batch wraps a gocql v1 batch.
type Batch struct {
	batch   *gocql.Batch
	session *gocql.Session
	entries []cql.BatchEntry
}

// Query adds a statement to the batch.
func (b *Batch) Query(stmt string, args ...any) cql.Batch {
	b.batch.Query(stmt, args...)
	b.entries = append(b.entries, cql.BatchEntry{
		Statement: stmt,
		Args:      args,
	})
	return b
}

// Consistency sets the consistency level.
func (b *Batch) Consistency(c cql.Consistency) cql.Batch {
	b.batch.SetConsistency(gocql.Consistency(c))
	return b
}

// SetConsistency sets the consistency level.
//
// Deprecated: Use Consistency() instead. This method exists for gocql v1 compatibility.
func (b *Batch) SetConsistency(c cql.Consistency) {
	b.batch.SetConsistency(gocql.Consistency(c))
}

// WithContext associates a context with the batch.
func (b *Batch) WithContext(ctx context.Context) cql.Batch {
	b.batch.WithContext(ctx)
	return b
}

// WithTimestamp sets the write timestamp for all statements.
func (b *Batch) WithTimestamp(ts int64) cql.Batch {
	b.batch.WithTimestamp(ts)
	return b
}

// Exec executes the batch.
func (b *Batch) Exec() error {
	return b.session.ExecuteBatch(b.batch)
}

// ExecContext executes the batch with context.
func (b *Batch) ExecContext(ctx context.Context) error {
	b.batch.WithContext(ctx)
	return b.session.ExecuteBatch(b.batch)
}

// IterContext executes the batch with context and returns an iterator.
// Note: gocql v1 doesn't have a direct IterContext for batches,
// so we execute and return a nil-safe iterator.
func (b *Batch) IterContext(ctx context.Context) cql.Iter {
	b.batch.WithContext(ctx)
	// v1 doesn't return an iterator from ExecuteBatch, return empty iter
	// This is mainly for API compatibility with v2
	_ = b.session.ExecuteBatch(b.batch)
	return &Iter{iter: nil}
}

// ExecCAS executes a batch lightweight transaction.
func (b *Batch) ExecCAS(dest ...any) (applied bool, iter cql.Iter, err error) {
	applied, gocqlIter, err := b.session.ExecuteBatchCAS(b.batch, dest...)
	if gocqlIter != nil {
		return applied, &Iter{iter: gocqlIter}, err
	}
	return applied, &Iter{iter: nil}, err
}

// ExecCASContext executes a batch lightweight transaction with context.
func (b *Batch) ExecCASContext(ctx context.Context, dest ...any) (applied bool, iter cql.Iter, err error) {
	b.batch.WithContext(ctx)
	return b.ExecCAS(dest...)
}

// MapExecCAS executes a batch lightweight transaction and scans into a map.
func (b *Batch) MapExecCAS(dest map[string]any) (applied bool, iter cql.Iter, err error) {
	applied, gocqlIter, err := b.session.MapExecuteBatchCAS(b.batch, dest)
	if gocqlIter != nil {
		return applied, &Iter{iter: gocqlIter}, err
	}
	return applied, &Iter{iter: nil}, err
}

// MapExecCASContext executes a batch lightweight transaction with context.
func (b *Batch) MapExecCASContext(ctx context.Context, dest map[string]any) (applied bool, iter cql.Iter, err error) {
	b.batch.WithContext(ctx)
	return b.MapExecCAS(dest)
}

// Statements returns all statements in the batch.
func (b *Batch) Statements() []cql.BatchEntry {
	return b.entries
}

// SerialConsistency sets the consistency level for the serial phase of CAS operations.
func (b *Batch) SerialConsistency(c cql.Consistency) cql.Batch {
	b.batch.SerialConsistency(gocql.SerialConsistency(c))
	return b
}

// Size returns the number of statements in the batch.
func (b *Batch) Size() int {
	return len(b.entries)
}

// Iter wraps a gocql v1 iterator.
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

	return i.iter.Close()
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

	return i.iter.SliceMap()
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

	return s.scanner.Scan(dest...)
}

func (s *scanner) Err() error {
	if s.scanner == nil {
		return nil
	}

	return s.scanner.Err()
}
