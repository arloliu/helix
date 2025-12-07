package testutil

import (
	"context"
	"time"

	"github.com/arloliu/helix/adapter/cql"
)

// SlowCQLSession wraps a CQL session and adds artificial delay to all operations.
// This is useful for testing latency-aware components like AdaptiveDualWrite
// and LatencyCircuitBreaker.
type SlowCQLSession struct {
	Session cql.Session
	Delay   time.Duration
}

// Compile-time assertion that SlowCQLSession implements cql.Session.
var _ cql.Session = (*SlowCQLSession)(nil)

// Query returns a query that adds delay before execution.
func (s *SlowCQLSession) Query(stmt string, values ...any) cql.Query {
	return &SlowCQLQuery{
		Query: s.Session.Query(stmt, values...),
		Delay: s.Delay,
	}
}

// Batch returns a batch from the underlying session.
func (s *SlowCQLSession) Batch(kind cql.BatchType) cql.Batch {
	return s.Session.Batch(kind)
}

// NewBatch creates a new Batch.
func (s *SlowCQLSession) NewBatch(kind cql.BatchType) cql.Batch {
	return s.Batch(kind)
}

// ExecuteBatch executes a batch.
func (s *SlowCQLSession) ExecuteBatch(batch cql.Batch) error {
	return batch.Exec()
}

// ExecuteBatchCAS executes a batch with CAS semantics.
func (s *SlowCQLSession) ExecuteBatchCAS(batch cql.Batch, dest ...any) (applied bool, iter cql.Iter, err error) {
	return batch.ExecCAS(dest...)
}

// MapExecuteBatchCAS executes a batch CAS operation with map result.
func (s *SlowCQLSession) MapExecuteBatchCAS(batch cql.Batch, dest map[string]any) (applied bool, iter cql.Iter, err error) {
	return batch.MapExecCAS(dest)
}

// Close is intentionally a no-op because SlowCQLSession wraps shared sessions
// that are managed by TestMain's teardown. Calling Close() on the wrapper
// should not close the underlying shared session.
func (s *SlowCQLSession) Close() {
	// Intentionally empty - shared sessions are cleaned up by TestMain.
}

// SlowCQLQuery wraps a CQL query and adds delay to Exec and Scan operations.
type SlowCQLQuery struct {
	Query cql.Query
	Delay time.Duration
}

// Compile-time assertion that SlowCQLQuery implements cql.Query.
var _ cql.Query = (*SlowCQLQuery)(nil)

// WithContext sets the context for the query.
func (q *SlowCQLQuery) WithContext(ctx context.Context) cql.Query {
	q.Query = q.Query.WithContext(ctx)
	return q
}

// Consistency sets the consistency level.
func (q *SlowCQLQuery) Consistency(c cql.Consistency) cql.Query {
	q.Query = q.Query.Consistency(c)
	return q
}

// SetConsistency sets the consistency level (deprecated API).
func (q *SlowCQLQuery) SetConsistency(c cql.Consistency) {
	q.Consistency(c)
}

// SerialConsistency sets the serial consistency level.
func (q *SlowCQLQuery) SerialConsistency(c cql.Consistency) cql.Query {
	q.Query = q.Query.SerialConsistency(c)
	return q
}

// PageSize sets the page size.
func (q *SlowCQLQuery) PageSize(n int) cql.Query {
	q.Query = q.Query.PageSize(n)
	return q
}

// PageState sets the page state for pagination.
func (q *SlowCQLQuery) PageState(state []byte) cql.Query {
	q.Query = q.Query.PageState(state)
	return q
}

// WithTimestamp sets the timestamp for the query.
func (q *SlowCQLQuery) WithTimestamp(ts int64) cql.Query {
	q.Query = q.Query.WithTimestamp(ts)
	return q
}

// Exec executes the query with artificial delay.
func (q *SlowCQLQuery) Exec() error {
	time.Sleep(q.Delay)
	return q.Query.Exec()
}

// ExecContext executes the query with context and artificial delay.
func (q *SlowCQLQuery) ExecContext(ctx context.Context) error {
	time.Sleep(q.Delay)
	return q.Query.ExecContext(ctx)
}

// Scan executes the query and scans the result with artificial delay.
func (q *SlowCQLQuery) Scan(dest ...any) error {
	time.Sleep(q.Delay)
	return q.Query.Scan(dest...)
}

// ScanContext executes and scans a single row with context.
func (q *SlowCQLQuery) ScanContext(ctx context.Context, dest ...any) error {
	time.Sleep(q.Delay)
	return q.Query.ScanContext(ctx, dest...)
}

// Iter returns an iterator for the query results with artificial delay.
func (q *SlowCQLQuery) Iter() cql.Iter {
	time.Sleep(q.Delay)
	return q.Query.Iter()
}

// IterContext returns an iterator for the query results with context.
func (q *SlowCQLQuery) IterContext(ctx context.Context) cql.Iter {
	time.Sleep(q.Delay)
	return q.Query.IterContext(ctx)
}

// MapScan executes the query and scans into a map with artificial delay.
func (q *SlowCQLQuery) MapScan(m map[string]any) error {
	time.Sleep(q.Delay)
	return q.Query.MapScan(m)
}

// MapScanContext executes the query and scans into a map with context.
func (q *SlowCQLQuery) MapScanContext(ctx context.Context, m map[string]any) error {
	time.Sleep(q.Delay)
	return q.Query.MapScanContext(ctx, m)
}

// ScanCAS executes a lightweight transaction and scans the result.
func (q *SlowCQLQuery) ScanCAS(dest ...any) (applied bool, err error) {
	time.Sleep(q.Delay)
	return q.Query.ScanCAS(dest...)
}

// ScanCASContext executes a lightweight transaction with context.
func (q *SlowCQLQuery) ScanCASContext(ctx context.Context, dest ...any) (applied bool, err error) {
	time.Sleep(q.Delay)
	return q.Query.ScanCASContext(ctx, dest...)
}

// MapScanCAS executes a lightweight transaction and scans into a map.
func (q *SlowCQLQuery) MapScanCAS(dest map[string]any) (applied bool, err error) {
	time.Sleep(q.Delay)
	return q.Query.MapScanCAS(dest)
}

// MapScanCASContext executes a lightweight transaction with context.
func (q *SlowCQLQuery) MapScanCASContext(ctx context.Context, dest map[string]any) (applied bool, err error) {
	time.Sleep(q.Delay)
	return q.Query.MapScanCASContext(ctx, dest)
}

// Statement returns the CQL statement.
func (q *SlowCQLQuery) Statement() string {
	return q.Query.Statement()
}

// Values returns the bound values.
func (q *SlowCQLQuery) Values() []any {
	return q.Query.Values()
}

// Release returns the query to a pool (if applicable).
func (q *SlowCQLQuery) Release() {
	q.Query.Release()
}
