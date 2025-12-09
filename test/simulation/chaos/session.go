package chaos

import (
	"context"
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/types"
)

// SessionConfig holds the chaos configuration for a session.
type SessionConfig struct {
	LatencyFunc func() time.Duration // Return 0 for no delay
	ErrorFunc   func() error         // Return nil for no error
	DropRate    float64              // 0.0-1.0 probability to drop
}

// Session wraps a cql.Session to inject chaos.
type Session struct {
	wrapped cql.Session
	config  *atomic.Pointer[SessionConfig]
}

// Compile-time assertion that Session implements cql.Session.
var _ cql.Session = (*Session)(nil)

// NewSession creates a new chaos session wrapping the provided real session.
func NewSession(wrapped cql.Session) *Session {
	return &Session{
		wrapped: wrapped,
		config:  &atomic.Pointer[SessionConfig]{},
	}
}

// SetConfig updates the chaos configuration for the session.
func (s *Session) SetConfig(cfg SessionConfig) {
	s.config.Store(&cfg)
}

// Query creates a new query for the given statement.
func (s *Session) Query(stmt string, values ...any) cql.Query {
	return &Query{
		wrapped: s.wrapped.Query(stmt, values...),
		config:  s.config,
	}
}

// Batch creates a new batch of the given type.
func (s *Session) Batch(kind cql.BatchType) cql.Batch {
	return &Batch{
		wrapped: s.wrapped.Batch(kind),
		config:  s.config,
	}
}

// NewBatch creates a new batch of the given type.
// Deprecated: Use Batch() instead.
func (s *Session) NewBatch(kind cql.BatchType) cql.Batch {
	return s.Batch(kind)
}

// ExecuteBatch executes a batch.
// Deprecated: Use Batch().Exec() instead.
func (s *Session) ExecuteBatch(batch cql.Batch) error {
	return batch.Exec()
}

// ExecuteBatchCAS executes a batch lightweight transaction.
// Deprecated: Use Batch().ExecCAS() instead.
func (s *Session) ExecuteBatchCAS(batch cql.Batch, dest ...any) (applied bool, iter cql.Iter, err error) {
	return batch.ExecCAS(dest...)
}

// MapExecuteBatchCAS executes a batch lightweight transaction and scans into a map.
// Deprecated: Use Batch().MapExecCAS() instead.
func (s *Session) MapExecuteBatchCAS(batch cql.Batch, dest map[string]any) (applied bool, iter cql.Iter, err error) {
	return batch.MapExecCAS(dest)
}

// Close terminates the session.
func (s *Session) Close() {
	s.wrapped.Close()
}

// Query wraps a cql.Query to inject chaos.
type Query struct {
	wrapped cql.Query
	config  *atomic.Pointer[SessionConfig]
}

// Compile-time assertion that Query implements cql.Query.
var _ cql.Query = (*Query)(nil)

func (q *Query) injectChaos() error {
	cfg := q.config.Load()
	if cfg != nil {
		if cfg.LatencyFunc != nil {
			time.Sleep(cfg.LatencyFunc())
		}
		if cfg.DropRate > 0 {
			// Use crypto/rand for better randomness distribution
			n, _ := rand.Int(rand.Reader, big.NewInt(1000000))
			if float64(n.Int64())/1000000.0 < cfg.DropRate {
				return types.ErrWriteDropped
			}
		}
		if cfg.ErrorFunc != nil {
			if err := cfg.ErrorFunc(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (q *Query) WithContext(ctx context.Context) cql.Query {
	q.wrapped = q.wrapped.WithContext(ctx)
	return q
}

func (q *Query) Consistency(c cql.Consistency) cql.Query {
	q.wrapped = q.wrapped.Consistency(c)
	return q
}

func (q *Query) SetConsistency(c cql.Consistency) {
	q.wrapped.SetConsistency(c) //nolint:staticcheck // deprecated but required by interface
}

func (q *Query) PageSize(n int) cql.Query {
	q.wrapped = q.wrapped.PageSize(n)
	return q
}

func (q *Query) PageState(state []byte) cql.Query {
	q.wrapped = q.wrapped.PageState(state)
	return q
}

func (q *Query) WithTimestamp(ts int64) cql.Query {
	q.wrapped = q.wrapped.WithTimestamp(ts)
	return q
}

func (q *Query) Exec() error {
	if err := q.injectChaos(); err != nil {
		return err
	}

	return q.wrapped.Exec()
}

func (q *Query) ExecContext(ctx context.Context) error {
	if err := q.injectChaos(); err != nil {
		return err
	}

	return q.wrapped.ExecContext(ctx)
}

func (q *Query) Scan(dest ...any) error {
	if err := q.injectChaos(); err != nil {
		return err
	}

	return q.wrapped.Scan(dest...)
}

func (q *Query) ScanContext(ctx context.Context, dest ...any) error {
	if err := q.injectChaos(); err != nil {
		return err
	}

	return q.wrapped.ScanContext(ctx, dest...)
}

func (q *Query) Iter() cql.Iter {
	if err := q.injectChaos(); err != nil {
		return &ErrorIter{err: err}
	}

	return q.wrapped.Iter()
}

func (q *Query) IterContext(ctx context.Context) cql.Iter {
	if err := q.injectChaos(); err != nil {
		return &ErrorIter{err: err}
	}

	return q.wrapped.IterContext(ctx)
}

func (q *Query) MapScan(m map[string]any) error {
	if err := q.injectChaos(); err != nil {
		return err
	}

	return q.wrapped.MapScan(m)
}

func (q *Query) MapScanContext(ctx context.Context, m map[string]any) error {
	if err := q.injectChaos(); err != nil {
		return err
	}

	return q.wrapped.MapScanContext(ctx, m)
}

func (q *Query) ScanCAS(dest ...any) (applied bool, err error) {
	if err := q.injectChaos(); err != nil {
		return false, err
	}

	return q.wrapped.ScanCAS(dest...)
}

func (q *Query) ScanCASContext(ctx context.Context, dest ...any) (applied bool, err error) {
	if err := q.injectChaos(); err != nil {
		return false, err
	}

	return q.wrapped.ScanCASContext(ctx, dest...)
}

func (q *Query) MapScanCAS(dest map[string]any) (applied bool, err error) {
	if err := q.injectChaos(); err != nil {
		return false, err
	}

	return q.wrapped.MapScanCAS(dest)
}

func (q *Query) MapScanCASContext(ctx context.Context, dest map[string]any) (applied bool, err error) {
	if err := q.injectChaos(); err != nil {
		return false, err
	}

	return q.wrapped.MapScanCASContext(ctx, dest)
}

func (q *Query) SerialConsistency(c cql.Consistency) cql.Query {
	q.wrapped = q.wrapped.SerialConsistency(c)
	return q
}

func (q *Query) Statement() string {
	return q.wrapped.Statement()
}

func (q *Query) Values() []any {
	return q.wrapped.Values()
}

func (q *Query) Release() {
	q.wrapped.Release()
}

// Batch wraps a cql.Batch to inject chaos.
type Batch struct {
	wrapped cql.Batch
	config  *atomic.Pointer[SessionConfig]
}

// Compile-time assertion that Batch implements cql.Batch.
var _ cql.Batch = (*Batch)(nil)

func (b *Batch) injectChaos() error {
	cfg := b.config.Load()
	if cfg != nil {
		if cfg.LatencyFunc != nil {
			time.Sleep(cfg.LatencyFunc())
		}
		if cfg.DropRate > 0 {
			n, _ := rand.Int(rand.Reader, big.NewInt(1000000))
			if float64(n.Int64())/1000000.0 < cfg.DropRate {
				return types.ErrWriteDropped
			}
		}
		if cfg.ErrorFunc != nil {
			if err := cfg.ErrorFunc(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *Batch) Query(stmt string, args ...any) cql.Batch {
	b.wrapped = b.wrapped.Query(stmt, args...)
	return b
}

func (b *Batch) Consistency(c cql.Consistency) cql.Batch {
	b.wrapped = b.wrapped.Consistency(c)
	return b
}

func (b *Batch) SetConsistency(c cql.Consistency) {
	b.wrapped.SetConsistency(c) //nolint:staticcheck // deprecated but required by interface
}

func (b *Batch) WithContext(ctx context.Context) cql.Batch {
	b.wrapped = b.wrapped.WithContext(ctx)
	return b
}

func (b *Batch) WithTimestamp(ts int64) cql.Batch {
	b.wrapped = b.wrapped.WithTimestamp(ts)
	return b
}

func (b *Batch) Exec() error {
	if err := b.injectChaos(); err != nil {
		return err
	}

	return b.wrapped.Exec()
}

func (b *Batch) ExecContext(ctx context.Context) error {
	if err := b.injectChaos(); err != nil {
		return err
	}

	return b.wrapped.ExecContext(ctx)
}

func (b *Batch) IterContext(ctx context.Context) cql.Iter {
	if err := b.injectChaos(); err != nil {
		return &ErrorIter{err: err}
	}

	return b.wrapped.IterContext(ctx)
}

func (b *Batch) ExecCAS(dest ...any) (applied bool, iter cql.Iter, err error) {
	if err := b.injectChaos(); err != nil {
		return false, nil, err
	}

	return b.wrapped.ExecCAS(dest...)
}

func (b *Batch) ExecCASContext(ctx context.Context, dest ...any) (applied bool, iter cql.Iter, err error) {
	if err := b.injectChaos(); err != nil {
		return false, nil, err
	}

	return b.wrapped.ExecCASContext(ctx, dest...)
}

func (b *Batch) MapExecCAS(dest map[string]any) (applied bool, iter cql.Iter, err error) {
	if err := b.injectChaos(); err != nil {
		return false, nil, err
	}

	return b.wrapped.MapExecCAS(dest)
}

func (b *Batch) MapExecCASContext(ctx context.Context, dest map[string]any) (applied bool, iter cql.Iter, err error) {
	if err := b.injectChaos(); err != nil {
		return false, nil, err
	}

	return b.wrapped.MapExecCASContext(ctx, dest)
}

func (b *Batch) SerialConsistency(c cql.Consistency) cql.Batch {
	b.wrapped = b.wrapped.SerialConsistency(c)
	return b
}

func (b *Batch) Size() int {
	return b.wrapped.Size()
}

func (b *Batch) Statements() []cql.BatchEntry {
	return b.wrapped.Statements()
}

// ErrorIter is a cql.Iter implementation that always returns an error.
type ErrorIter struct {
	err error
}

func (i *ErrorIter) Scan(dest ...any) bool {
	return false
}

func (i *ErrorIter) Close() error {
	return i.err
}

func (i *ErrorIter) MapScan(m map[string]any) bool {
	return false
}

func (i *ErrorIter) SliceMap() ([]map[string]any, error) {
	return nil, i.err
}

func (i *ErrorIter) PageState() []byte {
	return nil
}

func (i *ErrorIter) NumRows() int {
	return 0
}

func (i *ErrorIter) Columns() []cql.ColumnInfo {
	return nil
}

func (i *ErrorIter) Scanner() cql.Scanner {
	return &ErrorScanner{err: i.err}
}

func (i *ErrorIter) Warnings() []string {
	return nil
}

// ErrorScanner is a cql.Scanner implementation that always returns an error.
type ErrorScanner struct {
	err error
}

func (s *ErrorScanner) Next() bool {
	return false
}

func (s *ErrorScanner) Scan(dest ...any) error {
	return s.err
}

func (s *ErrorScanner) Err() error {
	return s.err
}

// SetLatency sets a fixed latency for all operations.
func (s *Session) SetLatency(d time.Duration) {
	s.SetConfig(SessionConfig{
		LatencyFunc: func() time.Duration { return d },
	})
}

// SetErrorRate sets a probability of error for operations.
// Note: This overwrites any previous configuration.
func (s *Session) SetErrorRate(rate float64) {
	// We use DropRate for "complete failure" (1.0) which simulates network partition/timeout.
	// For partial errors, we could use ErrorFunc.
	// The scenarios use SetErrorRate(1.0) to kill a cluster.
	s.SetConfig(SessionConfig{
		DropRate: rate,
	})
}
