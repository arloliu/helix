package chaos

import (
	"context"
	"crypto/rand"
	"errors"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/types"
)

// ErrChaosSimulated is returned by SetErrorRate to simulate real cluster failures.
// Unlike types.ErrWriteDropped (which Helix treats as an operational state),
// this error is treated as a genuine write failure by AdaptiveDualWrite and
// the CQL client's DualClusterError path.
var ErrChaosSimulated = errors.New("chaos: simulated failure")

// SessionConfig holds the chaos configuration for a session.
type SessionConfig struct {
	LatencyFunc func() time.Duration // Return 0 for no delay
	ErrorFunc   func() error         // Return nil for no error
	DropRate    float64              // 0.0-1.0 probability to drop
}

// Session wraps a cql.Session to inject chaos.
type Session struct {
	wrapped   cql.Session
	config    *atomic.Pointer[SessionConfig]
	execCount atomic.Int64
	scanCount atomic.Int64
	dropCount atomic.Int64
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
		session: s,
	}
}

// Batch creates a new batch of the given type.
func (s *Session) Batch(kind cql.BatchType) cql.Batch {
	return &Batch{
		wrapped: s.wrapped.Batch(kind),
		config:  s.config,
		session: s,
	}
}

// Close is a no-op for the chaos session. The underlying session lifecycle
// is managed externally (e.g., by testutil.CQLCluster). This allows the
// chaos session to be reused across multiple helix client instances in
// simulation tests without prematurely closing the database connection.
func (s *Session) Close() {}

// Query wraps a cql.Query to inject chaos.
type Query struct {
	wrapped cql.Query
	config  *atomic.Pointer[SessionConfig]
	session *Session
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
				q.session.dropCount.Add(1)
				return types.ErrWriteDropped
			}
		}
		if cfg.ErrorFunc != nil {
			if err := cfg.ErrorFunc(); err != nil {
				q.session.dropCount.Add(1)
				return err
			}
		}
	}

	return nil
}

func (q *Query) Consistency(c cql.Consistency) cql.Query {
	q.wrapped = q.wrapped.Consistency(c)
	return q
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

	err := q.wrapped.Exec()
	if err == nil {
		q.session.execCount.Add(1)
	}

	return err
}

func (q *Query) ExecContext(ctx context.Context) error {
	if err := q.injectChaos(); err != nil {
		return err
	}

	err := q.wrapped.ExecContext(ctx)
	if err == nil {
		q.session.execCount.Add(1)
	}

	return err
}

func (q *Query) Scan(dest ...any) error {
	if err := q.injectChaos(); err != nil {
		return err
	}

	err := q.wrapped.Scan(dest...)
	if err == nil {
		q.session.scanCount.Add(1)
	}

	return err
}

func (q *Query) ScanContext(ctx context.Context, dest ...any) error {
	if err := q.injectChaos(); err != nil {
		return err
	}

	err := q.wrapped.ScanContext(ctx, dest...)
	if err == nil {
		q.session.scanCount.Add(1)
	}

	return err
}

func (q *Query) Iter() cql.Iter {
	if err := q.injectChaos(); err != nil {
		return &ErrorIter{err: err}
	}

	q.session.scanCount.Add(1)

	return q.wrapped.Iter()
}

func (q *Query) IterContext(ctx context.Context) cql.Iter {
	if err := q.injectChaos(); err != nil {
		return &ErrorIter{err: err}
	}

	q.session.scanCount.Add(1)

	return q.wrapped.IterContext(ctx)
}

func (q *Query) MapScan(m map[string]any) error {
	if err := q.injectChaos(); err != nil {
		return err
	}

	err := q.wrapped.MapScan(m)
	if err == nil {
		q.session.scanCount.Add(1)
	}

	return err
}

func (q *Query) MapScanContext(ctx context.Context, m map[string]any) error {
	if err := q.injectChaos(); err != nil {
		return err
	}

	err := q.wrapped.MapScanContext(ctx, m)
	if err == nil {
		q.session.scanCount.Add(1)
	}

	return err
}

func (q *Query) ScanCAS(dest ...any) (applied bool, err error) {
	if err := q.injectChaos(); err != nil {
		return false, err
	}

	applied, err = q.wrapped.ScanCAS(dest...)
	if err == nil {
		q.session.scanCount.Add(1)
	}

	return applied, err
}

func (q *Query) ScanCASContext(ctx context.Context, dest ...any) (applied bool, err error) {
	if err := q.injectChaos(); err != nil {
		return false, err
	}

	applied, err = q.wrapped.ScanCASContext(ctx, dest...)
	if err == nil {
		q.session.scanCount.Add(1)
	}

	return applied, err
}

func (q *Query) MapScanCAS(dest map[string]any) (applied bool, err error) {
	if err := q.injectChaos(); err != nil {
		return false, err
	}

	applied, err = q.wrapped.MapScanCAS(dest)
	if err == nil {
		q.session.scanCount.Add(1)
	}

	return applied, err
}

func (q *Query) MapScanCASContext(ctx context.Context, dest map[string]any) (applied bool, err error) {
	if err := q.injectChaos(); err != nil {
		return false, err
	}

	applied, err = q.wrapped.MapScanCASContext(ctx, dest)
	if err == nil {
		q.session.scanCount.Add(1)
	}

	return applied, err
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
	session *Session
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
				b.session.dropCount.Add(1)
				return types.ErrWriteDropped
			}
		}
		if cfg.ErrorFunc != nil {
			if err := cfg.ErrorFunc(); err != nil {
				b.session.dropCount.Add(1)
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

func (b *Batch) WithTimestamp(ts int64) cql.Batch {
	b.wrapped = b.wrapped.WithTimestamp(ts)
	return b
}

func (b *Batch) Exec() error {
	if err := b.injectChaos(); err != nil {
		return err
	}

	err := b.wrapped.Exec()
	if err == nil {
		b.session.execCount.Add(1)
	}

	return err
}

func (b *Batch) ExecContext(ctx context.Context) error {
	if err := b.injectChaos(); err != nil {
		return err
	}

	err := b.wrapped.ExecContext(ctx)
	if err == nil {
		b.session.execCount.Add(1)
	}

	return err
}

func (b *Batch) IterContext(ctx context.Context) cql.Iter {
	if err := b.injectChaos(); err != nil {
		return &ErrorIter{err: err}
	}

	b.session.scanCount.Add(1)

	return b.wrapped.IterContext(ctx)
}

func (b *Batch) ExecCAS(dest ...any) (applied bool, iter cql.Iter, err error) {
	if err := b.injectChaos(); err != nil {
		return false, nil, err
	}

	applied, iter, err = b.wrapped.ExecCAS(dest...)
	if err == nil {
		b.session.execCount.Add(1)
	}

	return applied, iter, err
}

func (b *Batch) ExecCASContext(ctx context.Context, dest ...any) (applied bool, iter cql.Iter, err error) {
	if err := b.injectChaos(); err != nil {
		return false, nil, err
	}

	applied, iter, err = b.wrapped.ExecCASContext(ctx, dest...)
	if err == nil {
		b.session.execCount.Add(1)
	}

	return applied, iter, err
}

func (b *Batch) MapExecCAS(dest map[string]any) (applied bool, iter cql.Iter, err error) {
	if err := b.injectChaos(); err != nil {
		return false, nil, err
	}

	applied, iter, err = b.wrapped.MapExecCAS(dest)
	if err == nil {
		b.session.execCount.Add(1)
	}

	return applied, iter, err
}

func (b *Batch) MapExecCASContext(ctx context.Context, dest map[string]any) (applied bool, iter cql.Iter, err error) {
	if err := b.injectChaos(); err != nil {
		return false, nil, err
	}

	applied, iter, err = b.wrapped.MapExecCASContext(ctx, dest)
	if err == nil {
		b.session.execCount.Add(1)
	}

	return applied, iter, err
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

// Counters returns the cumulative operation counts since creation or last ResetCounters call.
// exec is the number of successful Exec/ExecCAS calls, scan is the number of successful
// Scan/Iter/MapScan calls, and drop is the number of operations rejected by chaos injection.
func (s *Session) Counters() (exec, scan, drop int64) {
	return s.execCount.Load(), s.scanCount.Load(), s.dropCount.Load()
}

// ResetCounters zeroes all operation counters.
func (s *Session) ResetCounters() {
	s.execCount.Store(0)
	s.scanCount.Store(0)
	s.dropCount.Store(0)
}

// SetLatency sets a fixed latency for all operations.
func (s *Session) SetLatency(d time.Duration) {
	s.SetConfig(SessionConfig{
		LatencyFunc: func() time.Duration { return d },
	})
}

// SetErrorRate sets a probability of error for operations.
// The injected error is ErrChaosSimulated (a real error), not types.ErrWriteDropped.
// This ensures AdaptiveDualWrite treats chaos failures as genuine write errors
// and accumulates strikes for degradation detection.
func (s *Session) SetErrorRate(rate float64) {
	s.SetConfig(SessionConfig{
		ErrorFunc: func() error {
			n, _ := rand.Int(rand.Reader, big.NewInt(1000000))
			if float64(n.Int64())/1000000.0 < rate {
				return ErrChaosSimulated
			}
			return nil
		},
	})
}
