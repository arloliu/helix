package helix

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// These tests pin down the agreed behaviour of the read and failover path
// for a set of confirmed bugs.
// Each one is skipped until its fix lands so the suite stays green;
// the skip reason states, in plain language, what still misbehaves today.
// Remove the skip once the fix is in place.

var errReadProbeCluster = errors.New("read probe: cluster error")

// readProbeSession is a cql.Session whose reads are scripted per test and
// counted per session, so a test can assert which cluster served each read.
type readProbeSession struct {
	mu           sync.Mutex
	scan         func(ctx context.Context) error
	iterCloseErr error
	scans        atomic.Int64
	iters        atomic.Int64
}

var _ cql.Session = (*readProbeSession)(nil)

// readProbeQuery implements cql.Query and reports reads back to its session.
type readProbeQuery struct {
	session   *readProbeSession
	statement string
	values    []any
}

var _ cql.Query = (*readProbeQuery)(nil)

// readProbeIter is a mockIter whose Close error is scripted by the session.
type readProbeIter struct {
	mockIter
	closeErr error
}

func newReadProbeSession() *readProbeSession {
	return &readProbeSession{}
}

// newReadProbeClient builds a dual-cluster client over two probe sessions
// with the caller's options, closing the client when the test ends.
func newReadProbeClient(t *testing.T, sessionA, sessionB *readProbeSession, opts ...Option) *CQLClient {
	t.Helper()
	client, err := NewCQLClient(sessionA, sessionB, opts...)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	return client
}

// setScan scripts what every subsequent read on this session returns.
// A nil fn means the read succeeds immediately.
func (s *readProbeSession) setScan(fn func(ctx context.Context) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scan = fn
}

// setIterCloseErr scripts the error returned by Close on iterators this
// session hands out from now on.
func (s *readProbeSession) setIterCloseErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.iterCloseErr = err
}

func (s *readProbeSession) runScan(ctx context.Context) error {
	s.scans.Add(1)
	s.mu.Lock()
	fn := s.scan
	s.mu.Unlock()
	if fn == nil {
		return nil
	}

	return fn(ctx)
}

func (s *readProbeSession) newIter() cql.Iter {
	s.iters.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()

	return &readProbeIter{closeErr: s.iterCloseErr}
}

func (s *readProbeSession) Query(stmt string, values ...any) cql.Query {
	return &readProbeQuery{session: s, statement: stmt, values: values}
}

func (s *readProbeSession) Batch(_ cql.BatchType) cql.Batch {
	return &mockBatch{session: newMockSession()}
}

func (s *readProbeSession) Close() {}

func (q *readProbeQuery) Consistency(_ cql.Consistency) cql.Query       { return q }
func (q *readProbeQuery) SerialConsistency(_ cql.Consistency) cql.Query { return q }
func (q *readProbeQuery) PageSize(_ int) cql.Query                      { return q }
func (q *readProbeQuery) PageState(_ []byte) cql.Query                  { return q }
func (q *readProbeQuery) WithTimestamp(_ int64) cql.Query               { return q }
func (q *readProbeQuery) Statement() string                             { return q.statement }
func (q *readProbeQuery) Values() []any                                 { return q.values }
func (q *readProbeQuery) Release()                                      {}
func (q *readProbeQuery) Exec() error                                   { return nil }
func (q *readProbeQuery) ExecContext(_ context.Context) error           { return nil }

func (q *readProbeQuery) Scan(_ ...any) error {
	return q.session.runScan(context.Background())
}

func (q *readProbeQuery) ScanContext(ctx context.Context, _ ...any) error {
	return q.session.runScan(ctx)
}

func (q *readProbeQuery) MapScan(_ map[string]any) error {
	return q.session.runScan(context.Background())
}

func (q *readProbeQuery) MapScanContext(ctx context.Context, _ map[string]any) error {
	return q.session.runScan(ctx)
}

func (q *readProbeQuery) Iter() cql.Iter                         { return q.session.newIter() }
func (q *readProbeQuery) IterContext(_ context.Context) cql.Iter { return q.session.newIter() }

func (q *readProbeQuery) ScanCAS(_ ...any) (applied bool, err error) { return true, nil }
func (q *readProbeQuery) ScanCASContext(_ context.Context, _ ...any) (applied bool, err error) {
	return true, nil
}
func (q *readProbeQuery) MapScanCAS(_ map[string]any) (applied bool, err error) { return true, nil }
func (q *readProbeQuery) MapScanCASContext(_ context.Context, _ map[string]any) (applied bool, err error) {
	return true, nil
}

func (i *readProbeIter) Close() error { return i.closeErr }

// TestRead_CallerContextErrorIsNotClusterFailure asserts that a read whose
// caller-side context is already cancelled, or expires while the cluster is
// hanging, is reported to the caller as the context error and nothing else:
// the sticky preference does not move, no cluster records a failure, and the
// other cluster is never contacted with a dead context.
func TestRead_CallerContextErrorIsNotClusterFailure(t *testing.T) {
	type fixture struct {
		client *CQLClient
		sa, sb *readProbeSession
		sticky *policy.StickyRead
		cb     *policy.CircuitBreaker
	}
	setup := func(t *testing.T) fixture {
		t.Helper()
		f := fixture{
			sa:     newReadProbeSession(),
			sb:     newReadProbeSession(),
			sticky: policy.NewStickyRead(policy.WithPreferredCluster(ClusterA), policy.WithStickyReadCooldown(0)),
			cb:     policy.NewCircuitBreaker(policy.WithThreshold(1)),
		}
		f.client = newReadProbeClient(t, f.sa, f.sb,
			WithReadStrategy(f.sticky),
			WithFailoverPolicy(f.cb),
		)

		return f
	}

	// Real drivers hand the caller's context error back verbatim.
	returnCtxErr := func(ctx context.Context) error { return ctx.Err() }

	assertUntouched := func(t *testing.T, f fixture, err error) {
		t.Helper()
		var dual *types.DualClusterError
		require.False(t, errors.As(err, &dual), "caller context error must not be reported as a two-cluster failure, got %v", err)
		require.Equal(t, ClusterA, f.sticky.Preferred(), "sticky preference must not move on a caller context error")
		require.Zero(t, f.cb.Failures(ClusterA), "cluster A must not record a failure for the caller's context error")
		require.Zero(t, f.cb.Failures(ClusterB), "cluster B must not record a failure it was never given a live context for")
		require.Zero(t, f.sb.scans.Load(), "cluster B must never be contacted with a dead context")
	}

	t.Run("context cancelled before the read", func(t *testing.T) {
		f := setup(t)
		f.sa.setScan(returnCtxErr)
		f.sb.setScan(returnCtxErr)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		var v string
		err := f.client.Query("SELECT v FROM t WHERE k = ?", "k").ScanContext(ctx, &v)
		require.ErrorIs(t, err, context.Canceled)
		assertUntouched(t, f, err)
	})

	t.Run("deadline expires while cluster A hangs", func(t *testing.T) {
		f := setup(t)
		// Cluster A hangs until the caller's deadline fires; B would answer
		// instantly but must never be asked with an already-expired context.
		f.sa.setScan(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
		f.sb.setScan(returnCtxErr)

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
		defer cancel()

		var v string
		err := f.client.Query("SELECT v FROM t WHERE k = ?", "k").ScanContext(ctx, &v)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assertUntouched(t, f, err)
	})
}

// TestRead_OpenLatencyBreakerReroutesReads asserts that once a
// LatencyCircuitBreaker has opened on a cluster that answers correctly but
// too slowly, later reads are routed to the other cluster instead of being
// sent to the slow one again.
func TestRead_OpenLatencyBreakerReroutesReads(t *testing.T) {
	t.Skip("pending: an open LatencyCircuitBreaker never moves reads away from the slow cluster because routing only consults the breaker on the error path; the route-veto option that lets the breaker steer routing does not exist yet and must be added and enabled on this client when it lands")

	sa, sb := newReadProbeSession(), newReadProbeSession()
	sticky := policy.NewStickyRead(policy.WithPreferredCluster(ClusterA))
	// Any real read takes longer than one nanosecond, so every successful
	// read on A is a slow success and a single one opens the breaker.
	lcb := policy.NewLatencyCircuitBreaker(
		policy.WithLatencyAbsoluteMax(time.Nanosecond),
		policy.WithLatencyThreshold(1),
		policy.WithLatencyResetTimeout(time.Hour),
	)
	client := newReadProbeClient(t, sa, sb,
		WithReadStrategy(sticky),
		WithFailoverPolicy(lcb),
	)

	var v string
	require.NoError(t, client.Query("SELECT v FROM t").ScanContext(t.Context(), &v))
	require.Equal(t, int64(1), sa.scans.Load(), "the first read is served by the preferred cluster A")
	require.True(t, lcb.ShouldFailover(ClusterA, nil), "one slow success must open the breaker for cluster A")

	const followUps = 4
	for range followUps {
		require.NoError(t, client.Query("SELECT v FROM t").ScanContext(t.Context(), &v))
	}

	require.Equal(t, int64(1), sa.scans.Load(), "no further reads may be sent to cluster A while its latency breaker is open")
	require.Equal(t, int64(followUps), sb.scans.Load(), "reads issued after the breaker opened must be served by cluster B")
}

// TestRead_CircuitBreakerBelowThresholdRetriesOnHealthyCluster asserts that
// a CircuitBreaker that has not yet reached its threshold still lets a
// failed read be retried on the healthy cluster, so the caller sees a
// successful read rather than the failing cluster's error.
func TestRead_CircuitBreakerBelowThresholdRetriesOnHealthyCluster(t *testing.T) {
	t.Skip("pending: CircuitBreaker returns the first threshold-1 failures straight to the caller instead of retrying on the healthy cluster; the WithFailoverBelowThreshold(true) breaker option does not exist yet and must be added to the breaker built in this test when it lands")

	sa, sb := newReadProbeSession(), newReadProbeSession()
	sa.setScan(func(context.Context) error { return errReadProbeCluster })
	sticky := policy.NewStickyRead(policy.WithPreferredCluster(ClusterA), policy.WithStickyReadCooldown(0))
	cb := policy.NewCircuitBreaker(policy.WithThreshold(3))
	client := newReadProbeClient(t, sa, sb,
		WithReadStrategy(sticky),
		WithFailoverPolicy(cb),
	)

	var v string
	for i := 1; i <= 3; i++ {
		err := client.Query("SELECT v FROM t").ScanContext(t.Context(), &v)
		require.NoError(t, err, "read %d must succeed via cluster B while cluster A is failing", i)
		require.Equal(t, int64(i), sb.scans.Load(), "read %d must have been served by cluster B", i)
	}
}

// TestIter_PageStatePinsClusterAcrossPreferenceChange asserts that an
// iterator carrying a PageState is sent to the cluster that issued that
// cursor, even after the sticky preference has moved to the other cluster.
func TestIter_PageStatePinsClusterAcrossPreferenceChange(t *testing.T) {
	t.Skip("pending: Iter with a PageState re-resolves the cluster on every call and ships cluster A's paging cursor to cluster B once the sticky preference has moved")

	sa, sb := newReadProbeSession(), newReadProbeSession()
	sticky := policy.NewStickyRead(policy.WithPreferredCluster(ClusterA), policy.WithStickyReadCooldown(0))
	client := newReadProbeClient(t, sa, sb,
		WithReadStrategy(sticky),
		WithFailoverPolicy(policy.NewActiveFailover()),
	)

	// Page 1 goes to the preferred cluster A.
	page1 := client.Query("SELECT v FROM t").PageSize(10).IterContext(t.Context())
	require.NoError(t, page1.Close())
	require.Equal(t, int64(1), sa.iters.Load(), "page 1 must be served by cluster A")
	require.Zero(t, sb.iters.Load())

	// An unrelated read fails on A and moves the sticky preference to B.
	sa.setScan(func(context.Context) error { return errReadProbeCluster })
	var v string
	require.NoError(t, client.Query("SELECT v FROM other").ScanContext(t.Context(), &v), "the unrelated read fails over to B")
	require.Equal(t, ClusterB, sticky.Preferred(), "sticky preference must have moved to cluster B")

	// Page 2 carries A's cursor and must still go to A.
	cursor := []byte("paging-state-issued-by-cluster-a")
	page2 := client.Query("SELECT v FROM t").PageSize(10).PageState(cursor).IterContext(t.Context())
	require.NoError(t, page2.Close())
	require.Equal(t, int64(2), sa.iters.Load(), "page 2 carries cluster A's cursor and must be served by cluster A")
	require.Zero(t, sb.iters.Load(), "cluster A's paging cursor must never be sent to cluster B")
}

// TestIter_CloseErrorReachesFailoverPolicyAndReadStrategy asserts that an
// error returned by an iterator's Close is reported to the failover policy
// and the read strategy like any other read failure, and that a clean Close
// reports a success that resets the breaker's failure count.
func TestIter_CloseErrorReachesFailoverPolicyAndReadStrategy(t *testing.T) {
	sa, sb := newReadProbeSession(), newReadProbeSession()
	sticky := policy.NewStickyRead(policy.WithPreferredCluster(ClusterA), policy.WithStickyReadCooldown(0))
	cb := policy.NewCircuitBreaker(policy.WithThreshold(1))
	client := newReadProbeClient(t, sa, sb,
		WithReadStrategy(sticky),
		WithFailoverPolicy(cb),
	)

	// One iterator on A whose Close fails.
	sa.setIterCloseErr(errReadProbeCluster)
	it := client.Query("SELECT v FROM t").IterContext(t.Context())
	require.ErrorIs(t, it.Close(), errReadProbeCluster)
	require.Positive(t, cb.Failures(ClusterA), "iterator Close errors must be recorded as failures on cluster A")
	require.Equal(t, ClusterB, sticky.Preferred(), "iterator Close errors must move the sticky preference to cluster B")

	// One failing iterator on B moves the preference back to A, so the
	// next iterator is served by A again.
	sb.setIterCloseErr(errReadProbeCluster)
	it = client.Query("SELECT v FROM t").IterContext(t.Context())
	require.ErrorIs(t, it.Close(), errReadProbeCluster)
	require.Equal(t, int64(1), sb.iters.Load(), "the iterator after the preference moved must be served by cluster B")
	require.Equal(t, ClusterA, sticky.Preferred(), "a Close error on cluster B must move the sticky preference back to cluster A")

	// A clean Close on A reports a success and resets A's failure count.
	sa.setIterCloseErr(nil)
	it = client.Query("SELECT v FROM t").IterContext(t.Context())
	require.NoError(t, it.Close())
	require.Equal(t, int64(2), sa.iters.Load(), "the clean iterator must be served by cluster A")
	require.Zero(t, cb.Failures(ClusterA), "a clean iterator Close must reset cluster A's failure count")
}
