package helix

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// stalledReadSession is a cql.Session whose reads never answer: every read
// blocks until the context it was given ends. It stands in for a cluster
// that accepts connections but leaves the request hanging, which is what a
// driver produces once nothing bounds the request below Helix.
type stalledReadSession struct {
	reads chan struct{}

	mu           sync.Mutex
	lastDeadline time.Time
	hasDeadline  bool
}

func newStalledReadSession() *stalledReadSession {
	return &stalledReadSession{reads: make(chan struct{}, 8)}
}

// readCount reports how many reads reached the session.
func (s *stalledReadSession) readCount() int { return len(s.reads) }

// budget reports how much time the last iterator's context still had, and
// whether it carried a deadline at all.
func (s *stalledReadSession) budget() (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasDeadline {
		return 0, false
	}

	return time.Until(s.lastDeadline), true
}

func (s *stalledReadSession) Query(_ string, _ ...any) cql.Query {
	return &stalledReadQuery{session: s}
}
func (s *stalledReadSession) Batch(_ cql.BatchType) cql.Batch { return nil }
func (s *stalledReadSession) Close()                          {}

type stalledReadQuery struct {
	session *stalledReadSession
}

// block records the read and waits for the context to end.
func (q *stalledReadQuery) block(ctx context.Context) error {
	select {
	case q.session.reads <- struct{}{}:
	default:
	}
	<-ctx.Done()

	return ctx.Err()
}

func (q *stalledReadQuery) Consistency(_ cql.Consistency) cql.Query       { return q }
func (q *stalledReadQuery) SerialConsistency(_ cql.Consistency) cql.Query { return q }
func (q *stalledReadQuery) PageSize(_ int) cql.Query                      { return q }
func (q *stalledReadQuery) PageState(_ []byte) cql.Query                  { return q }
func (q *stalledReadQuery) WithTimestamp(_ int64) cql.Query               { return q }
func (q *stalledReadQuery) Statement() string                             { return "" }
func (q *stalledReadQuery) Values() []any                                 { return nil }
func (q *stalledReadQuery) Release()                                      {}
func (q *stalledReadQuery) Exec() error                                   { return nil }
func (q *stalledReadQuery) ExecContext(_ context.Context) error           { return nil }
func (q *stalledReadQuery) Scan(_ ...any) error                           { return nil }
func (q *stalledReadQuery) ScanContext(ctx context.Context, _ ...any) error {
	return q.block(ctx)
}
func (q *stalledReadQuery) Iter() cql.Iter { return &mockIter{} }
func (q *stalledReadQuery) IterContext(ctx context.Context) cql.Iter {
	if deadline, ok := ctx.Deadline(); ok {
		q.session.mu.Lock()
		q.session.lastDeadline = deadline
		q.session.hasDeadline = true
		q.session.mu.Unlock()
	}

	return &stalledReadIter{query: q, ctx: ctx}
}

// stalledReadIter is the iterator half of stalledReadSession: it never
// yields a row and reports the context's error on Close.
type stalledReadIter struct {
	query *stalledReadQuery
	ctx   context.Context
}

func (i *stalledReadIter) MapScan(_ map[string]any) bool {
	_ = i.query.block(i.ctx)

	return false
}
func (i *stalledReadIter) Scan(_ ...any) bool                  { return i.MapScan(nil) }
func (i *stalledReadIter) Close() error                        { return i.ctx.Err() }
func (i *stalledReadIter) SliceMap() ([]map[string]any, error) { return nil, i.ctx.Err() }
func (i *stalledReadIter) PageState() []byte                   { return nil }
func (i *stalledReadIter) NumRows() int                        { return 0 }
func (i *stalledReadIter) Columns() []cql.ColumnInfo           { return nil }
func (i *stalledReadIter) Scanner() cql.Scanner                { return &mockScanner{} }
func (i *stalledReadIter) Warnings() []string                  { return nil }
func (q *stalledReadQuery) MapScan(_ map[string]any) error     { return nil }
func (q *stalledReadQuery) MapScanContext(ctx context.Context, _ map[string]any) error {
	return q.block(ctx)
}
func (q *stalledReadQuery) ScanCAS(_ ...any) (bool, error) { return false, nil }
func (q *stalledReadQuery) ScanCASContext(_ context.Context, _ ...any) (bool, error) {
	return false, nil
}
func (q *stalledReadQuery) MapScanCAS(_ map[string]any) (bool, error) { return false, nil }
func (q *stalledReadQuery) MapScanCASContext(_ context.Context, _ map[string]any) (bool, error) {
	return false, nil
}

func TestClusterReadTimeout_ExpiredLegFailsOverToAlternative(t *testing.T) {
	sa, sb := newStalledReadSession(), newMockSession()
	sb.scanValues = []any{42}
	client, err := NewCQLClient(sa, sb, WithClusterReadTimeout(20*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	var got int
	start := time.Now()
	err = client.Query("SELECT v FROM t WHERE k = ?", 1).ScanContext(ctx, &got)
	require.NoError(t, err, "the alternative cluster answers within the caller's remaining budget")
	require.Less(t, time.Since(start), time.Second, "the read must leave A once its own deadline expires")
	require.Equal(t, 42, got)
	require.Equal(t, 1, sa.readCount(), "A was contacted once")
	require.Equal(t, int32(1), client.statsForCluster(ClusterA).consecutiveFailures.Load(),
		"a leg deadline is Helix's own, so its expiry is a health signal for A")
	require.Equal(t, int32(0), client.statsForCluster(ClusterB).consecutiveFailures.Load())
}

func TestClusterReadTimeout_ExpiredLegReportsClusterTimeout(t *testing.T) {
	sa, sb := newStalledReadSession(), newStalledReadSession()
	client, err := NewCQLClient(sa, sb, WithClusterReadTimeout(20*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	var got int
	err = client.Query("SELECT v FROM t WHERE k = ?", 1).ScanContext(ctx, &got)
	require.ErrorIs(t, err, types.ErrClusterTimeout, "both legs report Helix's own deadline")

	var dual *types.DualClusterError
	require.ErrorAs(t, err, &dual, "both clusters failed, so the caller sees both errors")
	require.Equal(t, 1, sa.readCount())
	require.Equal(t, 1, sb.readCount(), "the second leg still had budget to run")
}

func TestClusterReadTimeout_DefaultLeavesTheLegOnTheCallerContext(t *testing.T) {
	sa, sb := newStalledReadSession(), newMockSession()
	sb.scanValues = []any{42}
	client, err := NewCQLClient(sa, sb)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	t.Cleanup(cancel)

	var got int
	err = client.Query("SELECT v FROM t WHERE k = ?", 1).ScanContext(ctx, &got)
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"without a leg deadline the first cluster consumes the caller's whole budget")
	require.Empty(t, sb.queries, "no budget was left to contact the alternative cluster")
	require.Equal(t, int32(0), client.statsForCluster(ClusterA).consecutiveFailures.Load(),
		"the caller's own deadline is not a health signal")
}

func TestClusterReadTimeout_SingleClusterRunsOnTheCallerContext(t *testing.T) {
	sa := newStalledReadSession()
	client, err := NewCQLClient(sa, nil, WithClusterReadTimeout(20*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	t.Cleanup(cancel)

	var got int
	err = client.Query("SELECT v FROM t WHERE k = ?", 1).ScanContext(ctx, &got)
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"a single cluster has no alternative to preserve budget for")
	require.NotErrorIs(t, err, types.ErrClusterTimeout)
	require.Equal(t, int32(0), client.statsForCluster(ClusterA).consecutiveFailures.Load())
}

func TestClusterReadTimeout_NegativeIsRejected(t *testing.T) {
	_, err := NewCQLClient(newMockSession(), newMockSession(), WithClusterReadTimeout(-time.Second))
	require.Error(t, err)
	require.Contains(t, err.Error(), "WithClusterReadTimeout")
}

func TestClusterReadTimeout_BoundsTheFallbackReadProbe(t *testing.T) {
	sa, sb := newMockSession(), newStalledReadSession()
	sa.scanErr = types.ErrNotFound
	client, err := NewCQLClient(sa, sb,
		WithClusterReadTimeout(20*time.Millisecond),
		WithDefaultFallbackRead(true),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	var got int
	start := time.Now()
	err = client.Query("SELECT v FROM t WHERE k = ?", 1).ScanContext(ctx, &got)
	require.ErrorIs(t, err, types.ErrNotFound,
		"an unreachable alternative keeps the primary's not-found")
	require.Less(t, time.Since(start), time.Second, "the probe must end on its own deadline")
	require.Equal(t, 1, sb.readCount(), "the alternative was probed once")
	require.Equal(t, int32(1), client.statsForCluster(ClusterB).consecutiveFailures.Load(),
		"the probe's expiry is a health signal for the alternative")
}

// pacedRowSession answers reads a row at a time, pausing between rows and
// giving up as soon as its context ends. It stands in for a healthy but
// unhurried cluster, so a drain that outlives its leg context is visible.
type pacedRowSession struct {
	rows  int
	pause time.Duration
}

func (s *pacedRowSession) Query(_ string, _ ...any) cql.Query { return &pacedRowQuery{session: s} }
func (s *pacedRowSession) Batch(_ cql.BatchType) cql.Batch    { return nil }
func (s *pacedRowSession) Close()                             {}

type pacedRowQuery struct {
	stalledReadQuery
	session *pacedRowSession
}

func (q *pacedRowQuery) Consistency(_ cql.Consistency) cql.Query       { return q }
func (q *pacedRowQuery) SerialConsistency(_ cql.Consistency) cql.Query { return q }
func (q *pacedRowQuery) PageSize(_ int) cql.Query                      { return q }
func (q *pacedRowQuery) PageState(_ []byte) cql.Query                  { return q }
func (q *pacedRowQuery) WithTimestamp(_ int64) cql.Query               { return q }
func (q *pacedRowQuery) IterContext(ctx context.Context) cql.Iter {
	return &pacedRowIter{ctx: ctx, left: q.session.rows, pause: q.session.pause}
}

type pacedRowIter struct {
	ctx   context.Context
	left  int
	pause time.Duration
	err   error
}

func (i *pacedRowIter) MapScan(m map[string]any) bool {
	if i.left == 0 {
		return false
	}
	select {
	case <-time.After(i.pause):
	case <-i.ctx.Done():
		i.err = i.ctx.Err()

		return false
	}
	i.left--
	m["v"] = i.left

	return true
}
func (i *pacedRowIter) Scan(_ ...any) bool                  { return i.MapScan(map[string]any{}) }
func (i *pacedRowIter) Close() error                        { return i.err }
func (i *pacedRowIter) SliceMap() ([]map[string]any, error) { return nil, i.err }
func (i *pacedRowIter) PageState() []byte                   { return nil }
func (i *pacedRowIter) NumRows() int                        { return 0 }
func (i *pacedRowIter) Columns() []cql.ColumnInfo           { return nil }
func (i *pacedRowIter) Scanner() cql.Scanner                { return &mockScanner{} }
func (i *pacedRowIter) Warnings() []string                  { return nil }

func TestClusterReadTimeout_SliceReadKeepsItsLegContextForTheWholeDrain(t *testing.T) {
	sa := &pacedRowSession{rows: 3, pause: 20 * time.Millisecond}
	client, err := NewCQLClient(sa, newMockSession(), WithClusterReadTimeout(2*time.Second))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	rows, err := client.Query("SELECT v FROM t").SliceMapContext(t.Context())
	require.NoError(t, err, "the leg context must stay live until the drain finishes")
	require.Len(t, rows, 3, "every row must arrive; the leg is not cancelled row by row")
}

func TestClusterReadTimeout_BoundsASliceReadLeg(t *testing.T) {
	sa, sb := newStalledReadSession(), newStalledReadSession()
	client, err := NewCQLClient(sa, sb, WithClusterReadTimeout(20*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	start := time.Now()
	_, err = client.Query("SELECT v FROM t").SliceMapContext(ctx)
	require.ErrorIs(t, err, types.ErrClusterTimeout, "a slice leg reports Helix's own deadline")
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, 1, sa.readCount())
	require.Equal(t, 1, sb.readCount(), "the slice read still failed over")
}

func TestClusterReadTimeout_LeavesAPublicIteratorOnTheCallerContext(t *testing.T) {
	sa := newStalledReadSession()
	client, err := NewCQLClient(sa, newMockSession(), WithClusterReadTimeout(20*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	iter := client.Query("SELECT v FROM t").IterContext(ctx)
	t.Cleanup(func() { _ = iter.Close() })

	left, ok := sa.budget()
	require.True(t, ok, "the iterator ran on a context with a deadline")
	require.Greater(t, left, time.Second,
		"an iterator the caller drains itself keeps the caller's budget, not a leg deadline")
}

func TestClusterReadTimeout_BoundsASliceFallbackReadProbe(t *testing.T) {
	sa, sb := newMockSession(), newStalledReadSession()
	client, err := NewCQLClient(sa, sb,
		WithClusterReadTimeout(20*time.Millisecond),
		WithDefaultFallbackRead(true),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	start := time.Now()
	rows, err := client.Query("SELECT v FROM t").SliceMapContext(ctx)
	require.NoError(t, err, "an unreachable alternative keeps the primary's empty result")
	require.Nil(t, rows)
	require.Less(t, time.Since(start), time.Second, "the slice probe must end on its own deadline")
	require.Equal(t, 1, sb.readCount(), "the alternative was probed once")
	require.Equal(t, int32(1), client.statsForCluster(ClusterB).consecutiveFailures.Load(),
		"the probe's expiry is a health signal for the alternative")
}
