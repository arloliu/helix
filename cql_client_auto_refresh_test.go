package helix_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
)

// Tests for the auto-refresh detector.
//
// The detector runs as a background goroutine (autoRefreshLoop) that
// ticks every CheckInterval and calls maybeAutoRefresh per cluster.
// To make tests time-deterministic without depending on wall clock or
// goroutine scheduling, the test path:
//
//   1. Injects a manualClock so c.nowFunc() returns a value the test
//      controls.
//   2. Sets CheckInterval long enough that the production goroutine
//      effectively never fires during the test (10 minutes).
//   3. Drives maybeAutoRefresh DIRECTLY from the test body, skipping
//      the goroutine. This isolates "the detection logic" from "the
//      goroutine plumbing".
//
// One smoke test (TestAutoRefresh_GoroutineFiresOnTick) exercises the
// real-goroutine path with a tight CheckInterval to confirm the
// plumbing works end-to-end.

// ----- helpers -------------------------------------------------------------

// manualClock returns whatever the test sets via Advance / SetTo.
// All accesses go through atomic operations so the manual clock is safe
// to share between the test goroutine and any background goroutine
// the client might run.
type manualClock struct {
	now atomic.Int64
}

func newManualClock(t time.Time) *manualClock {
	c := &manualClock{}
	c.now.Store(t.UnixNano())
	return c
}

func (c *manualClock) NowFunc() helix.NowProvider {
	return func() int64 { return c.now.Load() }
}

func (c *manualClock) Advance(d time.Duration) {
	c.now.Add(int64(d))
}

func (c *manualClock) SetTo(t time.Time) {
	c.now.Store(t.UnixNano())
}

// failingMock toggles between success and failure based on a fail
// atomic. Its Query / Batch return queries/batches whose Exec /
// ExecContext / Scan respect that fail flag.
type failingMock struct {
	fail   atomic.Bool
	closed atomic.Bool
	mu     sync.Mutex
	count  int
}

func newFailingMock() *failingMock { return &failingMock{} }

func (m *failingMock) Query(stmt string, _ ...any) cql.Query {
	m.mu.Lock()
	m.count++
	m.mu.Unlock()
	return &failingQuery{stmt: stmt, fail: &m.fail}
}

func (m *failingMock) Batch(_ cql.BatchType) cql.Batch {
	return &failingBatch{fail: &m.fail}
}

func (m *failingMock) Close() { m.closed.Store(true) }

type failingQuery struct {
	stmt string
	fail *atomic.Bool
}

func (q *failingQuery) Consistency(cql.Consistency) cql.Query       { return q }
func (q *failingQuery) SerialConsistency(cql.Consistency) cql.Query { return q }
func (q *failingQuery) PageSize(int) cql.Query                      { return q }
func (q *failingQuery) PageState([]byte) cql.Query                  { return q }
func (q *failingQuery) WithTimestamp(int64) cql.Query               { return q }
func (q *failingQuery) Statement() string                           { return q.stmt }
func (q *failingQuery) Values() []any                               { return nil }
func (q *failingQuery) Release()                                    {}
func (q *failingQuery) errOrNil() error {
	if q.fail.Load() {
		return errors.New("simulated cluster failure")
	}
	return nil
}

func (q *failingQuery) Exec() error                                          { return q.errOrNil() }
func (q *failingQuery) ExecContext(context.Context) error                    { return q.errOrNil() }
func (q *failingQuery) Scan(...any) error                                    { return q.errOrNil() }
func (q *failingQuery) ScanContext(context.Context, ...any) error            { return q.errOrNil() }
func (q *failingQuery) MapScan(map[string]any) error                         { return q.errOrNil() }
func (q *failingQuery) MapScanContext(context.Context, map[string]any) error { return q.errOrNil() }
func (q *failingQuery) ScanCAS(...any) (bool, error)                         { return true, q.errOrNil() }
func (q *failingQuery) ScanCASContext(context.Context, ...any) (bool, error) {
	return true, q.errOrNil()
}
func (q *failingQuery) MapScanCAS(map[string]any) (bool, error) { return true, q.errOrNil() }
func (q *failingQuery) MapScanCASContext(context.Context, map[string]any) (bool, error) {
	return true, q.errOrNil()
}
func (q *failingQuery) Iter() cql.Iter                       { return &failingIter{err: q.errOrNil()} }
func (q *failingQuery) IterContext(context.Context) cql.Iter { return &failingIter{err: q.errOrNil()} }

type failingBatch struct {
	fail *atomic.Bool
	stmt string
}

func (b *failingBatch) Query(stmt string, _ ...any) cql.Batch       { b.stmt = stmt; return b }
func (b *failingBatch) Consistency(cql.Consistency) cql.Batch       { return b }
func (b *failingBatch) SerialConsistency(cql.Consistency) cql.Batch { return b }
func (b *failingBatch) WithTimestamp(int64) cql.Batch               { return b }
func (b *failingBatch) errOrNil() error {
	if b.fail.Load() {
		return errors.New("simulated cluster failure")
	}
	return nil
}
func (b *failingBatch) Exec() error                            { return b.errOrNil() }
func (b *failingBatch) ExecContext(context.Context) error      { return b.errOrNil() }
func (b *failingBatch) IterContext(context.Context) cql.Iter   { return &failingIter{err: b.errOrNil()} }
func (b *failingBatch) ExecCAS(...any) (bool, cql.Iter, error) { return true, nil, b.errOrNil() }
func (b *failingBatch) ExecCASContext(context.Context, ...any) (bool, cql.Iter, error) {
	return true, nil, b.errOrNil()
}
func (b *failingBatch) MapExecCAS(map[string]any) (bool, cql.Iter, error) {
	return true, nil, b.errOrNil()
}
func (b *failingBatch) MapExecCASContext(context.Context, map[string]any) (bool, cql.Iter, error) {
	return true, nil, b.errOrNil()
}
func (b *failingBatch) Size() int                    { return 0 }
func (b *failingBatch) Statements() []cql.BatchEntry { return nil }

type failingIter struct{ err error }

func (i *failingIter) Scan(...any) bool                    { return false }
func (i *failingIter) Close() error                        { return i.err }
func (i *failingIter) MapScan(map[string]any) bool         { return false }
func (i *failingIter) SliceMap() ([]map[string]any, error) { return nil, i.err }
func (i *failingIter) PageState() []byte                   { return nil }
func (i *failingIter) NumRows() int                        { return 0 }
func (i *failingIter) Columns() []cql.ColumnInfo           { return nil }
func (i *failingIter) Scanner() cql.Scanner                { return nil }
func (i *failingIter) Warnings() []string                  { return nil }

// fastAutoRefreshOpts returns options producing a millisecond-scale
// configuration with a 10-minute CheckInterval. Tests advance() the
// manual clock and call maybeAutoRefresh directly to drive the detector
// deterministically, so the production goroutine effectively never
// fires during these tests.
func fastAutoRefreshOpts() []helix.AutoRefreshOption {
	return []helix.AutoRefreshOption{
		helix.WithAutoRefreshFailureThreshold(3),
		helix.WithAutoRefreshSustainedFailureWindow(20 * time.Millisecond),
		helix.WithAutoRefreshMinRetryInterval(10 * time.Millisecond),
		helix.WithAutoRefreshCheckInterval(10 * time.Minute),
		helix.WithAutoRefreshRefreshTimeout(1 * time.Second),
	}
}

// driveOp issues client.Query(...).Exec() N times. The actual cluster
// outcome is determined by whether the underlying mock has fail=true.
func driveOps(t *testing.T, c *helix.CQLClient, n int) {
	t.Helper()
	for i := range n {
		// Explicitly use ExecContext to take the documented exec path.
		_ = c.Query("INSERT INTO t (k,v) VALUES (?, ?)", i, "v").ExecContext(context.Background())
	}
}

// ----- Cases ---------------------------------------------------------------

func TestAutoRefresh_TriggersAfterSustainedFailures(t *testing.T) {
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	mockA := newFailingMock()
	mockB := newFailingMock() // healthy throughout
	newMockA := newFailingMock()

	mc := testutil.NewTestMetricsCollector()
	var refresherCalls atomic.Int32
	refresher := func(_ context.Context, _ helix.ClusterID, _ error) (cql.Session, error) {
		refresherCalls.Add(1)
		return newMockA, nil
	}

	client, err := helix.NewCQLClient(mockA, mockB,
		helix.WithSessionRefresher(refresher),
		helix.WithAutoRefresh(fastAutoRefreshOpts()...),
		helix.WithMetrics(mc),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	helix.SetClientNowFuncForTest(client, clock.NowFunc())

	// Drive failures through cluster A. SyncDualWrite (default) returns
	// the err from A; B succeeds so the dual-write returns partial-success.
	mockA.fail.Store(true)
	driveOps(t, client, 5)

	// Time passes — past the SustainedFailureWindow (20ms).
	clock.Advance(50 * time.Millisecond)

	// Drive maybeAutoRefresh directly. consecutiveFailures>=3 ✓,
	// time-since-success>=20ms ✓, time-since-refresh>=10ms ✓.
	helix.MaybeAutoRefreshForTest(client, helix.ClusterA)

	assert.EqualValues(t, 1, refresherCalls.Load(),
		"refresher must be invoked exactly once")
	assert.Equal(t, int64(1), mc.GetSessionRefreshAttempts(helix.ClusterA))
	assert.Equal(t, int64(1), mc.GetSessionRefreshSuccesses(helix.ClusterA))
	assert.Equal(t, int64(0), mc.GetSessionRefreshErrors(helix.ClusterA))
}

func TestAutoRefresh_ThrottledByMinRetryInterval(t *testing.T) {
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	mockA := newFailingMock()
	mockB := newFailingMock()

	mc := testutil.NewTestMetricsCollector()
	rebuildErr := errors.New("rebuild failed every time")
	refresher := func(_ context.Context, _ helix.ClusterID, _ error) (cql.Session, error) {
		return nil, rebuildErr
	}

	client, err := helix.NewCQLClient(mockA, mockB,
		helix.WithSessionRefresher(refresher),
		helix.WithAutoRefresh(fastAutoRefreshOpts()...),
		helix.WithMetrics(mc),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	helix.SetClientNowFuncForTest(client, clock.NowFunc())

	mockA.fail.Store(true)
	driveOps(t, client, 5)
	clock.Advance(50 * time.Millisecond)

	// First call: predicates hold → attempt fires.
	helix.MaybeAutoRefreshForTest(client, helix.ClusterA)
	require.Equal(t, int64(1), mc.GetSessionRefreshAttempts(helix.ClusterA))

	// Subsequent calls within MinRetryInterval (10ms): throttle.
	clock.Advance(5 * time.Millisecond)
	helix.MaybeAutoRefreshForTest(client, helix.ClusterA)
	helix.MaybeAutoRefreshForTest(client, helix.ClusterA)
	helix.MaybeAutoRefreshForTest(client, helix.ClusterA)
	assert.Equal(t, int64(1), mc.GetSessionRefreshAttempts(helix.ClusterA),
		"throttle must prevent multiple attempts within MinRetryInterval")

	// Past the throttle window → another attempt fires.
	clock.Advance(15 * time.Millisecond)
	helix.MaybeAutoRefreshForTest(client, helix.ClusterA)
	assert.Equal(t, int64(2), mc.GetSessionRefreshAttempts(helix.ClusterA))
}

func TestAutoRefresh_DoesNotFireOnSporadicFailures(t *testing.T) {
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	mockA := newFailingMock()
	mockB := newFailingMock()

	mc := testutil.NewTestMetricsCollector()
	var invoked atomic.Bool
	refresher := func(_ context.Context, _ helix.ClusterID, _ error) (cql.Session, error) {
		invoked.Store(true)
		return newFailingMock(), nil
	}

	client, err := helix.NewCQLClient(mockA, mockB,
		helix.WithSessionRefresher(refresher),
		helix.WithAutoRefresh(fastAutoRefreshOpts()...),
		helix.WithMetrics(mc),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	helix.SetClientNowFuncForTest(client, clock.NowFunc())

	// Alternate: 3 failures, 1 success, repeat. Each success resets
	// consecutiveFailures and updates lastSuccess so the predicates
	// never both hold.
	for range 5 {
		mockA.fail.Store(true)
		driveOps(t, client, 3)
		mockA.fail.Store(false)
		driveOps(t, client, 1)
		clock.Advance(10 * time.Millisecond)
		helix.MaybeAutoRefreshForTest(client, helix.ClusterA)
	}

	assert.Equal(t, int64(0), mc.GetSessionRefreshAttempts(helix.ClusterA))
	assert.False(t, invoked.Load(),
		"refresher must not be invoked when failures are sporadic")
}

func TestAutoRefresh_NoRefresherConfigured(t *testing.T) {
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	mockA := newFailingMock()
	mockB := newFailingMock()

	mc := testutil.NewTestMetricsCollector()

	// AutoRefresh enabled, NO SessionRefresher.
	client, err := helix.NewCQLClient(mockA, mockB,
		helix.WithAutoRefresh(fastAutoRefreshOpts()...),
		helix.WithMetrics(mc),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	helix.SetClientNowFuncForTest(client, clock.NowFunc())

	mockA.fail.Store(true)
	driveOps(t, client, 5)
	clock.Advance(50 * time.Millisecond)

	// Calling maybeAutoRefresh directly is safe — the refresher==nil
	// guard returns early.
	helix.MaybeAutoRefreshForTest(client, helix.ClusterA)
	assert.Equal(t, int64(0), mc.GetSessionRefreshAttempts(helix.ClusterA),
		"no refresher = no attempts (silently no-op)")
}

func TestAutoRefresh_NotEnabledByDefault(t *testing.T) {
	mockA := newFailingMock()
	mockB := newFailingMock()

	var invoked atomic.Bool
	refresher := func(_ context.Context, _ helix.ClusterID, _ error) (cql.Session, error) {
		invoked.Store(true)
		return newFailingMock(), nil
	}

	// SessionRefresher registered but no WithAutoRefresh call.
	client, err := helix.NewCQLClient(mockA, mockB,
		helix.WithSessionRefresher(refresher),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	assert.False(t, helix.AutoRefreshEnabledForTest(client),
		"AutoRefresh.Enabled defaults to false")
	assert.Nil(t, helix.AutoRefreshCtxForTest(client),
		"the detector goroutine is not started when AutoRefresh is disabled")
	assert.False(t, invoked.Load(),
		"refresher must not be invoked when WithAutoRefresh was not configured")
}

func TestAutoRefresh_RefresherErrorIncrementsErrorMetric(t *testing.T) {
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	mockA := newFailingMock()
	mockB := newFailingMock()

	mc := testutil.NewTestMetricsCollector()
	rebuildErr := errors.New("rebuild failed")
	refresher := func(_ context.Context, _ helix.ClusterID, _ error) (cql.Session, error) {
		return nil, rebuildErr
	}

	client, err := helix.NewCQLClient(mockA, mockB,
		helix.WithSessionRefresher(refresher),
		helix.WithAutoRefresh(fastAutoRefreshOpts()...),
		helix.WithMetrics(mc),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	helix.SetClientNowFuncForTest(client, clock.NowFunc())

	mockA.fail.Store(true)
	driveOps(t, client, 5)
	clock.Advance(50 * time.Millisecond)

	helix.MaybeAutoRefreshForTest(client, helix.ClusterA)

	assert.Equal(t, int64(1), mc.GetSessionRefreshAttempts(helix.ClusterA))
	assert.Equal(t, int64(0), mc.GetSessionRefreshSuccesses(helix.ClusterA))
	assert.Equal(t, int64(1), mc.GetSessionRefreshErrors(helix.ClusterA),
		"refresher error must increment SessionRefreshError")
}

func TestAutoRefresh_PartialSuccessDoesNotMisfireOnHealthyCluster(t *testing.T) {
	// Regression: in a dual-write where A succeeds and B fails, A's
	// lastSuccess must advance independently. Otherwise the detector
	// could falsely fire on the healthy cluster.
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	mockA := newFailingMock() // healthy
	mockB := newFailingMock() // dead

	mc := testutil.NewTestMetricsCollector()
	refresher := func(_ context.Context, _ helix.ClusterID, _ error) (cql.Session, error) {
		return newFailingMock(), nil
	}

	client, err := helix.NewCQLClient(mockA, mockB,
		helix.WithSessionRefresher(refresher),
		helix.WithAutoRefresh(fastAutoRefreshOpts()...),
		helix.WithMetrics(mc),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	helix.SetClientNowFuncForTest(client, clock.NowFunc())

	// A healthy, B dead.
	mockA.fail.Store(false)
	mockB.fail.Store(true)

	// Drive 100 dual writes. Each one: errA=nil (success), errB=err
	// (failure). recordOpOutcome must record success on A and failure
	// on B independently.
	driveOps(t, client, 100)

	// Advance well past SustainedFailureWindow.
	clock.Advance(1 * time.Second)

	helix.MaybeAutoRefreshForTest(client, helix.ClusterA)
	helix.MaybeAutoRefreshForTest(client, helix.ClusterA)
	helix.MaybeAutoRefreshForTest(client, helix.ClusterB)

	assert.Equal(t, int64(0), mc.GetSessionRefreshAttempts(helix.ClusterA),
		"healthy cluster A must NOT misfire — its lastSuccess was advancing on every dual-write")
	assert.GreaterOrEqual(t, mc.GetSessionRefreshAttempts(helix.ClusterB), int64(1),
		"failing cluster B must have triggered the detector")
}

func TestAutoRefresh_SingleClusterFires(t *testing.T) {
	// Regression test for the single-cluster wiring: without explicit
	// recordOpOutcome in executeWriteWithReplay's single-cluster fast
	// path, this scenario would silently not fire.
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	mockA := newFailingMock()

	mc := testutil.NewTestMetricsCollector()
	refresher := func(_ context.Context, _ helix.ClusterID, _ error) (cql.Session, error) {
		return newFailingMock(), nil
	}

	// Single-cluster: sessionB is nil.
	client, err := helix.NewCQLClient(mockA, nil,
		helix.WithSessionRefresher(refresher),
		helix.WithAutoRefresh(fastAutoRefreshOpts()...),
		helix.WithMetrics(mc),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	helix.SetClientNowFuncForTest(client, clock.NowFunc())

	mockA.fail.Store(true)
	driveOps(t, client, 5)
	clock.Advance(50 * time.Millisecond)
	helix.MaybeAutoRefreshForTest(client, helix.ClusterA)

	assert.Equal(t, int64(1), mc.GetSessionRefreshAttempts(helix.ClusterA),
		"single-cluster auto-refresh must fire on the read/write fast path")
}

func TestAutoRefresh_GoroutineFiresOnTick(t *testing.T) {
	// The one wall-clock smoke test: confirm the production goroutine
	// actually wakes and calls maybeAutoRefresh on its tick. Other tests
	// drive the detector logic synchronously to avoid wall-clock flake.
	mockA := newFailingMock()
	mockB := newFailingMock()

	mc := testutil.NewTestMetricsCollector()
	var refresherCalls atomic.Int32
	refresher := func(_ context.Context, _ helix.ClusterID, _ error) (cql.Session, error) {
		refresherCalls.Add(1)
		return newFailingMock(), nil
	}

	// Use real wall-clock with tight intervals.
	client, err := helix.NewCQLClient(mockA, mockB,
		helix.WithSessionRefresher(refresher),
		helix.WithAutoRefresh(
			helix.WithAutoRefreshFailureThreshold(2),
			helix.WithAutoRefreshSustainedFailureWindow(20*time.Millisecond),
			helix.WithAutoRefreshMinRetryInterval(20*time.Millisecond),
			helix.WithAutoRefreshCheckInterval(20*time.Millisecond),
			helix.WithAutoRefreshRefreshTimeout(1*time.Second),
		),
		helix.WithMetrics(mc),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	mockA.fail.Store(true)
	driveOps(t, client, 5)

	// Wait up to 500ms for the goroutine to fire at least once.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if mc.GetSessionRefreshAttempts(helix.ClusterA) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.GreaterOrEqual(t, mc.GetSessionRefreshAttempts(helix.ClusterA), int64(1),
		"the detector goroutine must have fired at least once within 500ms")
}

func TestAutoRefresh_StopsOnClose(t *testing.T) {
	mockA := newFailingMock()
	mockB := newFailingMock()

	refresher := func(_ context.Context, _ helix.ClusterID, _ error) (cql.Session, error) {
		return newFailingMock(), nil
	}

	client, err := helix.NewCQLClient(mockA, mockB,
		helix.WithSessionRefresher(refresher),
		helix.WithAutoRefresh(
			helix.WithAutoRefreshCheckInterval(10*time.Millisecond),
		),
	)
	require.NoError(t, err)

	require.NotNil(t, helix.AutoRefreshCtxForTest(client),
		"detector goroutine should be running")

	client.Close()

	// After Close, the detector context should be Done.
	ctx := helix.AutoRefreshCtxForTest(client)
	select {
	case <-ctx.Done():
		// expected
	case <-time.After(500 * time.Millisecond):
		t.Fatal("auto-refresh ctx not cancelled within 500ms of Close")
	}
}

// driveIters issues client.Query(...).IterContext(...).Close() N times.
// Iterator close errors must feed recordOpOutcome the same way Exec
// errors do, otherwise iterator-driven workloads stay invisible to the
// auto-refresh detector.
func driveIters(t *testing.T, c *helix.CQLClient, n int) {
	t.Helper()
	for i := range n {
		_ = c.Query("SELECT * FROM t WHERE k = ?", i).IterContext(context.Background()).Close()
	}
}

func TestAutoRefresh_IteratorFailuresAdvanceCounters(t *testing.T) {
	// Regression: iterator-driven reads must feed the auto-refresh
	// detector. Before this fix, cqlIter.Close only called
	// ReadStrategy.OnSuccess on a clean close; failures were invisible
	// to recordOpOutcome so consecutiveFailures never advanced.
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	mockA := newFailingMock()
	mockB := newFailingMock() // healthy throughout

	mc := testutil.NewTestMetricsCollector()
	var refresherCalls atomic.Int32
	refresher := func(_ context.Context, _ helix.ClusterID, _ error) (cql.Session, error) {
		refresherCalls.Add(1)
		return newFailingMock(), nil
	}

	client, err := helix.NewCQLClient(mockA, mockB,
		helix.WithSessionRefresher(refresher),
		helix.WithAutoRefresh(fastAutoRefreshOpts()...),
		helix.WithMetrics(mc),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	helix.SetClientNowFuncForTest(client, clock.NowFunc())

	// Drive iterator failures only — no Exec calls. The detector must
	// still trip purely from iterator close errors.
	mockA.fail.Store(true)
	driveIters(t, client, 5)
	clock.Advance(50 * time.Millisecond)

	helix.MaybeAutoRefreshForTest(client, helix.ClusterA)

	assert.EqualValues(t, 1, refresherCalls.Load(),
		"iterator failures must advance auto-refresh counters and trigger the refresher")
	assert.Equal(t, int64(1), mc.GetSessionRefreshAttempts(helix.ClusterA))
	assert.Equal(t, int64(1), mc.GetSessionRefreshSuccesses(helix.ClusterA))
}

func TestAutoRefresh_IteratorFailureThreadsLastErrToRefresher(t *testing.T) {
	// Iterator failures must populate lastErr so the refresher can
	// inspect the observed failure mode.
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	mockA := newFailingMock()
	mockB := newFailingMock()

	mc := testutil.NewTestMetricsCollector()
	var observed atomic.Value
	refresher := func(_ context.Context, _ helix.ClusterID, lastErr error) (cql.Session, error) {
		if lastErr != nil {
			observed.Store(lastErr.Error())
		}
		return newFailingMock(), nil
	}

	client, err := helix.NewCQLClient(mockA, mockB,
		helix.WithSessionRefresher(refresher),
		helix.WithAutoRefresh(fastAutoRefreshOpts()...),
		helix.WithMetrics(mc),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	helix.SetClientNowFuncForTest(client, clock.NowFunc())

	mockA.fail.Store(true)
	driveIters(t, client, 5)
	clock.Advance(50 * time.Millisecond)
	helix.MaybeAutoRefreshForTest(client, helix.ClusterA)

	got, _ := observed.Load().(string)
	assert.Equal(t, "simulated cluster failure", got,
		"refresher's lastErr must reflect the iterator failure")
}

func TestNewCQLClient_RejectsInvalidAutoRefreshKnobs(t *testing.T) {
	mockA := newFailingMock()
	refresher := func(_ context.Context, _ helix.ClusterID, _ error) (cql.Session, error) {
		return newFailingMock(), nil
	}

	_, err := helix.NewCQLClient(mockA, nil,
		helix.WithSessionRefresher(refresher),
		helix.WithAutoRefresh(
			helix.WithAutoRefreshCheckInterval(0),
			helix.WithAutoRefreshRefreshTimeout(-1*time.Second),
			helix.WithAutoRefreshMinRetryInterval(0),
			helix.WithAutoRefreshSustainedFailureWindow(-1),
			helix.WithAutoRefreshFailureThreshold(0),
		),
	)
	require.Error(t, err)
	assert.True(t, types.IsOptionError(err))
	assert.ErrorContains(t, err, "WithAutoRefreshCheckInterval")
	assert.ErrorContains(t, err, "WithAutoRefreshRefreshTimeout")
	assert.ErrorContains(t, err, "WithAutoRefreshMinRetryInterval")
	assert.ErrorContains(t, err, "WithAutoRefreshSustainedFailureWindow")
	assert.ErrorContains(t, err, "WithAutoRefreshFailureThreshold")
}
