package helix

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regression tests for the write acknowledgement, auto-recovery, and auto-refresh paths.
// Each test asserts the agreed behaviour and is skipped until the matching fix lands;
// remove the skip to reproduce the bug.
//
// The mocks below are safe under concurrency
// because AdaptiveDualWrite runs degraded legs on background goroutines
// and the replay worker executes on its own goroutines.
// Tests wait on channels and counters, never on sleeps.

const regressionWaitTimeout = 2 * time.Second

// recordingSession is a cql.Session whose every write reports to an atomic counter and a buffered channel.
// A nil err means every operation succeeds.
type recordingSession struct {
	err    error
	execs  atomic.Int32
	closed atomic.Bool
	execCh chan string
}

type recordingQuery struct {
	session *recordingSession
	stmt    string
	values  []any
}

type recordingBatch struct {
	session *recordingSession
	entries []cql.BatchEntry
}

type recordingIter struct {
	err error
}

// regressionClock is a manual clock for the auto-refresh detector.
type regressionClock struct {
	now atomic.Int64
}

// replayEnqueueCounter counts replay admissions for cluster B.
type replayEnqueueCounter struct {
	metrics.NopMetrics
	enqueuedB atomic.Int64
}

func newRecordingSession(err error) *recordingSession {
	return &recordingSession{err: err, execCh: make(chan string, 256)}
}

func newRegressionClock() *regressionClock {
	c := &regressionClock{}
	c.now.Store(time.Now().UnixNano())

	return c
}

// waitForExecs blocks until session has executed at least n writes in total.
func waitForExecs(t *testing.T, session *recordingSession, n int32) {
	t.Helper()
	for session.execs.Load() < n {
		select {
		case <-session.execCh:
		case <-time.After(regressionWaitTimeout):
			t.Fatalf("timed out waiting for %d executions, saw %d", n, session.execs.Load())
		}
	}
}

// fastAutoRefresh returns detector knobs that trip after three failures and
// a 20ms window, with a check interval long enough that the background
// goroutine never runs during a test.
func fastAutoRefresh() Option {
	return WithAutoRefresh(
		WithAutoRefreshFailureThreshold(3),
		WithAutoRefreshSustainedFailureWindow(20*time.Millisecond),
		WithAutoRefreshMinRetryInterval(10*time.Millisecond),
		WithAutoRefreshCheckInterval(10*time.Minute),
		WithAutoRefreshRefreshTimeout(time.Second),
	)
}

func (c *regressionClock) option() Option {
	return func(cfg *ClientConfig) {
		cfg.NowProvider = func() int64 { return c.now.Load() }
	}
}

func (c *regressionClock) advance(d time.Duration) {
	c.now.Add(int64(d))
}

func (m *replayEnqueueCounter) IncReplayEnqueued(cluster types.ClusterID) {
	if cluster == ClusterB {
		m.enqueuedB.Add(1)
	}
}

func (s *recordingSession) Query(stmt string, values ...any) cql.Query {
	return &recordingQuery{session: s, stmt: stmt, values: values}
}

func (s *recordingSession) Batch(_ cql.BatchType) cql.Batch {
	return &recordingBatch{session: s}
}

func (s *recordingSession) Close() {
	s.closed.Store(true)
}

func (s *recordingSession) exec(stmt string) error {
	s.execs.Add(1)
	select {
	case s.execCh <- stmt:
	default:
	}

	return s.err
}

func (q *recordingQuery) Consistency(cql.Consistency) cql.Query       { return q }
func (q *recordingQuery) SerialConsistency(cql.Consistency) cql.Query { return q }
func (q *recordingQuery) PageSize(int) cql.Query                      { return q }
func (q *recordingQuery) PageState([]byte) cql.Query                  { return q }
func (q *recordingQuery) WithTimestamp(int64) cql.Query               { return q }
func (q *recordingQuery) Statement() string                           { return q.stmt }
func (q *recordingQuery) Values() []any                               { return q.values }
func (q *recordingQuery) Release()                                    {}
func (q *recordingQuery) Exec() error                                 { return q.session.exec(q.stmt) }
func (q *recordingQuery) ExecContext(context.Context) error           { return q.session.exec(q.stmt) }
func (q *recordingQuery) Scan(...any) error                           { return q.session.err }
func (q *recordingQuery) ScanContext(context.Context, ...any) error   { return q.session.err }
func (q *recordingQuery) MapScan(map[string]any) error                { return q.session.err }
func (q *recordingQuery) MapScanContext(context.Context, map[string]any) error {
	return q.session.err
}
func (q *recordingQuery) ScanCAS(...any) (bool, error) { return true, q.session.exec(q.stmt) }
func (q *recordingQuery) ScanCASContext(context.Context, ...any) (bool, error) {
	return true, q.session.exec(q.stmt)
}
func (q *recordingQuery) MapScanCAS(map[string]any) (bool, error) {
	return true, q.session.exec(q.stmt)
}
func (q *recordingQuery) MapScanCASContext(context.Context, map[string]any) (bool, error) {
	return true, q.session.exec(q.stmt)
}
func (q *recordingQuery) Iter() cql.Iter { return &recordingIter{err: q.session.err} }
func (q *recordingQuery) IterContext(context.Context) cql.Iter {
	return &recordingIter{err: q.session.err}
}

func (b *recordingBatch) Query(stmt string, args ...any) cql.Batch {
	b.entries = append(b.entries, cql.BatchEntry{Statement: stmt, Args: args})
	return b
}
func (b *recordingBatch) Consistency(cql.Consistency) cql.Batch       { return b }
func (b *recordingBatch) SerialConsistency(cql.Consistency) cql.Batch { return b }
func (b *recordingBatch) WithTimestamp(int64) cql.Batch               { return b }
func (b *recordingBatch) Size() int                                   { return len(b.entries) }
func (b *recordingBatch) Statements() []cql.BatchEntry                { return b.entries }
func (b *recordingBatch) Exec() error                                 { return b.session.exec("batch") }
func (b *recordingBatch) ExecContext(context.Context) error           { return b.session.exec("batch") }
func (b *recordingBatch) IterContext(context.Context) cql.Iter {
	return &recordingIter{err: b.session.exec("batch")}
}
func (b *recordingBatch) ExecCAS(...any) (bool, cql.Iter, error) {
	return true, nil, b.session.exec("batch")
}
func (b *recordingBatch) ExecCASContext(context.Context, ...any) (bool, cql.Iter, error) {
	return true, nil, b.session.exec("batch")
}
func (b *recordingBatch) MapExecCAS(map[string]any) (bool, cql.Iter, error) {
	return true, nil, b.session.exec("batch")
}
func (b *recordingBatch) MapExecCASContext(context.Context, map[string]any) (bool, cql.Iter, error) {
	return true, nil, b.session.exec("batch")
}

func (i *recordingIter) Scan(...any) bool                    { return false }
func (i *recordingIter) Close() error                        { return i.err }
func (i *recordingIter) MapScan(map[string]any) bool         { return false }
func (i *recordingIter) SliceMap() ([]map[string]any, error) { return nil, i.err }
func (i *recordingIter) PageState() []byte                   { return nil }
func (i *recordingIter) NumRows() int                        { return 0 }
func (i *recordingIter) Columns() []cql.ColumnInfo           { return nil }
func (i *recordingIter) Scanner() cql.Scanner                { return nil }
func (i *recordingIter) Warnings() []string                  { return nil }

// TestAdaptiveWrite_ZeroSynchronousAckIsAnError verifies that a write which no cluster
// acknowledged synchronously is reported to the caller as an error.
// With both clusters degraded, AdaptiveDualWrite dispatches both legs fire-and-forget;
// nil would tell the caller the write exists somewhere
// when it is backed by nothing (no Replayer) or only by an in-memory queue.
func TestAdaptiveWrite_ZeroSynchronousAckIsAnError(t *testing.T) {
	cases := []struct {
		name string
		opts []Option
	}{
		{name: "no replayer"},
		{name: "memory replayer, default acknowledgement mode",
			opts: []Option{WithReplayer(replay.NewMemoryReplayer())}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outage := errors.New("simulated outage on both clusters")
			sa, sb := newRecordingSession(outage), newRecordingSession(outage)
			adaptive := policy.NewAdaptiveDualWrite()
			adaptive.ForceDegrade(ClusterA)
			adaptive.ForceDegrade(ClusterB)

			opts := append([]Option{
				WithWriteStrategy(adaptive),
				WithRecoveryProbeDisabled(),
			}, tc.opts...)
			client, err := NewCQLClient(sa, sb, opts...)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			writeErr := client.Query("INSERT INTO t (k, v) VALUES (?, ?)", 1, "v").ExecContext(t.Context())

			// Let both background legs finish before asserting so no goroutine
			// outlives the test.
			waitForExecs(t, sa, 1)
			waitForExecs(t, sb, 1)

			var noAck *types.NoSynchronousAckError
			require.ErrorAs(t, writeErr, &noAck,
				"a write acknowledged by neither cluster must not be reported as success")
			require.ErrorIs(t, noAck.ResultA, types.ErrWriteAsync)
			require.ErrorIs(t, noAck.ResultB, types.ErrWriteAsync)
		})
	}
}

// TestAutoRefresh_DegradedClusterFailuresStillTriggerRefresh verifies that a cluster whose
// session is dead is still refreshed when AdaptiveDualWrite has moved it to fire-and-forget.
// The failures happen on the background leg and must still reach the auto-refresh detector.
func TestAutoRefresh_DegradedClusterFailuresStillTriggerRefresh(t *testing.T) {
	t.Skip("pending: failures on AdaptiveDualWrite fire-and-forget legs never reach the " +
		"auto-refresh counters, so a degraded cluster with a dead session is never refreshed")

	clock := newRegressionClock()
	dead := errors.New("simulated connectivity failure: no connections available")
	sa, sb := newRecordingSession(dead), newRecordingSession(nil)
	adaptive := policy.NewAdaptiveDualWrite()
	adaptive.ForceDegrade(ClusterA)

	var refreshedA atomic.Int32
	refresher := func(_ context.Context, cluster ClusterID, _ error) (cql.Session, error) {
		if cluster == ClusterA {
			refreshedA.Add(1)
		}

		return newRecordingSession(nil), nil
	}

	client, err := NewCQLClient(sa, sb,
		WithWriteStrategy(adaptive),
		WithRecoveryProbeDisabled(),
		WithSessionRefresher(refresher),
		fastAutoRefresh(),
		clock.option(),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	const writes = 5
	for i := range writes {
		_ = client.Query("INSERT INTO t (k, v) VALUES (?, ?)", i, "v").ExecContext(t.Context())
	}
	// Every fire-and-forget leg has now executed and failed on cluster A.
	waitForExecs(t, sa, writes)
	clock.advance(50 * time.Millisecond)

	// The background leg reports its outcome just after the mock returns, so
	// poll the detector instead of ticking it exactly once.
	require.Eventually(t, func() bool {
		client.maybeAutoRefresh(ClusterA)

		return refreshedA.Load() >= 1
	}, regressionWaitTimeout, 5*time.Millisecond,
		"refresher must be invoked for cluster A after sustained fire-and-forget failures")
}

// TestAutoRefresh_DoesNotReplaceHealthySessionOnSchemaErrors verifies that the auto-refresh
// detector neither fires before the first success has been observed
// nor counts errors that say nothing about connectivity.
// Either would close a perfectly healthy session on a fresh client.
func TestAutoRefresh_DoesNotReplaceHealthySessionOnSchemaErrors(t *testing.T) {
	t.Skip("pending: the auto-refresh window is unarmed until the first success and every " +
		"error counts, so schema errors on a fresh client replace and close a healthy session")

	cases := []struct {
		name    string
		err     error
		advance time.Duration
	}{
		{
			name:    "schema error is not a connectivity signal",
			err:     errors.New("Unconfigured table"),
			advance: 50 * time.Millisecond, // past the sustained-failure window
		},
		{
			name: "window is armed at construction",
			// Swap for the typed connectivity sentinel once it exists.
			err:     errors.New("simulated connectivity failure: no connections available"),
			advance: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clock := newRegressionClock()
			sa, sb := newRecordingSession(tc.err), newRecordingSession(nil)

			var refreshCalls atomic.Int32
			refresher := func(_ context.Context, _ ClusterID, _ error) (cql.Session, error) {
				refreshCalls.Add(1)

				return newRecordingSession(nil), nil
			}

			client, err := NewCQLClient(sa, sb,
				WithSessionRefresher(refresher),
				fastAutoRefresh(),
				clock.option(),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			for i := range 10 {
				_ = client.Query("INSERT INTO t (k, v) VALUES (?, ?)", i, "v").ExecContext(t.Context())
			}
			clock.advance(tc.advance)

			client.maybeAutoRefresh(ClusterA)

			require.Zero(t, refreshCalls.Load(), "refresher must not be invoked")
			require.False(t, sa.closed.Load(), "original session A must not be closed")
			require.Same(t, sa, client.loadSessionA(), "session A must not be replaced")
		})
	}
}

// TestRecoveryProbe_DoesNotClearForceDegrade verifies that ForceDegrade is a
// sticky operator latch: successful recovery probes must not restore
// synchronous writes on the latched cluster, and only ForceRecover clears it.
//
// Cluster A is degraded by a real failure (not latched) and its probe always fails,
// so A stays degraded and its probe calls count the ticks of the probe loops,
// which share one interval.
// Cluster B is latched and probes fast.
func TestRecoveryProbe_DoesNotClearForceDegrade(t *testing.T) {
	t.Skip("pending: a successful recovery probe credits recovery on a cluster the operator " +
		"degraded by hand, so ForceDegrade is undone within a few probe ticks")

	failA := errors.New("simulated cluster A failure")
	sa, sb := newRecordingSession(failA), newRecordingSession(nil)
	adaptive := policy.NewAdaptiveDualWrite(policy.WithAdaptiveStrikeThreshold(1))
	adaptive.ForceDegrade(ClusterB)

	probesA := make(chan struct{}, 256)
	probe := RecoveryProbe{
		Probe: func(_ context.Context, s cql.Session) error {
			if s == sa {
				select {
				case probesA <- struct{}{}:
				default:
				}

				return failA
			}

			return nil
		},
		Interval: 5 * time.Millisecond,
		Timeout:  50 * time.Millisecond,
	}

	client, err := NewCQLClient(sa, sb,
		WithWriteStrategy(adaptive),
		WithRecoveryProbe(probe),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	// One failed synchronous write on A degrades it through the strike path.
	require.NoError(t, client.Query("INSERT INTO t (k, v) VALUES (?, ?)", 1, "v").ExecContext(t.Context()))
	require.True(t, adaptive.IsDegraded(ClusterA), "cluster A must be degraded by its failed write")

	// Wait for several times the recovery threshold worth of probe ticks.
	for range 20 {
		select {
		case <-probesA:
		case <-time.After(regressionWaitTimeout):
			t.Fatal("timed out waiting for recovery probe ticks")
		}
	}

	require.True(t, adaptive.IsDegraded(ClusterB),
		"ForceDegrade must stay latched while the recovery probe succeeds")

	adaptive.ForceRecover(ClusterB)
	require.False(t, adaptive.IsDegraded(ClusterB), "ForceRecover must clear the latch")
}

// TestAdaptiveWrite_DegradedClusterAppliesStatementOnce verifies that a write
// to a degraded cluster is applied exactly once on that cluster: replay must
// be enqueued only when the fire-and-forget leg reports failure, never as a
// safety net beside a leg that succeeded.
func TestAdaptiveWrite_DegradedClusterAppliesStatementOnce(t *testing.T) {
	t.Skip("pending: a write to a degraded cluster is executed by the fire-and-forget leg and " +
		"again by replay, so counter updates are applied twice")

	sa, sb := newRecordingSession(nil), newRecordingSession(nil)
	adaptive := policy.NewAdaptiveDualWrite()
	adaptive.ForceDegrade(ClusterB)
	replayer := replay.NewMemoryReplayer()
	mc := &replayEnqueueCounter{}

	client, err := NewCQLClient(sa, sb,
		WithWriteStrategy(adaptive),
		WithRecoveryProbeDisabled(),
		WithReplayer(replayer),
		WithMetrics(mc),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	replayed := make(chan struct{}, 16)
	worker := replay.NewMemoryWorker(replayer, client.DefaultExecuteFunc(),
		replay.WithPollInterval(time.Millisecond),
		replay.WithOnSuccess(func(types.ReplayPayload) { replayed <- struct{}{} }),
	)
	require.NoError(t, worker.Start())
	t.Cleanup(worker.Stop)

	const stmt = "UPDATE counters SET hits = hits + 1 WHERE id = ?"
	require.NoError(t, client.Query(stmt, 1).ExecContext(t.Context()))

	// The fire-and-forget leg has landed on B.
	waitForExecs(t, sb, 1)

	// Drain every replay the client admitted so the execution count is final.
	enqueued := mc.enqueuedB.Load()
	for range enqueued {
		select {
		case <-replayed:
		case <-time.After(regressionWaitTimeout):
			t.Fatal("timed out waiting for the replay worker")
		}
	}

	assert.Zero(t, enqueued, "no replay must be enqueued while the fire-and-forget leg succeeded")
	require.Equal(t, int32(1), sb.execs.Load(), "cluster B must execute the statement exactly once")
	require.Equal(t, int32(1), sa.execs.Load(), "cluster A must execute the statement exactly once")
}
