package helix

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSession implements cql.Session for testing.
type mockSession struct {
	queries    []string
	execErr    error
	execPanic  any // if set, ExecContext panics with this value instead of returning execErr
	scanErr    error
	scanValues []any
	closed     atomic.Bool
	closedAt   atomic.Int64 // Unix nanoseconds of the first Close
	lastQuery  *mockQuery   // Track last query for inspection
}

func newMockSession() *mockSession {
	return &mockSession{}
}

func (m *mockSession) Query(stmt string, values ...any) cql.Query {
	m.queries = append(m.queries, stmt)
	q := &mockQuery{
		session:   m,
		statement: stmt,
		values:    values,
	}
	m.lastQuery = q

	return q
}

func (m *mockSession) Batch(kind cql.BatchType) cql.Batch {
	return &mockBatch{session: m}
}

func (m *mockSession) Close() {
	if m.closed.CompareAndSwap(false, true) {
		m.closedAt.Store(time.Now().UnixNano())
	}
}

// mockQuery implements cql.Query for testing.
type mockQuery struct {
	session           *mockSession
	statement         string
	values            []any
	pageSize          *int
	pageState         []byte
	consistency       *cql.Consistency
	serialConsistency *cql.Consistency
	timestamp         *int64
	ctx               context.Context // Track context
}

func (q *mockQuery) Consistency(c cql.Consistency) cql.Query {
	q.consistency = &c
	return q
}

func (q *mockQuery) SerialConsistency(c cql.Consistency) cql.Query {
	q.serialConsistency = &c
	return q
}

func (q *mockQuery) PageSize(n int) cql.Query {
	q.pageSize = &n
	return q
}

func (q *mockQuery) PageState(state []byte) cql.Query {
	q.pageState = state
	return q
}

func (q *mockQuery) WithTimestamp(ts int64) cql.Query {
	q.timestamp = &ts
	return q
}
func (q *mockQuery) Statement() string { return q.statement }
func (q *mockQuery) Values() []any     { return q.values }
func (q *mockQuery) Release()          {}

func (q *mockQuery) Exec() error {
	return q.session.execErr
}

func (q *mockQuery) Scan(dest ...any) error {
	if q.session.scanErr != nil {
		return q.session.scanErr
	}
	for i, v := range q.session.scanValues {
		if i < len(dest) {
			switch d := dest[i].(type) {
			case *string:
				if s, ok := v.(string); ok {
					*d = s
				}
			case *int:
				if n, ok := v.(int); ok {
					*d = n
				}
			}
		}
	}

	return nil
}

func (q *mockQuery) Iter() cql.Iter {
	return &mockIter{}
}

func (q *mockQuery) MapScan(m map[string]any) error {
	return q.session.scanErr
}

func (q *mockQuery) ExecContext(ctx context.Context) error {
	q.ctx = ctx
	if q.session.execPanic != nil {
		panic(q.session.execPanic)
	}
	return q.session.execErr
}

func (q *mockQuery) ScanContext(ctx context.Context, dest ...any) error {
	q.ctx = ctx
	return q.Scan(dest...)
}

func (q *mockQuery) IterContext(ctx context.Context) cql.Iter {
	q.ctx = ctx
	return &mockIter{}
}

func (q *mockQuery) MapScanContext(ctx context.Context, m map[string]any) error {
	q.ctx = ctx
	return q.session.scanErr
}

func (q *mockQuery) ScanCAS(_ ...any) (applied bool, err error) {
	return true, q.session.execErr
}

func (q *mockQuery) ScanCASContext(ctx context.Context, _ ...any) (applied bool, err error) {
	q.ctx = ctx
	return true, q.session.execErr
}

func (q *mockQuery) MapScanCAS(_ map[string]any) (applied bool, err error) {
	return true, q.session.execErr
}

func (q *mockQuery) MapScanCASContext(ctx context.Context, _ map[string]any) (applied bool, err error) {
	q.ctx = ctx
	return true, q.session.execErr
}

// mockBatch implements cql.Batch for testing.
type mockBatch struct {
	session *mockSession
	entries []cql.BatchEntry
	ctx     context.Context // Track context
}

func (b *mockBatch) Query(stmt string, args ...any) cql.Batch {
	b.entries = append(b.entries, cql.BatchEntry{Statement: stmt, Args: args})
	return b
}

func (b *mockBatch) Consistency(_ cql.Consistency) cql.Batch       { return b }
func (b *mockBatch) SerialConsistency(_ cql.Consistency) cql.Batch { return b }
func (b *mockBatch) WithTimestamp(_ int64) cql.Batch               { return b }
func (b *mockBatch) Size() int                                     { return len(b.entries) }
func (b *mockBatch) Statements() []cql.BatchEntry                  { return slices.Clone(b.entries) }

func (b *mockBatch) Exec() error {
	return b.session.execErr
}

func (b *mockBatch) ExecContext(ctx context.Context) error {
	b.ctx = ctx
	return b.session.execErr
}

func (b *mockBatch) IterContext(ctx context.Context) cql.Iter {
	b.ctx = ctx
	return &mockIter{}
}

func (b *mockBatch) ExecCAS(_ ...any) (applied bool, iter cql.Iter, err error) {
	return true, &mockIter{}, b.session.execErr
}

func (b *mockBatch) ExecCASContext(ctx context.Context, _ ...any) (applied bool, iter cql.Iter, err error) {
	b.ctx = ctx
	return true, &mockIter{}, b.session.execErr
}

func (b *mockBatch) MapExecCAS(_ map[string]any) (applied bool, iter cql.Iter, err error) {
	return true, &mockIter{}, b.session.execErr
}

func (b *mockBatch) MapExecCASContext(ctx context.Context, _ map[string]any) (applied bool, iter cql.Iter, err error) {
	b.ctx = ctx
	return true, &mockIter{}, b.session.execErr
}

// mockIter implements cql.Iter for testing.
type mockIter struct{}

func (i *mockIter) Scan(_ ...any) bool                  { return false }
func (i *mockIter) Close() error                        { return nil }
func (i *mockIter) MapScan(_ map[string]any) bool       { return false }
func (i *mockIter) SliceMap() ([]map[string]any, error) { return nil, nil }
func (i *mockIter) PageState() []byte                   { return nil }
func (i *mockIter) NumRows() int                        { return 0 }
func (i *mockIter) Columns() []cql.ColumnInfo           { return nil }
func (i *mockIter) Scanner() cql.Scanner                { return &mockScanner{} }
func (i *mockIter) Warnings() []string                  { return nil }

// mockScanner implements cql.Scanner for testing.
type mockScanner struct{}

func (s *mockScanner) Next() bool          { return false }
func (s *mockScanner) Scan(_ ...any) error { return nil }
func (s *mockScanner) Err() error          { return nil }

// mockReplayWorker implements ReplayWorker for testing.
type mockReplayWorker struct {
	startErr  error
	running   atomic.Bool
	stopCalls atomic.Int32
}

func newMockReplayWorker(startErr error) *mockReplayWorker {
	return &mockReplayWorker{startErr: startErr}
}

func (w *mockReplayWorker) Start() error {
	if w.startErr != nil {
		return w.startErr
	}
	w.running.Store(true)
	return nil
}

func (w *mockReplayWorker) Stop() {
	w.running.Store(false)
	w.stopCalls.Add(1)
}

func (w *mockReplayWorker) IsRunning() bool {
	return w.running.Load()
}

// recordingWriteStrategy implements WriteStrategy and types.ClusterNamer to
// detect whether propagateClusterNames was called on a rejected constructor.
type recordingWriteStrategy struct {
	called bool
}

func (s *recordingWriteStrategy) Execute(
	ctx context.Context,
	writeA func(context.Context) error,
	_ func(context.Context) error,
) (resultA, resultB error) {
	return writeA(ctx), nil
}

func (s *recordingWriteStrategy) SetClusterNames(_ types.ClusterNames) {
	s.called = true
}

// mockTopologyWatcher implements TopologyWatcher for testing.
type mockTopologyWatcher struct {
	mu      sync.RWMutex
	drainA  bool
	drainB  bool
	updates chan TopologyUpdate
	done    chan struct{}
	closed  bool
}

func newMockTopologyWatcher() *mockTopologyWatcher {
	return &mockTopologyWatcher{
		updates: make(chan TopologyUpdate, 10),
		done:    make(chan struct{}),
	}
}

func (w *mockTopologyWatcher) Watch(ctx context.Context) <-chan TopologyUpdate {
	go func() {
		select {
		case <-ctx.Done():
		case <-w.done:
		}
		w.mu.Lock()
		if !w.closed {
			w.closed = true
			close(w.updates)
		}
		w.mu.Unlock()
	}()

	return w.updates
}

func (w *mockTopologyWatcher) IsDraining(cluster ClusterID) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if cluster == ClusterA {
		return w.drainA
	}

	return w.drainB
}

func (w *mockTopologyWatcher) GetDrainReason(cluster ClusterID) string {
	if w.IsDraining(cluster) {
		return "test drain"
	}

	return ""
}

func (w *mockTopologyWatcher) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.closed {
		close(w.done)
	}

	return nil
}

func (w *mockTopologyWatcher) SetDrain(cluster ClusterID, drain bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return
	}

	var changed bool
	switch cluster {
	case ClusterA:
		changed = w.drainA != drain
		w.drainA = drain
	case ClusterB:
		changed = w.drainB != drain
		w.drainB = drain
	}

	if changed {
		select {
		case w.updates <- TopologyUpdate{
			Cluster:   cluster,
			Available: !drain,
			DrainMode: drain,
		}:
		default:
		}
	}
}

// neverClosingTopologyWatcher reproduces a misbehaving/custom TopologyWatcher
// whose Watch ignores ctx cancellation and never closes its returned
// channel. Used to regression-test that watchTopology's goroutine still
// exits via topologyCtx.Done() rather than blocking forever on `for range`.
type neverClosingTopologyWatcher struct {
	updates chan TopologyUpdate
}

func newNeverClosingTopologyWatcher() *neverClosingTopologyWatcher {
	return &neverClosingTopologyWatcher{updates: make(chan TopologyUpdate)}
}

func (w *neverClosingTopologyWatcher) Watch(_ context.Context) <-chan TopologyUpdate {
	return w.updates
}

// nilChanTopologyWatcher reproduces a TopologyWatcher whose Watch returns a
// nil channel — ranging over it blocks forever identically to the
// never-closing case above. invoked is closed synchronously inside Watch,
// before the nil channel is returned, so a test can prove watchTopology's
// goroutine actually started and reached the Watch() call — as opposed to
// merely observing "not in the stack", which could pass trivially before
// the goroutine is ever scheduled and would mask a real leak just as
// easily as it proves an immediate return.
type nilChanTopologyWatcher struct {
	invoked chan struct{}
}

func newNilChanTopologyWatcher() *nilChanTopologyWatcher {
	return &nilChanTopologyWatcher{invoked: make(chan struct{})}
}

func (w *nilChanTopologyWatcher) Watch(_ context.Context) <-chan TopologyUpdate {
	close(w.invoked)
	return nil
}

// finalUpdateThenCloseWatcher is a TopologyWatcher whose Watch returns a
// buffered channel fully controlled by the test: the test sends updates and
// closes the channel directly, independent of ctx cancellation. Used to
// prove watchTopology's `case update, ok := <-updates` drain path applies a
// final buffered update before returning on channel closure alone — as
// opposed to via topologyCtx.Done() (covered separately).
type finalUpdateThenCloseWatcher struct {
	updates chan TopologyUpdate
}

func newFinalUpdateThenCloseWatcher() *finalUpdateThenCloseWatcher {
	return &finalUpdateThenCloseWatcher{updates: make(chan TopologyUpdate, 1)}
}

func (w *finalUpdateThenCloseWatcher) Watch(_ context.Context) <-chan TopologyUpdate {
	return w.updates
}

// watchTopologyGoroutineRunning reports whether watchTopology's goroutine
// currently appears in any running goroutine's stack trace. Used to detect
// whether it is still alive without requiring a dedicated WaitGroup/hook.
func watchTopologyGoroutineRunning() bool {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)

	return strings.Contains(string(buf[:n]), "CQLClient).watchTopology")
}

// TestWatchTopology_MisbehavingWatcherGoroutineExitsOnClose is a regression
// test for cql_client.go:713 (nil-safety finding): watchTopology previously
// had no select on topologyCtx.Done(), so a TopologyWatcher that ignores ctx
// cancellation and never closes its channel would leak the goroutine forever
// and make Close()'s cancellation of topologyCtx a no-op for it.
func TestWatchTopology_MisbehavingWatcherGoroutineExitsOnClose(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	watcher := newNeverClosingTopologyWatcher()

	client, err := NewCQLClient(sessionA, sessionB, WithTopologyWatcher(watcher))
	require.NoError(t, err)

	require.Eventually(t, watchTopologyGoroutineRunning, time.Second, 5*time.Millisecond,
		"watchTopology goroutine must be running before Close()")

	client.Close()

	require.Eventually(t, func() bool {
		return !watchTopologyGoroutineRunning()
	}, time.Second, 5*time.Millisecond,
		"watchTopology goroutine must exit after Close(), even though the misbehaving watcher never closes its update channel or observes ctx")
}

// TestWatchTopology_NilChannelReturnsImmediately is a regression test for the
// nil-channel case of the same finding: a TopologyWatcher.Watch that returns
// a nil channel must not block the goroutine forever.
//
// The invoked signal proves the watchTopology goroutine actually started and
// called Watch() before we assert it has exited — a bare "not present in the
// stack" check could otherwise pass trivially before the goroutine is ever
// scheduled, without ever proving the immediate-return behavior under test.
func TestWatchTopology_NilChannelReturnsImmediately(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	watcher := newNilChanTopologyWatcher()

	client, err := NewCQLClient(sessionA, sessionB, WithTopologyWatcher(watcher))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	select {
	case <-watcher.invoked:
	case <-time.After(time.Second):
		t.Fatal("watchTopology goroutine must call Watch() — it never started")
	}

	require.Eventually(t, func() bool {
		return !watchTopologyGoroutineRunning()
	}, time.Second, 5*time.Millisecond, "watchTopology must return immediately for a nil update channel, not block on 'for range nil'")
}

// TestWatchTopology_FinalBufferedUpdateAppliedBeforeCloseReturn verifies the
// select's `case update, ok := <-updates` drain path: when a well-behaved
// watcher sends a final update and then closes its channel — WITHOUT the
// client's topologyCtx ever being cancelled — watchTopology must still
// receive and apply that buffered update before it observes ok == false and
// returns, exactly like a plain (unselected) `for update := range updates`
// would. Only client.Close() (in t.Cleanup) cancels topologyCtx, and it runs
// after both assertions below.
func TestWatchTopology_FinalBufferedUpdateAppliedBeforeCloseReturn(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	watcher := newFinalUpdateThenCloseWatcher()

	client, err := NewCQLClient(sessionA, sessionB, WithTopologyWatcher(watcher))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	watcher.updates <- TopologyUpdate{Cluster: ClusterA, Available: false, DrainMode: true}
	close(watcher.updates)

	require.Eventually(t, func() bool {
		return client.IsDraining(ClusterA)
	}, time.Second, 5*time.Millisecond,
		"the final buffered update must be applied via the channel-close drain path, not dropped when ok becomes false")

	require.Eventually(t, func() bool {
		return !watchTopologyGoroutineRunning()
	}, time.Second, 5*time.Millisecond, "watchTopology must return once the update channel is drained and closed")
}

func TestNewCQLClient(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestCQLClientImplementsCQLSession(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)
	defer client.Close()

	// Verify Session() returns the client as CQLSession
	session := client.Session()
	require.NotNil(t, session)

	// Verify it's the same client
	require.Equal(t, client, session)

	// Verify CQLSession methods work through the interface
	query := session.Query("SELECT * FROM test")
	require.NotNil(t, query)

	batch := session.Batch(LoggedBatch)
	require.NotNil(t, batch)

	batch2 := session.Batch(UnloggedBatch)
	require.NotNil(t, batch2)
}

func TestNewCQLClientNilSession(t *testing.T) {
	sessionA := newMockSession()

	// sessionA is required
	_, err := NewCQLClient(nil, sessionA)
	require.ErrorIs(t, err, types.ErrNilSession)

	// sessionB is optional (nil = single-cluster mode)
	client, err := NewCQLClient(sessionA, nil)
	require.NoError(t, err)
	require.True(t, client.IsSingleCluster())
}

func TestNewCQLClientRejectsInvalidClusterNames(t *testing.T) {
	_, err := NewCQLClient(newMockSession(), nil, WithClusterNames("", "valid"))
	require.Error(t, err)
	assert.True(t, types.IsOptionError(err))
	assert.ErrorContains(t, err, "WithClusterNames")
}

func TestNewCQLClientJoinsMultipleInvalidRootOptions(t *testing.T) {
	_, err := NewCQLClient(newMockSession(), nil,
		WithClusterNames("", "valid"),
		WithAutoRefresh(
			WithAutoRefreshFailureThreshold(0),
		),
	)
	require.Error(t, err)
	assert.True(t, types.IsOptionError(err))
	assert.ErrorContains(t, err, "WithClusterNames")
	assert.ErrorContains(t, err, "WithAutoRefreshFailureThreshold")
}

func TestNewCQLClientDoesNotPropagateNamesToStrategyOnValidationFailure(t *testing.T) {
	strategy := &recordingWriteStrategy{}

	_, err := NewCQLClient(newMockSession(), newMockSession(),
		WithWriteStrategy(strategy),
		WithClusterNames("", "valid"),
	)
	require.Error(t, err)
	require.False(t, strategy.called, "SetClusterNames must not be called when the constructor rejects the config")
}

func TestNewCQLClientDoesNotPropagateNamesOnAutoWorkerValidationFailure(t *testing.T) {
	strategy := &recordingWriteStrategy{}

	_, err := NewCQLClient(newMockSession(), newMockSession(),
		WithWriteStrategy(strategy),
		WithClusterNames("primary", "secondary"),
		WithAutoMemoryWorker(100, replay.WithBatchSize(0)),
	)
	require.Error(t, err)
	require.True(t, types.IsOptionError(err))
	require.ErrorContains(t, err, "WithBatchSize")
	require.False(t, strategy.called, "SetClusterNames must not be called when nested auto-worker validation fails")
}

// ─────────────────────────────────────────────
// Deprecated batch-compat shims: nil Batch guard
// ─────────────────────────────────────────────

func TestExecuteBatch_NilBatch_ReturnsErrNilBatch(t *testing.T) {
	client, err := NewCQLClient(newMockSession(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	err = client.ExecuteBatch(nil)
	require.ErrorIs(t, err, errNilBatch)
}

func TestExecuteBatchCAS_NilBatch_ReturnsErrNilBatch(t *testing.T) {
	client, err := NewCQLClient(newMockSession(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	applied, iter, err := client.ExecuteBatchCAS(nil)
	require.ErrorIs(t, err, errNilBatch)
	assert.False(t, applied)
	assert.Nil(t, iter)
}

func TestMapExecuteBatchCAS_NilBatch_ReturnsErrNilBatch(t *testing.T) {
	client, err := NewCQLClient(newMockSession(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	applied, iter, err := client.MapExecuteBatchCAS(nil, map[string]any{})
	require.ErrorIs(t, err, errNilBatch)
	assert.False(t, applied)
	assert.Nil(t, iter)
}

// TestExecuteBatch_NonNilBatch_StillWorks locks in that the nil guard did not
// disturb the happy path for the deprecated shims.
func TestExecuteBatch_NonNilBatch_StillWorks(t *testing.T) {
	session := newMockSession()
	client, err := NewCQLClient(session, nil)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	batch := client.NewBatch(LoggedBatch)
	batch.Query("INSERT INTO t (id) VALUES (?)", 1)

	require.NoError(t, client.ExecuteBatch(batch))
}

func TestCQLClientSingleClusterMode(t *testing.T) {
	t.Run("write goes to single session", func(t *testing.T) {
		session := newMockSession()

		client, err := NewCQLClient(session, nil)
		require.NoError(t, err)
		require.True(t, client.IsSingleCluster())
		defer client.Close()

		err = client.Query("INSERT INTO test (id) VALUES (?)", 1).Exec()
		require.NoError(t, err)

		// Only one session should receive the query
		require.Contains(t, session.queries, "INSERT INTO test (id) VALUES (?)")
	})

	t.Run("read goes to single session", func(t *testing.T) {
		session := newMockSession()
		session.scanValues = []any{"test-value"}

		client, err := NewCQLClient(session, nil)
		require.NoError(t, err)
		defer client.Close()

		var value string
		err = client.Query("SELECT value FROM test").Scan(&value)
		require.NoError(t, err)
		require.Equal(t, "test-value", value)
	})

	t.Run("batch goes to single session", func(t *testing.T) {
		session := newMockSession()

		client, err := NewCQLClient(session, nil)
		require.NoError(t, err)
		defer client.Close()

		err = client.Batch(LoggedBatch).
			Query("INSERT INTO test (id) VALUES (?)", 1).
			Query("INSERT INTO test (id) VALUES (?)", 2).
			Exec()
		require.NoError(t, err)
	})

	t.Run("close only closes single session", func(t *testing.T) {
		session := newMockSession()

		client, err := NewCQLClient(session, nil)
		require.NoError(t, err)

		client.Close()
		require.True(t, session.closed.Load())
	})

	t.Run("dual cluster mode returns false for IsSingleCluster", func(t *testing.T) {
		sessionA := newMockSession()
		sessionB := newMockSession()

		client, err := NewCQLClient(sessionA, sessionB)
		require.NoError(t, err)
		require.False(t, client.IsSingleCluster())
		client.Close()
	})
}

func TestCQLClientQueryExec(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)

	err = client.Query("INSERT INTO test (id) VALUES (?)", 1).Exec()
	require.NoError(t, err)

	// Both sessions should have received the query
	require.Contains(t, sessionA.queries, "INSERT INTO test (id) VALUES (?)")
	require.Contains(t, sessionB.queries, "INSERT INTO test (id) VALUES (?)")
}

func TestCQLClientQueryExecPartialFailure(t *testing.T) {
	sessionA := newMockSession()
	sessionA.execErr = errors.New("cluster A failed")
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)

	// Should succeed because B succeeds
	err = client.Query("INSERT INTO test (id) VALUES (?)", 1).Exec()
	require.NoError(t, err)
}

func TestCQLClientQueryExecBothFail(t *testing.T) {
	sessionA := newMockSession()
	sessionA.execErr = errors.New("cluster A failed")
	sessionB := newMockSession()
	sessionB.execErr = errors.New("cluster B failed")

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)

	err = client.Query("INSERT INTO test (id) VALUES (?)", 1).Exec()
	require.Error(t, err)
	require.ErrorIs(t, err, types.ErrBothClustersFailed)
}

func TestCQLClientClose(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)

	client.Close()

	require.True(t, sessionA.closed.Load())
	require.True(t, sessionB.closed.Load())
}

func TestCQLClientCloseIdempotent(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)

	// Multiple closes should not panic
	client.Close()
	client.Close()
	client.Close()
}

func TestCQLClientBatch(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)

	err = client.Batch(LoggedBatch).
		Query("INSERT INTO test (id) VALUES (?)", 1).
		Query("INSERT INTO test (id) VALUES (?)", 2).
		Exec()
	require.NoError(t, err)
}

func TestCQLClientQueryScan(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanValues = []any{"test-value", 42}
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)

	var name string
	var count int
	err = client.Query("SELECT name, count FROM test WHERE id = ?", 1).Scan(&name, &count)
	require.NoError(t, err)
	require.Equal(t, "test-value", name)
	require.Equal(t, 42, count)
}

func TestCQLClientQueryConfigPropagation(t *testing.T) {
	t.Run("PageSize is propagated to underlying query", func(t *testing.T) {
		sessionA := newMockSession()
		sessionB := newMockSession()

		client, err := NewCQLClient(sessionA, sessionB)
		require.NoError(t, err)
		defer client.Close()

		// Execute a query with PageSize
		_ = client.Query("SELECT * FROM test").PageSize(100).Iter()

		// Verify PageSize was set on the underlying query
		require.NotNil(t, sessionA.lastQuery)
		require.NotNil(t, sessionA.lastQuery.pageSize)
		require.Equal(t, 100, *sessionA.lastQuery.pageSize)
	})

	t.Run("PageState is propagated to underlying query", func(t *testing.T) {
		sessionA := newMockSession()
		sessionB := newMockSession()

		client, err := NewCQLClient(sessionA, sessionB)
		require.NoError(t, err)
		defer client.Close()

		pageToken := []byte{0x01, 0x02, 0x03, 0x04}

		// Execute a query with PageState
		_ = client.Query("SELECT * FROM test").PageState(pageToken).Iter()

		// Verify PageState was set on the underlying query
		require.NotNil(t, sessionA.lastQuery)
		require.Equal(t, pageToken, sessionA.lastQuery.pageState)
	})

	t.Run("Consistency is propagated to underlying query", func(t *testing.T) {
		sessionA := newMockSession()
		sessionB := newMockSession()

		client, err := NewCQLClient(sessionA, sessionB)
		require.NoError(t, err)
		defer client.Close()

		// Execute a query with Consistency
		_ = client.Query("SELECT * FROM test").Consistency(Quorum).Iter()

		// Verify Consistency was set on the underlying query
		require.NotNil(t, sessionA.lastQuery)
		require.NotNil(t, sessionA.lastQuery.consistency)
		require.Equal(t, Quorum, *sessionA.lastQuery.consistency)
	})

	t.Run("SerialConsistency is propagated to underlying query", func(t *testing.T) {
		sessionA := newMockSession()
		sessionB := newMockSession()

		client, err := NewCQLClient(sessionA, sessionB)
		require.NoError(t, err)
		defer client.Close()

		// Execute a query with SerialConsistency
		_ = client.Query("SELECT * FROM test").SerialConsistency(LocalSerial).Iter()

		// Verify SerialConsistency was set on the underlying query
		require.NotNil(t, sessionA.lastQuery)
		require.NotNil(t, sessionA.lastQuery.serialConsistency)
		require.Equal(t, LocalSerial, *sessionA.lastQuery.serialConsistency)
	})

	t.Run("WithTimestamp is propagated to underlying query on Exec", func(t *testing.T) {
		sessionA := newMockSession()
		sessionB := newMockSession()

		client, err := NewCQLClient(sessionA, sessionB)
		require.NoError(t, err)
		defer client.Close()

		ts := int64(1234567890)

		// Execute a write with explicit timestamp
		err = client.Query("INSERT INTO test (id) VALUES (?)", 1).WithTimestamp(ts).Exec()
		require.NoError(t, err)

		// Verify timestamp was set on the underlying query
		require.NotNil(t, sessionA.lastQuery)
		require.NotNil(t, sessionA.lastQuery.timestamp)
		require.Equal(t, ts, *sessionA.lastQuery.timestamp)
	})

	t.Run("multiple configs are propagated together", func(t *testing.T) {
		sessionA := newMockSession()
		sessionB := newMockSession()

		client, err := NewCQLClient(sessionA, sessionB)
		require.NoError(t, err)
		defer client.Close()

		pageToken := []byte{0xAB, 0xCD}

		// Execute a query with multiple configurations
		_ = client.Query("SELECT * FROM test").
			PageSize(50).
			PageState(pageToken).
			Consistency(LocalQuorum).
			Iter()

		// Verify all configs were set
		require.NotNil(t, sessionA.lastQuery)
		require.NotNil(t, sessionA.lastQuery.pageSize)
		require.Equal(t, 50, *sessionA.lastQuery.pageSize)
		require.Equal(t, pageToken, sessionA.lastQuery.pageState)
		require.NotNil(t, sessionA.lastQuery.consistency)
		require.Equal(t, LocalQuorum, *sessionA.lastQuery.consistency)
	})

	t.Run("context is propagated to underlying query", func(t *testing.T) {
		sessionA := newMockSession()
		sessionB := newMockSession()

		client, err := NewCQLClient(sessionA, sessionB)
		require.NoError(t, err)
		defer client.Close()

		type contextKey string
		key := contextKey("test-key")
		ctx := context.WithValue(context.Background(), key, "test-value")

		// Execute a query with context
		err = client.Query("SELECT * FROM test").ExecContext(ctx)
		require.NoError(t, err)

		// Verify context was set on the underlying query
		require.NotNil(t, sessionA.lastQuery)
		require.NotNil(t, sessionA.lastQuery.ctx)
		require.Equal(t, "test-value", sessionA.lastQuery.ctx.Value(key))
	})
}

// mockFailoverPolicy implements FailoverPolicy for testing.
type mockFailoverPolicy struct {
	shouldFailover bool
	failureCount   map[ClusterID]int
	successCount   map[ClusterID]int
}

func newMockFailoverPolicy(shouldFailover bool) *mockFailoverPolicy {
	return &mockFailoverPolicy{
		shouldFailover: shouldFailover,
		failureCount:   make(map[ClusterID]int),
		successCount:   make(map[ClusterID]int),
	}
}

func (p *mockFailoverPolicy) ShouldFailover(_ ClusterID, _ error) bool {
	return p.shouldFailover
}

func (p *mockFailoverPolicy) RecordFailure(cluster ClusterID) {
	p.failureCount[cluster]++
}

func (p *mockFailoverPolicy) RecordSuccess(cluster ClusterID) {
	p.successCount[cluster]++
}

// mockReadStrategy implements ReadStrategy for testing.
type mockReadStrategy struct {
	selectedCluster    ClusterID
	alternativeCluster ClusterID
	allowFailover      bool
	onSuccessCalled    map[ClusterID]int
	onFailureCalled    map[ClusterID]int
}

func newMockReadStrategy(selected, alternative ClusterID, allowFailover bool) *mockReadStrategy {
	return &mockReadStrategy{
		selectedCluster:    selected,
		alternativeCluster: alternative,
		allowFailover:      allowFailover,
		onSuccessCalled:    make(map[ClusterID]int),
		onFailureCalled:    make(map[ClusterID]int),
	}
}

func (s *mockReadStrategy) Select(_ context.Context) ClusterID {
	return s.selectedCluster
}

func (s *mockReadStrategy) OnSuccess(cluster ClusterID) {
	s.onSuccessCalled[cluster]++
}

func (s *mockReadStrategy) OnFailure(cluster ClusterID, _ error) (ClusterID, bool) {
	s.onFailureCalled[cluster]++
	return s.alternativeCluster, s.allowFailover
}

func TestCQLClientFailoverPolicyGatesReadStrategy(t *testing.T) {
	t.Run("policy blocks failover - strategy not consulted for alternative", func(t *testing.T) {
		sessionA := newMockSession()
		sessionA.scanErr = errors.New("cluster A failed")
		sessionB := newMockSession()
		sessionB.scanValues = []any{"from-B"}

		// Policy says NO to failover
		policy := newMockFailoverPolicy(false)
		// Strategy would allow failover if asked
		strategy := newMockReadStrategy(ClusterA, ClusterB, true)

		client, err := NewCQLClient(sessionA, sessionB,
			WithFailoverPolicy(policy),
			WithReadStrategy(strategy),
		)
		require.NoError(t, err)
		defer client.Close()

		var value string
		err = client.Query("SELECT value FROM test").Scan(&value)

		// Should fail - policy blocked failover
		require.Error(t, err)
		require.Equal(t, "cluster A failed", err.Error())

		// Policy recorded the failure
		require.Equal(t, 1, policy.failureCount[ClusterA])

		// Strategy's OnFailure was NOT called because policy blocked it
		require.Equal(t, 0, strategy.onFailureCalled[ClusterA])
	})

	t.Run("policy allows failover - strategy provides alternative", func(t *testing.T) {
		sessionA := newMockSession()
		sessionA.scanErr = errors.New("cluster A failed")
		sessionB := newMockSession()
		sessionB.scanValues = []any{"from-B"}

		// Policy says YES to failover
		policy := newMockFailoverPolicy(true)
		// Strategy provides alternative
		strategy := newMockReadStrategy(ClusterA, ClusterB, true)

		client, err := NewCQLClient(sessionA, sessionB,
			WithFailoverPolicy(policy),
			WithReadStrategy(strategy),
		)
		require.NoError(t, err)
		defer client.Close()

		var value string
		err = client.Query("SELECT value FROM test").Scan(&value)

		// Should succeed via failover to B
		require.NoError(t, err)
		require.Equal(t, "from-B", value)

		// Policy recorded failure on A, success on B
		require.Equal(t, 1, policy.failureCount[ClusterA])
		require.Equal(t, 1, policy.successCount[ClusterB])

		// Strategy's OnFailure was called
		require.Equal(t, 1, strategy.onFailureCalled[ClusterA])
		require.Equal(t, 1, strategy.onSuccessCalled[ClusterB])
	})

	t.Run("no policy configured - failover proceeds based on strategy only", func(t *testing.T) {
		sessionA := newMockSession()
		sessionA.scanErr = errors.New("cluster A failed")
		sessionB := newMockSession()
		sessionB.scanValues = []any{"from-B"}

		// No policy - strategy controls everything
		strategy := newMockReadStrategy(ClusterA, ClusterB, true)

		client, err := NewCQLClient(sessionA, sessionB,
			WithReadStrategy(strategy),
		)
		require.NoError(t, err)
		defer client.Close()

		var value string
		err = client.Query("SELECT value FROM test").Scan(&value)

		// Should succeed via failover
		require.NoError(t, err)
		require.Equal(t, "from-B", value)

		// Strategy was consulted
		require.Equal(t, 1, strategy.onFailureCalled[ClusterA])
	})

	t.Run("policy allows but strategy denies failover", func(t *testing.T) {
		sessionA := newMockSession()
		sessionA.scanErr = errors.New("cluster A failed")
		sessionB := newMockSession()
		sessionB.scanValues = []any{"from-B"}

		// Policy says YES
		policy := newMockFailoverPolicy(true)
		// Strategy says NO (e.g., cooldown active)
		strategy := newMockReadStrategy(ClusterA, ClusterB, false)

		client, err := NewCQLClient(sessionA, sessionB,
			WithFailoverPolicy(policy),
			WithReadStrategy(strategy),
		)
		require.NoError(t, err)
		defer client.Close()

		var value string
		err = client.Query("SELECT value FROM test").Scan(&value)

		// Should fail - strategy denied failover
		require.Error(t, err)
		require.Equal(t, "cluster A failed", err.Error())

		// Both were consulted
		require.Equal(t, 1, policy.failureCount[ClusterA])
		require.Equal(t, 1, strategy.onFailureCalled[ClusterA])
	})

	t.Run("failover from B to A when B is preferred", func(t *testing.T) {
		sessionA := newMockSession()
		sessionA.scanValues = []any{"from-A"}
		sessionB := newMockSession()
		sessionB.scanErr = errors.New("cluster B failed")

		// Policy allows failover
		policy := newMockFailoverPolicy(true)
		// Strategy prefers B, alternative is A
		strategy := newMockReadStrategy(ClusterB, ClusterA, true)

		client, err := NewCQLClient(sessionA, sessionB,
			WithFailoverPolicy(policy),
			WithReadStrategy(strategy),
		)
		require.NoError(t, err)
		defer client.Close()

		var value string
		err = client.Query("SELECT value FROM test").Scan(&value)

		// Should succeed via failover to A
		require.NoError(t, err)
		require.Equal(t, "from-A", value)

		// Policy recorded failure on B, success on A
		require.Equal(t, 1, policy.failureCount[ClusterB])
		require.Equal(t, 1, policy.successCount[ClusterA])

		// Strategy's OnFailure was called for B
		require.Equal(t, 1, strategy.onFailureCalled[ClusterB])
		require.Equal(t, 1, strategy.onSuccessCalled[ClusterA])
	})
}

// TestCQLClient_DrainMode tests the drain mode integration.
func TestCQLClient_DrainMode(t *testing.T) {
	t.Run("writes skip draining cluster A", func(t *testing.T) {
		sessionA := newMockSession()
		sessionB := newMockSession()

		// Create a local topology watcher for testing
		watcher := newMockTopologyWatcher()

		client, err := NewCQLClient(sessionA, sessionB,
			WithTopologyWatcher(watcher),
		)
		require.NoError(t, err)
		defer client.Close()

		// Drain cluster A
		watcher.SetDrain(ClusterA, true)

		// Allow time for the watcher goroutine to process the update
		time.Sleep(50 * time.Millisecond)

		// Execute a write
		err = client.Query("INSERT INTO test (id) VALUES (1)").Exec()
		require.NoError(t, err)

		// Only sessionB should have received the query
		require.Len(t, sessionA.queries, 0)
		require.Len(t, sessionB.queries, 1)
	})

	t.Run("writes skip draining cluster B", func(t *testing.T) {
		sessionA := newMockSession()
		sessionB := newMockSession()

		watcher := newMockTopologyWatcher()

		client, err := NewCQLClient(sessionA, sessionB,
			WithTopologyWatcher(watcher),
		)
		require.NoError(t, err)
		defer client.Close()

		// Drain cluster B
		watcher.SetDrain(ClusterB, true)

		// Allow time for the watcher goroutine to process the update
		time.Sleep(50 * time.Millisecond)

		// Execute a write
		err = client.Query("INSERT INTO test (id) VALUES (1)").Exec()
		require.NoError(t, err)

		// Only sessionA should have received the query
		require.Len(t, sessionA.queries, 1)
		require.Len(t, sessionB.queries, 0)
	})

	t.Run("writes fail when both clusters draining", func(t *testing.T) {
		sessionA := newMockSession()
		sessionB := newMockSession()

		watcher := newMockTopologyWatcher()

		client, err := NewCQLClient(sessionA, sessionB,
			WithTopologyWatcher(watcher),
		)
		require.NoError(t, err)
		defer client.Close()

		// Drain both clusters
		watcher.SetDrain(ClusterA, true)
		watcher.SetDrain(ClusterB, true)

		// Allow time for the watcher goroutine to process the updates
		time.Sleep(50 * time.Millisecond)

		// Execute a write - should fail
		err = client.Query("INSERT INTO test (id) VALUES (1)").Exec()
		require.ErrorIs(t, err, types.ErrBothClustersDraining)

		// Neither session should have received the query
		require.Len(t, sessionA.queries, 0)
		require.Len(t, sessionB.queries, 0)
	})

	t.Run("reads failover away from draining cluster", func(t *testing.T) {
		sessionA := newMockSession()
		sessionA.scanValues = []any{"from-A"}
		sessionB := newMockSession()
		sessionB.scanValues = []any{"from-B"}

		watcher := newMockTopologyWatcher()

		// Strategy prefers A
		strategy := newMockReadStrategy(ClusterA, ClusterB, true)

		client, err := NewCQLClient(sessionA, sessionB,
			WithTopologyWatcher(watcher),
			WithReadStrategy(strategy),
		)
		require.NoError(t, err)
		defer client.Close()

		// Drain cluster A (the preferred one)
		watcher.SetDrain(ClusterA, true)

		// Allow time for the watcher goroutine to process the update
		time.Sleep(50 * time.Millisecond)

		// Execute a read - should go to B
		var value string
		err = client.Query("SELECT value FROM test").Scan(&value)
		require.NoError(t, err)
		require.Equal(t, "from-B", value)

		// sessionA should NOT have the query, sessionB should
		require.Len(t, sessionA.queries, 0)
		require.Len(t, sessionB.queries, 1)
	})

	t.Run("reads use draining cluster if both draining", func(t *testing.T) {
		sessionA := newMockSession()
		sessionA.scanValues = []any{"from-A"}
		sessionB := newMockSession()
		sessionB.scanValues = []any{"from-B"}

		watcher := newMockTopologyWatcher()

		// Strategy prefers A
		strategy := newMockReadStrategy(ClusterA, ClusterB, true)

		client, err := NewCQLClient(sessionA, sessionB,
			WithTopologyWatcher(watcher),
			WithReadStrategy(strategy),
		)
		require.NoError(t, err)
		defer client.Close()

		// Drain both clusters
		watcher.SetDrain(ClusterA, true)
		watcher.SetDrain(ClusterB, true)

		// Allow time for the watcher goroutine to process the updates
		time.Sleep(50 * time.Millisecond)

		// Execute a read - best effort, goes to the original selection (A)
		var value string
		err = client.Query("SELECT value FROM test").Scan(&value)
		require.NoError(t, err)
		require.Equal(t, "from-A", value)

		// sessionA should have the query
		require.Len(t, sessionA.queries, 1)
	})

	t.Run("drain mode cleared resumes normal operation", func(t *testing.T) {
		sessionA := newMockSession()
		sessionB := newMockSession()

		watcher := newMockTopologyWatcher()

		client, err := NewCQLClient(sessionA, sessionB,
			WithTopologyWatcher(watcher),
		)
		require.NoError(t, err)
		defer client.Close()

		// Drain cluster A
		watcher.SetDrain(ClusterA, true)
		time.Sleep(50 * time.Millisecond)

		// Clear drain
		watcher.SetDrain(ClusterA, false)
		time.Sleep(50 * time.Millisecond)

		// Execute a write - both should receive it
		err = client.Query("INSERT INTO test (id) VALUES (1)").Exec()
		require.NoError(t, err)

		// Both sessions should have received the query
		require.Len(t, sessionA.queries, 1)
		require.Len(t, sessionB.queries, 1)
	})
}

// ExampleNewCQLClient demonstrates creating a Helix CQL client with custom strategies.
func ExampleNewCQLClient() {
	// In real code, create proper gocql sessions wrapped with v1.NewSession() or v2.NewSession().
	sessionA := newMockSession()
	sessionB := newMockSession()

	// Create Helix client with custom strategies
	client, err := NewCQLClient(sessionA, sessionB,
		WithReadStrategy(policy.NewStickyRead()),
		WithWriteStrategy(policy.NewConcurrentDualWrite()),
		WithFailoverPolicy(policy.NewActiveFailover()),
	)
	if err != nil {
		// Handle error
		return
	}
	defer client.Close()

	// Client is now ready for dual-cluster operations
	_ = client
}

// ExampleCQLClient_Query demonstrates basic query operations with dual-cluster client.
func ExampleCQLClient_Query() {
	sessionA := newMockSession()
	sessionB := newMockSession()
	client, err := NewCQLClient(sessionA, sessionB)
	if err != nil {
		// Handle error
		return
	}
	defer client.Close()

	// Write operation - triggers dual-write to both clusters
	err = client.Query("INSERT INTO users (id, name) VALUES (?, ?)", "user-1", "Alice").Exec()
	if err != nil {
		// Both clusters failed
		return
	}

	// Read operation - uses sticky read strategy
	var name string
	err = client.Query("SELECT name FROM users WHERE id = ?", "user-1").Scan(&name)
	if err != nil {
		// Read failed (with failover attempted)
		return
	}

	_ = name
}

// Example_errorHandling demonstrates inspecting DualClusterError when both clusters fail.
func Example_errorHandling() {
	sessionA := newMockSession()
	sessionA.execErr = errors.New("cluster A unavailable")
	sessionB := newMockSession()
	sessionB.execErr = errors.New("cluster B unavailable")

	client, err := NewCQLClient(sessionA, sessionB)
	if err != nil {
		// Handle construction error
		return
	}
	defer client.Close()

	// Perform a write operation
	err = client.Query("INSERT INTO users (id, name) VALUES (?, ?)", "user-1", "Alice").Exec()
	// Check if both clusters failed
	if err != nil {
		var dualErr *types.DualClusterError
		if errors.As(err, &dualErr) {
			// Inspect individual cluster errors
			_ = dualErr.ErrorA // Error from cluster A
			_ = dualErr.ErrorB // Error from cluster B
		}

		// Check for specific sentinel errors
		if errors.Is(err, types.ErrSessionClosed) {
			// Session was closed, recreate client or handle gracefully
			return
		}
	}
}

// ExampleWithOnClusterEvent demonstrates registering a cluster event handler
// alongside a component that produces events. Most kinds come from an
// optional component — here the circuit-breaker failover policy produces the
// circuit_breaker_open/_closed kinds — and the constructor logs any kinds
// left unreachable by the rest of the configuration.
func ExampleWithOnClusterEvent() {
	// In real code, create proper gocql sessions wrapped with v1.NewSession() or v2.NewSession().
	sessionA := newMockSession()
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB,
		// circuit_breaker_open / _closed come from the failover policy,
		// which is unset by default.
		WithFailoverPolicy(policy.NewCircuitBreaker()),

		WithOnClusterEvent(func(ev types.ClusterEvent) {
			switch ev.Kind { //nolint:exhaustive // an alerting handler handles only the kinds it pages on
			case types.EventCircuitBreakerOpen:
				// Page: reads are being routed away from ev.Cluster.
				_ = ev.Cluster
			case types.EventFailover:
				// A read failed over from ev.FromCluster to ev.ToCluster.
				_ = ev.FromCluster
			case types.EventReplayDropped:
				// Potential data loss: a failed write could not be
				// enqueued for replay.
				_ = ev.Err
			default:
				// Ignore the kinds this application does not alert on.
			}
		}),
	)
	if err != nil {
		// Handle error
		return
	}
	defer client.Close()

	// The handler now receives events asynchronously; it must not call
	// client.Close() synchronously.
	_ = client
}

// mockWriteStrategy is a configurable WriteStrategy for unit tests.
// It returns the pre-set errA/errB values on every Execute call.
type mockWriteStrategy struct {
	errA, errB error
}

func (s *mockWriteStrategy) Execute(
	ctx context.Context,
	writeA func(context.Context) error,
	writeB func(context.Context) error,
) (errA, errB error) {
	// Execute both writes so the sessions record the call, then replace results.
	_ = writeA(ctx)
	_ = writeB(ctx)
	return s.errA, s.errB
}

// ---------------------------------------------------------------------------
// Tests for executeDualWrite ErrWriteAsync / ErrWriteDropped handling
// ---------------------------------------------------------------------------

// TestExecuteDualWrite_BothAsync verifies that when both clusters are degraded
// and return ErrWriteAsync, the client returns nil (writes in flight) and
// enqueues replay for both clusters — not a DualClusterError.
// requireNoSynchronousAck asserts that err reports a write no cluster
// acknowledged, carrying both leg results, and that the write was admitted
// to the replay queue.
func requireNoSynchronousAck(t *testing.T, err, wantA, wantB error) {
	t.Helper()
	var noAck *types.NoSynchronousAckError
	require.ErrorAs(t, err, &noAck, "a write with no synchronous acknowledgement must be reported")
	require.ErrorIs(t, err, types.ErrNoSynchronousAck)
	require.ErrorIs(t, noAck.ResultA, wantA)
	require.ErrorIs(t, noAck.ResultB, wantB)
	require.NoError(t, noAck.Replay, "the write must have been admitted to the replay queue")
	var dual *types.DualClusterError
	require.False(t, errors.As(err, &dual), "a write that reached the replay queue is not a two-cluster failure")
}

func TestExecuteDualWrite_BothAsync(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	replayer := &mockReplayer{}

	client, err := NewCQLClient(sessionA, sessionB,
		WithWriteStrategy(&mockWriteStrategy{errA: types.ErrWriteAsync, errB: types.ErrWriteAsync}),
		WithReplayer(replayer),
	)
	require.NoError(t, err)
	defer client.Close()

	err = client.Query("INSERT INTO t (id) VALUES (?)", 1).Exec()
	requireNoSynchronousAck(t, err, types.ErrWriteAsync, types.ErrWriteAsync)

	// Both clusters must have replay enqueued
	payloads := replayer.payloads
	require.Len(t, payloads, 2, "replay must be enqueued for both clusters")
	clusters := map[ClusterID]bool{}
	for _, p := range payloads {
		clusters[p.TargetCluster] = true
	}
	require.True(t, clusters[ClusterA], "replay must include cluster A")
	require.True(t, clusters[ClusterB], "replay must include cluster B")
}

// TestExecuteDualWrite_BothDropped verifies that when both clusters are at the
// fire-and-forget concurrency limit, the client enqueues replay for both
// clusters and reports the missing acknowledgement — not a DualClusterError.
func TestExecuteDualWrite_BothDropped(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	replayer := &mockReplayer{}

	client, err := NewCQLClient(sessionA, sessionB,
		WithWriteStrategy(&mockWriteStrategy{errA: types.ErrWriteDropped, errB: types.ErrWriteDropped}),
		WithReplayer(replayer),
	)
	require.NoError(t, err)
	defer client.Close()

	err = client.Query("INSERT INTO t (id) VALUES (?)", 1).Exec()
	requireNoSynchronousAck(t, err, types.ErrWriteDropped, types.ErrWriteDropped)

	require.Len(t, replayer.payloads, 2, "replay must be enqueued for both clusters")
}

// TestExecuteDualWrite_AsyncPlusRealError verifies that one async + one real error
// enqueues replay for both and returns nil (not a DualClusterError).
func TestExecuteDualWrite_AsyncPlusRealError(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	replayer := &mockReplayer{}
	realErr := errors.New("cluster B connection refused")

	client, err := NewCQLClient(sessionA, sessionB,
		WithWriteStrategy(&mockWriteStrategy{errA: types.ErrWriteAsync, errB: realErr}),
		WithReplayer(replayer),
	)
	require.NoError(t, err)
	defer client.Close()

	err = client.Query("INSERT INTO t (id) VALUES (?)", 1).Exec()
	requireNoSynchronousAck(t, err, types.ErrWriteAsync, realErr)

	require.Len(t, replayer.payloads, 2, "replay must be enqueued for both A (async) and B (error)")
}

// TestExecuteDualWrite_DroppedPlusRealError verifies that one dropped + one real error
// enqueues replay for both and reports the missing acknowledgement.
func TestExecuteDualWrite_DroppedPlusRealError(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	replayer := &mockReplayer{}
	realErr := errors.New("cluster B timeout")

	client, err := NewCQLClient(sessionA, sessionB,
		WithWriteStrategy(&mockWriteStrategy{errA: types.ErrWriteDropped, errB: realErr}),
		WithReplayer(replayer),
	)
	require.NoError(t, err)
	defer client.Close()

	err = client.Query("INSERT INTO t (id) VALUES (?)", 1).Exec()
	requireNoSynchronousAck(t, err, types.ErrWriteDropped, realErr)

	require.Len(t, replayer.payloads, 2)
}

// TestExecuteDualWrite_BothRealErrors still returns DualClusterError.
func TestExecuteDualWrite_BothRealErrors(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	errA := errors.New("cluster A failed")
	errB := errors.New("cluster B failed")

	client, err := NewCQLClient(sessionA, sessionB,
		WithWriteStrategy(&mockWriteStrategy{errA: errA, errB: errB}),
	)
	require.NoError(t, err)
	defer client.Close()

	err = client.Query("INSERT INTO t (id) VALUES (?)", 1).Exec()
	require.Error(t, err)
	require.ErrorIs(t, err, types.ErrBothClustersFailed)
}

// TestExecuteDualWrite_PanicInA_JoinsAndEnqueuesReplay is a regression test
// for the default (nil WriteStrategy) dual-write path: a panic on cluster
// A's write — which runs inline on the calling goroutine — must be
// recovered by safeCQLWrite and converted into A's error rather than
// unwinding out of executeDualWrite. Unwinding would skip wg.Wait (leaving
// B's goroutine detached), metrics recording, and the replay-enqueue path
// below. This proves the sibling write (B) is always joined — its query
// reaches the session — and A's panic is classified exactly like any other
// real error: partial success (nil to the caller) with replay enqueued for
// the failed cluster only.
func TestExecuteDualWrite_PanicInA_JoinsAndEnqueuesReplay(t *testing.T) {
	sessionA := newMockSession()
	sessionA.execPanic = "driver exploded"
	sessionB := newMockSession()
	replayer := &mockReplayer{}

	client, err := NewCQLClient(sessionA, sessionB, WithReplayer(replayer))
	require.NoError(t, err)
	defer client.Close()

	err = client.Query("INSERT INTO t (id) VALUES (?)", 1).ExecContext(context.Background())
	require.NoError(t, err, "partial success (A panicked, B succeeded) must not surface as an error")

	require.Len(t, sessionB.queries, 1, "cluster B's write must still run — wg.Wait must join it despite A's panic")

	require.Len(t, replayer.payloads, 1, "replay must be enqueued for the panicking cluster only")
	assert.Equal(t, ClusterA, replayer.payloads[0].TargetCluster)
}

// TestExecuteDualWrite_AsyncDoesNotIncrementWriteError verifies that ErrWriteAsync
// increments IncWriteAsync (not IncWriteError) in the metrics collector.
func TestExecuteDualWrite_AsyncDoesNotIncrementWriteError(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	met := &mockMetricsCollector{}

	client, err := NewCQLClient(sessionA, sessionB,
		WithWriteStrategy(&mockWriteStrategy{errA: types.ErrWriteAsync, errB: nil}),
		WithMetrics(met),
	)
	require.NoError(t, err)
	defer client.Close()

	_ = client.Query("INSERT INTO t (id) VALUES (?)", 1).Exec()

	require.Equal(t, int64(0), met.writeErrors[ClusterA], "ErrWriteAsync must not increment write_errors")
	require.Equal(t, int64(1), met.writeAsync[ClusterA], "ErrWriteAsync must increment write_async")
}

// TestExecuteDualWrite_DroppedDoesNotIncrementWriteError verifies that ErrWriteDropped
// increments IncWriteDropped (not IncWriteError).
func TestExecuteDualWrite_DroppedDoesNotIncrementWriteError(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	met := &mockMetricsCollector{}

	client, err := NewCQLClient(sessionA, sessionB,
		WithWriteStrategy(&mockWriteStrategy{errA: nil, errB: types.ErrWriteDropped}),
		WithMetrics(met),
	)
	require.NoError(t, err)
	defer client.Close()

	_ = client.Query("INSERT INTO t (id) VALUES (?)", 1).Exec()

	require.Equal(t, int64(0), met.writeErrors[ClusterB], "ErrWriteDropped must not increment write_errors")
	require.Equal(t, int64(1), met.writeDropped[ClusterB], "ErrWriteDropped must increment write_dropped")
}

// TestExecuteDualWrite_AsyncEnqueuesReplayForOneCluster verifies that when only
// cluster A is async (B succeeds), replay is enqueued only for A.
func TestExecuteDualWrite_AsyncEnqueuesReplayForOneCluster(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	replayer := &mockReplayer{}

	client, err := NewCQLClient(sessionA, sessionB,
		WithWriteStrategy(&mockWriteStrategy{errA: types.ErrWriteAsync, errB: nil}),
		WithReplayer(replayer),
	)
	require.NoError(t, err)
	defer client.Close()

	err = client.Query("INSERT INTO t (id) VALUES (?)", 1).Exec()
	require.NoError(t, err)

	require.Len(t, replayer.payloads, 1)
	require.Equal(t, ClusterA, replayer.payloads[0].TargetCluster)
}

// TestExecuteDualWrite_DroppedLogsDroppedNotAsync is a regression test for the bug
// where ErrWriteDropped (write never attempted) emitted "write dispatched asynchronously"
// because the log branch checked a single isOperational flag that conflated both states.
// After the fix, each sentinel has its own log message.
func TestExecuteDualWrite_DroppedLogsDroppedNotAsync(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	logger := &captureLogger{}
	replayer := &mockReplayer{}

	client, err := NewCQLClient(sessionA, sessionB,
		WithWriteStrategy(&mockWriteStrategy{errA: types.ErrWriteDropped, errB: nil}),
		WithReplayer(replayer),
		WithLogger(logger),
	)
	require.NoError(t, err)
	defer client.Close()

	err = client.Query("INSERT INTO t (id) VALUES (?)", 1).Exec()
	require.NoError(t, err)

	require.Len(t, replayer.payloads, 1, "dropped write must be enqueued for replay")

	// The info log must say "dropped", not "dispatched asynchronously".
	found := false
	for _, msg := range logger.infoMsgs {
		if strings.Contains(msg, "dropped") {
			found = true
		}
		assert.NotContains(t, msg, "dispatched asynchronously",
			"ErrWriteDropped must not log the async message")
	}
	assert.True(t, found, "expected a log message containing 'dropped'")
}

// TestExecuteDualWrite_AsyncLogIsInfoNotWarn verifies that ErrWriteAsync uses an
// Info log and does not emit the warning reserved for real cluster failures.
func TestExecuteDualWrite_AsyncLogIsInfoNotWarn(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	logger := &captureLogger{}
	replayer := &mockReplayer{}

	client, err := NewCQLClient(sessionA, sessionB,
		WithWriteStrategy(&mockWriteStrategy{errA: types.ErrWriteAsync, errB: nil}),
		WithReplayer(replayer),
		WithLogger(logger),
	)
	require.NoError(t, err)
	defer client.Close()

	err = client.Query("INSERT INTO t (id) VALUES (?)", 1).Exec()
	require.NoError(t, err)

	require.Len(t, replayer.payloads, 1, "async write must be enqueued for replay")
	require.Empty(t, logger.warnMsgs, "ErrWriteAsync must not emit a warning log")

	found := false
	for _, msg := range logger.infoMsgs {
		if strings.Contains(msg, "dispatched asynchronously") {
			found = true
		}
		assert.NotContains(t, msg, "dropped",
			"ErrWriteAsync must not use the dropped-write log message")
	}
	assert.True(t, found, "expected an async info log message")
}

// blockingSession is a cql.Session whose Query().ExecContext blocks until
// release is closed or the context ends, recording entry via entered before
// blocking. Used to
// prove that the default (no WriteStrategy) dual-write path still executes
// both cluster writes concurrently after the alloc-perf fix at
// cql_client.go:1602/:1623 (running writeA inline instead of in its own
// goroutine).
type blockingSession struct {
	entered    chan struct{}
	release    chan struct{}
	releaseErr error // returned by the write once release is closed
}

func newBlockingSession() *blockingSession {
	return &blockingSession{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (s *blockingSession) Query(_ string, _ ...any) cql.Query { return &blockingQuery{session: s} }
func (s *blockingSession) Batch(_ cql.BatchType) cql.Batch    { return nil }
func (s *blockingSession) Close()                             {}

type blockingQuery struct {
	session *blockingSession
}

func (q *blockingQuery) Consistency(_ cql.Consistency) cql.Query       { return q }
func (q *blockingQuery) SerialConsistency(_ cql.Consistency) cql.Query { return q }
func (q *blockingQuery) PageSize(_ int) cql.Query                      { return q }
func (q *blockingQuery) PageState(_ []byte) cql.Query                  { return q }
func (q *blockingQuery) WithTimestamp(_ int64) cql.Query               { return q }
func (q *blockingQuery) Statement() string                             { return "" }
func (q *blockingQuery) Values() []any                                 { return nil }
func (q *blockingQuery) Release()                                      {}
func (q *blockingQuery) Exec() error                                   { return q.ExecContext(context.Background()) }
func (q *blockingQuery) ExecContext(ctx context.Context) error {
	select {
	case q.session.entered <- struct{}{}:
	default:
	}
	select {
	case <-q.session.release:
		return q.session.releaseErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (q *blockingQuery) Scan(_ ...any) error                           { return nil }
func (q *blockingQuery) ScanContext(_ context.Context, _ ...any) error { return nil }
func (q *blockingQuery) Iter() cql.Iter                                { return &mockIter{} }
func (q *blockingQuery) IterContext(_ context.Context) cql.Iter        { return &mockIter{} }
func (q *blockingQuery) MapScan(_ map[string]any) error                { return nil }
func (q *blockingQuery) MapScanContext(_ context.Context, _ map[string]any) error {
	return nil
}
func (q *blockingQuery) ScanCAS(_ ...any) (bool, error) { return false, nil }
func (q *blockingQuery) ScanCASContext(_ context.Context, _ ...any) (bool, error) {
	return false, nil
}
func (q *blockingQuery) MapScanCAS(_ map[string]any) (bool, error) { return false, nil }
func (q *blockingQuery) MapScanCASContext(_ context.Context, _ map[string]any) (bool, error) {
	return false, nil
}

// TestExecuteDualWrite_DefaultPath_BothClustersRunConcurrently is a
// regression test for the cql_client.go:1602/:1623 alloc-perf finding: the
// fix runs writeA inline on the calling goroutine (instead of spawning a
// second goroutine for it) while writeB still runs in a spawned goroutine.
// This must not turn the writes sequential — both must still be in flight
// at the same time. Proven here by blocking both sessions' ExecContext and
// asserting BOTH report entry before either is released.
func TestExecuteDualWrite_DefaultPath_BothClustersRunConcurrently(t *testing.T) {
	sessionA := newBlockingSession()
	sessionB := newBlockingSession()

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)
	defer client.Close()

	done := make(chan error, 1)
	go func() {
		done <- client.Query("INSERT INTO t (id) VALUES (?)", 1).ExecContext(context.Background())
	}()

	// Both sessions must report entry before either is released — if the
	// writes ran sequentially, the second session would never enter until
	// the first is released, and this would time out.
	timeout := time.After(2 * time.Second)
	for range 2 {
		select {
		case <-sessionA.entered:
		case <-sessionB.entered:
		case <-timeout:
			t.Fatal("both clusters' writes must be in flight concurrently — timed out waiting for entry")
		}
	}

	close(sessionA.release)
	close(sessionB.release)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Exec did not return after both sessions were released")
	}
}

// captureLogger records Info/Warn/Error messages, plus Info key-value
// pairs, for assertion in tests.
type captureLogger struct {
	sync.Mutex
	infoMsgs []string
	infoKVs  [][]any
	warnMsgs []string
}

func (l *captureLogger) Debug(_ string, _ ...any) {}
func (l *captureLogger) Info(msg string, keysAndValues ...any) {
	l.Lock()
	defer l.Unlock()
	l.infoMsgs = append(l.infoMsgs, msg)
	l.infoKVs = append(l.infoKVs, keysAndValues)
}

func (l *captureLogger) Warn(msg string, _ ...any) {
	l.Lock()
	defer l.Unlock()
	l.warnMsgs = append(l.warnMsgs, msg)
}
func (l *captureLogger) Error(_ string, _ ...any) {}
func (l *captureLogger) Fatal(_ string, _ ...any) {}

// mockMetricsCollector is a minimal inline metrics collector for write-classification tests.
// It avoids the testutil package import cycle inside the helix package tests.
type mockMetricsCollector struct {
	sync.Mutex
	readTotal    map[ClusterID]int64
	writeErrors  map[ClusterID]int64
	writeAsync   map[ClusterID]int64
	writeDropped map[ClusterID]int64
}

func (m *mockMetricsCollector) inc(mp *map[ClusterID]int64, c ClusterID) {
	m.Lock()
	defer m.Unlock()
	if *mp == nil {
		*mp = make(map[ClusterID]int64)
	}
	(*mp)[c]++
}

func (m *mockMetricsCollector) IncReadTotal(c ClusterID)                     { m.inc(&m.readTotal, c) }
func (m *mockMetricsCollector) IncReadError(_ ClusterID)                     {}
func (m *mockMetricsCollector) ObserveReadDuration(_ ClusterID, _ float64)   {}
func (m *mockMetricsCollector) IncWriteTotal(_ ClusterID)                    {}
func (m *mockMetricsCollector) IncWriteError(c ClusterID)                    { m.inc(&m.writeErrors, c) }
func (m *mockMetricsCollector) IncWriteAsync(c ClusterID)                    { m.inc(&m.writeAsync, c) }
func (m *mockMetricsCollector) IncWriteDropped(c ClusterID)                  { m.inc(&m.writeDropped, c) }
func (m *mockMetricsCollector) ObserveWriteDuration(_ ClusterID, _ float64)  {}
func (m *mockMetricsCollector) IncFailoverTotal(_, _ ClusterID)              {}
func (m *mockMetricsCollector) SetCircuitBreakerState(_ ClusterID, _ int)    {}
func (m *mockMetricsCollector) IncCircuitBreakerTrip(_ ClusterID)            {}
func (m *mockMetricsCollector) IncReplayEnqueued(_ ClusterID)                {}
func (m *mockMetricsCollector) IncReplaySuccess(_ ClusterID)                 {}
func (m *mockMetricsCollector) IncReplayError(_ ClusterID)                   {}
func (m *mockMetricsCollector) IncReplayDropped(_ ClusterID)                 {}
func (m *mockMetricsCollector) SetReplayQueueDepth(_ ClusterID, _ int)       {}
func (m *mockMetricsCollector) ObserveReplayDuration(_ ClusterID, _ float64) {}
func (m *mockMetricsCollector) SetClusterDraining(_ ClusterID, _ bool)       {}
func (m *mockMetricsCollector) IncDrainModeEntered(_ ClusterID)              {}
func (m *mockMetricsCollector) IncDrainModeExited(_ ClusterID)               {}
func (m *mockMetricsCollector) IncReadDivergence(_ ClusterID)                {}

// mockReplayer is a minimal inline replayer for write-classification tests.
type mockReplayer struct {
	sync.Mutex
	payloads []types.ReplayPayload
}

func (r *mockReplayer) Enqueue(_ context.Context, p types.ReplayPayload) error {
	r.Lock()
	defer r.Unlock()
	r.payloads = append(r.payloads, p)
	return nil
}

// Example_contextUsage demonstrates context handling in queries.
func Example_contextUsage() {
	sessionA := newMockSession()
	sessionB := newMockSession()
	client, err := NewCQLClient(sessionA, sessionB)
	if err != nil {
		// Handle construction error
		return
	}
	defer client.Close()

	ctx := context.Background()

	// Use *Context methods to pass context directly.
	err = client.Query("INSERT INTO users (id, name) VALUES (?, ?)", "user-1", "Alice").
		ExecContext(ctx)
	_ = err

	err = client.Query("SELECT name FROM users WHERE id = ?", "user-1").
		Consistency(Quorum).
		ScanContext(ctx)
	_ = err
}

// TestNewCQLClient_TopologyWatcherCleanedUpOnWorkerStartFailure verifies that the
// watchTopology goroutine is stopped when ReplayWorker.Start() fails during
// initialization — i.e., no goroutine is leaked.
func TestNewCQLClient_TopologyWatcherCleanedUpOnWorkerStartFailure(t *testing.T) {
	sessionA := newMockSession()
	watcher := newMockTopologyWatcher()
	workerErr := errors.New("worker already running")
	worker := newMockReplayWorker(workerErr)

	_, err := NewCQLClient(sessionA, nil,
		WithTopologyWatcher(watcher),
		WithReplayWorker(worker),
	)
	require.ErrorIs(t, err, workerErr)

	// The topology watcher's Watch goroutine should have been unblocked by the
	// context cancellation. Give it a moment to propagate.
	require.Eventually(t, func() bool {
		watcher.mu.RLock()
		defer watcher.mu.RUnlock()
		return watcher.closed
	}, time.Second, 10*time.Millisecond, "topology watcher context should be canceled on init failure")
}

// TestNewCQLClient_MirrorComponentsStoppedOnReplayWorkerStartFailure verifies
// that the mirror engine and mirror replay worker started by setupMirror are
// stopped — not leaked — when the later config.ReplayWorker.Start() call
// fails during initialization. Regression test: this error path previously
// only unwound the topology watcher, leaking the mirror engine's worker
// goroutines and the mirror replay worker whenever both mirroring and a
// ReplayWorker were configured together.
func TestNewCQLClient_MirrorComponentsStoppedOnReplayWorkerStartFailure(t *testing.T) {
	mirrorTarget, err := NewCQLClient(newMockSession(), nil)
	require.NoError(t, err)
	defer mirrorTarget.Close()

	mirrorReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(16))
	workerErr := errors.New("worker already running")

	client, err := buildCQLClient(newMockSession(), nil,
		WithMirror(mirrorTarget),
		WithMirrorReplayer(mirrorReplayer),
		WithReplayWorker(newMockReplayWorker(workerErr)),
	)
	require.ErrorIs(t, err, workerErr)
	require.NotNil(t, client)

	require.NotNil(t, client.runtime.mirrorEngine)
	require.False(t, client.runtime.mirrorEngine.TryEnqueue(types.ReplayPayload{Query: "after failed construction"}),
		"mirror engine must be stopped, not leaked, when ReplayWorker.Start() fails")

	require.NotNil(t, client.runtime.mirrorReplayWorker)
	require.False(t, client.runtime.mirrorReplayWorker.IsRunning(),
		"mirror replay worker must be stopped, not leaked, when ReplayWorker.Start() fails")
}

// TestNewCQLClient_MirrorPublisherEngineStoppedOnReplayWorkerStartFailure
// verifies the same leak-on-failure guard for publisher-mode mirroring
// (WithMirrorPublisher): setupMirrorPublisherMode only ever builds the
// mirror engine (there is no auto-built mirror replay worker in this
// mode), so stopMirrorComponents must still stop that engine when the later
// config.ReplayWorker.Start() call fails during initialization.
func TestNewCQLClient_MirrorPublisherEngineStoppedOnReplayWorkerStartFailure(t *testing.T) {
	pub := &recordingReplayer{}
	workerErr := errors.New("worker already running")

	client, err := buildCQLClient(newMockSession(), nil,
		WithMirrorPublisher(pub),
		WithReplayWorker(newMockReplayWorker(workerErr)),
	)
	require.ErrorIs(t, err, workerErr)
	require.NotNil(t, client)

	require.NotNil(t, client.runtime.mirrorEngine)
	require.False(t, client.runtime.mirrorEngine.TryEnqueue(types.ReplayPayload{Query: "after failed construction"}),
		"publisher-mode mirror engine must be stopped, not leaked, when ReplayWorker.Start() fails")
	require.Nil(t, client.runtime.mirrorReplayWorker,
		"publisher mode never sets the mirror replay worker; stopMirrorComponents must nil-guard it")
}

// Compile-time assertion: errorIter implements Iter.
var _ Iter = (*errorIter)(nil)

// --- AllowedClusters override tests ---

func TestAllowedClusters_SingleClusterRouting(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	strategy := newMockReadStrategy(ClusterA, ClusterB, true)

	client, err := NewCQLClient(sessionA, sessionB,
		WithReadStrategy(strategy),
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterB}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	var result string
	_ = client.Query("SELECT name FROM users").Scan(&result)
	// Reads should route to B, not strategy's A
	assert.Equal(t, 1, len(sessionB.queries), "query should route to cluster B")
	assert.Equal(t, 0, len(sessionA.queries), "query should NOT route to cluster A")
	// Strategy should not have been called for OnSuccess (frozen)
	assert.Equal(t, 0, strategy.onSuccessCalled[ClusterB], "OnSuccess should NOT be called during override")
}

func TestAllowedClusters_SingleClusterReadFails_NoFailover(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	sessionB.scanErr = errors.New("cluster B unavailable")

	client, err := NewCQLClient(sessionA, sessionB,
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterB}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	var result string
	err = client.Query("SELECT name FROM users").Scan(&result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cluster B unavailable")
	// No failover — only B in the list
	assert.Equal(t, 0, len(sessionA.queries))
}

func TestAllowedClusters_TwoClusters_PrimaryFails_FailoverToSecond(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	sessionB.scanErr = errors.New("cluster B down")
	// A should succeed on failover
	sessionA.scanErr = nil

	client, err := NewCQLClient(sessionA, sessionB,
		WithFailoverPolicy(newMockFailoverPolicy(true)),
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterB, ClusterA}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	var result string
	err = client.Query("SELECT name FROM users").Scan(&result)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(sessionB.queries), "should try B first")
	assert.Equal(t, 1, len(sessionA.queries), "should failover to A")
}

func TestAllowedClusters_NilReturn_NormalBehavior(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	strategy := newMockReadStrategy(ClusterA, ClusterB, true)

	client, err := NewCQLClient(sessionA, sessionB,
		WithReadStrategy(strategy),
		WithAllowedClusters(func() []ClusterID {
			return nil
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	var result string
	_ = client.Query("SELECT name FROM users").Scan(&result)
	// Strategy selects A normally
	assert.Equal(t, 1, len(sessionA.queries), "should use strategy's selection (A)")
	assert.Equal(t, 1, strategy.onSuccessCalled[ClusterA], "OnSuccess should be called normally")
}

func TestAllowedClusters_EmptySlice_NormalBehavior(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	strategy := newMockReadStrategy(ClusterA, ClusterB, true)

	client, err := NewCQLClient(sessionA, sessionB,
		WithReadStrategy(strategy),
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	var result string
	_ = client.Query("SELECT name FROM users").Scan(&result)
	assert.Equal(t, 1, len(sessionA.queries), "empty slice = no override, normal behavior")
	assert.Equal(t, 1, strategy.onSuccessCalled[ClusterA])
}

func TestAllowedClusters_ToggleOverride(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	var overrideActive atomic.Bool
	overrideActive.Store(true)

	strategy := newMockReadStrategy(ClusterA, ClusterB, true)

	client, err := NewCQLClient(sessionA, sessionB,
		WithReadStrategy(strategy),
		WithAllowedClusters(func() []ClusterID {
			if overrideActive.Load() {
				return []ClusterID{ClusterB}
			}
			return nil
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	// First read: override active, routes to B
	var result string
	_ = client.Query("SELECT name FROM users").Scan(&result)
	assert.Equal(t, 1, len(sessionB.queries))
	assert.Equal(t, 0, len(sessionA.queries))

	// Remove override
	overrideActive.Store(false)

	// Second read: no override, strategy selects A
	_ = client.Query("SELECT name FROM users").Scan(&result)
	assert.Equal(t, 1, len(sessionA.queries))
}

func TestAllowedClusters_FailoverPolicyDenies(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	sessionB.scanErr = errors.New("cluster B down")

	policy := newMockFailoverPolicy(false) // deny failover

	client, err := NewCQLClient(sessionA, sessionB,
		WithFailoverPolicy(policy),
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterB, ClusterA}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	var result string
	err = client.Query("SELECT name FROM users").Scan(&result)
	assert.Error(t, err)
	// CB blocked failover even though A is in the override list
	assert.Equal(t, 0, len(sessionA.queries), "failover policy should block failover to A")
}

func TestAllowedClusters_DrainOnAllowedCluster(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB,
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterB, ClusterA}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	// Set A to draining
	client.drainA.Store(true)

	var result string
	_ = client.Query("SELECT name FROM users").Scan(&result)
	// B is primary (first non-draining), A is filtered by drain
	assert.Equal(t, 1, len(sessionB.queries))
	assert.Equal(t, 0, len(sessionA.queries))
}

func TestAllowedClusters_DrainConflict_NoValidClusters(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB,
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterA}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	// A is draining but override says only A
	client.drainA.Store(true)

	var result string
	err = client.Query("SELECT name FROM users").Scan(&result)
	assert.ErrorIs(t, err, types.ErrNoValidClusters)
}

func TestAllowedClusters_StrategyStateFrozen(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	strategy := newMockReadStrategy(ClusterA, ClusterB, true)

	client, err := NewCQLClient(sessionA, sessionB,
		WithReadStrategy(strategy),
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterB}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	// Successful read — strategy.OnSuccess should NOT be called
	var result string
	_ = client.Query("SELECT name FROM users").Scan(&result)
	assert.Equal(t, 0, strategy.onSuccessCalled[ClusterB], "OnSuccess must be frozen during override")
}

func TestAllowedClusters_OverrideRemoved_StrategyResumes(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	var overrideActive atomic.Bool
	overrideActive.Store(true)

	strategy := newMockReadStrategy(ClusterA, ClusterB, true)

	client, err := NewCQLClient(sessionA, sessionB,
		WithReadStrategy(strategy),
		WithAllowedClusters(func() []ClusterID {
			if overrideActive.Load() {
				return []ClusterID{ClusterB}
			}
			return nil
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	// Override active: frozen
	var result string
	_ = client.Query("SELECT name FROM users").Scan(&result)
	assert.Equal(t, 0, strategy.onSuccessCalled[ClusterB])

	// Remove override
	overrideActive.Store(false)

	// Normal: strategy should be called again
	_ = client.Query("SELECT name FROM users").Scan(&result)
	assert.Equal(t, 1, strategy.onSuccessCalled[ClusterA])
}

func TestAllowedClusters_IterContext_OverrideApplied(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	strategy := newMockReadStrategy(ClusterA, ClusterB, true)

	client, err := NewCQLClient(sessionA, sessionB,
		WithReadStrategy(strategy),
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterB}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	iter := client.Query("SELECT name FROM users").Iter()
	err = iter.Close()
	assert.NoError(t, err)

	// Should have gone to B
	assert.Equal(t, 1, len(sessionB.queries))
	assert.Equal(t, 0, len(sessionA.queries))

	// Strategy.OnSuccess should NOT be called (frozen during override)
	assert.Equal(t, 0, strategy.onSuccessCalled[ClusterB])
}

func TestAllowedClusters_CAS_NotOverridden(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	// Strategy selects A — CAS should follow strategy, not override
	strategy := newMockReadStrategy(ClusterA, ClusterB, true)

	client, err := NewCQLClient(sessionA, sessionB,
		WithReadStrategy(strategy),
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterB} // Override says B only
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	// CAS uses selectClusterForCAS which follows strategy → A
	_, _ = client.Query("INSERT INTO users (name) VALUES (?) IF NOT EXISTS", "test").ScanCAS()
	assert.Equal(t, 1, len(sessionA.queries), "CAS should go to strategy-selected cluster, not override")
	assert.Equal(t, 0, len(sessionB.queries), "CAS should NOT be affected by override")
}

func TestAllowedClusters_FallbackRead_Fenced(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	// B returns not-found
	sessionB.scanErr = types.ErrNotFound

	client, err := NewCQLClient(sessionA, sessionB,
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterB} // only B; A excluded
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	var result string
	err = client.Query("SELECT name FROM users").FallbackRead().Scan(&result)
	assert.ErrorIs(t, err, types.ErrNotFound)
	// A should NOT be probed — it's excluded by override
	assert.Equal(t, 0, len(sessionA.queries), "fallback must NOT probe fenced cluster")
}

func TestAllowedClusters_FallbackRead_AllowedWhenBothInList(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	// B returns not-found, A has the data
	sessionB.scanErr = types.ErrNotFound
	sessionA.scanErr = nil

	client, err := NewCQLClient(sessionA, sessionB,
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterB, ClusterA}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	var result string
	err = client.Query("SELECT name FROM users").FallbackRead().Scan(&result)
	assert.NoError(t, err)
	// A should be probed because it's in the allowed list
	assert.Equal(t, 1, len(sessionA.queries), "fallback should probe A since it's in the allowed list")
}

func TestAllowedClusters_UnknownClusterID_FailClosed(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB,
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{"X", "Y"}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	var result string
	err = client.Query("SELECT name FROM users").Scan(&result)
	assert.ErrorIs(t, err, types.ErrInvalidClusterOverride)
}

func TestAllowedClusters_Panic_FailClosed(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB,
		WithAllowedClusters(func() []ClusterID {
			panic("provider crashed")
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	var result string
	err = client.Query("SELECT name FROM users").Scan(&result)
	assert.ErrorIs(t, err, types.ErrClusterOverridePanic)
}

func TestAllowedClusters_IteratorCloseDoesNotCallOnSuccessDuringOverride(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	strategy := newMockReadStrategy(ClusterA, ClusterB, true)

	client, err := NewCQLClient(sessionA, sessionB,
		WithReadStrategy(strategy),
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterB}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	iter := client.Query("SELECT name FROM users").Iter()
	_ = iter.Close()

	assert.Equal(t, 0, strategy.onSuccessCalled[ClusterB],
		"iterator Close must not call OnSuccess during override")
}

func TestAllowedClusters_IteratorSnapshotConsistency(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	var overrideActive atomic.Bool
	overrideActive.Store(true)

	strategy := newMockReadStrategy(ClusterA, ClusterB, true)

	client, err := NewCQLClient(sessionA, sessionB,
		WithReadStrategy(strategy),
		WithAllowedClusters(func() []ClusterID {
			if overrideActive.Load() {
				return []ClusterID{ClusterB}
			}
			return nil
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	// Create iterator under override
	iter := client.Query("SELECT name FROM users").Iter()

	// Remove override AFTER iterator creation
	overrideActive.Store(false)

	// Close should still use the snapshot (override was active at creation)
	_ = iter.Close()
	assert.Equal(t, 0, strategy.onSuccessCalled[ClusterB],
		"OnSuccess must not leak when override removed between creation and Close")
}

func TestAllowedClusters_SingleClusterMode_ClusterB_FailClosed(t *testing.T) {
	sessionA := newMockSession()
	// Single-cluster mode: no sessionB

	client, err := NewCQLClient(sessionA, nil,
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterB}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	var result string
	err = client.Query("SELECT name FROM users").Scan(&result)
	assert.ErrorIs(t, err, types.ErrInvalidClusterOverride)
}

func TestAllowedClusters_DuplicateEntries_Deduped(t *testing.T) {
	t.Run("[B,B,A] resolves to primary=B fallback=A", func(t *testing.T) {
		sessionA := newMockSession()
		sessionB := newMockSession()
		sessionB.scanErr = errors.New("cluster B down")

		client, err := NewCQLClient(sessionA, sessionB,
			WithFailoverPolicy(newMockFailoverPolicy(true)),
			WithAllowedClusters(func() []ClusterID {
				return []ClusterID{ClusterB, ClusterB, ClusterA}
			}),
		)
		require.NoError(t, err)
		defer client.Close()

		var result string
		err = client.Query("SELECT name FROM users").Scan(&result)
		// B fails, failover to A (deduped fallback)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(sessionA.queries), "should failover to A")
	})

	t.Run("[B,B] resolves to primary=B no fallback", func(t *testing.T) {
		sessionA := newMockSession()
		sessionB := newMockSession()
		sessionB.scanErr = errors.New("cluster B down")

		client, err := NewCQLClient(sessionA, sessionB,
			WithAllowedClusters(func() []ClusterID {
				return []ClusterID{ClusterB, ClusterB}
			}),
		)
		require.NoError(t, err)
		defer client.Close()

		var result string
		err = client.Query("SELECT name FROM users").Scan(&result)
		assert.Error(t, err)
		// No fallback — deduped to just B
		assert.Equal(t, 0, len(sessionA.queries), "no fallback should be attempted")
	})
}

func TestAllowedClusters_IterContext_ErrorIter(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB,
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{"unknown"}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	iter := client.Query("SELECT name FROM users").Iter()

	// Scan returns false
	var result string
	assert.False(t, iter.Scan(&result), "errorIter.Scan should return false")

	// Close returns the error
	err = iter.Close()
	assert.ErrorIs(t, err, types.ErrInvalidClusterOverride)

	// SliceMap returns the error
	iter2 := client.Query("SELECT name FROM users").Iter()
	_, sliceErr := iter2.SliceMap()
	assert.ErrorIs(t, sliceErr, types.ErrInvalidClusterOverride)

	// Metadata methods return zero values
	iter3 := client.Query("SELECT name FROM users").Iter()
	assert.Nil(t, iter3.PageState())
	assert.Equal(t, 0, iter3.NumRows())
	assert.Nil(t, iter3.Columns())
	scanner := iter3.Scanner()
	assert.NotNil(t, scanner, "errorIter.Scanner should return a non-nil errorScanner")
	assert.False(t, scanner.Next(), "errorScanner.Next should return false")
	assert.ErrorIs(t, scanner.Err(), types.ErrInvalidClusterOverride)
	assert.Nil(t, iter3.Warnings())
	_ = iter3.Close()
}

func TestAllowedClusters_DrainFiltersWithinOverride(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB,
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterA, ClusterB}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	// A is draining; override says [A, B]; effective = B only
	client.drainA.Store(true)

	var result string
	_ = client.Query("SELECT name FROM users").Scan(&result)
	assert.Equal(t, 1, len(sessionB.queries), "drain should filter A within override")
	assert.Equal(t, 0, len(sessionA.queries))
}

func TestAllowedClusters_BatchIterContext_OverrideApplied(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	strategy := newMockReadStrategy(ClusterA, ClusterB, true)

	client, err := NewCQLClient(sessionA, sessionB,
		WithReadStrategy(strategy),
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterB}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	batch := client.Batch(UnloggedBatch)
	batch.Query("INSERT INTO t (id) VALUES (?)", 1)
	iter := batch.IterContext(context.Background())
	err = iter.Close()
	assert.NoError(t, err)
	assert.Equal(t, 0, strategy.onSuccessCalled[ClusterB],
		"batch IterContext should freeze strategy during override")
}

func TestAllowedClusters_BatchIterContext_ErrorIter(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB,
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{"unknown"}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	batch := client.Batch(UnloggedBatch)
	batch.Query("INSERT INTO t (id) VALUES (?)", 1)
	iter := batch.IterContext(context.Background())
	err = iter.Close()
	assert.ErrorIs(t, err, types.ErrInvalidClusterOverride)
}

func TestAllowedClusters_NoReadStrategy_OverrideStillApplies(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	// No ReadStrategy configured
	client, err := NewCQLClient(sessionA, sessionB,
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterB}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	var result string
	_ = client.Query("SELECT name FROM users").Scan(&result)
	assert.Equal(t, 1, len(sessionB.queries), "override should apply even without ReadStrategy")
	assert.Equal(t, 0, len(sessionA.queries))
}

func TestAllowedClusters_FallbackReadSnapshotConsistency(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	sessionB.scanErr = types.ErrNotFound

	var overrideActive atomic.Bool
	overrideActive.Store(true)

	client, err := NewCQLClient(sessionA, sessionB,
		WithAllowedClusters(func() []ClusterID {
			if overrideActive.Load() {
				return []ClusterID{ClusterB} // only B; A excluded
			}
			return nil
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	// Override active: A is fenced. Even if override is removed mid-request,
	// the snapshot ensures A stays fenced.
	var result string
	err = client.Query("SELECT name FROM users").FallbackRead().Scan(&result)
	assert.ErrorIs(t, err, types.ErrNotFound)
	assert.Equal(t, 0, len(sessionA.queries), "snapshot should prevent A probe even if override changes")
}

func TestAllowedClusters_SingleClusterMode_ClusterA_Works(t *testing.T) {
	sessionA := newMockSession()

	client, err := NewCQLClient(sessionA, nil,
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterA}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	var result string
	_ = client.Query("SELECT name FROM users").Scan(&result)
	assert.Equal(t, 1, len(sessionA.queries))
}

func TestAllowedClusters_SingleCluster_NilReturn_NoPhantomClusterB(t *testing.T) {
	sessionA := newMockSession()

	strategy := newMockReadStrategy(ClusterA, ClusterB, true)

	client, err := NewCQLClient(sessionA, nil,
		WithReadStrategy(strategy),
		WithAllowedClusters(func() []ClusterID {
			return nil // opt-out
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	var result string
	_ = client.Query("SELECT name FROM users").Scan(&result)
	// Must route to A (single session) with exactly 1 query
	assert.Equal(t, 1, len(sessionA.queries))
	// In single-cluster mode the strategy is not consulted for OnSuccess
	// since there is only one session — the fast path skips strategy calls.
}

func TestAllowedClusters_SingleCluster_EmptyReturn_NoPhantomClusterB(t *testing.T) {
	sessionA := newMockSession()

	client, err := NewCQLClient(sessionA, nil,
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	var result string
	_ = client.Query("SELECT name FROM users").Scan(&result)
	assert.Equal(t, 1, len(sessionA.queries))
}

func TestAllowedClusters_SingleCluster_NilReturn_ErrorDoesNotFailover(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = errors.New("cluster A failed")

	client, err := NewCQLClient(sessionA, nil,
		WithReadStrategy(newMockReadStrategy(ClusterA, ClusterB, true)),
		WithFailoverPolicy(newMockFailoverPolicy(true)),
		WithAllowedClusters(func() []ClusterID {
			return nil
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	var result string
	err = client.Query("SELECT name FROM users").Scan(&result)
	assert.Error(t, err)
	// Must NOT attempt failover to ClusterB — there is only one session.
	// The query count should be exactly 1: the single failed attempt on A.
	assert.Equal(t, 1, len(sessionA.queries),
		"single-cluster mode must not retry via failover when override returns nil")
}

func TestAllowedClusters_SingleCluster_NilReturn_FallbackReadSuppressed(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound

	client, err := NewCQLClient(sessionA, nil,
		WithAllowedClusters(func() []ClusterID {
			return nil
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	var result string
	err = client.Query("SELECT name FROM users").FallbackRead().Scan(&result)
	assert.ErrorIs(t, err, types.ErrNotFound)
	// FallbackRead must be suppressed in single-cluster mode:
	// only one query should execute (no phantom probe on ClusterB).
	assert.Equal(t, 1, len(sessionA.queries),
		"FallbackRead must not probe a phantom ClusterB in single-cluster mode")
}

func BenchmarkResolveReadTarget_OverrideActive(b *testing.B) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB,
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterB, ClusterA}
		}),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		rt := client.resolveReadTarget(ctx, readOptions{})
		if rt.err != nil {
			b.Fatal(rt.err)
		}
	}
}

func BenchmarkResolveReadTarget_NoOverride(b *testing.B) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB,
		WithReadStrategy(newMockReadStrategy(ClusterA, ClusterB, true)),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		rt := client.resolveReadTarget(ctx, readOptions{})
		if rt.err != nil {
			b.Fatal(rt.err)
		}
	}
}
