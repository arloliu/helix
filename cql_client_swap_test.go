package helix_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/types"
)

// Tests for CQLClient.SwapSession and CQLClient.RefreshSession.
//
// The session-swap feature replaces the live cql.Session for a given cluster
// at runtime so a Helix client can recover from a permanently-broken
// underlying session (e.g., the cluster restarted at a different endpoint
// and the gocql driver cannot reconnect) without tearing down the entire
// CQLClient — preserving the topology watcher, replay worker, and any
// references the application holds.

// ----- SwapSession ---------------------------------------------------------

func TestSwapSession_RoutesNewQueriesToNewSession(t *testing.T) {
	mockA, mockB := newAlwaysOKMock(), newAlwaysOKMock()
	client := newDualClusterTestClient(t, mockA, mockB)

	// Sanity: a write goes to A under SyncDualWrite (default config writes
	// to both, but a round-trip is enough to know A is in the loop).
	require.NoError(t, client.Query("INSERT INTO t (k,v) VALUES (?, ?)", 1, "v").
		ExecContext(context.Background()))
	require.NotEmpty(t, mockA.queries, "pre-swap write must reach mockA")
	preCount := len(mockA.queries)

	// Swap A.
	newMockA := newAlwaysOKMock()
	old, err := client.SwapSession(helix.ClusterA, newMockA)
	require.NoError(t, err)
	assert.Same(t, mockA, old, "Swap must return the previously installed session")

	// Next write must go to newMockA, not mockA.
	require.NoError(t, client.Query("INSERT INTO t (k,v) VALUES (?, ?)", 2, "v").
		ExecContext(context.Background()))
	assert.Equal(t, preCount, len(mockA.queries),
		"old session must NOT receive the post-swap write")
	assert.NotEmpty(t, newMockA.queries, "new session must receive the post-swap write")
}

func TestSwapSession_DoesNotCloseOldSession(t *testing.T) {
	mockA, mockB := newAlwaysOKMock(), newAlwaysOKMock()
	client := newDualClusterTestClient(t, mockA, mockB)

	old, err := client.SwapSession(helix.ClusterA, newAlwaysOKMock())
	require.NoError(t, err)
	oldMock, ok := old.(*alwaysOKMock)
	require.True(t, ok, "old session is the originally-installed mock")
	assert.False(t, oldMock.closed.Load(),
		"SwapSession must NOT close the returned old session")

	// Caller closes explicitly.
	old.Close()
	assert.True(t, oldMock.closed.Load(),
		"Old session must be closeable by the caller")
}

func TestSwapSession_CloseAfterSwapClosesNewSession(t *testing.T) {
	mockA, mockB := newAlwaysOKMock(), newAlwaysOKMock()
	client := newDualClusterTestClient(t, mockA, mockB)

	newMockA := newAlwaysOKMock()
	old, err := client.SwapSession(helix.ClusterA, newMockA)
	require.NoError(t, err)
	_ = old

	client.Close()
	assert.True(t, newMockA.closed.Load(),
		"client.Close after swap must close the NEW session (current live)")
	assert.False(t, mockA.closed.Load(),
		"client.Close after swap must NOT close the swapped-out session — that is the caller's responsibility")
	assert.True(t, mockB.closed.Load(), "B closed as usual")
}

func TestSwapSession_RejectsNilSession(t *testing.T) {
	client := newDualClusterTestClient(t, newAlwaysOKMock(), newAlwaysOKMock())
	_, err := client.SwapSession(helix.ClusterA, nil)
	assert.ErrorIs(t, err, types.ErrNilSession)
}

func TestSwapSession_RejectsClosedClient(t *testing.T) {
	client := newDualClusterTestClient(t, newAlwaysOKMock(), newAlwaysOKMock())
	client.Close()
	_, err := client.SwapSession(helix.ClusterA, newAlwaysOKMock())
	assert.ErrorIs(t, err, types.ErrSessionClosed)
}

func TestSwapSession_RejectsClusterBOnSingleCluster(t *testing.T) {
	client, err := helix.NewCQLClient(newAlwaysOKMock(), nil)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	_, err = client.SwapSession(helix.ClusterB, newAlwaysOKMock())
	assert.ErrorIs(t, err, types.ErrInvalidCluster,
		"single-cluster clients must reject SwapSession(ClusterB, ...)")
}

func TestSwapSession_RejectsUnknownClusterID(t *testing.T) {
	client := newDualClusterTestClient(t, newAlwaysOKMock(), newAlwaysOKMock())
	_, err := client.SwapSession(helix.ClusterID("Z"), newAlwaysOKMock())
	assert.ErrorIs(t, err, types.ErrInvalidCluster)
}

func TestSwapSession_AllowsDifferentConcreteTypes(t *testing.T) {
	// alwaysOKMock and wrappedAlwaysOKMock are distinct dynamic types both
	// implementing cql.Session. atomic.Value would panic on the second
	// Store — the wrapper-struct + atomic.Pointer choice is what makes
	// this work.
	plain := newAlwaysOKMock()
	wrapped := &wrappedAlwaysOKMock{inner: newAlwaysOKMock()}

	client := newDualClusterTestClient(t, plain, newAlwaysOKMock())

	old, err := client.SwapSession(helix.ClusterA, wrapped)
	require.NoError(t, err)
	assert.Same(t, plain, old)

	// Swap back to a plain (different concrete type than wrapped).
	old2, err := client.SwapSession(helix.ClusterA, newAlwaysOKMock())
	require.NoError(t, err)
	assert.Same(t, wrapped, old2)
}

func TestSwapSession_PublicGetterReflectsSwap(t *testing.T) {
	mockA, mockB := newAlwaysOKMock(), newAlwaysOKMock()
	client := newDualClusterTestClient(t, mockA, mockB)

	assert.Same(t, mockA, client.SessionA(), "SessionA returns the live session pre-swap")

	newMockA := newAlwaysOKMock()
	_, err := client.SwapSession(helix.ClusterA, newMockA)
	require.NoError(t, err)
	assert.Same(t, newMockA, client.SessionA(),
		"SessionA returns the live session post-swap (callers who cache the prior return have a stale ref)")
}

// ----- RefreshSession ------------------------------------------------------

func TestRefreshSession_InvokesRefresherAndClosesOld(t *testing.T) {
	mockA, mockB := newAlwaysOKMock(), newAlwaysOKMock()
	newMockA := newAlwaysOKMock()

	var calls atomic.Int32
	refresher := func(_ context.Context, cluster helix.ClusterID, lastErr error) (cql.Session, error) {
		calls.Add(1)
		assert.Equal(t, helix.ClusterA, cluster)
		// No prior op failure has been recorded (this test calls
		// RefreshSession directly without driving any failing ops first),
		// so lastErr is nil. The "lastErr always nil" guarantee from v1
		// no longer holds in v2: see TestRefreshSession_LastErrThreadedFromObservedFailure.
		assert.Nil(t, lastErr, "no failure recorded before this RefreshSession; lastErr is nil")

		return newMockA, nil
	}

	client, err := helix.NewCQLClient(mockA, mockB, helix.WithSessionRefresher(refresher))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.NoError(t, client.RefreshSession(context.Background(), helix.ClusterA))
	assert.EqualValues(t, 1, calls.Load(), "refresher invoked exactly once")
	assert.Same(t, newMockA, client.SessionA(), "new session installed")
	assert.True(t, mockA.closed.Load(),
		"RefreshSession closes the old session on the caller's behalf (refresh contract)")
}

// TestRefreshSession_LastErrThreadedFromObservedFailure verifies the
// v2 lastErr threading: when ops have failed against a cluster before
// RefreshSession is called, the most recently observed failure is
// surfaced to the SessionRefresher's lastErr parameter.
func TestRefreshSession_LastErrThreadedFromObservedFailure(t *testing.T) {
	failErr := errors.New("simulated network partition")
	mockA := newAlwaysFailSession(failErr)
	mockB := newAlwaysOKMock()

	var capturedLastErr error
	refresher := func(_ context.Context, _ helix.ClusterID, lastErr error) (cql.Session, error) {
		capturedLastErr = lastErr
		return newAlwaysOKMock(), nil
	}

	client, err := helix.NewCQLClient(mockA, mockB,
		helix.WithSessionRefresher(refresher),
		helix.WithWriteStrategy(policy.NewSyncDualWrite()),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	// Drive a write so cluster A records the failure via recordOpOutcome.
	// SyncDualWrite under partial failure (A=err, B=ok) returns nil to
	// the caller per the documented contract, but recordOpOutcome runs
	// per-cluster so A's lastErr is updated.
	require.NoError(t,
		client.Query("INSERT INTO t (k,v) VALUES (?, ?)", 1, "v").
			ExecContext(context.Background()),
		"partial-success write returns nil to caller (B succeeded)")

	require.NoError(t, client.RefreshSession(context.Background(), helix.ClusterA))
	assert.ErrorIs(t, capturedLastErr, failErr,
		"refresher must receive the most recent observed failure as lastErr")
}

func TestRefreshSession_NoRefresherConfigured(t *testing.T) {
	client := newDualClusterTestClient(t, newAlwaysOKMock(), newAlwaysOKMock())
	err := client.RefreshSession(context.Background(), helix.ClusterA)
	assert.ErrorIs(t, err, types.ErrNoSessionRefresher)
}

func TestRefreshSession_RefresherErrorsPropagated(t *testing.T) {
	mockA, mockB := newAlwaysOKMock(), newAlwaysOKMock()
	rebuildErr := errors.New("rebuild failed")
	refresher := func(_ context.Context, _ helix.ClusterID, _ error) (cql.Session, error) {
		return nil, rebuildErr
	}

	client, err := helix.NewCQLClient(mockA, mockB, helix.WithSessionRefresher(refresher))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	err = client.RefreshSession(context.Background(), helix.ClusterA)
	assert.ErrorIs(t, err, rebuildErr,
		"refresher error must be wrapped and returned")
	assert.Same(t, mockA, client.SessionA(),
		"failed refresh must NOT swap — old session remains")
	assert.False(t, mockA.closed.Load(),
		"failed refresh must NOT close the old session")
}

func TestRefreshSession_RefresherReturnsNil(t *testing.T) {
	mockA, mockB := newAlwaysOKMock(), newAlwaysOKMock()
	refresher := func(_ context.Context, _ helix.ClusterID, _ error) (cql.Session, error) {
		// Test the (nil, nil) corner case explicitly — Helix must reject
		// it with ErrNilSession rather than silently swap in a nil.
		return nil, nil //nolint:nilnil // intentional: probing corner case
	}

	client, err := helix.NewCQLClient(mockA, mockB, helix.WithSessionRefresher(refresher))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	err = client.RefreshSession(context.Background(), helix.ClusterA)
	assert.ErrorIs(t, err, types.ErrNilSession)
	assert.Same(t, mockA, client.SessionA(), "no swap on nil-session result")
}

func TestRefreshSession_RejectsClosedClient(t *testing.T) {
	mockA, mockB := newAlwaysOKMock(), newAlwaysOKMock()
	var invoked atomic.Bool
	refresher := func(_ context.Context, _ helix.ClusterID, _ error) (cql.Session, error) {
		invoked.Store(true)
		return newAlwaysOKMock(), nil
	}
	client, err := helix.NewCQLClient(mockA, mockB, helix.WithSessionRefresher(refresher))
	require.NoError(t, err)

	client.Close()
	err = client.RefreshSession(context.Background(), helix.ClusterA)
	assert.ErrorIs(t, err, types.ErrSessionClosed)
	assert.False(t, invoked.Load(),
		"refresher must not be invoked when client is closed")
}

func TestRefreshSession_RejectsClusterBOnSingleCluster(t *testing.T) {
	var invoked atomic.Bool
	refresher := func(_ context.Context, _ helix.ClusterID, _ error) (cql.Session, error) {
		invoked.Store(true)
		return newAlwaysOKMock(), nil
	}
	client, err := helix.NewCQLClient(newAlwaysOKMock(), nil, helix.WithSessionRefresher(refresher))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	err = client.RefreshSession(context.Background(), helix.ClusterB)
	assert.ErrorIs(t, err, types.ErrInvalidCluster)
	assert.False(t, invoked.Load(),
		"refresher must not be invoked when cluster is invalid")
}

// ----- Race tests ----------------------------------------------------------

// TestSwapSession_ConcurrentSwapAndQuery is intended to be run under -race.
// It interleaves many concurrent reads/writes with periodic SwapSession calls
// and asserts no data race, no panic, no nil deref. Returned old sessions
// are drained into a slice and Closed at the end so we don't leak (in this
// case the leak is harmless — alwaysOKMock has no real resources — but the
// pattern matches what production callers should do).
func TestSwapSession_ConcurrentSwapAndQuery(t *testing.T) {
	mockA, mockB := newAlwaysOKMock(), newAlwaysOKMock()
	client := newDualClusterTestClient(t, mockA, mockB)

	const (
		queryGoroutines = 32
		swapGoroutines  = 4
		opsPerWorker    = 200
	)

	var (
		wg            sync.WaitGroup
		discardedMu   sync.Mutex
		discardedOlds []cql.Session
	)

	stop := make(chan struct{})

	// Query workers.
	for range queryGoroutines {
		wg.Go(func() {
			ctx := context.Background()
			for j := range opsPerWorker {
				select {
				case <-stop:
					return
				default:
				}
				_ = client.Query("INSERT INTO t (k,v) VALUES (?, ?)", j, "v").ExecContext(ctx)
			}
		})
	}

	// Swap workers.
	for range swapGoroutines {
		wg.Go(func() {
			for j := range opsPerWorker / 10 {
				cluster := helix.ClusterA
				if j%2 == 0 {
					cluster = helix.ClusterB
				}
				old, err := client.SwapSession(cluster, newAlwaysOKMock())
				if err != nil {
					return
				}
				discardedMu.Lock()
				discardedOlds = append(discardedOlds, old)
				discardedMu.Unlock()
			}
		})
	}

	wg.Wait()
	close(stop)

	// Close all the swapped-out sessions.
	for _, s := range discardedOlds {
		s.Close()
	}
}

// ----- Test helpers --------------------------------------------------------

// alwaysOKMock is the minimal cql.Session that returns nil from every op.
// It tracks queries[] (for assertion of routing) and closed (for lifecycle).
type alwaysOKMock struct {
	mu      sync.Mutex
	queries []string
	closed  atomic.Bool
}

func newAlwaysOKMock() *alwaysOKMock { return &alwaysOKMock{} }

func (m *alwaysOKMock) Query(stmt string, values ...any) cql.Query {
	m.mu.Lock()
	m.queries = append(m.queries, stmt)
	m.mu.Unlock()

	return &okQuery{stmt: stmt, values: values}
}

func (m *alwaysOKMock) Batch(_ cql.BatchType) cql.Batch { return &okBatch{} }
func (m *alwaysOKMock) Close()                          { m.closed.Store(true) }

// wrappedAlwaysOKMock is a different concrete type that also implements
// cql.Session, used to prove SwapSession accepts heterogeneous concrete
// types across calls. atomic.Value would panic on a Store of a different
// dynamic type — atomic.Pointer[sessionHolder] doesn't.
type wrappedAlwaysOKMock struct {
	inner *alwaysOKMock
}

func (w *wrappedAlwaysOKMock) Query(stmt string, values ...any) cql.Query {
	return w.inner.Query(stmt, values...)
}

func (w *wrappedAlwaysOKMock) Batch(kind cql.BatchType) cql.Batch { return w.inner.Batch(kind) }
func (w *wrappedAlwaysOKMock) Close()                             { w.inner.Close() }

type okQuery struct {
	stmt   string
	values []any
}

func (q *okQuery) Consistency(cql.Consistency) cql.Query                           { return q }
func (q *okQuery) SerialConsistency(cql.Consistency) cql.Query                     { return q }
func (q *okQuery) PageSize(int) cql.Query                                          { return q }
func (q *okQuery) PageState([]byte) cql.Query                                      { return q }
func (q *okQuery) WithTimestamp(int64) cql.Query                                   { return q }
func (q *okQuery) Statement() string                                               { return q.stmt }
func (q *okQuery) Values() []any                                                   { return q.values }
func (q *okQuery) Release()                                                        {}
func (q *okQuery) Exec() error                                                     { return nil }
func (q *okQuery) ExecContext(context.Context) error                               { return nil }
func (q *okQuery) Scan(...any) error                                               { return nil }
func (q *okQuery) ScanContext(context.Context, ...any) error                       { return nil }
func (q *okQuery) MapScan(map[string]any) error                                    { return nil }
func (q *okQuery) MapScanContext(context.Context, map[string]any) error            { return nil }
func (q *okQuery) ScanCAS(...any) (bool, error)                                    { return true, nil }
func (q *okQuery) ScanCASContext(context.Context, ...any) (bool, error)            { return true, nil }
func (q *okQuery) MapScanCAS(map[string]any) (bool, error)                         { return true, nil }
func (q *okQuery) MapScanCASContext(context.Context, map[string]any) (bool, error) { return true, nil }
func (q *okQuery) Iter() cql.Iter                                                  { return &emptyOkIter{} }
func (q *okQuery) IterContext(context.Context) cql.Iter                            { return &emptyOkIter{} }

type okBatch struct{ stmt string }

func (b *okBatch) Query(stmt string, _ ...any) cql.Batch       { b.stmt = stmt; return b }
func (b *okBatch) Consistency(cql.Consistency) cql.Batch       { return b }
func (b *okBatch) SerialConsistency(cql.Consistency) cql.Batch { return b }
func (b *okBatch) WithTimestamp(int64) cql.Batch               { return b }
func (b *okBatch) Exec() error                                 { return nil }
func (b *okBatch) ExecContext(context.Context) error           { return nil }
func (b *okBatch) IterContext(context.Context) cql.Iter        { return &emptyOkIter{} }
func (b *okBatch) ExecCAS(...any) (bool, cql.Iter, error)      { return true, nil, nil }
func (b *okBatch) ExecCASContext(context.Context, ...any) (bool, cql.Iter, error) {
	return true, nil, nil
}
func (b *okBatch) MapExecCAS(map[string]any) (bool, cql.Iter, error) {
	return true, nil, nil
}

func (b *okBatch) MapExecCASContext(context.Context, map[string]any) (bool, cql.Iter, error) {
	return true, nil, nil
}
func (b *okBatch) Size() int                    { return 0 }
func (b *okBatch) Statements() []cql.BatchEntry { return nil }

type emptyOkIter struct{}

func (i *emptyOkIter) Scan(...any) bool            { return false }
func (i *emptyOkIter) Close() error                { return nil }
func (i *emptyOkIter) MapScan(map[string]any) bool { return false }
func (i *emptyOkIter) SliceMap() ([]map[string]any, error) {
	return nil, nil
}
func (i *emptyOkIter) PageState() []byte         { return nil }
func (i *emptyOkIter) NumRows() int              { return 0 }
func (i *emptyOkIter) Columns() []cql.ColumnInfo { return nil }
func (i *emptyOkIter) Scanner() cql.Scanner      { return nil }
func (i *emptyOkIter) Warnings() []string        { return nil }

// newDualClusterTestClient builds a CQLClient with the two given mocks,
// skipping any options that would start a real replay/topology background
// goroutine. Failures here are test bugs, not user errors.
func newDualClusterTestClient(t *testing.T, a, b cql.Session) *helix.CQLClient {
	t.Helper()
	client, err := helix.NewCQLClient(a, b)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	return client
}
