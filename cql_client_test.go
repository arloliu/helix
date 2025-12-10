package helix

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// mockSession implements cql.Session for testing.
type mockSession struct {
	queries    []string
	execErr    error
	scanErr    error
	scanValues []any
	closed     atomic.Bool
	lastQuery  *mockQuery // Track last query for inspection
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

func (m *mockSession) NewBatch(kind cql.BatchType) cql.Batch {
	return m.Batch(kind)
}

func (m *mockSession) ExecuteBatch(batch cql.Batch) error {
	return batch.Exec()
}

func (m *mockSession) ExecuteBatchCAS(batch cql.Batch, dest ...any) (applied bool, iter cql.Iter, err error) {
	return batch.ExecCAS(dest...)
}

func (m *mockSession) MapExecuteBatchCAS(batch cql.Batch, dest map[string]any) (applied bool, iter cql.Iter, err error) {
	return batch.MapExecCAS(dest)
}

func (m *mockSession) Close() {
	m.closed.Store(true)
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
}

func (q *mockQuery) WithContext(_ context.Context) cql.Query { return q }
func (q *mockQuery) Consistency(c cql.Consistency) cql.Query {
	q.consistency = &c
	return q
}
func (q *mockQuery) SetConsistency(c cql.Consistency) { q.Consistency(c) }
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

func (q *mockQuery) ExecContext(_ context.Context) error {
	return q.session.execErr
}

func (q *mockQuery) ScanContext(_ context.Context, dest ...any) error {
	return q.Scan(dest...)
}

func (q *mockQuery) IterContext(_ context.Context) cql.Iter {
	return &mockIter{}
}

func (q *mockQuery) MapScanContext(_ context.Context, m map[string]any) error {
	return q.session.scanErr
}

func (q *mockQuery) ScanCAS(_ ...any) (applied bool, err error) {
	return true, q.session.execErr
}

func (q *mockQuery) ScanCASContext(_ context.Context, _ ...any) (applied bool, err error) {
	return true, q.session.execErr
}

func (q *mockQuery) MapScanCAS(_ map[string]any) (applied bool, err error) {
	return true, q.session.execErr
}

func (q *mockQuery) MapScanCASContext(_ context.Context, _ map[string]any) (applied bool, err error) {
	return true, q.session.execErr
}

// mockBatch implements cql.Batch for testing.
type mockBatch struct {
	session *mockSession
	entries []cql.BatchEntry
}

func (b *mockBatch) Query(stmt string, args ...any) cql.Batch {
	b.entries = append(b.entries, cql.BatchEntry{Statement: stmt, Args: args})
	return b
}

func (b *mockBatch) Consistency(_ cql.Consistency) cql.Batch       { return b }
func (b *mockBatch) SetConsistency(c cql.Consistency)              { b.Consistency(c) }
func (b *mockBatch) SerialConsistency(_ cql.Consistency) cql.Batch { return b }
func (b *mockBatch) WithContext(_ context.Context) cql.Batch       { return b }
func (b *mockBatch) WithTimestamp(_ int64) cql.Batch               { return b }
func (b *mockBatch) Size() int                                     { return len(b.entries) }
func (b *mockBatch) Statements() []cql.BatchEntry                  { return b.entries }

func (b *mockBatch) Exec() error {
	return b.session.execErr
}

func (b *mockBatch) ExecContext(_ context.Context) error {
	return b.session.execErr
}

func (b *mockBatch) IterContext(_ context.Context) cql.Iter {
	return &mockIter{}
}

func (b *mockBatch) ExecCAS(_ ...any) (applied bool, iter cql.Iter, err error) {
	return true, &mockIter{}, b.session.execErr
}

func (b *mockBatch) ExecCASContext(_ context.Context, _ ...any) (applied bool, iter cql.Iter, err error) {
	return true, &mockIter{}, b.session.execErr
}

func (b *mockBatch) MapExecCAS(_ map[string]any) (applied bool, iter cql.Iter, err error) {
	return true, &mockIter{}, b.session.execErr
}

func (b *mockBatch) MapExecCASContext(_ context.Context, _ map[string]any) (applied bool, iter cql.Iter, err error) {
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

	// Verify deprecated methods also work
	batch2 := session.NewBatch(UnloggedBatch)
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
	// Note: This example uses nil sessions for illustration purposes.
	// In real code, create proper gocql sessions wrapped with v1.NewSession() or v2.NewSession().

	// Simulating session creation (replace with actual gocql sessions in real code)
	var sessionA, sessionB cql.Session // would be v1.NewSession(gocqlSession)

	// Create Helix client with custom strategies
	client, err := NewCQLClient(sessionA, sessionB,
		WithReadStrategy(nil),   // e.g., policy.NewStickyRead()
		WithWriteStrategy(nil),  // e.g., policy.NewConcurrentDualWrite()
		WithFailoverPolicy(nil), // e.g., policy.NewActiveFailover()
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
	// Note: Assumes client is already created (see ExampleNewCQLClient)
	var client *CQLClient

	// Write operation - triggers dual-write to both clusters
	err := client.Query("INSERT INTO users (id, name) VALUES (?, ?)", "user-1", "Alice").Exec()
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
	var client *CQLClient

	// Perform a write operation
	err := client.Query("INSERT INTO users (id, name) VALUES (?, ?)", "user-1", "Alice").Exec()
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

// Example_contextUsage demonstrates two patterns for context handling in queries.
func Example_contextUsage() {
	var client *CQLClient
	ctx := context.Background()

	// Pattern 1: Direct context method (simple cases)
	err := client.Query("INSERT INTO users (id, name) VALUES (?, ?)", "user-1", "Alice").
		ExecContext(ctx)
	_ = err

	// Pattern 2: Method chaining (multiple options)
	err = client.Query("SELECT name FROM users WHERE id = ?", "user-1").
		Consistency(Quorum).
		WithContext(ctx).
		Exec()
	_ = err
}
