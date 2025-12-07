package helix_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/adapter/cql"
	sqladapter "github.com/arloliu/helix/adapter/sql"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
	_ "github.com/mattn/go-sqlite3"
)

// =============================================================================
// Benchmark Infrastructure
// =============================================================================

// mockDB provides a zero-overhead mock for benchmarking.
// It measures only Helix overhead, not actual database operations.
type mockDB struct {
	execCount  atomic.Int64
	queryCount atomic.Int64
}

func (m *mockDB) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	m.execCount.Add(1)

	return mockResult{}, nil
}

func (m *mockDB) QueryContext(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	m.queryCount.Add(1)

	return nil, nil //nolint:nilnil // mock returns nil for benchmarking
}

func (m *mockDB) QueryRowContext(_ context.Context, _ string, _ ...any) *sql.Row {
	return nil
}

func (m *mockDB) PingContext(_ context.Context) error {
	return nil
}

func (m *mockDB) Close() error {
	return nil
}

type mockResult struct{}

func (m mockResult) LastInsertId() (int64, error) { return 0, nil }
func (m mockResult) RowsAffected() (int64, error) { return 1, nil }

// mockReplayer measures replay enqueue overhead without actual queueing.
type mockReplayer struct {
	enqueueCount atomic.Int64
}

func (m *mockReplayer) Enqueue(_ context.Context, _ types.ReplayPayload) error {
	m.enqueueCount.Add(1)

	return nil
}

// failingMockDB fails every operation to trigger replay.
type failingMockDB struct{}

func (m *failingMockDB) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	return nil, sql.ErrConnDone
}

func (m *failingMockDB) QueryContext(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, sql.ErrConnDone
}

func (m *failingMockDB) QueryRowContext(_ context.Context, _ string, _ ...any) *sql.Row {
	return nil
}

func (m *failingMockDB) PingContext(_ context.Context) error {
	return sql.ErrConnDone
}

func (m *failingMockDB) Close() error {
	return nil
}

// =============================================================================
// Single-Cluster Mode Benchmarks
// =============================================================================

// BenchmarkSQLSingleClusterExec measures single-cluster write overhead.
func BenchmarkSQLSingleClusterExec(b *testing.B) {
	mock := &mockDB{}
	client, err := helix.NewSQLClient(mock, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = client.ExecContext(ctx, "INSERT INTO t (id) VALUES (?)", 1)
	}
}

// BenchmarkSQLSingleClusterQuery measures single-cluster read overhead.
func BenchmarkSQLSingleClusterQuery(b *testing.B) {
	mock := &mockDB{}
	client, err := helix.NewSQLClient(mock, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = client.QueryContext(ctx, "SELECT * FROM t WHERE id = ?", 1) //nolint:sqlclosecheck,rowserrcheck // mock returns nil
	}
}

// BenchmarkSQLDirectExec measures raw mock execution (baseline).
func BenchmarkSQLDirectExec(b *testing.B) {
	mock := &mockDB{}
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = mock.ExecContext(ctx, "INSERT INTO t (id) VALUES (?)", 1)
	}
}

// BenchmarkSQLDirectQuery measures raw mock query (baseline).
func BenchmarkSQLDirectQuery(b *testing.B) {
	mock := &mockDB{}
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = mock.QueryContext(ctx, "SELECT * FROM t WHERE id = ?", 1) //nolint:rowserrcheck,sqlclosecheck // mock returns nil
	}
}

// =============================================================================
// Dual-Cluster Mode Benchmarks
// =============================================================================

// BenchmarkSQLDualClusterExec measures dual-cluster write overhead.
func BenchmarkSQLDualClusterExec(b *testing.B) {
	mockA := &mockDB{}
	mockB := &mockDB{}
	client, err := helix.NewSQLClient(mockA, mockB)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = client.ExecContext(ctx, "INSERT INTO t (id) VALUES (?)", 1)
	}
}

// BenchmarkSQLDualClusterQuery measures dual-cluster read with StickyRead.
func BenchmarkSQLDualClusterQuery(b *testing.B) {
	mockA := &mockDB{}
	mockB := &mockDB{}
	client, err := helix.NewSQLClient(mockA, mockB,
		helix.WithReadStrategy(policy.NewStickyRead()),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = client.QueryContext(ctx, "SELECT * FROM t WHERE id = ?", 1) //nolint:sqlclosecheck,rowserrcheck // mock returns nil
	}
}

// BenchmarkSQLDualClusterExecWithReplay measures write with replay enqueue.
func BenchmarkSQLDualClusterExecWithReplay(b *testing.B) {
	mockA := &mockDB{}
	mockB := &mockDB{}
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100000))

	client, err := helix.NewSQLClient(mockA, mockB,
		helix.WithReplayer(replayer),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = client.ExecContext(ctx, "INSERT INTO t (id) VALUES (?)", 1)
	}
}

// BenchmarkSQLPartialFailureWithReplay measures partial failure + replay path.
func BenchmarkSQLPartialFailureWithReplay(b *testing.B) {
	mockA := &mockDB{}
	mockB := &failingMockDB{} // B always fails
	replayer := &mockReplayer{}

	client, err := helix.NewSQLClient(mockA, mockB,
		helix.WithReplayer(replayer),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = client.ExecContext(ctx, "INSERT INTO t (id) VALUES (?)", 1)
	}
}

// =============================================================================
// Read Strategy Benchmarks
// =============================================================================

// BenchmarkStickyReadSelect measures sticky read cluster selection.
func BenchmarkStickyReadSelect(b *testing.B) {
	strategy := policy.NewStickyRead()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = strategy.Select(ctx)
	}
}

// BenchmarkRoundRobinReadSelect measures round-robin cluster selection.
func BenchmarkRoundRobinReadSelect(b *testing.B) {
	strategy := policy.NewRoundRobinRead()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = strategy.Select(ctx)
	}
}

// BenchmarkPrimaryOnlyReadSelect measures primary-only cluster selection.
func BenchmarkPrimaryOnlyReadSelect(b *testing.B) {
	strategy := policy.NewPrimaryOnlyRead()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = strategy.Select(ctx)
	}
}

// =============================================================================
// Memory Replayer Benchmarks
// =============================================================================

// BenchmarkMemoryReplayerEnqueue measures replay enqueue overhead.
func BenchmarkMemoryReplayerEnqueue(b *testing.B) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(b.N + 1000))
	ctx := context.Background()

	payload := types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT INTO t (id, name) VALUES (?, ?)",
		Args:          []any{1, "test"},
		Timestamp:     time.Now().UnixMicro(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = replayer.Enqueue(ctx, payload)
	}
}

// BenchmarkMemoryReplayerDequeue measures replay dequeue overhead.
func BenchmarkMemoryReplayerDequeue(b *testing.B) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(b.N + 1000))
	ctx := context.Background()

	// Pre-fill the queue
	payload := types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT INTO t (id, name) VALUES (?, ?)",
		Args:          []any{1, "test"},
		Timestamp:     time.Now().UnixMicro(),
	}
	for range b.N {
		_ = replayer.Enqueue(ctx, payload)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = replayer.TryDequeue()
	}
}

// =============================================================================
// Concurrent Access Benchmarks
// =============================================================================

// BenchmarkSQLDualClusterExecParallel measures concurrent dual-cluster writes.
func BenchmarkSQLDualClusterExecParallel(b *testing.B) {
	mockA := &mockDB{}
	mockB := &mockDB{}
	client, err := helix.NewSQLClient(mockA, mockB)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = client.ExecContext(ctx, "INSERT INTO t (id) VALUES (?)", 1)
		}
	})
}

// BenchmarkSQLSingleClusterExecParallel measures concurrent single-cluster writes.
func BenchmarkSQLSingleClusterExecParallel(b *testing.B) {
	mock := &mockDB{}
	client, err := helix.NewSQLClient(mock, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = client.ExecContext(ctx, "INSERT INTO t (id) VALUES (?)", 1)
		}
	})
}

// BenchmarkMemoryReplayerEnqueueParallel measures concurrent enqueue.
func BenchmarkMemoryReplayerEnqueueParallel(b *testing.B) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(b.N*10 + 1000))
	ctx := context.Background()

	payload := types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT INTO t (id, name) VALUES (?, ?)",
		Args:          []any{1, "test"},
		Timestamp:     time.Now().UnixMicro(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = replayer.Enqueue(ctx, payload)
		}
	})
}

// =============================================================================
// Real Database Benchmarks (SQLite)
// =============================================================================

// BenchmarkSQLiteSingleClusterExec measures single-cluster with real SQLite.
func BenchmarkSQLiteSingleClusterExec(b *testing.B) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		b.Fatal(err)
	}

	client, err := helix.NewSQLClientFromDB(db, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = client.ExecContext(ctx, "INSERT INTO t (value) VALUES (?)", "test")
	}
}

// BenchmarkSQLiteDirectExec measures raw SQLite execution (baseline).
func BenchmarkSQLiteDirectExec(b *testing.B) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = db.ExecContext(ctx, "INSERT INTO t (value) VALUES (?)", "test")
	}
}

// BenchmarkSQLiteDualClusterExec measures dual-cluster with real SQLite.
func BenchmarkSQLiteDualClusterExec(b *testing.B) {
	dbA, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer dbA.Close()

	dbB, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer dbB.Close()

	for _, db := range []*sql.DB{dbA, dbB} {
		_, err = db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
		if err != nil {
			b.Fatal(err)
		}
	}

	client, err := helix.NewSQLClientFromDB(dbA, dbB)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = client.ExecContext(ctx, "INSERT INTO t (value) VALUES (?)", "test")
	}
}

// =============================================================================
// Write Strategy Benchmarks
// =============================================================================

// BenchmarkConcurrentWriteStrategy measures concurrent write strategy overhead.
func BenchmarkConcurrentWriteStrategy(b *testing.B) {
	strategy := policy.NewConcurrentDualWrite()
	ctx := context.Background()

	writeA := func(_ context.Context) error { return nil }
	writeB := func(_ context.Context) error { return nil }

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = strategy.Execute(ctx, writeA, writeB)
	}
}

// BenchmarkSyncWriteStrategy measures sync write strategy overhead.
func BenchmarkSyncWriteStrategy(b *testing.B) {
	strategy := policy.NewSyncDualWrite()
	ctx := context.Background()

	writeA := func(_ context.Context) error { return nil }
	writeB := func(_ context.Context) error { return nil }

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = strategy.Execute(ctx, writeA, writeB)
	}
}

// =============================================================================
// Overhead Comparison Summary Benchmarks
// =============================================================================

// BenchmarkOverheadComparison runs all overhead comparisons in a structured way.
func BenchmarkOverheadComparison(b *testing.B) {
	b.Run("Direct", func(b *testing.B) {
		mock := &mockDB{}
		ctx := context.Background()

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			_, _ = mock.ExecContext(ctx, "INSERT", 1)
		}
	})

	b.Run("SingleCluster", func(b *testing.B) {
		mock := &mockDB{}
		client, _ := helix.NewSQLClient(mock, nil)
		defer client.Close()

		ctx := context.Background()

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			_, _ = client.ExecContext(ctx, "INSERT", 1)
		}
	})

	b.Run("DualCluster", func(b *testing.B) {
		mockA := &mockDB{}
		mockB := &mockDB{}
		client, _ := helix.NewSQLClient(mockA, mockB)
		defer client.Close()

		ctx := context.Background()

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			_, _ = client.ExecContext(ctx, "INSERT", 1)
		}
	})

	b.Run("DualClusterWithReplayer", func(b *testing.B) {
		mockA := &mockDB{}
		mockB := &mockDB{}
		replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100000))
		client, _ := helix.NewSQLClient(mockA, mockB,
			helix.WithReplayer(replayer),
		)
		defer client.Close()

		ctx := context.Background()

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			_, _ = client.ExecContext(ctx, "INSERT", 1)
		}
	})

	b.Run("DualClusterWithStrategies", func(b *testing.B) {
		mockA := &mockDB{}
		mockB := &mockDB{}
		client, _ := helix.NewSQLClient(mockA, mockB,
			helix.WithReadStrategy(policy.NewStickyRead()),
			helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
			helix.WithFailoverPolicy(policy.NewActiveFailover()),
		)
		defer client.Close()

		ctx := context.Background()

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			_, _ = client.ExecContext(ctx, "INSERT", 1)
		}
	})
}

// =============================================================================
// Client Creation Benchmarks
// =============================================================================

// BenchmarkNewSQLClientSingle measures single-cluster client creation overhead.
func BenchmarkNewSQLClientSingle(b *testing.B) {
	mock := &mockDB{}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		client, _ := helix.NewSQLClient(mock, nil)
		client.Close()
	}
}

// BenchmarkNewSQLClientDual measures dual-cluster client creation overhead.
func BenchmarkNewSQLClientDual(b *testing.B) {
	mockA := &mockDB{}
	mockB := &mockDB{}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		client, _ := helix.NewSQLClient(mockA, mockB)
		client.Close()
	}
}

// BenchmarkNewSQLClientWithOptions measures client creation with full options.
func BenchmarkNewSQLClientWithOptions(b *testing.B) {
	mockA := &mockDB{}
	mockB := &mockDB{}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		client, _ := helix.NewSQLClient(mockA, mockB,
			helix.WithReadStrategy(policy.NewStickyRead()),
			helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
			helix.WithFailoverPolicy(policy.NewActiveFailover()),
			helix.WithReplayer(replay.NewMemoryReplayer()),
		)
		client.Close()
	}
}

// =============================================================================
// Adapter Wrapper Overhead Benchmarks
// =============================================================================

// BenchmarkSQLAdapterExec measures SQL adapter wrapper overhead.
func BenchmarkSQLAdapterExec(b *testing.B) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)")
	if err != nil {
		b.Fatal(err)
	}

	adapter := sqladapter.NewDBAdapter(db)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = adapter.ExecContext(ctx, "INSERT INTO t (id) VALUES (?)", 1)
	}
}

// BenchmarkSQLDirectExecRaw measures raw *sql.DB execution for comparison.
func BenchmarkSQLDirectExecRaw(b *testing.B) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)")
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = db.ExecContext(ctx, "INSERT INTO t (id) VALUES (?)", 1)
	}
}

// =============================================================================
// Failover Policy Benchmarks
// =============================================================================

// BenchmarkActiveFailoverRecordSuccess measures failover success recording.
func BenchmarkActiveFailoverRecordSuccess(b *testing.B) {
	fp := policy.NewActiveFailover()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		fp.RecordSuccess(types.ClusterA)
	}
}

// BenchmarkActiveFailoverRecordFailure measures failover failure recording.
func BenchmarkActiveFailoverRecordFailure(b *testing.B) {
	fp := policy.NewActiveFailover()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		fp.RecordFailure(types.ClusterA)
	}
}

// BenchmarkCircuitBreakerShouldFailover measures circuit breaker check overhead.
func BenchmarkCircuitBreakerShouldFailover(b *testing.B) {
	cb := policy.NewCircuitBreaker()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = cb.ShouldFailover(types.ClusterA, nil)
	}
}

// =============================================================================
// Memory Allocation Pattern Benchmarks
// =============================================================================

// BenchmarkReplayPayloadCreation measures replay payload struct creation.
func BenchmarkReplayPayloadCreation(b *testing.B) {
	b.ReportAllocs()

	for b.Loop() {
		_ = types.ReplayPayload{
			TargetCluster: types.ClusterA,
			Query:         "INSERT INTO t (id, name, value) VALUES (?, ?, ?)",
			Args:          []any{1, "test", 42.5},
			Timestamp:     time.Now().UnixMicro(),
			Priority:      types.PriorityHigh,
		}
	}
}

// BenchmarkWaitGroupGo measures sync.WaitGroup.Go() overhead (Go 1.25+).
func BenchmarkWaitGroupGo(b *testing.B) {
	b.ReportAllocs()

	for b.Loop() {
		var wg sync.WaitGroup
		wg.Go(func() {})
		wg.Go(func() {})
		wg.Wait()
	}
}

// BenchmarkWaitGroupAddGo measures traditional wg.Add/go/Done overhead.
// This benchmark intentionally uses the old pattern to compare with wg.Go().
func BenchmarkWaitGroupAddGo(b *testing.B) {
	b.ReportAllocs()

	for b.Loop() {
		var wg sync.WaitGroup
		wg.Add(2) //nolint:revive // intentional: comparing old wg.Add/go pattern vs new wg.Go
		go func() { wg.Done() }()
		go func() { wg.Done() }()
		wg.Wait()
	}
}

// =============================================================================
// CQL Benchmark Infrastructure
// =============================================================================

// mockCQLSession provides a zero-overhead mock for CQL benchmarking.
type mockCQLSession struct {
	execCount  atomic.Int64
	queryCount atomic.Int64
}

func (m *mockCQLSession) Query(stmt string, values ...any) cql.Query {
	m.queryCount.Add(1)

	return &mockCQLQuery{
		session:   m,
		statement: stmt,
		values:    values,
	}
}

func (m *mockCQLSession) Batch(kind cql.BatchType) cql.Batch {
	return &mockCQLBatch{
		session: m,
		kind:    kind,
		entries: make([]cql.BatchEntry, 0, 10),
	}
}

func (m *mockCQLSession) NewBatch(kind cql.BatchType) cql.Batch {
	return m.Batch(kind)
}

func (m *mockCQLSession) ExecuteBatch(batch cql.Batch) error {
	return batch.Exec()
}

func (m *mockCQLSession) ExecuteBatchCAS(batch cql.Batch, dest ...any) (applied bool, iter cql.Iter, err error) {
	return batch.ExecCAS(dest...)
}

func (m *mockCQLSession) MapExecuteBatchCAS(batch cql.Batch, dest map[string]any) (applied bool, iter cql.Iter, err error) {
	return batch.MapExecCAS(dest)
}

func (m *mockCQLSession) Close() {}

// mockCQLQuery provides a zero-overhead mock CQL query.
type mockCQLQuery struct {
	session   *mockCQLSession
	statement string
	values    []any
}

func (q *mockCQLQuery) WithContext(_ context.Context) cql.Query       { return q }
func (q *mockCQLQuery) Consistency(_ cql.Consistency) cql.Query       { return q }
func (q *mockCQLQuery) SetConsistency(c cql.Consistency)              { q.Consistency(c) }
func (q *mockCQLQuery) SerialConsistency(_ cql.Consistency) cql.Query { return q }
func (q *mockCQLQuery) PageSize(_ int) cql.Query                      { return q }
func (q *mockCQLQuery) PageState(_ []byte) cql.Query                  { return q }
func (q *mockCQLQuery) WithTimestamp(_ int64) cql.Query               { return q }

func (q *mockCQLQuery) Exec() error {
	q.session.execCount.Add(1)

	return nil
}

func (q *mockCQLQuery) Scan(_ ...any) error { return nil }

func (q *mockCQLQuery) Iter() cql.Iter {
	return &mockCQLIter{}
}

func (q *mockCQLQuery) MapScan(_ map[string]any) error { return nil }
func (q *mockCQLQuery) Statement() string              { return q.statement }
func (q *mockCQLQuery) Values() []any                  { return q.values }
func (q *mockCQLQuery) Release()                       {}

func (q *mockCQLQuery) ExecContext(_ context.Context) error {
	q.session.execCount.Add(1)
	return nil
}

func (q *mockCQLQuery) ScanContext(_ context.Context, _ ...any) error {
	return nil
}

func (q *mockCQLQuery) IterContext(_ context.Context) cql.Iter {
	return &mockCQLIter{}
}

func (q *mockCQLQuery) MapScanContext(_ context.Context, _ map[string]any) error {
	return nil
}

func (q *mockCQLQuery) ScanCAS(_ ...any) (applied bool, err error) {
	return true, nil
}

func (q *mockCQLQuery) ScanCASContext(_ context.Context, _ ...any) (applied bool, err error) {
	return true, nil
}

func (q *mockCQLQuery) MapScanCAS(_ map[string]any) (applied bool, err error) {
	return true, nil
}

func (q *mockCQLQuery) MapScanCASContext(_ context.Context, _ map[string]any) (applied bool, err error) {
	return true, nil
}

// mockCQLBatch provides a zero-overhead mock CQL batch.
type mockCQLBatch struct {
	session *mockCQLSession
	kind    cql.BatchType
	entries []cql.BatchEntry
}

func (b *mockCQLBatch) Query(stmt string, args ...any) cql.Batch {
	b.entries = append(b.entries, cql.BatchEntry{Statement: stmt, Args: args})

	return b
}

func (b *mockCQLBatch) Consistency(_ cql.Consistency) cql.Batch       { return b }
func (b *mockCQLBatch) SetConsistency(c cql.Consistency)              { b.Consistency(c) }
func (b *mockCQLBatch) SerialConsistency(_ cql.Consistency) cql.Batch { return b }
func (b *mockCQLBatch) WithContext(_ context.Context) cql.Batch       { return b }
func (b *mockCQLBatch) WithTimestamp(_ int64) cql.Batch               { return b }
func (b *mockCQLBatch) Size() int                                     { return len(b.entries) }

func (b *mockCQLBatch) Exec() error {
	b.session.execCount.Add(int64(len(b.entries)))

	return nil
}

func (b *mockCQLBatch) ExecContext(_ context.Context) error {
	b.session.execCount.Add(int64(len(b.entries)))
	return nil
}

func (b *mockCQLBatch) IterContext(_ context.Context) cql.Iter {
	return &mockCQLIter{}
}

func (b *mockCQLBatch) ExecCAS(_ ...any) (applied bool, iter cql.Iter, err error) {
	return true, &mockCQLIter{}, nil
}

func (b *mockCQLBatch) ExecCASContext(_ context.Context, _ ...any) (applied bool, iter cql.Iter, err error) {
	return true, &mockCQLIter{}, nil
}

func (b *mockCQLBatch) MapExecCAS(_ map[string]any) (applied bool, iter cql.Iter, err error) {
	return true, &mockCQLIter{}, nil
}

func (b *mockCQLBatch) MapExecCASContext(_ context.Context, _ map[string]any) (applied bool, iter cql.Iter, err error) {
	return true, &mockCQLIter{}, nil
}

func (b *mockCQLBatch) Statements() []cql.BatchEntry { return b.entries }

// mockCQLIter provides a zero-overhead mock CQL iterator.
type mockCQLIter struct {
	scanned int
}

func (i *mockCQLIter) Scan(_ ...any) bool {
	i.scanned++

	return i.scanned <= 1 // Return one row
}

func (i *mockCQLIter) Close() error                        { return nil }
func (i *mockCQLIter) MapScan(_ map[string]any) bool       { return false }
func (i *mockCQLIter) SliceMap() ([]map[string]any, error) { return nil, nil }
func (i *mockCQLIter) PageState() []byte                   { return nil }
func (i *mockCQLIter) NumRows() int                        { return 1 }
func (i *mockCQLIter) Columns() []cql.ColumnInfo           { return nil }
func (i *mockCQLIter) Scanner() cql.Scanner                { return &mockCQLScanner{} }
func (i *mockCQLIter) Warnings() []string                  { return nil }

// mockCQLScanner provides a zero-overhead mock CQL scanner.
type mockCQLScanner struct{}

func (s *mockCQLScanner) Next() bool          { return false }
func (s *mockCQLScanner) Scan(_ ...any) error { return nil }
func (s *mockCQLScanner) Err() error          { return nil }

// failingMockCQLSession fails every operation to trigger replay.
type failingMockCQLSession struct{}

func (m *failingMockCQLSession) Query(stmt string, values ...any) cql.Query {
	return &failingMockCQLQuery{statement: stmt, values: values}
}

func (m *failingMockCQLSession) Batch(kind cql.BatchType) cql.Batch {
	return &failingMockCQLBatch{kind: kind}
}

func (m *failingMockCQLSession) NewBatch(kind cql.BatchType) cql.Batch {
	return m.Batch(kind)
}

func (m *failingMockCQLSession) ExecuteBatch(batch cql.Batch) error {
	return batch.Exec()
}

func (m *failingMockCQLSession) ExecuteBatchCAS(batch cql.Batch, dest ...any) (applied bool, iter cql.Iter, err error) {
	return batch.ExecCAS(dest...)
}

func (m *failingMockCQLSession) MapExecuteBatchCAS(batch cql.Batch, dest map[string]any) (applied bool, iter cql.Iter, err error) {
	return batch.MapExecCAS(dest)
}

func (m *failingMockCQLSession) Close() {}

type failingMockCQLQuery struct {
	statement string
	values    []any
}

func (q *failingMockCQLQuery) WithContext(_ context.Context) cql.Query       { return q }
func (q *failingMockCQLQuery) Consistency(_ cql.Consistency) cql.Query       { return q }
func (q *failingMockCQLQuery) SetConsistency(c cql.Consistency)              { q.Consistency(c) }
func (q *failingMockCQLQuery) SerialConsistency(_ cql.Consistency) cql.Query { return q }
func (q *failingMockCQLQuery) PageSize(_ int) cql.Query                      { return q }
func (q *failingMockCQLQuery) PageState(_ []byte) cql.Query                  { return q }
func (q *failingMockCQLQuery) WithTimestamp(_ int64) cql.Query               { return q }
func (q *failingMockCQLQuery) Exec() error                                   { return errMockFailure }
func (q *failingMockCQLQuery) Scan(_ ...any) error                           { return errMockFailure }
func (q *failingMockCQLQuery) Iter() cql.Iter                                { return &failingMockCQLIter{} }
func (q *failingMockCQLQuery) MapScan(_ map[string]any) error                { return errMockFailure }
func (q *failingMockCQLQuery) Statement() string                             { return q.statement }
func (q *failingMockCQLQuery) Values() []any                                 { return q.values }
func (q *failingMockCQLQuery) Release()                                      {}

func (q *failingMockCQLQuery) ExecContext(_ context.Context) error { return errMockFailure }

func (q *failingMockCQLQuery) ScanContext(_ context.Context, _ ...any) error { return errMockFailure }

func (q *failingMockCQLQuery) IterContext(_ context.Context) cql.Iter { return &failingMockCQLIter{} }

func (q *failingMockCQLQuery) MapScanContext(_ context.Context, _ map[string]any) error {
	return errMockFailure
}

func (q *failingMockCQLQuery) ScanCAS(_ ...any) (applied bool, err error) {
	return false, errMockFailure
}

func (q *failingMockCQLQuery) ScanCASContext(_ context.Context, _ ...any) (applied bool, err error) {
	return false, errMockFailure
}

func (q *failingMockCQLQuery) MapScanCAS(_ map[string]any) (applied bool, err error) {
	return false, errMockFailure
}

func (q *failingMockCQLQuery) MapScanCASContext(_ context.Context, _ map[string]any) (applied bool, err error) {
	return false, errMockFailure
}

type failingMockCQLBatch struct {
	kind    cql.BatchType
	entries []cql.BatchEntry
}

func (b *failingMockCQLBatch) Query(stmt string, args ...any) cql.Batch {
	b.entries = append(b.entries, cql.BatchEntry{Statement: stmt, Args: args})

	return b
}

func (b *failingMockCQLBatch) Consistency(_ cql.Consistency) cql.Batch       { return b }
func (b *failingMockCQLBatch) SetConsistency(c cql.Consistency)              { b.Consistency(c) }
func (b *failingMockCQLBatch) SerialConsistency(_ cql.Consistency) cql.Batch { return b }
func (b *failingMockCQLBatch) WithContext(_ context.Context) cql.Batch       { return b }
func (b *failingMockCQLBatch) WithTimestamp(_ int64) cql.Batch               { return b }
func (b *failingMockCQLBatch) Size() int                                     { return len(b.entries) }
func (b *failingMockCQLBatch) Exec() error                                   { return errMockFailure }
func (b *failingMockCQLBatch) Statements() []cql.BatchEntry                  { return b.entries }

func (b *failingMockCQLBatch) ExecContext(_ context.Context) error { return errMockFailure }

func (b *failingMockCQLBatch) IterContext(_ context.Context) cql.Iter { return &failingMockCQLIter{} }

func (b *failingMockCQLBatch) ExecCAS(_ ...any) (applied bool, iter cql.Iter, err error) {
	return false, &failingMockCQLIter{}, errMockFailure
}

func (b *failingMockCQLBatch) ExecCASContext(_ context.Context, _ ...any) (applied bool, iter cql.Iter, err error) {
	return false, &failingMockCQLIter{}, errMockFailure
}

func (b *failingMockCQLBatch) MapExecCAS(_ map[string]any) (applied bool, iter cql.Iter, err error) {
	return false, &failingMockCQLIter{}, errMockFailure
}

func (b *failingMockCQLBatch) MapExecCASContext(_ context.Context, _ map[string]any) (applied bool, iter cql.Iter, err error) {
	return false, &failingMockCQLIter{}, errMockFailure
}

type failingMockCQLIter struct{}

func (i *failingMockCQLIter) Scan(_ ...any) bool                  { return false }
func (i *failingMockCQLIter) Close() error                        { return errMockFailure }
func (i *failingMockCQLIter) MapScan(_ map[string]any) bool       { return false }
func (i *failingMockCQLIter) SliceMap() ([]map[string]any, error) { return nil, errMockFailure }
func (i *failingMockCQLIter) PageState() []byte                   { return nil }
func (i *failingMockCQLIter) NumRows() int                        { return 0 }
func (i *failingMockCQLIter) Columns() []cql.ColumnInfo           { return nil }
func (i *failingMockCQLIter) Scanner() cql.Scanner                { return &failingMockCQLScanner{} }
func (i *failingMockCQLIter) Warnings() []string                  { return nil }

// failingMockCQLScanner fails all operations.
type failingMockCQLScanner struct{}

func (s *failingMockCQLScanner) Next() bool          { return false }
func (s *failingMockCQLScanner) Scan(_ ...any) error { return errMockFailure }
func (s *failingMockCQLScanner) Err() error          { return errMockFailure }

// errMockFailure is returned by failing mocks.
var errMockFailure = errors.New("mock failure")

// =============================================================================
// CQL Single-Cluster Mode Benchmarks
// =============================================================================

// BenchmarkCQLSingleClusterExec measures single-cluster write overhead.
func BenchmarkCQLSingleClusterExec(b *testing.B) {
	mock := &mockCQLSession{}
	client, err := helix.NewCQLClient(mock, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = client.Query("INSERT INTO t (id) VALUES (?)", 1).ExecContext(ctx)
	}
}

// BenchmarkCQLSingleClusterQuery measures single-cluster read overhead.
func BenchmarkCQLSingleClusterQuery(b *testing.B) {
	mock := &mockCQLSession{}
	client, err := helix.NewCQLClient(mock, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	var id int

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = client.Query("SELECT id FROM t WHERE id = ?", 1).ScanContext(ctx, &id)
	}
}

// BenchmarkCQLDirectExec measures raw mock execution (baseline).
func BenchmarkCQLDirectExec(b *testing.B) {
	mock := &mockCQLSession{}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = mock.Query("INSERT INTO t (id) VALUES (?)", 1).Exec()
	}
}

// BenchmarkCQLDirectQuery measures raw mock query (baseline).
func BenchmarkCQLDirectQuery(b *testing.B) {
	mock := &mockCQLSession{}
	var id int

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = mock.Query("SELECT id FROM t WHERE id = ?", 1).Scan(&id)
	}
}

// =============================================================================
// CQL Dual-Cluster Mode Benchmarks
// =============================================================================

// BenchmarkCQLDualClusterExec measures dual-cluster write overhead.
func BenchmarkCQLDualClusterExec(b *testing.B) {
	mockA := &mockCQLSession{}
	mockB := &mockCQLSession{}
	client, err := helix.NewCQLClient(mockA, mockB)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = client.Query("INSERT INTO t (id) VALUES (?)", 1).ExecContext(ctx)
	}
}

// BenchmarkCQLDualClusterQuery measures dual-cluster read with StickyRead.
func BenchmarkCQLDualClusterQuery(b *testing.B) {
	mockA := &mockCQLSession{}
	mockB := &mockCQLSession{}
	client, err := helix.NewCQLClient(mockA, mockB,
		helix.WithReadStrategy(policy.NewStickyRead()),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	var id int

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = client.Query("SELECT id FROM t WHERE id = ?", 1).ScanContext(ctx, &id)
	}
}

// BenchmarkCQLDualClusterExecWithReplay measures write with replay enqueue.
func BenchmarkCQLDualClusterExecWithReplay(b *testing.B) {
	mockA := &mockCQLSession{}
	mockB := &mockCQLSession{}
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100000))

	client, err := helix.NewCQLClient(mockA, mockB,
		helix.WithReplayer(replayer),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = client.Query("INSERT INTO t (id) VALUES (?)", 1).ExecContext(ctx)
	}
}

// BenchmarkCQLPartialFailureWithReplay measures write when one cluster fails.
func BenchmarkCQLPartialFailureWithReplay(b *testing.B) {
	mockA := &mockCQLSession{}
	failingB := &failingMockCQLSession{}
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100000))

	client, err := helix.NewCQLClient(mockA, failingB,
		helix.WithReplayer(replayer),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = client.Query("INSERT INTO t (id) VALUES (?)", 1).ExecContext(ctx)
	}
}

// =============================================================================
// CQL Batch Benchmarks
// =============================================================================

// BenchmarkCQLSingleClusterBatch measures single-cluster batch overhead.
func BenchmarkCQLSingleClusterBatch(b *testing.B) {
	mock := &mockCQLSession{}
	client, err := helix.NewCQLClient(mock, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		batch := client.Batch(types.UnloggedBatch)
		batch.Query("INSERT INTO t (id) VALUES (?)", 1)
		batch.Query("INSERT INTO t (id) VALUES (?)", 2)
		batch.Query("INSERT INTO t (id) VALUES (?)", 3)
		_ = batch.ExecContext(ctx)
	}
}

// BenchmarkCQLDualClusterBatch measures dual-cluster batch overhead.
func BenchmarkCQLDualClusterBatch(b *testing.B) {
	mockA := &mockCQLSession{}
	mockB := &mockCQLSession{}
	client, err := helix.NewCQLClient(mockA, mockB)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		batch := client.Batch(types.UnloggedBatch)
		batch.Query("INSERT INTO t (id) VALUES (?)", 1)
		batch.Query("INSERT INTO t (id) VALUES (?)", 2)
		batch.Query("INSERT INTO t (id) VALUES (?)", 3)
		_ = batch.ExecContext(ctx)
	}
}

// BenchmarkCQLDirectBatch measures raw mock batch (baseline).
func BenchmarkCQLDirectBatch(b *testing.B) {
	mock := &mockCQLSession{}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		batch := mock.Batch(cql.UnloggedBatch)
		batch.Query("INSERT INTO t (id) VALUES (?)", 1)
		batch.Query("INSERT INTO t (id) VALUES (?)", 2)
		batch.Query("INSERT INTO t (id) VALUES (?)", 3)
		_ = batch.Exec()
	}
}

// =============================================================================
// CQL Parallel Benchmarks
// =============================================================================

// BenchmarkCQLDualClusterExecParallel measures concurrent dual-cluster writes.
func BenchmarkCQLDualClusterExecParallel(b *testing.B) {
	mockA := &mockCQLSession{}
	mockB := &mockCQLSession{}
	client, err := helix.NewCQLClient(mockA, mockB)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = client.Query("INSERT INTO t (id) VALUES (?)", 1).ExecContext(ctx)
		}
	})
}

// BenchmarkCQLSingleClusterExecParallel measures concurrent single-cluster writes.
func BenchmarkCQLSingleClusterExecParallel(b *testing.B) {
	mock := &mockCQLSession{}
	client, err := helix.NewCQLClient(mock, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = client.Query("INSERT INTO t (id) VALUES (?)", 1).ExecContext(ctx)
		}
	})
}

// BenchmarkCQLDualClusterQueryParallel measures concurrent dual-cluster reads.
func BenchmarkCQLDualClusterQueryParallel(b *testing.B) {
	mockA := &mockCQLSession{}
	mockB := &mockCQLSession{}
	client, err := helix.NewCQLClient(mockA, mockB,
		helix.WithReadStrategy(policy.NewStickyRead()),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		var id int
		for pb.Next() {
			_ = client.Query("SELECT id FROM t WHERE id = ?", 1).ScanContext(ctx, &id)
		}
	})
}

// =============================================================================
// CQL Overhead Comparison Benchmarks
// =============================================================================

// BenchmarkCQLOverheadComparison compares overhead across configurations.
func BenchmarkCQLOverheadComparison(b *testing.B) {
	b.Run("Direct", func(b *testing.B) {
		mock := &mockCQLSession{}

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			_ = mock.Query("INSERT INTO t (id) VALUES (?)", 1).Exec()
		}
	})

	b.Run("SingleCluster", func(b *testing.B) {
		mock := &mockCQLSession{}
		client, err := helix.NewCQLClient(mock, nil)
		if err != nil {
			b.Fatal(err)
		}
		defer client.Close()

		ctx := context.Background()

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			_ = client.Query("INSERT INTO t (id) VALUES (?)", 1).ExecContext(ctx)
		}
	})

	b.Run("DualCluster", func(b *testing.B) {
		mockA := &mockCQLSession{}
		mockB := &mockCQLSession{}
		client, err := helix.NewCQLClient(mockA, mockB)
		if err != nil {
			b.Fatal(err)
		}
		defer client.Close()

		ctx := context.Background()

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			_ = client.Query("INSERT INTO t (id) VALUES (?)", 1).ExecContext(ctx)
		}
	})

	b.Run("DualClusterWithReplayer", func(b *testing.B) {
		mockA := &mockCQLSession{}
		mockB := &mockCQLSession{}
		replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100000))

		client, err := helix.NewCQLClient(mockA, mockB,
			helix.WithReplayer(replayer),
		)
		if err != nil {
			b.Fatal(err)
		}
		defer client.Close()

		ctx := context.Background()

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			_ = client.Query("INSERT INTO t (id) VALUES (?)", 1).ExecContext(ctx)
		}
	})

	b.Run("DualClusterWithStrategies", func(b *testing.B) {
		mockA := &mockCQLSession{}
		mockB := &mockCQLSession{}
		replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100000))

		client, err := helix.NewCQLClient(mockA, mockB,
			helix.WithReplayer(replayer),
			helix.WithReadStrategy(policy.NewStickyRead()),
			helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
			helix.WithFailoverPolicy(policy.NewActiveFailover()),
		)
		if err != nil {
			b.Fatal(err)
		}
		defer client.Close()

		ctx := context.Background()

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			_ = client.Query("INSERT INTO t (id) VALUES (?)", 1).ExecContext(ctx)
		}
	})
}

// BenchmarkCQLIterator measures iterator overhead.
func BenchmarkCQLIterator(b *testing.B) {
	b.Run("Direct", func(b *testing.B) {
		mock := &mockCQLSession{}

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			iter := mock.Query("SELECT id FROM t").Iter()
			var id int
			for iter.Scan(&id) {
			}
			_ = iter.Close()
		}
	})

	b.Run("SingleCluster", func(b *testing.B) {
		mock := &mockCQLSession{}
		client, err := helix.NewCQLClient(mock, nil)
		if err != nil {
			b.Fatal(err)
		}
		defer client.Close()

		ctx := context.Background()

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			iter := client.Query("SELECT id FROM t").IterContext(ctx)
			var id int
			for iter.Scan(&id) {
			}
			_ = iter.Close()
		}
	})

	b.Run("DualCluster", func(b *testing.B) {
		mockA := &mockCQLSession{}
		mockB := &mockCQLSession{}
		client, err := helix.NewCQLClient(mockA, mockB)
		if err != nil {
			b.Fatal(err)
		}
		defer client.Close()

		ctx := context.Background()

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			iter := client.Query("SELECT id FROM t").IterContext(ctx)
			var id int
			for iter.Scan(&id) {
			}
			_ = iter.Close()
		}
	})
}

// BenchmarkCQLClientCreation measures client instantiation overhead.
func BenchmarkCQLClientCreation(b *testing.B) {
	b.Run("SingleCluster", func(b *testing.B) {
		mock := &mockCQLSession{}

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			client, _ := helix.NewCQLClient(mock, nil)
			client.Close()
		}
	})

	b.Run("DualCluster", func(b *testing.B) {
		mockA := &mockCQLSession{}
		mockB := &mockCQLSession{}

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			client, _ := helix.NewCQLClient(mockA, mockB)
			client.Close()
		}
	})

	b.Run("DualClusterWithOptions", func(b *testing.B) {
		mockA := &mockCQLSession{}
		mockB := &mockCQLSession{}
		replayer := replay.NewMemoryReplayer()

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			client, _ := helix.NewCQLClient(mockA, mockB,
				helix.WithReplayer(replayer),
				helix.WithReadStrategy(policy.NewStickyRead()),
				helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
			)
			client.Close()
		}
	})
}
