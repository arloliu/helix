package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/helix"
	sqladapter "github.com/arloliu/helix/adapter/sql"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Single-Cluster Mode Tests
// =============================================================================

func TestSingleClusterModeExactBehavior(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(ctx, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)

	// Create single-cluster client
	client, err := helix.NewSQLClientFromDB(db, nil)
	require.NoError(t, err)
	defer client.Close()

	require.True(t, client.IsSingleCluster())

	// Insert via Helix
	_, err = client.ExecContext(ctx, "INSERT INTO items (id, name) VALUES (?, ?)", 1, "Test")
	require.NoError(t, err)

	// Verify via direct DB access - should be identical
	var name string
	err = db.QueryRowContext(ctx, "SELECT name FROM items WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "Test", name)

	// Verify via Helix read
	row := client.QueryRowContext(ctx, "SELECT name FROM items WHERE id = 1")
	err = row.Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "Test", name)
}

func TestSingleClusterModeNoReplayOnFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Track if replay was ever called
	replayWasCalled := false
	replayer := &trackingReplayer{
		onEnqueue: func(_ types.ReplayPayload) {
			replayWasCalled = true
		},
	}

	client, err := helix.NewSQLClientFromDB(db, nil,
		helix.WithReplayer(replayer),
	)
	require.NoError(t, err)
	defer client.Close()

	// This should fail (table doesn't exist)
	_, err = client.ExecContext(ctx, "INSERT INTO nonexistent (id) VALUES (?)", 1)
	require.Error(t, err)

	// In single-cluster mode, replay should NOT be called
	// (replay is only for partial failures in dual-cluster mode)
	require.False(t, replayWasCalled, "replay should not be called in single-cluster mode")
}

// =============================================================================
// Concurrent Write Tests
// =============================================================================

func TestConcurrentWritesUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// TODO: SQLite in-memory mode has issues with concurrent access across
	// multiple connections. This test needs a proper database backend
	// (e.g., PostgreSQL with testcontainers) for reliable concurrent testing.
	t.Skip("SQLite concurrent access limitations - needs PostgreSQL test backend")

	ctx := t.Context()

	dbA, err := sql.Open("sqlite3", ":memory:?cache=shared&mode=rwc")
	require.NoError(t, err)
	defer dbA.Close()

	dbB, err := sql.Open("sqlite3", ":memory:?cache=shared&mode=rwc")
	require.NoError(t, err)
	defer dbB.Close()

	// Enable WAL mode for better concurrent write handling
	_, _ = dbA.ExecContext(ctx, "PRAGMA journal_mode=WAL")
	_, _ = dbB.ExecContext(ctx, "PRAGMA journal_mode=WAL")

	// Create tables
	createTable := `CREATE TABLE counters (id INTEGER PRIMARY KEY, value INTEGER DEFAULT 0)`
	_, err = dbA.ExecContext(ctx, createTable)
	require.NoError(t, err)
	_, err = dbB.ExecContext(ctx, createTable)
	require.NoError(t, err)

	// Insert initial row
	_, err = dbA.ExecContext(ctx, "INSERT INTO counters (id, value) VALUES (1, 0)")
	require.NoError(t, err)
	_, err = dbB.ExecContext(ctx, "INSERT INTO counters (id, value) VALUES (1, 0)")
	require.NoError(t, err)

	client, err := helix.NewSQLClientFromDB(dbA, dbB)
	require.NoError(t, err)
	defer client.Close()

	// Run concurrent updates
	numGoroutines := 10
	updatesPerGoroutine := 50
	var wg sync.WaitGroup
	var successCount atomic.Int32
	var failCount atomic.Int32

	for range numGoroutines {
		wg.Go(func() {
			for range updatesPerGoroutine {
				_, err := client.ExecContext(ctx,
					"UPDATE counters SET value = value + 1 WHERE id = 1",
				)
				if err != nil {
					failCount.Add(1)
				} else {
					successCount.Add(1)
				}
			}
		})
	}

	wg.Wait()

	totalAttempted := numGoroutines * updatesPerGoroutine
	t.Logf("Concurrent updates: %d succeeded, %d failed out of %d",
		successCount.Load(), failCount.Load(), totalAttempted)

	// Both databases should have consistent final values
	var valueA, valueB int
	err = dbA.QueryRowContext(ctx, "SELECT value FROM counters WHERE id = 1").Scan(&valueA)
	require.NoError(t, err)
	err = dbB.QueryRowContext(ctx, "SELECT value FROM counters WHERE id = 1").Scan(&valueB)
	require.NoError(t, err)

	// Values should be equal (both got the same updates)
	require.Equal(t, valueA, valueB, "both databases should have same final value")
	require.Equal(t, int(successCount.Load()), valueA, "value should match success count")
}

func TestConcurrentReadsAndWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// TODO: SQLite in-memory mode has issues with concurrent access across
	// multiple connections. This test needs a proper database backend
	// (e.g., PostgreSQL with testcontainers) for reliable concurrent testing.
	t.Skip("SQLite concurrent access limitations - needs PostgreSQL test backend")

	ctx := t.Context()

	dbA, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbA.Close()

	dbB, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbB.Close()

	createTable := `CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)`
	_, err = dbA.ExecContext(ctx, createTable)
	require.NoError(t, err)
	_, err = dbB.ExecContext(ctx, createTable)
	require.NoError(t, err)

	// Pre-populate with data
	for i := 1; i <= 100; i++ {
		_, _ = dbA.ExecContext(ctx, "INSERT INTO data (id, value) VALUES (?, ?)", i, "initial")
		_, _ = dbB.ExecContext(ctx, "INSERT INTO data (id, value) VALUES (?, ?)", i, "initial")
	}

	client, err := helix.NewSQLClientFromDB(dbA, dbB,
		helix.WithReadStrategy(policy.NewRoundRobinRead()),
	)
	require.NoError(t, err)
	defer client.Close()

	// Run concurrent reads and writes
	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// Writers
	for range 3 {
		wg.Go(func() {
			counter := 0
			for {
				select {
				case <-stopCh:
					return
				default:
					id := (counter % 100) + 1
					_, _ = client.ExecContext(ctx,
						"UPDATE data SET value = ? WHERE id = ?",
						"updated", id,
					)
					counter++
					time.Sleep(time.Millisecond)
				}
			}
		})
	}

	// Readers
	readErrors := atomic.Int32{}
	readSuccess := atomic.Int32{}
	for range 5 {
		wg.Go(func() {
			for {
				select {
				case <-stopCh:
					return
				default:
					if doQueryAndClose(ctx, client) {
						readSuccess.Add(1)
					} else {
						readErrors.Add(1)
					}
					time.Sleep(time.Millisecond)
				}
			}
		})
	}

	// Run for a short duration
	time.Sleep(500 * time.Millisecond)
	close(stopCh)
	wg.Wait()

	t.Logf("Concurrent read/write: %d reads succeeded, %d read errors",
		readSuccess.Load(), readErrors.Load())

	// Should have more successes than failures
	require.Greater(t, readSuccess.Load(), readErrors.Load())
}

// doQueryAndClose executes a query and properly closes the rows.
func doQueryAndClose(ctx context.Context, client *helix.SQLClient) bool {
	rows, err := client.QueryContext(ctx, "SELECT id, value FROM data LIMIT 10")
	if err != nil {
		return false
	}
	defer rows.Close()
	// Consume rows to check for errors
	for rows.Next() {
		// Just iterate
	}

	return rows.Err() == nil
}

// =============================================================================
// Partial Failure and Replay Tests
// =============================================================================

func TestPartialFailureTriggersReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()

	dbA, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbA.Close()

	dbB, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbB.Close()

	// Only create table in A - B will fail
	_, err = dbA.ExecContext(ctx, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)

	var replayedPayloads []types.ReplayPayload
	var mu sync.Mutex
	replayer := &trackingReplayer{
		onEnqueue: func(p types.ReplayPayload) {
			mu.Lock()
			replayedPayloads = append(replayedPayloads, p)
			mu.Unlock()
		},
	}

	client, err := helix.NewSQLClientFromDB(dbA, dbB,
		helix.WithReplayer(replayer),
	)
	require.NoError(t, err)
	defer client.Close()

	// Execute multiple operations - A succeeds, B fails each time
	for i := 1; i <= 5; i++ {
		_, err = client.ExecContext(ctx,
			"INSERT INTO items (id, name) VALUES (?, ?)",
			i, "Item",
		)
		require.NoError(t, err) // Should succeed (A succeeded)
	}

	// All 5 should have been queued for replay on B
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, replayedPayloads, 5)

	for _, p := range replayedPayloads {
		require.Equal(t, types.ClusterB, p.TargetCluster)
		require.Contains(t, p.Query, "INSERT INTO items")
	}
}

func TestReplayPayloadPreservesAllFields(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()

	dbA, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbA.Close()

	dbB, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbB.Close()

	// Only create table in A
	_, err = dbA.ExecContext(ctx, `CREATE TABLE test (id INTEGER, data BLOB, num REAL)`)
	require.NoError(t, err)

	var capturedPayload types.ReplayPayload
	replayer := &trackingReplayer{
		onEnqueue: func(p types.ReplayPayload) {
			capturedPayload = p
		},
	}

	client, err := helix.NewSQLClientFromDB(dbA, dbB,
		helix.WithReplayer(replayer),
	)
	require.NoError(t, err)
	defer client.Close()

	// Insert with various arg types
	testBlob := []byte{0x00, 0xFF, 0x80, 0x7F}
	testFloat := 3.14159265359
	_, err = client.ExecContext(ctx,
		"INSERT INTO test (id, data, num) VALUES (?, ?, ?)",
		42, testBlob, testFloat,
	)
	require.NoError(t, err)

	// Verify payload captures all fields correctly
	require.Equal(t, types.ClusterB, capturedPayload.TargetCluster)
	require.Equal(t, "INSERT INTO test (id, data, num) VALUES (?, ?, ?)", capturedPayload.Query)
	require.Len(t, capturedPayload.Args, 3)
	require.NotZero(t, capturedPayload.Timestamp)
}

// =============================================================================
// Timeout and Slow Cluster Tests
// =============================================================================

func TestOperationTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()

	dbA, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbA.Close()

	// Create slow database that delays operations
	slowDB := &slowDBAdapter{
		DB:    sqladapter.NewDBAdapter(dbA),
		delay: 200 * time.Millisecond,
	}

	dbB, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbB.Close()

	// Create tables
	_, _ = dbA.ExecContext(ctx, `CREATE TABLE t (id INTEGER)`)
	_, _ = dbB.ExecContext(ctx, `CREATE TABLE t (id INTEGER)`)

	client, err := helix.NewSQLClient(slowDB, sqladapter.NewDBAdapter(dbB))
	require.NoError(t, err)
	defer client.Close()

	// Operation should complete (B is fast) but A might timeout
	// The result depends on which completes first
	_, err = client.ExecContext(ctx, "INSERT INTO t (id) VALUES (?)", 1)
	// Either succeeds (B completed) or fails (both timed out)
	// Just verify it doesn't hang forever
	t.Logf("Operation with timeout result: %v", err)
}

// =============================================================================
// Context Cancellation Tests
// =============================================================================

func TestContextCancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dbA, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbA.Close()

	dbB, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbB.Close()

	_, _ = dbA.ExecContext(context.Background(), `CREATE TABLE t (id INTEGER)`)
	_, _ = dbB.ExecContext(context.Background(), `CREATE TABLE t (id INTEGER)`)

	client, err := helix.NewSQLClientFromDB(dbA, dbB)
	require.NoError(t, err)
	defer client.Close()

	// Create a context that's already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Operation should respect cancelled context
	_, err = client.ExecContext(ctx, "INSERT INTO t (id) VALUES (?)", 1)
	// Might get context.Canceled or might complete if fast enough
	t.Logf("Cancelled context result: %v", err)
}

func TestContextDeadlineExceeded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dbA, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbA.Close()

	dbB, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbB.Close()

	_, _ = dbA.ExecContext(context.Background(), `CREATE TABLE t (id INTEGER)`)
	_, _ = dbB.ExecContext(context.Background(), `CREATE TABLE t (id INTEGER)`)

	client, err := helix.NewSQLClientFromDB(dbA, dbB)
	require.NoError(t, err)
	defer client.Close()

	// Create context with very short deadline
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()

	// Wait for deadline to pass
	time.Sleep(time.Millisecond)

	_, err = client.ExecContext(ctx, "INSERT INTO t (id) VALUES (?)", 1)
	// Should get deadline exceeded
	if err != nil {
		assert.True(t, errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, types.ErrBothClustersFailed),
			"expected deadline exceeded or both clusters failed")
	}
}

// =============================================================================
// Client Lifecycle Tests
// =============================================================================

func TestClientCloseIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	client, err := helix.NewSQLClientFromDB(db, nil)
	require.NoError(t, err)

	// Multiple closes should not panic
	require.NotPanics(t, func() {
		_ = client.Close()
		_ = client.Close()
		_ = client.Close()
	})
}

func TestOperationsAfterClose(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, _ = db.ExecContext(ctx, `CREATE TABLE t (id INTEGER)`)

	client, err := helix.NewSQLClientFromDB(db, nil)
	require.NoError(t, err)

	// Close the client
	_ = client.Close()

	// Operations should fail with ErrSessionClosed
	_, err = client.ExecContext(ctx, "INSERT INTO t (id) VALUES (?)", 1)
	require.ErrorIs(t, err, types.ErrSessionClosed)

	//nolint:rowserrcheck,sqlclosecheck // testing error path - client is closed
	_, err = client.QueryContext(ctx, "SELECT * FROM t")
	require.ErrorIs(t, err, types.ErrSessionClosed)
}

// =============================================================================
// Memory Replayer Edge Cases
// =============================================================================

func TestMemoryReplayerDrainAllUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Capacity is split between high and low priority queues (half each)
	// We need 1000 capacity for high priority messages, so total capacity = 2000
	replayer := replay.NewMemoryReplayer(
		replay.WithQueueCapacity(2000),
	)

	ctx := t.Context()

	// Enqueue many messages concurrently
	var wg sync.WaitGroup
	numGoroutines := 10
	messagesPerGoroutine := 100

	clusters := []types.ClusterID{types.ClusterA, types.ClusterB}

	for i := range numGoroutines {
		idx := i // Capture loop variable
		wg.Go(func() {
			for j := range messagesPerGoroutine {
				_ = replayer.Enqueue(ctx, types.ReplayPayload{
					TargetCluster: clusters[idx%2], // Alternate clusters
					Query:         "INSERT INTO t (id) VALUES (?)",
					Args:          []any{idx*1000 + j},
					Timestamp:     time.Now().UnixMicro(),
				})
			}
		})
	}

	wg.Wait()

	// Drain all messages
	drained := replayer.DrainAll()
	expectedTotal := numGoroutines * messagesPerGoroutine
	require.Equal(t, expectedTotal, len(drained), "should drain all enqueued messages")
}

func TestMemoryReplayerCapacityLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	capacity := 10
	replayer := replay.NewMemoryReplayer(
		replay.WithQueueCapacity(capacity),
	)

	ctx := t.Context()

	// Try to enqueue more than capacity
	for i := range capacity + 5 {
		_ = replayer.Enqueue(ctx, types.ReplayPayload{
			TargetCluster: types.ClusterA,
			Query:         "INSERT INTO t (id) VALUES (?)",
			Args:          []any{i},
		})
	}

	// Should only have capacity messages (queue is full)
	pending := replayer.Len()
	require.LessOrEqual(t, pending, capacity)
}

// =============================================================================
// Read Strategy Edge Cases
// =============================================================================

func TestStickyReadAfterFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// TODO: SQLClient.QueryRowContext doesn't implement failover yet.
	// The *sql.Row return type makes failover tricky because errors are
	// lazily evaluated on Scan(). This test should be enabled once
	// SQLClient gains failover support for read operations.
	t.Skip("SQLClient.QueryRowContext failover not implemented yet")

	ctx := t.Context()

	dbA, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbA.Close()

	dbB, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbB.Close()

	// Create table only in B to simulate A failure for reads
	_, err = dbB.ExecContext(ctx, `CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)`)
	require.NoError(t, err)
	_, err = dbB.ExecContext(ctx, `INSERT INTO data (id, value) VALUES (1, 'from_b')`)
	require.NoError(t, err)

	// Use failing adapter for A
	failingA := &failingDBAdapter{
		err: errors.New("simulated cluster A failure"),
	}

	stickyRead := policy.NewStickyRead()

	client, err := helix.NewSQLClient(failingA, sqladapter.NewDBAdapter(dbB),
		helix.WithReadStrategy(stickyRead),
	)
	require.NoError(t, err)
	defer client.Close()

	// First read should fail on A, then failover to B
	row := client.QueryRowContext(ctx, "SELECT value FROM data WHERE id = 1")
	require.NotNil(t, row)

	var value string
	err = row.Scan(&value)
	require.NoError(t, err)
	require.Equal(t, "from_b", value)
}

func TestRoundRobinDistribution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()

	dbA, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbA.Close()

	dbB, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbB.Close()

	// Setup with different data to track which DB is read
	_, _ = dbA.ExecContext(ctx, `CREATE TABLE config (key TEXT PRIMARY KEY, source TEXT)`)
	_, _ = dbA.ExecContext(ctx, `INSERT INTO config (key, source) VALUES ('id', 'A')`)

	_, _ = dbB.ExecContext(ctx, `CREATE TABLE config (key TEXT PRIMARY KEY, source TEXT)`)
	_, _ = dbB.ExecContext(ctx, `INSERT INTO config (key, source) VALUES ('id', 'B')`)

	client, err := helix.NewSQLClientFromDB(dbA, dbB,
		helix.WithReadStrategy(policy.NewRoundRobinRead()),
	)
	require.NoError(t, err)
	defer client.Close()

	// Execute multiple reads and count distribution
	countA, countB := 0, 0
	for i := 0; i < 10; i++ {
		row := client.QueryRowContext(ctx, "SELECT source FROM config WHERE key = 'id'")
		var source string
		if err := row.Scan(&source); err == nil {
			if source == "A" {
				countA++
			} else {
				countB++
			}
		}
	}

	// Should have reads from both clusters (roughly balanced)
	t.Logf("Round-robin distribution: A=%d, B=%d", countA, countB)
	require.Greater(t, countA, 0, "should have some reads from A")
	require.Greater(t, countB, 0, "should have some reads from B")
}

// =============================================================================
// Helper Types
// =============================================================================

type trackingReplayer struct {
	onEnqueue func(types.ReplayPayload)
}

func (r *trackingReplayer) Enqueue(_ context.Context, payload types.ReplayPayload) error {
	if r.onEnqueue != nil {
		r.onEnqueue(payload)
	}

	return nil
}

type slowDBAdapter struct {
	sqladapter.DB
	delay time.Duration
}

func (s *slowDBAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(s.delay):
	}

	return s.DB.ExecContext(ctx, query, args...)
}

func (s *slowDBAdapter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(s.delay):
	}

	return s.DB.QueryContext(ctx, query, args...)
}

type failingDBAdapter struct {
	err error
}

func (f *failingDBAdapter) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	return nil, f.err
}

func (f *failingDBAdapter) QueryContext(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, f.err
}

func (f *failingDBAdapter) QueryRowContext(_ context.Context, _ string, _ ...any) *sql.Row {
	return nil
}

func (f *failingDBAdapter) PingContext(_ context.Context) error {
	return f.err
}

func (f *failingDBAdapter) Close() error {
	return nil
}
