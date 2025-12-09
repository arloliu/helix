package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/adapter/cql"
	cqlv1 "github.com/arloliu/helix/adapter/cql/v1"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
)

// =============================================================================
// CQL Single-Cluster Mode Tests
// =============================================================================

func TestCQLSingleClusterModeIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, _ := getSharedSessions(t)
	ctx := t.Context()

	usersTable := createTestTableOnOne(t, sessionA, "users_single")

	// Create single-cluster client
	client, err := helix.NewCQLClient(cqlv1.WrapSession(sessionA), nil)
	require.NoError(t, err)
	require.True(t, client.IsSingleCluster())

	userID := gocql.TimeUUID()

	// Insert via Helix
	err = client.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
		userID, "SingleCluster", "single@example.com", time.Now(),
	).ExecContext(ctx)
	require.NoError(t, err)

	// Read via Helix
	var name string
	err = client.Query(
		"SELECT name FROM "+usersTable+" WHERE id = ?", userID,
	).ScanContext(ctx, &name)
	require.NoError(t, err)
	require.Equal(t, "SingleCluster", name)
}

func TestCQLSingleClusterNoReplayOnError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, _ := getSharedSessions(t)
	ctx := t.Context()

	// Track if replay was called
	replayWasCalled := false
	replayer := &testReplayerTracker{
		onEnqueue: func(_ types.ReplayPayload) {
			replayWasCalled = true
		},
	}

	client, err := helix.NewCQLClient(cqlv1.WrapSession(sessionA), nil,
		helix.WithReplayer(replayer),
	)
	require.NoError(t, err)

	// Query non-existent table - should fail
	err = client.Query(
		"INSERT INTO nonexistent_table_xyz (id) VALUES (?)", gocql.TimeUUID(),
	).ExecContext(ctx)
	require.Error(t, err)

	// In single-cluster mode, replay should NOT be called
	require.False(t, replayWasCalled, "replay should not be called in single-cluster mode")
}

// =============================================================================
// CQL High-Concurrency Tests
// =============================================================================

func TestCQLConcurrentDualWritesIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	eventsTable := createTestTableOnBoth(t, "events_concurrent", eventsTableCQLSchema)

	client, err := helix.NewCQLClient(cqlv1.WrapSession(sessionA), cqlv1.WrapSession(sessionB))
	require.NoError(t, err)

	// Run concurrent inserts
	numGoroutines := 10
	insertsPerGoroutine := 20
	var wg sync.WaitGroup
	var successCount atomic.Int64
	var failCount atomic.Int64

	insertedIDs := make(chan gocql.UUID, numGoroutines*insertsPerGoroutine)

	for range numGoroutines {
		wg.Go(func() {
			for range insertsPerGoroutine {
				eventID := gocql.TimeUUID()
				err := client.Query(
					"INSERT INTO "+eventsTable+" (id, event_type, data, created_at) VALUES (?, ?, ?, ?)",
					eventID, "concurrent_test", "goroutine data", time.Now(),
				).ExecContext(ctx)
				if err != nil {
					failCount.Add(1)
				} else {
					successCount.Add(1)
					insertedIDs <- eventID
				}
			}
		})
	}

	wg.Wait()
	close(insertedIDs)

	t.Logf("Concurrent inserts: %d succeeded, %d failed",
		successCount.Load(), failCount.Load())

	// All inserts should succeed
	require.Equal(t, int64(numGoroutines*insertsPerGoroutine), successCount.Load())

	// Verify all successful inserts exist in both clusters
	verifiedA, verifiedB := 0, 0
	for eventID := range insertedIDs {
		var eventType string
		if err := sessionA.Query("SELECT event_type FROM "+eventsTable+" WHERE id = ?", eventID).Scan(&eventType); err == nil {
			verifiedA++
		}
		if err := sessionB.Query("SELECT event_type FROM "+eventsTable+" WHERE id = ?", eventID).Scan(&eventType); err == nil {
			verifiedB++
		}
	}

	t.Logf("Verified: A=%d, B=%d out of %d", verifiedA, verifiedB, successCount.Load())
	require.Equal(t, int(successCount.Load()), verifiedA)
	require.Equal(t, int(successCount.Load()), verifiedB)
}

func TestCQLConcurrentBatchWritesIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	usersTable := createTestTableOnBoth(t, "users_batch_concurrent", usersTableSchema)

	client, err := helix.NewCQLClient(cqlv1.WrapSession(sessionA), cqlv1.WrapSession(sessionB))
	require.NoError(t, err)

	// Run concurrent batch inserts
	numGoroutines := 5
	batchesPerGoroutine := 10
	usersPerBatch := 3
	var wg sync.WaitGroup
	var successCount atomic.Int64

	for range numGoroutines {
		wg.Go(func() {
			for range batchesPerGoroutine {
				batch := client.Batch(helix.UnloggedBatch)
				for range usersPerBatch {
					batch.Query(
						"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
						gocql.TimeUUID(), "BatchUser", "batch@example.com", time.Now(),
					)
				}
				if err := batch.ExecContext(ctx); err == nil {
					successCount.Add(1)
				}
			}
		})
	}

	wg.Wait()

	t.Logf("Concurrent batch inserts: %d succeeded out of %d",
		successCount.Load(), numGoroutines*batchesPerGoroutine)

	// Most batches should succeed
	require.GreaterOrEqual(t, successCount.Load(), int64(numGoroutines*batchesPerGoroutine*8/10))
}

// =============================================================================
// CQL Timestamp Consistency Tests
// =============================================================================

func TestCQLTimestampConsistencyIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	productsTable := createTestTableOnBoth(t, "products_ts", productsTableSchema)

	client, err := helix.NewCQLClient(cqlv1.WrapSession(sessionA), cqlv1.WrapSession(sessionB))
	require.NoError(t, err)

	productID := gocql.TimeUUID()

	// Insert with explicit timestamp
	ts := time.Now().UnixMicro()
	err = client.Query(
		"INSERT INTO "+productsTable+" (id, name, price, stock) VALUES (?, ?, ?, ?)",
		productID, "TimestampTest", 10.00, 100,
	).WithTimestamp(ts).ExecContext(ctx)
	require.NoError(t, err)

	// Update with later timestamp
	ts2 := ts + 1000 // 1ms later
	err = client.Query(
		"UPDATE "+productsTable+" SET price = ? WHERE id = ?",
		20.00, productID,
	).WithTimestamp(ts2).ExecContext(ctx)
	require.NoError(t, err)

	// Both clusters should have the updated price
	var priceA, priceB float64
	err = sessionA.Query("SELECT price FROM "+productsTable+" WHERE id = ?", productID).Scan(&priceA)
	require.NoError(t, err)
	err = sessionB.Query("SELECT price FROM "+productsTable+" WHERE id = ?", productID).Scan(&priceB)
	require.NoError(t, err)

	require.Equal(t, 20.00, priceA)
	require.Equal(t, 20.00, priceB)
}

func TestCQLLWTDualWriteIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	usersTable := createTestTableOnBoth(t, "users_lwt", usersTableSchema)

	client, err := helix.NewCQLClient(cqlv1.WrapSession(sessionA), cqlv1.WrapSession(sessionB))
	require.NoError(t, err)

	userID := gocql.TimeUUID()

	// Insert with IF NOT EXISTS (LWT)
	err = client.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
		userID, "LWTUser", "lwt@example.com", time.Now(),
	).ExecContext(ctx)
	require.NoError(t, err)

	// Verify exists in both
	var name string
	err = sessionA.Query("SELECT name FROM "+usersTable+" WHERE id = ?", userID).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "LWTUser", name)

	err = sessionB.Query("SELECT name FROM "+usersTable+" WHERE id = ?", userID).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "LWTUser", name)
}

// =============================================================================
// CQL Iterator Edge Cases
// =============================================================================

func TestCQLIteratorLargeResultSetIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	eventsTable := createTestTableOnBoth(t, "events_large", eventsTableCQLSchema)

	client, err := helix.NewCQLClient(cqlv1.WrapSession(sessionA), cqlv1.WrapSession(sessionB))
	require.NoError(t, err)

	// Insert many rows
	numRows := 100
	for range numRows {
		err = client.Query(
			"INSERT INTO "+eventsTable+" (id, event_type, data, created_at) VALUES (?, ?, ?, ?)",
			gocql.TimeUUID(), "large_test", "test data", time.Now(),
		).ExecContext(ctx)
		require.NoError(t, err)
	}

	// Read with small page size
	iter := client.Query("SELECT id, event_type FROM " + eventsTable).
		PageSize(10).
		IterContext(ctx)

	var id gocql.UUID
	var eventType string
	count := 0
	for iter.Scan(&id, &eventType) {
		count++
	}
	require.NoError(t, iter.Close())
	require.Equal(t, numRows, count)
}

func TestCQLIteratorEmptyResultIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	usersTable := createTestTableOnBoth(t, "users_empty", usersTableSchema)

	client, err := helix.NewCQLClient(cqlv1.WrapSession(sessionA), cqlv1.WrapSession(sessionB))
	require.NoError(t, err)

	// Query empty table
	iter := client.Query("SELECT id, name FROM " + usersTable).IterContext(ctx)

	var id gocql.UUID
	var name string
	count := 0
	for iter.Scan(&id, &name) {
		count++
	}
	require.NoError(t, iter.Close())
	require.Equal(t, 0, count)
}

// =============================================================================
// CQL Replay Integration Tests
// =============================================================================

func TestCQLReplayWorkerEndToEndIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	ordersTable := createTestTableOnBoth(t, "orders_replay", ordersTableCQLSchema)

	// Create memory replayer
	memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100))

	// Track simulated partial failures
	var partialFailureCount atomic.Int32
	partialFailureSession := &partialFailureCQLSession{
		session:       cqlv1.WrapSession(sessionB),
		failNextWrite: &partialFailureCount,
	}

	// Create client with replay
	client, err := helix.NewCQLClient(cqlv1.WrapSession(sessionA), partialFailureSession,
		helix.WithReplayer(memReplayer),
	)
	require.NoError(t, err)

	// Simulate partial failures
	partialFailureCount.Store(3) // Next 3 writes to B will fail

	// Insert orders - A will succeed, B will fail
	orderIDs := make([]gocql.UUID, 3)
	for i := range 3 {
		orderIDs[i] = gocql.TimeUUID()
		err = client.Query(
			"INSERT INTO "+ordersTable+" (id, user_id, total, status) VALUES (?, ?, ?, ?)",
			orderIDs[i], gocql.TimeUUID(), 99.99, "pending",
		).ExecContext(ctx)
		require.NoError(t, err) // Should succeed (A succeeded)
	}

	// Verify replayer has queued messages for B
	pending := memReplayer.Len()
	assert.Equal(t, 3, pending)

	// Drain and verify payloads
	msgs := memReplayer.DrainAll()
	assert.Len(t, msgs, 3)

	for _, msg := range msgs {
		assert.Equal(t, types.ClusterB, msg.TargetCluster)
		assert.Contains(t, msg.Query, "INSERT INTO")
	}
}

// =============================================================================
// CQL Failover Edge Cases
// =============================================================================

func TestCQLReadFailoverIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	configTable := createTestTableOnBoth(t, "config_failover", configTableCQLSchema)

	// Insert data in both clusters with different values to track which is read
	err := sessionA.Query("INSERT INTO "+configTable+" (key, value) VALUES (?, ?)",
		"source", "cluster_a").Exec()
	require.NoError(t, err)
	err = sessionB.Query("INSERT INTO "+configTable+" (key, value) VALUES (?, ?)",
		"source", "cluster_b").Exec()
	require.NoError(t, err)

	// Create client with active failover
	client, err := helix.NewCQLClient(cqlv1.WrapSession(sessionA), cqlv1.WrapSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead()),
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
	)
	require.NoError(t, err)

	// Read should succeed (uses primary by default)
	var value string
	err = client.Query("SELECT value FROM "+configTable+" WHERE key = ?", "source").
		ScanContext(ctx, &value)
	require.NoError(t, err)
	assert.Contains(t, []string{"cluster_a", "cluster_b"}, value)
}

// =============================================================================
// CQL TTL and Tombstone Tests
// =============================================================================

func TestCQLTTLDualWriteIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	cacheTable := createTestTableOnBoth(t, "cache_ttl", cacheTableCQLSchema)

	client, err := helix.NewCQLClient(cqlv1.WrapSession(sessionA), cqlv1.WrapSession(sessionB))
	require.NoError(t, err)

	// Insert with TTL
	err = client.Query(
		"INSERT INTO "+cacheTable+" (key, value) VALUES (?, ?) USING TTL 3",
		"temp_key", "temp_value",
	).ExecContext(ctx)
	require.NoError(t, err)

	// Verify exists immediately in both
	var value string
	err = sessionA.Query("SELECT value FROM "+cacheTable+" WHERE key = ?", "temp_key").Scan(&value)
	require.NoError(t, err)
	require.Equal(t, "temp_value", value)

	err = sessionB.Query("SELECT value FROM "+cacheTable+" WHERE key = ?", "temp_key").Scan(&value)
	require.NoError(t, err)
	require.Equal(t, "temp_value", value)

	// Wait for TTL to expire
	time.Sleep(4 * time.Second)

	// Should be gone from both clusters
	err = sessionA.Query("SELECT value FROM "+cacheTable+" WHERE key = ?", "temp_key").Scan(&value)
	require.Error(t, err) // Not found

	err = sessionB.Query("SELECT value FROM "+cacheTable+" WHERE key = ?", "temp_key").Scan(&value)
	require.Error(t, err) // Not found
}

// =============================================================================
// Helper Types and Functions
// =============================================================================

// testReplayerTracker is a minimal replayer that tracks enqueue calls.
type testReplayerTracker struct {
	onEnqueue func(types.ReplayPayload)
}

func (r *testReplayerTracker) Enqueue(_ context.Context, payload types.ReplayPayload) error {
	if r.onEnqueue != nil {
		r.onEnqueue(payload)
	}

	return nil
}

// partialFailureCQLSession wraps a CQL session (adapter interface) and fails N writes.
type partialFailureCQLSession struct {
	session       cql.Session
	failNextWrite *atomic.Int32
}

func (p *partialFailureCQLSession) Query(stmt string, values ...any) cql.Query {
	return &partialFailureCQLQuery{
		Query:         p.session.Query(stmt, values...),
		failNextWrite: p.failNextWrite,
	}
}

func (p *partialFailureCQLSession) Batch(kind cql.BatchType) cql.Batch {
	return p.session.Batch(kind)
}

func (p *partialFailureCQLSession) NewBatch(kind cql.BatchType) cql.Batch {
	return p.Batch(kind)
}

func (p *partialFailureCQLSession) ExecuteBatch(batch cql.Batch) error {
	return batch.Exec()
}

func (p *partialFailureCQLSession) ExecuteBatchCAS(batch cql.Batch, dest ...any) (applied bool, iter cql.Iter, err error) {
	return batch.ExecCAS(dest...)
}

func (p *partialFailureCQLSession) MapExecuteBatchCAS(batch cql.Batch, dest map[string]any) (applied bool, iter cql.Iter, err error) {
	return batch.MapExecCAS(dest)
}

func (p *partialFailureCQLSession) Close() {
	// Note: We intentionally don't close the underlying session because
	// it may be wrapping a shared gocql session used across tests.
	// The shared sessions are cleaned up by TestMain's teardown.
}

// partialFailureCQLQuery wraps a CQL query and fails if counter > 0.
type partialFailureCQLQuery struct {
	cql.Query
	failNextWrite *atomic.Int32
}

func (p *partialFailureCQLQuery) WithContext(ctx context.Context) cql.Query {
	p.Query = p.Query.WithContext(ctx)

	return p
}

func (p *partialFailureCQLQuery) Consistency(c cql.Consistency) cql.Query {
	p.Query = p.Query.Consistency(c)

	return p
}

func (p *partialFailureCQLQuery) SetConsistency(c cql.Consistency) {
	p.Consistency(c)
}

func (p *partialFailureCQLQuery) PageSize(n int) cql.Query {
	p.Query = p.Query.PageSize(n)

	return p
}

func (p *partialFailureCQLQuery) PageState(state []byte) cql.Query {
	p.Query = p.Query.PageState(state)

	return p
}

func (p *partialFailureCQLQuery) WithTimestamp(ts int64) cql.Query {
	p.Query = p.Query.WithTimestamp(ts)

	return p
}

func (p *partialFailureCQLQuery) Exec() error {
	if p.failNextWrite.Load() > 0 {
		p.failNextWrite.Add(-1)

		return errors.New("simulated partial failure")
	}

	return p.Query.Exec()
}

func (p *partialFailureCQLQuery) Scan(dest ...any) error {
	return p.Query.Scan(dest...)
}

// createTestTableOnOne creates a table on a single session for single-cluster mode tests.
func createTestTableOnOne(t *testing.T, session *gocql.Session, tableNameSuffix string) string {
	t.Helper()

	tableName := fmt.Sprintf("test_%s_%d", tableNameSuffix, time.Now().UnixNano())

	fullSchema := fmt.Sprintf(usersTableSchema, tableName)
	err := session.Query(fullSchema).Exec()
	require.NoError(t, err, "failed to create table %s", tableName)

	t.Cleanup(func() {
		_ = session.Query("DROP TABLE IF EXISTS " + tableName).Exec()
	})

	return tableName
}

// Additional table schemas for edge case tests.
const eventsTableCQLSchema = `
	CREATE TABLE IF NOT EXISTS %s (
		id UUID PRIMARY KEY,
		event_type TEXT,
		data TEXT,
		created_at TIMESTAMP
	)
`

const ordersTableCQLSchema = `
	CREATE TABLE IF NOT EXISTS %s (
		id UUID PRIMARY KEY,
		user_id UUID,
		total DOUBLE,
		status TEXT
	)
`

const configTableCQLSchema = `
	CREATE TABLE IF NOT EXISTS %s (
		key TEXT PRIMARY KEY,
		value TEXT
	)
`

const cacheTableCQLSchema = `
	CREATE TABLE IF NOT EXISTS %s (
		key TEXT PRIMARY KEY,
		value TEXT
	)
`

// =============================================================================
// AdaptiveDualWrite Integration Tests
// =============================================================================

func TestCQLAdaptiveDualWriteIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	ordersTable := createTestTableOnBoth(t, "orders_adaptive", ordersTableCQLSchema)

	// Create adaptive dual write strategy with low thresholds for testing
	adaptiveWrite := policy.NewAdaptiveDualWrite(
		policy.WithAdaptiveAbsoluteMax(2*time.Second),
		policy.WithAdaptiveDeltaThreshold(100*time.Millisecond),
		policy.WithAdaptiveMinFloor(50*time.Millisecond),
		policy.WithAdaptiveStrikeThreshold(2),
		policy.WithAdaptiveRecoveryThreshold(3),
	)

	client, err := helix.NewCQLClient(cqlv1.WrapSession(sessionA), cqlv1.WrapSession(sessionB),
		helix.WithWriteStrategy(adaptiveWrite),
	)
	require.NoError(t, err)

	// Execute several writes - both clusters healthy
	for i := 0; i < 5; i++ {
		orderID := gocql.TimeUUID()
		err = client.Query(
			"INSERT INTO "+ordersTable+" (id, user_id, total, status) VALUES (?, ?, ?, ?)",
			orderID, gocql.TimeUUID(), float64(i)*10.0, "pending",
		).ExecContext(ctx)
		require.NoError(t, err)
	}

	// Verify neither cluster is degraded after normal writes
	assert.False(t, adaptiveWrite.IsDegraded(types.ClusterA))
	assert.False(t, adaptiveWrite.IsDegraded(types.ClusterB))
}

func TestCQLAdaptiveDualWriteWithReplayIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	ordersTable := createTestTableOnBoth(t, "orders_adaptive_replay", ordersTableCQLSchema)

	// Create memory replayer to capture fire-and-forget failures
	memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100))

	// Create adaptive strategy and force one cluster degraded
	adaptiveWrite := policy.NewAdaptiveDualWrite(
		policy.WithAdaptiveFireForgetTimeout(5 * time.Second),
	)

	client, err := helix.NewCQLClient(cqlv1.WrapSession(sessionA), cqlv1.WrapSession(sessionB),
		helix.WithWriteStrategy(adaptiveWrite),
		helix.WithReplayer(memReplayer),
	)
	require.NoError(t, err)

	// Force degrade cluster B
	adaptiveWrite.ForceDegrade(types.ClusterB)
	require.True(t, adaptiveWrite.IsDegraded(types.ClusterB))

	// Insert orders - A executes sync, B executes async (fire-and-forget)
	orderID := gocql.TimeUUID()
	err = client.Query(
		"INSERT INTO "+ordersTable+" (id, user_id, total, status) VALUES (?, ?, ?, ?)",
		orderID, gocql.TimeUUID(), 99.99, "pending",
	).ExecContext(ctx)
	require.NoError(t, err)

	// Give fire-and-forget time to complete
	time.Sleep(100 * time.Millisecond)

	// Verify data exists in both clusters (B got fire-and-forget write)
	var status string
	err = sessionA.Query("SELECT status FROM "+ordersTable+" WHERE id = ?", orderID).Scan(&status)
	require.NoError(t, err)
	require.Equal(t, "pending", status)

	err = sessionB.Query("SELECT status FROM "+ordersTable+" WHERE id = ?", orderID).Scan(&status)
	require.NoError(t, err)
	require.Equal(t, "pending", status)
}

func TestCQLAdaptiveDualWriteRecoveryIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	ordersTable := createTestTableOnBoth(t, "orders_recovery", ordersTableCQLSchema)

	// Create adaptive strategy with low recovery threshold
	adaptiveWrite := policy.NewAdaptiveDualWrite(
		policy.WithAdaptiveRecoveryThreshold(3),
		policy.WithAdaptiveMinFloor(10*time.Millisecond),
	)

	client, err := helix.NewCQLClient(cqlv1.WrapSession(sessionA), cqlv1.WrapSession(sessionB),
		helix.WithWriteStrategy(adaptiveWrite),
	)
	require.NoError(t, err)

	// Force degrade cluster A
	adaptiveWrite.ForceDegrade(types.ClusterA)
	require.True(t, adaptiveWrite.IsDegraded(types.ClusterA))

	// Simulate recovery by recording fast writes
	for i := 0; i < 3; i++ {
		adaptiveWrite.RecordFastWrite(types.ClusterA)
	}

	// Cluster A should have recovered
	assert.False(t, adaptiveWrite.IsDegraded(types.ClusterA))

	// Normal writes should work
	orderID := gocql.TimeUUID()
	err = client.Query(
		"INSERT INTO "+ordersTable+" (id, user_id, total, status) VALUES (?, ?, ?, ?)",
		orderID, gocql.TimeUUID(), 50.0, "completed",
	).ExecContext(ctx)
	require.NoError(t, err)
}

// =============================================================================
// LatencyCircuitBreaker Integration Tests
// =============================================================================

func TestCQLLatencyCircuitBreakerIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	configTable := createTestTableOnBoth(t, "config_lcb", configTableCQLSchema)

	// Insert test data in both clusters
	err := sessionA.Query("INSERT INTO "+configTable+" (key, value) VALUES (?, ?)", "test_key", "value_a").Exec()
	require.NoError(t, err)
	err = sessionB.Query("INSERT INTO "+configTable+" (key, value) VALUES (?, ?)", "test_key", "value_b").Exec()
	require.NoError(t, err)

	// Create latency circuit breaker with low threshold for testing
	lcb := policy.NewLatencyCircuitBreaker(
		policy.WithLatencyAbsoluteMax(1*time.Second),
		policy.WithLatencyThreshold(2),
	)

	client, err := helix.NewCQLClient(cqlv1.WrapSession(sessionA), cqlv1.WrapSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(types.ClusterA),
			policy.WithStickyReadCooldown(100*time.Millisecond),
		)),
		helix.WithFailoverPolicy(lcb),
	)
	require.NoError(t, err)

	// Read should succeed
	var value string
	err = client.Query("SELECT value FROM "+configTable+" WHERE key = ?", "test_key").ScanContext(ctx, &value)
	require.NoError(t, err)
	require.Contains(t, []string{"value_a", "value_b"}, value)
}

func TestCQLLatencyCircuitBreakerTripsOnSlowReads(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Create a latency circuit breaker and verify behavior
	lcb := policy.NewLatencyCircuitBreaker(
		policy.WithLatencyAbsoluteMax(100*time.Millisecond),
		policy.WithLatencyThreshold(3),
	)

	// Record slow latencies (above threshold)
	lcb.RecordLatency(types.ClusterA, 200*time.Millisecond)
	lcb.RecordLatency(types.ClusterA, 250*time.Millisecond)
	assert.Equal(t, 2, lcb.Failures(types.ClusterA))
	assert.False(t, lcb.ShouldFailover(types.ClusterA, nil))

	// Third slow response should trip circuit
	lcb.RecordLatency(types.ClusterA, 300*time.Millisecond)
	assert.Equal(t, 3, lcb.Failures(types.ClusterA))
	assert.True(t, lcb.ShouldFailover(types.ClusterA, nil))

	// Fast response should reset counter
	lcb.RecordLatency(types.ClusterA, 50*time.Millisecond)
	assert.Equal(t, 0, lcb.Failures(types.ClusterA))
}

func TestCQLLatencyCircuitBreakerWithFailoverIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	configTable := createTestTableOnBoth(t, "config_failover_lcb", configTableCQLSchema)

	// Insert different values to track which cluster is read
	err := sessionA.Query("INSERT INTO "+configTable+" (key, value) VALUES (?, ?)", "source", "cluster_a").Exec()
	require.NoError(t, err)
	err = sessionB.Query("INSERT INTO "+configTable+" (key, value) VALUES (?, ?)", "source", "cluster_b").Exec()
	require.NoError(t, err)

	// Use a slow session wrapper for cluster A to simulate latency
	slowSessionA := &testutil.SlowCQLSession{
		Session: cqlv1.WrapSession(sessionA),
		Delay:   50 * time.Millisecond, // Small delay - won't trip breaker
	}

	lcb := policy.NewLatencyCircuitBreaker(
		policy.WithLatencyAbsoluteMax(1*time.Second),
		policy.WithLatencyThreshold(3),
	)

	client, err := helix.NewCQLClient(slowSessionA, cqlv1.WrapSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(types.ClusterA),
			policy.WithStickyReadCooldown(10*time.Millisecond),
		)),
		helix.WithFailoverPolicy(lcb),
	)
	require.NoError(t, err)

	// Read should succeed from slow cluster A (delay < absoluteMax)
	var value string
	err = client.Query("SELECT value FROM "+configTable+" WHERE key = ?", "source").ScanContext(ctx, &value)
	require.NoError(t, err)
	// Value can be from either cluster depending on sticky preference
	require.Contains(t, []string{"cluster_a", "cluster_b"}, value)
}

// =============================================================================
// Enhanced AdaptiveDualWrite Tests - Natural Degradation
// =============================================================================

// TestCQLAdaptiveDualWriteNaturalDegradationIntegration tests that AdaptiveDualWrite
// naturally degrades a slow cluster based on actual latency (not forced).
func TestCQLAdaptiveDualWriteNaturalDegradationIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	ordersTable := createTestTableOnBoth(t, "orders_natural_degrade", ordersTableCQLSchema)

	// Create slow session for cluster B that exceeds absoluteMax
	slowSessionB := &testutil.SlowCQLSession{
		Session: cqlv1.WrapSession(sessionB),
		Delay:   300 * time.Millisecond, // Higher than absoluteMax of 100ms
	}

	// Create adaptive strategy with low thresholds for testing
	adaptiveWrite := policy.NewAdaptiveDualWrite(
		policy.WithAdaptiveAbsoluteMax(100*time.Millisecond),
		policy.WithAdaptiveStrikeThreshold(2), // 2 slow writes = degraded
		policy.WithAdaptiveMinFloor(10*time.Millisecond),
	)

	// Create metrics collector
	metricsCollector := testutil.NewTestMetricsCollector()

	client, err := helix.NewCQLClient(cqlv1.WrapSession(sessionA), slowSessionB,
		helix.WithWriteStrategy(adaptiveWrite),
		helix.WithMetrics(metricsCollector),
	)
	require.NoError(t, err)
	// Note: We don't defer client.Close() because it would close the shared gocql sessions.

	// Initially, B should not be degraded
	assert.False(t, adaptiveWrite.IsDegraded(types.ClusterB), "B should start healthy")

	// Execute writes - B will be slow and accumulate strikes
	for i := 0; i < 3; i++ {
		orderID := gocql.TimeUUID()
		err = client.Query(
			"INSERT INTO "+ordersTable+" (id, user_id, total, status) VALUES (?, ?, ?, ?)",
			orderID, gocql.TimeUUID(), float64(i*10), "degradation_test",
		).ExecContext(ctx)
		require.NoError(t, err)
	}

	// After 3 slow writes (strikeThreshold=2), B should be degraded
	assert.True(t, adaptiveWrite.IsDegraded(types.ClusterB),
		"ClusterB should be degraded after slow writes exceeding absoluteMax")

	// Cluster A should still be healthy
	assert.False(t, adaptiveWrite.IsDegraded(types.ClusterA))

	t.Log("AdaptiveDualWrite natural degradation test passed: B degraded after slow writes")
}
