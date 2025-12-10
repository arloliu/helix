package integration_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	cqlv1 "github.com/arloliu/helix/adapter/cql/v1"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
)

// =============================================================================
// WithAutoMemoryWorker Integration Tests
// =============================================================================

func TestCQLWithAutoMemoryWorkerCreatesReplayerAndWorker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	ordersTable := createTestTableOnBoth(t, "orders_auto_worker", ordersTableCQLSchema)

	// Create client with auto memory worker (default capacity)
	// This should not error - auto replayer and worker are created internally
	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithAutoMemoryWorker(0), // Use default capacity (10000)
	)
	require.NoError(t, err, "WithAutoMemoryWorker should create client without error")

	// Insert some data - should succeed normally
	orderID := gocql.TimeUUID()
	err = client.Query(
		"INSERT INTO "+ordersTable+" (id, user_id, total, status) VALUES (?, ?, ?, ?)",
		orderID, gocql.TimeUUID(), 99.99, "pending",
	).ExecContext(ctx)
	require.NoError(t, err)

	// Verify data exists in both clusters
	var status string
	err = sessionA.Query("SELECT status FROM "+ordersTable+" WHERE id = ?", orderID).Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, "pending", status)

	err = sessionB.Query("SELECT status FROM "+ordersTable+" WHERE id = ?", orderID).Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, "pending", status)

	// Note: We don't close the client because it would close the shared sessions.
	// The auto-created worker will be stopped when client.Close() is called.
}

func TestCQLWithAutoMemoryWorkerCustomCapacity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)

	// Create client with custom capacity - should not error
	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithAutoMemoryWorker(500), // Custom capacity
	)
	require.NoError(t, err, "WithAutoMemoryWorker with custom capacity should not error")
	_ = client // Use client to avoid unused variable warning
}

func TestCQLWithAutoMemoryWorkerReplayOnPartialFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	ordersTable := createTestTableOnBoth(t, "orders_auto_replay", ordersTableCQLSchema)

	// Track execution via custom success callback
	var replayedCount atomic.Int32
	var replayedPayloads []types.ReplayPayload

	// Use failing session for B to trigger replay
	var partialFailureCount atomic.Int32
	partialFailureSession := &partialFailureCQLSession{
		session:       cqlv1.WrapSession(sessionB),
		failNextWrite: &partialFailureCount,
	}

	// Create client with auto memory worker and success tracking
	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		partialFailureSession,
		helix.WithAutoMemoryWorker(100,
			replay.WithPollInterval(10*time.Millisecond),
			replay.WithOnSuccess(func(p types.ReplayPayload) {
				replayedCount.Add(1)
				replayedPayloads = append(replayedPayloads, p)
			}),
		),
	)
	require.NoError(t, err)

	// Simulate 2 partial failures to B
	partialFailureCount.Store(2)

	// Insert orders - A succeeds, B fails and goes to replay queue
	orderIDs := make([]gocql.UUID, 2)
	for i := range 2 {
		orderIDs[i] = gocql.TimeUUID()
		err = client.Query(
			"INSERT INTO "+ordersTable+" (id, user_id, total, status) VALUES (?, ?, ?, ?)",
			orderIDs[i], gocql.TimeUUID(), float64(i)*10.0, "pending",
		).ExecContext(ctx)
		require.NoError(t, err) // A succeeded
	}

	// Wait for replay to process the queued messages
	require.Eventually(t, func() bool {
		return replayedCount.Load() >= 2
	}, 5*time.Second, 50*time.Millisecond, "replay should process enqueued messages")

	// Verify replayed payloads target cluster B
	for _, payload := range replayedPayloads {
		assert.Equal(t, types.ClusterB, payload.TargetCluster)
		assert.Contains(t, payload.Query, "INSERT INTO")
	}

	// Verify data now exists in both clusters after replay
	for _, orderID := range orderIDs {
		var status string
		err = sessionA.Query("SELECT status FROM "+ordersTable+" WHERE id = ?", orderID).Scan(&status)
		require.NoError(t, err)
		assert.Equal(t, "pending", status)

		err = sessionB.Query("SELECT status FROM "+ordersTable+" WHERE id = ?", orderID).Scan(&status)
		require.NoError(t, err)
		assert.Equal(t, "pending", status)
	}
}

// =============================================================================
// DefaultExecuteFunc Integration Tests
// =============================================================================

func TestCQLDefaultExecuteFuncRoutesSingleQueries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	ordersTable := createTestTableOnBoth(t, "orders_exec_func", ordersTableCQLSchema)

	// Create memory replayer manually
	memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100))

	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithReplayer(memReplayer),
	)
	require.NoError(t, err)

	// Get the default execute func
	executeFunc := client.DefaultExecuteFunc()

	// Create a payload targeting cluster A
	orderID := gocql.TimeUUID()
	ts := time.Now().UnixMicro()
	payloadA := types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT INTO " + ordersTable + " (id, user_id, total, status) VALUES (?, ?, ?, ?)",
		Args:          []any{orderID, gocql.TimeUUID(), 50.0, "from_replay_a"},
		Timestamp:     ts,
		Priority:      types.PriorityHigh,
	}

	// Execute via DefaultExecuteFunc
	err = executeFunc(ctx, payloadA)
	require.NoError(t, err)

	// Verify data exists in cluster A only (direct execution, not dual-write)
	var status string
	err = sessionA.Query("SELECT status FROM "+ordersTable+" WHERE id = ?", orderID).Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, "from_replay_a", status)

	// Create a payload targeting cluster B
	orderIDB := gocql.TimeUUID()
	payloadB := types.ReplayPayload{
		TargetCluster: types.ClusterB,
		Query:         "INSERT INTO " + ordersTable + " (id, user_id, total, status) VALUES (?, ?, ?, ?)",
		Args:          []any{orderIDB, gocql.TimeUUID(), 75.0, "from_replay_b"},
		Timestamp:     ts,
		Priority:      types.PriorityHigh,
	}

	// Execute via DefaultExecuteFunc
	err = executeFunc(ctx, payloadB)
	require.NoError(t, err)

	// Verify data exists in cluster B only
	err = sessionB.Query("SELECT status FROM "+ordersTable+" WHERE id = ?", orderIDB).Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, "from_replay_b", status)
}

func TestCQLDefaultExecuteFuncHandlesBatchPayloads(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	ordersTable := createTestTableOnBoth(t, "orders_batch_exec", ordersTableCQLSchema)

	// Create memory replayer manually
	memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100))

	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithReplayer(memReplayer),
	)
	require.NoError(t, err)

	// Get the default execute func
	executeFunc := client.DefaultExecuteFunc()

	// Create batch payload targeting cluster A
	orderIDs := []gocql.UUID{gocql.TimeUUID(), gocql.TimeUUID(), gocql.TimeUUID()}
	ts := time.Now().UnixMicro()

	batchStmts := make([]types.BatchStatement, 0, len(orderIDs))
	for i, orderID := range orderIDs {
		batchStmts = append(batchStmts, types.BatchStatement{
			Query: "INSERT INTO " + ordersTable + " (id, user_id, total, status) VALUES (?, ?, ?, ?)",
			Args:  []any{orderID, gocql.TimeUUID(), float64(i * 100), "batch_replay"},
		})
	}

	batchPayload := types.ReplayPayload{
		TargetCluster:   types.ClusterA,
		IsBatch:         true,
		BatchType:       helix.UnloggedBatch,
		BatchStatements: batchStmts,
		Timestamp:       ts,
		Priority:        types.PriorityHigh,
	}

	// Execute batch via DefaultExecuteFunc
	err = executeFunc(ctx, batchPayload)
	require.NoError(t, err)

	// Verify all batch items exist in cluster A
	for _, orderID := range orderIDs {
		var status string
		err = sessionA.Query("SELECT status FROM "+ordersTable+" WHERE id = ?", orderID).Scan(&status)
		require.NoError(t, err)
		assert.Equal(t, "batch_replay", status)
	}
}

func TestCQLDefaultExecuteFuncPreservesTimestamp(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	ordersTable := createTestTableOnBoth(t, "orders_ts_exec", ordersTableCQLSchema)

	// Create memory replayer manually
	memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100))

	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithReplayer(memReplayer),
	)
	require.NoError(t, err)

	executeFunc := client.DefaultExecuteFunc()

	orderID := gocql.TimeUUID()

	// First insert with older timestamp
	oldTs := time.Now().Add(-time.Hour).UnixMicro()
	payloadOld := types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT INTO " + ordersTable + " (id, user_id, total, status) VALUES (?, ?, ?, ?)",
		Args:          []any{orderID, gocql.TimeUUID(), 100.0, "old_value"},
		Timestamp:     oldTs,
		Priority:      types.PriorityHigh,
	}

	err = executeFunc(ctx, payloadOld)
	require.NoError(t, err)

	// Second insert with newer timestamp (should win)
	newTs := time.Now().UnixMicro()
	payloadNew := types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT INTO " + ordersTable + " (id, user_id, total, status) VALUES (?, ?, ?, ?)",
		Args:          []any{orderID, gocql.TimeUUID(), 200.0, "new_value"},
		Timestamp:     newTs,
		Priority:      types.PriorityHigh,
	}

	err = executeFunc(ctx, payloadNew)
	require.NoError(t, err)

	// Newer timestamp should win - verify we see "new_value"
	var status string
	err = sessionA.Query("SELECT status FROM "+ordersTable+" WHERE id = ?", orderID).Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, "new_value", status)

	// Now replay with even older timestamp (should not overwrite)
	veryOldTs := time.Now().Add(-2 * time.Hour).UnixMicro()
	payloadVeryOld := types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT INTO " + ordersTable + " (id, user_id, total, status) VALUES (?, ?, ?, ?)",
		Args:          []any{orderID, gocql.TimeUUID(), 50.0, "very_old_value"},
		Timestamp:     veryOldTs,
		Priority:      types.PriorityHigh,
	}

	err = executeFunc(ctx, payloadVeryOld)
	require.NoError(t, err)

	// Should still see "new_value" because it has the newest timestamp
	err = sessionA.Query("SELECT status FROM "+ordersTable+" WHERE id = ?", orderID).Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, "new_value", status, "Older timestamp should not overwrite newer value")
}

// =============================================================================
// Full End-to-End Replay Worker Integration Tests
// =============================================================================

func TestCQLReplayWorkerFullEndToEndWithManualSetup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	ordersTable := createTestTableOnBoth(t, "orders_e2e_manual", ordersTableCQLSchema)

	// Manual setup: Create replayer, client, then worker
	memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100))

	// Create client with partial failure on B
	var partialFailureCount atomic.Int32
	partialFailureSession := &partialFailureCQLSession{
		session:       cqlv1.WrapSession(sessionB),
		failNextWrite: &partialFailureCount,
	}

	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		partialFailureSession,
		helix.WithReplayer(memReplayer),
	)
	require.NoError(t, err)

	// Track replayed payloads
	var replayedCount atomic.Int32
	worker := replay.NewMemoryWorker(memReplayer, client.DefaultExecuteFunc(),
		replay.WithPollInterval(10*time.Millisecond),
		replay.WithOnSuccess(func(_ types.ReplayPayload) {
			replayedCount.Add(1)
		}),
	)

	err = worker.Start()
	require.NoError(t, err)
	defer worker.Stop()

	// Simulate partial failure
	partialFailureCount.Store(1)

	// Insert - A succeeds, B fails
	orderID := gocql.TimeUUID()
	err = client.Query(
		"INSERT INTO "+ordersTable+" (id, user_id, total, status) VALUES (?, ?, ?, ?)",
		orderID, gocql.TimeUUID(), 123.45, "e2e_test",
	).ExecContext(ctx)
	require.NoError(t, err)

	// Wait for replay to complete
	require.Eventually(t, func() bool {
		return replayedCount.Load() >= 1
	}, 5*time.Second, 50*time.Millisecond)

	// Verify data exists in both clusters after replay
	var statusA, statusB string
	err = sessionA.Query("SELECT status FROM "+ordersTable+" WHERE id = ?", orderID).Scan(&statusA)
	require.NoError(t, err)
	err = sessionB.Query("SELECT status FROM "+ordersTable+" WHERE id = ?", orderID).Scan(&statusB)
	require.NoError(t, err)

	assert.Equal(t, "e2e_test", statusA)
	assert.Equal(t, "e2e_test", statusB)
}

func TestCQLReplayWorkerWithContextCancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)

	ordersTable := createTestTableOnBoth(t, "orders_ctx_cancel", ordersTableCQLSchema)

	memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100))

	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithReplayer(memReplayer),
	)
	require.NoError(t, err)

	executeFunc := client.DefaultExecuteFunc()

	// Create a cancelable context
	ctx, cancel := context.WithCancel(context.Background())

	// Payload
	payload := types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT INTO " + ordersTable + " (id, user_id, total, status) VALUES (?, ?, ?, ?)",
		Args:          []any{gocql.TimeUUID(), gocql.TimeUUID(), 100.0, "ctx_test"},
		Timestamp:     time.Now().UnixMicro(),
		Priority:      types.PriorityHigh,
	}

	// Cancel context before execution
	cancel()

	// Execute with cancelled context - should fail
	err = executeFunc(ctx, payload)
	assert.Error(t, err, "execution with cancelled context should fail")
}

// =============================================================================
// Replay Priority Tests
// =============================================================================

func TestCQLReplayPayloadPriorityIsPreserved(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	ordersTable := createTestTableOnBoth(t, "orders_priority", ordersTableCQLSchema)

	// Track enqueued payloads
	var enqueuedPayloads []types.ReplayPayload

	memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100))

	// Use failing session for B
	var partialFailureCount atomic.Int32
	partialFailureSession := &partialFailureCQLSession{
		session:       cqlv1.WrapSession(sessionB),
		failNextWrite: &partialFailureCount,
	}

	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		partialFailureSession,
		helix.WithReplayer(memReplayer),
	)
	require.NoError(t, err)

	// Fail next write to trigger enqueue
	partialFailureCount.Store(1)

	// Insert with high priority (default)
	orderID := gocql.TimeUUID()
	err = client.Query(
		"INSERT INTO "+ordersTable+" (id, user_id, total, status) VALUES (?, ?, ?, ?)",
		orderID, gocql.TimeUUID(), 99.99, "priority_test",
	).ExecContext(ctx)
	require.NoError(t, err)

	// Drain and check priority
	enqueuedPayloads = memReplayer.DrainAll()
	require.Len(t, enqueuedPayloads, 1)
	assert.Equal(t, types.PriorityHigh, enqueuedPayloads[0].Priority,
		"default priority should be PriorityHigh")
}
