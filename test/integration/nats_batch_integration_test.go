package integration_test

// End-to-end integration tests for NATS batch replay.
//
// These tests exercise the full pipeline from enqueueing a batch ReplayPayload
// into a NATSReplayer through to a NATSWorker executing it against real
// Cassandra/ScyllaDB clusters. They complement the unit-level corrupt-batch
// tests in replay/nats_batch_internal_test.go, which cover error-path behaviour
// that cannot be triggered through the public API.

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	cqlv1 "github.com/arloliu/helix/adapter/cql/v1"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
)

// =============================================================================
// Payload serialisation round-trip — NATS only, no CQL required
// =============================================================================

// TestNATSBatchPayloadRoundTripIntegration enqueues a batch ReplayPayload via
// NATSReplayer and dequeues it, verifying that every BatchStatement (query and
// args) survives the msgpack serialisation round-trip intact.
//
// This catches regressions in the batch serialisation path independently of
// the CQL execution layer.
func TestNATSBatchPayloadRoundTripIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	js := testutil.StartEmbeddedNATS(t)
	ctx := t.Context()

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-batch-rt"),
		replay.WithSubjectPrefix("test.batch.rt"),
	)
	require.NoError(t, err)
	defer replayer.Close()

	// Build a 3-statement batch with diverse argument types.
	id1 := gocql.TimeUUID()
	id2 := gocql.TimeUUID()
	id3 := gocql.TimeUUID()

	payload := types.ReplayPayload{
		TargetCluster: types.ClusterA,
		IsBatch:       true,
		BatchType:     helix.UnloggedBatch,
		BatchStatements: []types.BatchStatement{
			{
				Query: "INSERT INTO orders (id, user_id, total, status) VALUES (?, ?, ?, ?)",
				Args:  []any{id1, gocql.TimeUUID(), float64(10.00), "pending"},
			},
			{
				Query: "INSERT INTO orders (id, user_id, total, status) VALUES (?, ?, ?, ?)",
				Args:  []any{id2, gocql.TimeUUID(), float64(20.00), "shipped"},
			},
			{
				Query: "INSERT INTO orders (id, user_id, total, status) VALUES (?, ?, ?, ?)",
				Args:  []any{id3, gocql.TimeUUID(), float64(30.00), "delivered"},
			},
		},
		Timestamp: time.Now().UnixMicro(),
		Priority:  types.PriorityHigh,
	}

	require.NoError(t, replayer.Enqueue(ctx, payload))

	msgs, err := replayer.Dequeue(ctx, types.ClusterA, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	got := msgs[0].Payload
	assert.True(t, got.IsBatch)
	assert.Equal(t, helix.UnloggedBatch, got.BatchType)
	require.Len(t, got.BatchStatements, 3)

	// Verify each statement's query is preserved exactly.
	assert.Equal(t, payload.BatchStatements[0].Query, got.BatchStatements[0].Query)
	assert.Equal(t, payload.BatchStatements[1].Query, got.BatchStatements[1].Query)
	assert.Equal(t, payload.BatchStatements[2].Query, got.BatchStatements[2].Query)

	// Spot-check the status string arg (position 3) on each statement.
	assert.Equal(t, "pending", got.BatchStatements[0].Args[3])
	assert.Equal(t, "shipped", got.BatchStatements[1].Args[3])
	assert.Equal(t, "delivered", got.BatchStatements[2].Args[3])

	require.NoError(t, msgs[0].Ack())
}

// TestNATSBatchReplayStatementOrderPreservedIntegration verifies that the order
// of BatchStatements is preserved across the NATS serialisation round-trip.
// A re-ordered batch could apply writes in the wrong order, causing silent data
// corruption in scenarios where later statements depend on earlier ones.
func TestNATSBatchReplayStatementOrderPreservedIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	js := testutil.StartEmbeddedNATS(t)
	ctx := t.Context()

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-batch-order"),
		replay.WithSubjectPrefix("test.batch.order"),
	)
	require.NoError(t, err)
	defer replayer.Close()

	// 5 statements with distinct sentinel values that make ordering detectable.
	stmts := make([]types.BatchStatement, 5)
	for i := range stmts {
		stmts[i] = types.BatchStatement{
			Query: "INSERT INTO t (seq) VALUES (?)",
			Args:  []any{int64(i)},
		}
	}

	payload := types.ReplayPayload{
		TargetCluster:   types.ClusterB,
		IsBatch:         true,
		BatchStatements: stmts,
		Timestamp:       time.Now().UnixMicro(),
		Priority:        types.PriorityHigh,
	}

	require.NoError(t, replayer.Enqueue(ctx, payload))

	msgs, err := replayer.Dequeue(ctx, types.ClusterB, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	got := msgs[0].Payload
	require.Len(t, got.BatchStatements, 5)

	for i, stmt := range got.BatchStatements {
		require.Len(t, stmt.Args, 1)
		assert.Equal(t, int64(i), stmt.Args[0],
			"statement %d: expected sequence value %d, got %v", i, i, stmt.Args[0])
	}

	require.NoError(t, msgs[0].Ack())
}

// =============================================================================
// End-to-end: NATSReplayer + NATSWorker + real CQL
// =============================================================================

// TestNATSBatchReplayWorkerExecutesBatchIntegration exercises the full pipeline:
//
//  1. A batch ReplayPayload targeting ClusterA is enqueued directly into a
//     NATSReplayer (simulating what the helix client would do after a partial
//     write failure).
//  2. A NATSWorker picks it up and executes the batch against a real CQL
//     cluster via client.DefaultExecuteFunc().
//  3. All rows written by the batch are verified to exist in ClusterA.
func TestNATSBatchReplayWorkerExecutesBatchIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	ordersTable := createTestTableOnBoth(t, "orders_nats_batch_e2e", ordersTableCQLSchema)

	// Set up a NATS replayer backed by an embedded NATS server.
	js := testutil.StartEmbeddedNATS(t)

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-batch-e2e"),
		replay.WithSubjectPrefix("test.batch.e2e"),
	)
	require.NoError(t, err)
	defer replayer.Close()

	// Create a Helix client so we can obtain a DefaultExecuteFunc.
	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithReplayer(replayer),
	)
	require.NoError(t, err)

	// Track successful replays.
	var successCount atomic.Int32

	worker := replay.NewNATSWorker(
		replayer,
		client.DefaultExecuteFunc(),
		replay.WithPollInterval(10*time.Millisecond),
		replay.WithOnSuccess(func(_ types.ReplayPayload) {
			successCount.Add(1)
		}),
	)
	require.NoError(t, worker.Start())
	defer worker.Stop()

	// Enqueue a 3-row batch payload targeting ClusterA.
	orderIDs := []gocql.UUID{gocql.TimeUUID(), gocql.TimeUUID(), gocql.TimeUUID()}
	stmts := make([]types.BatchStatement, len(orderIDs))
	for i, id := range orderIDs {
		stmts[i] = types.BatchStatement{
			Query: "INSERT INTO " + ordersTable + " (id, user_id, total, status) VALUES (?, ?, ?, ?)",
			Args:  []any{id, gocql.TimeUUID(), float64(i+1) * 10.0, "nats_batch_replay"},
		}
	}

	payload := types.ReplayPayload{
		TargetCluster:   types.ClusterA,
		IsBatch:         true,
		BatchType:       helix.UnloggedBatch,
		BatchStatements: stmts,
		Timestamp:       time.Now().UnixMicro(),
		Priority:        types.PriorityHigh,
	}

	require.NoError(t, replayer.Enqueue(ctx, payload))

	// Wait for the worker to execute the batch.
	require.Eventually(t, func() bool {
		return successCount.Load() >= 1
	}, 10*time.Second, 50*time.Millisecond, "NATS worker should replay the batch")

	// Verify all rows were written to ClusterA.
	for _, id := range orderIDs {
		var status string
		err := sessionA.Query(
			"SELECT status FROM "+ordersTable+" WHERE id = ?", id,
		).Scan(&status)
		require.NoError(t, err, "row %s should exist in ClusterA after NATS batch replay", id)
		assert.Equal(t, "nats_batch_replay", status)
	}
}

// TestNATSBatchReplayLargeStatementCountIntegration verifies that a batch with
// many statements (larger than typical use) is correctly serialised, transported
// through NATS, and executed against a real CQL cluster. This guards against
// off-by-one errors in array allocation and loop bounds.
func TestNATSBatchReplayLargeStatementCountIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	ordersTable := createTestTableOnBoth(t, "orders_nats_batch_large", ordersTableCQLSchema)

	js := testutil.StartEmbeddedNATS(t)

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-batch-large"),
		replay.WithSubjectPrefix("test.batch.large"),
	)
	require.NoError(t, err)
	defer replayer.Close()

	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithReplayer(replayer),
	)
	require.NoError(t, err)

	var successCount atomic.Int32

	worker := replay.NewNATSWorker(
		replayer,
		client.DefaultExecuteFunc(),
		replay.WithPollInterval(10*time.Millisecond),
		replay.WithOnSuccess(func(_ types.ReplayPayload) {
			successCount.Add(1)
		}),
	)
	require.NoError(t, worker.Start())
	defer worker.Stop()

	// 50-statement batch — larger than the default fetch batch size.
	const numStmts = 50
	orderIDs := make([]gocql.UUID, numStmts)
	stmts := make([]types.BatchStatement, numStmts)

	for i := range numStmts {
		orderIDs[i] = gocql.TimeUUID()
		stmts[i] = types.BatchStatement{
			Query: "INSERT INTO " + ordersTable + " (id, user_id, total, status) VALUES (?, ?, ?, ?)",
			Args:  []any{orderIDs[i], gocql.TimeUUID(), float64(i) * 1.5, "large_batch"},
		}
	}

	payload := types.ReplayPayload{
		TargetCluster:   types.ClusterA,
		IsBatch:         true,
		BatchType:       helix.UnloggedBatch,
		BatchStatements: stmts,
		Timestamp:       time.Now().UnixMicro(),
		Priority:        types.PriorityHigh,
	}

	require.NoError(t, replayer.Enqueue(ctx, payload))

	require.Eventually(t, func() bool {
		return successCount.Load() >= 1
	}, 15*time.Second, 50*time.Millisecond, "worker should replay the large batch")

	// Spot-check a sample of rows to avoid making 50 CQL queries.
	for _, i := range []int{0, 10, 25, 49} {
		var status string
		err := sessionA.Query(
			"SELECT status FROM "+ordersTable+" WHERE id = ?", orderIDs[i],
		).Scan(&status)
		require.NoError(t, err, "row %d should exist in ClusterA after large batch replay", i)
		assert.Equal(t, "large_batch", status)
	}
}

// TestNATSBatchReplayTargetsCorrectClusterIntegration verifies that the
// TargetCluster field in the payload is honoured: a batch enqueued for ClusterB
// must be replayed to ClusterB only, leaving ClusterA unchanged.
func TestNATSBatchReplayTargetsCorrectClusterIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	ordersTable := createTestTableOnBoth(t, "nats_batch_target", ordersTableCQLSchema)

	js := testutil.StartEmbeddedNATS(t)

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-batch-target"),
		replay.WithSubjectPrefix("test.batch.target"),
	)
	require.NoError(t, err)
	defer replayer.Close()

	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithReplayer(replayer),
	)
	require.NoError(t, err)

	var successCount atomic.Int32

	worker := replay.NewNATSWorker(
		replayer,
		client.DefaultExecuteFunc(),
		replay.WithPollInterval(10*time.Millisecond),
		replay.WithOnSuccess(func(_ types.ReplayPayload) {
			successCount.Add(1)
		}),
	)
	require.NoError(t, worker.Start())
	defer worker.Stop()

	// Enqueue a batch targeting ClusterB only.
	orderID := gocql.TimeUUID()

	payload := types.ReplayPayload{
		TargetCluster: types.ClusterB,
		IsBatch:       true,
		BatchType:     helix.UnloggedBatch,
		BatchStatements: []types.BatchStatement{
			{
				Query: "INSERT INTO " + ordersTable + " (id, user_id, total, status) VALUES (?, ?, ?, ?)",
				Args:  []any{orderID, gocql.TimeUUID(), float64(99.99), "cluster_b_only"},
			},
		},
		Timestamp: time.Now().UnixMicro(),
		Priority:  types.PriorityHigh,
	}

	require.NoError(t, replayer.Enqueue(ctx, payload))

	require.Eventually(t, func() bool {
		return successCount.Load() >= 1
	}, 10*time.Second, 50*time.Millisecond, "worker should replay to ClusterB")

	// Row must exist in ClusterB.
	var statusB string
	require.NoError(t,
		sessionB.Query("SELECT status FROM "+ordersTable+" WHERE id = ?", orderID).Scan(&statusB),
		"row should exist in ClusterB",
	)
	assert.Equal(t, "cluster_b_only", statusB)

	// Row must NOT exist in ClusterA (replay targeted ClusterB only).
	var statusA string
	errA := sessionA.Query("SELECT status FROM "+ordersTable+" WHERE id = ?", orderID).Scan(&statusA)
	assert.Error(t, errA, "row should not exist in ClusterA when batch targeted ClusterB")
}
