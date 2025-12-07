package integration_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	cqlv1 "github.com/arloliu/helix/adapter/cql/v1"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/topology"
	"github.com/arloliu/helix/types"
)

// drainChannel consumes all messages from a channel until it's closed.
func drainChannel[T any](ch <-chan T) {
	for range ch {
		_ = struct{}{} // consume item
	}
}

func TestLocalDrainReasonIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	watcher := topology.NewLocal()
	defer watcher.Close()

	ctx := t.Context()
	updates := watcher.Watch(ctx)

	// Initially no drain reason
	assert.Empty(t, watcher.GetDrainReason())

	// Set drain with reason
	_ = watcher.SetDrain(ctx, types.ClusterA, true, "OS Patching")

	select {
	case update := <-updates:
		assert.Equal(t, types.ClusterA, update.Cluster)
		assert.True(t, update.DrainMode)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for drain update")
	}

	assert.Equal(t, "OS Patching", watcher.GetDrainReason())
	assert.True(t, watcher.IsDraining(types.ClusterA))

	// Clear drain - reason should remain if other cluster is still draining
	_ = watcher.SetDrain(ctx, types.ClusterB, true, "Scaling")
	<-updates // consume update

	_ = watcher.SetDrain(ctx, types.ClusterA, false, "")
	<-updates // consume update

	// Reason should still be set because ClusterB is draining
	assert.True(t, watcher.IsDraining(types.ClusterB))

	// Clear all drains
	_ = watcher.SetDrain(ctx, types.ClusterB, false, "")
	<-updates // consume update

	// Now reason should be cleared
	assert.Empty(t, watcher.GetDrainReason())
}

func TestNATSDrainTransitionIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	js := testutil.StartEmbeddedNATS(t)

	ctx := t.Context()
	kv, err := js.CreateKeyValue(ctx, testutil.CreateKVConfig("test-drain-transition"))
	require.NoError(t, err)

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)
	defer watcher.Close()

	watchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	updates := watcher.Watch(watchCtx)

	// Initially not draining
	assert.False(t, watcher.IsDraining(types.ClusterA))
	assert.False(t, watcher.IsDraining(types.ClusterB))

	// Transition: No drain -> Drain A
	drainConfig := topology.DrainConfig{
		Drain:  []types.ClusterID{types.ClusterA},
		Reason: "Upgrade to v4.1",
	}
	data, _ := json.Marshal(drainConfig)
	_, err = kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	// Should receive update for cluster A
	select {
	case update := <-updates:
		assert.Equal(t, types.ClusterA, update.Cluster)
		assert.True(t, update.DrainMode)
		assert.False(t, update.Available)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for drain A update")
	}

	assert.True(t, watcher.IsDraining(types.ClusterA))
	assert.False(t, watcher.IsDraining(types.ClusterB))
	assert.Equal(t, "Upgrade to v4.1", watcher.GetDrainReason())

	// Transition: Drain A -> Drain A+B
	drainConfig.Drain = []types.ClusterID{types.ClusterA, types.ClusterB}
	data, _ = json.Marshal(drainConfig)
	_, err = kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	// Should receive update for cluster B (A already draining)
	select {
	case update := <-updates:
		assert.Equal(t, types.ClusterB, update.Cluster)
		assert.True(t, update.DrainMode)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for drain B update")
	}

	assert.True(t, watcher.IsDraining(types.ClusterA))
	assert.True(t, watcher.IsDraining(types.ClusterB))

	// Transition: Drain A+B -> No drain (delete key)
	err = kv.Delete(ctx, "helix.topology.drain")
	require.NoError(t, err)

	// Should receive updates for both clusters clearing drain
	received := make(map[types.ClusterID]bool)
	for len(received) < 2 {
		select {
		case update := <-updates:
			assert.False(t, update.DrainMode)
			assert.True(t, update.Available)
			received[update.Cluster] = true
		case <-time.After(3 * time.Second):
			t.Fatalf("timeout waiting for clear updates, got %d", len(received))
		}
	}

	assert.False(t, watcher.IsDraining(types.ClusterA))
	assert.False(t, watcher.IsDraining(types.ClusterB))
}

func TestNATSConcurrentAccessIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	js := testutil.StartEmbeddedNATS(t)

	ctx := t.Context()
	kv, err := js.CreateKeyValue(ctx, testutil.CreateKVConfig("test-concurrent"))
	require.NoError(t, err)

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)
	defer watcher.Close()

	watchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	updates := watcher.Watch(watchCtx)

	// Drain the updates channel in background
	go drainChannel(updates)

	// Concurrent reads should not panic or deadlock
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Go(func() {
			for j := range 100 {
				_ = watcher.IsDraining(types.ClusterA)
				_ = watcher.IsDraining(types.ClusterB)
				_ = watcher.GetDrainReason()

				// Occasionally update drain state
				if j%20 == 0 {
					// Alternate between clusters
					var cluster types.ClusterID
					if i%2 == 0 {
						cluster = types.ClusterA
					} else {
						cluster = types.ClusterB
					}
					drainConfig := topology.DrainConfig{
						Drain:  []types.ClusterID{cluster},
						Reason: "test",
					}
					data, _ := json.Marshal(drainConfig)
					_, _ = kv.Put(ctx, "helix.topology.drain", data)
				}
			}
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success - no deadlock
	case <-time.After(30 * time.Second):
		t.Fatal("timeout - possible deadlock in concurrent access")
	}
}

func TestLocalConcurrentSetDrainIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	watcher := topology.NewLocal()
	defer watcher.Close()

	ctx := t.Context()
	updates := watcher.Watch(ctx)

	// Drain updates in background
	go drainChannel(updates)

	// Concurrent SetDrain calls should not panic
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Go(func() {
			cluster := types.ClusterA
			if i%2 == 1 {
				cluster = types.ClusterB
			}
			_ = watcher.SetDrain(ctx, cluster, i%3 == 0, "")
			_ = watcher.IsDraining(cluster)
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(10 * time.Second):
		t.Fatal("timeout in concurrent SetDrain")
	}
}

func TestLocalCloseWhileSetDrainIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	for i := range 50 {
		t.Run("iteration", func(t *testing.T) {
			watcher := topology.NewLocal()
			ctx := t.Context()
			updates := watcher.Watch(ctx)

			// Drain updates in background
			go drainChannel(updates)

			// Start concurrent SetDrain calls
			var wg sync.WaitGroup
			stop := make(chan struct{})

			for j := range 5 {
				wg.Go(func() {
					cluster := types.ClusterA
					if j%2 == 1 {
						cluster = types.ClusterB
					}
					for {
						select {
						case <-stop:
							return
						default:
							_ = watcher.SetDrain(ctx, cluster, true, "")
							_ = watcher.SetDrain(ctx, cluster, false, "")
						}
					}
				})
			}

			// Let SetDrain run for a bit, then close
			time.Sleep(time.Duration(i%5+1) * time.Millisecond)
			err := watcher.Close()
			require.NoError(t, err)

			close(stop)
			wg.Wait()
		})
	}
}

func TestNATSInvalidJSONIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	js := testutil.StartEmbeddedNATS(t)

	ctx := t.Context()
	kv, err := js.CreateKeyValue(ctx, testutil.CreateKVConfig("test-invalid-json"))
	require.NoError(t, err)

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)
	defer watcher.Close()

	watchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	updates := watcher.Watch(watchCtx)

	// Put invalid JSON
	_, err = kv.Put(ctx, "helix.topology.drain", []byte("not valid json"))
	require.NoError(t, err)

	// Wait for watcher to process - invalid JSON should be treated as no drain
	require.Eventually(t, func() bool {
		return !watcher.IsDraining(types.ClusterA) && !watcher.IsDraining(types.ClusterB)
	}, 2*time.Second, 50*time.Millisecond)

	// Should not be draining (invalid JSON treated as no drain)
	assert.False(t, watcher.IsDraining(types.ClusterA))
	assert.False(t, watcher.IsDraining(types.ClusterB))

	// Now put valid JSON
	drainConfig := topology.DrainConfig{
		Drain:  []types.ClusterID{types.ClusterA},
		Reason: "Valid config",
	}
	data, _ := json.Marshal(drainConfig)
	_, err = kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	// Should receive drain update
	select {
	case update := <-updates:
		assert.Equal(t, types.ClusterA, update.Cluster)
		assert.True(t, update.DrainMode)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for valid drain update")
	}

	assert.True(t, watcher.IsDraining(types.ClusterA))
}

func TestTopologyWatcherWithCQLClientIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// This test verifies that TopologyWatcher integrates correctly with CQLClient
	watcher := topology.NewLocal()
	defer watcher.Close()

	// Simulate what CQLClient does with the watcher
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	updates := watcher.Watch(ctx)

	// Track drain state as CQLClient would
	drainState := make(map[types.ClusterID]bool)
	var mu sync.Mutex

	// Consumer goroutine (simulates CQLClient's drain monitor)
	go func() {
		for update := range updates {
			mu.Lock()
			drainState[update.Cluster] = update.DrainMode
			mu.Unlock()
		}
	}()

	// Simulate ops team draining cluster B
	_ = watcher.SetDrain(ctx, types.ClusterB, true, "Maintenance window")

	// Wait for update to be processed
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return drainState[types.ClusterB]
	}, time.Second, 10*time.Millisecond)

	// Verify drain state is correct
	mu.Lock()
	assert.False(t, drainState[types.ClusterA])
	assert.True(t, drainState[types.ClusterB])
	mu.Unlock()

	// Verify watcher state matches
	assert.False(t, watcher.IsDraining(types.ClusterA))
	assert.True(t, watcher.IsDraining(types.ClusterB))
	assert.Equal(t, "Maintenance window", watcher.GetDrainReason())
}

func TestNATSContextCancellationIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	js := testutil.StartEmbeddedNATS(t)

	ctx := t.Context()
	kv, err := js.CreateKeyValue(ctx, testutil.CreateKVConfig("test-ctx-cancel"))
	require.NoError(t, err)

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)
	defer watcher.Close()

	// Use a cancellable context
	watchCtx, cancel := context.WithCancel(ctx)
	updates := watcher.Watch(watchCtx)

	// Put initial drain config
	drainConfig := topology.DrainConfig{
		Drain:  []types.ClusterID{types.ClusterA},
		Reason: "test",
	}
	data, _ := json.Marshal(drainConfig)
	_, err = kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	// Receive initial update
	select {
	case <-updates:
		// Got update
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for initial update")
	}

	// Cancel context
	cancel()

	// Channel should close
	select {
	case _, ok := <-updates:
		if ok {
			// Might get one more update, drain the rest
			drainChannel(updates)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("channel not closed after context cancellation")
	}
}

func TestLocalMultipleWatchCallsIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	watcher := topology.NewLocal()
	defer watcher.Close()

	ctx := t.Context()

	// Call Watch multiple times - each should return the same channel
	updates1 := watcher.Watch(ctx)
	updates2 := watcher.Watch(ctx)

	// Both should be the same channel
	assert.Equal(t, updates1, updates2)

	// SetDrain should be received on the channel
	_ = watcher.SetDrain(ctx, types.ClusterA, true, "")

	select {
	case update := <-updates1:
		assert.Equal(t, types.ClusterA, update.Cluster)
		assert.True(t, update.DrainMode)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for update")
	}
}

// =============================================================================
// NATS TopologyWatcher + CQLClient Integration Tests
// =============================================================================

// TestNATSTopologyDrainSkipsCQLWritesIntegration tests that when a cluster
// is draining via NATS KV, CQL writes skip the draining cluster and queue to replay.
func TestNATSTopologyDrainSkipsCQLWritesIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, sessionB := getSharedSessions(t)

	// Create test table
	drainTable := createTestTableOnBoth(t, "drain_writes", ordersTableCQLSchema)

	// Start embedded NATS and create KV
	js := testutil.StartEmbeddedNATS(t)
	kv, err := js.CreateKeyValue(ctx, testutil.CreateKVConfig("test-drain-cql"))
	require.NoError(t, err)

	// Create NATS topology watcher
	watcher, err := topology.NewNATS(kv,
		topology.WithPollInterval(50*time.Millisecond),
	)
	require.NoError(t, err)
	defer watcher.Close()

	// Create memory replayer to track enqueued messages
	memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(10))

	// Create metrics collector
	metricsCollector := testutil.NewTestMetricsCollector()

	// Create CQL client with topology watcher
	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithTopologyWatcher(watcher),
		helix.WithReplayer(memReplayer),
		helix.WithMetrics(metricsCollector),
	)
	require.NoError(t, err)
	// Note: We don't defer client.Close() because it would close the shared gocql sessions.

	// Verify initial state - not draining
	assert.False(t, client.IsDraining(types.ClusterA))
	assert.False(t, client.IsDraining(types.ClusterB))

	// Set ClusterB to draining via NATS KV
	drainConfig := topology.DrainConfig{
		Drain:  []types.ClusterID{types.ClusterB},
		Reason: "CQL integration test",
	}
	data, err := json.Marshal(drainConfig)
	require.NoError(t, err)
	_, err = kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	// Wait for drain state to propagate
	require.Eventually(t, func() bool {
		return client.IsDraining(types.ClusterB)
	}, 5*time.Second, 50*time.Millisecond, "ClusterB should be draining")

	// Verify ClusterA is NOT draining
	assert.False(t, client.IsDraining(types.ClusterA))

	// Now write data - should succeed on A, skip B, and enqueue to replay
	orderID := gocql.TimeUUID()
	err = client.Query(
		"INSERT INTO "+drainTable+" (id, user_id, total, status) VALUES (?, ?, ?, ?)",
		orderID, gocql.TimeUUID(), 42.50, "drain_test",
	).ExecContext(ctx)
	require.NoError(t, err, "write should succeed on A")

	// Verify data exists in A
	var total float64
	err = sessionA.Query("SELECT total FROM "+drainTable+" WHERE id = ?", orderID).Scan(&total)
	require.NoError(t, err)
	assert.Equal(t, 42.50, total)

	// Verify replay was enqueued for draining cluster B
	assert.Equal(t, int64(1), metricsCollector.GetReplayEnqueued(types.ClusterB),
		"write to draining cluster should be enqueued for replay")

	// Drain and verify payload
	msgs := memReplayer.DrainAll()
	require.Len(t, msgs, 1)
	assert.Equal(t, types.ClusterB, msgs[0].TargetCluster)
	assert.Contains(t, msgs[0].Query, "INSERT INTO")
}

// TestNATSTopologyDrainRecoveryCQLIntegration tests that when drain mode is cleared,
// writes resume to the recovered cluster.
func TestNATSTopologyDrainRecoveryCQLIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, sessionB := getSharedSessions(t)

	// Create test table
	recoveryTable := createTestTableOnBoth(t, "drain_recovery", ordersTableCQLSchema)

	// Start embedded NATS and create KV
	js := testutil.StartEmbeddedNATS(t)
	kv, err := js.CreateKeyValue(ctx, testutil.CreateKVConfig("test-drain-recovery"))
	require.NoError(t, err)

	// Create NATS topology watcher
	watcher, err := topology.NewNATS(kv,
		topology.WithPollInterval(50*time.Millisecond),
	)
	require.NoError(t, err)
	defer watcher.Close()

	// Create metrics collector
	metricsCollector := testutil.NewTestMetricsCollector()

	// Create CQL client
	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithTopologyWatcher(watcher),
		helix.WithMetrics(metricsCollector),
	)
	require.NoError(t, err)
	// Note: We don't defer client.Close() because it would close the shared gocql sessions.

	// Step 1: Set drain on ClusterB
	drainConfig := topology.DrainConfig{
		Drain:  []types.ClusterID{types.ClusterB},
		Reason: "Recovery test - phase 1",
	}
	data, err := json.Marshal(drainConfig)
	require.NoError(t, err)
	_, err = kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	// Wait for drain to propagate
	require.Eventually(t, func() bool {
		return client.IsDraining(types.ClusterB)
	}, 5*time.Second, 50*time.Millisecond)

	// Step 2: Clear drain
	clearConfig := topology.DrainConfig{
		Drain:  []types.ClusterID{}, // Empty = no clusters draining
		Reason: "",
	}
	data, err = json.Marshal(clearConfig)
	require.NoError(t, err)
	_, err = kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	// Wait for drain to be cleared
	require.Eventually(t, func() bool {
		return !client.IsDraining(types.ClusterB)
	}, 5*time.Second, 50*time.Millisecond, "ClusterB drain should be cleared")

	// Step 3: Write data - should go to BOTH clusters now
	orderID := gocql.TimeUUID()
	err = client.Query(
		"INSERT INTO "+recoveryTable+" (id, user_id, total, status) VALUES (?, ?, ?, ?)",
		orderID, gocql.TimeUUID(), 88.88, "recovery_test",
	).ExecContext(ctx)
	require.NoError(t, err)

	// Verify data exists in BOTH clusters (no replay needed)
	var totalA, totalB float64
	err = sessionA.Query("SELECT total FROM "+recoveryTable+" WHERE id = ?", orderID).Scan(&totalA)
	require.NoError(t, err, "data should exist in A")
	err = sessionB.Query("SELECT total FROM "+recoveryTable+" WHERE id = ?", orderID).Scan(&totalB)
	require.NoError(t, err, "data should exist in B after recovery")

	assert.Equal(t, 88.88, totalA)
	assert.Equal(t, 88.88, totalB)

	// No replay should have been triggered
	assert.Equal(t, int64(0), metricsCollector.GetReplayEnqueued(types.ClusterB),
		"no replay should be needed after recovery")
}
