package topology_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/topology"
	"github.com/arloliu/helix/types"
)

// drainUpdates drains a topology update channel in the background.
func drainUpdates(ch <-chan helix.TopologyUpdate) {
	go func() {
		for range ch {
			_ = struct{}{} // consume item
		}
	}()
}

// createTestKV creates a test KV bucket.
func createTestKV(t *testing.T, js jetstream.JetStream, bucket string) jetstream.KeyValue {
	t.Helper()

	ctx := context.Background()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: bucket,
	})
	require.NoError(t, err)

	return kv
}

func TestNewNATSNilKV(t *testing.T) {
	_, err := topology.NewNATS(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KeyValue store is nil")
}

func TestNewNATSDefaults(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-defaults")

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)
	defer watcher.Close()

	assert.Equal(t, "helix.topology.drain", watcher.Config().Key)
	assert.Equal(t, 5*time.Second, watcher.Config().PollInterval)
	assert.Equal(t, 10*time.Second, watcher.Config().InitialFetchTimeout)
}

func TestNewNATSOptions(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-options")

	watcher, err := topology.NewNATS(kv,
		topology.WithKey("custom.drain.key"),
		topology.WithPollInterval(10*time.Second),
		topology.WithInitialFetchTimeout(30*time.Second),
	)
	require.NoError(t, err)
	defer watcher.Close()

	assert.Equal(t, "custom.drain.key", watcher.Config().Key)
	assert.Equal(t, 10*time.Second, watcher.Config().PollInterval)
	assert.Equal(t, 30*time.Second, watcher.Config().InitialFetchTimeout)
}

func TestNATSDrainClusterB(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-drain-b")

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)
	defer watcher.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	updates := watcher.Watch(ctx)

	// Initially not draining
	assert.False(t, watcher.IsDraining(types.ClusterA))
	assert.False(t, watcher.IsDraining(types.ClusterB))

	// Set drain on cluster B
	drainConfig := topology.DrainConfig{
		Drain:  []types.ClusterID{types.ClusterB},
		Reason: "OS Patching",
	}
	data, _ := json.Marshal(drainConfig)
	_, err = kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	// Wait for update
	select {
	case update := <-updates:
		assert.Equal(t, types.ClusterB, update.Cluster)
		assert.True(t, update.DrainMode)
		assert.False(t, update.Available)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for topology update")
	}

	// Verify state
	assert.False(t, watcher.IsDraining(types.ClusterA))
	assert.True(t, watcher.IsDraining(types.ClusterB))
}

func TestNATSDrainBothClusters(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-drain-both")

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)
	defer watcher.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	updates := watcher.Watch(ctx)

	// Set drain on both clusters
	drainConfig := topology.DrainConfig{
		Drain:  []types.ClusterID{types.ClusterA, types.ClusterB},
		Reason: "Full maintenance",
	}
	data, _ := json.Marshal(drainConfig)
	_, err = kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	// Collect both updates
	received := make(map[types.ClusterID]helix.TopologyUpdate)
	for i := 0; i < 2; i++ {
		select {
		case update := <-updates:
			received[update.Cluster] = update
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for topology update")
		}
	}

	assert.Len(t, received, 2)
	assert.True(t, received[types.ClusterA].DrainMode)
	assert.True(t, received[types.ClusterB].DrainMode)
}

func TestNATSClearDrain(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-clear-drain")

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)
	defer watcher.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Pre-set drain before watching
	drainConfig := topology.DrainConfig{
		Drain:  []types.ClusterID{types.ClusterB},
		Reason: "Upgrade",
	}
	data, _ := json.Marshal(drainConfig)
	_, err = kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	updates := watcher.Watch(ctx)

	// Wait for initial drain update
	select {
	case update := <-updates:
		assert.Equal(t, types.ClusterB, update.Cluster)
		assert.True(t, update.DrainMode)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for initial drain update")
	}

	// Delete the key to clear drain
	err = kv.Delete(ctx, "helix.topology.drain")
	require.NoError(t, err)

	// Wait for clear update
	select {
	case update := <-updates:
		assert.Equal(t, types.ClusterB, update.Cluster)
		assert.False(t, update.DrainMode)
		assert.True(t, update.Available)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for clear drain update")
	}

	assert.False(t, watcher.IsDraining(types.ClusterB))
}

func TestNATSEmptyDrainList(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-empty-drain")

	// Pre-set drain
	ctx := t.Context()
	drainConfig := topology.DrainConfig{
		Drain:  []types.ClusterID{types.ClusterA},
		Reason: "Test",
	}
	data, _ := json.Marshal(drainConfig)
	_, err := kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)
	defer watcher.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	updates := watcher.Watch(ctx)

	// Wait for initial update
	select {
	case <-updates:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for initial update")
	}

	assert.True(t, watcher.IsDraining(types.ClusterA))

	// Clear by setting empty drain list
	drainConfig = topology.DrainConfig{
		Drain:  []types.ClusterID{},
		Reason: "",
	}
	data, _ = json.Marshal(drainConfig)
	_, err = kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	// Wait for clear update
	select {
	case update := <-updates:
		assert.Equal(t, types.ClusterA, update.Cluster)
		assert.False(t, update.DrainMode)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for clear update")
	}

	assert.False(t, watcher.IsDraining(types.ClusterA))
}

func TestNATSInvalidJSON(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-invalid-json")

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)
	defer watcher.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_ = watcher.Watch(ctx)

	// Put invalid JSON
	_, err = kv.Put(ctx, "helix.topology.drain", []byte("not valid json"))
	require.NoError(t, err)

	// Wait for watcher to process - invalid JSON should be treated as no drain
	// Since we start with no drain, we verify the state remains unchanged
	require.Eventually(t, func() bool {
		// Just ensure the watcher had time to process by checking it's still not draining
		return !watcher.IsDraining(types.ClusterA) && !watcher.IsDraining(types.ClusterB)
	}, 2*time.Second, 50*time.Millisecond)

	// Should treat invalid JSON as no drain
	assert.False(t, watcher.IsDraining(types.ClusterA))
	assert.False(t, watcher.IsDraining(types.ClusterB))
}

func TestNATSClose(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-close")

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)

	ctx := t.Context()
	updates := watcher.Watch(ctx)

	// Close should be safe to call
	err = watcher.Close()
	require.NoError(t, err)

	// Double close should be safe
	err = watcher.Close()
	require.NoError(t, err)

	// Channel should eventually close
	select {
	case _, ok := <-updates:
		if ok {
			drainUpdates(updates)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("channel not closed after Close()")
	}
}

func TestNATSContextCancellation(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-ctx-cancel")

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)
	defer watcher.Close()

	ctx, cancel := context.WithCancel(t.Context())
	updates := watcher.Watch(ctx)

	// Cancel context
	cancel()

	// Channel should eventually close
	select {
	case _, ok := <-updates:
		if ok {
			drainUpdates(updates)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("channel not closed after context cancellation")
	}
}

func TestNATSGetDrainReason(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-drain-reason")

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)
	defer watcher.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Start watching - this is required for reason caching
	updates := watcher.Watch(ctx)
	defer drainUpdates(updates)

	// Initially no reason
	assert.Empty(t, watcher.GetDrainReason())

	// Set drain with reason
	drainConfig := topology.DrainConfig{
		Drain:  []types.ClusterID{types.ClusterB},
		Reason: "Scheduled maintenance window",
	}
	data, _ := json.Marshal(drainConfig)
	_, err = kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	// Wait for watcher to process the update and cache the reason
	require.Eventually(t, func() bool {
		return watcher.GetDrainReason() == "Scheduled maintenance window"
	}, 2*time.Second, 10*time.Millisecond)
}

func TestNATSMultipleWatchCalls(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-multi-watch")

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)
	defer watcher.Close()

	ctx := t.Context()

	// Call Watch multiple times - should return the same channel
	updates1 := watcher.Watch(ctx)
	updates2 := watcher.Watch(ctx)

	// Both should be the same channel
	assert.Equal(t, updates1, updates2)

	// Set drain and verify we receive update on the channel
	drainConfig := topology.DrainConfig{
		Drain:  []types.ClusterID{types.ClusterA},
		Reason: "test",
	}
	data, _ := json.Marshal(drainConfig)
	_, err = kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	select {
	case update := <-updates1:
		assert.Equal(t, types.ClusterA, update.Cluster)
		assert.True(t, update.DrainMode)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for update")
	}

	// Close should not panic (only closes channel once)
	err = watcher.Close()
	require.NoError(t, err)
}

func TestDrainConfigContainsCluster(t *testing.T) {
	tests := []struct {
		name     string
		drain    []types.ClusterID
		cluster  types.ClusterID
		expected bool
	}{
		{
			name:     "empty drain list",
			drain:    []types.ClusterID{},
			cluster:  types.ClusterA,
			expected: false,
		},
		{
			name:     "cluster A in list",
			drain:    []types.ClusterID{types.ClusterA},
			cluster:  types.ClusterA,
			expected: true,
		},
		{
			name:     "cluster A not in list",
			drain:    []types.ClusterID{types.ClusterB},
			cluster:  types.ClusterA,
			expected: false,
		},
		{
			name:     "both clusters in list",
			drain:    []types.ClusterID{types.ClusterA, types.ClusterB},
			cluster:  types.ClusterB,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := topology.DrainConfig{Drain: tt.drain}
			assert.Equal(t, tt.expected, config.ContainsCluster(tt.cluster))
		})
	}
}
