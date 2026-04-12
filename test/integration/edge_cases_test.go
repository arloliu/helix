package integration_test

import (
	"sync"
	"testing"
	"time"

	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

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
