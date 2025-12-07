package replay

import (
	"context"
	"testing"
	"time"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

func TestMemoryReplayerEnqueue(t *testing.T) {
	replayer := NewMemoryReplayer(WithQueueCapacity(10))
	defer replayer.Close()

	payload := types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT INTO test (id) VALUES (?)",
		Args:          []any{1},
		Timestamp:     time.Now().UnixMicro(),
		Priority:      types.PriorityHigh,
	}

	err := replayer.Enqueue(context.Background(), payload)
	require.NoError(t, err)
	require.Equal(t, 1, replayer.Len())
}

func TestMemoryReplayerDequeue(t *testing.T) {
	replayer := NewMemoryReplayer()
	defer replayer.Close()

	payload := types.ReplayPayload{
		TargetCluster: types.ClusterB,
		Query:         "UPDATE test SET val = ? WHERE id = ?",
		Args:          []any{"test", 1},
		Timestamp:     12345,
		Priority:      types.PriorityLow,
	}

	err := replayer.Enqueue(context.Background(), payload)
	require.NoError(t, err)

	dequeued, ok := replayer.TryDequeue()
	require.True(t, ok)
	require.Equal(t, payload.TargetCluster, dequeued.TargetCluster)
	require.Equal(t, payload.Query, dequeued.Query)
	require.Equal(t, payload.Timestamp, dequeued.Timestamp)
	require.Equal(t, payload.Priority, dequeued.Priority)
}

func TestMemoryReplayerQueueFull(t *testing.T) {
	replayer := NewMemoryReplayer(WithQueueCapacity(2))
	defer replayer.Close()

	payload := types.ReplayPayload{Query: "SELECT 1"}

	// Fill the queue
	require.NoError(t, replayer.Enqueue(context.Background(), payload))
	require.NoError(t, replayer.Enqueue(context.Background(), payload))

	// Third enqueue should fail
	err := replayer.Enqueue(context.Background(), payload)
	require.ErrorIs(t, err, types.ErrReplayQueueFull)
}

func TestMemoryReplayerContextCancellation(t *testing.T) {
	replayer := NewMemoryReplayer(WithQueueCapacity(1))
	defer replayer.Close()

	// Fill the queue
	payload := types.ReplayPayload{Query: "SELECT 1"}
	require.NoError(t, replayer.Enqueue(context.Background(), payload))

	// Cancel context and try to enqueue
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := replayer.Enqueue(ctx, payload)
	require.ErrorIs(t, err, context.Canceled)
}

func TestMemoryReplayerDequeueBlocking(t *testing.T) {
	replayer := NewMemoryReplayer()
	defer replayer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Dequeue should block and return false on context timeout
	_, ok := replayer.Dequeue(ctx)
	require.False(t, ok)
}

func TestMemoryReplayerTryDequeueEmpty(t *testing.T) {
	replayer := NewMemoryReplayer()
	defer replayer.Close()

	_, ok := replayer.TryDequeue()
	require.False(t, ok)
}

func TestMemoryReplayerDrainAll(t *testing.T) {
	replayer := NewMemoryReplayer()
	defer replayer.Close()

	// Enqueue multiple items
	for i := range 5 {
		payload := types.ReplayPayload{
			Query:     "SELECT ?",
			Args:      []any{i},
			Timestamp: int64(i),
		}
		require.NoError(t, replayer.Enqueue(context.Background(), payload))
	}

	require.Equal(t, 5, replayer.Len())

	// Drain all
	payloads := replayer.DrainAll()
	require.Len(t, payloads, 5)
	require.Equal(t, 0, replayer.Len())

	// Verify order
	for i, p := range payloads {
		require.Equal(t, int64(i), p.Timestamp)
	}
}

func TestMemoryReplayerClose(t *testing.T) {
	replayer := NewMemoryReplayer()

	payload := types.ReplayPayload{Query: "SELECT 1"}
	require.NoError(t, replayer.Enqueue(context.Background(), payload))

	replayer.Close()

	// Enqueue after close should fail
	err := replayer.Enqueue(context.Background(), payload)
	require.ErrorIs(t, err, types.ErrSessionClosed)
}

func TestMemoryReplayerCapacity(t *testing.T) {
	replayer := NewMemoryReplayer(WithQueueCapacity(100))
	defer replayer.Close()

	require.Equal(t, 100, replayer.Cap())
}
