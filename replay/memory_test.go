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
	// Capacity 4 = 2 per priority queue
	replayer := NewMemoryReplayer(WithQueueCapacity(4))
	defer replayer.Close()

	highPayload := types.ReplayPayload{Query: "SELECT 1", Priority: types.PriorityHigh}
	lowPayload := types.ReplayPayload{Query: "SELECT 2", Priority: types.PriorityLow}

	// Fill the high queue (capacity 2)
	require.NoError(t, replayer.Enqueue(context.Background(), highPayload))
	require.NoError(t, replayer.Enqueue(context.Background(), highPayload))

	// Third high-priority enqueue should fail
	err := replayer.Enqueue(context.Background(), highPayload)
	require.ErrorIs(t, err, types.ErrReplayQueueFull)

	// Low queue still has space
	require.NoError(t, replayer.Enqueue(context.Background(), lowPayload))
	require.NoError(t, replayer.Enqueue(context.Background(), lowPayload))

	// Low queue now full
	err = replayer.Enqueue(context.Background(), lowPayload)
	require.ErrorIs(t, err, types.ErrReplayQueueFull)
}

func TestMemoryReplayerContextCancellation(t *testing.T) {
	// Capacity 2 = 1 per priority queue
	replayer := NewMemoryReplayer(WithQueueCapacity(2))
	defer replayer.Close()

	// Fill the high queue
	payload := types.ReplayPayload{Query: "SELECT 1", Priority: types.PriorityHigh}
	require.NoError(t, replayer.Enqueue(context.Background(), payload))

	// Cancel context and try to enqueue (queue is full)
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

	// Enqueue multiple items with mixed priorities
	for i := range 5 {
		priority := types.PriorityHigh
		if i%2 == 1 {
			priority = types.PriorityLow
		}
		payload := types.ReplayPayload{
			Query:     "SELECT ?",
			Args:      []any{i},
			Timestamp: int64(i),
			Priority:  priority,
		}
		require.NoError(t, replayer.Enqueue(context.Background(), payload))
	}

	require.Equal(t, 5, replayer.Len())

	// Drain all - high priority first, then low
	payloads := replayer.DrainAll()
	require.Len(t, payloads, 5)
	require.Equal(t, 0, replayer.Len())

	// Verify high priority items come first (timestamps 0, 2, 4)
	// Then low priority items (timestamps 1, 3)
	highCount := 0
	for i, p := range payloads {
		if i < 3 {
			require.Equal(t, types.PriorityHigh, p.Priority, "First 3 should be high priority")
			highCount++
		} else {
			require.Equal(t, types.PriorityLow, p.Priority, "Last 2 should be low priority")
		}
	}
	require.Equal(t, 3, highCount)
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

func TestMemoryReplayerPriorityRouting(t *testing.T) {
	replayer := NewMemoryReplayer(WithQueueCapacity(20))
	defer replayer.Close()

	// Enqueue high and low priority messages
	high := types.ReplayPayload{Query: "HIGH", Priority: types.PriorityHigh}
	low := types.ReplayPayload{Query: "LOW", Priority: types.PriorityLow}

	require.NoError(t, replayer.Enqueue(context.Background(), high))
	require.NoError(t, replayer.Enqueue(context.Background(), low))

	require.Equal(t, 1, replayer.HighLen())
	require.Equal(t, 1, replayer.LowLen())
	require.Equal(t, 2, replayer.Len())
}

func TestMemoryReplayerStrictPriority(t *testing.T) {
	replayer := NewMemoryReplayer(
		WithQueueCapacity(20),
		WithMemoryStrictPriority(true),
	)
	defer replayer.Close()

	// Enqueue: low, high, high
	low := types.ReplayPayload{Query: "LOW", Priority: types.PriorityLow}
	high1 := types.ReplayPayload{Query: "HIGH1", Priority: types.PriorityHigh}
	high2 := types.ReplayPayload{Query: "HIGH2", Priority: types.PriorityHigh}

	require.NoError(t, replayer.Enqueue(context.Background(), low))
	require.NoError(t, replayer.Enqueue(context.Background(), high1))
	require.NoError(t, replayer.Enqueue(context.Background(), high2))

	// In strict mode, high priority should be drained first
	p1, ok := replayer.TryDequeue()
	require.True(t, ok)
	require.Equal(t, "HIGH1", p1.Query)

	p2, ok := replayer.TryDequeue()
	require.True(t, ok)
	require.Equal(t, "HIGH2", p2.Query)

	// Now low priority
	p3, ok := replayer.TryDequeue()
	require.True(t, ok)
	require.Equal(t, "LOW", p3.Query)

	// Empty
	_, ok = replayer.TryDequeue()
	require.False(t, ok)
}

func TestMemoryReplayerRatioBasedFairness(t *testing.T) {
	// Ratio of 2:1 - process 2 high before 1 low
	replayer := NewMemoryReplayer(
		WithQueueCapacity(20),
		WithMemoryHighPriorityRatio(2),
	)
	defer replayer.Close()

	// Enqueue 3 high and 3 low
	for i := range 3 {
		high := types.ReplayPayload{Query: "HIGH", Priority: types.PriorityHigh, Timestamp: int64(i)}
		low := types.ReplayPayload{Query: "LOW", Priority: types.PriorityLow, Timestamp: int64(i)}
		require.NoError(t, replayer.Enqueue(context.Background(), high))
		require.NoError(t, replayer.Enqueue(context.Background(), low))
	}

	// With 2:1 ratio, expect: HIGH, HIGH, LOW, HIGH, LOW, LOW
	results := make([]string, 0, 6)
	for range 6 {
		p, ok := replayer.TryDequeue()
		require.True(t, ok)
		results = append(results, p.Query)
	}

	// Count pattern: should have at least some interleaving
	highCount := 0
	lowCount := 0
	for _, r := range results {
		if r == "HIGH" {
			highCount++
		} else {
			lowCount++
		}
	}
	require.Equal(t, 3, highCount)
	require.Equal(t, 3, lowCount)

	// Verify that low is not starved - should appear before position 4
	firstLowIdx := -1
	for i, r := range results {
		if r == "LOW" {
			firstLowIdx = i
			break
		}
	}
	require.LessOrEqual(t, firstLowIdx, 3, "Low priority should not be starved beyond ratio")
}

func TestMemoryReplayerHighLenLowLen(t *testing.T) {
	replayer := NewMemoryReplayer(WithQueueCapacity(100))
	defer replayer.Close()

	for i := range 5 {
		p := types.ReplayPayload{Query: "HIGH", Priority: types.PriorityHigh, Timestamp: int64(i)}
		require.NoError(t, replayer.Enqueue(context.Background(), p))
	}
	for i := range 3 {
		p := types.ReplayPayload{Query: "LOW", Priority: types.PriorityLow, Timestamp: int64(i)}
		require.NoError(t, replayer.Enqueue(context.Background(), p))
	}

	require.Equal(t, 5, replayer.HighLen())
	require.Equal(t, 3, replayer.LowLen())
	require.Equal(t, 8, replayer.Len())
}
