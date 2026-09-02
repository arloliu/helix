package replay

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/types"
)

// A payload addressed to a cluster that no client can resolve is refused
// at every boundary: memory enqueue, NATS enqueue, and NATS decode.

func TestMemoryReplayerEnqueueRejectsUnknownTargetCluster(t *testing.T) {
	m := NewMemoryReplayer(WithQueueCapacity(4))

	err := m.Enqueue(t.Context(), types.ReplayPayload{TargetCluster: "C", Query: "INSERT"})

	require.ErrorIs(t, err, types.ErrInvalidCluster)
	assert.Equal(t, 0, m.Len(), "a rejected payload must not hold queue capacity")
}

func TestNATSReplayerEnqueueRejectsUnknownTargetCluster(t *testing.T) {
	js := startNATSForTest(t)
	r := newTestReplayer(t, js, "test-target-cluster-enqueue", "test.target.enqueue")

	err := r.Enqueue(t.Context(), types.ReplayPayload{TargetCluster: "C", Query: "INSERT"})

	require.ErrorIs(t, err, types.ErrInvalidCluster)
	pending, err := r.Pending(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 0, pending, "a rejected payload must not be published")
}

func TestNATSReplayerDequeueTerminatesUnknownTargetCluster(t *testing.T) {
	js := startNATSForTest(t)

	var corrupt atomic.Int32
	const prefix = "test.target.decode"
	r := newTestReplayer(t, js, "test-target-cluster-decode", prefix,
		WithOnCorruptMessage(func(cbErr error) {
			assert.ErrorIs(t, cbErr, types.ErrInvalidCluster)
			corrupt.Add(1)
		}),
	)

	// Bypass Enqueue so the unknown target reaches the decoder.
	publishRawBatch(t, js, highSubject(prefix), natsReplayMessage{
		TargetCluster: "C",
		Query:         "INSERT",
		Priority:      int(types.PriorityHigh),
	})

	msgs, err := r.Dequeue(t.Context(), types.ClusterA, 10)
	require.NoError(t, err)
	assert.Empty(t, msgs, "an unroutable payload must not be handed to the worker")

	pending, err := r.Pending(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 0, pending, "an unroutable payload must be terminated, not redelivered")
	assert.Equal(t, int32(1), corrupt.Load())
}
