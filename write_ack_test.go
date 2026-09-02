package helix

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// replayDropCounter counts IncReplayDropped calls per cluster.
type replayDropCounter struct {
	metrics.NopMetrics
	dropped map[ClusterID]*atomic.Int32
}

func newReplayDropCounter() *replayDropCounter {
	return &replayDropCounter{dropped: map[ClusterID]*atomic.Int32{ClusterA: {}, ClusterB: {}}}
}

func (m *replayDropCounter) IncReplayDropped(cluster types.ClusterID) {
	m.dropped[cluster].Add(1)
}

// TestWrite_AckOnReplayAdmissionReturnsNilOnceEnqueued asserts that the
// restore mode reports success for a write no cluster acknowledged as long
// as every leg was admitted to the replay queue.
func TestWrite_AckOnReplayAdmissionReturnsNilOnceEnqueued(t *testing.T) {
	replayer := &mockReplayer{}
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithWriteStrategy(&mockWriteStrategy{errA: types.ErrWriteAsync, errB: types.ErrWriteDropped}),
		WithReplayer(replayer),
		WithAckMode(AckOnReplayAdmission),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.NoError(t, client.Query("INSERT INTO t (id) VALUES (?)", 1).Exec())
	require.Len(t, replayer.payloads, 2)
}

// TestWrite_AckOnReplayAdmissionStillFailsWhenEnqueueFails asserts that the
// restore mode never reports success for a write that reached neither a
// cluster nor the replay queue.
func TestWrite_AckOnReplayAdmissionStillFailsWhenEnqueueFails(t *testing.T) {
	// One slot: the second leg's enqueue overflows.
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(1))
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithWriteStrategy(&mockWriteStrategy{errA: types.ErrWriteAsync, errB: types.ErrWriteAsync}),
		WithReplayer(replayer),
		WithAckMode(AckOnReplayAdmission),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	err = client.Query("INSERT INTO t (id) VALUES (?)", 1).Exec()
	var noAck *types.NoSynchronousAckError
	require.ErrorAs(t, err, &noAck)
	require.ErrorIs(t, noAck.Replay, types.ErrReplayQueueFull)
}

// TestWrite_NoReplayerReportsEveryUnacknowledgedLeg asserts that without a
// replayer a leg that would have needed replay is counted and reported as
// a dropped replay, so the loss is visible, and that a partial failure is
// still success for the caller while a zero-ack write is not.
func TestWrite_NoReplayerReportsEveryUnacknowledgedLeg(t *testing.T) {
	failB := errors.New("cluster B down")
	mc := newReplayDropCounter()
	var dropped []types.ClusterEvent
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithWriteStrategy(&mockWriteStrategy{errB: failB}),
		WithMetrics(mc),
		WithOnClusterEvent(func(ev types.ClusterEvent) {
			if ev.Kind == types.EventReplayDropped {
				dropped = append(dropped, ev)
			}
		}),
	)
	require.NoError(t, err)

	require.NoError(t, client.Query("INSERT INTO t (id) VALUES (?)", 1).Exec(),
		"one acknowledgement is still success")
	client.Close()

	require.Equal(t, int32(1), mc.dropped[ClusterB].Load())
	require.Zero(t, mc.dropped[ClusterA].Load())
	require.Len(t, dropped, 1)
	require.Equal(t, ClusterB, dropped[0].Cluster)
	require.ErrorIs(t, dropped[0].Err, types.ErrNoReplayer)

	zeroAck, err := NewCQLClient(newMockSession(), newMockSession(),
		WithWriteStrategy(&mockWriteStrategy{errA: types.ErrWriteAsync, errB: types.ErrWriteAsync}),
	)
	require.NoError(t, err)
	t.Cleanup(zeroAck.Close)

	err = zeroAck.Query("INSERT INTO t (id) VALUES (?)", 1).Exec()
	var noAck *types.NoSynchronousAckError
	require.ErrorAs(t, err, &noAck)
	require.ErrorIs(t, noAck.Replay, types.ErrNoReplayer)
}
