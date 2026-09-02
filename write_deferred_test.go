package helix

import (
	"errors"
	"testing"
	"time"

	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// TestAdaptiveWrite_DeferredFailureIsReplayed asserts that a fire-and-forget
// leg that fails in the background is enqueued for replay once, and that
// the write itself is still success because the other cluster acknowledged.
func TestAdaptiveWrite_DeferredFailureIsReplayed(t *testing.T) {
	failB := errors.New("cluster B rejected the background write")
	sa, sb := newRecordingSession(nil), newRecordingSession(failB)
	adaptive := policy.NewAdaptiveDualWrite()
	adaptive.ForceDegrade(ClusterB)
	replayer := &mockReplayer{}

	client, err := NewCQLClient(sa, sb,
		WithWriteStrategy(adaptive),
		WithRecoveryProbeDisabled(),
		WithReplayer(replayer),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	const writes = 2
	for i := range writes {
		require.NoError(t, client.Query("INSERT INTO t (k, v) VALUES (?, ?)", i, "v").ExecContext(t.Context()))
	}
	waitForExecs(t, sb, writes)

	require.Eventually(t, func() bool {
		replayer.Lock()
		defer replayer.Unlock()

		return len(replayer.payloads) == writes
	}, regressionWaitTimeout, time.Millisecond, "each failed background leg must be enqueued for replay once")

	replayer.Lock()
	defer replayer.Unlock()
	for _, p := range replayer.payloads {
		require.Equal(t, ClusterB, p.TargetCluster)
		require.Equal(t, "INSERT INTO t (k, v) VALUES (?, ?)", p.Query)
	}
}

// TestAdaptiveWrite_DeferredFailureWithoutReplayerIsReported asserts that a
// background failure with no replayer is counted as a dropped replay.
func TestAdaptiveWrite_DeferredFailureWithoutReplayerIsReported(t *testing.T) {
	failB := errors.New("cluster B rejected the background write")
	sa, sb := newRecordingSession(nil), newRecordingSession(failB)
	adaptive := policy.NewAdaptiveDualWrite()
	adaptive.ForceDegrade(ClusterB)
	mc := newReplayDropCounter()
	dropped := make(chan types.ClusterEvent, 4)

	client, err := NewCQLClient(sa, sb,
		WithWriteStrategy(adaptive),
		WithRecoveryProbeDisabled(),
		WithMetrics(mc),
		WithOnClusterEvent(func(ev types.ClusterEvent) {
			if ev.Kind == types.EventReplayDropped {
				dropped <- ev
			}
		}),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.NoError(t, client.Query("INSERT INTO t (k, v) VALUES (?, ?)", 1, "v").ExecContext(t.Context()))
	waitForExecs(t, sb, 1)

	select {
	case ev := <-dropped:
		require.Equal(t, ClusterB, ev.Cluster)
		require.ErrorIs(t, ev.Err, types.ErrNoReplayer)
	case <-time.After(regressionWaitTimeout):
		t.Fatal("the failed background leg must be reported as a dropped replay")
	}
	require.Equal(t, int32(1), mc.dropped[ClusterB].Load())
}
