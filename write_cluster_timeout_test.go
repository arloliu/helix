package helix

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

func TestClusterWriteTimeout_SlowLegIsReplayedAndAcknowledged(t *testing.T) {
	sa, sb := newBlockingSession(), newMockSession()
	replayer := &mockReplayer{}
	client, err := NewCQLClient(sa, sb,
		WithClusterWriteTimeout(20*time.Millisecond),
		WithReplayer(replayer),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	start := time.Now()
	err = client.Query("INSERT INTO t (k, v) VALUES (?, ?)", 1, "v").ExecContext(t.Context())
	require.NoError(t, err, "the fast leg's acknowledgement stands")
	require.Less(t, time.Since(start), time.Second, "the write must return once the slow leg expires")

	replayer.Lock()
	defer replayer.Unlock()
	require.Len(t, replayer.payloads, 1)
	require.Equal(t, ClusterA, replayer.payloads[0].TargetCluster, "the expired leg is replayed")
	require.Equal(t, int32(1), client.statsForCluster(ClusterA).consecutiveFailures.Load(),
		"a leg deadline is Helix's own, so its expiry is a health signal")
	require.Equal(t, int32(0), client.statsForCluster(ClusterB).consecutiveFailures.Load())
}

func TestClusterWriteTimeout_StrictLegExpiresAsClusterTimeout(t *testing.T) {
	sa, sb := newBlockingSession(), newMockSession()
	client, err := NewCQLClient(sa, sb, WithClusterWriteTimeout(20*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	start := time.Now()
	err = client.Query("INSERT INTO t (k, v) VALUES (?, ?)", 1, "v").Strict().ExecContext(t.Context())
	require.Less(t, time.Since(start), time.Second, "the strict write must return once the slow leg expires")
	require.ErrorIs(t, err, types.ErrClusterTimeout, "a strict leg reports its own deadline")
	require.Equal(t, int32(1), client.statsForCluster(ClusterA).consecutiveFailures.Load())
	require.Equal(t, int32(0), client.statsForCluster(ClusterB).consecutiveFailures.Load())
}

func TestClusterWriteTimeout_DegradedBackgroundLegIsReplayed(t *testing.T) {
	sa, sb := newBlockingSession(), newMockSession()
	adaptive := policy.NewAdaptiveDualWrite()
	replayer := &mockReplayer{}
	client, err := NewCQLClient(sa, sb,
		WithWriteStrategy(adaptive),
		WithClusterWriteTimeout(20*time.Millisecond),
		WithReplayer(replayer),
		WithRecoveryProbeDisabled(),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	degradeClusterA(t, adaptive)

	require.NoError(t, client.Query("INSERT INTO t (k, v) VALUES (?, ?)", 1, "v").ExecContext(t.Context()),
		"the degraded cluster's leg runs in the background")
	require.Eventually(t, func() bool {
		replayer.Lock()
		defer replayer.Unlock()

		return len(replayer.payloads) == 1 && replayer.payloads[0].TargetCluster == ClusterA
	}, time.Second, time.Millisecond, "the expired background leg is replayed")
	require.Eventually(t, func() bool { return client.statsForCluster(ClusterA).consecutiveFailures.Load() == 1 },
		time.Second, time.Millisecond, "the background leg's deadline is a health signal")
}

func TestClusterWriteTimeout_CallerDeadlineStillWins(t *testing.T) {
	sa, sb := newBlockingSession(), newMockSession()
	replayer := &mockReplayer{}
	client, err := NewCQLClient(sa, sb,
		WithClusterWriteTimeout(time.Second),
		WithReplayer(replayer),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	err = client.Query("INSERT INTO t (k, v) VALUES (?, ?)", 1, "v").ExecContext(ctx)
	require.NoError(t, err, "cluster B acknowledged the write")

	require.Equal(t, int32(0), client.statsForCluster(ClusterA).consecutiveFailures.Load(),
		"a failure observed after the caller's deadline is the caller's doing")
	replayer.Lock()
	defer replayer.Unlock()
	require.Len(t, replayer.payloads, 1, "the unacknowledged leg is still replayed")
}

func TestClusterWriteTimeout_Disabled(t *testing.T) {
	sa, sb := newBlockingSession(), newMockSession()
	client, err := NewCQLClient(sa, sb)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	require.NoError(t, client.Query("INSERT INTO t (k, v) VALUES (?, ?)", 1, "v").ExecContext(ctx))
	require.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond,
		"without the option the slow leg holds the write until the caller's deadline")
}

func TestClusterWriteTimeout_NegativeRejected(t *testing.T) {
	_, err := NewCQLClient(newMockSession(), newMockSession(), WithClusterWriteTimeout(-time.Second))
	var optErr *types.OptionError
	require.True(t, errors.As(err, &optErr))
	require.Equal(t, "WithClusterWriteTimeout", optErr.Option)
}
