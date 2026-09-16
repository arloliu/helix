package helix

import (
	"context"
	"testing"
	"time"

	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// stallUntilCancelled is a scripted read that accepts the request and never
// answers, leaving the leg to end on whichever deadline reaches it first.
// It is what a cluster that is up but not answering looks like from below
// Helix, and the only fault a per-leg deadline exists to bound.
func stallUntilCancelled(ctx context.Context) error {
	<-ctx.Done()

	return ctx.Err()
}

// TestClusterReadTimeout_TripsTheLatencyBreakerAndReroutes walks the whole
// chain the per-leg read deadline and a LatencyCircuitBreaker form together:
// a leg the deadline ends is a cluster failure, consecutive failures reach
// the breaker's threshold, the breaker opens, and reads leave the stalled
// cluster.
//
// absoluteMax is set beyond any duration the test can produce, so the trip
// can only have come from the deadline's failures: the breaker's latency
// branch needs a read that *succeeds* slowly ([clusterHealth.readSucceeded]
// is its only caller), and a leg the deadline ends never succeeds.
func TestClusterReadTimeout_TripsTheLatencyBreakerAndReroutes(t *testing.T) {
	sa, sb := newReadProbeSession(), newReadProbeSession()
	sa.setScan(stallUntilCancelled)

	const threshold = 3
	lcb := policy.NewLatencyCircuitBreaker(
		policy.WithLatencyAbsoluteMax(time.Hour),
		policy.WithLatencyThreshold(threshold),
		policy.WithLatencyResetTimeout(time.Hour),
	)
	sticky := policy.NewStickyRead(policy.WithPreferredCluster(ClusterA))
	client := newReadProbeClient(t, sa, sb,
		WithReadStrategy(sticky),
		WithFailoverPolicy(lcb),
		WithClusterReadTimeout(20*time.Millisecond),
	)

	var v string

	// Below the threshold the breaker refuses failover, so each expired leg
	// reaches the caller as Helix's own timeout while counting against A.
	for i := 1; i < threshold; i++ {
		err := client.Query("SELECT v FROM t").ScanContext(t.Context(), &v)
		require.ErrorIs(t, err, types.ErrClusterTimeout,
			"read %d must surface the expired leg while the breaker is still closed", i)
		require.Equal(t, i, lcb.Failures(ClusterA),
			"read %d: an expired leg is a cluster failure, not the caller's doing", i)
		require.False(t, lcb.ShouldFailover(ClusterA, nil), "read %d must leave the breaker closed", i)
		require.Zero(t, sb.scans.Load(), "read %d may not reach B while the breaker is closed", i)
		require.Equal(t, ClusterA, sticky.Preferred(),
			"read %d: a failure the breaker refuses to act on may not move the preference", i)
	}

	// The read that reaches the threshold opens the breaker and then finds
	// failover allowed, so the caller sees B's answer rather than a timeout.
	require.NoError(t, client.Query("SELECT v FROM t").ScanContext(t.Context(), &v),
		"the read that trips the breaker must be retried on B")
	require.Equal(t, threshold, lcb.Failures(ClusterA))
	require.True(t, lcb.ShouldFailover(ClusterA, nil), "consecutive expired legs must open A's breaker")
	require.Equal(t, int64(threshold), sa.scans.Load(), "every read so far was offered to A first")
	require.Equal(t, int64(1), sb.scans.Load(), "only the tripping read reached B")

	// The failover the open breaker allowed is also what moves the sticky
	// preference, so later reads go straight to B and stop paying the leg
	// deadline on a cluster that is not answering.
	require.Equal(t, ClusterB, sticky.Preferred(), "the failover must carry the preference to B")

	const followUps = 3
	start := time.Now()
	for i := range followUps {
		require.NoError(t, client.Query("SELECT v FROM t").ScanContext(t.Context(), &v),
			"follow-up read %d must be served by B", i+1)
	}
	require.Equal(t, int64(threshold), sa.scans.Load(), "no follow-up read may be offered to the stalled cluster")
	require.Equal(t, int64(1+followUps), sb.scans.Load())
	require.Less(t, time.Since(start), 20*time.Millisecond,
		"a follow-up read goes straight to B, so none of them waits out a leg deadline")
}

// TestClusterReadTimeout_CallerDeadlineLeavesTheBreakerClosed is the
// negative half of [TestClusterReadTimeout_TripsTheLatencyBreakerAndReroutes]:
// the same stalled cluster, but the caller's own budget is what expires.
// That is the caller's doing, so it must not count against A no matter how
// often it happens.
func TestClusterReadTimeout_CallerDeadlineLeavesTheBreakerClosed(t *testing.T) {
	sa, sb := newReadProbeSession(), newReadProbeSession()
	sa.setScan(stallUntilCancelled)

	lcb := policy.NewLatencyCircuitBreaker(
		policy.WithLatencyAbsoluteMax(time.Hour),
		policy.WithLatencyThreshold(3),
		policy.WithLatencyResetTimeout(time.Hour),
	)
	client := newReadProbeClient(t, sa, sb,
		WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(ClusterA),
			policy.WithStickyReadCooldown(0),
		)),
		WithFailoverPolicy(lcb),
		// No WithClusterReadTimeout: the leg runs on the caller's context.
	)

	var v string
	for i := 1; i <= 3; i++ {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		err := client.Query("SELECT v FROM t").ScanContext(ctx, &v)
		cancel()
		require.ErrorIs(t, err, context.DeadlineExceeded, "read %d ends on the caller's budget", i)
		require.NotErrorIs(t, err, types.ErrClusterTimeout,
			"read %d: without a leg deadline the expiry is the caller's, not Helix's", i)
	}

	require.Zero(t, lcb.Failures(ClusterA), "a caller that gives up says nothing about the cluster")
	require.False(t, lcb.ShouldFailover(ClusterA, nil))
	require.Zero(t, sb.scans.Load(), "a caller-expired read never reaches the other cluster")
}
