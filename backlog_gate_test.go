package helix

import (
	"sync/atomic"
	"testing"

	"github.com/arloliu/helix/policy"
	"github.com/stretchr/testify/require"
)

func TestExcludeWhileReplayBacklog(t *testing.T) {
	var depthA, depthB atomic.Int64
	depth := func(cluster ClusterID) int {
		if cluster == ClusterA {
			return int(depthA.Load())
		}

		return int(depthB.Load())
	}
	allowed := ExcludeWhileReplayBacklog(depth, 10)

	require.Nil(t, allowed(), "no backlog: normal routing")
	depthA.Store(11)
	require.Equal(t, []ClusterID{ClusterB}, allowed(), "A over the threshold is excluded")
	depthA.Store(10)
	require.Nil(t, allowed(), "at the threshold is not over it")
	depthB.Store(50)
	require.Equal(t, []ClusterID{ClusterA}, allowed())
	depthA.Store(50)
	require.Nil(t, allowed(), "both over the threshold: nothing better than normal routing")
}

func TestExcludeWhileReplayBacklog_RoutesReadsUntilDrained(t *testing.T) {
	sa, sb := newReadProbeSession(), newReadProbeSession()
	var depthA atomic.Int64
	depth := func(cluster ClusterID) int {
		if cluster == ClusterA {
			return int(depthA.Load())
		}

		return 0
	}
	client := newReadProbeClient(t, sa, sb,
		WithReadStrategy(policy.NewStickyRead(policy.WithPreferredCluster(ClusterA))),
		WithFailoverPolicy(policy.NewActiveFailover()),
		WithAllowedClusters(ExcludeWhileReplayBacklog(depth, 0)),
	)

	var v string
	depthA.Store(3)
	require.NoError(t, client.Query("SELECT v FROM t").ScanContext(t.Context(), &v))
	require.Zero(t, sa.scans.Load(), "reads stay away from A while its backlog is pending")
	require.Equal(t, int64(1), sb.scans.Load())

	depthA.Store(0)
	require.NoError(t, client.Query("SELECT v FROM t").ScanContext(t.Context(), &v))
	require.Equal(t, int64(1), sa.scans.Load(), "reads return to the preferred cluster once the backlog drained")
}
