package helix_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
)

// TestReadStrategy_ReceivesCollectorAndEmitter proves the client installs
// its collector and its event dispatcher on a read strategy that reports
// its route, so a preference move reaches both the gauge and the handler.
func TestReadStrategy_ReceivesCollectorAndEmitter(t *testing.T) {
	sticky := policy.NewStickyRead(policy.WithPreferredCluster(types.ClusterA))
	mc := testutil.NewTestMetricsCollector()
	rec := newEventRecorder()
	client, err := helix.NewCQLClient(newAlwaysOKSession(), newAlwaysOKSession(),
		helix.WithReadStrategy(sticky),
		helix.WithMetrics(mc),
		helix.WithOnClusterEvent(rec.handler),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.True(t, mc.ReadPreferred[types.ClusterA], "the gauge is published when the collector is installed")

	sticky.SetPreferred(types.ClusterB)
	ev := rec.waitFor(t, func(ev types.ClusterEvent) bool { return ev.Kind == types.EventReadRouteChanged })
	require.Equal(t, types.ClusterB, ev.ToCluster)
	require.Equal(t, "manual", ev.Reason)
	require.True(t, mc.ReadPreferred[types.ClusterB])
	require.False(t, mc.ReadPreferred[types.ClusterA])
}
