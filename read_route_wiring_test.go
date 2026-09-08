package helix_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/topology"
	"github.com/arloliu/helix/types"
)

// cancelOnFailoverPolicy allows failover and ends the caller's context from
// inside ShouldFailover, so the read reaches the failover gating with a
// live context and leaves it with a dead one.
type cancelOnFailoverPolicy struct {
	cancel context.CancelFunc
}

func (p *cancelOnFailoverPolicy) ShouldFailover(types.ClusterID, error) bool {
	p.cancel()

	return true
}
func (p *cancelOnFailoverPolicy) RecordFailure(types.ClusterID) {}
func (p *cancelOnFailoverPolicy) RecordSuccess(types.ClusterID) {}

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

// TestReadStrategy_NotConsultedForRefusedFailover proves a failover the
// client refuses on its own grounds never reaches the read strategy: the
// preference, the read_preferred gauge, and the event stream all stay on
// the cluster that keeps serving the reads.
func TestReadStrategy_NotConsultedForRefusedFailover(t *testing.T) {
	tests := []struct {
		name           string
		drainB         bool
		cancelInPolicy bool
	}{
		{name: "the alternative is draining", drainB: true},
		{name: "the caller's context already ended", cancelInPolicy: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			var failover helix.FailoverPolicy = policy.NewActiveFailover()
			if tt.cancelInPolicy {
				failover = &cancelOnFailoverPolicy{cancel: cancel}
			}

			sticky := policy.NewStickyRead(policy.WithPreferredCluster(types.ClusterA))
			mc := testutil.NewTestMetricsCollector()
			rec := newEventRecorder()
			watcher := topology.NewLocal()
			client, err := helix.NewCQLClient(
				newAlwaysFailSession(errors.New("cluster A down")), newAlwaysOKSession(),
				helix.WithReadStrategy(sticky),
				helix.WithFailoverPolicy(failover),
				helix.WithTopologyWatcher(watcher),
				helix.WithMetrics(mc),
				helix.WithOnClusterEvent(rec.handler),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			if tt.drainB {
				require.NoError(t, watcher.SetDrain(t.Context(), types.ClusterB, true, "maintenance"))
				rec.waitFor(t, func(ev types.ClusterEvent) bool { return ev.Kind == types.EventDrainEntered })
			}

			var v string
			require.Error(t, client.Query("SELECT v FROM t WHERE k = ?", 1).ScanContext(ctx, &v))

			require.Equal(t, types.ClusterA, sticky.Preferred(),
				"a refused failover must not move the preference")
			require.True(t, mc.ReadPreferred[types.ClusterA])
			require.False(t, mc.ReadPreferred[types.ClusterB])

			// Events arrive in enqueue order: a manual move made now lands
			// after any move the failover would have recorded, so the first
			// route change seen must be this one.
			sticky.SetPreferred(types.ClusterB)
			ev := rec.waitFor(t, func(ev types.ClusterEvent) bool { return ev.Kind == types.EventReadRouteChanged })
			require.Equal(t, "manual", ev.Reason, "no read_route_changed may precede the manual move")
		})
	}
}
