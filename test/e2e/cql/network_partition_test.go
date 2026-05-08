//go:build e2e

package cql_test

// Tests that exercise testutil.NetworkDisconnect — the realest reproducible
// analog to a cloud network blip. Different from Pause (process is still
// answering) and Kill (process is dead): the container keeps running but
// becomes unreachable from the host. Existing host-side TCP connections
// hang or die in zombie/half-open territory. None of the existing e2e
// tests use this failure mode.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/test/testutil"
	htypes "github.com/arloliu/helix/types"
)

// dockerBridgeNetwork is the default Docker network name. Testcontainers
// attaches CQLCluster containers here unless configured otherwise.
const dockerBridgeNetwork = "bridge"

// TestS_NetworkDisconnect_StickyReadFailover verifies StickyRead failover
// when cluster A is partitioned from the host (real network unreachable,
// not a paused process). After NetworkReconnect, sessions stay dead until
// rebuilt — we verify the failover, not the recovery (recovery would
// require AutoRefresh / SwapSession, covered by other tests).
func TestS_NetworkDisconnect_StickyReadFailover(t *testing.T) {
	a, b := sharedClusters(t)

	table := createKVTableOnBoth(t, "netdisc_sticky")
	seedKV(t, a, b, table, "k", "v")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			// Per-subtest disconnect/reconnect — the parent t.Cleanup pattern
			// only fires after BOTH subtests complete, leaving the container
			// disconnected when subtest #2 tries to disconnect it again.
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				_ = a.NetworkReconnect(ctx, dockerBridgeNetwork)
				_ = a.Reconnect(ctx)
			})

			rs := policy.NewStickyRead(
				policy.WithPreferredCluster(htypes.ClusterA),
				policy.WithStickyReadCooldown(1*time.Hour),
			)
			mc := testutil.NewTestMetricsCollector()

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithReadStrategy(rs),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
				helix.WithMetrics(mc),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			require.NoError(t, a.NetworkDisconnect(ctx, dockerBridgeNetwork),
				"network disconnect on cluster A")

			// Drive reads — first one fails over from A to B, subsequent ones
			// land on B directly under the sticky preference.
			for i := 0; i < 4; i++ {
				qCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
				var got string
				err := client.Query("SELECT value FROM "+table+" WHERE key = ?", "k").
					ScanContext(qCtx, &got)
				cancel()
				require.NoError(t, err,
					"[%s] read %d should succeed via cluster B during partition", d.name, i)
				assert.Equal(t, "v", got)
			}

			assert.GreaterOrEqual(t, mc.GetTotalFailovers(), int64(1),
				"[%s] expected at least one failover event", d.name)
			assert.Equal(t, htypes.ClusterB, rs.Preferred(),
				"[%s] StickyRead must have switched preferred to ClusterB during partition", d.name)
		})
	}
}
