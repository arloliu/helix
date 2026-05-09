//go:build e2e

package cql_test

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

// TestS5_LongOutageNoTraffic verifies StickyRead's no-passive-probe contract:
// after failover, cooldown expiry alone does not move the preferred cluster
// back. StickyRead only re-evaluates when the current preferred cluster fails
// again on a later request.
//
// The test:
//  1. Pause cluster A. Drive a small read burst so the strategy switches
//     to B and observes A as unavailable.
//  2. Idle for 30s with no traffic.
//  3. Unpause cluster A.
//  4. Continue idling another 5s with no traffic.
//  5. Resume reads. Assert the read succeeds and that preferred still stays on B.
//
// This locks in the current implementation contract so operators can rely on
// the fact that a recovered non-preferred cluster will not be probed again
// until the current preferred cluster produces another failure.
func TestS5_LongOutageNoTraffic(t *testing.T) {
	if testing.Short() {
		t.Skip("S5 takes ~45s of wall-clock — skipped in short mode")
	}

	a, b := sharedClusters(t)
	withRestoredCluster(t, a)

	table := createKVTableOnBoth(t, "s5_long_outage")
	seedKV(t, a, b, table, "k", "v")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			rs := policy.NewStickyRead(
				policy.WithPreferredCluster(htypes.ClusterA),
				policy.WithStickyReadCooldown(10*time.Second),
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
			require.NoError(t, a.Pause(ctx))

			// Phase 1: drive reads until the strategy switches to B.
			qCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			var got string
			err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k").
				ScanContext(qCtx, &got)
			cancel()
			require.NoError(t, err, "[%s] phase 1 read should fall over to B", d.name)
			require.Equal(t, htypes.ClusterB, rs.Preferred(),
				"[%s] StickyRead should be on ClusterB after failover", d.name)
			t.Logf("[%s] phase 1 done: preferred=%s failovers=%d",
				d.name, rs.Preferred(), mc.GetTotalFailovers())

			// Phase 2: idle for 30s with no traffic.
			t.Logf("[%s] phase 2: idle 30s with no traffic…", d.name)
			require.Never(t, func() bool {
				return rs.Preferred() != htypes.ClusterB
			}, 30*time.Second, 500*time.Millisecond,
				"[%s] StickyRead preference changed during idle period", d.name)

			// Phase 3: unpause A, then idle another 5s — exceeds the 10s
			// cooldown set above when combined with the 30s idle.
			require.NoError(t, a.Unpause(ctx))
			require.Never(t, func() bool {
				return rs.Preferred() != htypes.ClusterB
			}, 5*time.Second, 500*time.Millisecond,
				"[%s] StickyRead preference changed after recovery without traffic", d.name)

			// Phase 4: resume reads. StickyRead should continue using B —
			// cooldown expiry alone does not trigger a passive probe back to A.
			qCtx, cancel = context.WithTimeout(ctx, 15*time.Second)
			err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k").
				ScanContext(qCtx, &got)
			cancel()
			require.NoError(t, err,
				"[%s] phase 4 read should succeed via the current preferred cluster", d.name)
			assert.Equal(t, "v", got)
			assert.Equal(t, htypes.ClusterB, rs.Preferred(),
				"[%s] cooldown expiry alone must not switch StickyRead back to A", d.name)
			t.Logf("[%s] phase 4 done: preferred=%s failovers=%d", d.name, rs.Preferred(), mc.GetTotalFailovers())
		})
	}
}
