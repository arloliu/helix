//go:build e2e

package cql_test

// Cascading-failure scenarios: cluster A fails first, traffic moves to B,
// then B itself fails. This is a real production sequence (e.g., a
// region-wide blip A → operator drains B for maintenance assuming A is
// recovering → realization). Existing tests cover single-cluster failure
// in isolation but never the failover-then-failover-again sequence.

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

// TestS_SequentialFailures_AThenB exercises the cascading-failure path.
//
// Sequence:
//  1. Pause A. Read fails over to B (cluster A becomes unhealthy, traffic
//     moves to B).
//  2. Unpause A. Cooldown is short so StickyRead can re-evaluate.
//  3. Pause B. Read must now route back to A. The failover policy must
//     allow a second flip — health state is per-cluster, not a one-way
//     "is in failed list" — and the failover metric must increment again.
//
// What this proves:
//   - Helix's failover state for cluster A is cleared once A recovers, so
//     A is eligible to receive traffic again when B subsequently fails.
//   - The failover metric records every transition, not just the first.
func TestS_SequentialFailures_AThenB(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, a)
	withRestoredCluster(t, b)

	table := createKVTableOnBoth(t, "cascading_a_then_b")
	seedKV(t, a, b, table, "k", "v")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			// Short cooldown — we WANT the strategy to re-evaluate
			// preferred on the next failure rather than locking on B.
			rs := policy.NewStickyRead(
				policy.WithPreferredCluster(htypes.ClusterA),
				policy.WithStickyReadCooldown(500*time.Millisecond),
			)
			mc := testutil.NewTestMetricsCollector()

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithReadStrategy(rs),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
				helix.WithMetrics(mc),
				// A read leg must give up before the caller's budget is
				// gone: the v2 driver lets a caller deadline override the
				// connection's request timeout, so without a deadline of
				// Helix's own the first cluster consumes the whole 15s and
				// failover never reaches the second.
				helix.WithClusterReadTimeout(2*time.Second),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()

			// Phase 1: pause A, drive reads to force the first failover.
			require.NoError(t, a.Pause(ctx))
			for i := 0; i < 3; i++ {
				qCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
				var got string
				err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k").
					ScanContext(qCtx, &got)
				cancel()
				require.NoError(t, err, "[%s] phase 1 read %d", d.name, i)
				assert.Equal(t, "v", got)
			}
			phase1Failovers := mc.GetTotalFailovers()
			require.GreaterOrEqual(t, phase1Failovers, int64(1),
				"[%s] phase 1 should record at least one failover A→B", d.name)

			// Phase 2: unpause A; let the cooldown expire so the strategy
			// can re-evaluate on the next failure.
			require.NoError(t, a.Unpause(ctx))
			recoveredAt := time.Now()
			require.Eventually(t, func() bool {
				return time.Since(recoveredAt) >= 500*time.Millisecond && rs.Preferred() == htypes.ClusterB
			}, 2*time.Second, 50*time.Millisecond,
				"[%s] StickyRead cooldown must elapse while preference remains on B", d.name)

			// Phase 3: pause B, drive reads. Helix must failover from B to A.
			require.NoError(t, b.Pause(ctx))
			defer func() { _ = b.Unpause(context.Background()) }()
			for i := 0; i < 3; i++ {
				qCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
				var got string
				err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k").
					ScanContext(qCtx, &got)
				cancel()
				require.NoError(t, err,
					"[%s] phase 3 read %d should fall back to A after B paused", d.name, i)
				assert.Equal(t, "v", got)
			}
			assert.Greater(t, mc.GetTotalFailovers(), phase1Failovers,
				"[%s] phase 3 must record an additional failover B→A", d.name)
		})
	}
}
