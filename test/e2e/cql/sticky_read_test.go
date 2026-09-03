//go:build e2e

package cql_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/test/testutil"
	htypes "github.com/arloliu/helix/types"
)

// TestS_StickyRead_LeavesDeadPreferredDuringCooldown verifies that
// StickyRead abandons a preferred cluster that is hard down even while the
// failover cooldown is active, as long as the other cluster is known good.
//
// The test:
//  1. Pause cluster A. One read fails over to B and the preference moves
//     to B, which starts a long cooldown.
//  2. Unpause A, then pause B. The first read fails on B and retries on A
//     per request; that success marks A known good.
//  3. The next read fails on B again and, because A is known good, the
//     preference moves back to A inside the cooldown.
//  4. Reads now go straight to A and return fast.
//
// Before this contract, the cooldown pinned reads to the dead preferred
// cluster: every read paid B's timeout before reaching A.
func TestS_StickyRead_LeavesDeadPreferredDuringCooldown(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, a)
	withRestoredCluster(t, b)

	table := createKVTableOnBoth(t, "s_sticky_known_good")
	seedKV(t, a, b, table, "k", "v")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			rs := policy.NewStickyRead(
				policy.WithPreferredCluster(htypes.ClusterA),
				policy.WithStickyReadCooldown(5*time.Minute),
			)
			mc := testutil.NewTestMetricsCollector()

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithReadStrategy(rs),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
				helix.WithMetrics(mc),
				helix.WithLogger(testutil.NewTestLogger(t)),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			read := func() (string, time.Duration, error) {
				qCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
				defer cancel()
				start := time.Now()
				var got string
				err := client.Query("SELECT value FROM "+table+" WHERE key = ?", "k").
					ScanContext(qCtx, &got)

				return got, time.Since(start), err
			}

			// Phase 1: A down, the preference moves to B and the cooldown starts.
			require.NoError(t, a.Pause(ctx))
			got, _, err := read()
			require.NoError(t, err, "[%s] the read fails over to B", d.name)
			require.Equal(t, "v", got)
			require.Equal(t, htypes.ClusterB, rs.Preferred(), "[%s] preference moves to B", d.name)

			// Phase 2: A back, B down.
			// Each read still fails on the preferred B; the first retry on A proves A good again.
			require.NoError(t, a.Unpause(ctx))
			require.NoError(t, b.Pause(ctx))
			defer func() { _ = b.Unpause(context.Background()) }()

			got, _, err = read()
			require.NoError(t, err, "[%s] the read fails over to A per request", d.name)
			require.Equal(t, "v", got)

			// Phase 3: the next failure on B moves the preference to the
			// known-good A, cooldown or not.
			require.Eventually(t, func() bool {
				_, _, _ = read()

				return rs.Preferred() == htypes.ClusterA
			}, 30*time.Second, 100*time.Millisecond,
				"[%s] a known-good alternative ends the pin on the dead preferred", d.name)

			// Phase 4: reads go straight to A without paying B's timeout.
			got, elapsed, err := read()
			require.NoError(t, err)
			require.Equal(t, "v", got)
			require.Less(t, elapsed, time.Second,
				"[%s] a read on the restored preference must not wait on B", d.name)
			t.Logf("[%s] preferred=%s failovers=%d direct read=%s",
				d.name, rs.Preferred(), mc.GetTotalFailovers(), elapsed)

			// Restore B for the next driver.
			require.NoError(t, b.Unpause(ctx))
		})
	}
}
