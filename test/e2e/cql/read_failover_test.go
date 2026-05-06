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

// TestS2_PauseA_ReadFailover runs a read-failover scenario across the
// matrix {StickyRead, PrimaryOnlyRead, RoundRobinRead} × {v1, v2}.
//
// For each combination: pause cluster A, perform a read for a key seeded
// on both clusters, expect the value back from the surviving cluster B.
// This is the canonical "the cluster Helix is reading from died — does
// the read still succeed?" test, exercised against the real driver
// reconnect/timeout machinery rather than the chaos-session wrapper.
func TestS2_PauseA_ReadFailover(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, a)

	type strategyCase struct {
		name             string
		factory          func() helix.ReadStrategy
		expectsFailovers bool // does this strategy fire the failover metric?
		// stateAssert runs after the read returns. Each strategy has
		// different observable state to assert on.
		stateAssert func(t *testing.T, rs helix.ReadStrategy)
	}

	strategies := []strategyCase{
		{
			name: "StickyRead",
			factory: func() helix.ReadStrategy {
				return policy.NewStickyRead(
					policy.WithPreferredCluster(htypes.ClusterA),
					policy.WithStickyReadCooldown(1*time.Hour),
				)
			},
			expectsFailovers: true,
			stateAssert: func(t *testing.T, rs helix.ReadStrategy) {
				sr, ok := rs.(*policy.StickyRead)
				require.True(t, ok)
				assert.Equal(t, htypes.ClusterB, sr.Preferred(),
					"StickyRead must have switched preferred to ClusterB")
			},
		},
		{
			name: "PrimaryOnlyRead",
			factory: func() helix.ReadStrategy {
				return policy.NewPrimaryOnlyRead()
			},
			expectsFailovers: true,
			stateAssert: func(t *testing.T, rs helix.ReadStrategy) {
				por, ok := rs.(*policy.PrimaryOnlyRead)
				require.True(t, ok)
				assert.Equal(t, htypes.ClusterB, por.Select(context.Background()),
					"PrimaryOnlyRead must route to ClusterB while ClusterA is paused")
			},
		},
		{
			name: "RoundRobinRead",
			factory: func() helix.ReadStrategy {
				return policy.NewRoundRobinRead()
			},
			// RoundRobinRead has no "preferred" cluster, so a paused A is
			// not observed as a failover by the policy — the next op simply
			// rotates to B. This was confirmed by the e2e suite (see
			// SPIKE_FINDINGS.md §6 for whether this is the desired
			// observability contract).
			expectsFailovers: false,
			stateAssert:      func(t *testing.T, rs helix.ReadStrategy) {},
		},
	}

	table := createKVTableOnBoth(t, "s2_read_failover")
	seedKV(t, a, b, table, "k", "v")

	for _, d := range allDrivers {
		for _, s := range strategies {
			t.Run(d.name+"/"+s.name, func(t *testing.T) {
				rs := s.factory()
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
				defer func() { _ = a.Unpause(context.Background()) }()

				qCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
				defer cancel()

				var got string
				err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k").
					ScanContext(qCtx, &got)
				require.NoError(t, err,
					"[%s/%s] read should succeed via cluster B", d.name, s.name)
				assert.Equal(t, "v", got)

				if s.expectsFailovers {
					assert.GreaterOrEqual(t, mc.GetTotalFailovers(), int64(1),
						"[%s/%s] expected at least one failover event", d.name, s.name)
				} else {
					t.Logf("[%s/%s] failovers=%d (strategy doesn't track failovers)",
						d.name, s.name, mc.GetTotalFailovers())
				}

				s.stateAssert(t, rs)
			})
		}
	}
}
