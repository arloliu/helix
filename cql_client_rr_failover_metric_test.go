package helix_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
)

// TestRoundRobinRead_FailoverMetric_FiresOnFailedClusterRead verifies the
// SPIKE_FINDINGS §7 hypothesis ("RoundRobinRead does not increment the
// failover metric"). The original e2e observation was a single read that
// happened to land on the healthy cluster — so failover was never needed
// and the metric correctly stayed at 0. To prove the path works, drive
// enough reads that some land on the failed cluster.
//
// Setup: cluster A always errors on reads, cluster B always succeeds.
// RoundRobinRead alternates, so half the reads start on A → fail →
// failover to B. tryFallbackCluster fires IncFailoverTotal once per
// failover.
func TestRoundRobinRead_FailoverMetric_FiresOnFailedClusterRead(t *testing.T) {
	const reads = 20

	sessionA := newAlwaysFailSession(errors.New("cluster A read failure"))
	sessionB := newAlwaysOKSession()

	mc := testutil.NewTestMetricsCollector()
	client, err := helix.NewCQLClient(sessionA, sessionB,
		helix.WithReadStrategy(policy.NewRoundRobinRead()),
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
		helix.WithMetrics(mc),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	ctx := context.Background()
	for i := range reads {
		var got string
		_ = client.Query("SELECT v FROM t WHERE k = ?", i).ScanContext(ctx, &got)
	}

	failovers := mc.GetTotalFailovers()
	t.Logf("RoundRobinRead reads=%d failovers=%d", reads, failovers)

	// RR alternates A/B; ~half the calls Select A first, fail, then failover
	// to B. Exact count varies by initial counter state but should be
	// substantial — definitely not zero.
	assert.Greater(t, failovers, int64(0),
		"RoundRobinRead must fire IncFailoverTotal when Select hits the failed "+
			"cluster — observability gap in SPIKE_FINDINGS §7 was a test artifact "+
			"(single read landed on healthy cluster), not a real bug")

	// Sanity: A→B is the only direction (A always fails, B always succeeds).
	atob := mc.GetFailoverCount(types.ClusterA, types.ClusterB)
	btoa := mc.GetFailoverCount(types.ClusterB, types.ClusterA)
	assert.Greater(t, atob, int64(0), "A→B failover should fire")
	assert.Equal(t, int64(0), btoa, "B→A failover should not fire (B always healthy)")
}
