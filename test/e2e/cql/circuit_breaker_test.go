//go:build e2e

package cql_test

// Plain CircuitBreaker (vs LatencyCircuitBreaker) had no e2e coverage.
// LCB is tested by S3, but the simpler counter-based CircuitBreaker —
// trip on consecutive-failure threshold, half-open after reset timeout,
// close after a successful probe — has only unit-test coverage.

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

// TestS_PlainCircuitBreaker_TripAndClose verifies the counter-based
// CircuitBreaker lifecycle under a real outage:
//
//  1. Pause cluster A.
//  2. Drive reads against A; each timeout is a failure.
//  3. After threshold consecutive failures, ShouldFailover(A) → true (open).
//  4. Unpause A; after resetTimeout the client's recovery probe reserves
//     the breaker and its success closes it, so ShouldFailover(A) returns
//     false again without any caller read being used as the probe.
//  5. A successful probe closes the breaker.
func TestS_PlainCircuitBreaker_TripAndClose(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, a)

	table := createKVTableOnBoth(t, "plain_cb")
	seedKV(t, a, b, table, "k", "v")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			ensureReachable(t, a, d)
			cb := policy.NewCircuitBreaker(
				policy.WithThreshold(3),
				policy.WithResetTimeout(3*time.Second),
			)
			mc := testutil.NewTestMetricsCollector()

			opts := []helix.Option{
				helix.WithReadStrategy(policy.NewStickyRead(
					policy.WithPreferredCluster(htypes.ClusterA),
				)),
				helix.WithFailoverPolicy(cb),
				helix.WithMetrics(mc),
				helix.WithLogger(testutil.NewTestLogger(t)),
			}
			opts = append(opts, withSessionRebuild(a, d)...)
			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b), opts...)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			require.NoError(t, a.Pause(ctx))
			defer func() { _ = a.Unpause(context.Background()) }()

			// Drive enough reads to accumulate failures past the threshold.
			deadline := time.Now().Add(30 * time.Second)
			for !cb.ShouldFailover(htypes.ClusterA, nil) && time.Now().Before(deadline) {
				qCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				var got string
				_ = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k").
					ScanContext(qCtx, &got)
				cancel()
			}
			require.True(t, cb.ShouldFailover(htypes.ClusterA, nil),
				"[%s] CircuitBreaker did not trip within deadline", d.name)
			assert.GreaterOrEqual(t, mc.CircuitBreakerTrips[htypes.ClusterA], int64(1),
				"[%s] CircuitBreaker trip metric should fire on open", d.name)

			// Unpause + wait for the reset timeout. The client's recovery
			// probe (default interval) reserves the breaker and closes it.
			require.NoError(t, a.Unpause(ctx))

			require.Eventually(t, func() bool {
				return !cb.ShouldFailover(htypes.ClusterA, nil)
			}, 30*time.Second, 200*time.Millisecond,
				"[%s] CircuitBreaker did not close after Unpause + reset timeout via the recovery probe",
				d.name)
		})
	}
}
