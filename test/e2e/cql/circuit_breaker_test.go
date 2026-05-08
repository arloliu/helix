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
//  4. Unpause A; after resetTimeout the breaker enters half-open and
//     ShouldFailover(A) returns false to allow a probe.
//  5. A successful probe closes the breaker.
func TestS_PlainCircuitBreaker_TripAndClose(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, a)

	table := createKVTableOnBoth(t, "plain_cb")
	seedKV(t, a, b, table, "k", "v")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			mc := testutil.NewTestMetricsCollector()
			cb := policy.NewCircuitBreaker(
				policy.WithThreshold(3),
				policy.WithResetTimeout(3*time.Second),
				// FailoverPolicy is not auto-injected with the client's
				// metrics collector — wire it explicitly so the trip
				// counter is observable.
				policy.WithCircuitBreakerMetrics(mc),
			)

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithReadStrategy(policy.NewStickyRead(
					policy.WithPreferredCluster(htypes.ClusterA),
				)),
				helix.WithFailoverPolicy(cb),
				helix.WithMetrics(mc),
			)
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

			// Unpause + wait for reset timeout. ShouldFailover should
			// return false (half-open) allowing a probe.
			require.NoError(t, a.Unpause(ctx))

			closeDeadline := time.Now().Add(15 * time.Second)
			closed := false
			for time.Now().Before(closeDeadline) {
				if !cb.ShouldFailover(htypes.ClusterA, nil) {
					closed = true
					break
				}
				// Drive a read to give the policy a probe attempt.
				qCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				var got string
				_ = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k").
					ScanContext(qCtx, &got)
				cancel()
				time.Sleep(200 * time.Millisecond)
			}
			assert.True(t, closed,
				"[%s] CircuitBreaker did not return to closed/half-open after Unpause + reset timeout",
				d.name)
		})
	}
}
