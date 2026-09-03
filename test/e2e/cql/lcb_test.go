//go:build e2e

package cql_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/test/testutil"
	htypes "github.com/arloliu/helix/types"
)

// TestS3_PauseA_LatencyCircuitBreaker probes hypotheses H1 (slow vs fast
// failure surface) and H2 (context cancellation). It is the only scenario
// in the suite that the chaos suite cannot simulate at all — chaos's
// LatencyFunc is a time.Sleep BEFORE the real op, not a hung TCP connection.
//
// The test:
//  1. Configures LatencyCircuitBreaker with absolute_max=500ms, threshold=3.
//  2. Pauses cluster A — every read against A hangs until cluster.Timeout (2s).
//  3. Drives a stream of reads. Each one against a paused A should record
//     a latency far above absolute_max, accumulating breaker strikes.
//  4. After enough strikes, ShouldFailover(ClusterA) must return true.
//  5. Unpauses A. After the reset timeout elapses, the breaker must close.
//
// Driver-parameterized so we surface any timing or error-classification
// divergence between v1 and v2.
func TestS3_PauseA_LatencyCircuitBreaker(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, a)

	table := createKVTableOnBoth(t, "s3_lcb")
	seedKV(t, a, b, table, "k", "v")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			ensureReachable(t, a, d)
			lcb := policy.NewLatencyCircuitBreaker(
				policy.WithLatencyAbsoluteMax(500*time.Millisecond),
				policy.WithLatencyThreshold(3),
				policy.WithLatencyResetTimeout(5*time.Second),
			)
			mc := testutil.NewTestMetricsCollector()

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithReadStrategy(policy.NewStickyRead(
					policy.WithPreferredCluster(htypes.ClusterA),
				)),
				helix.WithFailoverPolicy(lcb),
				helix.WithMetrics(mc),
				helix.WithLogger(testutil.NewTestLogger(t)),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			require.NoError(t, a.Pause(ctx))
			defer func() { _ = a.Unpause(context.Background()) }()

			// Drive enough reads that the breaker accumulates strikes.
			// cluster.Timeout=2s + threshold=3 means the breaker should
			// open within ~6-10s on either driver.
			deadline := time.Now().Add(20 * time.Second)
			var lastReadErr error
			ops := 0
			for !lcb.ShouldFailover(htypes.ClusterA, nil) && time.Now().Before(deadline) {
				ops++
				qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				var got string
				lastReadErr = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k").
					ScanContext(qCtx, &got)
				cancel()
				_ = lastReadErr
			}

			t.Logf("[%s] breaker opened after %d ops (last err=%v)", d.name, ops, lastReadErr)
			assert.True(t, lcb.ShouldFailover(htypes.ClusterA, nil),
				"[%s] LatencyCircuitBreaker did not open within deadline (ops=%d)", d.name, ops)
			assert.Less(t, ops, 50,
				"[%s] breaker took too many ops to open — strike accumulation may be misclassified",
				d.name)

			// Unpause A; reset timeout is 5s. Once it has elapsed the
			// client's recovery probe reserves the breaker; a healthy probe
			// closes it. Reads driven meanwhile still go to A (route veto is
			// off), so a fast successful read may close it first.
			require.NoError(t, a.Unpause(ctx))

			require.Eventually(t, func() bool {
				if !lcb.ShouldFailover(htypes.ClusterA, nil) {
					return true
				}
				qCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				var got string
				_ = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k").
					ScanContext(qCtx, &got)
				cancel()

				return !lcb.ShouldFailover(htypes.ClusterA, nil)
			}, 30*time.Second, 200*time.Millisecond,
				"[%s] LatencyCircuitBreaker did not close after Unpause + reset timeout",
				d.name)
		})
	}
}

// TestS3_PauseA_HelixDegradesUnderHang sanity-checks that AdaptiveDualWrite
// detects a paused cluster as degraded within a bounded number of writes.
// This is a separate hypothesis (H1: does Helix's degradation detection
// fire on real timeouts, not just synthetic errors?) that doesn't need
// LatencyCircuitBreaker to surface.
func TestS3_PauseA_AdaptiveWriteDegrades(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, a)

	table := createKVTableOnBoth(t, "s3_adw")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			ensureReachable(t, a, d)
			adw := policy.NewAdaptiveDualWrite(
				policy.WithAdaptiveStrikeThreshold(3),
				policy.WithAdaptiveDeltaThreshold(100*time.Millisecond),
			)
			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithWriteStrategy(adw),
				helix.WithReadStrategy(policy.NewStickyRead()),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			require.NoError(t, a.Pause(ctx))
			defer func() { _ = a.Unpause(context.Background()) }()

			// Drive writes until ADW marks A degraded, or 30s elapses.
			deadline := time.Now().Add(30 * time.Second)
			ops := 0
			for !adw.IsDegraded(htypes.ClusterA) && time.Now().Before(deadline) {
				ops++
				_ = client.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
					fmt.Sprintf("k%d", ops), "v").Exec()
			}
			t.Logf("[%s] AdaptiveDualWrite marked A degraded after %d ops", d.name, ops)

			assert.True(t, adw.IsDegraded(htypes.ClusterA),
				"[%s] AdaptiveDualWrite did not mark A degraded under hang (ops=%d)", d.name, ops)
		})
	}
}
