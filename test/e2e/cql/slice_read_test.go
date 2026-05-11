//go:build e2e

package cql_test

// Phase 9 e2e tests for SliceMap / SliceScan under real cluster faults.
//
// Integration tests (test/integration/) cover functional correctness with
// real clusters but cannot inject network-level failures.  These e2e tests
// fill the gap:
//
//   TestS_SliceMap_FallbackRead_RowsMissingOnPreferred —
//     lag-recovery: rows only on the non-preferred cluster, FallbackRead
//     surfaces them transparently (both adapters).
//
//   TestS_SliceScan_FallbackRead_RowsMissingOnPreferred —
//     same scenario for SliceScan + typed accumulation (both adapters).
//
//   TestS_SliceMap_FallbackRead_AltUnreachable_ReturnsNilNil —
//     best-effort contract: when the alternative cluster is paused,
//     SliceMap returns (nil, nil) — availability is preserved over
//     error propagation.
//
//   TestS_SliceScan_PrimaryUnreachable_AltNotRetried —
//     no-retry contract: SliceScan never does standard failover, so a
//     paused primary returns an error to the caller and the alt cluster
//     is NOT contacted — even when FallbackRead is enabled and the alt
//     has rows.

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

// =============================================================================
// FallbackRead lag-recovery: preferred cluster has 0 rows, alt has rows
// =============================================================================

// TestS_SliceMap_FallbackRead_RowsMissingOnPreferred verifies the core
// lag-recovery use case for multi-row reads: a partition exists only on
// cluster B (A has not yet replicated it), and FallbackRead surfaces B's
// rows transparently via SliceMapContext.
func TestS_SliceMap_FallbackRead_RowsMissingOnPreferred(t *testing.T) {
	a, b := sharedClusters(t)
	table := createKVTableOnBoth(t, "sr_slicemap_lag")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			// Seed 3 rows on B only (A's partition is empty — simulates lag).
			for i := range 3 {
				key := fmt.Sprintf("%s_key_%d", d.name, i)
				require.NoError(t,
					b.Session.Query(fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table),
						key, fmt.Sprintf("v_%d", i)).Exec())
			}
			t.Cleanup(func() {
				for i := range 3 {
					key := fmt.Sprintf("%s_key_%d", d.name, i)
					_ = a.Session.Query(fmt.Sprintf("DELETE FROM %s WHERE key = ?", table), key).Exec()
					_ = b.Session.Query(fmt.Sprintf("DELETE FROM %s WHERE key = ?", table), key).Exec()
				}
			})

			mc := testutil.NewTestMetricsCollector()
			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithReadStrategy(policy.NewStickyRead(
					policy.WithPreferredCluster(htypes.ClusterA),
				)),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
				helix.WithMetrics(mc),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			stmt := fmt.Sprintf("SELECT key, value FROM %s", table)

			// Without FallbackRead: A is empty → SliceMap returns (nil, nil).
			rows, err := client.Query(stmt).SliceMapContext(ctx)
			require.NoError(t, err)
			assert.Nil(t, rows,
				"[%s] no FallbackRead: A empty must return nil", d.name)

			// With FallbackRead: A is empty → silently tries B → returns B's 3 rows.
			rows, err = client.Query(stmt).FallbackRead().SliceMapContext(ctx)
			require.NoError(t, err,
				"[%s] FallbackRead must not return an error when B has rows", d.name)
			assert.Len(t, rows, 3,
				"[%s] FallbackRead must surface all 3 rows from B", d.name)

			// Divergence fires on the stale primary (A).
			assert.Equal(t, int64(1), mc.GetReadDivergence(htypes.ClusterA),
				"[%s] divergence must fire on A (stale primary)", d.name)

			// Verify content identity — Len(3) alone does not catch wrong-table or
			// isolation issues.
			wantKeys := make(map[string]bool, 3)
			for i := range 3 {
				wantKeys[fmt.Sprintf("%s_key_%d", d.name, i)] = true
			}
			gotKeys := make(map[string]bool, len(rows))
			for _, row := range rows {
				if k, ok := row["key"].(string); ok {
					gotKeys[k] = true
				}
			}
			assert.Equal(t, wantKeys, gotKeys,
				"[%s] FallbackRead must return the exact 3 seeded keys from B", d.name)
		})
	}
}

// TestS_SliceScan_FallbackRead_RowsMissingOnPreferred verifies the same
// lag-recovery scenario for SliceScanContext: the scan callback is invoked
// for B's rows and the row count matches.
func TestS_SliceScan_FallbackRead_RowsMissingOnPreferred(t *testing.T) {
	a, b := sharedClusters(t)
	table := createKVTableOnBoth(t, "sr_slicescan_lag")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			for i := range 3 {
				key := fmt.Sprintf("%s_key_%d", d.name, i)
				require.NoError(t,
					b.Session.Query(fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table),
						key, fmt.Sprintf("v_%d", i)).Exec())
			}
			t.Cleanup(func() {
				for i := range 3 {
					key := fmt.Sprintf("%s_key_%d", d.name, i)
					_ = a.Session.Query(fmt.Sprintf("DELETE FROM %s WHERE key = ?", table), key).Exec()
					_ = b.Session.Query(fmt.Sprintf("DELETE FROM %s WHERE key = ?", table), key).Exec()
				}
			})

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithReadStrategy(policy.NewStickyRead(
					policy.WithPreferredCluster(htypes.ClusterA),
				)),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			stmt := fmt.Sprintf("SELECT value FROM %s", table)

			var got []string
			rowCount, err := client.Query(stmt).
				FallbackRead().
				SliceScanContext(ctx, func(r helix.RowScanner) error {
					var v string
					if scanErr := r.Scan(&v); scanErr != nil {
						return scanErr
					}
					got = append(got, v)
					return nil
				})

			require.NoError(t, err,
				"[%s] FallbackRead + SliceScan must not return an error", d.name)
			assert.Equal(t, 3, rowCount,
				"[%s] rowCount must equal the number of B rows scanned", d.name)
			assert.Len(t, got, 3,
				"[%s] scan callback must have been invoked 3 times", d.name)

			// Verify content identity — Len(3) alone does not catch wrong-table or
			// isolation issues.
			wantVals := map[string]bool{"v_0": true, "v_1": true, "v_2": true}
			gotVals := make(map[string]bool, len(got))
			for _, v := range got {
				gotVals[v] = true
			}
			assert.Equal(t, wantVals, gotVals,
				"[%s] scan callback must have received the exact 3 seeded values from B", d.name)
		})
	}
}

// =============================================================================
// FallbackRead: alternative cluster unreachable → (nil, nil)
// =============================================================================

// TestS_SliceMap_FallbackRead_AltUnreachable_ReturnsNilNil verifies the
// best-effort contract: when the alternative cluster is paused and the primary
// has 0 rows, SliceMap returns (nil, nil) rather than propagating the
// alternative's network error.
//
// This preserves availability — an unreachable alt during FallbackRead must not
// degrade the caller's read to an error when the row genuinely doesn't exist on
// the primary.
func TestS_SliceMap_FallbackRead_AltUnreachable_ReturnsNilNil(t *testing.T) {
	a, b := sharedClusters(t)
	// Register table cleanup first so withRestoredCluster fires before table
	// truncate in LIFO order, ensuring b is healthy before cleanup queries run.
	table := createKVTableOnBoth(t, "sr_slicemap_alt_down")
	withRestoredCluster(t, b)

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			mc := testutil.NewTestMetricsCollector()
			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithReadStrategy(policy.NewStickyRead(
					policy.WithPreferredCluster(htypes.ClusterA),
				)),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
				helix.WithMetrics(mc),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			require.NoError(t, b.Pause(ctx))
			t.Cleanup(func() {
				unCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				_ = b.Unpause(unCtx)
			})

			qCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()

			// Primary (A) has no rows; alt (B) is paused.
			// FallbackRead should return (nil, nil) — not a network error.
			rows, err := client.Query(
				fmt.Sprintf("SELECT key, value FROM %s", table),
			).FallbackRead().SliceMapContext(qCtx)

			require.NoError(t, err,
				"[%s] unreachable alt must not surface a network error; got: %v", d.name, err)
			assert.Nil(t, rows,
				"[%s] unreachable alt must return nil slice, not an error", d.name)

			// Proves the route taken: A attempted (empty), B attempted (fault suppressed).
			assert.Equal(t, int64(1), mc.ReadTotal[htypes.ClusterA],
				"[%s] primary must be attempted once", d.name)
			assert.Equal(t, int64(1), mc.ReadTotal[htypes.ClusterB],
				"[%s] alt must be attempted (fault suppressed, not skipped)", d.name)
		})
	}
}

// =============================================================================
// SliceScan: primary unreachable → error propagates, alt NOT retried
// =============================================================================

// TestS_SliceScan_PrimaryUnreachable_AltNotRetried verifies that SliceScan
// never retries on the alternative cluster when the primary has a real error
// (connection failure), even when FallbackRead is enabled and the alt has rows.
//
// This locks the no-standard-failover contract from clarification #1: SliceScan
// uses executeReadNoFailover so callers can safely retry from a known cursor
// without risk of double-scanning the alt cluster.
//
// Failure mode if regressed: the alt cluster is contacted, rowCount > 0 is
// returned from B's rows, and the caller silently processes B's data instead
// of receiving an error signal.
func TestS_SliceScan_PrimaryUnreachable_AltNotRetried(t *testing.T) {
	a, b := sharedClusters(t)
	// Register table cleanup first so withRestoredCluster fires before table
	// truncate in LIFO order, ensuring a is healthy before cleanup queries run.
	table := createKVTableOnBoth(t, "sr_slicescan_primary_down")
	withRestoredCluster(t, a)

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			// Seed B with 3 rows. If the alt were contacted, rowCount would be 3.
			for i := range 3 {
				key := fmt.Sprintf("%s_key_%d", d.name, i)
				require.NoError(t,
					b.Session.Query(fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table),
						key, fmt.Sprintf("v_%d", i)).Exec())
			}
			t.Cleanup(func() {
				for i := range 3 {
					key := fmt.Sprintf("%s_key_%d", d.name, i)
					_ = b.Session.Query(fmt.Sprintf("DELETE FROM %s WHERE key = ?", table), key).Exec()
				}
			})

			mc := testutil.NewTestMetricsCollector()
			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithReadStrategy(policy.NewStickyRead(
					policy.WithPreferredCluster(htypes.ClusterA),
				)),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
				helix.WithMetrics(mc),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			require.NoError(t, a.Pause(ctx))
			t.Cleanup(func() {
				unCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				_ = a.Unpause(unCtx)
			})

			qCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()

			rowCount, err := client.Query(
				fmt.Sprintf("SELECT value FROM %s", table),
			).FallbackRead().SliceScanContext(qCtx, func(r helix.RowScanner) error {
				var v string
				return r.Scan(&v)
			})

			// Primary (A) is down → error must propagate.
			require.Error(t, err,
				"[%s] paused primary must cause SliceScan to return an error", d.name)
			assert.Equal(t, 0, rowCount,
				"[%s] no rows must be reported when primary failed", d.name)

			// Alt (B) must NOT have been contacted — SliceScan never retries.
			assert.Equal(t, int64(0), mc.ReadTotal[htypes.ClusterB],
				"[%s] alt must not be contacted when SliceScan primary errors", d.name)
			// Primary must have been attempted once — guards against routing
			// failures that never reach the primary and still satisfy the above.
			assert.Equal(t, int64(1), mc.ReadTotal[htypes.ClusterA],
				"[%s] primary must be attempted once before the error is propagated", d.name)
		})
	}
}

// =============================================================================
// SliceMap: primary unreachable → standard failover to alt
// =============================================================================

// TestS_SliceMap_PrimaryUnreachable_FailsOverToAlt verifies the standard
// failover contract for SliceMap: when the primary is paused, executeRead
// triggers failover to the alt cluster and returns the alt's rows.
//
// This is the complementary case to TestS_SliceScan_PrimaryUnreachable_AltNotRetried:
// SliceMap uses executeRead (standard failover), while SliceScan uses
// executeReadNoFailover.  Both tests must exist to lock the intentional
// asymmetry between the two methods under a real network-level fault.
//
// Failure mode if regressed: the error from A propagates without contacting B
// (SliceMap silently adopts SliceScan's no-retry contract), and rows from B
// are never returned.
func TestS_SliceMap_PrimaryUnreachable_FailsOverToAlt(t *testing.T) {
	a, b := sharedClusters(t)
	table := createKVTableOnBoth(t, "sr_slicemap_primary_failover")
	withRestoredCluster(t, a)

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			// Seed B with 3 rows; A has none — failover must surface B's rows.
			for i := range 3 {
				key := fmt.Sprintf("%s_key_%d", d.name, i)
				require.NoError(t,
					b.Session.Query(fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table),
						key, fmt.Sprintf("v_%d", i)).Exec())
			}
			t.Cleanup(func() {
				for i := range 3 {
					key := fmt.Sprintf("%s_key_%d", d.name, i)
					_ = b.Session.Query(fmt.Sprintf("DELETE FROM %s WHERE key = ?", table), key).Exec()
				}
			})

			mc := testutil.NewTestMetricsCollector()
			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithReadStrategy(policy.NewStickyRead(
					policy.WithPreferredCluster(htypes.ClusterA),
				)),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
				helix.WithMetrics(mc),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			require.NoError(t, a.Pause(ctx))
			t.Cleanup(func() {
				unCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				_ = a.Unpause(unCtx)
			})

			qCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()

			// SliceMap uses executeRead: paused primary must trigger failover to B.
			rows, err := client.Query(
				fmt.Sprintf("SELECT key, value FROM %s", table),
			).SliceMapContext(qCtx)

			require.NoError(t, err,
				"[%s] SliceMap must fail over to alt when primary is paused", d.name)
			assert.Len(t, rows, 3,
				"[%s] SliceMap must return all 3 rows from B after failover", d.name)

			// Primary attempted once (failed), alt attempted once (succeeded).
			assert.Equal(t, int64(1), mc.ReadTotal[htypes.ClusterA],
				"[%s] primary must be attempted once before failover", d.name)
			assert.Equal(t, int64(1), mc.ReadTotal[htypes.ClusterB],
				"[%s] alt must be contacted exactly once after failover", d.name)
		})
	}
}
