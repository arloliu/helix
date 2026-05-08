//go:build e2e

package cql_test

// FallbackRead has integration coverage but no e2e under real cluster
// outage. Its contract is subtle: when the selected cluster returns
// not-found, helix silently tries the alternative; if the alternative
// is unreachable, helix returns ErrNotFound from the healthy primary
// (it does NOT propagate the alternative's network error). This e2e
// test exercises the exact behavior a migration scenario would hit:
// row missing on one cluster (lag), the other cluster reachable, and
// we want the lookup to succeed.

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

// TestS_FallbackRead_RowMissingOnPreferredButPresentOnOther exercises
// the lag-recovery use case under a real cluster pair: a row exists only
// on cluster B, the StickyRead-preferred cluster A returns not-found, and
// FallbackRead must transparently surface B's value.
//
// This is the most common production trigger for FallbackRead: replay
// lag means the primary cluster doesn't have the row yet; without
// FallbackRead the caller would see ErrNotFound and conclude "row
// doesn't exist."
func TestS_FallbackRead_RowMissingOnPreferredButPresentOnOther(t *testing.T) {
	a, b := sharedClusters(t)

	table := createKVTableOnBoth(t, "fallback_lag")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			// Seed the row only on cluster B (simulates B-only replay lag
			// hole on A).
			require.NoError(t,
				b.Session.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
					"only_on_b", "v_b").Exec())
			t.Cleanup(func() {
				_ = a.Session.Query("DELETE FROM "+table+" WHERE key = ?", "only_on_b").Exec()
				_ = b.Session.Query("DELETE FROM "+table+" WHERE key = ?", "only_on_b").Exec()
			})

			rs := policy.NewStickyRead(
				policy.WithPreferredCluster(htypes.ClusterA),
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

			// Without FallbackRead: A returns not-found, helix surfaces
			// it as ErrNotFound. (Sanity check that the row really is
			// missing on A.)
			var got string
			err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "only_on_b").
				ScanContext(ctx, &got)
			require.ErrorIs(t, err, htypes.ErrNotFound,
				"[%s] row should be missing on preferred cluster A", d.name)

			// With FallbackRead: A returns not-found, helix transparently
			// queries B and returns B's value.
			err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "only_on_b").
				FallbackRead().
				ScanContext(ctx, &got)
			require.NoError(t, err,
				"[%s] FallbackRead should recover the row from B", d.name)
			assert.Equal(t, "v_b", got)
		})
	}
}

// TestS_FallbackRead_AlternativeUnreachable_ReturnsNotFound verifies the
// "best-effort" contract: when FallbackRead finds the alternative cluster
// unreachable (paused), it returns ErrNotFound from the healthy primary
// rather than propagating the alternative's network error. This preserves
// availability — without this behavior, FallbackRead would cause reads to
// fail during single-cluster outages for rows that genuinely don't exist.
func TestS_FallbackRead_AlternativeUnreachable_ReturnsNotFound(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, b)

	table := createKVTableOnBoth(t, "fallback_paused")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			rs := policy.NewStickyRead(
				policy.WithPreferredCluster(htypes.ClusterA),
			)
			mc := testutil.NewTestMetricsCollector()

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithReadStrategy(rs),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
				helix.WithMetrics(mc),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			// Pause B (the alternative). The row genuinely doesn't exist
			// on A. FallbackRead should return ErrNotFound, NOT a network
			// error from B.
			ctx := context.Background()
			require.NoError(t, b.Pause(ctx))
			defer func() { _ = b.Unpause(context.Background()) }()

			qCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()

			var got string
			err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "absent").
				FallbackRead().
				ScanContext(qCtx, &got)
			require.ErrorIs(t, err, htypes.ErrNotFound,
				"[%s] FallbackRead must return ErrNotFound (not network err) when alt is unreachable",
				d.name)
		})
	}
}
