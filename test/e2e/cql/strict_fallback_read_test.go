//go:build e2e

package cql_test

import (
	"context"
	"errors"
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

// TestStrictPartialWriteThenPlainReadFailsFastOnMissedCluster verifies the
// fail-fast caller path after a strict partial write: the row exists only on
// the acknowledged cluster, and a plain read pinned to the missed cluster
// returns ErrNotFound.
func TestStrictPartialWriteThenPlainReadFailsFastOnMissedCluster(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, b)

	table := createKVTableOnBoth(t, "strict_plain_read_miss")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			t.Cleanup(func() { _ = b.Unpause(context.Background()) })

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithWriteStrategy(policy.NewSyncDualWrite()),
				helix.WithReadStrategy(policy.NewStickyRead(
					policy.WithPreferredCluster(htypes.ClusterB),
				)),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			require.NoError(t, b.Pause(ctx))

			key := fmt.Sprintf("strict_plain_read_miss_%s", d.name)
			insertStmt := fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table)
			selectStmt := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", table)

			qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = client.Query(insertStmt, key, "strict-value").Strict().ExecContext(qCtx)
			cancel()

			var partial *htypes.PartialWriteError
			require.True(t, errors.As(err, &partial),
				"[%s] expected *PartialWriteError, got %T: %v", d.name, err, err)
			assert.Equal(t, htypes.ClusterA, partial.Acknowledged,
				"[%s] A must be the acknowledged cluster", d.name)
			assert.Equal(t, htypes.ClusterB, partial.Unacknowledged,
				"[%s] B must be the unacknowledged cluster", d.name)

			require.NoError(t, b.Unpause(ctx))
			waitForReconnect(t, b, d.name)

			var gotA, gotB string
			require.NoError(t, a.Session.Query(selectStmt, key).Scan(&gotA),
				"[%s] row must exist on A after strict partial write", d.name)
			assert.Equal(t, "strict-value", gotA)
			assert.Error(t, b.Session.Query(selectStmt, key).Scan(&gotB),
				"[%s] row must not exist on B after strict partial write", d.name)

			var got string
			readErr := client.Query(selectStmt, key).ScanContext(ctx, &got)
			require.ErrorIs(t, readErr, htypes.ErrNotFound,
				"[%s] plain read pinned to B must fail fast with ErrNotFound", d.name)
			assert.Empty(t, got,
				"[%s] plain read must not recover value from A without FallbackRead", d.name)
		})
	}
}

// TestStrictPartialWriteThenFallbackReadGetsSurvivingRecord verifies the
// best-effort caller path after a strict partial write: FallbackRead recovers
// the row from the surviving cluster when the selected cluster returns
// not-found.
func TestStrictPartialWriteThenFallbackReadGetsSurvivingRecord(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, b)

	table := createKVTableOnBoth(t, "strict_fallback_read_recover")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			t.Cleanup(func() { _ = b.Unpause(context.Background()) })

			mc := testutil.NewTestMetricsCollector()
			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithWriteStrategy(policy.NewSyncDualWrite()),
				helix.WithReadStrategy(policy.NewStickyRead(
					policy.WithPreferredCluster(htypes.ClusterB),
				)),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
				helix.WithMetrics(mc),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			require.NoError(t, b.Pause(ctx))

			key := fmt.Sprintf("strict_fallback_read_recover_%s", d.name)
			insertStmt := fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table)
			selectStmt := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", table)

			qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = client.Query(insertStmt, key, "strict-value").Strict().ExecContext(qCtx)
			cancel()

			var partial *htypes.PartialWriteError
			require.True(t, errors.As(err, &partial),
				"[%s] expected *PartialWriteError, got %T: %v", d.name, err, err)
			assert.Equal(t, htypes.ClusterA, partial.Acknowledged,
				"[%s] A must be the acknowledged cluster", d.name)
			assert.Equal(t, htypes.ClusterB, partial.Unacknowledged,
				"[%s] B must be the unacknowledged cluster", d.name)

			require.NoError(t, b.Unpause(ctx))
			waitForReconnect(t, b, d.name)

			var gotA, gotB string
			require.NoError(t, a.Session.Query(selectStmt, key).Scan(&gotA),
				"[%s] row must exist on A after strict partial write", d.name)
			assert.Equal(t, "strict-value", gotA)
			assert.Error(t, b.Session.Query(selectStmt, key).Scan(&gotB),
				"[%s] row must not exist on B after strict partial write", d.name)

			var got string
			err = client.Query(selectStmt, key).FallbackRead().ScanContext(ctx, &got)
			require.NoError(t, err,
				"[%s] FallbackRead must recover row from A when B is stale", d.name)
			assert.Equal(t, "strict-value", got)

			// ReadTotal is safe to read after client operations complete.
			assert.Equal(t, int64(1), mc.ReadTotal[htypes.ClusterB],
				"[%s] selected stale cluster B should be read once", d.name)
			assert.Equal(t, int64(1), mc.ReadTotal[htypes.ClusterA],
				"[%s] fallback cluster A should be read once", d.name)
			assert.Equal(t, int64(1), mc.GetReadDivergence(htypes.ClusterB),
				"[%s] divergence should be recorded on stale selected cluster B", d.name)
			assert.Equal(t, int64(0), mc.GetReadDivergence(htypes.ClusterA),
				"[%s] divergence must not be recorded on cluster A", d.name)
		})
	}
}
