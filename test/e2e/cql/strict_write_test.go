//go:build e2e

package cql_test

// End-to-end tests for v1.4.0 strict writes.
// These exercise behavior that unit-mock tests cannot validate:
//
//   - Real *PartialWriteError surfaces through the gocql / cassandra-gocql-driver
//     paths when one cluster is paused.
//   - The "no replay" invariant holds under a real driver error, not just when a
//     strategy mock returns errA/errB directly.
//   - The default system.local recovery probe drives AdaptiveDualWrite back to
//     healthy after a real cluster restart — without operator action.
//   - Strict()+Mirror() preflight rejection fires before any network I/O.

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/mirror"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/topology"
	htypes "github.com/arloliu/helix/types"
)

// ─── In-file metric collector helpers ─────────────────────────────────────────

// strictSkippedCounter extends TestMetricsCollector with IncWriteSkipped so
// tests can assert degraded-skip accounting independently of write errors.
type strictSkippedCounter struct {
	*testutil.TestMetricsCollector
	skippedA, skippedB atomic.Int32
}

func newStrictSkippedCounter() *strictSkippedCounter {
	return &strictSkippedCounter{TestMetricsCollector: testutil.NewTestMetricsCollector()}
}

func (s *strictSkippedCounter) IncWriteSkipped(cluster htypes.ClusterID) {
	if cluster == htypes.ClusterA {
		s.skippedA.Add(1)
	} else {
		s.skippedB.Add(1)
	}
}

var (
	_ htypes.MetricsCollector = (*strictSkippedCounter)(nil)
	_ htypes.StrictMetrics    = (*strictSkippedCounter)(nil)
)

// recoveryProbeCounter extends TestMetricsCollector with probe success/failure
// counters so tests can assert that the background probe fires and credits
// recovery points.
type recoveryProbeCounter struct {
	*testutil.TestMetricsCollector
	successA, successB atomic.Int32
	failureA, failureB atomic.Int32
}

func newRecoveryProbeCounter() *recoveryProbeCounter {
	return &recoveryProbeCounter{TestMetricsCollector: testutil.NewTestMetricsCollector()}
}

func (p *recoveryProbeCounter) IncRecoveryProbeSuccess(cluster htypes.ClusterID) {
	if cluster == htypes.ClusterA {
		p.successA.Add(1)
	} else {
		p.successB.Add(1)
	}
}

func (p *recoveryProbeCounter) IncRecoveryProbeFailure(cluster htypes.ClusterID) {
	if cluster == htypes.ClusterA {
		p.failureA.Add(1)
	} else {
		p.failureB.Add(1)
	}
}

var (
	_ htypes.MetricsCollector     = (*recoveryProbeCounter)(nil)
	_ htypes.RecoveryProbeMetrics = (*recoveryProbeCounter)(nil)
)

// ─── Shared helpers ───────────────────────────────────────────────────────────

// waitForReconnect polls system.local until the cluster's raw session accepts
// queries. Used after Unpause/Start to confirm the driver reconnected before
// running verification reads.
func waitForReconnect(t *testing.T, cluster *testutil.CQLCluster, tag string) {
	t.Helper()
	require.Eventually(t, func() bool {
		var v string
		return cluster.Session.Query("SELECT release_version FROM system.local").Scan(&v) == nil
	}, 15*time.Second, 200*time.Millisecond, "%s: session must reconnect after restore", tag)
}

// degradeB issues non-strict writes with a short per-call deadline until
// AdaptiveDualWrite marks cluster B degraded. Returns when degradation is
// confirmed. The 500ms deadline avoids blocking on the 2s session timeout.
func degradeB(t *testing.T, adw *policy.AdaptiveDualWrite, client *helix.CQLClient, stmt, tag string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(15 * time.Second)
	for !adw.IsDegraded(htypes.ClusterB) {
		require.False(t, time.Now().After(deadline),
			"%s: cluster B did not degrade within 15s", tag)
		qCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		_ = client.Query(stmt, "warmup_"+tag, "v").ExecContext(qCtx)
		cancel()
	}
}

// ─── Tests ────────────────────────────────────────────────────────────────────

// TestStrict_PausedCluster_PartialWriteError verifies that a strict query write
// surfaces *PartialWriteError when cluster B is paused, and that the row lands
// only on cluster A (no replay enqueue occurs).
func TestStrict_PausedCluster_PartialWriteError(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, b)

	table := createKVTableOnBoth(t, "strict_partial_q")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			// Safety net: unpause B even if the subtest fails before the
			// explicit Unpause below, so the next driver variant starts clean.
			t.Cleanup(func() { _ = b.Unpause(context.Background()) })

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithWriteStrategy(policy.NewAdaptiveDualWrite()),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			require.NoError(t, b.Pause(ctx))

			key := fmt.Sprintf("strict_partial_q_%s", d.name)
			stmt := fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table)
			selectStmt := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", table)

			qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = client.Query(stmt, key, "v").Strict().ExecContext(qCtx)
			cancel()

			var partial *htypes.PartialWriteError
			require.True(t, errors.As(err, &partial),
				"[%s] expected *PartialWriteError, got %T: %v", d.name, err, err)
			assert.Equal(t, htypes.ClusterA, partial.Acknowledged,
				"[%s] A must be the acknowledged cluster", d.name)
			assert.Equal(t, htypes.ClusterB, partial.Unacknowledged,
				"[%s] B must be the unacknowledged cluster", d.name)
			assert.NotNil(t, partial.Cause,
				"[%s] PartialWriteError.Cause must be non-nil", d.name)

			// Row must exist on A.
			var gotA string
			require.NoError(t, a.Session.Query(selectStmt, key).Scan(&gotA),
				"[%s] row must be present on A", d.name)
			assert.Equal(t, "v", gotA)

			// Unpause B, wait for reconnection, then confirm the row is absent —
			// strict writes must not enqueue to the replayer.
			require.NoError(t, b.Unpause(ctx))
			waitForReconnect(t, b, d.name)

			var gotB string
			assert.Error(t, b.Session.Query(selectStmt, key).Scan(&gotB),
				"[%s] row must not exist on B: strict write must not enqueue", d.name)
		})
	}
}

// TestStrict_PausedCluster_BatchPartialWriteError verifies the batch path:
// a strict batch returns *PartialWriteError when cluster B is paused, and
// neither batch key appears on B after recovery.
func TestStrict_PausedCluster_BatchPartialWriteError(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, b)

	table := createKVTableOnBoth(t, "strict_partial_b")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			t.Cleanup(func() { _ = b.Unpause(context.Background()) })

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithWriteStrategy(policy.NewAdaptiveDualWrite()),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			require.NoError(t, b.Pause(ctx))

			k1 := fmt.Sprintf("strict_batch_k1_%s", d.name)
			k2 := fmt.Sprintf("strict_batch_k2_%s", d.name)
			stmt := fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table)
			selectStmt := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", table)

			qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = client.Batch(helix.LoggedBatch).
				Query(stmt, k1, "v1").
				Query(stmt, k2, "v2").
				Strict().ExecContext(qCtx)
			cancel()

			var partial *htypes.PartialWriteError
			require.True(t, errors.As(err, &partial),
				"[%s] expected *PartialWriteError from batch, got %T: %v", d.name, err, err)
			assert.Equal(t, htypes.ClusterB, partial.Unacknowledged,
				"[%s] B must be the unacknowledged cluster", d.name)
			assert.NotNil(t, partial.Cause,
				"[%s] batch PartialWriteError.Cause must be non-nil", d.name)

			// Both keys must be present on A.
			var v string
			require.NoError(t, a.Session.Query(selectStmt, k1).Scan(&v),
				"[%s] k1 must be present on A", d.name)
			assert.Equal(t, "v1", v)
			require.NoError(t, a.Session.Query(selectStmt, k2).Scan(&v),
				"[%s] k2 must be present on A", d.name)
			assert.Equal(t, "v2", v)

			// After Unpause, neither key should appear on B.
			require.NoError(t, b.Unpause(ctx))
			waitForReconnect(t, b, d.name)

			assert.Error(t, b.Session.Query(selectStmt, k1).Scan(&v),
				"[%s] k1 must not exist on B: strict batch must not enqueue", d.name)
			assert.Error(t, b.Session.Query(selectStmt, k2).Scan(&v),
				"[%s] k2 must not exist on B: strict batch must not enqueue", d.name)
		})
	}
}

// TestStrict_NoReplayUnderRealOutage locks the replay conservation law under
// real cluster failure: strict write failures must not enqueue to the replayer,
// while non-strict failures do. The asymmetric strict/non-strict count is the
// load-bearing assertion.
func TestStrict_NoReplayUnderRealOutage(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, b)

	table := createKVTableOnBoth(t, "strict_noreplay")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			t.Cleanup(func() { _ = b.Unpause(context.Background()) })

			// No draining worker: keeps the queue inspectable throughout.
			memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(64))

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithWriteStrategy(policy.NewSyncDualWrite()),
				helix.WithReplayer(memReplayer),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			require.NoError(t, b.Pause(ctx))

			stmt := fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table)

			// 5 strict writes: each must return PartialWriteError; none may enqueue.
			// 500ms deadline avoids blocking on the full 2s session timeout per call.
			for i := 0; i < 5; i++ {
				key := fmt.Sprintf("strict_%s_%d", d.name, i)
				// 3s deadline: ample time for a healthy cluster A (<50ms); B is paused
				// and will fail at the 2s session timeout before the context expires.
				// Using 500ms would make the test flaky if A is slow under CI load.
				qCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				writeErr := client.Query(stmt, key, "v").Strict().ExecContext(qCtx)
				cancel()

				var partial *htypes.PartialWriteError
				require.True(t, errors.As(writeErr, &partial),
					"[%s] strict write %d must return PartialWriteError, got %T: %v",
					d.name, i, writeErr, writeErr)
			}

			// 3 non-strict writes: A acks → nil return; B failure enqueues.
			for i := 0; i < 3; i++ {
				key := fmt.Sprintf("nonstict_%s_%d", d.name, i)
				qCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
				writeErr := client.Query(stmt, key, "v").ExecContext(qCtx)
				cancel()
				require.NoError(t, writeErr,
					"[%s] non-strict write %d must succeed via cluster A", d.name, i)
			}

			// Strict failures must not enqueue; only the 3 non-strict writes land here.
			assert.Equal(t, 3, memReplayer.Len(),
				"[%s] replayer must hold exactly 3 entries (the non-strict writes, not 8)",
				d.name)
		})
	}
}

// TestStrict_SingleClusterIsNoop verifies the documented single-cluster fast
// path with real drivers: Strict() behaves like a normal write on sessionA.
func TestStrict_SingleClusterIsNoop(t *testing.T) {
	a, _ := sharedClusters(t)
	table := createKVTableOnBoth(t, "strict_single_cluster")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			client, err := helix.NewCQLClient(d.wrap(a), nil)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			stmt := fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table)
			selectStmt := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", table)

			queryKey := fmt.Sprintf("strict_single_query_%s", d.name)
			err = client.Query(stmt, queryKey, "query").Strict().ExecContext(ctx)
			require.NoError(t, err, "[%s] strict query must be a no-op in single-cluster mode", d.name)

			var got string
			require.NoError(t, a.Session.Query(selectStmt, queryKey).Scan(&got),
				"[%s] strict query row must land on cluster A", d.name)
			assert.Equal(t, "query", got)

			batchKey := fmt.Sprintf("strict_single_batch_%s", d.name)
			err = client.Batch(helix.LoggedBatch).
				Query(stmt, batchKey, "batch").
				Strict().ExecContext(ctx)
			require.NoError(t, err, "[%s] strict batch must be a no-op in single-cluster mode", d.name)

			require.NoError(t, a.Session.Query(selectStmt, batchKey).Scan(&got),
				"[%s] strict batch row must land on cluster A", d.name)
			assert.Equal(t, "batch", got)
		})
	}
}

// TestStrict_CASIsNoop verifies that Strict() does not alter CAS/LWT routing:
// both query and batch CAS remain single-cluster operations on the selected
// cluster and are not duplicated to the peer cluster.
func TestStrict_CASIsNoop(t *testing.T) {
	a, b := sharedClusters(t)
	table := createKVTableOnBoth(t, "strict_cas_noop")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithReadStrategy(policy.NewStickyRead(
					policy.WithPreferredCluster(helix.ClusterA),
				)),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			insertStmt := fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?) IF NOT EXISTS", table)
			selectStmt := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", table)

			var existingKey, existingValue string
			queryKey := fmt.Sprintf("strict_cas_query_%s", d.name)
			applied, err := client.Query(insertStmt, queryKey, "query").
				Strict().
				ScanCASContext(ctx, &existingKey, &existingValue)
			require.NoError(t, err, "[%s] strict query CAS must execute normally", d.name)
			require.True(t, applied, "[%s] strict query CAS should apply on first insert", d.name)

			var got string
			require.NoError(t, a.Session.Query(selectStmt, queryKey).Scan(&got),
				"[%s] strict query CAS row must land on cluster A", d.name)
			assert.Equal(t, "query", got)

			assert.Error(t, b.Session.Query(selectStmt, queryKey).Scan(&got),
				"[%s] strict query CAS must not duplicate to cluster B", d.name)

			batchKey := fmt.Sprintf("strict_cas_batch_%s", d.name)
			batch := client.Batch(helix.LoggedBatch)
			batch.Query(insertStmt, batchKey, "batch")

			applied, iter, err := batch.Strict().ExecCASContext(ctx, &existingKey, &existingValue)
			require.NoError(t, err, "[%s] strict batch CAS must execute normally", d.name)
			require.True(t, applied, "[%s] strict batch CAS should apply on first insert", d.name)
			require.NoError(t, iter.Close())

			require.NoError(t, a.Session.Query(selectStmt, batchKey).Scan(&got),
				"[%s] strict batch CAS row must land on cluster A", d.name)
			assert.Equal(t, "batch", got)

			assert.Error(t, b.Session.Query(selectStmt, batchKey).Scan(&got),
				"[%s] strict batch CAS must not duplicate to cluster B", d.name)
		})
	}
}

// TestStrict_DrainingCluster_PartialWriteError verifies the real topology-drain
// path: Strict() returns PartialWriteError{Cause: ErrClusterDraining}, writes
// to the healthy cluster, and does not enqueue to the draining cluster.
func TestStrict_DrainingCluster_PartialWriteError(t *testing.T) {
	a, b := sharedClusters(t)
	table := createKVTableOnBoth(t, "strict_draining_partial")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			watcher := topology.NewLocal()
			t.Cleanup(func() { _ = watcher.Close() })

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithTopologyWatcher(watcher),
				helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			require.NoError(t, watcher.SetDrain(ctx, htypes.ClusterA, true, "maintenance"))
			require.Eventually(t, func() bool {
				return client.IsDraining(helix.ClusterA)
			}, time.Second, 10*time.Millisecond, "[%s] drain state should propagate", d.name)

			key := fmt.Sprintf("strict_draining_%s", d.name)
			stmt := fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table)
			selectStmt := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", table)

			err = client.Query(stmt, key, "v").Strict().ExecContext(ctx)

			var partial *htypes.PartialWriteError
			require.True(t, errors.As(err, &partial),
				"[%s] expected *PartialWriteError, got %T: %v", d.name, err, err)
			assert.Equal(t, htypes.ClusterB, partial.Acknowledged,
				"[%s] B must acknowledge when A is draining", d.name)
			assert.Equal(t, htypes.ClusterA, partial.Unacknowledged,
				"[%s] A must be the drained cluster", d.name)
			assert.ErrorIs(t, partial.Cause, htypes.ErrClusterDraining,
				"[%s] strict drain error must surface ErrClusterDraining", d.name)

			var gotB string
			require.NoError(t, b.Session.Query(selectStmt, key).Scan(&gotB),
				"[%s] strict drain write must land on healthy cluster B", d.name)
			assert.Equal(t, "v", gotB)

			var gotA string
			assert.Error(t, a.Session.Query(selectStmt, key).Scan(&gotA),
				"[%s] strict drain write must not be sent to draining cluster A", d.name)
		})
	}
}

// TestStrict_BothDrainingReturnsDualClusterError verifies the real topology-
// drain double-failure path: Strict() returns DualClusterError with
// ErrClusterDraining on both sides and dispatches no writes.
func TestStrict_BothDrainingReturnsDualClusterError(t *testing.T) {
	a, b := sharedClusters(t)
	table := createKVTableOnBoth(t, "strict_both_draining")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			watcher := topology.NewLocal()
			t.Cleanup(func() { _ = watcher.Close() })

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithTopologyWatcher(watcher),
				helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			require.NoError(t, watcher.SetDrain(ctx, htypes.ClusterA, true, "maintenance"))
			require.NoError(t, watcher.SetDrain(ctx, htypes.ClusterB, true, "maintenance"))
			require.Eventually(t, func() bool {
				return client.IsDraining(helix.ClusterA) && client.IsDraining(helix.ClusterB)
			}, time.Second, 10*time.Millisecond, "[%s] drain state should propagate to both clusters", d.name)

			key := fmt.Sprintf("strict_both_draining_%s", d.name)
			stmt := fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table)
			selectStmt := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", table)

			err = client.Query(stmt, key, "v").Strict().ExecContext(ctx)

			var dual *htypes.DualClusterError
			require.True(t, errors.As(err, &dual),
				"[%s] expected *DualClusterError, got %T: %v", d.name, err, err)
			assert.ErrorIs(t, dual.ErrorA, htypes.ErrClusterDraining,
				"[%s] cluster A must surface ErrClusterDraining", d.name)
			assert.ErrorIs(t, dual.ErrorB, htypes.ErrClusterDraining,
				"[%s] cluster B must surface ErrClusterDraining", d.name)

			var got string
			assert.Error(t, a.Session.Query(selectStmt, key).Scan(&got),
				"[%s] no write should reach cluster A when both are draining", d.name)
			assert.Error(t, b.Session.Query(selectStmt, key).Scan(&got),
				"[%s] no write should reach cluster B when both are draining", d.name)
		})
	}
}

// TestStrict_Mirror_PreflightRejection_NoSideEffects verifies that combining
// Strict() and Mirror() is rejected with ErrStrictMirrorUnsupported before any
// network I/O occurs: no rows appear on any cluster and the mirror engine does
// not enqueue.
func TestStrict_Mirror_PreflightRejection_NoSideEffects(t *testing.T) {
	a, b := sharedClusters(t)

	table := createKVTableOnBoth(t, "strict_mirror_pf")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			// Mirror destination wraps cluster B — it must never be invoked.
			mirrorClient, err := helix.NewCQLClient(d.wrap(b), nil)
			require.NoError(t, err)
			t.Cleanup(mirrorClient.Close)

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithMirror(mirrorClient,
					mirror.WithWorkers(1),
					mirror.WithQueueSize(8),
				),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			keyQ := fmt.Sprintf("strict_mirror_q_%s", d.name)
			keyB := fmt.Sprintf("strict_mirror_b_%s", d.name)
			stmt := fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table)
			selectStmt := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", table)

			beforeStats := client.Mirror().Stats()

			// Query: Strict()+Mirror() must be rejected before any write reaches either cluster.
			err = client.Query(stmt, keyQ, "v").Strict().Mirror().ExecContext(ctx)
			assert.ErrorIs(t, err, htypes.ErrStrictMirrorUnsupported,
				"[%s] query: expected ErrStrictMirrorUnsupported", d.name)

			var notFound string
			assert.Error(t, a.Session.Query(selectStmt, keyQ).Scan(&notFound),
				"[%s] keyQ must not exist on A after preflight rejection", d.name)
			assert.Error(t, b.Session.Query(selectStmt, keyQ).Scan(&notFound),
				"[%s] keyQ must not exist on B after preflight rejection", d.name)

			// Batch: Strict()+Mirror() must also be rejected.
			err = client.Batch(helix.LoggedBatch).
				Query(stmt, keyB, "v").
				Strict().Mirror().ExecContext(ctx)
			assert.ErrorIs(t, err, htypes.ErrStrictMirrorUnsupported,
				"[%s] batch: expected ErrStrictMirrorUnsupported", d.name)

			// Preflight rejection is synchronous — no async work is in flight;
			// confirm the engine did not enqueue.
			afterStats := client.Mirror().Stats()
			assert.Equal(t, beforeStats.Enqueued, afterStats.Enqueued,
				"[%s] mirror engine must not enqueue on Strict()+Mirror() rejection", d.name)
		})
	}
}

// TestStrict_DegradedSkipMetric_AdaptiveDualWrite verifies that when
// AdaptiveDualWrite has degraded cluster B, a strict write returns
// *PartialWriteError{Cause: ErrClusterDegraded} and records IncWriteSkipped
// for B only — not IncWriteError.
func TestStrict_DegradedSkipMetric_AdaptiveDualWrite(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, b)

	table := createKVTableOnBoth(t, "strict_degraded_skip")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			t.Cleanup(func() { _ = b.Unpause(context.Background()) })

			adw := policy.NewAdaptiveDualWrite(
				policy.WithAdaptiveStrikeThreshold(2),
				policy.WithAdaptiveDeltaThreshold(50*time.Millisecond),
			)
			mc := newStrictSkippedCounter()

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithWriteStrategy(adw),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
				helix.WithMetrics(mc),
				helix.WithAutoMemoryWorker(0),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			require.NoError(t, b.Pause(ctx))

			stmt := fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table)

			// Warm up: non-strict writes until AdaptiveDualWrite degrades B.
			degradeB(t, adw, client, stmt, d.name)

			// Snapshot counters after warm-up; delta-check after the strict write.
			skippedBBefore := mc.skippedB.Load()
			writeErrBBefore := mc.GetWriteErrors(htypes.ClusterB)

			// Issue one strict write — B is degraded, so it is skipped.
			qCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err = client.Query(stmt, fmt.Sprintf("strict_degraded_%s", d.name), "v").Strict().ExecContext(qCtx)
			cancel()

			var partial *htypes.PartialWriteError
			require.True(t, errors.As(err, &partial),
				"[%s] expected *PartialWriteError, got %T: %v", d.name, err, err)
			assert.ErrorIs(t, err, htypes.ErrClusterDegraded,
				"[%s] PartialWriteError.Cause must be ErrClusterDegraded", d.name)
			assert.Equal(t, htypes.ClusterB, partial.Unacknowledged,
				"[%s] B must be the unacknowledged cluster", d.name)

			// IncWriteSkipped must have incremented for B exactly once.
			assert.Equal(t, int32(1), mc.skippedB.Load()-skippedBBefore,
				"[%s] IncWriteSkipped(B) must fire exactly once for the strict skip", d.name)
			assert.Equal(t, int32(0), mc.skippedA.Load(),
				"[%s] IncWriteSkipped(A) must not fire", d.name)

			// IncWriteError must not have fired for B — a degraded skip is not an error.
			assert.Equal(t, writeErrBBefore, mc.GetWriteErrors(htypes.ClusterB),
				"[%s] IncWriteError(B) must not increment for a degraded skip", d.name)
		})
	}
}

// TestStrict_RecoveryProbe_DefaultProbeRestoresCluster is the headline
// auto-healing test for v1.4.0: after cluster B is degraded, the default
// system.local probe fires autonomously and drives AdaptiveDualWrite back to
// healthy — no operator action required. A strict write issued after recovery
// must succeed and land on both clusters.
//
// Scylla-only: testcontainers' Cassandra module cannot survive Pause+Unpause
// reliably under the probe's reconnection cadence (see SPIKE_FINDINGS.md §1).
func TestStrict_RecoveryProbe_DefaultProbeRestoresCluster(t *testing.T) {
	a, b := sharedClusters(t)
	if b.Type != testutil.CQLClusterTypeScyllaDB {
		t.Skipf("recovery probe test requires Scylla on cluster B (Cassandra Pause/Unpause is unreliable under the probe reconnection cadence; see SPIKE_FINDINGS.md §1); got %s", b.Type)
	}
	withRestoredCluster(t, b)

	table := createKVTableOnBoth(t, "strict_probe_default")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			t.Cleanup(func() { _ = b.Unpause(context.Background()) })

			adw := policy.NewAdaptiveDualWrite(
				policy.WithAdaptiveStrikeThreshold(2),
				policy.WithAdaptiveDeltaThreshold(50*time.Millisecond),
				policy.WithAdaptiveRecoveryThreshold(3),
			)
			mc := newRecoveryProbeCounter()

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithWriteStrategy(adw),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
				helix.WithMetrics(mc),
				// Default probe (system.local) with a short interval so the
				// test completes inside ~15s after B recovers.
				helix.WithRecoveryProbe(helix.RecoveryProbe{
					Interval: 200 * time.Millisecond,
					Timeout:  100 * time.Millisecond,
				}),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			require.NoError(t, b.Pause(ctx))

			stmt := fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table)

			// Degrade B with non-strict writes.
			degradeB(t, adw, client, stmt, d.name)

			// Confirm the strict path reflects the degraded state before recovery.
			qCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err = client.Query(stmt, fmt.Sprintf("before_recovery_%s", d.name), "v").Strict().ExecContext(qCtx)
			cancel()
			var partial *htypes.PartialWriteError
			require.True(t, errors.As(err, &partial),
				"[%s] expected *PartialWriteError before recovery, got %T: %v", d.name, err, err)
			assert.Equal(t, htypes.ClusterA, partial.Acknowledged,
				"[%s] A must acknowledge before recovery while B is degraded", d.name)
			assert.Equal(t, htypes.ClusterB, partial.Unacknowledged,
				"[%s] B must be the degraded cluster before recovery", d.name)
			assert.ErrorIs(t, partial.Cause, htypes.ErrClusterDegraded,
				"[%s] strict write before recovery must surface ErrClusterDegraded", d.name)

			// Confirm the probe fired and failed against the degraded (paused) B before
			// recovery — proves the probe loop ran, not just that recovery happened.
			assert.Greater(t, mc.failureB.Load(), int32(0),
				"[%s] recovery probe must have accumulated failures against paused B", d.name)

			// Unpause B — the probe fires against the live session and credits recovery
			// points without any operator action.
			require.NoError(t, b.Unpause(ctx))
			waitForReconnect(t, b, d.name)

			// 1. AdaptiveDualWrite must mark B healthy — the primary observable guarantee.
			require.Eventually(t, func() bool {
				return !adw.IsDegraded(htypes.ClusterB)
			}, 15*time.Second, 100*time.Millisecond,
				"[%s] B must no longer be degraded after probe recovery", d.name)

			// 2. Probe success counter must reach the configured recovery threshold (3).
			require.Eventually(t, func() bool {
				return mc.successB.Load() >= 3
			}, 15*time.Second, 100*time.Millisecond,
				"[%s] recovery probe must record ≥3 successes for B", d.name)

			// 3. A fresh strict write must succeed and land on both clusters.
			recoveryKey := fmt.Sprintf("after_recovery_%s", d.name)
			qCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
			err = client.Query(stmt, recoveryKey, "recovered").Strict().ExecContext(qCtx)
			cancel()
			require.NoError(t, err, "[%s] strict write must succeed after probe recovery", d.name)

			selectStmt := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", table)
			var gotA, gotB string
			require.NoError(t, a.Session.Query(selectStmt, recoveryKey).Scan(&gotA),
				"[%s] row must be present on A after recovery", d.name)
			assert.Equal(t, "recovered", gotA)
			require.NoError(t, b.Session.Query(selectStmt, recoveryKey).Scan(&gotB),
				"[%s] row must be present on B after recovery", d.name)
			assert.Equal(t, "recovered", gotB)

			// Cluster A must remain healthy throughout — probe activity on B must not
			// affect A's degraded state in AdaptiveDualWrite.
			assert.False(t, adw.IsDegraded(htypes.ClusterA),
				"[%s] cluster A must remain healthy throughout recovery", d.name)
		})
	}
}

// TestStrict_RecoveryProbe_StopAndStart_RestoresCluster exercises the harder
// failure path: a real container Stop+Start (not Pause). The probe must survive
// connection-RST on existing sockets, and AutoRefresh must rebuild the B session
// before the probe can accumulate recovery credits.
//
// Scylla-only: only Scylla survives Stop+Start (see SPIKE_FINDINGS.md §1).
func TestStrict_RecoveryProbe_StopAndStart_RestoresCluster(t *testing.T) {
	a, b := sharedClusters(t)
	if b.Type != testutil.CQLClusterTypeScyllaDB {
		t.Skipf("Stop+Start recovery test requires Scylla on cluster B (Cassandra does not survive Stop+Start reliably; see SPIKE_FINDINGS.md §1); got %s", b.Type)
	}
	withRestoredCluster(t, b)

	table := createKVTableOnBoth(t, "strict_probe_stopstart")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			adw := policy.NewAdaptiveDualWrite(
				policy.WithAdaptiveStrikeThreshold(2),
				policy.WithAdaptiveDeltaThreshold(50*time.Millisecond),
				policy.WithAdaptiveRecoveryThreshold(3),
			)
			mc := newRecoveryProbeCounter()
			var refresherCalls atomic.Int32

			refresher := func(rctx context.Context, cluster helix.ClusterID, _ error) (cql.Session, error) {
				refresherCalls.Add(1)
				if cluster != helix.ClusterB {
					return nil, fmt.Errorf("unexpected cluster refresh: %s", cluster)
				}
				if err := b.Reconnect(rctx); err != nil {
					return nil, fmt.Errorf("rebuild cluster B session: %w", err)
				}
				return d.wrap(b), nil
			}

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithWriteStrategy(adw),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
				helix.WithMetrics(mc),
				helix.WithRecoveryProbe(helix.RecoveryProbe{
					Interval: 200 * time.Millisecond,
					Timeout:  100 * time.Millisecond,
				}),
				helix.WithSessionRefresher(refresher),
				helix.WithAutoRefresh(
					helix.WithAutoRefreshFailureThreshold(2),
					helix.WithAutoRefreshSustainedFailureWindow(3*time.Second),
					helix.WithAutoRefreshMinRetryInterval(2*time.Second),
					helix.WithAutoRefreshCheckInterval(500*time.Millisecond),
					helix.WithAutoRefreshRefreshTimeout(10*time.Second),
				),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()

			// Stop B: existing TCP connections receive RST.
			require.NoError(t, b.Stop(ctx, 30*time.Second))

			stmt := fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table)

			// Degrade B with non-strict writes against the stopped cluster.
			degradeB(t, adw, client, stmt, d.name)

			// Confirm strict write reflects the degraded state.
			qCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err = client.Query(stmt, fmt.Sprintf("before_recovery_%s", d.name), "v").Strict().ExecContext(qCtx)
			cancel()
			var partial *htypes.PartialWriteError
			require.True(t, errors.As(err, &partial),
				"[%s] expected *PartialWriteError before recovery, got %T: %v", d.name, err, err)
			assert.Equal(t, htypes.ClusterA, partial.Acknowledged,
				"[%s] A must acknowledge before recovery while B is degraded", d.name)
			assert.Equal(t, htypes.ClusterB, partial.Unacknowledged,
				"[%s] B must be the degraded cluster before recovery", d.name)
			assert.ErrorIs(t, partial.Cause, htypes.ErrClusterDegraded,
				"[%s] strict write before recovery must surface ErrClusterDegraded", d.name)

			// Start B. AutoRefresh detects the dead session and invokes the refresher;
			// the recovery probe then fires against the rebuilt session.
			require.NoError(t, b.Start(ctx))
			// Rebuild the test-side session for direct verification reads.
			// AutoRefresh will also call b.Reconnect concurrently via the refresher;
			// both reconnects create independent gocql sessions pointing at the same
			// node — functionally safe, as each session manages its own connection pool.
			require.NoError(t, b.Reconnect(ctx), "[%s] test-side B session rebuild after Start", d.name)

			// 1. AutoRefresh must detect the dead session and invoke the refresher.
			require.Eventually(t, func() bool {
				return refresherCalls.Load() >= 1 && mc.GetSessionRefreshSuccesses(htypes.ClusterB) >= 1
			}, 60*time.Second, 100*time.Millisecond,
				"[%s] auto-refresh must rebuild cluster B after Stop+Start", d.name)

			// 2. The rebuilt session plus recovery probe must clear the degraded flag.
			require.Eventually(t, func() bool {
				return !adw.IsDegraded(htypes.ClusterB)
			}, 60*time.Second, 100*time.Millisecond,
				"[%s] B must be healthy after Stop+Start recovery", d.name)

			// 3. Strict write must succeed with the row on both clusters.
			recoveryKey := fmt.Sprintf("after_stopstart_%s", d.name)
			qCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
			err = client.Query(stmt, recoveryKey, "recovered").Strict().ExecContext(qCtx)
			cancel()
			require.NoError(t, err, "[%s] strict write must succeed after Stop+Start recovery", d.name)

			selectStmt := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", table)
			var gotA, gotB string
			require.NoError(t, a.Session.Query(selectStmt, recoveryKey).Scan(&gotA),
				"[%s] row must be on A after Stop+Start recovery", d.name)
			assert.Equal(t, "recovered", gotA)
			require.NoError(t, b.Session.Query(selectStmt, recoveryKey).Scan(&gotB),
				"[%s] row must be on B after Stop+Start recovery", d.name)
			assert.Equal(t, "recovered", gotB)

			// Cluster A must remain healthy throughout — Stop+Start of B must not
			// affect A's health state in AdaptiveDualWrite.
			assert.False(t, adw.IsDegraded(htypes.ClusterA),
				"[%s] cluster A must remain healthy after Stop+Start recovery", d.name)
		})
	}
}

// TestStrict_DegradedCluster_BothFailReturnsDualClusterError verifies the
// negative path: when both clusters are unavailable, a strict write returns
// *DualClusterError (not *PartialWriteError) and nothing is enqueued.
func TestStrict_DegradedCluster_BothFailReturnsDualClusterError(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, a)
	withRestoredCluster(t, b)

	table := createKVTableOnBoth(t, "strict_dual_fail")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			defer func() {
				_ = a.Unpause(context.Background())
				_ = b.Unpause(context.Background())
			}()

			memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(64))

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithWriteStrategy(policy.NewSyncDualWrite()),
				helix.WithReplayer(memReplayer),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			require.NoError(t, a.Pause(ctx))
			require.NoError(t, b.Pause(ctx))

			qCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err = client.Query(
				fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table),
				fmt.Sprintf("both_fail_%s", d.name), "v",
			).Strict().ExecContext(qCtx)
			cancel()

			require.Error(t, err,
				"[%s] strict write must fail when both clusters are down", d.name)

			var dual *htypes.DualClusterError
			require.True(t, errors.As(err, &dual),
				"[%s] expected *DualClusterError, got %T: %v", d.name, err, err)
			assert.NotNil(t, dual.ErrorA,
				"[%s] DualClusterError.ErrorA must be non-nil", d.name)
			assert.NotNil(t, dual.ErrorB,
				"[%s] DualClusterError.ErrorB must be non-nil", d.name)
			// Verify the error came from the write path, not an accidental drain routing.
			assert.NotErrorIs(t, dual.ErrorA, htypes.ErrClusterDraining,
				"[%s] DualClusterError.ErrorA must be a real write error, not a drain sentinel", d.name)
			assert.NotErrorIs(t, dual.ErrorB, htypes.ErrClusterDraining,
				"[%s] DualClusterError.ErrorB must be a real write error, not a drain sentinel", d.name)

			assert.Equal(t, 0, memReplayer.Len(),
				"[%s] replayer must not enqueue on a strict write with both clusters down", d.name)
		})
	}
}
