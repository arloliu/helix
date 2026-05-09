//go:build e2e

package cql_test

// End-to-end tests for the v1.4.0 async mirror feature using real
// container lifecycle (Pause/Unpause). These exercise behavior that the
// integration suite's mock-based failure injection cannot:
//
//   - Real network timeouts on the mirror destination during outage.
//   - Real gocql session reconnection after the destination recovers.
//   - The auto-built replay worker draining a real backlog into a
//     destination that comes back online.
//   - Real DualClusterError shape from the primary path when both A and B
//     are unreachable, suppressing mirror dispatch.
//
// The e2e harness has only two CQL clusters; tests below either use one
// as primary single-cluster + the other as mirror destination, or use
// both as a dual-primary with an inert mirror destination (which is
// never invoked because the primary write fails).

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

// TestMirror_DestinationPausedAndRecovered verifies the production-style
// recovery path: with WithMirrorReplayer attached, captures whose mirror
// exec fails (because the destination cluster is paused) land in the
// replayer; after the destination recovers the auto-built replay worker
// drains them and the rows appear on the mirror cluster.
//
// This is the migration-use-case test: "mirror cluster goes down for a
// few minutes, app keeps writing, mirror catches up."
func TestMirror_DestinationPausedAndRecovered(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, b)

	// Single table replicated to both clusters; primary writes go to A,
	// mirror writes go to B.
	table := createKVTableOnBoth(t, "mirror_dest_outage")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			mirrorClient, err := helix.NewCQLClient(d.wrap(b), nil)
			require.NoError(t, err)
			t.Cleanup(mirrorClient.Close)

			memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(64))

			primary, err := helix.NewCQLClient(d.wrap(a), nil,
				helix.WithMirror(mirrorClient,
					mirror.WithWorkers(1),
					mirror.WithQueueSize(8),
				),
				helix.WithMirrorReplayer(memReplayer,
					replay.WithPollInterval(50*time.Millisecond),
					replay.WithRetryDelay(50*time.Millisecond),
					replay.WithMaxAttempts(200),
				),
			)
			require.NoError(t, err)
			t.Cleanup(primary.Close)

			ctx := context.Background()
			stmt := fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table)

			// Pause B so mirror exec times out and lands in the replayer.
			require.NoError(t, b.Pause(ctx))

			require.NoError(t, primary.Session().Query(stmt, "k_during_outage", "v1").
				Mirror().ExecContext(ctx),
				"[%s] primary write should still succeed during mirror outage", d.name)

			// Resume B; the auto-built replay worker should eventually
			// retry against the now-live destination.
			require.NoError(t, b.Unpause(ctx))

			require.Eventually(t, func() bool {
				var got string
				err := b.Session.Query(
					fmt.Sprintf("SELECT value FROM %s WHERE key = ?", table),
					"k_during_outage").Scan(&got)

				return err == nil && got == "v1"
			}, 30*time.Second, 100*time.Millisecond,
				"[%s] mirror replay did not catch up after destination recovered (stats=%+v)",
				d.name, primary.Mirror().Stats())
		})
	}
}

// TestMirror_BothPrimaryClustersPaused_NoMirrorFire verifies that when the
// primary's dual-write returns DualClusterError (both A and B unreachable),
// the mirror engine does NOT dispatch — answering the post-Phase-5 audit
// question "did we test A & B both down, not just A or B?"
//
// We use a real cluster as the mirror destination but it is never invoked
// because the primary path fails before the mirror dispatch gates on
// err == nil.
func TestMirror_BothPrimaryClustersPaused_NoMirrorFire(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, a)
	withRestoredCluster(t, b)

	table := createKVTableOnBoth(t, "mirror_dual_paused")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			// Mirror destination wraps cluster A's session — but it is
			// never invoked because primary fails before mirror gates on
			// err == nil.
			mirrorClient, err := helix.NewCQLClient(d.wrap(a), nil)
			require.NoError(t, err)
			t.Cleanup(mirrorClient.Close)

			primary, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithWriteStrategy(policy.NewSyncDualWrite()),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
				helix.WithMirror(mirrorClient,
					mirror.WithWorkers(1),
					mirror.WithQueueSize(8),
				),
			)
			require.NoError(t, err)
			t.Cleanup(primary.Close)

			ctx := context.Background()
			require.NoError(t, a.Pause(ctx))
			require.NoError(t, b.Pause(ctx))
			defer func() {
				_ = a.Unpause(context.Background())
				_ = b.Unpause(context.Background())
			}()

			before := primary.Mirror().Stats()

			qCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			err = primary.Session().Query(
				fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table),
				"k_both_paused", "v1").Mirror().ExecContext(qCtx)

			require.Error(t, err, "[%s] expected error when both primary clusters paused", d.name)

			var dual *htypes.DualClusterError
			assert.True(t, errors.As(err, &dual),
				"[%s] expected DualClusterError, got %T: %v", d.name, err, err)

			require.Never(t, func() bool {
				return primary.Mirror().Stats().Enqueued != before.Enqueued
			}, 200*time.Millisecond, 50*time.Millisecond,
				"[%s] mirror engine must not enqueue any capture when primary write failed", d.name)

			after := primary.Mirror().Stats()
			assert.Equal(t, before.Enqueued, after.Enqueued,
				"[%s] mirror engine must not enqueue any capture when primary write failed; got delta=%d",
				d.name, after.Enqueued-before.Enqueued)
		})
	}
}

// TestMirror_DestinationStopAndStart_AutoRefreshRecovers verifies a real
// graceful restart of the mirror destination cluster: while paused-style
// outage tests cover "TCP open, hung," Stop actually closes the container
// and Start brings it back. The mirror client uses AutoRefresh +
// SessionRefresher to detect the dead session and rebuild against the
// restarted cluster; the auto-built replay worker then drains the backlog
// it accumulated during the outage.
//
// Scylla-only: testcontainers' Cassandra module cannot survive Stop/Start
// (num_tokens persistence mismatch — see SPIKE_FINDINGS.md §1).
func TestMirror_DestinationStopAndStart_AutoRefreshRecovers(t *testing.T) {
	a, b := sharedClusters(t)
	if b.Type != testutil.CQLClusterTypeScyllaDB {
		t.Skipf("mirror Stop/Start needs Scylla on cluster B; got %s", b.Type)
	}
	withRestoredCluster(t, b)

	table := createKVTableOnBoth(t, "mirror_dest_stopstart")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()

			var refresherCalls atomic.Int32
			refresher := func(rctx context.Context, cluster helix.ClusterID, _ error) (cql.Session, error) {
				refresherCalls.Add(1)
				if cluster != helix.ClusterA {
					return nil, fmt.Errorf("mirror is single-cluster; only ClusterA expected, got %s", cluster)
				}
				if err := b.Reconnect(rctx); err != nil {
					return nil, fmt.Errorf("rebuild cluster B session: %w", err)
				}

				return d.wrap(b), nil
			}

			mirrorClient, err := helix.NewCQLClient(d.wrap(b), nil,
				helix.WithSessionRefresher(refresher),
				helix.WithAutoRefresh(
					helix.WithAutoRefreshFailureThreshold(3),
					helix.WithAutoRefreshSustainedFailureWindow(3*time.Second),
					helix.WithAutoRefreshMinRetryInterval(2*time.Second),
					helix.WithAutoRefreshCheckInterval(500*time.Millisecond),
					helix.WithAutoRefreshRefreshTimeout(10*time.Second),
				),
			)
			require.NoError(t, err)
			t.Cleanup(mirrorClient.Close)

			memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(64))

			primary, err := helix.NewCQLClient(d.wrap(a), nil,
				helix.WithMirror(mirrorClient,
					mirror.WithWorkers(1),
					mirror.WithQueueSize(8),
				),
				helix.WithMirrorReplayer(memReplayer,
					replay.WithPollInterval(100*time.Millisecond),
					replay.WithRetryDelay(200*time.Millisecond),
					replay.WithMaxAttempts(200),
				),
			)
			require.NoError(t, err)
			t.Cleanup(primary.Close)

			stmt := fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table)

			// Phase 1: graceful Stop on the mirror destination.
			require.NoError(t, b.Stop(ctx, 30*time.Second))

			// Issue mirror writes during the outage. Primary still
			// succeeds; mirror exec will fail and land in the replayer.
			for i := 0; i < 5; i++ {
				require.NoError(t,
					primary.Session().Query(stmt, fmt.Sprintf("k_stop_%d", i), "v").
						Mirror().ExecContext(ctx),
					"primary write should still succeed during mirror outage")
			}

			// Phase 2: bring B back. AutoRefresh on the mirror client must
			// independently detect the dead session, invoke the refresher, and
			// replay through the refreshed client-owned session.
			require.NoError(t, b.Start(ctx))

			require.Eventually(t, func() bool {
				var got string
				err := mirrorClient.SessionA().Query(
					fmt.Sprintf("SELECT value FROM %s WHERE key = ?", table),
					"k_stop_4").Scan(&got)

				return err == nil && got == "v" && refresherCalls.Load() >= 1
			}, 60*time.Second, 200*time.Millisecond,
				"[%s] mirror replay never caught up after Stop+Start; refresherCalls=%d, mirror stats=%+v",
				d.name, refresherCalls.Load(), primary.Mirror().Stats())
		})
	}
}

// TestMirror_DestinationKilled_AutoRefreshRecovers exercises the harder
// failure path: SIGKILL on the mirror destination. The container exits
// abruptly, gocql sees RST on existing connections rather than graceful
// close. After Start + AutoRefresh, the replay worker drains.
//
// Scylla-only: only Scylla survives Stop+Start (which Kill+Start uses).
func TestMirror_DestinationKilled_AutoRefreshRecovers(t *testing.T) {
	a, b := sharedClusters(t)
	if b.Type != testutil.CQLClusterTypeScyllaDB {
		t.Skipf("mirror Kill+Start needs Scylla on cluster B; got %s", b.Type)
	}
	withRestoredCluster(t, b)

	table := createKVTableOnBoth(t, "mirror_dest_kill")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()

			var refresherCalls atomic.Int32
			refresher := func(rctx context.Context, cluster helix.ClusterID, _ error) (cql.Session, error) {
				refresherCalls.Add(1)
				if err := b.Reconnect(rctx); err != nil {
					return nil, fmt.Errorf("rebuild cluster B session: %w", err)
				}

				return d.wrap(b), nil
			}

			mirrorClient, err := helix.NewCQLClient(d.wrap(b), nil,
				helix.WithSessionRefresher(refresher),
				helix.WithAutoRefresh(
					helix.WithAutoRefreshFailureThreshold(3),
					helix.WithAutoRefreshSustainedFailureWindow(3*time.Second),
					helix.WithAutoRefreshMinRetryInterval(2*time.Second),
					helix.WithAutoRefreshCheckInterval(500*time.Millisecond),
					helix.WithAutoRefreshRefreshTimeout(10*time.Second),
				),
			)
			require.NoError(t, err)
			t.Cleanup(mirrorClient.Close)

			memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(64))

			primary, err := helix.NewCQLClient(d.wrap(a), nil,
				helix.WithMirror(mirrorClient,
					mirror.WithWorkers(1),
					mirror.WithQueueSize(8),
				),
				helix.WithMirrorReplayer(memReplayer,
					replay.WithPollInterval(100*time.Millisecond),
					replay.WithRetryDelay(200*time.Millisecond),
					replay.WithMaxAttempts(200),
				),
			)
			require.NoError(t, err)
			t.Cleanup(primary.Close)

			stmt := fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table)

			// SIGKILL on B. Existing TCP gets RST.
			require.NoError(t, b.Kill(ctx))

			// Issue mirror writes during the crash. Primary succeeds;
			// mirror exec sees connection-refused / RST.
			for i := 0; i < 5; i++ {
				require.NoError(t,
					primary.Session().Query(stmt, fmt.Sprintf("k_kill_%d", i), "v").
						Mirror().ExecContext(ctx),
					"primary write should still succeed during mirror destination crash")
			}

			// Restart B. AutoRefresh on the mirror client must independently
			// detect the dead session and refresh before replay drains.
			require.NoError(t, b.Start(ctx))

			require.Eventually(t, func() bool {
				var got string
				err := mirrorClient.SessionA().Query(
					fmt.Sprintf("SELECT value FROM %s WHERE key = ?", table),
					"k_kill_4").Scan(&got)

				return err == nil && got == "v" && refresherCalls.Load() >= 1
			}, 60*time.Second, 200*time.Millisecond,
				"[%s] mirror replay never caught up after Kill+Start; refresherCalls=%d, stats=%+v",
				d.name, refresherCalls.Load(), primary.Mirror().Stats())
		})
	}
}

// TestMirror_WithAdaptiveDualWritePrimary verifies the v1.4.0 plan's
// orthogonality claim under real outage: the mirror engine consumes only
// the err return of executeWriteWithReplay, never its internals, so
// AdaptiveDualWrite's degradation behavior must not affect mirror dispatch
// when the primary write returns nil (any-cluster-ack).
//
// Sequence:
//  1. Pause cluster A. AdaptiveDualWrite degrades A; writes that ack on B
//     (or get fire-and-forwarded as ErrWriteAsync) return success-equiv to
//     the caller.
//  2. Each write opted into Mirror() must dispatch to the mirror destination.
//  3. The mirror engine's success counter increments accordingly.
//
// This is the core orthogonality regression guard for the entire Phase 1
// architecture decision.
func TestMirror_WithAdaptiveDualWritePrimary(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, a)

	table := createKVTableOnBoth(t, "mirror_adaptive")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			adw := policy.NewAdaptiveDualWrite(
				policy.WithAdaptiveStrikeThreshold(2),
				policy.WithAdaptiveDeltaThreshold(100*time.Millisecond),
			)
			// Mirror destination uses the still-healthy cluster B's session.
			mirrorClient, err := helix.NewCQLClient(d.wrap(b), nil)
			require.NoError(t, err)
			t.Cleanup(mirrorClient.Close)

			primary, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithWriteStrategy(adw),
				helix.WithReadStrategy(policy.NewStickyRead()),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
				helix.WithMirror(mirrorClient,
					mirror.WithWorkers(1),
					mirror.WithQueueSize(64),
				),
				helix.WithAutoMemoryWorker(0), // primary replay so degraded writes don't crash setup
			)
			require.NoError(t, err)
			t.Cleanup(primary.Close)

			ctx := context.Background()
			require.NoError(t, a.Pause(ctx))
			defer func() { _ = a.Unpause(context.Background()) }()

			stmt := fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table)

			// Drive enough writes to (a) trigger AdaptiveDualWrite degradation
			// and (b) prove mirror dispatch fires on every successful write.
			const writes = 10
			for i := 0; i < writes; i++ {
				key := fmt.Sprintf("k_adaptive_%d", i)
				err := primary.Session().Query(stmt, key, "v").Mirror().ExecContext(ctx)
				// Accept ErrWriteAsync as success — AdaptiveDualWrite reports
				// fire-and-forget dispatch via this sentinel; mirror still
				// fires because executeWriteWithReplay returns nil.
				if err != nil && !errors.Is(err, htypes.ErrWriteAsync) {
					t.Logf("[%s] write %d returned: %v", d.name, i, err)
				}
			}

			// AdaptiveDualWrite should have degraded A by now.
			assert.True(t, adw.IsDegraded(htypes.ClusterA),
				"[%s] AdaptiveDualWrite must mark A degraded under outage", d.name)

			// Mirror engine must have dispatched the writes (some count >=
			// half of total — exact count depends on degradation timing,
			// but orthogonality means mirror is unaffected by ADW state).
			require.Eventually(t, func() bool {
				return primary.Mirror().Stats().Success >= writes/2
			}, 10*time.Second, 100*time.Millisecond,
				"[%s] mirror dispatch did not fire under AdaptiveDualWrite degradation; stats=%+v",
				d.name, primary.Mirror().Stats())
		})
	}
}

// TestMirror_PrimaryClusterDraining_MirrorStillFires verifies the
// combination of mirror with the topology drain mode. When primary cluster
// A is in drain mode, writes to A are skipped and enqueued for replay; B
// succeeds; executeWriteWithReplay returns nil (any-cluster-ack). The
// mirror engine must still dispatch — the orthogonality contract holds
// against drain just as against pause/kill.
//
// Real-world relevance: during a phased migration, ops may drain the old
// cluster pair piece-by-piece while still wanting mirror writes to land on
// the new pair.
func TestMirror_PrimaryClusterDraining_MirrorStillFires(t *testing.T) {
	a, b := sharedClusters(t)

	table := createKVTableOnBoth(t, "mirror_drain")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			topo := topology.NewLocal()

			// Mirror destination (use cluster B's session — it stays
			// healthy throughout this test).
			mirrorClient, err := helix.NewCQLClient(d.wrap(b), nil)
			require.NoError(t, err)
			t.Cleanup(mirrorClient.Close)

			primary, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithTopologyWatcher(topo),
				helix.WithAutoMemoryWorker(0), // primary replay accepts the drained-A writes
				helix.WithMirror(mirrorClient,
					mirror.WithWorkers(1),
					mirror.WithQueueSize(64),
				),
			)
			require.NoError(t, err)
			t.Cleanup(primary.Close)

			ctx := context.Background()

			// Drain cluster A. Writes targeting A will be skipped and
			// enqueued for primary replay; cluster B will receive them.
			require.NoError(t, topo.SetDrain(ctx, htypes.ClusterA, true, "test drain"))
			defer func() { _ = topo.SetDrain(context.Background(), htypes.ClusterA, false, "") }()

			// Wait briefly for the topology watcher goroutine to observe
			// the drain event (the client polls via the Watch channel).
			require.Eventually(t, func() bool {
				return primary.IsDraining(htypes.ClusterA)
			}, 5*time.Second, 50*time.Millisecond,
				"[%s] primary client did not observe drain on A", d.name)

			stmt := fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table)
			const writes = 5
			for i := 0; i < writes; i++ {
				key := fmt.Sprintf("k_drain_%d", i)
				err := primary.Session().Query(stmt, key, "v").Mirror().ExecContext(ctx)
				require.NoError(t, err,
					"[%s] write %d should succeed via cluster B while A is draining", d.name, i)
			}

			// Mirror engine must have fired for each successful primary
			// write — orthogonality: drain on the source should not
			// suppress mirror dispatch when the write reached at least
			// one cluster.
			require.Eventually(t, func() bool {
				return primary.Mirror().Stats().Success >= writes
			}, 5*time.Second, 50*time.Millisecond,
				"[%s] mirror dispatch did not fire under drain mode; stats=%+v",
				d.name, primary.Mirror().Stats())
		})
	}
}

// TestMirror_PrimaryPartialOutage_MirrorStillFires verifies the
// any-cluster-ack contract under real conditions: with one primary cluster
// paused the dual-write returns nil (the healthy cluster acked); mirror
// dispatch must fire because, from the caller's perspective, the write
// succeeded.
func TestMirror_PrimaryPartialOutage_MirrorStillFires(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, b)

	// One mirror destination that we'll observe via Stats. We also need
	// a real CQL endpoint behind it for the engine's execute to succeed —
	// reuse cluster A (which stays healthy).
	mirrorTable := createKVTableOnBoth(t, "mirror_partial_outage")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			mirrorClient, err := helix.NewCQLClient(d.wrap(a), nil)
			require.NoError(t, err)
			t.Cleanup(mirrorClient.Close)

			primary, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithWriteStrategy(policy.NewSyncDualWrite()),
				helix.WithMirror(mirrorClient,
					mirror.WithWorkers(1),
					mirror.WithQueueSize(8),
				),
				helix.WithAutoMemoryWorker(0), // primary replay for the down cluster
			)
			require.NoError(t, err)
			t.Cleanup(primary.Close)

			ctx := context.Background()
			require.NoError(t, b.Pause(ctx))
			defer func() { _ = b.Unpause(context.Background()) }()

			qCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			require.NoError(t, primary.Session().Query(
				fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", mirrorTable),
				"k_partial", "v1").Mirror().ExecContext(qCtx),
				"[%s] partial-success (cluster A acked) must surface as success", d.name)

			// Mirror engine should record a successful exec against
			// cluster A (the mirror destination).
			require.Eventually(t, func() bool {
				return primary.Mirror().Stats().Success >= 1
			}, 5*time.Second, 50*time.Millisecond,
				"[%s] mirror engine did not record a successful exec; stats=%+v",
				d.name, primary.Mirror().Stats())
		})
	}
}
