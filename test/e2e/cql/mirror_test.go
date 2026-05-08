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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/mirror"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
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

			deadline := time.Now().Add(30 * time.Second)
			for time.Now().Before(deadline) {
				var got string
				err := b.Session.Query(
					fmt.Sprintf("SELECT value FROM %s WHERE key = ?", table),
					"k_during_outage").Scan(&got)
				if err == nil && got == "v1" {
					return // success — replayer caught up
				}
				time.Sleep(100 * time.Millisecond)
			}
			t.Fatalf("[%s] mirror replay did not catch up after destination recovered (stats=%+v)",
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

			// Give the engine a moment to demonstrate it did NOT fire.
			time.Sleep(200 * time.Millisecond)

			after := primary.Mirror().Stats()
			assert.Equal(t, before.Enqueued, after.Enqueued,
				"[%s] mirror engine must not enqueue any capture when primary write failed; got delta=%d",
				d.name, after.Enqueued-before.Enqueued)
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
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				if primary.Mirror().Stats().Success >= 1 {
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
			t.Fatalf("[%s] mirror engine did not record a successful exec; stats=%+v",
				d.name, primary.Mirror().Stats())
		})
	}
}
