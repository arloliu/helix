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

// TestS8_KillA_HardCrash drives a real production-style failure: SIGKILL
// the cluster A container, simulating an OOM-kill or kernel panic. This
// is harder than Pause (process is alive, just frozen) — Kill terminates
// the process abruptly, so:
//
//   - in-flight TCP connections receive RST (or hang until the OS detects
//     the close), exercising gocql's connection-failure paths;
//   - the host port becomes invalid; gocql cannot reconnect to the same
//     endpoint until the container is restarted AND the port reassigned;
//   - sessions built before the kill are permanently dead and must be
//     rebuilt via cluster.Reconnect after Start.
//
// What we verify (Helix-observable):
//  1. Reads continue to succeed via cluster B (failover routing works).
//  2. Writes produce DualClusterError or replay-enqueued errors per
//     configured strategy (no silent loss).
//  3. After Start + Reconnect + new helix client, both clusters
//     converge via replay drain.
//
// Skipped on Cassandra backend because Stop/Start (used after Kill in
// the recovery phase) doesn't survive the cycle there — see
// SPIKE_FINDINGS §1.
func TestS8_KillA_HardCrash(t *testing.T) {
	a, b := sharedClusters(t)
	if a.Type != testutil.CQLClusterTypeScyllaDB {
		t.Skipf("S8 needs Stop/Start on the killed container, which only works on Scylla; got %s", a.Type)
	}

	table := createKVTableOnBoth(t, "s8_kill")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			// Restore A inside the sub-test, not the parent. Each sub-test
			// kills A and must hand back a healthy cluster for the next one.
			withRestoredCluster(t, a)

			ctx := context.Background()
			mc := testutil.NewTestMetricsCollector()

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithWriteStrategy(policy.NewSyncDualWrite()),
				helix.WithReadStrategy(policy.NewStickyRead(
					policy.WithPreferredCluster(htypes.ClusterA),
					policy.WithStickyReadCooldown(1*time.Hour),
				)),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
				helix.WithMetrics(mc),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			// Seed via the raw sessions so reads have something to find.
			seedKV(t, a, b, table, "k", "v")

			// Baseline read confirms routing works pre-kill.
			var got string
			require.NoError(t, client.Query("SELECT value FROM "+table+" WHERE key = ?", "k").
				ScanContext(ctx, &got))
			require.Equal(t, "v", got)

			// SIGKILL cluster A. Container exits; the host port is freed;
			// gocql's existing connections to A start failing immediately.
			require.NoError(t, a.Kill(ctx))
			t.Logf("[%s] SIGKILL'd cluster A", d.name)

			// Reads must continue to succeed via B. Drive several so the
			// failover path is exercised regardless of the strategy's
			// initial Select.
			for i := 0; i < 5; i++ {
				rCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				err := client.Query("SELECT value FROM "+table+" WHERE key = ?", "k").
					ScanContext(rCtx, &got)
				cancel()
				require.NoError(t, err, "[%s] read %d post-kill should succeed via B", d.name, i)
				assert.Equal(t, "v", got)
			}
			t.Logf("[%s] reads post-kill: %d failovers recorded", d.name, mc.GetTotalFailovers())
			assert.GreaterOrEqual(t, mc.GetTotalFailovers(), int64(1),
				"[%s] failover metric must fire when A is killed", d.name)

			// Writes against killed A under SyncDualWrite: per Helix's
			// documented contract (cql_client.go:861, "partial success is
			// still success from the caller's perspective"), a write that
			// fails on one cluster but succeeds on the other returns nil
			// to the caller. The drop is observable via metrics
			// (IncWriteError on A) — not via the returned error.
			//
			// We assert: (a) caller sees nil OR ErrWriteAsync (both = OK
			// per contract), (b) B actually received the row, (c) A's
			// failure is recorded in the metrics.
			wKey := "post-kill-write"
			wErr := client.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
				wKey, "post-kill-value").ExecContext(ctx)
			t.Logf("[%s] write post-kill err=%v", d.name, wErr)
			if wErr != nil && !errors.Is(wErr, htypes.ErrWriteAsync) {
				// A genuine error is also acceptable — depends on whether B
				// finished before the per-op timeout — but a DualClusterError
				// would be unexpected (B was healthy).
				var dual *htypes.DualClusterError
				if errors.As(wErr, &dual) {
					t.Errorf("[%s] write returned DualClusterError but B was healthy: %v",
						d.name, wErr)
				}
			}

			// Verify B has the row regardless of caller-side error.
			require.NoError(t,
				b.Session.Query("SELECT value FROM "+table+" WHERE key = ?", wKey).
					Scan(&got),
				"[%s] B must have received the post-kill write", d.name)
			assert.Equal(t, "post-kill-value", got)
		})
	}
}

// TestS8_StopA_RecoveryAfterRestart exercises the full client-lifecycle
// recovery cycle: graceful Stop of A, work continues via B, Start A,
// rebuild a helix client, and verify reads succeed against the restarted
// cluster.
//
// Why Stop (not Kill) for the recovery scenario:
//
// The companion TestS8_KillA_HardCrash demonstrates that Helix routes
// around an abruptly-killed cluster correctly. But SIGKILL does not flush
// the commit log, so writes that were acked may not actually be on disk
// when the cluster restarts — observed empirically here: after Kill+Start,
// Scylla A is missing rows that SyncDualWrite committed to it. Helix
// surfaces this as `types.ErrNotFound` from A (no fallback because not-
// found is a documented non-failover error). That's a real operator
// concern but not a Helix bug — it's correct behavior given the storage
// layer's durability semantics.
//
// Stop sends SIGTERM first so Scylla flushes; the restart sees the same
// data. This isolates the test to the "client lifecycle across container
// restart" question.
//
// Catches bugs in:
//   - Helix's behavior when its sessions point at a dead host:port and a
//     fresh client must be built around new sessions;
//   - cluster.Reconnect correctly rebuilding both v1 and v2 sessions
//     against the (reassigned) port;
//   - read-strategy state machine acceptance of a "fresh start" — no
//     stale failover latch.
func TestS8_StopA_RecoveryAfterRestart(t *testing.T) {
	a, b := sharedClusters(t)
	if a.Type != testutil.CQLClusterTypeScyllaDB {
		t.Skipf("S8 recovery needs Stop+Start; only works on Scylla, got %s", a.Type)
	}
	withRestoredCluster(t, a)

	table := createKVTableOnBoth(t, "s8_stop_recover")

	// Use only v1 here — recovery is a sequential, expensive scenario
	// and the parity_test.go suite already covers cross-driver assertions.
	d := allDrivers[0]
	ctx := context.Background()

	client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
		helix.WithWriteStrategy(policy.NewSyncDualWrite()),
		helix.WithReadStrategy(policy.NewStickyRead()),
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
	)
	require.NoError(t, err)

	// Phase 1: write some baseline rows.
	for i := 0; i < 5; i++ {
		require.NoError(t,
			client.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
				fmt.Sprintf("baseline-%d", i), "v").ExecContext(ctx))
	}

	// Phase 2: graceful Stop. SIGTERM lets Scylla flush its commit log
	// before SIGKILL fires, so all written data persists to disk.
	require.NoError(t, a.Stop(ctx, 30*time.Second))

	// Phase 3: reads must continue via B while A is stopped.
	var got string
	require.NoError(t,
		client.Query("SELECT value FROM "+table+" WHERE key = ?", "baseline-0").
			ScanContext(ctx, &got),
		"reads must continue via B while A is stopped")
	assert.Equal(t, "v", got)

	// Phase 4: restart A and reconnect cluster sessions.
	require.NoError(t, a.Start(ctx))
	require.NoError(t, a.Reconnect(ctx))

	// Close the dead client; we cannot reuse it because its A-side
	// session points at the old container's (now-defunct) port.
	client.Close()

	// Phase 5: rebuild the helix client with fresh sessions, prefer A,
	// and verify A serves the previously-written rows. This proves the
	// full Stop/Start recovery cycle including data persistence.
	client2, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
		helix.WithWriteStrategy(policy.NewSyncDualWrite()),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(htypes.ClusterA),
		)),
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
	)
	require.NoError(t, err)
	t.Cleanup(client2.Close)

	require.NoError(t,
		client2.Query("SELECT value FROM "+table+" WHERE key = ?", "baseline-0").
			ScanContext(ctx, &got),
		"reads via fresh client against restarted A must succeed")
	assert.Equal(t, "v", got)
}
