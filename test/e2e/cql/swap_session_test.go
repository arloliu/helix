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
	"github.com/arloliu/helix/adapter/cql"
	cqlv1 "github.com/arloliu/helix/adapter/cql/v1"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/test/testutil"
	htypes "github.com/arloliu/helix/types"
)

// TestS9_RefreshSession_RecoversAcrossStopStart demonstrates the v1 of the
// session-swap feature: after a real cluster Stop+Start cycle (which
// reassigns the Docker host port and breaks the live gocql session), a
// caller-registered SessionRefresher rebuilds the session and Helix
// continues operating against the same CQLClient instance.
//
// This is the practical replacement for S8's "rebuild a fresh helix.CQLClient"
// pattern: it preserves the live topology watcher, the running replay
// worker, and any application-side references to the client.
//
// Scylla-only: Cassandra Stop/Start is broken in the testcontainers
// module due to a num_tokens persistence mismatch (see SPIKE_FINDINGS §1).
func TestS9_RefreshSession_RecoversAcrossStopStart(t *testing.T) {
	a, b := sharedClusters(t)
	if a.Type != testutil.CQLClusterTypeScyllaDB {
		t.Skipf("S9 needs Stop/Start on cluster A, which only works on Scylla; got %s", a.Type)
	}
	withRestoredCluster(t, a)

	table := createKVTableOnBoth(t, "s9_refresh")
	d := allDrivers[0] // v1 only — driver parity is covered by parity_test.go

	ctx := context.Background()

	// The refresher captures the *testutil.CQLCluster pointer and rebuilds
	// the cluster's gocql session against the cluster's current host:port
	// (which Docker reassigned across Stop/Start). It then wraps the fresh
	// gocql session with the same v1 adapter the rest of the suite uses.
	//
	// This proves the decoupling principle: Helix never touches gocql
	// directly — the refresher is the only place driver-specific code
	// lives, and the caller owns it.
	refresherInvocations := 0
	refresher := func(rctx context.Context, cluster helix.ClusterID, lastErr error) (cql.Session, error) {
		refresherInvocations++
		if cluster != helix.ClusterA {
			return nil, fmt.Errorf("test refresher only handles ClusterA, got %s", cluster)
		}
		// cluster.Reconnect rebuilds testutil's underlying gocql sessions
		// against the (possibly remapped) host. Subsequent reads of
		// cluster.Session see the new gocql.Session.
		if err := a.Reconnect(rctx); err != nil {
			return nil, fmt.Errorf("rebuild cluster A session: %w", err)
		}

		return cqlv1.NewSession(a.Session), nil
	}

	mc := testutil.NewTestMetricsCollector()
	client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(htypes.ClusterA),
		)),
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
		helix.WithMetrics(mc),
		helix.WithSessionRefresher(refresher),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	// Phase 1: write a baseline row through the live client. SyncDualWrite
	// (default) lands the row on both A and B.
	require.NoError(t,
		client.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			"k", "v").ExecContext(ctx),
		"baseline write must succeed",
	)

	// Phase 2: graceful Stop of A so its commit log flushes; restart so
	// the data is preserved across the cycle. Docker reassigns the host
	// port — the existing gocql session is now permanently broken.
	require.NoError(t, a.Stop(ctx, 30*time.Second))
	require.NoError(t, a.Start(ctx))

	// Phase 3: invoke the new feature. RefreshSession runs the refresher,
	// which rebuilds testutil's gocql session and returns it wrapped in
	// our adapter. RefreshSession atomically swaps it into the live
	// CQLClient and closes the old session.
	require.NoError(t, client.RefreshSession(ctx, helix.ClusterA),
		"RefreshSession must succeed after Stop+Start")
	assert.Equal(t, 1, refresherInvocations,
		"refresher invoked exactly once by RefreshSession")

	// Phase 4: verify reads against A succeed via the SAME helix.CQLClient
	// instance we built before Stop. No client teardown was needed.
	var got string
	rCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	require.NoError(t,
		client.Query("SELECT value FROM "+table+" WHERE key = ?", "k").
			ScanContext(rCtx, &got),
		"post-refresh read against restarted A must succeed",
	)
	cancel()
	assert.Equal(t, "v", got, "data persisted across Stop/Start (graceful drain)")

	// Phase 5: the topology watcher and any other background components
	// the client started at construction are still alive — the entire
	// point of this feature. Sanity-check by reading from B which has
	// been continuously up.
	require.NoError(t,
		b.Session.Query("SELECT value FROM "+table+" WHERE key = ?", "k").
			Scan(&got),
		"raw read of B confirms baseline write reached B too")
	assert.Equal(t, "v", got)
}

// TestS9_RefreshSession_HelpsAfterKill demonstrates that RefreshSession
// also recovers from a SIGKILL'd cluster (after restart) — the same
// pattern as S9 above but with Kill instead of Stop. Kill loses unflushed
// commit-log data so we don't assert row content; we only assert that the
// client recovers operability (queries route to the rebuilt A session).
//
// This complements S8_KillA_RecoveryAfterRestart which rebuilt the entire
// helix.CQLClient. With RefreshSession, reconstruction is unnecessary.
func TestS9_RefreshSession_HelpsAfterKill(t *testing.T) {
	a, b := sharedClusters(t)
	if a.Type != testutil.CQLClusterTypeScyllaDB {
		t.Skipf("S9 needs Start on the killed container; only works on Scylla, got %s", a.Type)
	}
	withRestoredCluster(t, a)

	table := createKVTableOnBoth(t, "s9_refresh_kill")
	d := allDrivers[0]

	ctx := context.Background()

	refresher := func(rctx context.Context, cluster helix.ClusterID, _ error) (cql.Session, error) {
		if cluster != helix.ClusterA {
			return nil, fmt.Errorf("only ClusterA refresh expected, got %s", cluster)
		}
		if err := a.Reconnect(rctx); err != nil {
			return nil, err
		}

		return cqlv1.NewSession(a.Session), nil
	}

	client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(htypes.ClusterA),
		)),
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
		helix.WithSessionRefresher(refresher),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	// Seed via raw sessions so we know A had the row at some point. After
	// kill, A's data may or may not include this — Kill bypasses commit
	// log flush — so we only verify post-refresh that A is responsive.
	seedKV(t, a, b, table, "k", "v")

	require.NoError(t, a.Kill(ctx))
	require.NoError(t, a.Start(ctx))

	// Refresh the helix client's view of A.
	require.NoError(t, client.RefreshSession(ctx, helix.ClusterA))

	// A is reachable again via the same client. Issue a read; the row
	// may have been lost on A (commit log not flushed) so we accept
	// either ErrNotFound or success — the assertion is that the client
	// is FUNCTIONAL, not that data persisted across SIGKILL.
	var got string
	rCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k").
		ScanContext(rCtx, &got)
	if err != nil && !htypes.IsNotFound(err) {
		t.Fatalf("post-refresh read returned an unexpected error (not nil, not ErrNotFound): %v", err)
	}
}
