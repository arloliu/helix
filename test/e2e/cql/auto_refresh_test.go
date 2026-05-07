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
	"github.com/arloliu/helix/adapter/cql"
	cqlv1 "github.com/arloliu/helix/adapter/cql/v1"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/test/testutil"
	htypes "github.com/arloliu/helix/types"
)

// TestS10_AutoRefresh_RecoversFromStopStartWithoutManualCall is the
// headline demo for the v2 auto-refresh feature: caller wires
// WithSessionRefresher + WithAutoRefresh ONCE at construction and
// never calls RefreshSession. Helix detects the dead session and
// invokes the refresher itself.
//
// Replaces the S9 manual-trigger pattern (where the caller invoked
// RefreshSession on its own observation) with the auto-trigger
// pattern from v2. The same underlying mechanism (RefreshSession +
// SessionRefresher); the difference is who decides when.
//
// Scylla-only because the testcontainers Cassandra module's
// Stop/Start is broken (num_tokens persistence mismatch — see
// SPIKE_FINDINGS §1).
func TestS10_AutoRefresh_RecoversFromStopStartWithoutManualCall(t *testing.T) {
	a, b := sharedClusters(t)
	if a.Type != testutil.CQLClusterTypeScyllaDB {
		t.Skipf("S10 needs Stop/Start on cluster A; only works on Scylla, got %s", a.Type)
	}
	withRestoredCluster(t, a)

	table := createKVTableOnBoth(t, "s10_auto_refresh")
	d := allDrivers[0] // v1 only — driver parity is covered by parity_test.go

	ctx := context.Background()

	// Refresher rebuilds the cluster's gocql session against the (now
	// reassigned) host:port. Same pattern as S9.
	refresher := func(rctx context.Context, cluster helix.ClusterID, _ error) (cql.Session, error) {
		if cluster != helix.ClusterA {
			return nil, fmt.Errorf("test refresher only handles ClusterA, got %s", cluster)
		}
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
		// Tight time scales so the test runs in seconds, not minutes.
		helix.WithAutoRefresh(
			helix.WithAutoRefreshFailureThreshold(3),
			helix.WithAutoRefreshSustainedFailureWindow(5*time.Second),
			helix.WithAutoRefreshMinRetryInterval(2*time.Second),
			helix.WithAutoRefreshCheckInterval(1*time.Second),
			helix.WithAutoRefreshRefreshTimeout(10*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	// Phase 1: baseline write/read confirms routing works.
	require.NoError(t,
		client.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			"k", "v").ExecContext(ctx),
		"baseline write must succeed",
	)
	var got string
	require.NoError(t,
		client.Query("SELECT value FROM "+table+" WHERE key = ?", "k").
			ScanContext(ctx, &got),
		"baseline read must succeed",
	)

	// Phase 2: graceful Stop+Start. Data persists, but the host port is
	// reassigned so the live gocql session is permanently broken.
	require.NoError(t, a.Stop(ctx, 30*time.Second))
	require.NoError(t, a.Start(ctx))

	// Phase 3: drive WRITE traffic continuously without manually calling
	// RefreshSession. Reads under StickyRead+failover flow to B after
	// the first A error and never come back, starving the detector of
	// failure samples. Writes under dual-write hit BOTH clusters every
	// time (errA=err, errB=nil → partial success) so A's
	// consecutiveFailures climbs continuously, which is exactly the
	// signal the detector watches.
	deadline := time.Now().Add(15 * time.Second)
	writeIdx := 0
	for time.Now().Before(deadline) {
		wCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_ = client.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			fmt.Sprintf("auto-%d", writeIdx), "v").ExecContext(wCtx)
		cancel()
		writeIdx++
		// Once auto-refresh fired AND succeeded, subsequent ops should
		// be hitting the rebuilt session. Bail early.
		if mc.GetSessionRefreshAttempts(htypes.ClusterA) >= 1 &&
			mc.GetSessionRefreshSuccesses(htypes.ClusterA) >= 1 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Logf("auto-refresh: attempts=%d successes=%d errors=%d",
		mc.GetSessionRefreshAttempts(htypes.ClusterA),
		mc.GetSessionRefreshSuccesses(htypes.ClusterA),
		mc.GetSessionRefreshErrors(htypes.ClusterA),
	)
	require.GreaterOrEqual(t, mc.GetSessionRefreshAttempts(htypes.ClusterA), int64(1),
		"the detector must have fired at least once for ClusterA")
	require.GreaterOrEqual(t, mc.GetSessionRefreshSuccesses(htypes.ClusterA), int64(1),
		"the refresher must have rebuilt the session successfully")

	// Phase 4: post-refresh reads succeed against the SAME helix.CQLClient
	// instance — the topology watcher and any other background components
	// stayed alive throughout.
	rCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	require.NoError(t,
		client.Query("SELECT value FROM "+table+" WHERE key = ?", "k").
			ScanContext(rCtx, &got),
		"post-auto-refresh read must succeed",
	)
	cancel()
	assert.Equal(t, "v", got, "data persisted across Stop/Start (graceful drain)")
}

// TestS10_AutoRefresh_NoStormUnderPersistentFailure verifies the
// throttle bound. With a refresher that always errors and a
// MinRetryInterval of 2s, attempts MUST be capped near
// (test_duration / MinRetryInterval) — never higher.
func TestS10_AutoRefresh_NoStormUnderPersistentFailure(t *testing.T) {
	a, b := sharedClusters(t)
	if a.Type != testutil.CQLClusterTypeScyllaDB {
		t.Skipf("S10 storm-prevention needs Stop on cluster A; only works on Scylla, got %s", a.Type)
	}
	withRestoredCluster(t, a)

	table := createKVTableOnBoth(t, "s10_no_storm")
	d := allDrivers[0]

	ctx := context.Background()

	rebuildErr := errors.New("refresher always fails for this test")
	refresher := func(_ context.Context, _ helix.ClusterID, _ error) (cql.Session, error) {
		return nil, rebuildErr
	}

	mc := testutil.NewTestMetricsCollector()
	client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(htypes.ClusterA),
		)),
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
		helix.WithMetrics(mc),
		helix.WithSessionRefresher(refresher),
		helix.WithAutoRefresh(
			helix.WithAutoRefreshFailureThreshold(3),
			helix.WithAutoRefreshSustainedFailureWindow(2*time.Second),
			helix.WithAutoRefreshMinRetryInterval(2*time.Second),
			helix.WithAutoRefreshCheckInterval(500*time.Millisecond),
			helix.WithAutoRefreshRefreshTimeout(2*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	// Seed so the read-path has a key to fetch, then break A.
	seedKV(t, a, b, table, "k", "v")
	require.NoError(t, a.Stop(ctx, 30*time.Second))
	t.Cleanup(func() {
		// Ensure A is restarted before the next test; ignore errors.
		_ = a.Start(context.Background())
	})

	// Drive failing writes (not reads — see S10 #1's rationale) for
	// ~12 seconds. With MinRetryInterval=2s, attempts should be at
	// most ~6-7 (12s / 2s + the initial fire).
	const testDuration = 12 * time.Second
	deadline := time.Now().Add(testDuration)
	writeIdx := 0
	for time.Now().Before(deadline) {
		wCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_ = client.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			fmt.Sprintf("storm-%d", writeIdx), "v").ExecContext(wCtx)
		cancel()
		writeIdx++
		time.Sleep(200 * time.Millisecond)
	}

	attempts := mc.GetSessionRefreshAttempts(htypes.ClusterA)
	errCount := mc.GetSessionRefreshErrors(htypes.ClusterA)
	t.Logf("auto-refresh storm-prevention: attempts=%d errors=%d in %s",
		attempts, errCount, testDuration,
	)

	// Each failing refresh increments both attempts and errors.
	require.GreaterOrEqual(t, attempts, int64(1),
		"detector must have fired at least once")
	// Strict bound: with MinRetryInterval=2s over 12s, at most 7 attempts
	// (1 initial + 6 retries at 2s intervals). Allow 8 for tick alignment.
	assert.LessOrEqual(t, attempts, int64(8),
		"throttle must cap attempts well below the failure rate")
	assert.Equal(t, attempts, errCount,
		"every failing attempt should increment SessionRefreshError")
}
