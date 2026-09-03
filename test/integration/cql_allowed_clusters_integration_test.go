package integration_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/adapter/cql"
	cqlv1 "github.com/arloliu/helix/adapter/cql/v1"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/topology"
	"github.com/arloliu/helix/types"
)

// =============================================================================
// AllowedClusters Integration Tests
//
// These tests exercise the WithAllowedClusters override with real Cassandra
// clusters. They verify that:
//   - Reads route to the correct physical cluster
//   - Data comes from the expected cluster (not just the right session object)
//   - Override toggle, failover, FallbackRead fence, drain intersection, CAS
//     exclusion, and iterator paths all work end-to-end
//   - Metrics reflect the real routing decisions
// =============================================================================

// noopCloseCQLSession wraps a CQL session with a no-op Close.
// This allows client.Close() to stop background workers (e.g. auto memory
// worker) without closing the shared driver sessions that are reused across
// tests. Shared sessions are cleaned up by TestMain.
type noopCloseCQLSession struct {
	session cql.Session
}

func noopClose(s cql.Session) *noopCloseCQLSession {
	return &noopCloseCQLSession{session: s}
}

func (s *noopCloseCQLSession) Query(stmt string, values ...any) cql.Query {
	return s.session.Query(stmt, values...)
}

func (s *noopCloseCQLSession) Batch(kind cql.BatchType) cql.Batch {
	return s.session.Batch(kind)
}

func (s *noopCloseCQLSession) Close() {
	// Intentionally empty — shared sessions are cleaned up by TestMain.
}

// TestAllowedClusters_Integration_ExcludeCluster verifies that when a cluster
// is excluded by the override, reads come from the allowed cluster even if the
// strategy would pick the excluded one.
func TestAllowedClusters_Integration_ExcludeCluster(t *testing.T) {
	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "ac_exclude", configTableCQLSchema)

	// Write different data to each cluster to prove which one is read.
	require.NoError(t,
		sessionA.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			"k1", "from_A").WithContext(ctx).Exec())
	require.NoError(t,
		sessionB.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			"k1", "from_B").WithContext(ctx).Exec())

	metrics := testutil.NewTestMetricsCollector()

	// Strategy prefers A, but override excludes A.
	client, err := helix.NewCQLClient(
		cqlv1.NewSession(sessionA),
		cqlv1.NewSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(helix.ClusterA),
		)),
		helix.WithAllowedClusters(func() []helix.ClusterID {
			return []helix.ClusterID{helix.ClusterB}
		}),
		helix.WithMetrics(metrics),
	)
	require.NoError(t, err)
	// Note: We do NOT call client.Close() because it would close the shared gocql sessions.

	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k1").
		ScanContext(ctx, &value)
	require.NoError(t, err)
	assert.Equal(t, "from_B", value, "read must come from cluster B, not strategy-preferred A")

	// Metrics must show a read on B, none on A.
	assert.Equal(t, int64(1), metrics.ReadTotal[helix.ClusterB])
	assert.Equal(t, int64(0), metrics.ReadTotal[helix.ClusterA])
}

// TestAllowedClusters_Integration_ToggleOverride verifies that toggling the
// override at runtime switches which physical cluster serves reads, and that
// the data returned matches the new routing.
func TestAllowedClusters_Integration_ToggleOverride(t *testing.T) {
	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "ac_toggle", configTableCQLSchema)

	// Different data per cluster.
	require.NoError(t,
		sessionA.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			"k1", "from_A").WithContext(ctx).Exec())
	require.NoError(t,
		sessionB.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			"k1", "from_B").WithContext(ctx).Exec())

	var overrideActive atomic.Bool
	overrideActive.Store(true)

	client, err := helix.NewCQLClient(
		cqlv1.NewSession(sessionA),
		cqlv1.NewSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(helix.ClusterA),
		)),
		helix.WithAllowedClusters(func() []helix.ClusterID {
			if overrideActive.Load() {
				return []helix.ClusterID{helix.ClusterB}
			}
			return nil
		}),
	)
	require.NoError(t, err)
	// Note: We do NOT call client.Close() because it would close the shared gocql sessions.

	// Phase 1: override active → data from B.
	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k1").
		ScanContext(ctx, &value)
	require.NoError(t, err)
	assert.Equal(t, "from_B", value)

	// Phase 2: remove override → strategy picks A.
	overrideActive.Store(false)

	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k1").
		ScanContext(ctx, &value)
	require.NoError(t, err)
	assert.Equal(t, "from_A", value)
}

// TestAllowedClusters_Integration_FailoverWithinOverride verifies that when
// the override lists two clusters and the primary fails, the read fails over
// to the override's fallback — and the data comes from the right cluster.
func TestAllowedClusters_Integration_FailoverWithinOverride(t *testing.T) {
	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "ac_failover", configTableCQLSchema)

	require.NoError(t,
		sessionA.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			"k1", "from_A").WithContext(ctx).Exec())
	require.NoError(t,
		sessionB.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			"k1", "from_B").WithContext(ctx).Exec())

	// Wrap B to simulate read failures.
	failB := &atomic.Bool{}
	failB.Store(true)
	readErr := errors.New("simulated cluster B read timeout")
	wrapB := newToggleReadFailSession(cqlv1.WrapSession(sessionB), failB, readErr)

	metrics := testutil.NewTestMetricsCollector()

	// Override: [B, A]. B is primary but fails, should failover to A.
	client, err := helix.NewCQLClient(
		cqlv1.NewSession(sessionA),
		wrapB,
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
		helix.WithAllowedClusters(func() []helix.ClusterID {
			return []helix.ClusterID{helix.ClusterB, helix.ClusterA}
		}),
		helix.WithMetrics(metrics),
	)
	require.NoError(t, err)
	// Note: We do NOT call client.Close() because it would close the shared gocql sessions.

	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k1").
		ScanContext(ctx, &value)
	require.NoError(t, err)
	assert.Equal(t, "from_A", value, "should failover to A within override")
	assert.Equal(t, int64(1), metrics.GetTotalFailovers())
}

// TestAllowedClusters_Integration_FallbackReadFenced verifies that FallbackRead
// does NOT probe an excluded cluster, even when the excluded cluster has the
// data. This prevents reading stale data from a recovering cluster.
func TestAllowedClusters_Integration_FallbackReadFenced(t *testing.T) {
	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "ac_fb_fence", configTableCQLSchema)

	// Write data ONLY to A. B has nothing → B returns not-found.
	// Override excludes A, so FallbackRead must NOT probe A.
	require.NoError(t,
		sessionA.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			"k1", "from_A").WithContext(ctx).Exec())

	metrics := testutil.NewTestMetricsCollector()

	client, err := helix.NewCQLClient(
		cqlv1.NewSession(sessionA),
		cqlv1.NewSession(sessionB),
		helix.WithAllowedClusters(func() []helix.ClusterID {
			return []helix.ClusterID{helix.ClusterB} // A excluded
		}),
		helix.WithMetrics(metrics),
	)
	require.NoError(t, err)
	// Note: We do NOT call client.Close() because it would close the shared gocql sessions.

	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k1").
		FallbackRead().
		ScanContext(ctx, &value)

	// Must get not-found, NOT "from_A" — A is fenced.
	assert.ErrorIs(t, err, types.ErrNotFound)

	// Only B should have read metrics — A must not have been probed.
	assert.Equal(t, int64(1), metrics.ReadTotal[helix.ClusterB])
	assert.Equal(t, int64(0), metrics.ReadTotal[helix.ClusterA])
	assert.Equal(t, int64(0), metrics.ReadDivergence[helix.ClusterB],
		"no divergence — fenced cluster was not probed")
}

// TestAllowedClusters_Integration_FallbackReadAllowedWhenBothInList verifies
// that FallbackRead DOES probe the alternative when it's in the allowed list,
// and detects divergence correctly with real data.
func TestAllowedClusters_Integration_FallbackReadAllowedWhenBothInList(t *testing.T) {
	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "ac_fb_allowed", configTableCQLSchema)

	// Write data ONLY to A. B returns not-found on the primary read.
	// Override: [B, A]. FallbackRead should probe A and find the data.
	require.NoError(t,
		sessionA.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			"k1", "from_A").WithContext(ctx).Exec())

	metrics := testutil.NewTestMetricsCollector()

	client, err := helix.NewCQLClient(
		cqlv1.NewSession(sessionA),
		cqlv1.NewSession(sessionB),
		helix.WithAllowedClusters(func() []helix.ClusterID {
			return []helix.ClusterID{helix.ClusterB, helix.ClusterA}
		}),
		helix.WithMetrics(metrics),
	)
	require.NoError(t, err)
	// Note: We do NOT call client.Close() because it would close the shared gocql sessions.

	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k1").
		FallbackRead().
		ScanContext(ctx, &value)

	require.NoError(t, err)
	assert.Equal(t, "from_A", value, "FallbackRead should find data on A")

	// Both clusters should have read metrics, and divergence should be detected.
	assert.Equal(t, int64(1), metrics.ReadTotal[helix.ClusterB], "primary read on B")
	assert.Equal(t, int64(1), metrics.ReadTotal[helix.ClusterA], "fallback read on A")
	assert.Equal(t, int64(1), metrics.ReadDivergence[helix.ClusterB],
		"divergence on B (selected cluster was B, data found on A)")
}

// TestAllowedClusters_Integration_CAS_NotOverridden verifies that CAS
// operations (lightweight transactions) bypass the AllowedClusters override
// and use the ReadStrategy's selection. This is critical because CAS is a
// write-like operation and rerouting it would increase divergence.
func TestAllowedClusters_Integration_CAS_NotOverridden(t *testing.T) {
	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "ac_cas", configTableCQLSchema)

	// Override says B only — but CAS should still go to A (strategy's pick).
	client, err := helix.NewCQLClient(
		cqlv1.NewSession(sessionA),
		cqlv1.NewSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(helix.ClusterA),
		)),
		helix.WithAllowedClusters(func() []helix.ClusterID {
			return []helix.ClusterID{helix.ClusterB}
		}),
	)
	require.NoError(t, err)
	// Note: We do NOT call client.Close() because it would close the shared gocql sessions.

	// CAS INSERT on the strategy-selected cluster (A).
	//
	// We pass (key, value) destinations even though we only care about
	// `applied`. This is because Scylla's IF NOT EXISTS returns 3 columns
	// ([applied], key, value) even when applied=true, while Cassandra
	// returns only [applied] in that case. gocql's ScanCAS errors with
	// "not enough columns to scan into" on a column-count mismatch, so
	// the destination shape must match Scylla's wider response. Cassandra
	// happily fills in zero values for the missing columns when applied=true.
	var existingKey, existingValue string
	applied, err := client.Query(
		"INSERT INTO "+table+" (key, value) VALUES (?, ?) IF NOT EXISTS",
		"cas_key", "cas_value",
	).ScanCASContext(ctx, &existingKey, &existingValue)
	require.NoError(t, err)
	assert.True(t, applied, "CAS should be applied (new row)")

	// Verify the row landed on A (strategy's cluster), not B (override).
	var value string
	err = sessionA.Query("SELECT value FROM "+table+" WHERE key = ?", "cas_key").
		WithContext(ctx).Scan(&value)
	require.NoError(t, err)
	assert.Equal(t, "cas_value", value, "CAS row must be on cluster A")

	// Negative assertion: B must NOT have the row. A regression that duplicated
	// CAS to both clusters would still put data on B and this would catch it.
	var valueB string
	errB := sessionB.Query("SELECT value FROM "+table+" WHERE key = ?", "cas_key").
		WithContext(ctx).Scan(&valueB)
	assert.ErrorIs(t, errB, gocql.ErrNotFound, "CAS must not be rerouted to cluster B by the override")
}

// TestAllowedClusters_Integration_DrainConflict verifies that when the override
// allows only a cluster that is draining, the read fails with ErrNoValidClusters
// rather than silently reading stale data.
func TestAllowedClusters_Integration_DrainConflict(t *testing.T) {
	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "ac_drain_conflict", configTableCQLSchema)

	require.NoError(t,
		sessionA.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			"k1", "value").WithContext(ctx).Exec())

	watcher := topology.NewLocal()
	defer watcher.Close()

	client, err := helix.NewCQLClient(
		cqlv1.NewSession(sessionA),
		cqlv1.NewSession(sessionB),
		helix.WithTopologyWatcher(watcher),
		helix.WithAllowedClusters(func() []helix.ClusterID {
			return []helix.ClusterID{helix.ClusterA} // only A
		}),
	)
	require.NoError(t, err)
	// Note: We do NOT call client.Close() because it would close the shared gocql sessions.

	// Set A to draining via topology watcher.
	require.NoError(t, watcher.SetDrain(ctx, types.ClusterA, true, "maintenance"))

	// Wait for drain state to propagate.
	require.Eventually(t, func() bool {
		return client.IsDraining(helix.ClusterA)
	}, time.Second, 10*time.Millisecond, "drain state should propagate")

	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k1").
		ScanContext(ctx, &value)
	assert.ErrorIs(t, err, types.ErrNoValidClusters)
}

// TestAllowedClusters_Integration_DrainFiltersWithinOverride verifies that
// when the override lists both clusters but one is draining, reads go to the
// non-draining cluster.
func TestAllowedClusters_Integration_DrainFiltersWithinOverride(t *testing.T) {
	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "ac_drain_filter", configTableCQLSchema)

	require.NoError(t,
		sessionA.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			"k1", "from_A").WithContext(ctx).Exec())
	require.NoError(t,
		sessionB.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			"k1", "from_B").WithContext(ctx).Exec())

	metrics := testutil.NewTestMetricsCollector()
	watcher := topology.NewLocal()
	defer watcher.Close()

	client, err := helix.NewCQLClient(
		cqlv1.NewSession(sessionA),
		cqlv1.NewSession(sessionB),
		helix.WithTopologyWatcher(watcher),
		helix.WithAllowedClusters(func() []helix.ClusterID {
			return []helix.ClusterID{helix.ClusterA, helix.ClusterB}
		}),
		helix.WithMetrics(metrics),
	)
	require.NoError(t, err)
	// Note: We do NOT call client.Close() because it would close the shared gocql sessions.

	// Drain A — reads should go to B even though A is first in the list.
	require.NoError(t, watcher.SetDrain(ctx, types.ClusterA, true, "maintenance"))
	require.Eventually(t, func() bool {
		return client.IsDraining(helix.ClusterA)
	}, time.Second, 10*time.Millisecond)

	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k1").
		ScanContext(ctx, &value)
	require.NoError(t, err)
	assert.Equal(t, "from_B", value, "A is draining — must read from B")
	assert.Equal(t, int64(1), metrics.ReadTotal[helix.ClusterB])
	assert.Equal(t, int64(0), metrics.ReadTotal[helix.ClusterA])
}

// TestAllowedClusters_Integration_IterContext verifies that IterContext
// respects the override, routing the iterator to the correct cluster.
func TestAllowedClusters_Integration_IterContext(t *testing.T) {
	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "ac_iter", configTableCQLSchema)

	// Write to B only — data from A's perspective doesn't exist.
	require.NoError(t,
		sessionB.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			"iter_key", "iter_val").WithContext(ctx).Exec())

	client, err := helix.NewCQLClient(
		cqlv1.NewSession(sessionA),
		cqlv1.NewSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(helix.ClusterA), // strategy says A
		)),
		helix.WithAllowedClusters(func() []helix.ClusterID {
			return []helix.ClusterID{helix.ClusterB} // override says B
		}),
	)
	require.NoError(t, err)
	// Note: We do NOT call client.Close() because it would close the shared gocql sessions.

	iter := client.Query("SELECT key, value FROM "+table+" WHERE key = ?", "iter_key").
		Consistency(helix.LocalOne).
		IterContext(ctx)

	var key, value string
	found := iter.Scan(&key, &value)
	require.True(t, found, "iterator should find the row on cluster B")
	assert.Equal(t, "iter_key", key)
	assert.Equal(t, "iter_val", value)

	err = iter.Close()
	require.NoError(t, err)
}

// TestAllowedClusters_Integration_IterContext_ErrorIter verifies that
// IterContext returns an errorIter when resolveReadTarget fails, and that
// Close() surfaces the error.
func TestAllowedClusters_Integration_IterContext_ErrorIter(t *testing.T) {
	sessionA, sessionB := getSharedSessions(t)

	client, err := helix.NewCQLClient(
		cqlv1.NewSession(sessionA),
		cqlv1.NewSession(sessionB),
		helix.WithAllowedClusters(func() []helix.ClusterID {
			return []helix.ClusterID{"UNKNOWN"} // fail-closed
		}),
	)
	require.NoError(t, err)
	// Note: We do NOT call client.Close() because it would close the shared gocql sessions.

	iter := client.Query("SELECT key FROM some_table").Iter()

	// Scan returns false — no data.
	var key string
	assert.False(t, iter.Scan(&key))

	// Close surfaces the fail-closed error.
	err = iter.Close()
	assert.ErrorIs(t, err, types.ErrInvalidClusterOverride)
}

// TestAllowedClusters_Integration_StrategyFrozen_ThenResumes verifies that
// when the override is active the ReadStrategy state is not mutated, and
// when it's removed the strategy resumes from its frozen state — routing
// reads to the correct cluster.
func TestAllowedClusters_Integration_StrategyFrozen_ThenResumes(t *testing.T) {
	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "ac_freeze", configTableCQLSchema)

	require.NoError(t,
		sessionA.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			"k1", "from_A").WithContext(ctx).Exec())
	require.NoError(t,
		sessionB.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			"k1", "from_B").WithContext(ctx).Exec())

	var overrideActive atomic.Bool
	overrideActive.Store(true)

	// StickyRead prefers A.
	client, err := helix.NewCQLClient(
		cqlv1.NewSession(sessionA),
		cqlv1.NewSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(helix.ClusterA),
		)),
		helix.WithAllowedClusters(func() []helix.ClusterID {
			if overrideActive.Load() {
				return []helix.ClusterID{helix.ClusterB}
			}
			return nil
		}),
	)
	require.NoError(t, err)
	// Note: We do NOT call client.Close() because it would close the shared gocql sessions.

	// Phase 1: override active — reads from B, strategy frozen.
	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k1").
		ScanContext(ctx, &value)
	require.NoError(t, err)
	assert.Equal(t, "from_B", value)

	// Phase 2: remove override — strategy resumes, should prefer A.
	overrideActive.Store(false)

	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k1").
		ScanContext(ctx, &value)
	require.NoError(t, err)
	assert.Equal(t, "from_A", value,
		"after override removed, strategy should resume preferring A")
}

// TestAllowedClusters_Integration_WritesThenOverridedReads verifies the
// full operator workflow: writes go to both clusters, override fences reads
// away from cluster A, and reads come from B.
func TestAllowedClusters_Integration_WritesThenOverridedReads(t *testing.T) {
	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "ac_write_read", configTableCQLSchema)

	var overrideActive atomic.Bool

	client, err := helix.NewCQLClient(
		cqlv1.NewSession(sessionA),
		cqlv1.NewSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(helix.ClusterA),
		)),
		helix.WithAllowedClusters(func() []helix.ClusterID {
			if overrideActive.Load() {
				return []helix.ClusterID{helix.ClusterB}
			}
			return nil
		}),
	)
	require.NoError(t, err)
	// Note: We do NOT call client.Close() because it would close the shared gocql sessions.

	// Dual-write: data lands on both clusters.
	err = client.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
		"k1", "dual_write_val").ExecContext(ctx)
	require.NoError(t, err)

	// Verify data on both clusters via raw sessions.
	var valA, valB string
	require.NoError(t,
		sessionA.Query("SELECT value FROM "+table+" WHERE key = ?", "k1").
			WithContext(ctx).Scan(&valA))
	require.NoError(t,
		sessionB.Query("SELECT value FROM "+table+" WHERE key = ?", "k1").
			WithContext(ctx).Scan(&valB))
	assert.Equal(t, "dual_write_val", valA)
	assert.Equal(t, "dual_write_val", valB)

	// Enable override — reads from B only.
	overrideActive.Store(true)

	var readVal string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k1").
		ScanContext(ctx, &readVal)
	require.NoError(t, err)
	assert.Equal(t, "dual_write_val", readVal)

	// Mutate A's data directly to prove reads are from B.
	require.NoError(t,
		sessionA.Query("UPDATE "+table+" SET value = ? WHERE key = ?",
			"mutated_on_A", "k1").WithContext(ctx).Exec())

	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k1").
		ScanContext(ctx, &readVal)
	require.NoError(t, err)
	assert.Equal(t, "dual_write_val", readVal,
		"must read from B (still has original value), not A (mutated)")
}

// TestAllowedClusters_Integration_MapScan verifies override routing with
// MapScan (not just Scan), confirming the executeRead path is used.
func TestAllowedClusters_Integration_MapScan(t *testing.T) {
	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "ac_mapscan", configTableCQLSchema)

	require.NoError(t,
		sessionB.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			"mk1", "map_val_B").WithContext(ctx).Exec())

	client, err := helix.NewCQLClient(
		cqlv1.NewSession(sessionA),
		cqlv1.NewSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(helix.ClusterA),
		)),
		helix.WithAllowedClusters(func() []helix.ClusterID {
			return []helix.ClusterID{helix.ClusterB}
		}),
	)
	require.NoError(t, err)
	// Note: We do NOT call client.Close() because it would close the shared gocql sessions.

	m := make(map[string]any)
	err = client.Query("SELECT key, value FROM "+table+" WHERE key = ?", "mk1").
		MapScanContext(ctx, m)
	require.NoError(t, err)
	assert.Equal(t, "mk1", m["key"])
	assert.Equal(t, "map_val_B", m["value"])
}

// TestAllowedClusters_Integration_PanicRecovery verifies that a panicking
// AllowedClusters function is recovered and surfaced as ErrClusterOverridePanic.
func TestAllowedClusters_Integration_PanicRecovery(t *testing.T) {
	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "ac_panic", configTableCQLSchema)

	client, err := helix.NewCQLClient(
		cqlv1.NewSession(sessionA),
		cqlv1.NewSession(sessionB),
		helix.WithAllowedClusters(func() []helix.ClusterID {
			panic("feature flag service crashed")
		}),
	)
	require.NoError(t, err)
	// Note: We do NOT call client.Close() because it would close the shared gocql sessions.

	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k1").
		ScanContext(ctx, &value)
	assert.ErrorIs(t, err, types.ErrClusterOverridePanic)
}

// TestAllowedClusters_Integration_ForceDegrade_PlusOverride verifies the
// operator workflow where ForceDegrade controls writes and AllowedClusters
// controls reads — both targeting the same cluster exclusion.
func TestAllowedClusters_Integration_ForceDegrade_PlusOverride(t *testing.T) {
	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "ac_degrade", configTableCQLSchema)

	var overrideActive atomic.Bool

	writeStrategy := policy.NewAdaptiveDualWrite()
	metrics := testutil.NewTestMetricsCollector()

	// noopClose wraps the shared sessions so client.Close() stops the
	// auto memory worker without tearing down the shared driver connections.
	client, err := helix.NewCQLClient(
		noopClose(cqlv1.NewSession(sessionA)),
		noopClose(cqlv1.NewSession(sessionB)),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(helix.ClusterA),
		)),
		helix.WithWriteStrategy(writeStrategy),
		helix.WithAutoMemoryWorker(0),
		helix.WithMetrics(metrics),
		helix.WithAllowedClusters(func() []helix.ClusterID {
			if overrideActive.Load() {
				return []helix.ClusterID{helix.ClusterB}
			}
			return nil
		}),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close) // stops the auto memory worker; shared sessions are unaffected

	// Phase 1: Normal — write lands on both.
	err = client.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
		"k1", "phase1").ExecContext(ctx)
	require.NoError(t, err)

	// Phase 2: Degrade A + override reads to B.
	writeStrategy.ForceDegrade(helix.ClusterA)
	overrideActive.Store(true)

	// A is degraded: write to A is fire-and-forget (non-blocking); B receives
	// the synchronous write. The caller still gets nil — partial success is success.
	err = client.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
		"k2", "phase2").ExecContext(ctx)
	require.NoError(t, err)

	// Verify that the write routing reflects degraded state: A was fire-and-forget
	// (WriteAsync), B was synchronous. The background write to A is replayed only
	// if it fails, and A is reachable, so nothing is enqueued.
	assert.Equal(t, int64(1), metrics.GetWriteAsync(helix.ClusterA),
		"A degraded: write must be fire-and-forget (WriteAsync=1)")
	assert.Equal(t, int64(0), metrics.GetWriteAsync(helix.ClusterB),
		"B healthy: write must be synchronous (WriteAsync=0)")
	assert.Equal(t, int64(0), metrics.GetReplayEnqueued(helix.ClusterA),
		"A degraded but reachable: the background write is not replayed")

	// Read must come from B (override is active).
	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k2").
		ScanContext(ctx, &value)
	require.NoError(t, err)
	assert.Equal(t, "phase2", value)

	// Mutate k2 on A directly to prove subsequent reads come from B.
	require.NoError(t,
		sessionA.Query("UPDATE "+table+" SET value = ? WHERE key = ?",
			"mutated_on_A", "k2").WithContext(ctx).Exec())

	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k2").
		ScanContext(ctx, &value)
	require.NoError(t, err)
	assert.Equal(t, "phase2", value,
		"must read from B (override active), not A (which has mutated value)")

	// Phase 3: Recover A + remove override.
	writeStrategy.ForceRecover(helix.ClusterA)
	overrideActive.Store(false)

	// New write should go to both again.
	err = client.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
		"k3", "phase3").ExecContext(ctx)
	require.NoError(t, err)

	// Read from strategy-preferred A.
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k3").
		ScanContext(ctx, &value)
	require.NoError(t, err)
	assert.Equal(t, "phase3", value)

	// Verify B also received the write (dual-write recovered).
	var valueB string
	errB := sessionB.Query("SELECT value FROM "+table+" WHERE key = ?", "k3").
		WithContext(ctx).Scan(&valueB)
	require.NoError(t, errB, "post-recovery write must land on cluster B")
	assert.Equal(t, "phase3", valueB, "cluster B must have the recovered dual-write value")
}

// TestAllowedClusters_Integration_DefaultFallbackRead_Fenced verifies that
// WithDefaultFallbackRead(true) is also fenced by the override, not just
// per-query FallbackRead().
func TestAllowedClusters_Integration_DefaultFallbackRead_Fenced(t *testing.T) {
	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "ac_dfb_fence", configTableCQLSchema)

	// Data only on A. Override says B only.
	require.NoError(t,
		sessionA.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			"k1", "from_A").WithContext(ctx).Exec())

	metrics := testutil.NewTestMetricsCollector()

	client, err := helix.NewCQLClient(
		cqlv1.NewSession(sessionA),
		cqlv1.NewSession(sessionB),
		helix.WithDefaultFallbackRead(true), // client-level FallbackRead
		helix.WithAllowedClusters(func() []helix.ClusterID {
			return []helix.ClusterID{helix.ClusterB}
		}),
		helix.WithMetrics(metrics),
	)
	require.NoError(t, err)
	// Note: We do NOT call client.Close() because it would close the shared gocql sessions.

	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k1").
		ScanContext(ctx, &value)

	assert.ErrorIs(t, err, types.ErrNotFound)
	assert.Equal(t, int64(0), metrics.ReadTotal[helix.ClusterA],
		"A must not be probed by DefaultFallbackRead when fenced by override")
}

// TestAllowedClusters_Integration_SingleCluster_NilReturn verifies that
// single-cluster mode with AllowedClusters configured but returning nil
// works correctly — no phantom ClusterB behavior.
func TestAllowedClusters_Integration_SingleCluster_NilReturn(t *testing.T) {
	sessionA, _ := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "ac_sc_nil", configTableCQLSchema)

	require.NoError(t,
		sessionA.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			"k1", "from_A").WithContext(ctx).Exec())

	metrics := testutil.NewTestMetricsCollector()

	client, err := helix.NewCQLClient(
		cqlv1.NewSession(sessionA),
		nil, // single-cluster mode
		helix.WithAllowedClusters(func() []helix.ClusterID {
			return nil // opt-out
		}),
		helix.WithMetrics(metrics),
	)
	require.NoError(t, err)
	// Note: We do NOT call client.Close() because it would close the shared gocql sessions.

	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k1").
		ScanContext(ctx, &value)
	require.NoError(t, err)
	assert.Equal(t, "from_A", value)

	// Only ClusterA metrics.
	assert.Equal(t, int64(1), metrics.ReadTotal[helix.ClusterA])
	assert.Equal(t, int64(0), metrics.ReadTotal[helix.ClusterB],
		"no phantom ClusterB reads in single-cluster mode")
}
