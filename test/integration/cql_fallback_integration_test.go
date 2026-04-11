package integration_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	gocqlv2 "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/adapter/cql"
	cqlv1 "github.com/arloliu/helix/adapter/cql/v1"
	cqlv2 "github.com/arloliu/helix/adapter/cql/v2"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
)

// =============================================================================
// Adapter ErrNotFound Round-Trip Tests
//
// These verify that gocql.ErrNotFound from a real Cassandra query arrives as
// types.ErrNotFound through the adapter layer. The v1/v2 adapter unit tests
// only check sentinel identity; these tests close the loop with real clusters.
//
// If the adapter mapping silently breaks, every mock-based FallbackRead test
// still passes green — but the feature stops working in production. These
// tests catch that.
// =============================================================================

func TestV1Adapter_Scan_ReturnsErrNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, _ := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "v1_nf_scan", usersTableSchema)

	adapter := cqlv1.NewSession(sessionA)

	// Query a row that does not exist.
	var name string
	err := adapter.Query("SELECT name FROM "+table+" WHERE id = ?", gocql.TimeUUID()).
		ScanContext(ctx, &name)

	require.Error(t, err)
	assert.True(t, types.IsNotFound(err),
		"v1 adapter Scan must map gocql.ErrNotFound to types.ErrNotFound; got: %v", err)
}

func TestV1Adapter_MapScan_ReturnsErrNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, _ := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "v1_nf_mapscan", usersTableSchema)

	adapter := cqlv1.NewSession(sessionA)

	result := make(map[string]any)
	err := adapter.Query("SELECT name FROM "+table+" WHERE id = ?", gocql.TimeUUID()).
		MapScanContext(ctx, result)

	require.Error(t, err)
	assert.True(t, types.IsNotFound(err),
		"v1 adapter MapScan must map gocql.ErrNotFound to types.ErrNotFound; got: %v", err)
}

func TestV2Adapter_Scan_ReturnsErrNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, _ := getSharedSessionsV2(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "v2_nf_scan", usersTableSchema)

	adapter := cqlv2.NewSession(sessionA)

	var name string
	err := adapter.Query("SELECT name FROM "+table+" WHERE id = ?", gocqlv2.TimeUUID()).
		ScanContext(ctx, &name)

	require.Error(t, err)
	assert.True(t, types.IsNotFound(err),
		"v2 adapter Scan must map gocql.ErrNotFound to types.ErrNotFound; got: %v", err)
}

func TestV2Adapter_MapScan_ReturnsErrNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, _ := getSharedSessionsV2(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "v2_nf_mapscan", usersTableSchema)

	adapter := cqlv2.NewSession(sessionA)

	result := make(map[string]any)
	err := adapter.Query("SELECT name FROM "+table+" WHERE id = ?", gocqlv2.TimeUUID()).
		MapScanContext(ctx, result)

	require.Error(t, err)
	assert.True(t, types.IsNotFound(err),
		"v2 adapter MapScan must map gocql.ErrNotFound to types.ErrNotFound; got: %v", err)
}

// =============================================================================
// FallbackRead End-to-End Tests
//
// These prove the user's scenario against real Cassandra clusters: write lands
// on only one cluster (simulating a partial dual-write failure or replay lag),
// and FallbackRead recovers the data from the other cluster.
// =============================================================================

func TestFallbackRead_WriteToOneCluster_ReadBackViaFallback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "fb_write_one", configTableCQLSchema)

	// Write directly to cluster B only (simulates partial write — A missed it).
	err := sessionB.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
		"critical_key", "important_value").Exec()
	require.NoError(t, err)

	// Create a Helix client with StickyRead preferring A.
	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(types.ClusterA),
		)),
	)
	require.NoError(t, err)

	// Without FallbackRead: A returns not-found.
	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "critical_key").
		ScanContext(ctx, &value)
	require.Error(t, err)
	assert.True(t, types.IsNotFound(err),
		"without FallbackRead, must return ErrNotFound; got: %v", err)

	// With FallbackRead: finds the data on B.
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "critical_key").
		FallbackRead().ScanContext(ctx, &value)
	require.NoError(t, err, "FallbackRead must find the data on cluster B")
	assert.Equal(t, "important_value", value)
}

func TestFallbackRead_WriteToOneCluster_MapScan(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "fb_mapscan", configTableCQLSchema)

	// Write only to cluster B.
	err := sessionB.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
		"map_key", "map_value").Exec()
	require.NoError(t, err)

	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(types.ClusterA),
		)),
	)
	require.NoError(t, err)

	// FallbackRead + MapScan: finds the data on B.
	result := make(map[string]any)
	err = client.Query("SELECT key, value FROM "+table+" WHERE key = ?", "map_key").
		FallbackRead().MapScanContext(ctx, result)
	require.NoError(t, err, "FallbackRead MapScan must find the data on cluster B")
	assert.Equal(t, "map_value", result["value"])
}

func TestFallbackRead_BothNotFound_ReturnsErrNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "fb_both_nf", configTableCQLSchema)

	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
	)
	require.NoError(t, err)

	// Neither cluster has the row — definitively not found.
	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "nonexistent_key").
		FallbackRead().ScanContext(ctx, &value)

	require.Error(t, err)
	assert.True(t, types.IsNotFound(err),
		"both clusters empty: must return ErrNotFound; got: %v", err)
}

func TestFallbackRead_PrimaryHasData_NoFallbackAttempted(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "fb_primary_has", configTableCQLSchema)

	// Write to both clusters.
	err := sessionA.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
		"present_key", "from_a").Exec()
	require.NoError(t, err)
	err = sessionB.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
		"present_key", "from_b").Exec()
	require.NoError(t, err)

	metricsCollector := testutil.NewTestMetricsCollector()

	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(types.ClusterA),
		)),
		helix.WithMetrics(metricsCollector),
	)
	require.NoError(t, err)

	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "present_key").
		FallbackRead().ScanContext(ctx, &value)
	require.NoError(t, err)
	assert.Equal(t, "from_a", value)

	// No divergence ��� primary had the data, fallback never attempted.
	assert.Equal(t, int64(0), metricsCollector.GetReadDivergence(types.ClusterA))
	assert.Equal(t, int64(0), metricsCollector.GetReadDivergence(types.ClusterB))

	// Only primary was queried. ReadTotal is safe to read after client operations complete.
	assert.Equal(t, int64(1), metricsCollector.ReadTotal[types.ClusterA])
	assert.Equal(t, int64(0), metricsCollector.ReadTotal[types.ClusterB])
}

// =============================================================================
// Divergence Metric Tests
//
// When FallbackRead finds data on the alternative cluster, IncReadDivergence
// must fire on the stale cluster. This is the operational signal operators
// use to correlate replay lag with specific clusters.
// =============================================================================

func TestFallbackRead_DivergenceMetric_RealClusters(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "fb_diverge", configTableCQLSchema)

	// Write only to cluster B — simulates replay lag on A.
	err := sessionB.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
		"diverge_key", "diverge_value").Exec()
	require.NoError(t, err)

	metricsCollector := testutil.NewTestMetricsCollector()

	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(types.ClusterA),
		)),
		helix.WithMetrics(metricsCollector),
	)
	require.NoError(t, err)

	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "diverge_key").
		FallbackRead().ScanContext(ctx, &value)
	require.NoError(t, err)
	assert.Equal(t, "diverge_value", value)

	// Divergence recorded on the stale cluster (A), not the one that had data (B).
	assert.Equal(t, int64(1), metricsCollector.GetReadDivergence(types.ClusterA),
		"IncReadDivergence must fire on the stale cluster (A)")
	assert.Equal(t, int64(0), metricsCollector.GetReadDivergence(types.ClusterB),
		"IncReadDivergence must NOT fire on the cluster that had data (B)")
}

// =============================================================================
// Error Classification with Real Clusters
//
// Not-found must never trip health signals. These tests verify the error
// classification is correct end-to-end — not just against mocks.
// =============================================================================

func TestNotFound_DoesNotTripFailoverPolicy_RealClusters(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "nf_no_failover", configTableCQLSchema)

	metricsCollector := testutil.NewTestMetricsCollector()

	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(types.ClusterA),
		)),
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
		helix.WithMetrics(metricsCollector),
	)
	require.NoError(t, err)

	// Query multiple non-existent rows.
	for range 10 {
		var value string
		err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "no_such_key").
			ScanContext(ctx, &value)
		require.True(t, types.IsNotFound(err))
	}

	// ReadTotal counts every attempt; ReadError must be zero.
	assert.Equal(t, int64(10), metricsCollector.ReadTotal[types.ClusterA],
		"ReadTotal must count every not-found read")
	assert.Equal(t, int64(0), metricsCollector.GetReadErrors(types.ClusterA),
		"not-found must not increment ReadError")
	assert.Equal(t, int64(0), metricsCollector.GetReadErrors(types.ClusterB),
		"cluster B must not have any read errors (never queried)")

	// No failover events — not-found is not a failure.
	assert.Equal(t, int64(0), metricsCollector.GetTotalFailovers(),
		"not-found must not trigger failover")
}

// =============================================================================
// WithDefaultFallbackRead Client Option
//
// Verifies the client-level default propagates through real read paths.
// =============================================================================

func TestWithDefaultFallbackRead_EnablesForAllQueries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "fb_default", configTableCQLSchema)

	// Write only to cluster B.
	err := sessionB.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
		"default_fb_key", "found_via_default").Exec()
	require.NoError(t, err)

	metricsCollector := testutil.NewTestMetricsCollector()

	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(types.ClusterA),
		)),
		helix.WithDefaultFallbackRead(true),
		helix.WithMetrics(metricsCollector),
	)
	require.NoError(t, err)

	// No per-query FallbackRead() — the client default should activate it.
	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "default_fb_key").
		ScanContext(ctx, &value)
	require.NoError(t, err, "WithDefaultFallbackRead(true) must enable fallback for all queries")
	assert.Equal(t, "found_via_default", value)

	// Confirm divergence was recorded.
	assert.Equal(t, int64(1), metricsCollector.GetReadDivergence(types.ClusterA))
}

// =============================================================================
// Context-Level FallbackRead
// =============================================================================

func TestWithFallbackReadContext_EnablesFallback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)

	table := createTestTableOnBoth(t, "fb_ctx", configTableCQLSchema)

	// Write only to cluster B.
	err := sessionB.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
		"ctx_fb_key", "found_via_ctx").Exec()
	require.NoError(t, err)

	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(types.ClusterA),
		)),
	)
	require.NoError(t, err)

	// Context-level FallbackRead.
	ctx := helix.WithFallbackRead(t.Context())
	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "ctx_fb_key").
		ScanContext(ctx, &value)
	require.NoError(t, err, "context-level WithFallbackRead must enable fallback")
	assert.Equal(t, "found_via_ctx", value)
}

// =============================================================================
// FallbackRead with v2 Adapter
//
// The ErrNotFound mapping is a separate code path in v2. This ensures
// FallbackRead works end-to-end through the v2 adapter as well.
// =============================================================================

func TestFallbackRead_V2Adapter_WriteToOneCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionAv2, sessionBv2 := getSharedSessionsV2(t)
	// Use v1 sessions for direct inserts (both session flavors share the same clusters).
	_, sessionBv1 := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "fb_v2", configTableCQLSchema)

	// Write directly to cluster B only via raw v1 session.
	err := sessionBv1.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
		"v2_key", "v2_value").Exec()
	require.NoError(t, err)

	client, err := helix.NewCQLClient(
		cqlv2.NewSession(sessionAv2),
		cqlv2.NewSession(sessionBv2),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(types.ClusterA),
		)),
	)
	require.NoError(t, err)

	// Without FallbackRead: not-found from v2 adapter.
	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "v2_key").
		ScanContext(ctx, &value)
	require.Error(t, err)
	assert.True(t, types.IsNotFound(err),
		"v2 adapter must return types.ErrNotFound; got: %v", err)

	// With FallbackRead: finds data on B through v2 adapter.
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "v2_key").
		FallbackRead().ScanContext(ctx, &value)
	require.NoError(t, err, "FallbackRead through v2 adapter must find data on cluster B")
	assert.Equal(t, "v2_value", value)
}

// =============================================================================
// FallbackRead after Dual-Write Partial Failure + Replay Convergence
//
// This is the closest to the real production scenario: a dual-write partially
// fails, FallbackRead helps in the interim, then replay converges the data.
// =============================================================================

func TestFallbackRead_PartialWrite_ThenReplayConverges(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "fb_partial_replay", configTableCQLSchema)

	metricsCollector := testutil.NewTestMetricsCollector()

	// Use a partial-failure session for B: first write fails, subsequent succeed.
	var failCounter atomic.Int32
	failCounter.Store(1) // Next 1 write to B fails.
	failCounterB := &partialFailureCQLSession{
		session:       cqlv1.WrapSession(sessionB),
		failNextWrite: &failCounter,
	}

	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		failCounterB,
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(types.ClusterA),
		)),
		helix.WithAutoMemoryWorker(0),
		helix.WithMetrics(metricsCollector),
	)
	require.NoError(t, err)
	// Note: We don't defer client.Close() because it would close the shared gocql sessions.

	// Write — succeeds on A, fails on B (enqueued for replay).
	err = client.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
		"partial_key", "partial_value").Exec()
	require.NoError(t, err, "partial write must return nil (A succeeded)")

	// FallbackRead from A's perspective: A has it, returns immediately.
	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "partial_key").
		FallbackRead().ScanContext(ctx, &value)
	require.NoError(t, err)
	assert.Equal(t, "partial_value", value)

	// Allow replay worker to converge.
	time.Sleep(500 * time.Millisecond)

	// After replay, B should also have the data — read from B directly.
	var valueB string
	err = sessionB.Query("SELECT value FROM "+table+" WHERE key = ?", "partial_key").
		Scan(&valueB)
	require.NoError(t, err, "replay must converge data to cluster B")
	assert.Equal(t, "partial_value", valueB)
}

// =============================================================================
// Primary Real Error + Failover Not-Found (AP Availability)
//
// When the primary cluster has a real error and the failover cluster returns
// not-found, the caller receives ErrNotFound — the healthy cluster's answer.
// In an AP system, returning the primary's downtime error would fail the
// request for rows that genuinely do not exist, breaking availability during
// single-cluster outages. Health recording still targets the correct cluster.
// =============================================================================

func TestFailover_PrimaryError_FailoverNotFound_ReturnsNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "fo_err_nf", configTableCQLSchema)

	// Wrap A with a read-failing adapter so reads error but writes succeed.
	failingA := &readFailingCQLSession{
		session: cqlv1.WrapSession(sessionA),
		readErr: errors.New("simulated cluster A read timeout"),
	}

	metricsCollector := testutil.NewTestMetricsCollector()

	client, err := helix.NewCQLClient(
		failingA,
		cqlv1.WrapSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(types.ClusterA),
		)),
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
		helix.WithMetrics(metricsCollector),
	)
	require.NoError(t, err)

	// A returns a real error → failover to B → B returns not-found (empty table).
	var value string
	scanErr := client.Query("SELECT value FROM "+table+" WHERE key = ?", "nonexistent_key").
		ScanContext(ctx, &value)

	require.Error(t, scanErr)
	// Healthy cluster's not-found must be returned to preserve availability.
	assert.True(t, types.IsNotFound(scanErr),
		"healthy cluster's not-found must be returned; got: %v", scanErr)

	// Cluster B responded correctly (not-found) — no health impact on B.
	assert.Equal(t, int64(0), metricsCollector.GetReadErrors(types.ClusterB),
		"failover not-found must NOT increment ReadError on cluster B")
	// Cluster A had a real error — health impact on A.
	assert.Equal(t, int64(1), metricsCollector.GetReadErrors(types.ClusterA),
		"primary real error must increment ReadError on cluster A")
}

// =============================================================================
// ReadTotal Accounting
//
// When FallbackRead triggers, both the primary and fallback reads must be
// counted in ReadTotal.
// =============================================================================

func TestFallbackRead_ReadTotal_BothClustersRecorded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "fb_readtotal", configTableCQLSchema)

	// Write only to B.
	err := sessionB.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
		"total_key", "total_value").Exec()
	require.NoError(t, err)

	metricsCollector := testutil.NewTestMetricsCollector()

	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(types.ClusterA),
		)),
		helix.WithMetrics(metricsCollector),
	)
	require.NoError(t, err)

	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "total_key").
		FallbackRead().ScanContext(ctx, &value)
	require.NoError(t, err)

	// Both clusters had a read attempt. ReadTotal is safe to read after client operations complete.
	assert.Equal(t, int64(1), metricsCollector.ReadTotal[types.ClusterA],
		"primary not-found read must be counted")
	assert.Equal(t, int64(1), metricsCollector.ReadTotal[types.ClusterB],
		"fallback read must be counted")

	// No read errors — not-found on A is not an error, success on B is not an error.
	assert.Equal(t, int64(0), metricsCollector.GetReadErrors(types.ClusterA))
	assert.Equal(t, int64(0), metricsCollector.GetReadErrors(types.ClusterB))
}

// =============================================================================
// Helpers
// =============================================================================

// readFailingCQLSession wraps a CQL session and returns a fixed error on all
// read operations (Scan, MapScan, etc.) while allowing writes to pass through.
type readFailingCQLSession struct {
	session cql.Session
	readErr error
}

func (s *readFailingCQLSession) Query(stmt string, values ...any) cql.Query {
	return &readFailingCQLQuery{
		Query:   s.session.Query(stmt, values...),
		readErr: s.readErr,
	}
}

func (s *readFailingCQLSession) Batch(kind cql.BatchType) cql.Batch {
	return s.session.Batch(kind)
}

func (s *readFailingCQLSession) Close() {
	// Intentionally empty — shared sessions are cleaned up by TestMain.
}

// readFailingCQLQuery wraps a query and fails all read methods.
type readFailingCQLQuery struct {
	cql.Query
	readErr error
}

func (q *readFailingCQLQuery) Consistency(c cql.Consistency) cql.Query {
	q.Query = q.Query.Consistency(c)
	return q
}

func (q *readFailingCQLQuery) PageSize(n int) cql.Query {
	q.Query = q.Query.PageSize(n)
	return q
}

func (q *readFailingCQLQuery) PageState(state []byte) cql.Query {
	q.Query = q.Query.PageState(state)
	return q
}

func (q *readFailingCQLQuery) WithTimestamp(ts int64) cql.Query {
	q.Query = q.Query.WithTimestamp(ts)
	return q
}

func (q *readFailingCQLQuery) SerialConsistency(c cql.Consistency) cql.Query {
	q.Query = q.Query.SerialConsistency(c)
	return q
}

func (q *readFailingCQLQuery) Scan(_ ...any) error                           { return q.readErr }
func (q *readFailingCQLQuery) ScanContext(_ context.Context, _ ...any) error { return q.readErr }
func (q *readFailingCQLQuery) MapScan(_ map[string]any) error                { return q.readErr }
func (q *readFailingCQLQuery) MapScanContext(_ context.Context, _ map[string]any) error {
	return q.readErr
}
