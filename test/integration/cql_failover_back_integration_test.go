package integration_test

import (
	"context"
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
	"github.com/arloliu/helix/types"
)

// =============================================================================
// Failover-Back Tests
//
// These tests verify that read strategies recover when the failover target
// also fails and the original cluster has come back online. Without the fix,
// reads would fail entirely even when the original cluster is available.
// =============================================================================

func TestPrimaryOnlyRead_FailoverBack_BFailsRecoversToA(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "fo_back_primary", configTableCQLSchema)

	// Write data to both clusters.
	for _, s := range []*gocql.Session{sessionA, sessionB} {
		err := s.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			"failover_key", "failover_value").Exec()
		require.NoError(t, err)
	}

	// Wrap both sessions with toggleable read failures.
	failA := &atomic.Bool{}
	failB := &atomic.Bool{}
	readErr := errors.New("simulated read timeout")
	wrapA := newToggleReadFailSession(cqlv1.WrapSession(sessionA), failA, readErr)
	wrapB := newToggleReadFailSession(cqlv1.WrapSession(sessionB), failB, readErr)

	metricsCollector := testutil.NewTestMetricsCollector()

	client, err := helix.NewCQLClient(
		wrapA,
		wrapB,
		helix.WithReadStrategy(policy.NewPrimaryOnlyRead()),
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
		helix.WithMetrics(metricsCollector),
	)
	require.NoError(t, err)

	// Phase 1: Both healthy — reads go to A.
	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "failover_key").
		ScanContext(ctx, &value)
	require.NoError(t, err)
	assert.Equal(t, "failover_value", value)

	// Phase 2: A fails — reads failover to B.
	failA.Store(true)
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "failover_key").
		ScanContext(ctx, &value)
	require.NoError(t, err, "should failover to B when A fails")
	assert.Equal(t, "failover_value", value)

	// Phase 3: A recovers, B fails — reads should failover back to A.
	failA.Store(false)
	failB.Store(true)
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "failover_key").
		ScanContext(ctx, &value)
	require.NoError(t, err, "should failover back to A when B fails and A recovered")
	assert.Equal(t, "failover_value", value)

	// Verify failover metrics: two failover events (A→B, then B→A).
	assert.True(t, metricsCollector.GetTotalFailovers() >= 2,
		"expected at least 2 failover events; got %d", metricsCollector.GetTotalFailovers())
}

func TestPrimaryOnlyRead_FailoverBack_BothDown_ReturnsDualError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "fo_back_dual_err", configTableCQLSchema)

	failA := &atomic.Bool{}
	failB := &atomic.Bool{}
	readErr := errors.New("simulated read timeout")
	wrapA := newToggleReadFailSession(cqlv1.WrapSession(sessionA), failA, readErr)
	wrapB := newToggleReadFailSession(cqlv1.WrapSession(sessionB), failB, readErr)

	client, err := helix.NewCQLClient(
		wrapA,
		wrapB,
		helix.WithReadStrategy(policy.NewPrimaryOnlyRead()),
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
	)
	require.NoError(t, err)

	// A fails → switch to B.
	failA.Store(true)
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "any_key").
		ScanContext(ctx, new(string))
	// B is healthy — returns not-found (empty table), which is fine.
	require.True(t, types.IsNotFound(err) || err == nil)

	// Both fail — should return DualClusterError.
	failB.Store(true)
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "any_key").
		ScanContext(ctx, new(string))
	require.Error(t, err)

	var dualErr *types.DualClusterError
	assert.True(t, errors.As(err, &dualErr),
		"both clusters down: expected DualClusterError; got: %T: %v", err, err)
}

func TestStickyRead_FailoverBack_WithinCooldown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()

	table := createTestTableOnBoth(t, "fo_back_sticky", configTableCQLSchema)

	// Write data to both clusters.
	for _, s := range []*gocql.Session{sessionA, sessionB} {
		err := s.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			"sticky_key", "sticky_value").Exec()
		require.NoError(t, err)
	}

	failA := &atomic.Bool{}
	failB := &atomic.Bool{}
	readErr := errors.New("simulated read timeout")
	wrapA := newToggleReadFailSession(cqlv1.WrapSession(sessionA), failA, readErr)
	wrapB := newToggleReadFailSession(cqlv1.WrapSession(sessionB), failB, readErr)

	metricsCollector := testutil.NewTestMetricsCollector()

	client, err := helix.NewCQLClient(
		wrapA,
		wrapB,
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(types.ClusterA),
			policy.WithStickyReadCooldown(1*time.Hour), // Long cooldown — force the within-cooldown path
		)),
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
		helix.WithMetrics(metricsCollector),
	)
	require.NoError(t, err)

	// Phase 1: A fails → failover to B (preferred switches to B).
	failA.Store(true)
	var value string
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "sticky_key").
		ScanContext(ctx, &value)
	require.NoError(t, err, "should failover to B when A fails")

	// Phase 2: A recovers, B fails — within cooldown, should still return A
	// as failover target for this request.
	failA.Store(false)
	failB.Store(true)
	err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "sticky_key").
		ScanContext(ctx, &value)
	require.NoError(t, err, "should failover back to A when B fails (even within cooldown)")
	assert.Equal(t, "sticky_value", value)
}

// =============================================================================
// Helpers
// =============================================================================

// toggleReadFailCQLSession wraps a CQL session with an atomic toggle for read
// failures. When failing is true, all reads return readErr; writes pass through.
type toggleReadFailCQLSession struct {
	session cql.Session
	failing *atomic.Bool
	readErr error
}

func newToggleReadFailSession(session cql.Session, failing *atomic.Bool, readErr error) *toggleReadFailCQLSession {
	return &toggleReadFailCQLSession{session: session, failing: failing, readErr: readErr}
}

func (s *toggleReadFailCQLSession) Query(stmt string, values ...any) cql.Query {
	return &toggleReadFailCQLQuery{
		Query:   s.session.Query(stmt, values...),
		failing: s.failing,
		readErr: s.readErr,
	}
}

func (s *toggleReadFailCQLSession) Batch(kind cql.BatchType) cql.Batch {
	return s.session.Batch(kind)
}

func (s *toggleReadFailCQLSession) Close() {
	// Intentionally empty — shared sessions are cleaned up by TestMain.
}

// toggleReadFailCQLQuery wraps a query and fails reads when the toggle is set.
type toggleReadFailCQLQuery struct {
	cql.Query
	failing *atomic.Bool
	readErr error
}

func (q *toggleReadFailCQLQuery) Consistency(c cql.Consistency) cql.Query {
	q.Query = q.Query.Consistency(c)
	return q
}

func (q *toggleReadFailCQLQuery) PageSize(n int) cql.Query {
	q.Query = q.Query.PageSize(n)
	return q
}

func (q *toggleReadFailCQLQuery) PageState(state []byte) cql.Query {
	q.Query = q.Query.PageState(state)
	return q
}

func (q *toggleReadFailCQLQuery) WithTimestamp(ts int64) cql.Query {
	q.Query = q.Query.WithTimestamp(ts)
	return q
}

func (q *toggleReadFailCQLQuery) SerialConsistency(c cql.Consistency) cql.Query {
	q.Query = q.Query.SerialConsistency(c)
	return q
}

func (q *toggleReadFailCQLQuery) Scan(dest ...any) error {
	if q.failing.Load() {
		return q.readErr
	}
	return q.Query.Scan(dest...)
}

func (q *toggleReadFailCQLQuery) ScanContext(ctx context.Context, dest ...any) error {
	if q.failing.Load() {
		return q.readErr
	}
	return q.Query.ScanContext(ctx, dest...)
}

func (q *toggleReadFailCQLQuery) MapScan(dest map[string]any) error {
	if q.failing.Load() {
		return q.readErr
	}
	return q.Query.MapScan(dest)
}

func (q *toggleReadFailCQLQuery) MapScanContext(ctx context.Context, dest map[string]any) error {
	if q.failing.Load() {
		return q.readErr
	}
	return q.Query.MapScanContext(ctx, dest)
}
