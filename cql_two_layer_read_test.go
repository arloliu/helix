package helix

import (
	"context"
	"errors"
	"testing"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunPrimaryRead_ClientClosed_ReturnsAttemptedFalse_NoMetrics(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}

	client, err := NewCQLClient(sessionA, sessionB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)
	require.NoError(t, err)
	client.Close() // close the client BEFORE running the read

	res := client.runPrimaryRead(context.Background(), readOptions{},
		func(_ context.Context, _ cql.Session) error { return nil })
	require.False(t, res.attempted, "client-closed must yield attempted=false")
	require.ErrorIs(t, res.err, types.ErrSessionClosed)

	assert.Equal(t, int64(0), met.get(met.ReadTotal, ClusterA))
	assert.Equal(t, int64(0), met.get(met.ReadTotal, ClusterB))
	assert.Empty(t, policy.RecordFailureCalls)
}

func TestRunPrimaryRead_ResolveReadTargetFails_ReturnsAttemptedFalse_NoMetrics(t *testing.T) {
	sessionA := newMockSession()
	met := newReadTestMetrics()

	// Single-cluster + AllowedClusters targeting ClusterB triggers
	// ErrInvalidClusterOverride from resolveReadTarget.
	client, err := NewCQLClient(sessionA, nil,
		WithMetrics(met),
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterB}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	res := client.runPrimaryRead(context.Background(), readOptions{},
		func(_ context.Context, _ cql.Session) error { return nil })
	require.False(t, res.attempted)
	require.ErrorIs(t, res.err, types.ErrInvalidClusterOverride)

	assert.Equal(t, int64(0), met.get(met.ReadTotal, ClusterA),
		"pre-attempt fail-closed must not emit cluster metrics")
}

func TestRunPrimaryRead_SingleClusterMode_TreatsAsPrimaryAttempt(t *testing.T) {
	sessionA := newMockSession()
	met := newReadTestMetrics()

	client, err := NewCQLClient(sessionA, nil,
		WithMetrics(met),
	)
	require.NoError(t, err)
	defer client.Close()

	res := client.runPrimaryRead(context.Background(), readOptions{},
		func(_ context.Context, _ cql.Session) error { return nil })
	require.True(t, res.attempted, "single-cluster mode is a real attempt")
	require.Equal(t, ClusterA, res.selected)
	require.NoError(t, res.err)

	assert.Equal(t, int64(1), met.get(met.ReadTotal, ClusterA),
		"single-cluster attempt records IncReadTotal exactly once")
}

func TestExecuteRead_AttemptedFalse_PropagatesPreAttemptErr_NoMetrics(t *testing.T) {
	sessionA := newMockSession()
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}

	client, err := NewCQLClient(sessionA, nil,
		WithMetrics(met),
		WithFailoverPolicy(policy),
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterB}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	err = client.executeRead(context.Background(), readOptions{},
		func(_ context.Context, _ cql.Session) error {
			t.Fatal("readFunc must not run on pre-attempt fail-closed")
			return nil
		})
	require.ErrorIs(t, err, types.ErrInvalidClusterOverride)

	assert.Equal(t, int64(0), met.get(met.ReadTotal, ClusterA))
	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterA))
	assert.Empty(t, policy.RecordFailureCalls,
		"pre-attempt errors are not cluster faults; no RecordFailure")
}

func TestExecuteReadNoFailover_AttemptedFalse_PropagatesPreAttemptErr_NoMetrics(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}

	client, err := NewCQLClient(sessionA, sessionB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)
	require.NoError(t, err)
	client.Close()

	err = client.executeReadNoFailover(context.Background(), readOptions{},
		func(_ context.Context, _ cql.Session) error { return nil })
	require.ErrorIs(t, err, types.ErrSessionClosed)

	assert.Equal(t, int64(0), met.get(met.ReadTotal, ClusterA))
	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterA))
	assert.Empty(t, policy.RecordFailureCalls)
}

func TestExecuteReadNoFailover_EmitsSameMetricsAsExecuteRead_OnSuccess(t *testing.T) {
	sessionA1 := newMockSession()
	sessionB1 := newMockSession()
	sessionA2 := newMockSession()
	sessionB2 := newMockSession()
	met1 := newReadTestMetrics()
	met2 := newReadTestMetrics()

	clientER, err := NewCQLClient(sessionA1, sessionB1, WithMetrics(met1))
	require.NoError(t, err)
	defer clientER.Close()

	clientNF, err := NewCQLClient(sessionA2, sessionB2, WithMetrics(met2))
	require.NoError(t, err)
	defer clientNF.Close()

	// Both clients run a successful read.
	err = clientER.executeRead(context.Background(), readOptions{},
		func(_ context.Context, _ cql.Session) error { return nil })
	require.NoError(t, err)

	err = clientNF.executeReadNoFailover(context.Background(), readOptions{},
		func(_ context.Context, _ cql.Session) error { return nil })
	require.NoError(t, err)

	assert.Equal(t, met1.get(met1.ReadTotal, ClusterA), met2.get(met2.ReadTotal, ClusterA),
		"on success, both wrappers emit IncReadTotal identically")
	assert.Equal(t, int64(0), met2.get(met2.ReadErrors, ClusterA),
		"success path must not emit IncReadError")
}

func TestExecuteReadNoFailover_EmitsIncReadError_OnPrimaryRealError(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = errors.New("primary failed")
	sessionB := newMockSession()
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}

	client, err := NewCQLClient(sessionA, sessionB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)
	require.NoError(t, err)
	defer client.Close()

	rerr := client.executeReadNoFailover(context.Background(), readOptions{},
		func(ctx context.Context, sess cql.Session) error {
			return sess.Query("SELECT 1").ScanContext(ctx)
		})
	require.Error(t, rerr)
	assert.Contains(t, rerr.Error(), "primary failed")

	assert.Equal(t, int64(1), met.get(met.ReadTotal, ClusterA))
	assert.Equal(t, int64(1), met.get(met.ReadErrors, ClusterA),
		"executeReadNoFailover must emit IncReadError for real primary errors")
}

func TestExecuteReadNoFailover_DoesNotEnterFailover_OnPrimaryRealError(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = errors.New("primary failed")
	sessionB := newMockSession()
	sessionB.scanValues = []any{"would-be-failover-success"}
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}

	client, err := NewCQLClient(sessionA, sessionB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)
	require.NoError(t, err)
	defer client.Close()

	rerr := client.executeReadNoFailover(context.Background(), readOptions{},
		func(ctx context.Context, sess cql.Session) error {
			return sess.Query("SELECT 1").ScanContext(ctx)
		})
	require.Error(t, rerr)
	assert.Empty(t, sessionB.queries,
		"executeReadNoFailover must not contact the alternative cluster")
	assert.Equal(t, int64(0), met.get(met.ReadTotal, ClusterB))
}

func TestExecuteReadNoFailover_PreservesRecordOpOutcome_AndAutoRefresh(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = errors.New("primary failed")
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)
	defer client.Close()

	rerr := client.executeReadNoFailover(context.Background(), readOptions{},
		func(ctx context.Context, sess cql.Session) error {
			return sess.Query("SELECT 1").ScanContext(ctx)
		})
	require.Error(t, rerr)

	stats := client.statsForCluster(ClusterA)
	assert.Equal(t, int32(1), stats.consecutiveFailures.Load(),
		"executeReadNoFailover must call recordOpOutcome on real errors so auto-refresh sees the failure")
}

func TestExecuteReadNoFailover_RecordFailure_DualCluster(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = errors.New("primary failed")
	sessionB := newMockSession()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: false}

	client, err := NewCQLClient(sessionA, sessionB,
		WithFailoverPolicy(policy),
	)
	require.NoError(t, err)
	defer client.Close()

	_ = client.executeReadNoFailover(context.Background(), readOptions{},
		func(ctx context.Context, sess cql.Session) error {
			return sess.Query("SELECT 1").ScanContext(ctx)
		})

	require.Len(t, policy.RecordFailureCalls, 1,
		"dual-cluster + real error must call RecordFailure exactly once")
	assert.Equal(t, ClusterA, policy.RecordFailureCalls[0])
}

func TestExecuteReadNoFailover_RecordFailure_SingleClusterPreservedBehavior(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = errors.New("primary failed")
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: false}

	client, err := NewCQLClient(sessionA, nil,
		WithFailoverPolicy(policy),
	)
	require.NoError(t, err)
	defer client.Close()

	_ = client.executeReadNoFailover(context.Background(), readOptions{},
		func(ctx context.Context, sess cql.Session) error {
			return sess.Query("SELECT 1").ScanContext(ctx)
		})

	assert.Empty(t, policy.RecordFailureCalls,
		"single-cluster mode preserves today's behavior: no RecordFailure (no failover target)")
}

func TestExecuteReadNoFailover_HonorsFallbackRead_EmptyTriggersAltProbe(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	sessionB := newMockSession()
	sessionB.scanValues = []any{"alt-data"}

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)
	defer client.Close()

	rerr := client.executeReadNoFailover(context.Background(), readOptions{fallbackRead: true},
		func(ctx context.Context, sess cql.Session) error {
			return sess.Query("SELECT 1").ScanContext(ctx)
		})
	require.NoError(t, rerr,
		"fallbackRead with empty primary must invoke executeFallbackRead and surface alt success")
	require.NotEmpty(t, sessionB.queries,
		"alt cluster must be probed for empty-retry")
}

func TestExecuteReadNoFailover_FallbackRead_SingleClusterMode_NoSecondAttempt(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	met := newReadTestMetrics()

	client, err := NewCQLClient(sessionA, nil,
		WithMetrics(met),
	)
	require.NoError(t, err)
	defer client.Close()

	queriesBefore := len(sessionA.queries)
	rerr := client.executeReadNoFailover(context.Background(), readOptions{fallbackRead: true},
		func(ctx context.Context, sess cql.Session) error {
			return sess.Query("SELECT 1").ScanContext(ctx)
		})
	require.ErrorIs(t, rerr, types.ErrNotFound,
		"single-cluster + fallbackRead returns ErrNotFound directly; no second attempt against sessionA")

	assert.Equal(t, queriesBefore+1, len(sessionA.queries),
		"sessionA must be queried exactly once — no second probe via executeFallbackRead")
	assert.Equal(t, int64(0), met.get(met.ReadTotal, ClusterB),
		"single-cluster mode must not emit ClusterB metrics")
}

func TestRecordFailure_ExactlyOnce_NormalFailoverPath(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = errors.New("primary failed")
	sessionB := newMockSession()
	sessionB.scanValues = []any{"alt-success"}
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}

	client, err := NewCQLClient(sessionA, sessionB,
		WithFailoverPolicy(policy),
	)
	require.NoError(t, err)
	defer client.Close()

	rerr := client.Query("SELECT 1").Scan()
	require.NoError(t, rerr, "alt cluster succeeded; final return is nil")

	require.Len(t, policy.RecordFailureCalls, 1,
		"normal-failover path must call RecordFailure exactly once on the failing primary")
	assert.Equal(t, ClusterA, policy.RecordFailureCalls[0])
}

func TestRecordFailure_ExactlyOnce_NoFailoverPath(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = errors.New("primary failed")
	sessionB := newMockSession()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}

	client, err := NewCQLClient(sessionA, sessionB,
		WithFailoverPolicy(policy),
	)
	require.NoError(t, err)
	defer client.Close()

	_ = client.executeReadNoFailover(context.Background(), readOptions{},
		func(ctx context.Context, sess cql.Session) error {
			return sess.Query("SELECT 1").ScanContext(ctx)
		})

	require.Len(t, policy.RecordFailureCalls, 1,
		"executeReadNoFailover must call RecordFailure exactly once — not zero, not twice")
}
