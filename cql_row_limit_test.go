package helix

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSingleClusterRead_ErrRowLimitExceeded_NoIncReadError verifies that
// in single-cluster mode, ErrRowLimitExceeded propagates without
// incrementing IncReadError or calling RecordFailure.
func TestSingleClusterRead_ErrRowLimitExceeded_NoIncReadError(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrRowLimitExceeded
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}

	client, err := NewCQLClient(sessionA, nil,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)
	require.NoError(t, err)
	defer client.Close()

	scanErr := client.Query("SELECT 1").Scan()
	require.ErrorIs(t, scanErr, types.ErrRowLimitExceeded)

	assert.Equal(t, int64(1), met.get(met.ReadTotal, ClusterA),
		"the read consumed cluster resources; ReadTotal must still increment")
	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterA),
		"row-limit-exceeded is not a health signal; ReadError must not increment")
	assert.Empty(t, policy.RecordFailureCalls,
		"row-limit-exceeded must not call RecordFailure")
}

// TestExecuteRead_ErrRowLimitExceeded_NoFailover_NoIncReadError_NoRecordFailure
// verifies the dual-cluster primary error branch: ErrRowLimitExceeded
// propagates without firing failover or fallback, without IncReadError,
// and without calling RecordFailure.
func TestExecuteRead_ErrRowLimitExceeded_NoFailover_NoIncReadError_NoRecordFailure(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrRowLimitExceeded
	sessionB := newMockSession()
	// sessionB returns success — verify it is never queried.
	sessionB.scanValues = []any{"would-be-found"}

	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}

	client, err := NewCQLClient(sessionA, sessionB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)
	require.NoError(t, err)
	defer client.Close()

	// FallbackRead enabled — verify it does NOT fire on ErrRowLimitExceeded.
	scanErr := client.Query("SELECT 1").FallbackRead().Scan()
	require.ErrorIs(t, scanErr, types.ErrRowLimitExceeded,
		"row-limit-exceeded must propagate to the caller as-is")

	assert.Empty(t, sessionB.queries,
		"row-limit-exceeded must not trigger fallback or failover to cluster B")
	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterA))
	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterB))
	assert.Empty(t, policy.RecordFailureCalls,
		"row-limit-exceeded must not call RecordFailure on either cluster")
}

// TestTryFallbackCluster_ErrRowLimitExceeded_PropagatesNoDualClusterError
// verifies that when the primary fails with a real error and the fallback
// cluster returns ErrRowLimitExceeded, the sentinel propagates as-is —
// not wrapped in DualClusterError, no health recorded on the fallback.
func TestTryFallbackCluster_ErrRowLimitExceeded_PropagatesNoDualClusterError(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = errors.New("cluster A real error")
	sessionB := newMockSession()
	sessionB.scanErr = types.ErrRowLimitExceeded

	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}

	client, err := NewCQLClient(sessionA, sessionB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)
	require.NoError(t, err)
	defer client.Close()

	scanErr := client.Query("SELECT 1").Scan()
	require.ErrorIs(t, scanErr, types.ErrRowLimitExceeded,
		"fallback cluster's row-limit must propagate without being wrapped")

	var dualErr *types.DualClusterError
	assert.False(t, errors.As(scanErr, &dualErr),
		"row-limit-exceeded from fallback must NOT be wrapped in DualClusterError")

	// Cluster A's real error is health-recorded as today.
	assert.Equal(t, int64(1), met.get(met.ReadErrors, ClusterA))
	assert.Contains(t, policy.RecordFailureCalls, ClusterA)

	// Cluster B's row-limit is not a health signal.
	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterB))
	assert.NotContains(t, policy.RecordFailureCalls, ClusterB,
		"row-limit-exceeded on fallback must not call RecordFailure")
}

// TestExecuteFallbackRead_ErrRowLimitExceeded_PropagatesFromAlt verifies
// that when FallbackRead empty-retry routes to the alternative and the
// alt returns ErrRowLimitExceeded, the sentinel propagates as-is — not
// suppressed to ErrNotFound.
func TestExecuteFallbackRead_ErrRowLimitExceeded_PropagatesFromAlt(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	sessionB := newMockSession()
	sessionB.scanErr = types.ErrRowLimitExceeded

	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: false}

	client, err := NewCQLClient(sessionA, sessionB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)
	require.NoError(t, err)
	defer client.Close()

	scanErr := client.Query("SELECT 1").FallbackRead().Scan()
	require.ErrorIs(t, scanErr, types.ErrRowLimitExceeded,
		"row-limit from alt must propagate; suppressing to ErrNotFound would silently truncate")
	assert.False(t, types.IsNotFound(scanErr),
		"the sentinel must NOT be flattened to ErrNotFound")

	// Both clusters' read attempts are still counted.
	assert.Equal(t, int64(1), met.get(met.ReadTotal, ClusterA))
	assert.Equal(t, int64(1), met.get(met.ReadTotal, ClusterB))

	// Neither cluster should be recorded as a failure: A returned ErrNotFound
	// (data sentinel), B returned ErrRowLimitExceeded (data sentinel).
	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterA))
	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterB))
	assert.Empty(t, policy.RecordFailureCalls,
		"neither sentinel is a health signal; no RecordFailure calls expected")
}

// TestRecordOpOutcomeAt_ErrRowLimitExceeded_DoesNotPoisonHealth verifies
// that recordOpOutcomeAt's exclusion list covers ErrRowLimitExceeded:
// repeated ErrRowLimitExceeded results MUST NOT advance
// consecutiveFailures or trigger auto-refresh.
func TestRecordOpOutcomeAt_ErrRowLimitExceeded_DoesNotPoisonHealth(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrRowLimitExceeded
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)
	defer client.Close()

	// Drive several ErrRowLimitExceeded results.
	for range 5 {
		_ = client.Query("SELECT 1").Scan()
	}

	stats := client.statsForCluster(ClusterA)
	assert.Equal(t, int32(0), stats.consecutiveFailures.Load(),
		"row-limit-exceeded must NOT advance consecutiveFailures (data sentinel)")
	if errPtr := stats.lastErr.Load(); errPtr != nil {
		assert.NotErrorIs(t, *errPtr, types.ErrRowLimitExceeded,
			"row-limit-exceeded must NOT be stored as lastErr")
	}
}

// TestErrRowLimitExceeded_RootReExport verifies that callers can use
// the root-level helix.ErrRowLimitExceeded sentinel and helix.IsRowLimitExceeded
// helper without importing the types package.
func TestErrRowLimitExceeded_RootReExport(t *testing.T) {
	require.True(t, errors.Is(ErrRowLimitExceeded, types.ErrRowLimitExceeded),
		"helix.ErrRowLimitExceeded must alias types.ErrRowLimitExceeded")
	require.True(t, IsRowLimitExceeded(ErrRowLimitExceeded))
	require.True(t, IsRowLimitExceeded(types.ErrRowLimitExceeded))

	// Wrapped error is detected.
	wrapped := errors.New("contextual: " + types.ErrRowLimitExceeded.Error())
	require.False(t, IsRowLimitExceeded(wrapped),
		"plain string-formatted wrap is not detected — only errors.Is-compatible wrapping")
	require.False(t, IsRowLimitExceeded(types.ErrNotFound),
		"the helper must distinguish from other sentinels")
}

// TestClassifyReadErr verifies the single read classification function
// assigns each error family its own kind, attributes any error observed
// after the caller's context ended to the caller, and that only cluster
// errors count as health signals.
func TestClassifyReadErr(t *testing.T) {
	live := t.Context()
	dead, cancel := context.WithCancel(t.Context())
	cancel()

	cases := []struct {
		name string
		ctx  context.Context
		err  error
		want readErrKind
	}{
		{"nil", live, nil, readOK},
		{"not-found", live, types.ErrNotFound, readNotFound},
		{"wrapped not-found", live, fmt.Errorf("wrap: %w", types.ErrNotFound), readNotFound},
		{"row-limit", live, types.ErrRowLimitExceeded, readRowLimit},
		{"caller not-found", live, &scanFnNotFoundShieldError{err: types.ErrNotFound}, readCallerNotFound},
		{"driver timeout with live context", live, context.DeadlineExceeded, readClusterErr},
		{"cluster", live, errors.New("real cluster error"), readClusterErr},
		{"session closed", live, types.ErrSessionClosed, readClusterErr},
		{"context error after cancel", dead, context.Canceled, readCtxErr},
		{"cluster error after cancel", dead, errors.New("real cluster error"), readCtxErr},
		{"not-found after cancel", dead, types.ErrNotFound, readNotFound},
		{"success after cancel", dead, nil, readOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, classifyReadErr(tc.ctx, tc.err))
		})
	}

	require.False(t, readOK.isHealthSignal())
	require.False(t, readNotFound.isHealthSignal())
	require.False(t, readRowLimit.isHealthSignal())
	require.False(t, readCallerNotFound.isHealthSignal())
	require.False(t, readCtxErr.isHealthSignal())
	require.True(t, readClusterErr.isHealthSignal())
}
