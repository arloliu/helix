package helix

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─────────────────────────────────────────────
// Test helper types for FallbackRead tests
// ─────────────────────────────────────────────

// readTestMetrics tracks read-path metrics for FallbackRead assertions.
type readTestMetrics struct {
	sync.Mutex
	ReadTotal      map[ClusterID]int64
	ReadErrors     map[ClusterID]int64
	ReadDivergence map[ClusterID]int64
}

func newReadTestMetrics() *readTestMetrics {
	return &readTestMetrics{
		ReadTotal:      make(map[ClusterID]int64),
		ReadErrors:     make(map[ClusterID]int64),
		ReadDivergence: make(map[ClusterID]int64),
	}
}

func (m *readTestMetrics) inc(mp map[ClusterID]int64, c ClusterID) {
	m.Lock()
	defer m.Unlock()
	mp[c]++
}

func (m *readTestMetrics) get(mp map[ClusterID]int64, c ClusterID) int64 {
	m.Lock()
	defer m.Unlock()
	return mp[c]
}

func (m *readTestMetrics) IncReadTotal(c ClusterID)                     { m.inc(m.ReadTotal, c) }
func (m *readTestMetrics) IncReadError(c ClusterID)                     { m.inc(m.ReadErrors, c) }
func (m *readTestMetrics) IncReadDivergence(c ClusterID)                { m.inc(m.ReadDivergence, c) }
func (m *readTestMetrics) ObserveReadDuration(_ ClusterID, _ float64)   {}
func (m *readTestMetrics) IncWriteTotal(_ ClusterID)                    {}
func (m *readTestMetrics) IncWriteError(_ ClusterID)                    {}
func (m *readTestMetrics) IncWriteAsync(_ ClusterID)                    {}
func (m *readTestMetrics) IncWriteDropped(_ ClusterID)                  {}
func (m *readTestMetrics) ObserveWriteDuration(_ ClusterID, _ float64)  {}
func (m *readTestMetrics) IncFailoverTotal(_, _ ClusterID)              {}
func (m *readTestMetrics) SetCircuitBreakerState(_ ClusterID, _ int)    {}
func (m *readTestMetrics) IncCircuitBreakerTrip(_ ClusterID)            {}
func (m *readTestMetrics) IncReplayEnqueued(_ ClusterID)                {}
func (m *readTestMetrics) IncReplaySuccess(_ ClusterID)                 {}
func (m *readTestMetrics) IncReplayError(_ ClusterID)                   {}
func (m *readTestMetrics) IncReplayDropped(_ ClusterID)                 {}
func (m *readTestMetrics) SetReplayQueueDepth(_ ClusterID, _ int)       {}
func (m *readTestMetrics) ObserveReplayDuration(_ ClusterID, _ float64) {}
func (m *readTestMetrics) SetClusterDraining(_ ClusterID, _ bool)       {}
func (m *readTestMetrics) IncDrainModeEntered(_ ClusterID)              {}
func (m *readTestMetrics) IncDrainModeExited(_ ClusterID)               {}

// trackingFailoverPolicy records RecordFailure / RecordSuccess /
// RecordLatency calls for assertions.
type trackingFailoverPolicy struct {
	sync.Mutex
	RecordFailureCalls  []ClusterID
	RecordSuccessCalls  []ClusterID
	RecordLatencyCalls  []ClusterID
	ShouldFailoverAllow bool // what ShouldFailover returns
}

func (p *trackingFailoverPolicy) RecordFailure(cluster ClusterID) {
	p.Lock()
	defer p.Unlock()
	p.RecordFailureCalls = append(p.RecordFailureCalls, cluster)
}

func (p *trackingFailoverPolicy) RecordSuccess(cluster ClusterID) {
	p.Lock()
	defer p.Unlock()
	p.RecordSuccessCalls = append(p.RecordSuccessCalls, cluster)
}

func (p *trackingFailoverPolicy) RecordLatency(cluster ClusterID, _ any) {
	p.Lock()
	defer p.Unlock()
	p.RecordLatencyCalls = append(p.RecordLatencyCalls, cluster)
}

func (p *trackingFailoverPolicy) ShouldFailover(_ ClusterID, _ error) bool {
	p.Lock()
	defer p.Unlock()
	return p.ShouldFailoverAllow
}

// trackingReadStrategy records OnSuccess and OnFailure calls.
type trackingReadStrategy struct {
	sync.Mutex
	OnSuccessCalls []ClusterID
	OnFailureCalls []ClusterID
	preferred      ClusterID // cluster Select returns
	altCluster     ClusterID // cluster OnFailure returns
	altShouldFail  bool
}

func (s *trackingReadStrategy) Select(_ context.Context) ClusterID {
	s.Lock()
	defer s.Unlock()
	if s.preferred == "" {
		return ClusterA
	}
	return s.preferred
}

func (s *trackingReadStrategy) OnSuccess(cluster ClusterID) {
	s.Lock()
	defer s.Unlock()
	s.OnSuccessCalls = append(s.OnSuccessCalls, cluster)
}

func (s *trackingReadStrategy) OnFailure(cluster ClusterID, _ error) (ClusterID, bool) {
	s.Lock()
	defer s.Unlock()
	s.OnFailureCalls = append(s.OnFailureCalls, cluster)
	alt := s.altCluster
	if alt == "" {
		if cluster == ClusterA {
			alt = ClusterB
		} else {
			alt = ClusterA
		}
	}

	return alt, !s.altShouldFail
}

// ─────────────────────────────────────────────
// Change A: error classification tests
// ─────────────────────────────────────────────

// TestFallback_NotFoundDoesNotCallRecordFailure verifies that a not-found result
// never increments RecordFailure on the failover policy.
func TestFallback_NotFoundDoesNotCallRecordFailure(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound

	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}

	client, err := NewCQLClient(sessionA, newMockSession(),
		WithFailoverPolicy(policy),
	)
	require.NoError(t, err)
	defer client.Close()

	scanErr := client.Query("SELECT 1").Scan()
	require.ErrorIs(t, scanErr, types.ErrNotFound)
	require.Empty(t, policy.RecordFailureCalls, "not-found must not call RecordFailure")
}

// TestFallback_NotFoundDoesNotCallOnFailure verifies that a not-found result
// never invokes ReadStrategy.OnFailure.
func TestFallback_NotFoundDoesNotCallOnFailure(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound

	strategy := &trackingReadStrategy{}

	client, err := NewCQLClient(sessionA, newMockSession(),
		WithReadStrategy(strategy),
	)
	require.NoError(t, err)
	defer client.Close()

	scanErr := client.Query("SELECT 1").Scan()
	require.ErrorIs(t, scanErr, types.ErrNotFound)
	require.Empty(t, strategy.OnFailureCalls, "not-found must not call OnFailure")
}

// TestFallback_NotFoundDoesNotIncReadError verifies that a not-found result
// never increments IncReadError.
func TestFallback_NotFoundDoesNotIncReadError(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	met := newReadTestMetrics()

	client, err := NewCQLClient(sessionA, newMockSession(),
		WithMetrics(met),
	)
	require.NoError(t, err)
	defer client.Close()

	_ = client.Query("SELECT 1").Scan()

	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterA),
		"not-found must not increment ReadError")
}

// TestFallback_NotFoundRecordsReadTotal verifies that not-found reads still
// increment IncReadTotal (the read consumed real cluster resources).
func TestFallback_NotFoundRecordsReadTotal(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	met := newReadTestMetrics()

	client, err := NewCQLClient(sessionA, newMockSession(),
		WithMetrics(met),
	)
	require.NoError(t, err)
	defer client.Close()

	_ = client.Query("SELECT 1").Scan()

	assert.Equal(t, int64(1), met.get(met.ReadTotal, ClusterA),
		"not-found must still increment ReadTotal")
}

// TestFallback_NoFallback_NotFoundReturnsImmediately verifies that without
// FallbackRead, a not-found result is returned directly with no retry.
func TestFallback_NoFallback_NotFoundReturnsImmediately(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	// sessionB would return success — we verify it is never called
	sessionB := newMockSession()
	sessionB.scanValues = []any{"found"}

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)
	defer client.Close()

	scanErr := client.Query("SELECT 1").Scan()

	require.ErrorIs(t, scanErr, types.ErrNotFound)
	// sessionB was never queried
	require.Empty(t, sessionB.queries, "alternative cluster must not be tried without FallbackRead")
}

// TestFallback_SingleCluster_NotFoundDoesNotIncReadError verifies error classification
// in single-cluster mode.
func TestFallback_SingleCluster_NotFoundDoesNotIncReadError(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	met := newReadTestMetrics()

	// Single-cluster client (nil second session)
	client, err := NewCQLClient(sessionA, nil,
		WithMetrics(met),
	)
	require.NoError(t, err)
	defer client.Close()

	scanErr := client.Query("SELECT 1").Scan()

	require.ErrorIs(t, scanErr, types.ErrNotFound)
	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterA),
		"single-cluster: not-found must not increment ReadError")
	assert.Equal(t, int64(1), met.get(met.ReadTotal, ClusterA),
		"single-cluster: not-found must still increment ReadTotal")
}

// ─────────────────────────────────────────────
// Change B: FallbackRead behavior tests
// ─────────────────────────────────────────────

// TestFallback_FirstNotFound_SecondHasData verifies the primary success path:
// cluster A returns not-found, cluster B has the data → FallbackRead returns nil.
func TestFallback_FirstNotFound_SecondHasData(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	sessionB := newMockSession()
	// sessionB.scanErr = nil (success)

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)
	defer client.Close()

	scanErr := client.Query("SELECT 1").FallbackRead().Scan()
	require.NoError(t, scanErr, "FallbackRead must succeed when alternative has data")
}

// TestFallback_BothNotFound_ReturnsNotFound verifies that when both clusters
// return not-found, ErrNotFound is returned (definitively absent).
func TestFallback_BothNotFound_ReturnsNotFound(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	sessionB := newMockSession()
	sessionB.scanErr = types.ErrNotFound

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)
	defer client.Close()

	scanErr := client.Query("SELECT 1").FallbackRead().Scan()

	require.Error(t, scanErr)
	assert.True(t, types.IsNotFound(scanErr), "both not-found must return ErrNotFound")
}

// TestFallback_FirstNotFound_SecondRealError_ReturnsNotFound verifies that when
// the primary (healthy) cluster returns not-found and the alternative cluster
// returns a real error, the caller receives ErrNotFound — not the alternative's
// error. The primary already confirmed the row is absent; returning the
// alternative's error would make FallbackRead decrease availability.
func TestFallback_FirstNotFound_SecondRealError_ReturnsNotFound(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	sessionB := newMockSession()
	sessionB.scanErr = errors.New("cluster B connection refused")

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)
	defer client.Close()

	scanErr := client.Query("SELECT 1").FallbackRead().Scan()

	require.Error(t, scanErr)
	assert.True(t, types.IsNotFound(scanErr),
		"primary healthy not-found must be returned even when alternative has a real error")
}

// TestFallback_FirstRealError_NormalFailoverPath verifies that a real error from
// the primary cluster enters the normal health-based failover path, NOT the
// FallbackRead not-found path.
func TestFallback_FirstRealError_NormalFailoverPath(t *testing.T) {
	sessionA := newMockSession()
	realErr := errors.New("cluster A timeout")
	sessionA.scanErr = realErr
	sessionB := newMockSession()
	// sessionB returns success

	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}

	client, err := NewCQLClient(sessionA, sessionB,
		WithFailoverPolicy(policy),
	)
	require.NoError(t, err)
	defer client.Close()

	// With or without FallbackRead, a real error on A should go through the normal
	// failover path, which succeeds via B.
	scanErr := client.Query("SELECT 1").FallbackRead().Scan()

	require.NoError(t, scanErr, "normal failover must recover via cluster B")
	require.Len(t, policy.RecordFailureCalls, 1,
		"real error must call RecordFailure once (normal failover path)")
	assert.Equal(t, ClusterA, policy.RecordFailureCalls[0])
}

// TestFallback_PrimaryError_FailoverNotFound_ReturnsNotFound verifies that
// when the primary cluster has a real error and the failover cluster returns
// not-found, the caller receives ErrNotFound — the healthy cluster's answer.
// In an AP system, a healthy cluster's response is authoritative. Returning the
// primary's downtime error would fail the request for rows that genuinely do not
// exist, breaking availability during single-cluster outages.
func TestFallback_PrimaryError_FailoverNotFound_ReturnsNotFound(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = errors.New("cluster A timeout")
	sessionB := newMockSession()
	sessionB.scanErr = types.ErrNotFound

	fp := &trackingFailoverPolicy{ShouldFailoverAllow: true}

	client, err := NewCQLClient(sessionA, sessionB,
		WithFailoverPolicy(fp),
	)
	require.NoError(t, err)
	defer client.Close()

	scanErr := client.Query("SELECT 1").Scan()

	require.Error(t, scanErr)
	assert.True(t, types.IsNotFound(scanErr),
		"healthy cluster's not-found must be returned to preserve availability")
	// Must NOT be a DualClusterError.
	var dce *types.DualClusterError
	assert.False(t, errors.As(scanErr, &dce),
		"must not return DualClusterError when failover returned not-found")
}

// TestFallback_PrimaryError_FailoverNotFound_NoHealthImpactOnFailover verifies
// that when the failover cluster correctly returns not-found, its health state
// is not poisoned — no IncReadError, no RecordFailure.
func TestFallback_PrimaryError_FailoverNotFound_NoHealthImpactOnFailover(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = errors.New("cluster A timeout")
	sessionB := newMockSession()
	sessionB.scanErr = types.ErrNotFound

	fp := &trackingFailoverPolicy{ShouldFailoverAllow: true}
	met := newReadTestMetrics()

	client, err := NewCQLClient(sessionA, sessionB,
		WithFailoverPolicy(fp),
		WithMetrics(met),
	)
	require.NoError(t, err)
	defer client.Close()

	_ = client.Query("SELECT 1").Scan()

	// Cluster A had a real error — RecordFailure is expected.
	require.Len(t, fp.RecordFailureCalls, 1)
	assert.Equal(t, ClusterA, fp.RecordFailureCalls[0],
		"only the primary (real error) should have RecordFailure")

	// Cluster B returned not-found — no health impact.
	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterB),
		"failover not-found must NOT increment ReadError on cluster B")

	// Cluster A should have ReadError (it had a real failure).
	assert.Equal(t, int64(1), met.get(met.ReadErrors, ClusterA),
		"primary cluster must have ReadError")
}

// TestFallback_FirstHasData_NoFallbackAttempted verifies that when cluster A
// has the data, FallbackRead never tries cluster B.
func TestFallback_FirstHasData_NoFallbackAttempted(t *testing.T) {
	sessionA := newMockSession()
	// sessionA.scanErr = nil (success)
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)
	defer client.Close()

	scanErr := client.Query("SELECT 1").FallbackRead().Scan()

	require.NoError(t, scanErr)
	require.Empty(t, sessionB.queries, "cluster B must not be tried when A has data")
}

// TestFallback_SingleCluster_FallbackRead_ReturnsNotFound verifies that in
// single-cluster mode FallbackRead is a no-op (no alternative cluster to try).
func TestFallback_SingleCluster_FallbackRead_ReturnsNotFound(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound

	client, err := NewCQLClient(sessionA, nil)
	require.NoError(t, err)
	defer client.Close()

	require.NotPanics(t, func() {
		scanErr := client.Query("SELECT 1").FallbackRead().Scan()
		assert.True(t, types.IsNotFound(scanErr))
	}, "single-cluster FallbackRead must not panic")
}

// ─────────────────────────────────────────────
// Metrics verification tests
// ─────────────────────────────────────────────

// TestFallback_DivergenceMetricOnSuccess verifies that IncReadDivergence is called
// on the stale cluster when FallbackRead finds data on the alternative cluster.
func TestFallback_DivergenceMetricOnSuccess(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	sessionB := newMockSession()
	// sessionB returns success
	met := newReadTestMetrics()

	client, err := NewCQLClient(sessionA, sessionB,
		WithMetrics(met),
	)
	require.NoError(t, err)
	defer client.Close()

	scanErr := client.Query("SELECT 1").FallbackRead().Scan()
	require.NoError(t, scanErr)

	// Divergence is recorded on the stale cluster (A), not the one that had data
	assert.Equal(t, int64(1), met.get(met.ReadDivergence, ClusterA),
		"IncReadDivergence must be called on the stale cluster")
	assert.Equal(t, int64(0), met.get(met.ReadDivergence, ClusterB),
		"IncReadDivergence must not be called on the cluster that had data")
}

// TestFallback_DivergenceEventOnSuccess is the event-stream counterpart of
// TestFallback_DivergenceMetricOnSuccess: a registered cluster-event handler
// must receive an EventReadDivergence naming the stale cluster.
func TestFallback_DivergenceEventOnSuccess(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	sessionB := newMockSession()
	// sessionB returns success
	rec := newInternalEventRecorder()

	client, err := NewCQLClient(sessionA, sessionB,
		WithOnClusterEvent(rec.handler),
	)
	require.NoError(t, err)
	defer client.Close()

	scanErr := client.Query("SELECT 1").FallbackRead().Scan()
	require.NoError(t, scanErr)

	ev := rec.waitFor(t, func(ev types.ClusterEvent) bool {
		return ev.Kind == types.EventReadDivergence
	})
	assert.Equal(t, ClusterA, ev.Cluster, "the event must name the cluster that was missing the row")
	assert.NotEmpty(t, ev.Reason)
	assert.False(t, ev.Timestamp.IsZero())
}

// TestFallback_ReadTotalBothClustersRecorded verifies that IncReadTotal is called
// for both the primary attempt and the fallback attempt.
func TestFallback_ReadTotalBothClustersRecorded(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	sessionB := newMockSession()
	// sessionB returns success
	met := newReadTestMetrics()

	client, err := NewCQLClient(sessionA, sessionB,
		WithMetrics(met),
	)
	require.NoError(t, err)
	defer client.Close()

	_ = client.Query("SELECT 1").FallbackRead().Scan()

	assert.Equal(t, int64(1), met.get(met.ReadTotal, ClusterA),
		"primary read attempt must be counted")
	assert.Equal(t, int64(1), met.get(met.ReadTotal, ClusterB),
		"fallback read attempt must be counted")
}

// TestFallback_AltRealError_RecordFailureCalled verifies that when the fallback
// cluster returns a real error, RecordFailure and IncReadError are still called
// on it — even though the caller receives ErrNotFound (the primary's healthy answer).
func TestFallback_AltRealError_RecordFailureCalled(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	sessionB := newMockSession()
	sessionB.scanErr = errors.New("cluster B connection refused")

	fp := &trackingFailoverPolicy{ShouldFailoverAllow: false}
	met := newReadTestMetrics()

	client, err := NewCQLClient(sessionA, sessionB,
		WithFailoverPolicy(fp),
		WithMetrics(met),
	)
	require.NoError(t, err)
	defer client.Close()

	scanErr := client.Query("SELECT 1").FallbackRead().Scan()
	require.Error(t, scanErr)
	assert.True(t, types.IsNotFound(scanErr),
		"primary healthy not-found must be returned to preserve availability")

	// RecordFailure must be called on cluster B (the one that had the real error)
	require.Len(t, fp.RecordFailureCalls, 1)
	assert.Equal(t, ClusterB, fp.RecordFailureCalls[0])

	// IncReadError must be incremented on cluster B
	assert.Equal(t, int64(1), met.get(met.ReadErrors, ClusterB))
	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterA),
		"cluster A returned not-found, must not increment ReadError")
}

// TestFallback_BothNotFound_NoHealthImpact verifies that when both clusters
// return not-found via FallbackRead, no health state is modified.
func TestFallback_BothNotFound_NoHealthImpact(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	sessionB := newMockSession()
	sessionB.scanErr = types.ErrNotFound

	policy := &trackingFailoverPolicy{ShouldFailoverAllow: false}
	met := newReadTestMetrics()

	client, err := NewCQLClient(sessionA, sessionB,
		WithFailoverPolicy(policy),
		WithMetrics(met),
	)
	require.NoError(t, err)
	defer client.Close()

	scanErr := client.Query("SELECT 1").FallbackRead().Scan()
	require.True(t, types.IsNotFound(scanErr))

	assert.Empty(t, policy.RecordFailureCalls, "neither cluster failed; no RecordFailure")
	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterA))
	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterB))
}

// ─────────────────────────────────────────────
// Drain state bypass tests
// ─────────────────────────────────────────────

// TestFallback_DrainStateBypass verifies that FallbackRead attempts the alternative
// cluster even when it is in drain mode (drain state is bypassed for fallback).
func TestFallback_DrainStateBypass(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	sessionB := newMockSession()
	// sessionB returns success (has data despite being draining)

	watcher := newMockTopologyWatcher()
	watcher.drainB = true // cluster B is draining

	client, err := NewCQLClient(sessionA, sessionB,
		WithTopologyWatcher(watcher),
	)
	require.NoError(t, err)
	defer client.Close()

	// Without FallbackRead: A returns not-found, no fallback
	scanErr := client.Query("SELECT 1").Scan()
	require.ErrorIs(t, scanErr, types.ErrNotFound)
	require.Empty(t, sessionB.queries, "cluster B must not be tried without FallbackRead")

	// Reset queries
	sessionB.queries = nil

	// With FallbackRead: should bypass drain state and find data on B
	scanErr = client.Query("SELECT 1").FallbackRead().Scan()
	require.NoError(t, scanErr,
		"FallbackRead must bypass drain state and find data on B")
	require.NotEmpty(t, sessionB.queries, "cluster B must be tried with FallbackRead even while draining")
}

// ─────────────────────────────────────────────
// Chaining and precedence tests
// ─────────────────────────────────────────────

// TestFallback_ChainPreservesFlag verifies that FallbackRead() flag persists
// through query chaining regardless of order.
func TestFallback_ChainPreservesFlag(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	sessionB := newMockSession()
	// sessionB returns success

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)
	defer client.Close()

	tests := []struct {
		name  string
		query Query
	}{
		{
			"FallbackRead then Consistency",
			client.Query("SELECT 1").FallbackRead().Consistency(One),
		},
		{
			"Consistency then FallbackRead",
			client.Query("SELECT 1").Consistency(One).FallbackRead(),
		},
		{
			"FallbackRead then PageSize then Consistency",
			client.Query("SELECT 1").FallbackRead().PageSize(10).Consistency(Quorum),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sessionB.queries = nil // reset
			scanErr := tt.query.Scan()
			require.NoError(t, scanErr,
				"FallbackRead flag must persist through chaining")
			require.NotEmpty(t, sessionB.queries,
				"cluster B must be tried (flag persisted through chain)")
		})
	}
}

// TestFallback_WithDefaultFallbackRead_True_EnablesForAll verifies that
// WithDefaultFallbackRead(true) enables FallbackRead for all queries.
func TestFallback_WithDefaultFallbackRead_True_EnablesForAll(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	sessionB := newMockSession()
	// sessionB returns success

	client, err := NewCQLClient(sessionA, sessionB,
		WithDefaultFallbackRead(true),
	)
	require.NoError(t, err)
	defer client.Close()

	// No per-query FallbackRead() — client default should activate it
	scanErr := client.Query("SELECT 1").Scan()

	require.NoError(t, scanErr, "client-level default must enable FallbackRead")
	require.NotEmpty(t, sessionB.queries, "cluster B must be tried via client default")
}

// TestFallback_WithDefaultFallbackRead_False_DoesNotEnable verifies the default
// behaviour: WithDefaultFallbackRead(false) (or no option) disables fallback.
func TestFallback_WithDefaultFallbackRead_False_DoesNotEnable(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB,
		WithDefaultFallbackRead(false),
	)
	require.NoError(t, err)
	defer client.Close()

	scanErr := client.Query("SELECT 1").Scan()

	require.ErrorIs(t, scanErr, types.ErrNotFound)
	require.Empty(t, sessionB.queries, "cluster B must not be tried when default is false")
}

// TestFallback_WithFallbackRead_Ctx_Enables verifies context-level activation.
func TestFallback_WithFallbackRead_Ctx_Enables(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)
	defer client.Close()

	ctx := WithFallbackRead(t.Context())
	scanErr := client.Query("SELECT 1").ScanContext(ctx)

	require.NoError(t, scanErr, "context-level FallbackRead must enable fallback")
	require.NotEmpty(t, sessionB.queries, "cluster B must be tried via context")
}

// TestFallback_WithFallbackRead_Ctx_NotSet_DoesNotEnable verifies that a context
// without WithFallbackRead does not enable fallback.
func TestFallback_WithFallbackRead_Ctx_NotSet_DoesNotEnable(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)
	defer client.Close()

	scanErr := client.Query("SELECT 1").ScanContext(t.Context())

	require.ErrorIs(t, scanErr, types.ErrNotFound)
	require.Empty(t, sessionB.queries, "cluster B must not be tried without context FallbackRead")
}

// ─────────────────────────────────────────────
// Precedence tests
// ─────────────────────────────────────────────

// TestFallback_Precedence_QueryOverCtxAndDefault verifies that per-query
// FallbackRead() takes precedence over context and client default.
func TestFallback_Precedence_QueryOverCtxAndDefault(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	sessionB := newMockSession()
	// sessionB returns success

	// No context FallbackRead, no client default — only per-query
	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)
	defer client.Close()

	// Per-query FallbackRead should win
	scanErr := client.Query("SELECT 1").FallbackRead().ScanContext(t.Context())
	require.NoError(t, scanErr)
	require.NotEmpty(t, sessionB.queries, "per-query FallbackRead must activate fallback")
}

// TestFallback_Precedence_CtxOverDefault verifies that context-level FallbackRead
// overrides (or supplements) the client default.
func TestFallback_Precedence_CtxOverDefault(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	sessionB := newMockSession()

	// Client default is false, but context enables it
	client, err := NewCQLClient(sessionA, sessionB,
		WithDefaultFallbackRead(false),
	)
	require.NoError(t, err)
	defer client.Close()

	ctx := WithFallbackRead(t.Context())
	scanErr := client.Query("SELECT 1").ScanContext(ctx)

	require.NoError(t, scanErr, "context FallbackRead must override client default=false")
	require.NotEmpty(t, sessionB.queries)
}

// TestFallback_Precedence_ClientDefaultWhenNeitherQueryNorCtx verifies that
// client default activates when neither per-query nor context is set.
func TestFallback_Precedence_ClientDefaultWhenNeitherQueryNorCtx(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB,
		WithDefaultFallbackRead(true),
	)
	require.NoError(t, err)
	defer client.Close()

	// Plain context (no WithFallbackRead), no per-query flag
	scanErr := client.Query("SELECT 1").ScanContext(t.Context())

	require.NoError(t, scanErr)
	require.NotEmpty(t, sessionB.queries,
		"client default must activate when neither query nor context enables fallback")
}

// ─────────────────────────────────────────────
// MapScan variant tests
// ─────────────────────────────────────────────

// TestFallback_MapScan_FirstNotFound_SecondHasData verifies FallbackRead works
// with MapScan/MapScanContext just like Scan/ScanContext.
func TestFallback_MapScan_FirstNotFound_SecondHasData(t *testing.T) {
	sessionA := newMockSession()
	sessionA.scanErr = types.ErrNotFound
	sessionB := newMockSession()
	// sessionB returns success

	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)
	defer client.Close()

	result := make(map[string]any)
	scanErr := client.Query("SELECT 1").FallbackRead().MapScan(result)
	require.NoError(t, scanErr, "FallbackRead must work with MapScan")
}

// TestFallback_IsNotFound_HelperExposedOnPackage verifies that the package-level
// IsNotFound helper re-exports types.IsNotFound correctly.
func TestFallback_IsNotFound_HelperExposedOnPackage(t *testing.T) {
	assert.True(t, IsNotFound(ErrNotFound))
	assert.True(t, IsNotFound(types.ErrNotFound))
	assert.False(t, IsNotFound(errors.New("other")))
	assert.False(t, IsNotFound(nil))
}

// ─────────────────────────────────────────────
// Single-cluster mode tests
// ─────────────────────────────────────────────

// TestSingleCluster_IterContext_OnSuccessReceivesClusterA verifies that when a
// single-cluster client (sessionB == nil) is configured with a ReadStrategy
// whose Select returns ClusterB, IterContext still records OnSuccess(ClusterA).
// This guards against the bug where cqlIter.cluster could be set to ClusterB
// even though getSession always redirects to sessionA in single-cluster mode.
func TestSingleCluster_IterContext_OnSuccessReceivesClusterA(t *testing.T) {
	sessionA := newMockSession()

	// Strategy always picks ClusterB — must be overridden by IsSingleCluster guard.
	strategy := &trackingReadStrategy{preferred: ClusterB}

	client, err := NewCQLClient(sessionA, nil,
		WithReadStrategy(strategy),
	)
	require.NoError(t, err)
	defer client.Close()

	iter := client.Query("SELECT 1").IterContext(context.Background())
	require.NoError(t, iter.Close())

	strategy.Lock()
	successCalls := make([]ClusterID, len(strategy.OnSuccessCalls))
	copy(successCalls, strategy.OnSuccessCalls)
	strategy.Unlock()

	require.Len(t, successCalls, 1, "OnSuccess must be called exactly once")
	assert.Equal(t, ClusterA, successCalls[0],
		"single-cluster mode must report ClusterA to OnSuccess even when Select returns ClusterB")
}
