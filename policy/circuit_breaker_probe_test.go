package policy_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
)

// TestCircuitBreaker_TripMetric_FiresOncePerTripCycle verifies the
// observability claim that IncCircuitBreakerTrip fires once each time the
// breaker transitions into the open state — including across multi-trip
// cycles enabled by the half-open transition.
//
// Without `trippedA` being cleared on the timeout-reset branch in
// RecordFailure, this counter only increments on the FIRST trip; subsequent
// trip→half-open→probe-fail→re-trip cycles silently re-open without
// emitting a metric.
func TestCircuitBreaker_TripMetric_FiresOncePerTripCycle(t *testing.T) {
	mc := testutil.NewTestMetricsCollector()
	cb := policy.NewCircuitBreaker(
		policy.WithThreshold(2),
		policy.WithResetTimeout(50*time.Millisecond),
		policy.WithCircuitBreakerMetrics(mc),
	)

	// Cycle 1: trip.
	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA)
	require.EqualValues(t, 1, mc.CircuitBreakerTrips[types.ClusterA],
		"trip cycle 1 should fire metric")

	// Half-open window: wait past reset timeout.
	time.Sleep(100 * time.Millisecond)

	// Cycle 2: probe fails (failures reset to 1) then another failure
	// brings us to threshold again. Without clearing trippedA, the
	// !trippedA guard suppresses the second metric.
	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA)
	assert.EqualValues(t, 2, mc.CircuitBreakerTrips[types.ClusterA],
		"trip cycle 2 (after half-open timeout reset) should also fire IncCircuitBreakerTrip")
}

// TestCircuitBreaker_ShouldFailover_StaysTrueAfterResetTimeout verifies the
// SPIKE_FINDINGS §8 hypothesis: once tripped, ShouldFailover returns true
// indefinitely if no operation flows back to the cluster, even after the
// configured ResetTimeout has elapsed.
//
// This is a real-world correctness gap for read-only workloads with
// StickyRead + LatencyCircuitBreaker — once the breaker trips, all reads
// route to the survivor and the original cluster never gets a probe.
//
// Standard circuit-breaker pattern is closed → open → half-open (after
// timeout, allow ONE probe call). Helix's CircuitBreaker today only
// transitions on explicit RecordFailure / RecordSuccess.
func TestCircuitBreaker_ShouldFailover_StaysTrueAfterResetTimeout(t *testing.T) {
	cb := policy.NewCircuitBreaker(
		policy.WithThreshold(3),
		policy.WithResetTimeout(50*time.Millisecond),
	)

	// Trip the breaker on cluster A.
	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA)
	require.True(t, cb.ShouldFailover(types.ClusterA, nil), "breaker must be open after threshold")

	// Wait well past the reset timeout, with no further activity on A.
	time.Sleep(150 * time.Millisecond)

	// The half-open property: after ResetTimeout has elapsed since the
	// last failure, ShouldFailover should return false to allow a probe.
	// If this assertion fails, the breaker stays open forever for any
	// caller that has stopped routing traffic to A.
	assert.False(t, cb.ShouldFailover(types.ClusterA, nil),
		"after ResetTimeout elapses with no further activity, ShouldFailover "+
			"should return false to allow a half-open probe attempt")

	// The breaker must NOT be permanently closed by the half-open transition —
	// if the next operation fails, the breaker should reopen on the existing
	// failure count + the new RecordFailure call. We don't assert that here
	// (covered by integration tests); we just confirm probe-then-failure works.
	cb.RecordFailure(types.ClusterA)
	// Per existing semantics: lastFailure-now > resetTimeout resets failures
	// to 1, so after one new failure we should be below threshold again.
	assert.False(t, cb.ShouldFailover(types.ClusterA, nil),
		"after probe-fail, failures reset to 1 (< threshold=3); breaker should "+
			"be closed again until threshold is reached")
}
