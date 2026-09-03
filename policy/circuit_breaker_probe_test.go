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
// breaker transitions into the open state, including across cycles that
// pass through a probe and a close.
func TestCircuitBreaker_TripMetric_FiresOncePerTripCycle(t *testing.T) {
	mc := testutil.NewTestMetricsCollector()
	cb := policy.NewCircuitBreaker(
		policy.WithThreshold(2),
		policy.WithResetTimeout(20*time.Millisecond),
		policy.WithCircuitBreakerMetrics(mc),
	)

	// Cycle 1: trip.
	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA)
	require.EqualValues(t, 1, mc.CircuitBreakerTrips[types.ClusterA],
		"trip cycle 1 should fire metric")

	// The reset timeout elapses; a probe is reserved and fails, which is
	// not a trip.
	var token uint64
	require.Eventually(t, func() bool {
		var ok bool
		token, ok = cb.TryBeginFailoverProbe(types.ClusterA)

		return ok
	}, time.Second, time.Millisecond, "the breaker must reserve a probe once the reset timeout has elapsed")
	cb.CompleteFailoverProbe(types.ClusterA, token, types.ProbeFailed)
	assert.EqualValues(t, 1, mc.CircuitBreakerTrips[types.ClusterA], "a failed probe is not a trip")

	// Cycle 2: an operation closes the breaker, and two more failures
	// trip it again.
	cb.RecordSuccess(types.ClusterA)
	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA)
	assert.EqualValues(t, 2, mc.CircuitBreakerTrips[types.ClusterA],
		"trip cycle 2 should also fire IncCircuitBreakerTrip")
}

// TestCircuitBreaker_ShouldFailover_StaysTrueAfterResetTimeout verifies that
// the reset timeout no longer turns a caller's read into the probe: once
// tripped, ShouldFailover stays true until an operation or a client-run
// probe closes the breaker. The timeout only makes the breaker reservable.
func TestCircuitBreaker_ShouldFailover_StaysTrueAfterResetTimeout(t *testing.T) {
	cb := policy.NewCircuitBreaker(
		policy.WithThreshold(3),
		policy.WithResetTimeout(20*time.Millisecond),
	)

	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA)
	require.True(t, cb.ShouldFailover(types.ClusterA, nil), "breaker must be open after threshold")

	var token uint64
	require.Eventually(t, func() bool {
		var ok bool
		token, ok = cb.TryBeginFailoverProbe(types.ClusterA)

		return ok
	}, time.Second, time.Millisecond)
	assert.True(t, cb.ShouldFailover(types.ClusterA, nil),
		"a reserved breaker still fails over: the probe runs in the client, not on a caller's read")

	cb.CompleteFailoverProbe(types.ClusterA, token, types.ProbeSucceeded)
	assert.False(t, cb.ShouldFailover(types.ClusterA, nil), "the successful probe closed the breaker")
	assert.Equal(t, 0, cb.Failures(types.ClusterA))
}

// TestLatencyCircuitBreaker_ExternalCompositeLiteral_PointerEmbedShape is a
// compile-time and behavioral proof that LatencyCircuitBreaker's embedded
// CircuitBreaker field is *CircuitBreaker (pointer), not CircuitBreaker
// (value): the composite literal below only compiles because
// policy.NewCircuitBreaker() returns *CircuitBreaker and the field expects
// that exact pointer type — an external package building a
// LatencyCircuitBreaker via the promoted field name would fail to compile
// against a value-embedded field. Copying the resulting struct by value
// then proves the runtime consequence of that shape: only the pointer is
// copied, so the original and the copy share the same underlying breaker
// state instead of forking it (which a value embed would do, and which
// would also make `go vet`'s copylocks check fail on the embedded
// mutexes/atomics after first use).
func TestLatencyCircuitBreaker_ExternalCompositeLiteral_PointerEmbedShape(t *testing.T) {
	original := policy.LatencyCircuitBreaker{
		CircuitBreaker: policy.NewCircuitBreaker(policy.WithThreshold(1)),
	}

	valueCopy := original
	valueCopy.RecordFailure(types.ClusterA)

	assert.True(t, original.ShouldFailover(types.ClusterA, nil),
		"a value copy of LatencyCircuitBreaker must share the same underlying "+
			"*CircuitBreaker; a value-embedded CircuitBreaker would fork state "+
			"on copy instead")
}
