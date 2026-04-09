package policy

import (
	"sync"
	"testing"
	"time"

	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestActiveFailoverAlwaysFailsOver(t *testing.T) {
	policy := NewActiveFailover()

	require.True(t, policy.ShouldFailover(types.ClusterA, nil))
	require.True(t, policy.ShouldFailover(types.ClusterB, nil))
}

func TestCircuitBreakerThreshold(t *testing.T) {
	policy := NewCircuitBreaker(
		WithThreshold(3),
		WithResetTimeout(1*time.Hour),
	)

	// Should not failover before threshold
	require.False(t, policy.ShouldFailover(types.ClusterA, nil))

	// Record failures
	policy.RecordFailure(types.ClusterA)
	require.Equal(t, 1, policy.Failures(types.ClusterA))
	require.False(t, policy.ShouldFailover(types.ClusterA, nil))

	policy.RecordFailure(types.ClusterA)
	require.Equal(t, 2, policy.Failures(types.ClusterA))
	require.False(t, policy.ShouldFailover(types.ClusterA, nil))

	policy.RecordFailure(types.ClusterA)
	require.Equal(t, 3, policy.Failures(types.ClusterA))
	require.True(t, policy.ShouldFailover(types.ClusterA, nil))
}

func TestCircuitBreakerSuccessResets(t *testing.T) {
	policy := NewCircuitBreaker(WithThreshold(3))

	// Record some failures
	policy.RecordFailure(types.ClusterA)
	policy.RecordFailure(types.ClusterA)
	require.Equal(t, 2, policy.Failures(types.ClusterA))

	// Success should reset
	policy.RecordSuccess(types.ClusterA)
	require.Equal(t, 0, policy.Failures(types.ClusterA))
	require.False(t, policy.ShouldFailover(types.ClusterA, nil))
}

func TestCircuitBreakerIndependentClusters(t *testing.T) {
	policy := NewCircuitBreaker(WithThreshold(2))

	// Failures on A shouldn't affect B
	policy.RecordFailure(types.ClusterA)
	policy.RecordFailure(types.ClusterA)

	require.Equal(t, 2, policy.Failures(types.ClusterA))
	require.Equal(t, 0, policy.Failures(types.ClusterB))
	require.True(t, policy.ShouldFailover(types.ClusterA, nil))
	require.False(t, policy.ShouldFailover(types.ClusterB, nil))
}

func TestCircuitBreakerResetTimeout(t *testing.T) {
	policy := NewCircuitBreaker(
		WithThreshold(3),
		WithResetTimeout(10*time.Millisecond),
	)

	// Record failures
	policy.RecordFailure(types.ClusterA)
	policy.RecordFailure(types.ClusterA)
	require.Equal(t, 2, policy.Failures(types.ClusterA))

	// Wait for reset timeout
	time.Sleep(20 * time.Millisecond)

	// Next failure should reset counter
	policy.RecordFailure(types.ClusterA)
	require.Equal(t, 1, policy.Failures(types.ClusterA))
}

// TestCircuitBreaker_ConcurrentRecordFailure_NoRace verifies that concurrent
// RecordFailure calls don't data-race on internal state. Run with -race.
func TestCircuitBreaker_ConcurrentRecordFailure_NoRace(t *testing.T) {
	cb := NewCircuitBreaker(
		WithThreshold(3),
		WithResetTimeout(1*time.Hour),
	)

	const goroutines = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			cb.RecordFailure(types.ClusterA)
		}()
	}
	wg.Wait()

	// At least one failure must have been recorded.
	assert.GreaterOrEqual(t, cb.Failures(types.ClusterA), 1)
}

// TestCircuitBreaker_ConcurrentSuccessAndFailure_NoRace verifies that interleaved
// RecordFailure and RecordSuccess calls are race-free. Run with -race.
func TestCircuitBreaker_ConcurrentSuccessAndFailure_NoRace(t *testing.T) {
	cb := NewCircuitBreaker(WithThreshold(3))

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)
	for range goroutines {
		go func() {
			defer wg.Done()
			cb.RecordFailure(types.ClusterA)
		}()
		go func() {
			defer wg.Done()
			cb.RecordSuccess(types.ClusterA)
		}()
	}
	wg.Wait()
	// Final state is non-deterministic but must be internally consistent
	// (the -race detector verifies no data races occurred).
}

// TestCircuitBreaker_MetricEmittedOnce verifies that the "circuit tripped" metric
// is emitted exactly once when the threshold is first crossed, and NOT emitted
// again on subsequent failures above the threshold.
func TestCircuitBreaker_MetricEmittedOnce(t *testing.T) {
	m := testutil.NewTestMetricsCollector()
	cb := NewCircuitBreaker(
		WithThreshold(3),
		WithResetTimeout(1*time.Hour),
		WithCircuitBreakerMetrics(m),
	)

	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA)
	assert.Equal(t, int64(0), m.CircuitBreakerTrips[types.ClusterA], "no trip before threshold")

	// Third failure crosses the threshold — metric fires exactly once.
	cb.RecordFailure(types.ClusterA)
	assert.Equal(t, int64(1), m.CircuitBreakerTrips[types.ClusterA], "trip fires at threshold")

	// Additional failures must not re-emit the trip metric.
	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA)
	assert.Equal(t, int64(1), m.CircuitBreakerTrips[types.ClusterA], "trip metric must not re-fire")

	// Success closes the circuit; state metric must reflect closed.
	cb.RecordSuccess(types.ClusterA)
	assert.Equal(t, 0, m.CircuitBreakerState[types.ClusterA], "circuit must be closed after success")

	// After recovery, a fresh trip sequence must emit the metric again.
	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA)
	assert.Equal(t, int64(2), m.CircuitBreakerTrips[types.ClusterA], "second trip after recovery must fire")
}

// TestCircuitBreaker_SetClusterNames_ThreadSafe verifies that concurrent
// SetClusterNames and RecordFailure/RecordSuccess calls are race-free. Run with -race.
func TestCircuitBreaker_SetClusterNames_ThreadSafe(t *testing.T) {
	cb := NewCircuitBreaker(WithThreshold(1))

	var wg sync.WaitGroup
	wg.Go(func() {
		for range 200 {
			cb.SetClusterNames(types.ClusterNames{A: "primary", B: "secondary"})
		}
	})
	wg.Go(func() {
		for range 200 {
			cb.RecordFailure(types.ClusterA)
		}
	})
	wg.Go(func() {
		for range 200 {
			cb.RecordSuccess(types.ClusterA)
		}
	})

	wg.Wait()
}
