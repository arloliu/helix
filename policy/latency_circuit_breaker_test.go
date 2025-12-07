package policy

import (
	"testing"
	"time"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLatencyCircuitBreaker_Defaults(t *testing.T) {
	lcb := NewLatencyCircuitBreaker()

	assert.Equal(t, 2*time.Second, lcb.AbsoluteMax())
	// Verify default behavior: should NOT fail over until 3 failures
	lcb.RecordFailure(types.ClusterA)
	lcb.RecordFailure(types.ClusterA)
	assert.False(t, lcb.ShouldFailover(types.ClusterA, nil), "should not failover before threshold")
	lcb.RecordFailure(types.ClusterA)
	assert.True(t, lcb.ShouldFailover(types.ClusterA, nil), "should failover after 3 failures")
}

func TestLatencyCircuitBreaker_CustomOptions(t *testing.T) {
	lcb := NewLatencyCircuitBreaker(
		WithLatencyAbsoluteMax(500*time.Millisecond),
		WithLatencyThreshold(5),
		WithLatencyResetTimeout(10*time.Second),
	)

	assert.Equal(t, 500*time.Millisecond, lcb.AbsoluteMax())
	// Verify custom threshold: should NOT fail over until 5 failures
	for i := 0; i < 4; i++ {
		lcb.RecordFailure(types.ClusterA)
	}
	assert.False(t, lcb.ShouldFailover(types.ClusterA, nil), "should not failover before threshold=5")
	lcb.RecordFailure(types.ClusterA)
	assert.True(t, lcb.ShouldFailover(types.ClusterA, nil), "should failover after 5 failures")
}

func TestLatencyCircuitBreaker_RecordLatency_BelowThreshold(t *testing.T) {
	lcb := NewLatencyCircuitBreaker(
		WithLatencyAbsoluteMax(1*time.Second),
		WithLatencyThreshold(3),
	)

	// Record latencies below threshold - should call RecordSuccess
	lcb.RecordLatency(types.ClusterA, 100*time.Millisecond)
	lcb.RecordLatency(types.ClusterA, 200*time.Millisecond)
	lcb.RecordLatency(types.ClusterA, 500*time.Millisecond)

	// Should not trigger failover
	assert.False(t, lcb.ShouldFailover(types.ClusterA, nil))
	assert.Equal(t, 0, lcb.Failures(types.ClusterA))
}

func TestLatencyCircuitBreaker_RecordLatency_AboveThreshold(t *testing.T) {
	lcb := NewLatencyCircuitBreaker(
		WithLatencyAbsoluteMax(1*time.Second),
		WithLatencyThreshold(3),
	)

	// Record latencies above threshold - should count as failures
	lcb.RecordLatency(types.ClusterA, 1500*time.Millisecond)
	assert.Equal(t, 1, lcb.Failures(types.ClusterA))

	lcb.RecordLatency(types.ClusterA, 2000*time.Millisecond)
	assert.Equal(t, 2, lcb.Failures(types.ClusterA))

	lcb.RecordLatency(types.ClusterA, 3000*time.Millisecond)
	assert.Equal(t, 3, lcb.Failures(types.ClusterA))

	// Now should trigger failover
	assert.True(t, lcb.ShouldFailover(types.ClusterA, nil))
}

func TestLatencyCircuitBreaker_MixedLatencies(t *testing.T) {
	lcb := NewLatencyCircuitBreaker(
		WithLatencyAbsoluteMax(1*time.Second),
		WithLatencyThreshold(3),
	)

	// Two slow, then one fast (resets counter)
	lcb.RecordLatency(types.ClusterA, 1500*time.Millisecond)
	lcb.RecordLatency(types.ClusterA, 2000*time.Millisecond)
	assert.Equal(t, 2, lcb.Failures(types.ClusterA))

	lcb.RecordLatency(types.ClusterA, 500*time.Millisecond) // Fast - resets
	assert.Equal(t, 0, lcb.Failures(types.ClusterA))

	// Should not trigger failover
	assert.False(t, lcb.ShouldFailover(types.ClusterA, nil))
}

func TestLatencyCircuitBreaker_ErrorsAndLatency(t *testing.T) {
	lcb := NewLatencyCircuitBreaker(
		WithLatencyAbsoluteMax(1*time.Second),
		WithLatencyThreshold(3),
	)

	// Mix of hard failures and slow responses
	lcb.RecordFailure(types.ClusterA)                        // Hard failure
	lcb.RecordLatency(types.ClusterA, 1500*time.Millisecond) // Slow (soft failure)
	lcb.RecordFailure(types.ClusterA)                        // Hard failure

	assert.Equal(t, 3, lcb.Failures(types.ClusterA))
	assert.True(t, lcb.ShouldFailover(types.ClusterA, nil))
}

func TestLatencyCircuitBreaker_IndependentClusters(t *testing.T) {
	lcb := NewLatencyCircuitBreaker(
		WithLatencyAbsoluteMax(1*time.Second),
		WithLatencyThreshold(2),
	)

	// Cluster A is slow
	lcb.RecordLatency(types.ClusterA, 1500*time.Millisecond)
	lcb.RecordLatency(types.ClusterA, 2000*time.Millisecond)

	// Cluster B is fast
	lcb.RecordLatency(types.ClusterB, 100*time.Millisecond)
	lcb.RecordLatency(types.ClusterB, 200*time.Millisecond)

	// Only A should trigger failover
	assert.True(t, lcb.ShouldFailover(types.ClusterA, nil))
	assert.False(t, lcb.ShouldFailover(types.ClusterB, nil))
}

func TestLatencyCircuitBreaker_ImplementsFailoverPolicy(t *testing.T) {
	// Verify LatencyCircuitBreaker can be used where FailoverPolicy is expected
	var _ interface {
		ShouldFailover(types.ClusterID, error) bool
		RecordFailure(types.ClusterID)
		RecordSuccess(types.ClusterID)
	} = NewLatencyCircuitBreaker()
}

func TestLatencyCircuitBreaker_ResetTimeout(t *testing.T) {
	lcb := NewLatencyCircuitBreaker(
		WithLatencyAbsoluteMax(100*time.Millisecond),
		WithLatencyThreshold(3),
		WithLatencyResetTimeout(50*time.Millisecond),
	)

	// Accumulate some failures
	lcb.RecordLatency(types.ClusterA, 200*time.Millisecond)
	lcb.RecordLatency(types.ClusterA, 200*time.Millisecond)
	require.Equal(t, 2, lcb.Failures(types.ClusterA))

	// Wait for reset timeout
	time.Sleep(60 * time.Millisecond)

	// Next failure should reset counter to 1
	lcb.RecordLatency(types.ClusterA, 200*time.Millisecond)
	assert.Equal(t, 1, lcb.Failures(types.ClusterA))
}
