package policy

import (
	"testing"
	"time"

	"github.com/arloliu/helix/types"
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
