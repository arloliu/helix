package policy

import (
	"context"
	"testing"
	"time"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

func TestStickyReadSelect(t *testing.T) {
	strategy := NewStickyRead(WithPreferredCluster(types.ClusterA))

	selected := strategy.Select(context.Background())
	require.Equal(t, types.ClusterA, selected)

	// Should remain sticky
	for range 10 {
		require.Equal(t, types.ClusterA, strategy.Select(context.Background()))
	}
}

func TestStickyReadFailover(t *testing.T) {
	strategy := NewStickyRead(
		WithPreferredCluster(types.ClusterA),
		WithStickyReadCooldown(0), // Disable cooldown for testing
	)

	// Initial selection should be A
	require.Equal(t, types.ClusterA, strategy.Select(context.Background()))

	// Simulate failure on A
	alternative, shouldFailover := strategy.OnFailure(types.ClusterA, nil)
	require.True(t, shouldFailover)
	require.Equal(t, types.ClusterB, alternative)

	// After failover, preferred should be B
	require.Equal(t, types.ClusterB, strategy.Preferred())
}

func TestStickyReadFailoverCooldown(t *testing.T) {
	strategy := NewStickyRead(
		WithPreferredCluster(types.ClusterA),
		WithStickyReadCooldown(1*time.Hour), // Long cooldown
	)

	// First failover should succeed
	_, shouldFailover := strategy.OnFailure(types.ClusterA, nil)
	require.True(t, shouldFailover)

	// Second failover should be blocked by cooldown
	_, shouldFailover = strategy.OnFailure(types.ClusterB, nil)
	require.False(t, shouldFailover)
}

func TestStickyReadNoFailoverOnSecondaryFailure(t *testing.T) {
	strategy := NewStickyRead(
		WithPreferredCluster(types.ClusterA),
		WithStickyReadCooldown(0),
	)

	// Failure on non-preferred cluster should not trigger failover
	_, shouldFailover := strategy.OnFailure(types.ClusterB, nil)
	require.False(t, shouldFailover)

	// Preferred should still be A
	require.Equal(t, types.ClusterA, strategy.Preferred())
}

func TestPrimaryOnlyRead(t *testing.T) {
	strategy := NewPrimaryOnlyRead()

	// Should always select A initially
	require.Equal(t, types.ClusterA, strategy.Select(context.Background()))

	// Simulate failure
	alt, shouldFailover := strategy.OnFailure(types.ClusterA, nil)
	require.True(t, shouldFailover)
	require.Equal(t, types.ClusterB, alt)

	// After failover, should select B
	require.Equal(t, types.ClusterB, strategy.Select(context.Background()))

	// Reset should go back to A
	strategy.Reset()
	require.Equal(t, types.ClusterA, strategy.Select(context.Background()))
}

func TestRoundRobinRead(t *testing.T) {
	strategy := NewRoundRobinRead()

	// Should alternate between A and B
	first := strategy.Select(context.Background())
	second := strategy.Select(context.Background())
	third := strategy.Select(context.Background())

	require.NotEqual(t, first, second)
	require.Equal(t, first, third) // Should cycle back
}

func TestRoundRobinReadFailover(t *testing.T) {
	strategy := NewRoundRobinRead()

	// Failover should always return the other cluster
	alt, shouldFailover := strategy.OnFailure(types.ClusterA, nil)
	require.True(t, shouldFailover)
	require.Equal(t, types.ClusterB, alt)

	alt, shouldFailover = strategy.OnFailure(types.ClusterB, nil)
	require.True(t, shouldFailover)
	require.Equal(t, types.ClusterA, alt)
}

// TestPrimaryOnlyRead_AutoRecovery_ReturnsToA verifies that after the recovery
// timeout elapses, Select probes ClusterA again.
func TestPrimaryOnlyRead_AutoRecovery_ReturnsToA(t *testing.T) {
	strategy := NewPrimaryOnlyRead(WithPrimaryOnlyRecoveryTimeout(50 * time.Millisecond))

	// Fail over to B
	alt, ok := strategy.OnFailure(types.ClusterA, nil)
	require.True(t, ok)
	require.Equal(t, types.ClusterB, alt)
	require.Equal(t, types.ClusterB, strategy.Select(context.Background()))

	// Wait for recovery timeout to elapse
	time.Sleep(60 * time.Millisecond)

	// Select should now probe ClusterA
	require.Equal(t, types.ClusterA, strategy.Select(context.Background()),
		"after recovery timeout, Select should return ClusterA as a probe")
}

// TestPrimaryOnlyRead_AutoRecovery_SuccessCompletesRecovery verifies that a
// successful read on ClusterA during a probe clears the failed-over state.
func TestPrimaryOnlyRead_AutoRecovery_SuccessCompletesRecovery(t *testing.T) {
	strategy := NewPrimaryOnlyRead(WithPrimaryOnlyRecoveryTimeout(50 * time.Millisecond))

	strategy.OnFailure(types.ClusterA, nil)
	time.Sleep(60 * time.Millisecond)

	// Probe succeeds
	strategy.OnSuccess(types.ClusterA)

	// Strategy should now permanently prefer ClusterA again (no timeout needed)
	require.Equal(t, types.ClusterA, strategy.Select(context.Background()))
	require.False(t, strategy.failedOver.Load(), "failedOver should be cleared after successful probe")
}

// TestPrimaryOnlyRead_AutoRecovery_FailureResetsTimer verifies that a failed
// probe resets the recovery timer — the next probe only happens after another
// full recovery timeout.
func TestPrimaryOnlyRead_AutoRecovery_FailureResetsTimer(t *testing.T) {
	strategy := NewPrimaryOnlyRead(WithPrimaryOnlyRecoveryTimeout(50 * time.Millisecond))

	strategy.OnFailure(types.ClusterA, nil)
	time.Sleep(60 * time.Millisecond)

	// Probe selects A, but read fails again
	require.Equal(t, types.ClusterA, strategy.Select(context.Background()))
	strategy.OnFailure(types.ClusterA, nil)

	// Timer reset — should be on B again immediately
	require.Equal(t, types.ClusterB, strategy.Select(context.Background()),
		"after failed probe, should return to ClusterB immediately")

	// Wait for another recovery window
	time.Sleep(60 * time.Millisecond)
	require.Equal(t, types.ClusterA, strategy.Select(context.Background()),
		"after second recovery timeout, should probe ClusterA again")
}

// TestPrimaryOnlyRead_NoRecoveryTimeout_StaysOnB verifies that without a
// recovery timeout, failover is permanent until Reset is called.
func TestPrimaryOnlyRead_NoRecoveryTimeout_StaysOnB(t *testing.T) {
	strategy := NewPrimaryOnlyRead() // no timeout

	strategy.OnFailure(types.ClusterA, nil)
	time.Sleep(20 * time.Millisecond)

	// Should remain on B regardless of time elapsed
	require.Equal(t, types.ClusterB, strategy.Select(context.Background()))

	strategy.Reset()
	require.Equal(t, types.ClusterA, strategy.Select(context.Background()))
}
