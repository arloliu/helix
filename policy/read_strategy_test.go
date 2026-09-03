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

func TestStickyRead_InvalidOptionsPreserveDefaults(t *testing.T) {
	strategy := NewStickyRead(
		WithPreferredCluster(types.ClusterID("C")),
		WithStickyReadCooldown(-time.Second),
	)

	assertKnownCluster(t, strategy.Preferred())
	require.Equal(t, 5*time.Minute, strategy.failoverCooldown)
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

	// First failover should succeed and change preferred to B
	alt, shouldFailover := strategy.OnFailure(types.ClusterA, nil)
	require.True(t, shouldFailover)
	require.Equal(t, types.ClusterB, alt)
	require.Equal(t, types.ClusterB, strategy.Preferred())

	// Second failover within cooldown: should still return alternative for
	// this request, but preferred must NOT change (cooldown gates state change)
	alt, shouldFailover = strategy.OnFailure(types.ClusterB, nil)
	require.True(t, shouldFailover, "should provide failover target even within cooldown")
	require.Equal(t, types.ClusterA, alt, "alternative should be ClusterA")
	require.Equal(t, types.ClusterB, strategy.Preferred(), "preferred must not change within cooldown")
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

func TestRoundRobinRead_InvalidClusterDoesNotFailover(t *testing.T) {
	strategy := NewRoundRobinRead()

	alt, shouldFailover := strategy.OnFailure(types.ClusterID("C"), nil)
	require.False(t, shouldFailover)
	require.Empty(t, alt)
}

func assertKnownCluster(t *testing.T, cluster types.ClusterID) {
	t.Helper()
	require.True(t, cluster == types.ClusterA || cluster == types.ClusterB,
		"cluster %q must be one of the known Helix clusters", cluster)
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

// TestPrimaryOnlyRead_RecoveryTimeout_BFailure_PreservesTimer verifies that
// a transient B failure during the recovery-timeout window does not disrupt
// the recovery timer. After OnFailure(B) probes A and the caller's retry
// on A also fails, the recovery timeout still measures from the last
// OnFailure(A) timestamp.
func TestPrimaryOnlyRead_RecoveryTimeout_BFailure_PreservesTimer(t *testing.T) {
	strategy := NewPrimaryOnlyRead(WithPrimaryOnlyRecoveryTimeout(50 * time.Millisecond))

	// Failover to B
	strategy.OnFailure(types.ClusterA, nil)
	require.Equal(t, types.ClusterB, strategy.Select(t.Context()))

	// B has a transient failure while failed-over — probe A
	alt, ok := strategy.OnFailure(types.ClusterB, nil)
	require.True(t, ok)
	require.Equal(t, types.ClusterA, alt)

	// A is also down — caller gets DualClusterError.
	// Simulate: OnFailure(A) refreshes the failover timestamp.
	strategy.OnFailure(types.ClusterA, nil)

	// Immediately after, Select should return B (recovery timeout not yet elapsed)
	require.Equal(t, types.ClusterB, strategy.Select(t.Context()))

	// Wait for recovery timeout to elapse from the refreshed timestamp
	time.Sleep(60 * time.Millisecond)

	// Now Select should probe A again via the recovery-timeout path
	require.Equal(t, types.ClusterA, strategy.Select(t.Context()),
		"after recovery timeout, should probe ClusterA again")

	// If the probe succeeds, recovery completes
	strategy.OnSuccess(types.ClusterA)
	require.False(t, strategy.failedOver.Load())
	require.Equal(t, types.ClusterA, strategy.Select(t.Context()))
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

// TestPrimaryOnlyRead_FailoverB_ProbesA verifies that when ClusterB fails
// while in the failed-over state, the strategy returns ClusterA as a probe
// but does NOT reset the failover state. State is only cleared when
// OnSuccess(ClusterA) confirms A is healthy.
func TestPrimaryOnlyRead_FailoverB_ProbesA(t *testing.T) {
	strategy := NewPrimaryOnlyRead()

	// Fail over to B
	alt, ok := strategy.OnFailure(types.ClusterA, nil)
	require.True(t, ok)
	require.Equal(t, types.ClusterB, alt)
	require.True(t, strategy.failedOver.Load())

	// B fails while in failed-over state — should probe A without resetting state
	alt, ok = strategy.OnFailure(types.ClusterB, nil)
	require.True(t, ok, "should provide failover target when B fails in failed-over state")
	require.Equal(t, types.ClusterA, alt, "should probe ClusterA")
	require.True(t, strategy.failedOver.Load(), "failedOver must stay true until A is proven healthy")
	require.NotEqual(t, int64(0), strategy.nextProbeAt.Load(), "nextProbeAt must not be cleared")

	// Select still returns B because failedOver is still set
	require.Equal(t, types.ClusterB, strategy.Select(t.Context()))

	// OnSuccess(A) completes the recovery — now state resets
	strategy.OnSuccess(types.ClusterA)
	require.False(t, strategy.failedOver.Load(), "failedOver should be cleared after OnSuccess(A)")
	require.Equal(t, int64(0), strategy.nextProbeAt.Load(), "nextProbeAt should be reset after OnSuccess(A)")
	require.Equal(t, types.ClusterA, strategy.Select(t.Context()))
}

// TestPrimaryOnlyRead_FailureBNotFailedOver_NoProbe verifies that when
// ClusterB fails but we are NOT in the failed-over state (e.g., drain-state
// override routed the read to B), no failover to A is suggested.
func TestPrimaryOnlyRead_FailureBNotFailedOver_NoProbe(t *testing.T) {
	strategy := NewPrimaryOnlyRead()

	// Not failed over — B failure should not suggest A
	alt, ok := strategy.OnFailure(types.ClusterB, nil)
	require.False(t, ok, "should not failover when B fails outside failed-over state")
	require.Empty(t, alt)
}

// TestPrimaryOnlyRead_FailoverBoth_DualFailure verifies the full scenario:
// A fails → switch to B → B fails → probe A → A also fails = both down.
// Because state is never eagerly reset, failedOver stays true throughout.
func TestPrimaryOnlyRead_FailoverBoth_DualFailure(t *testing.T) {
	strategy := NewPrimaryOnlyRead()

	// A fails → failover to B
	alt, ok := strategy.OnFailure(types.ClusterA, nil)
	require.True(t, ok)
	require.Equal(t, types.ClusterB, alt)
	require.True(t, strategy.failedOver.Load())
	firstProbeAt := strategy.nextProbeAt.Load()

	// B fails → probe A (state stays failed-over)
	alt, ok = strategy.OnFailure(types.ClusterB, nil)
	require.True(t, ok)
	require.Equal(t, types.ClusterA, alt)
	require.True(t, strategy.failedOver.Load(), "failedOver must stay true — A not yet proven healthy")

	// A fails again while still failed-over — refreshes the failover timestamp
	alt, ok = strategy.OnFailure(types.ClusterA, nil)
	require.True(t, ok)
	require.Equal(t, types.ClusterB, alt)
	require.True(t, strategy.failedOver.Load())
	require.GreaterOrEqual(t, strategy.nextProbeAt.Load(), firstProbeAt,
		"nextProbeAt should be refreshed on re-failover")
}

// TestStickyRead_CooldownStillFailsOver verifies that after A→B failover,
// if B fails within the cooldown window, the alternative is still returned
// for the current request — but preferred does not change.
func TestStickyRead_CooldownStillFailsOver(t *testing.T) {
	strategy := NewStickyRead(
		WithPreferredCluster(types.ClusterA),
		WithStickyReadCooldown(1*time.Hour),
	)

	// Failover from A to B
	alt, ok := strategy.OnFailure(types.ClusterA, nil)
	require.True(t, ok)
	require.Equal(t, types.ClusterB, alt)
	require.Equal(t, types.ClusterB, strategy.Preferred())

	// B fails within cooldown — should return A for this request
	alt, ok = strategy.OnFailure(types.ClusterB, nil)
	require.True(t, ok, "should provide failover target within cooldown")
	require.Equal(t, types.ClusterA, alt, "alternative should be ClusterA")

	// Preferred must not change — cooldown gates state changes
	require.Equal(t, types.ClusterB, strategy.Preferred(),
		"preferred should remain ClusterB (cooldown prevents state change)")
}

// TestStickyRead_CooldownExpired_SwitchesPreferred verifies that after
// cooldown expires, a failure on the preferred cluster changes preferred.
func TestStickyRead_CooldownExpired_SwitchesPreferred(t *testing.T) {
	strategy := NewStickyRead(
		WithPreferredCluster(types.ClusterA),
		WithStickyReadCooldown(10*time.Millisecond),
	)

	// Failover A → B
	_, ok := strategy.OnFailure(types.ClusterA, nil)
	require.True(t, ok)
	require.Equal(t, types.ClusterB, strategy.Preferred())

	// Wait for cooldown to expire
	time.Sleep(15 * time.Millisecond)

	// B fails after cooldown — should switch preferred back to A
	alt, ok := strategy.OnFailure(types.ClusterB, nil)
	require.True(t, ok)
	require.Equal(t, types.ClusterA, alt)
	require.Equal(t, types.ClusterA, strategy.Preferred(),
		"preferred should switch to ClusterA after cooldown expires")
}

// TestStickyRead_CooldownExpiry_DoesNotProbePassively verifies that merely
// waiting for cooldown to expire does not move preferred back to the original
// cluster. StickyRead only changes preferred in response to a later failure on
// the currently preferred cluster.
func TestStickyRead_CooldownExpiry_DoesNotProbePassively(t *testing.T) {
	strategy := NewStickyRead(
		WithPreferredCluster(types.ClusterA),
		WithStickyReadCooldown(10*time.Millisecond),
	)

	_, ok := strategy.OnFailure(types.ClusterA, nil)
	require.True(t, ok)
	require.Equal(t, types.ClusterB, strategy.Preferred())

	time.Sleep(15 * time.Millisecond)

	require.Equal(t, types.ClusterB, strategy.Select(t.Context()),
		"cooldown expiry alone must not change preferred cluster")
	require.Equal(t, types.ClusterB, strategy.Preferred(),
		"preferred should remain ClusterB until a later failure triggers a switch")
}

// TestStickyRead_NonPreferredFailure_NoCooldownBypass verifies that failure
// on a non-preferred cluster does not trigger failover regardless of cooldown.
func TestStickyRead_NonPreferredFailure_NoCooldownBypass(t *testing.T) {
	strategy := NewStickyRead(
		WithPreferredCluster(types.ClusterA),
		WithStickyReadCooldown(0),
	)

	// B fails but A is preferred — no failover
	alt, ok := strategy.OnFailure(types.ClusterB, nil)
	require.False(t, ok)
	require.Empty(t, alt)
	require.Equal(t, types.ClusterA, strategy.Preferred())
}
