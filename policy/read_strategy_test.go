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
