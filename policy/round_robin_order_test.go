package policy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/types"
)

// TestRoundRobinRead_StartsOnClusterA pins the first cluster a fresh
// RoundRobinRead hands out.
// A strategy that starts on ClusterB sends the very first read of every
// process to the secondary cluster, which surprises an operator reading a
// single request's route and skews a short-lived client's traffic.
func TestRoundRobinRead_StartsOnClusterA(t *testing.T) {
	strategy := NewRoundRobinRead()

	require.Equal(t, types.ClusterA, strategy.Select(context.Background()),
		"the first Select of a fresh RoundRobinRead must return ClusterA")
	require.Equal(t, types.ClusterB, strategy.Select(context.Background()))
	require.Equal(t, types.ClusterA, strategy.Select(context.Background()))
	require.Equal(t, types.ClusterB, strategy.Select(context.Background()))
}
