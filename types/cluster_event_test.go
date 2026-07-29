package types_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/types"
)

func TestClusterEventKinds_DistinctAndNonEmpty(t *testing.T) {
	kinds := []types.ClusterEventKind{
		types.EventFailover,
		types.EventReadDivergence,
		types.EventCircuitBreakerOpen,
		types.EventCircuitBreakerClosed,
		types.EventWriteDegraded,
		types.EventWriteRecovered,
		types.EventDrainEntered,
		types.EventDrainExited,
		types.EventReplayDropped,
		types.EventMirrorReplayDropped,
		types.EventSessionRefreshAttempt,
		types.EventSessionRefreshSuccess,
		types.EventSessionRefreshError,
	}
	seen := make(map[types.ClusterEventKind]struct{}, len(kinds))
	for _, k := range kinds {
		require.NotEmpty(t, string(k))
		_, dup := seen[k]
		require.False(t, dup, "duplicate kind value %q", k)
		seen[k] = struct{}{}
	}
}
