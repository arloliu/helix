package helix

import (
	"errors"
	"testing"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// TestMirrorExecuteFunc_AppliesPayloadOptions asserts that the built-in
// mirror executor applies the captured consistency levels and timestamp,
// and that a non-idempotent statement takes the destination's strict path.
func TestMirrorExecuteFunc_AppliesPayloadOptions(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	replayer := &mockReplayer{}
	target, err := NewCQLClient(sa, sb, WithReplayer(replayer))
	require.NoError(t, err)
	t.Cleanup(target.Close)
	execute := mirrorExecuteFunc(target)

	quorum, serial := types.Quorum, types.LocalSerial
	require.NoError(t, execute(t.Context(), types.ReplayPayload{
		TargetCluster:     mirrorTargetCluster,
		Query:             "INSERT INTO t (id) VALUES (?)",
		Args:              []any{1},
		Timestamp:         7,
		Consistency:       &quorum,
		SerialConsistency: &serial,
	}))
	for _, s := range []*mockSession{sa, sb} {
		require.NotNil(t, s.lastQuery.consistency)
		require.EqualValues(t, types.Quorum, *s.lastQuery.consistency)
		require.NotNil(t, s.lastQuery.serialConsistency)
		require.EqualValues(t, types.LocalSerial, *s.lastQuery.serialConsistency)
		require.Equal(t, int64(7), *s.lastQuery.timestamp)
	}

	sb.execErr = errors.New("cluster B rejected the write")
	err = execute(t.Context(), types.ReplayPayload{
		TargetCluster: mirrorTargetCluster,
		Query:         "UPDATE c SET n = n + 1 WHERE id = ?",
		Args:          []any{1},
		Timestamp:     8,
		NonIdempotent: true,
	})
	var partial *types.PartialWriteError
	require.ErrorAs(t, err, &partial, "a non-idempotent statement fails strictly on the destination")
	replayer.Lock()
	defer replayer.Unlock()
	require.Empty(t, replayer.payloads, "the destination never replays a non-idempotent statement")
}
