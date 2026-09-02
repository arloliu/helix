package helix

import (
	"errors"
	"testing"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// TestReplay_PayloadCarriesConsistency asserts that a partial failure
// enqueues the consistency levels the write used, and nothing when the
// write used the session default.
func TestReplay_PayloadCarriesConsistency(t *testing.T) {
	sessionA, sessionB := newMockSession(), newMockSession()
	sessionB.execErr = errors.New("cluster B down")
	replayer := &mockReplayer{}
	client, err := NewCQLClient(sessionA, sessionB, WithReplayer(replayer))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.NoError(t, client.Query("INSERT INTO t (id) VALUES (?)", 1).
		Consistency(types.Quorum).SerialConsistency(types.LocalSerial).Exec())
	require.NoError(t, client.Query("INSERT INTO t (id) VALUES (?)", 2).Exec())
	require.NoError(t, client.Batch(LoggedBatch).Query("INSERT INTO t (id) VALUES (?)", 3).
		Consistency(types.All).Exec())

	require.Len(t, replayer.payloads, 3)
	withLevels, plain, batch := replayer.payloads[0], replayer.payloads[1], replayer.payloads[2]
	require.NotNil(t, withLevels.Consistency)
	require.Equal(t, types.Quorum, *withLevels.Consistency)
	require.NotNil(t, withLevels.SerialConsistency)
	require.Equal(t, types.LocalSerial, *withLevels.SerialConsistency)
	require.Nil(t, plain.Consistency, "a session-default write carries no level")
	require.Nil(t, plain.SerialConsistency)
	require.NotNil(t, batch.Consistency)
	require.Equal(t, types.All, *batch.Consistency)
	require.Nil(t, batch.SerialConsistency)
}

// TestDefaultExecuteFunc_AppliesPayloadConsistency asserts that a replayed
// write is executed at the level recorded in the payload.
func TestDefaultExecuteFunc_AppliesPayloadConsistency(t *testing.T) {
	sessionA, sessionB := newMockSession(), newMockSession()
	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	execute := client.DefaultExecuteFunc()

	quorum := types.Quorum
	require.NoError(t, execute(t.Context(), types.ReplayPayload{
		TargetCluster: ClusterB,
		Query:         "INSERT INTO t (id) VALUES (?)",
		Args:          []any{1},
		Timestamp:     7,
		Consistency:   &quorum,
	}))
	require.NotNil(t, sessionB.lastQuery.consistency)
	require.Equal(t, types.Quorum, *sessionB.lastQuery.consistency)
	require.Nil(t, sessionB.lastQuery.serialConsistency)

	require.NoError(t, execute(t.Context(), types.ReplayPayload{
		TargetCluster: ClusterA,
		Query:         "INSERT INTO t (id) VALUES (?)",
		Args:          []any{2},
		Timestamp:     7,
	}))
	require.Nil(t, sessionA.lastQuery.consistency, "no recorded level leaves the session default in place")
}
