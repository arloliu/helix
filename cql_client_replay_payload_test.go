package helix_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
)

// newReplayCaptureClient returns a client whose cluster B always fails,
// so every write leaves one payload in the returned memory replayer.
func newReplayCaptureClient(t *testing.T) (*helix.CQLClient, *replay.MemoryReplayer) {
	t.Helper()

	memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(10))
	client, err := helix.NewCQLClient(
		newAlwaysOKSession(),
		newAlwaysFailSession(errors.New("cluster B down")),
		helix.WithWriteStrategy(policy.NewSyncDualWrite()),
		helix.WithReadStrategy(policy.NewStickyRead()),
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
		helix.WithReplayer(memReplayer),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	return client, memReplayer
}

// A replay payload must keep its own copy of byte-slice arguments:
// callers routinely reuse their buffers as soon as Exec returns,
// and the replay runs later.
func TestReplayPayload_CopiesByteArgsOnFailedQuery(t *testing.T) {
	client, memReplayer := newReplayCaptureClient(t)

	blob := []byte("original")
	require.NoError(t, client.Query("INSERT INTO t (id, data) VALUES (?, ?)", "k", blob).Exec())

	copy(blob, "REWRITE!")

	payload, ok := memReplayer.TryDequeue()
	require.True(t, ok, "the failed cluster B leg must be enqueued for replay")
	require.Len(t, payload.Args, 2)
	require.Equal(t, []byte("original"), payload.Args[1])
}

func TestReplayPayload_CopiesByteArgsOnFailedBatch(t *testing.T) {
	client, memReplayer := newReplayCaptureClient(t)

	blob := []byte("original")
	batch := client.Batch(helix.LoggedBatch).Query("INSERT INTO t (id, data) VALUES (?, ?)", "k", blob)
	require.NoError(t, batch.Exec())

	copy(blob, "REWRITE!")

	payload, ok := memReplayer.TryDequeue()
	require.True(t, ok, "the failed cluster B leg must be enqueued for replay")
	require.True(t, payload.IsBatch)
	require.Len(t, payload.BatchStatements, 1)
	require.Equal(t, []byte("original"), payload.BatchStatements[0].Args[1])
}

// A payload whose target is neither configured cluster must be refused
// rather than silently executed against cluster B.
func TestDefaultExecuteFunc_RejectsUnknownTargetCluster(t *testing.T) {
	client, _ := newReplayCaptureClient(t)

	err := client.DefaultExecuteFunc()(t.Context(), types.ReplayPayload{
		TargetCluster: "C",
		Query:         "INSERT INTO t (id) VALUES (?)",
		Args:          []any{"k"},
	})
	require.ErrorIs(t, err, types.ErrInvalidCluster)
}
