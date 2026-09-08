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

// A NULL blob argument ([]byte(nil)) in a batch must stay NULL on
// replay, mirroring TestReplayPayload_PreservesNilByteArgOnFailedQuery
// for the batch path.
func TestReplayPayload_PreservesNilByteArgOnFailedBatch(t *testing.T) {
	client, memReplayer := newReplayCaptureClient(t)

	batch := client.Batch(helix.LoggedBatch).Query("INSERT INTO t (id, data) VALUES (?, ?)", "k", []byte(nil))
	require.NoError(t, batch.Exec())

	payload, ok := memReplayer.TryDequeue()
	require.True(t, ok, "the failed cluster B leg must be enqueued for replay")
	require.True(t, payload.IsBatch)
	require.Len(t, payload.BatchStatements, 1)
	b, ok := payload.BatchStatements[0].Args[1].([]byte)
	require.True(t, ok, "arg must stay typed as []byte")
	require.Nil(t, b, "a nil []byte arg must remain nil, not become an empty slice")
}

// A non-nil empty blob argument ([]byte{}) in a batch must stay non-nil
// and empty; pins the other direction for the batch path.
func TestReplayPayload_PreservesEmptyByteArgOnFailedBatch(t *testing.T) {
	client, memReplayer := newReplayCaptureClient(t)

	batch := client.Batch(helix.LoggedBatch).Query("INSERT INTO t (id, data) VALUES (?, ?)", "k", []byte{})
	require.NoError(t, batch.Exec())

	payload, ok := memReplayer.TryDequeue()
	require.True(t, ok, "the failed cluster B leg must be enqueued for replay")
	require.True(t, payload.IsBatch)
	require.Len(t, payload.BatchStatements, 1)
	b, ok := payload.BatchStatements[0].Args[1].([]byte)
	require.True(t, ok, "arg must stay typed as []byte")
	require.NotNil(t, b, "a non-nil empty []byte arg must stay non-nil")
	require.Empty(t, b)
}

// A NULL blob argument ([]byte(nil)) must stay NULL on replay, not turn
// into an empty (zero-length) blob: the driver encodes a nil []byte as
// NULL and an empty []byte as a zero-length value, so collapsing one
// into the other changes what gets written to the recovering cluster.
func TestReplayPayload_PreservesNilByteArgOnFailedQuery(t *testing.T) {
	client, memReplayer := newReplayCaptureClient(t)

	require.NoError(t, client.Query("INSERT INTO t (id, data) VALUES (?, ?)", "k", []byte(nil)).Exec())

	payload, ok := memReplayer.TryDequeue()
	require.True(t, ok, "the failed cluster B leg must be enqueued for replay")
	require.Len(t, payload.Args, 2)
	b, ok := payload.Args[1].([]byte)
	require.True(t, ok, "arg must stay typed as []byte")
	require.Nil(t, b, "a nil []byte arg must remain nil, not become an empty slice")
}

// A non-nil empty blob argument ([]byte{}) must stay non-nil and empty:
// this pins the other direction so a fix for the nil case does not
// collapse both into nil.
func TestReplayPayload_PreservesEmptyByteArgOnFailedQuery(t *testing.T) {
	client, memReplayer := newReplayCaptureClient(t)

	require.NoError(t, client.Query("INSERT INTO t (id, data) VALUES (?, ?)", "k", []byte{}).Exec())

	payload, ok := memReplayer.TryDequeue()
	require.True(t, ok, "the failed cluster B leg must be enqueued for replay")
	require.Len(t, payload.Args, 2)
	b, ok := payload.Args[1].([]byte)
	require.True(t, ok, "arg must stay typed as []byte")
	require.NotNil(t, b, "a non-nil empty []byte arg must stay non-nil")
	require.Empty(t, b)
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
