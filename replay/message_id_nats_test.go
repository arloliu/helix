package replay_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
)

// A retried publish of the same idempotent payload is stored once, while
// two non-idempotent payloads with identical fields are both stored.
func TestNATSReplayer_EnqueueDeduplicatesIdempotentPayloads(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-msgid"),
		replay.WithSubjectPrefix("test.msgid"),
		replay.WithDuplicateWindow(time.Minute),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = replayer.Close() })

	stream, err := js.Stream(t.Context(), "test-msgid")
	require.NoError(t, err)
	require.Equal(t, time.Minute, stream.CachedInfo().Config.Duplicates, "the window reaches the stream")

	write := types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT INTO t (k, v) VALUES (?, ?)",
		Args:          []any{1, "v"},
		Timestamp:     time.Now().UnixMicro(),
	}
	require.NoError(t, replayer.Enqueue(t.Context(), write))
	require.NoError(t, replayer.Enqueue(t.Context(), write), "the duplicate is acknowledged as a success")
	pending, err := replayer.PendingByCluster(t.Context(), types.ClusterA)
	require.NoError(t, err)
	require.Equal(t, 1, pending, "the stream keeps one copy")

	counter := types.ReplayPayload{
		TargetCluster: types.ClusterB,
		Query:         "UPDATE counters SET hits = hits + 1 WHERE id = ?",
		Args:          []any{1},
		Timestamp:     write.Timestamp,
		NonIdempotent: true,
	}
	require.NoError(t, replayer.Enqueue(t.Context(), counter))
	require.NoError(t, replayer.Enqueue(t.Context(), counter))
	pending, err = replayer.PendingByCluster(t.Context(), types.ClusterB)
	require.NoError(t, err)
	require.Equal(t, 2, pending, "distinct non-idempotent writes are both stored")
}
