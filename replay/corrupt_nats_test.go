package replay_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
)

// A message that does not decode is terminated at fetch, counted on the
// worker's collector, handed to the corrupt-message callback, and never
// fetched again.
func TestNATSWorker_CorruptMessageIsCountedAndTerminated(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	var callbackErr atomic.Pointer[error]
	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-corrupt"),
		replay.WithSubjectPrefix("test.corrupt"),
		replay.WithOnCorruptMessage(func(err error) { callbackErr.Store(&err) }),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = replayer.Close() })

	_, err = js.Publish(t.Context(), "test.corrupt.high.A", []byte("not a replay envelope"))
	require.NoError(t, err)

	mc := testutil.NewTestMetricsCollector()
	logger := testutil.NewTestLogger(t)
	var executed atomic.Int32
	worker := replay.NewNATSWorker(replayer,
		func(context.Context, types.ReplayPayload) error { executed.Add(1); return nil },
		replay.WithPollInterval(10*time.Millisecond),
		replay.WithWorkerMetrics(mc),
		replay.WithWorkerLogger(logger),
	)
	require.NoError(t, worker.Start())
	defer worker.Stop()

	require.Eventually(t, func() bool { return mc.GetReplayCorrupt(types.ClusterA) == 1 },
		5*time.Second, 20*time.Millisecond, "the decode failure is counted")
	require.Eventually(t, func() bool { return callbackErr.Load() != nil }, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		pending, err := replayer.PendingByCluster(t.Context(), types.ClusterA)

		return err == nil && pending == 0
	}, 5*time.Second, 20*time.Millisecond, "the terminated message leaves the stream")
	assert.Zero(t, executed.Load())
	assert.Zero(t, mc.GetReplayTermFailed(types.ClusterA), "the server accepted the Term")
}
