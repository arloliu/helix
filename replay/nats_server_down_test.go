package replay_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/internal/test/testutil"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
)

// TestNATSReplayer_EnqueueFailsWithinPublishTimeoutWhileServerDown pins
// what a write path sees while the NATS server is unreachable:
// Enqueue gives up after PublishTimeout with a deadline error
// rather than waiting for the client to reconnect,
// and the same replayer publishes again once the server is back.
//
// The failed Enqueue is not proof the payload was lost.
// The client holds the publish in its reconnect buffer
// and sends it once the server is back,
// so the stream stores it even though Enqueue reported an error.
func TestNATSReplayer_EnqueueFailsWithinPublishTimeoutWhileServerDown(t *testing.T) {
	const publishTimeout = 300 * time.Millisecond

	ns := testutil.StartRestartableNATS(t)
	reconnected := make(chan struct{}, 1)
	js := ns.Connect(t, nats.ReconnectHandler(func(*nats.Conn) {
		select {
		case reconnected <- struct{}{}:
		default:
		}
	}))

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-server-down"),
		replay.WithSubjectPrefix("test.server.down"),
		replay.WithPublishTimeout(publishTimeout),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = replayer.Close() })

	payload := types.ReplayPayload{
		TargetCluster: types.ClusterB,
		Query:         "INSERT INTO t (id) VALUES (?)",
		Args:          []any{1},
		Timestamp:     time.Now().UnixMicro(),
		Priority:      types.PriorityHigh,
	}
	require.NoError(t, replayer.Enqueue(t.Context(), payload))

	ns.Shutdown()

	// The caller's own deadline is far past PublishTimeout,
	// so an Enqueue that ignored PublishTimeout would run into it instead.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	start := time.Now()
	during := payload
	during.Args = []any{2}
	err = replayer.Enqueue(ctx, during)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, ctx.Err(), "the caller's deadline must not be what ended the publish")
	require.GreaterOrEqual(t, elapsed, publishTimeout)
	require.Less(t, elapsed, 2*time.Second)

	ns.Restart()
	select {
	case <-reconnected:
	case <-time.After(10 * time.Second):
		t.Fatal("client did not reconnect to the restarted server")
	}
	after := payload
	after.Args = []any{3}
	require.NoError(t, replayer.Enqueue(t.Context(), after))

	pending, err := replayer.PendingByCluster(t.Context(), types.ClusterB)
	require.NoError(t, err)
	require.Equal(t, 3, pending,
		"the stream holds the message from before the outage, the one whose Enqueue failed, and the one after")
}
