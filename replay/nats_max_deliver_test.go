package replay_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
)

// TestNATSReplayer_MaxDeliverIsCheckedOnlyWhereItIsUsed pins where the
// delivery budget has to be positive.
// A worker running RetryWhileRetained overwrites the consumer's MaxDeliver
// with -1, so the configured value never reaches the server and rejecting it
// at construction only blocked a setting the default policy ignores.
// Under RetryBounded the value is the budget, so it still has to be positive.
func TestNATSReplayer_MaxDeliverIsCheckedOnlyWhereItIsUsed(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	execute := func(context.Context, types.ReplayPayload) error { return nil }

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-max-deliver-unused"),
		replay.WithSubjectPrefix("test.replay.maxdeliver"),
		replay.WithMaxDeliver(0),
	)
	require.NoError(t, err, "a value the active retry policy never reads must not block construction")
	require.NotNil(t, replayer)
	t.Cleanup(func() { _ = replayer.Close() })

	// With no worker to take over the budget, MaxDeliver is still the budget,
	// and JetStream would read a non-positive one as unlimited deliveries.
	_, err = replayer.Dequeue(context.Background(), types.ClusterA, 1)
	require.Error(t, err, "a bounded consumer must never be created with a non-positive MaxDeliver")

	// The default policy is RetryWhileRetained, which overwrites the value.
	worker, err := replay.NewNATSWorkerChecked(replayer, execute)
	require.NoError(t, err)
	require.NotNil(t, worker)

	// RetryBounded reads it as the delivery budget.
	_, err = replay.NewNATSWorkerChecked(replayer, execute,
		replay.WithRetryPolicy(replay.RetryBounded),
	)
	require.Error(t, err)
	var optErr *types.OptionError
	require.True(t, errors.As(err, &optErr))
	require.Equal(t, "WithMaxDeliver", optErr.Option)

	// The unchecked constructor reports the same thing from Start.
	unchecked := replay.NewNATSWorker(replayer, execute,
		replay.WithRetryPolicy(replay.RetryBounded),
	)
	require.Error(t, unchecked.Start())
}

// TestNATSReplayer_RetainedDeliveryConsumesWithoutMaxDeliver walks a payload
// through a replayer whose MaxDeliver the checked constructor used to reject.
// Building the RetryWhileRetained worker is what installs the redelivery
// schedule the consumer takes -1 deliveries from, so this is the path that
// proves accepting the value at construction leaves a working queue rather
// than one that fails on every dequeue.
func TestNATSReplayer_RetainedDeliveryConsumesWithoutMaxDeliver(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	execute := func(context.Context, types.ReplayPayload) error { return nil }

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-max-deliver-retained"),
		replay.WithSubjectPrefix("test.replay.retained.maxdeliver"),
		replay.WithMaxDeliver(0),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = replayer.Close() })

	// The default policy is RetryWhileRetained; building the worker hands
	// the replayer the redelivery schedule its consumers use.
	_, err = replay.NewNATSWorkerChecked(replayer, execute)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, replayer.Enqueue(ctx, types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT INTO users (id) VALUES (?)",
		Args:          []any{"uuid-123"},
		Timestamp:     time.Now().UnixMicro(),
		Priority:      types.PriorityHigh,
	}))

	msgs, err := replayer.Dequeue(ctx, types.ClusterA, 10)
	require.NoError(t, err, "a retained consumer must be created without a positive MaxDeliver")
	require.Len(t, msgs, 1)
	require.NoError(t, msgs[0].Ack())
}
