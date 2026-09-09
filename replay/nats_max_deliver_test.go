package replay_test

import (
	"context"
	"errors"
	"testing"

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
