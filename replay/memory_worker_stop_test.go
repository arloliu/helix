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

// stopWaitTimeout bounds how long the test lets Worker.Stop run.
// It is far below the ExecuteTimeout of the attempt in flight,
// so a Stop that waits for the attempt fails the test instead of passing slowly.
const stopWaitTimeout = 2 * time.Second

// TestMemoryWorker_StopCancelsInFlightExecute proves Worker.Stop - and the
// client Close that waits on it - is not held for ExecuteTimeout by an
// attempt in flight, and that the payload whose attempt Stop interrupts is
// settled as a shutdown drop rather than as a failed attempt.
//
// Both retry policies are covered because they settle a failed attempt on
// different paths: RetryWhileRetained through settleRetained, RetryBounded
// through handleFirstAttempt.
func TestMemoryWorker_StopCancelsInFlightExecute(t *testing.T) {
	tests := []struct {
		name string
		opts []replay.WorkerOption
	}{
		{name: "retained", opts: []replay.WorkerOption{replay.WithRetryPolicy(replay.RetryWhileRetained)}},
		{
			name: "bounded",
			opts: []replay.WorkerOption{replay.WithRetryPolicy(replay.RetryBounded), replay.WithMaxAttempts(1)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(1))
			defer replayer.Close()
			mc := testutil.NewTestMetricsCollector()

			entered := make(chan struct{}, 1)
			var failures atomic.Int32
			dropped := make(chan error, 2)

			opts := append([]replay.WorkerOption{
				replay.WithExecuteTimeout(5 * time.Second), // well past the bound Stop is held to below
				replay.WithPollInterval(5 * time.Millisecond),
				replay.WithWorkerMetrics(mc),
				replay.WithOnError(func(types.ReplayPayload, error, int) { failures.Add(1) }),
				replay.WithOnDrop(func(_ types.ReplayPayload, err error) { dropped <- err }),
			}, tt.opts...)

			worker := replay.NewMemoryWorker(replayer, func(ctx context.Context, _ types.ReplayPayload) error {
				select {
				case entered <- struct{}{}:
				default:
				}
				<-ctx.Done()

				return ctx.Err()
			}, opts...)

			enqueueN(t, replayer, 1, types.ClusterA)
			require.NoError(t, worker.Start())

			select {
			case <-entered:
			case <-time.After(stopWaitTimeout):
				t.Fatal("the worker never executed the enqueued payload")
			}

			stopped := make(chan struct{})
			go func() {
				worker.Stop()
				close(stopped)
			}()
			select {
			case <-stopped:
			case <-time.After(stopWaitTimeout):
				t.Fatal("Stop waited for the attempt in flight instead of cancelling it")
			}

			require.Len(t, dropped, 1, "the interrupted payload is settled exactly once")
			require.ErrorIs(t, <-dropped, context.Canceled)
			assert.Equal(t, int64(1), mc.GetReplayWorkerDropped(types.ClusterA, types.ReplayDropShutdown),
				"the interrupted attempt is dropped as a shutdown, not as an exhausted or dead-lettered payload")
			assert.Zero(t, failures.Load(), "an attempt Stop cancelled is not a failed attempt")
			assert.Equal(t, 0, replayer.Len(), "the interrupted payload releases its slot")
		})
	}
}
