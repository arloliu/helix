package replay_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
)

func TestMemoryWorkerStartStop(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100))
	defer replayer.Close()

	executed := make(chan types.ReplayPayload, 10)
	execute := func(_ context.Context, payload types.ReplayPayload) error {
		executed <- payload

		return nil
	}

	worker := replay.NewMemoryWorker(replayer, execute,
		replay.WithPollInterval(10*time.Millisecond),
	)

	// Start worker
	err := worker.Start()
	require.NoError(t, err)
	assert.True(t, worker.IsRunning())

	// Double start should error
	err = worker.Start()
	require.Error(t, err)

	// Stop worker
	worker.Stop()
	assert.False(t, worker.IsRunning())

	// Double stop should be safe
	worker.Stop()
}

func TestMemoryWorkerProcessesMessages(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100))
	defer replayer.Close()

	var processedCount atomic.Int32
	execute := func(_ context.Context, payload types.ReplayPayload) error {
		processedCount.Add(1)

		return nil
	}

	worker := replay.NewMemoryWorker(replayer, execute,
		replay.WithPollInterval(10*time.Millisecond),
	)

	// Enqueue messages before starting worker
	ctx := context.Background()
	for i := range 5 {
		err := replayer.Enqueue(ctx, types.ReplayPayload{
			TargetCluster: types.ClusterA,
			Query:         "INSERT test",
			Timestamp:     int64(i),
			Priority:      types.PriorityHigh,
		})
		require.NoError(t, err)
	}

	// Start worker
	err := worker.Start()
	require.NoError(t, err)

	// Wait for messages to be processed
	require.Eventually(t, func() bool {
		return processedCount.Load() == 5
	}, 2*time.Second, 10*time.Millisecond)

	worker.Stop()
}

func TestMemoryWorkerOnSuccessCallback(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100))
	defer replayer.Close()

	var successCount atomic.Int32
	execute := func(_ context.Context, _ types.ReplayPayload) error {
		return nil
	}

	worker := replay.NewMemoryWorker(replayer, execute,
		replay.WithPollInterval(10*time.Millisecond),
		replay.WithOnSuccess(func(_ types.ReplayPayload) {
			successCount.Add(1)
		}),
	)

	ctx := context.Background()
	err := replayer.Enqueue(ctx, types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT test",
		Timestamp:     1,
		Priority:      types.PriorityHigh,
	})
	require.NoError(t, err)

	err = worker.Start()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return successCount.Load() == 1
	}, 2*time.Second, 10*time.Millisecond)

	worker.Stop()
}

func TestMemoryWorkerOnErrorCallback(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100))
	defer replayer.Close()

	testErr := errors.New("execution failed")
	var errorCount atomic.Int32

	execute := func(_ context.Context, _ types.ReplayPayload) error {
		return testErr
	}

	worker := replay.NewMemoryWorker(replayer, execute,
		replay.WithPollInterval(10*time.Millisecond),
		replay.WithRetryDelay(10*time.Millisecond),
		replay.WithOnError(func(_ types.ReplayPayload, err error, _ int) {
			if errors.Is(err, testErr) {
				errorCount.Add(1)
			}
		}),
	)

	ctx := context.Background()
	err := replayer.Enqueue(ctx, types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT test",
		Timestamp:     1,
		Priority:      types.PriorityHigh,
	})
	require.NoError(t, err)

	err = worker.Start()
	require.NoError(t, err)

	// Wait for at least one error callback
	require.Eventually(t, func() bool {
		return errorCount.Load() >= 1
	}, 2*time.Second, 10*time.Millisecond)

	worker.Stop()
}

func TestMemoryWorkerGracefulShutdown(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100))
	defer replayer.Close()

	// Slow execution to test shutdown during processing
	execute := func(ctx context.Context, _ types.ReplayPayload) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			return nil
		}
	}

	worker := replay.NewMemoryWorker(replayer, execute,
		replay.WithPollInterval(10*time.Millisecond),
	)

	ctx := context.Background()
	err := replayer.Enqueue(ctx, types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT test",
		Timestamp:     1,
		Priority:      types.PriorityHigh,
	})
	require.NoError(t, err)

	err = worker.Start()
	require.NoError(t, err)

	// Give worker time to start processing
	time.Sleep(20 * time.Millisecond)

	// Stop should complete without hanging
	done := make(chan struct{})
	go func() {
		worker.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("worker.Stop() did not complete in time")
	}
}

func TestNATSWorkerStartStop(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-worker-startstop"),
		replay.WithSubjectPrefix("test.worker.ss"),
	)
	require.NoError(t, err)
	defer replayer.Close()

	execute := func(_ context.Context, _ types.ReplayPayload) error {
		return nil
	}

	worker := replay.NewNATSWorker(replayer, execute,
		replay.WithPollInterval(10*time.Millisecond),
	)

	err = worker.Start()
	require.NoError(t, err)
	assert.True(t, worker.IsRunning())

	worker.Stop()
	assert.False(t, worker.IsRunning())
}

func TestNATSWorkerProcessesMessages(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-worker-process"),
		replay.WithSubjectPrefix("test.worker.proc"),
	)
	require.NoError(t, err)
	defer replayer.Close()

	var processedA, processedB atomic.Int32
	execute := func(_ context.Context, payload types.ReplayPayload) error {
		if payload.TargetCluster == types.ClusterA {
			processedA.Add(1)
		} else {
			processedB.Add(1)
		}

		return nil
	}

	worker := replay.NewNATSWorker(replayer, execute,
		replay.WithPollInterval(10*time.Millisecond),
		replay.WithBatchSize(5),
	)

	// Enqueue messages for both clusters
	ctx := context.Background()
	for i := range 3 {
		err := replayer.Enqueue(ctx, types.ReplayPayload{
			TargetCluster: types.ClusterA,
			Query:         "INSERT A",
			Timestamp:     int64(i),
			Priority:      types.PriorityHigh,
		})
		require.NoError(t, err)
	}
	for i := range 2 {
		err := replayer.Enqueue(ctx, types.ReplayPayload{
			TargetCluster: types.ClusterB,
			Query:         "INSERT B",
			Timestamp:     int64(i + 100),
			Priority:      types.PriorityHigh,
		})
		require.NoError(t, err)
	}

	err = worker.Start()
	require.NoError(t, err)

	// Wait for all messages to be processed
	require.Eventually(t, func() bool {
		return processedA.Load() == 3 && processedB.Load() == 2
	}, 5*time.Second, 50*time.Millisecond)

	worker.Stop()
}

func TestNATSWorkerParallelProcessing(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-worker-parallel"),
		replay.WithSubjectPrefix("test.worker.par"),
	)
	require.NoError(t, err)
	defer replayer.Close()

	// Track which clusters processed messages
	var processedA, processedB atomic.Int32

	execute := func(_ context.Context, payload types.ReplayPayload) error {
		if payload.TargetCluster == types.ClusterA {
			processedA.Add(1)
		} else {
			processedB.Add(1)
		}
		// Small delay to allow concurrency
		time.Sleep(10 * time.Millisecond)

		return nil
	}

	worker := replay.NewNATSWorker(replayer, execute,
		replay.WithPollInterval(10*time.Millisecond),
	)

	// Enqueue messages for both clusters
	ctx := context.Background()
	for range 3 {
		_ = replayer.Enqueue(ctx, types.ReplayPayload{
			TargetCluster: types.ClusterA,
			Query:         "INSERT A",
			Timestamp:     time.Now().UnixNano(),
			Priority:      types.PriorityHigh,
		})
		_ = replayer.Enqueue(ctx, types.ReplayPayload{
			TargetCluster: types.ClusterB,
			Query:         "INSERT B",
			Timestamp:     time.Now().UnixNano(),
			Priority:      types.PriorityHigh,
		})
	}

	err = worker.Start()
	require.NoError(t, err)

	// Wait for all messages to be processed
	require.Eventually(t, func() bool {
		return processedA.Load() == 3 && processedB.Load() == 3
	}, 5*time.Second, 50*time.Millisecond)

	worker.Stop()

	// Both clusters should have processed messages
	assert.Equal(t, int32(3), processedA.Load())
	assert.Equal(t, int32(3), processedB.Load())
}

// TestMemoryWorkerOnDropCalledDuringShutdown verifies that the OnDrop callback
// is invoked when a payload is dropped because the worker is stopped while
// waiting for a retry backoff.
func TestMemoryWorkerOnDropCalledDuringShutdown(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100))
	defer replayer.Close()

	var dropped atomic.Int32

	worker := replay.NewMemoryWorker(replayer,
		// Always fail so that the worker enters the retry backoff path.
		func(_ context.Context, _ types.ReplayPayload) error {
			return errors.New("always fail")
		},
		replay.WithPollInterval(10*time.Millisecond),
		replay.WithRetryDelay(5*time.Second), // Long backoff to guarantee stopCh fires first
		replay.WithOnDrop(func(_ types.ReplayPayload, _ error) {
			dropped.Add(1)
		}),
	)

	// Enqueue one payload
	ctx := t.Context()
	err := replayer.Enqueue(ctx, types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT INTO test (id) VALUES (?)",
		Timestamp:     time.Now().UnixNano(),
		Priority:      types.PriorityHigh,
	})
	require.NoError(t, err)

	err = worker.Start()
	require.NoError(t, err)

	// Wait for the execute to fail and enter the backoff select.
	time.Sleep(100 * time.Millisecond)

	// Stop the worker while it's waiting in the backoff select.
	worker.Stop()

	assert.GreaterOrEqual(t, dropped.Load(), int32(1),
		"OnDrop must be called for payloads dropped during shutdown")
}

// TestMemoryWorkerQueueDrainedOnShutdown verifies that items remaining in the
// in-memory queue when the worker stops also trigger OnDrop, not only the item
// currently sitting in retry backoff. Previously, queue items were silently
// abandoned because start() exited without draining.
func TestMemoryWorkerQueueDrainedOnShutdown(t *testing.T) {
	const total = 3
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100))
	defer replayer.Close()

	var dropped atomic.Int32

	worker := replay.NewMemoryWorker(replayer,
		func(_ context.Context, _ types.ReplayPayload) error {
			return errors.New("always fail")
		},
		replay.WithPollInterval(10*time.Millisecond),
		replay.WithRetryDelay(10*time.Second), // Long backoff so item 1 stays in backoff
		replay.WithOnDrop(func(_ types.ReplayPayload, _ error) {
			dropped.Add(1)
		}),
	)

	ctx := t.Context()
	for i := range total {
		err := replayer.Enqueue(ctx, types.ReplayPayload{
			TargetCluster: types.ClusterA,
			Query:         "INSERT INTO test (id) VALUES (?)",
			Timestamp:     int64(i),
			Priority:      types.PriorityLow,
		})
		require.NoError(t, err)
	}

	err := worker.Start()
	require.NoError(t, err)

	// Wait for item 1 to fail and enter the 10s backoff.
	// Items 2 and 3 remain in the queue during this window.
	time.Sleep(100 * time.Millisecond)

	// Stop: item 1 is dropped via the backoff-stopCh path;
	// items 2 and 3 are dropped by drainAndDrop on goroutine exit.
	worker.Stop()

	assert.Equal(t, int32(total), dropped.Load(),
		"OnDrop must be called for every payload: backoff item + remaining queue items")
}
