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
