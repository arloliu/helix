package replay_test

import (
	"context"
	"errors"
	"sync"
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

func TestMemoryWorkerCannotRestartAfterStop(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100))
	defer replayer.Close()

	worker := replay.NewMemoryWorker(replayer,
		func(_ context.Context, _ types.ReplayPayload) error { return nil },
		replay.WithPollInterval(10*time.Millisecond),
	)

	require.NoError(t, worker.Start())
	worker.Stop()

	err := worker.Start()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "worker has been stopped")
	assert.False(t, worker.IsRunning())
}

func TestMemoryWorkerStartRejectsNilInputs(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100))
	defer replayer.Close()

	worker := replay.NewMemoryWorker(nil,
		func(_ context.Context, _ types.ReplayPayload) error { return nil },
	)
	err := worker.Start()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-nil replayer")

	worker = replay.NewMemoryWorker(replayer, nil)
	err = worker.Start()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-nil execute")
}

func TestMemoryWorkerCheckedInvalidInputsReturnJoinedErrors(t *testing.T) {
	worker, err := replay.NewMemoryWorkerChecked(nil, nil,
		replay.WithBatchSize(0),
		replay.WithPollInterval(0),
		replay.WithRetryDelay(0),
		replay.WithMaxRetryDelay(0),
		replay.WithExecuteTimeout(0),
		replay.WithMaxAttempts(0),
		replay.WithHighPriorityRatio(-1),
		replay.WithWorkerClusterNames(types.ClusterNames{A: "", B: "valid"}),
	)

	require.Nil(t, worker)
	require.Error(t, err)
	assert.True(t, types.IsOptionError(err))

	var optionErr *types.OptionError
	require.True(t, errors.As(err, &optionErr))
	assert.Contains(t, err.Error(), "replayer")
	assert.Contains(t, err.Error(), "execute")
	assert.Contains(t, err.Error(), "WithBatchSize")
	assert.Contains(t, err.Error(), "WithHighPriorityRatio")
	assert.Contains(t, err.Error(), "WithWorkerClusterNames")
}

func TestMemoryWorkerCheckedValidConfig(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100))
	defer replayer.Close()

	worker, err := replay.NewMemoryWorkerChecked(replayer,
		func(_ context.Context, _ types.ReplayPayload) error { return nil },
		replay.WithBatchSize(10),
		replay.WithPollInterval(10*time.Millisecond),
		replay.WithRetryDelay(20*time.Millisecond),
		replay.WithMaxRetryDelay(40*time.Millisecond),
		replay.WithExecuteTimeout(2*time.Second),
		replay.WithMaxAttempts(2),
		replay.WithHighPriorityRatio(0),
	)
	require.NoError(t, err)
	require.NotNil(t, worker)

	require.NoError(t, worker.Start())
	worker.Stop()
}

func TestNATSWorkerCheckedInvalidInputsReturnJoinedErrors(t *testing.T) {
	worker, err := replay.NewNATSWorkerChecked(nil, nil,
		replay.WithBatchSize(0),
		replay.WithPollInterval(0),
		replay.WithRetryDelay(0),
		replay.WithExecuteTimeout(0),
	)

	require.Nil(t, worker)
	require.Error(t, err)
	assert.True(t, types.IsOptionError(err))
	assert.Contains(t, err.Error(), "replayer")
	assert.Contains(t, err.Error(), "execute")
	assert.Contains(t, err.Error(), "WithBatchSize")
}

func TestNATSWorkerCheckedValidConfig(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-worker-checked"),
		replay.WithSubjectPrefix("test.worker.checked"),
	)
	require.NoError(t, err)
	defer replayer.Close()

	worker, err := replay.NewNATSWorkerChecked(replayer,
		func(_ context.Context, _ types.ReplayPayload) error { return nil },
		replay.WithBatchSize(10),
		replay.WithPollInterval(10*time.Millisecond),
		replay.WithRetryDelay(10*time.Millisecond),
		replay.WithMaxRetryDelay(20*time.Millisecond),
		replay.WithExecuteTimeout(2*time.Second),
		replay.WithMaxAttempts(2),
	)
	require.NoError(t, err)
	require.NotNil(t, worker)

	require.NoError(t, worker.Start())
	worker.Stop()
}

func TestWorkerStartRejectsZeroValue(t *testing.T) {
	var worker replay.Worker

	err := worker.Start()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not initialized")
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

// TestMemoryWorker_BackoffGrowsBetweenAttempts verifies that successive retry
// attempts for the same payload observe an exponentially-growing delay. The
// previous re-enqueue retry strategy collapsed every attempt to attempt=1,
// which made calculateBackoff always return the base RetryDelay regardless
// of attempt number — exponential backoff was effectively a fixed delay.
func TestMemoryWorker_BackoffGrowsBetweenAttempts(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(10))
	defer replayer.Close()

	var attemptTimes []time.Time
	var mu sync.Mutex

	worker := replay.NewMemoryWorker(replayer,
		func(_ context.Context, _ types.ReplayPayload) error {
			mu.Lock()
			attemptTimes = append(attemptTimes, time.Now())
			mu.Unlock()

			return errors.New("always fail")
		},
		replay.WithPollInterval(10*time.Millisecond),
		replay.WithRetryDelay(40*time.Millisecond),
		replay.WithMaxRetryDelay(time.Second),
		replay.WithMaxAttempts(4),
	)

	err := replayer.Enqueue(t.Context(), types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT test",
		Timestamp:     1,
		Priority:      types.PriorityHigh,
	})
	require.NoError(t, err)

	require.NoError(t, worker.Start())

	// All four attempts plus the cumulative backoff (40+80+160 = 280ms)
	// should comfortably finish under 1.5s.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return len(attemptTimes) >= 4
	}, 2*time.Second, 10*time.Millisecond, "expected MaxAttempts=4 attempts")

	worker.Stop()

	mu.Lock()
	defer mu.Unlock()

	// Gaps should grow: gap(2-1) ~= 40ms, gap(3-2) ~= 80ms, gap(4-3) ~= 160ms.
	// Allow generous slack for scheduling jitter — we only assert each gap
	// is at least ~70% of the next-smaller-power-of-2 base.
	gap1 := attemptTimes[1].Sub(attemptTimes[0])
	gap2 := attemptTimes[2].Sub(attemptTimes[1])
	gap3 := attemptTimes[3].Sub(attemptTimes[2])

	assert.GreaterOrEqual(t, gap1, 25*time.Millisecond, "first backoff ~40ms")
	assert.GreaterOrEqual(t, gap2, 55*time.Millisecond, "second backoff ~80ms")
	assert.GreaterOrEqual(t, gap3, 110*time.Millisecond, "third backoff ~160ms")
	assert.Greater(t, gap2, gap1, "backoff must grow between attempts")
	assert.Greater(t, gap3, gap2, "backoff must grow between attempts")
}

// TestMemoryWorker_MaxAttemptsBoundsRetries verifies that a permanently-failing
// payload is dropped exactly once after MaxAttempts attempts, with no
// re-enqueue (the queue stays empty).
func TestMemoryWorker_MaxAttemptsBoundsRetries(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(10))
	defer replayer.Close()

	var attempts atomic.Int32
	var dropped atomic.Int32

	worker := replay.NewMemoryWorker(replayer,
		func(_ context.Context, _ types.ReplayPayload) error {
			attempts.Add(1)

			return errors.New("always fail")
		},
		replay.WithPollInterval(5*time.Millisecond),
		replay.WithRetryDelay(5*time.Millisecond),
		replay.WithMaxRetryDelay(20*time.Millisecond),
		replay.WithMaxAttempts(3),
		replay.WithRetryPolicy(replay.RetryBounded),
		replay.WithOnDrop(func(_ types.ReplayPayload, _ error) {
			dropped.Add(1)
		}),
	)

	err := replayer.Enqueue(t.Context(), types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT test",
		Timestamp:     1,
		Priority:      types.PriorityHigh,
	})
	require.NoError(t, err)

	require.NoError(t, worker.Start())

	require.Eventually(t, func() bool {
		return dropped.Load() == 1
	}, time.Second, 5*time.Millisecond, "OnDrop must fire exactly once after MaxAttempts")

	// Give the worker a moment to demonstrate it does NOT re-enqueue.
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(3), attempts.Load(), "exactly MaxAttempts attempts, no re-enqueue")
	assert.Equal(t, 0, replayer.Len(), "queue must remain empty after drop")

	worker.Stop()
}

// TestMemoryWorker_FailingPayloadDoesNotBlockHealthy is the head-of-line
// blocking regression guard. Inline retry on the dequeue loop would stall
// the entire backlog (including unrelated-cluster work) for the full
// cumulative backoff window of a single permanently-failing payload.
// Retries must run off the dequeue path so a healthy payload enqueued
// after the failing one still progresses in bounded time.
func TestMemoryWorker_FailingPayloadDoesNotBlockHealthy(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(10))
	defer replayer.Close()

	var processedHealthy atomic.Int32
	var failingAttempts atomic.Int32

	execute := func(_ context.Context, payload types.ReplayPayload) error {
		// Cluster B = healthy; cluster A = always fails.
		if payload.TargetCluster == types.ClusterB {
			processedHealthy.Add(1)
			return nil
		}
		failingAttempts.Add(1)

		return errors.New("cluster A is broken")
	}

	worker := replay.NewMemoryWorker(replayer, execute,
		replay.WithPollInterval(5*time.Millisecond),
		// Long backoff to make any head-of-line blocking observable.
		replay.WithRetryDelay(2*time.Second),
		replay.WithMaxRetryDelay(2*time.Second),
		replay.WithMaxAttempts(3),
	)

	// Enqueue the failing payload first so it occupies the worker before
	// the healthy one arrives.
	require.NoError(t, replayer.Enqueue(t.Context(), types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT A",
		Timestamp:     1,
		Priority:      types.PriorityHigh,
	}))
	require.NoError(t, replayer.Enqueue(t.Context(), types.ReplayPayload{
		TargetCluster: types.ClusterB,
		Query:         "INSERT B",
		Timestamp:     2,
		Priority:      types.PriorityHigh,
	}))

	require.NoError(t, worker.Start())
	t.Cleanup(worker.Stop)

	// Healthy payload must be processed within ~150ms — well under the
	// 2s backoff that would block it under inline-retry semantics.
	require.Eventually(t, func() bool {
		return processedHealthy.Load() == 1
	}, 500*time.Millisecond, 5*time.Millisecond,
		"healthy payload must not be blocked behind a failing payload's retry backoff")

	// Sanity: the failing payload's first attempt has run, but its
	// retry attempts (which would take 2+ seconds with the backoff
	// configured) have NOT yet run.
	assert.Equal(t, int32(1), failingAttempts.Load(),
		"failing payload should have attempted exactly once before healthy ran; "+
			"if this is >1 the dequeue loop blocked on retry sleep")
}

// TestMemoryWorker_RetryPoolSaturationDropsImmediately verifies that
// when the retry-goroutine pool is full, further failures drop
// immediately instead of stalling the dequeue loop. This is the
// safety valve that prevents a sustained-failure storm from using
// dequeue concurrency as an unbounded retry queue.
func TestMemoryWorker_RetryPoolSaturationDropsImmediately(t *testing.T) {
	const enqueueCount = 200 // intentionally exceeds the 100-slot retry pool
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(enqueueCount))
	defer replayer.Close()

	var attempts atomic.Int32
	var dropped atomic.Int32
	var saturatedDrops atomic.Int32

	execute := func(_ context.Context, _ types.ReplayPayload) error {
		attempts.Add(1)
		return errors.New("always fails")
	}

	worker := replay.NewMemoryWorker(replayer, execute,
		replay.WithPollInterval(1*time.Millisecond),
		// Long backoff so retries pile up in the goroutine pool rather
		// than completing quickly.
		replay.WithRetryDelay(10*time.Second),
		replay.WithMaxRetryDelay(10*time.Second),
		replay.WithMaxAttempts(5),
		replay.WithRetryPolicy(replay.RetryBounded),
		replay.WithOnDrop(func(_ types.ReplayPayload, err error) {
			dropped.Add(1)
			// Drops attributable to pool saturation see the original
			// first-attempt error string ("always fails"); shutdown
			// drops also see that. Both are valid for this assertion;
			// what matters is that dropped count grows beyond what
			// the 100-slot pool could absorb.
			if err != nil {
				saturatedDrops.Add(1)
			}
		}),
	)

	for range enqueueCount {
		require.NoError(t, replayer.Enqueue(t.Context(), types.ReplayPayload{
			TargetCluster: types.ClusterA,
			Query:         "INSERT",
			Timestamp:     1,
			Priority:      types.PriorityHigh,
		}))
	}

	require.NoError(t, worker.Start())

	// Wait until the dequeue loop has processed all enqueued payloads
	// (each gets one synchronous first attempt before either entering
	// retry or being saturation-dropped).
	require.Eventually(t, func() bool {
		return attempts.Load() >= int32(enqueueCount)
	}, 5*time.Second, 5*time.Millisecond,
		"dequeue loop must keep processing first attempts even when retry pool fills")

	// Saturation-drops should account for at least (enqueueCount - 100)
	// of the dropped count: the first 100 fit in the retry pool, the
	// remaining ~100 must drop immediately.
	require.Eventually(t, func() bool {
		return saturatedDrops.Load() >= int32(enqueueCount-defaultMemoryRetryConcurrencyForTest)
	}, time.Second, 5*time.Millisecond,
		"failures beyond the retry pool capacity must drop immediately rather than queue")

	worker.Stop()

	// After Stop, every payload should ultimately have been dropped
	// (saturation drops + shutdown drops of the in-flight retries).
	assert.Equal(t, int32(enqueueCount), dropped.Load(),
		"every payload should drop exactly once across saturation + shutdown paths")
}

// defaultMemoryRetryConcurrencyForTest mirrors the unexported constant in
// memory_worker.go. Update both when changing the cap.
const defaultMemoryRetryConcurrencyForTest = 100

// TestMemoryWorker_RecoveryBeforeMaxAttempts verifies that a transient failure
// recovered before MaxAttempts is exhausted yields a successful replay and
// no OnDrop call.
func TestMemoryWorker_RecoveryBeforeMaxAttempts(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(10))
	defer replayer.Close()

	var attempts atomic.Int32
	var success atomic.Int32
	var dropped atomic.Int32

	worker := replay.NewMemoryWorker(replayer,
		func(_ context.Context, _ types.ReplayPayload) error {
			n := attempts.Add(1)
			if n < 3 {
				return errors.New("transient")
			}

			return nil
		},
		replay.WithPollInterval(5*time.Millisecond),
		replay.WithRetryDelay(5*time.Millisecond),
		replay.WithMaxRetryDelay(20*time.Millisecond),
		replay.WithMaxAttempts(5),
		replay.WithOnSuccess(func(_ types.ReplayPayload) { success.Add(1) }),
		replay.WithOnDrop(func(_ types.ReplayPayload, _ error) { dropped.Add(1) }),
	)

	err := replayer.Enqueue(t.Context(), types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT test",
		Timestamp:     1,
		Priority:      types.PriorityHigh,
	})
	require.NoError(t, err)

	require.NoError(t, worker.Start())

	require.Eventually(t, func() bool {
		return success.Load() == 1
	}, time.Second, 5*time.Millisecond, "transient failure must recover within MaxAttempts")

	worker.Stop()

	assert.Equal(t, int32(3), attempts.Load(), "should stop attempting after first success")
	assert.Equal(t, int32(0), dropped.Load(), "OnDrop must not fire when payload eventually succeeds")
}
