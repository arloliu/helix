package replay_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
)

// unreachableFor returns an ExecuteFunc that reports the cluster as
// unreachable until the deadline, then succeeds, counting every attempt.
func unreachableFor(d time.Duration, attempts, successes *atomic.Int32) replay.ExecuteFunc {
	deadline := time.Now().Add(d)

	return func(_ context.Context, _ types.ReplayPayload) error {
		attempts.Add(1)
		if time.Now().Before(deadline) {
			return fmt.Errorf("%w: pool empty", types.ErrClusterUnreachable)
		}
		successes.Add(1)

		return nil
	}
}

// enqueuer is the enqueue half of both replayers.
type enqueuer interface {
	Enqueue(ctx context.Context, payload types.ReplayPayload) error
}

func enqueueN(t *testing.T, replayer enqueuer, n int, cluster types.ClusterID) {
	t.Helper()
	for i := range n {
		require.NoError(t, replayer.Enqueue(t.Context(), types.ReplayPayload{
			TargetCluster: cluster,
			Query:         "INSERT test",
			Timestamp:     time.Now().UnixMicro(),
			Priority:      types.PriorityHigh,
		}), "enqueue %d", i)
	}
}

// Under RetryWhileRetained an outage far longer than the bounded retry
// budget loses nothing: every payload keeps its slot, keeps retrying, and
// is replayed once the cluster returns.
func TestMemoryWorker_RetainedPolicySurvivesOutage(t *testing.T) {
	const payloads = 150 // more than the retry pool, so waiting must not drop

	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(payloads))
	defer replayer.Close()
	mc := testutil.NewTestMetricsCollector()

	var attempts, successes, dropped atomic.Int32
	worker := replay.NewMemoryWorker(replayer,
		unreachableFor(300*time.Millisecond, &attempts, &successes),
		replay.WithRetryPolicy(replay.RetryWhileRetained),
		replay.WithPollInterval(5*time.Millisecond),
		replay.WithRetryDelay(5*time.Millisecond),
		replay.WithMaxRetryDelay(20*time.Millisecond),
		replay.WithMaxAttempts(2), // the bounded policy would give up after ~5ms
		replay.WithWorkerMetrics(mc),
		replay.WithOnDrop(func(types.ReplayPayload, error) { dropped.Add(1) }),
	)

	enqueueN(t, replayer, payloads, types.ClusterB)
	require.NoError(t, worker.Start())

	// While the cluster is down the backlog stays visible: slots are held.
	require.Eventually(t, func() bool { return attempts.Load() >= payloads }, 2*time.Second, 5*time.Millisecond)
	assert.Equal(t, payloads, replayer.PendingByCluster(types.ClusterB), "waiting payloads must keep their slots")
	assert.Equal(t, payloads, mc.GetReplayQueueDepth(types.ClusterB), "queue depth gauge must count waiting payloads")

	require.Eventually(t, func() bool { return successes.Load() == payloads }, 5*time.Second, 10*time.Millisecond)
	worker.Stop()

	assert.Equal(t, int32(0), dropped.Load(), "no payload may be dropped during an outage")
	assert.Equal(t, 0, replayer.Len(), "slots must be released after success")
	assert.Equal(t, int64(0), mc.GetReplayDropped(types.ClusterB))
}

// A new admission fails loudly while the whole capacity is held by waiting
// payloads, instead of silently evicting or dropping them.
func TestMemoryWorker_RetainedPolicyHoldsCapacity(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(2))
	defer replayer.Close()

	var attempts, successes atomic.Int32
	worker := replay.NewMemoryWorker(replayer,
		unreachableFor(time.Hour, &attempts, &successes),
		replay.WithRetryPolicy(replay.RetryWhileRetained),
		replay.WithPollInterval(5*time.Millisecond),
		replay.WithRetryDelay(50*time.Millisecond),
		replay.WithMaxRetryDelay(50*time.Millisecond),
	)
	enqueueN(t, replayer, 2, types.ClusterA)
	require.NoError(t, worker.Start())
	defer worker.Stop()

	require.Eventually(t, func() bool { return attempts.Load() >= 2 }, 2*time.Second, 5*time.Millisecond)
	err := replayer.Enqueue(t.Context(), types.ReplayPayload{TargetCluster: types.ClusterA, Query: "INSERT"})
	require.ErrorIs(t, err, types.ErrReplayQueueFull)
}

// Only attempts the classifier marks dead-letter consume the poison budget;
// unreachable attempts before them do not count.
func TestMemoryWorker_RetainedPolicyDeadLettersAfterBudget(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(4))
	defer replayer.Close()
	mc := testutil.NewTestMetricsCollector()

	var attempts atomic.Int32
	dropped := make(chan error, 1)
	worker := replay.NewMemoryWorker(replayer,
		func(_ context.Context, _ types.ReplayPayload) error {
			if attempts.Add(1) <= 3 {
				return fmt.Errorf("%w: pool empty", types.ErrClusterUnreachable)
			}

			return fmt.Errorf("%w: replay target %q", types.ErrInvalidCluster, "C")
		},
		replay.WithRetryPolicy(replay.RetryWhileRetained),
		replay.WithPollInterval(5*time.Millisecond),
		replay.WithRetryDelay(time.Millisecond),
		replay.WithMaxRetryDelay(5*time.Millisecond),
		replay.WithMaxAttempts(2),
		replay.WithWorkerMetrics(mc),
		replay.WithOnDrop(func(_ types.ReplayPayload, err error) { dropped <- err }),
	)
	enqueueN(t, replayer, 1, types.ClusterA)
	require.NoError(t, worker.Start())
	defer worker.Stop()

	select {
	case err := <-dropped:
		require.ErrorIs(t, err, types.ErrInvalidCluster)
	case <-time.After(2 * time.Second):
		t.Fatal("payload was not dead-lettered")
	}
	assert.Equal(t, int32(5), attempts.Load(), "3 deferred attempts plus 2 dead-letter attempts")
	assert.Equal(t, int64(1), mc.GetReplayWorkerDropped(types.ClusterA, types.ReplayDropDeadLetter))
	assert.Equal(t, 0, replayer.Len(), "slot must be released after the drop")
}

// The retry window bounds how long an unreachable cluster is waited for.
func TestMemoryWorker_RetainedPolicyExpiresRetryWindow(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(4))
	defer replayer.Close()
	mc := testutil.NewTestMetricsCollector()

	var attempts, successes atomic.Int32
	dropped := make(chan error, 1)
	worker := replay.NewMemoryWorker(replayer,
		unreachableFor(time.Hour, &attempts, &successes),
		replay.WithRetryPolicy(replay.RetryWhileRetained),
		replay.WithRetryWindow(40*time.Millisecond),
		replay.WithPollInterval(5*time.Millisecond),
		replay.WithRetryDelay(5*time.Millisecond),
		replay.WithMaxRetryDelay(5*time.Millisecond),
		replay.WithWorkerMetrics(mc),
		replay.WithOnDrop(func(_ types.ReplayPayload, err error) { dropped <- err }),
	)
	enqueueN(t, replayer, 1, types.ClusterA)
	require.NoError(t, worker.Start())
	defer worker.Stop()

	select {
	case err := <-dropped:
		require.ErrorIs(t, err, types.ErrClusterUnreachable)
	case <-time.After(2 * time.Second):
		t.Fatal("payload was not dropped when the window expired")
	}
	assert.Equal(t, int64(1), mc.GetReplayWorkerDropped(types.ClusterA, types.ReplayDropRetryWindowExpired))
	assert.Equal(t, 0, replayer.Len())
}

// Stopping the worker reports every waiting payload through OnDrop and
// releases its slot.
func TestMemoryWorker_RetainedPolicyShutdownDropsWaiting(t *testing.T) {
	const payloads = 5
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(payloads))
	defer replayer.Close()
	mc := testutil.NewTestMetricsCollector()

	var attempts, successes, dropped atomic.Int32
	worker := replay.NewMemoryWorker(replayer,
		unreachableFor(time.Hour, &attempts, &successes),
		replay.WithRetryPolicy(replay.RetryWhileRetained),
		replay.WithPollInterval(5*time.Millisecond),
		replay.WithRetryDelay(time.Hour), // park every payload in the scheduler
		replay.WithMaxRetryDelay(time.Hour),
		replay.WithWorkerMetrics(mc),
		replay.WithOnDrop(func(types.ReplayPayload, error) { dropped.Add(1) }),
	)
	enqueueN(t, replayer, payloads, types.ClusterA)
	require.NoError(t, worker.Start())
	require.Eventually(t, func() bool { return attempts.Load() == payloads }, 2*time.Second, 5*time.Millisecond)

	worker.Stop()

	assert.Equal(t, int32(payloads), dropped.Load())
	assert.Equal(t, int64(payloads), mc.GetReplayWorkerDropped(types.ClusterA, types.ReplayDropShutdown))
	assert.Equal(t, 0, replayer.Len(), "every slot must be released on shutdown")
}

// A custom classifier can dead-letter errors the default one would retry.
func TestMemoryWorker_RetainedPolicyCustomClassifier(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(4))
	defer replayer.Close()

	poison := errors.New("syntax error")
	dropped := make(chan error, 1)
	worker := replay.NewMemoryWorker(replayer,
		func(context.Context, types.ReplayPayload) error { return poison },
		replay.WithRetryPolicy(replay.RetryWhileRetained),
		replay.WithReplayClassifier(func(err error) replay.ReplayDisposition {
			if errors.Is(err, poison) {
				return replay.DispositionDeadLetter
			}

			return replay.DefaultReplayClassifier(err)
		}),
		replay.WithPollInterval(5*time.Millisecond),
		replay.WithRetryDelay(time.Millisecond),
		replay.WithMaxAttempts(1),
		replay.WithOnDrop(func(_ types.ReplayPayload, err error) { dropped <- err }),
	)
	enqueueN(t, replayer, 1, types.ClusterA)
	require.NoError(t, worker.Start())
	defer worker.Stop()

	select {
	case err := <-dropped:
		require.ErrorIs(t, err, poison)
	case <-time.After(2 * time.Second):
		t.Fatal("custom classifier verdict was not applied")
	}
}

// The bounded policy still reports its drops through the per-reason series
// and the backlog gauges without changing when it gives up.
func TestMemoryWorker_BoundedPolicyReportsBacklogMetrics(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(4))
	defer replayer.Close()
	mc := testutil.NewTestMetricsCollector()

	dropped := make(chan struct{}, 1)
	var ageAtFirstAttempt atomic.Value
	worker := replay.NewMemoryWorker(replayer,
		func(context.Context, types.ReplayPayload) error { return errors.New("always fail") },
		replay.WithPollInterval(5*time.Millisecond),
		replay.WithRetryDelay(time.Millisecond),
		replay.WithMaxRetryDelay(2*time.Millisecond),
		replay.WithMaxAttempts(2),
		replay.WithRetryPolicy(replay.RetryBounded),
		replay.WithWorkerMetrics(mc),
		replay.WithOnError(func(_ types.ReplayPayload, _ error, attempt int) {
			if attempt == 1 {
				ageAtFirstAttempt.Store(mc.GetReplayOldestAge(types.ClusterB))
			}
		}),
		replay.WithOnDrop(func(types.ReplayPayload, error) { dropped <- struct{}{} }),
	)
	require.NoError(t, replayer.Enqueue(t.Context(), types.ReplayPayload{
		TargetCluster: types.ClusterB,
		Query:         "INSERT test",
		Timestamp:     time.Now().Add(-2 * time.Second).UnixMicro(),
	}))
	require.NoError(t, worker.Start())
	defer worker.Stop()

	select {
	case <-dropped:
	case <-time.After(2 * time.Second):
		t.Fatal("payload was not dropped")
	}
	assert.Equal(t, int64(1), mc.GetReplayWorkerDropped(types.ClusterB, types.ReplayDropMaxAttempts))
	age, _ := ageAtFirstAttempt.Load().(float64)
	assert.GreaterOrEqual(t, age, 1.5, "age is measured from the write timestamp")
	require.Eventually(t, func() bool { return mc.GetReplayOldestAge(types.ClusterB) == 0 }, time.Second, 5*time.Millisecond,
		"age gauge resets once nothing is pending")
}
