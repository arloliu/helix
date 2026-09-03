package replay_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
)

func newRetainedNATSReplayer(t *testing.T, js jetstream.JetStream, name string) *replay.NATSReplayer {
	t.Helper()
	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-retained-"+name),
		replay.WithSubjectPrefix("test.retained."+name),
		replay.WithMaxDeliver(2), // ignored under RetryWhileRetained
		replay.WithAckWait(2*time.Second),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = replayer.Close() })

	return replayer
}

// PendingByCluster reads the stream, so a replayer that has created no
// consumer (a fresh process, or one whose worker is gated) still sees the
// durable backlog.
func TestNATSReplayer_PendingByClusterSeesDurableBacklog(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	producer := newRetainedNATSReplayer(t, js, "backlog")
	enqueueN(t, producer, 3, types.ClusterA)

	fresh := newRetainedNATSReplayer(t, js, "backlog")
	pending, err := fresh.PendingByCluster(t.Context(), types.ClusterA)
	require.NoError(t, err)
	assert.Equal(t, 3, pending, "the backlog is visible without a consumer")
	pending, err = fresh.PendingByCluster(t.Context(), types.ClusterB)
	require.NoError(t, err)
	assert.Zero(t, pending)
}

func TestNATSReplayer_PendingByClusterRejectsUnknownCluster(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	replayer := newRetainedNATSReplayer(t, js, "unknown")
	enqueueN(t, replayer, 1, types.ClusterA)

	for _, cluster := range []types.ClusterID{"C", "*", ">"} {
		_, err := replayer.PendingByCluster(t.Context(), cluster)
		require.ErrorIs(t, err, types.ErrInvalidCluster, "cluster %q", cluster)
	}
}

// PendingByCluster and Pending share the stream handle; calling them
// concurrently must not race on its cached info.
func TestNATSReplayer_PendingCountsAreConcurrencySafe(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	replayer := newRetainedNATSReplayer(t, js, "concurrent")
	enqueueN(t, replayer, 2, types.ClusterA)

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 10 {
				_, err := replayer.Pending(t.Context())
				assert.NoError(t, err)
				_, err = replayer.PendingByCluster(t.Context(), types.ClusterA)
				assert.NoError(t, err)
			}
		})
	}
	wg.Wait()
}

// A fetched message counts as pending while its execution is in flight
// and while it waits for a delayed redelivery, and stops counting once it
// is acknowledged.
func TestNATSReplayer_PendingByClusterCountsInFlightAndDelayed(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	replayer := newRetainedNATSReplayer(t, js, "inflight")

	entered := make(chan struct{}, 1)
	release := make(chan error)
	worker := replay.NewNATSWorker(replayer,
		func(context.Context, types.ReplayPayload) error {
			entered <- struct{}{}

			return <-release
		},
		replay.WithRetryPolicy(replay.RetryWhileRetained),
		replay.WithPollInterval(10*time.Millisecond),
		replay.WithBatchSize(1),
		replay.WithRetryDelay(200*time.Millisecond),
		replay.WithMaxRetryDelay(200*time.Millisecond),
	)
	enqueueN(t, replayer, 1, types.ClusterA)
	require.NoError(t, worker.Start())
	defer worker.Stop()

	pending := func() int {
		n, err := replayer.PendingByCluster(t.Context(), types.ClusterA)
		require.NoError(t, err)

		return n
	}
	<-entered
	assert.Equal(t, 1, pending(), "in flight, not yet acknowledged")
	release <- fmt.Errorf("%w: pool empty", types.ErrClusterUnreachable)
	time.Sleep(50 * time.Millisecond) // inside the delayed NAK window
	assert.Equal(t, 1, pending(), "waiting for redelivery")

	<-entered
	release <- nil
	require.Eventually(t, func() bool { return pending() == 0 }, 5*time.Second, 20*time.Millisecond,
		"acknowledged")
}

// Under RetryWhileRetained the consumer never drops a message on its own:
// an unreachable cluster is retried past any attempt budget until it returns.
func TestNATSWorker_RetainedPolicySurvivesOutage(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	replayer := newRetainedNATSReplayer(t, js, "outage")
	mc := testutil.NewTestMetricsCollector()

	const payloads = 5

	var attempts, successes, dropped atomic.Int32
	worker := replay.NewNATSWorker(replayer,
		unreachableFor(1500*time.Millisecond, &attempts, &successes),
		replay.WithRetryPolicy(replay.RetryWhileRetained),
		replay.WithPollInterval(10*time.Millisecond),
		replay.WithBatchSize(payloads), // a full batch returns without waiting for the fetch timeout
		replay.WithRetryDelay(20*time.Millisecond),
		replay.WithMaxRetryDelay(50*time.Millisecond),
		replay.WithMaxAttempts(2),
		replay.WithWorkerMetrics(mc),
		replay.WithOnDrop(func(types.ReplayPayload, error) { dropped.Add(1) }),
	)

	enqueueN(t, replayer, payloads, types.ClusterA)
	require.NoError(t, worker.Start())
	defer worker.Stop()

	require.Eventually(t, func() bool { return successes.Load() == payloads }, 10*time.Second, 20*time.Millisecond)
	assert.Greater(t, attempts.Load(), int32(payloads*2), "attempts must exceed a bounded budget of 2")
	assert.Equal(t, int32(0), dropped.Load())

	require.Eventually(t, func() bool {
		pending, err := replayer.PendingByCluster(t.Context(), types.ClusterA)

		return err == nil && pending == 0
	}, 5*time.Second, 20*time.Millisecond, "every message must be acknowledged")

	info, err := js.Consumer(t.Context(), replayer.StreamName(), "helix-worker-high-A")
	require.NoError(t, err)
	cfg := info.CachedInfo().Config
	assert.Equal(t, -1, cfg.MaxDeliver, "consumer must allow unlimited deliveries")
	require.NotEmpty(t, cfg.BackOff)
	assert.Equal(t, 2*time.Second, cfg.BackOff[0], "server-side redelivery starts at AckWait")
}

// A dead-letter disposition still terminates the message once the poison
// budget (MaxAttempts) is spent; unreachable attempts before it do not count.
func TestNATSWorker_RetainedPolicyDeadLettersAfterBudget(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	replayer := newRetainedNATSReplayer(t, js, "deadletter")
	mc := testutil.NewTestMetricsCollector()

	var attempts atomic.Int32
	dropped := make(chan error, 1)
	worker := replay.NewNATSWorker(replayer,
		func(context.Context, types.ReplayPayload) error {
			if attempts.Add(1) <= 3 {
				return fmt.Errorf("%w: pool empty", types.ErrClusterUnreachable)
			}

			return fmt.Errorf("%w: replay target %q", types.ErrInvalidCluster, "C")
		},
		replay.WithRetryPolicy(replay.RetryWhileRetained),
		replay.WithPollInterval(10*time.Millisecond),
		replay.WithBatchSize(1),
		replay.WithRetryDelay(10*time.Millisecond),
		replay.WithMaxRetryDelay(20*time.Millisecond),
		replay.WithMaxAttempts(2),
		replay.WithWorkerMetrics(mc),
		replay.WithOnDrop(func(_ types.ReplayPayload, err error) { dropped <- err }),
	)
	enqueueN(t, replayer, 1, types.ClusterB)
	require.NoError(t, worker.Start())
	defer worker.Stop()

	select {
	case err := <-dropped:
		require.ErrorIs(t, err, types.ErrInvalidCluster)
	case <-time.After(10 * time.Second):
		t.Fatal("message was not dead-lettered")
	}
	assert.Equal(t, int32(5), attempts.Load(), "3 deferred attempts plus 2 dead-letter attempts")
	assert.Equal(t, int64(1), mc.GetReplayWorkerDropped(types.ClusterB, types.ReplayDropDeadLetter))

	require.Eventually(t, func() bool {
		pending, err := replayer.Pending(t.Context())

		return err == nil && pending == 0
	}, 5*time.Second, 20*time.Millisecond, "a dead-lettered message must leave the stream")
}
