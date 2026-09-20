package replay_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
)

// hungClusterExecuteTimeout is far longer than siblingDrainBound below.
// One dequeue loop serving both clusters would release the healthy
// cluster's payloads one per attempt timeout, so the bound is missed by
// more than an order of magnitude rather than by a margin.
const hungClusterExecuteTimeout = 3 * time.Second

// siblingDrainBound is how long the healthy cluster's payloads are given
// while the other cluster's target hangs.
const siblingDrainBound = 500 * time.Millisecond

// TestMemoryWorker_HungClusterDoesNotHoldTheSibling proves the memory
// worker dequeues each cluster on its own loop: while one cluster's target
// hangs in an attempt that has not yet timed out, the other cluster's
// backlog drains at full speed.
//
// Both retry policies are covered because they run the first attempt on
// different paths: RetryWhileRetained through handleFirstAttemptRetained,
// RetryBounded through handleFirstAttempt.
func TestMemoryWorker_HungClusterDoesNotHoldTheSibling(t *testing.T) {
	const siblingPayloads = 3

	for _, policy := range []replay.ReplayRetryPolicy{replay.RetryBounded, replay.RetryWhileRetained} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(8))
			hung := make(chan struct{}, 1)
			// Buffered past what the test consumes, and sent to without
			// blocking, so a payload that wrongly ran twice fails the
			// counts below instead of parking a worker goroutine.
			succeeded := make(chan types.ClusterID, siblingPayloads+2)
			var executedSibling atomic.Int32

			execute := func(ctx context.Context, payload types.ReplayPayload) error {
				if payload.TargetCluster == types.ClusterB {
					executedSibling.Add(1)

					return nil
				}
				select {
				case hung <- struct{}{}:
				default:
				}
				<-ctx.Done()

				return ctx.Err()
			}

			worker := replay.NewMemoryWorker(replayer, execute,
				replay.WithRetryPolicy(policy),
				replay.WithExecuteTimeout(hungClusterExecuteTimeout),
				replay.WithPollInterval(2*time.Millisecond),
				replay.WithOnSuccess(func(payload types.ReplayPayload) {
					select {
					case succeeded <- payload.TargetCluster:
					default:
					}
				}),
			)
			enqueueN(t, replayer, 1, types.ClusterA)
			startWorker(t, worker)

			// Enqueue the sibling's payloads only once the other cluster is
			// inside its hung attempt, so they cannot have been taken before
			// the hold began.
			select {
			case <-hung:
			case <-time.After(2 * time.Second):
				t.Fatal("the worker never reached the hung cluster's attempt")
			}
			enqueueN(t, replayer, siblingPayloads, types.ClusterB)

			deadline := time.After(siblingDrainBound)
			for i := range siblingPayloads {
				select {
				case cluster := <-succeeded:
					require.Equal(t, types.ClusterB, cluster, "only the sibling's payloads succeed")
				case <-deadline:
					t.Fatalf("the sibling drained %d of %d payloads in %s while the other cluster hung",
						i, siblingPayloads, siblingDrainBound)
				}
			}

			// Stop joins both loops, so the counts can no longer move.
			worker.Stop()
			require.Equal(t, int32(siblingPayloads), executedSibling.Load(),
				"each of the sibling's payloads executes exactly once")
		})
	}
}

// TestMemoryWorker_StopSettlesBothClustersExactlyOnce proves the shared
// teardown runs once, after every dequeue loop has exited: with an attempt
// in flight and a payload still queued on each cluster, every payload is
// reported exactly once, the queued ones never execute, and the replayer
// is left empty.
func TestMemoryWorker_StopSettlesBothClustersExactlyOnce(t *testing.T) {
	const perCluster = 2

	for _, policy := range []replay.ReplayRetryPolicy{replay.RetryBounded, replay.RetryWhileRetained} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(2 * perCluster))
			mc := testutil.NewTestMetricsCollector()
			entered := make(chan types.ClusterID, 2*perCluster)
			var executed, dropped atomic.Int32

			execute := func(ctx context.Context, payload types.ReplayPayload) error {
				executed.Add(1)
				select {
				case entered <- payload.TargetCluster:
				default:
				}
				<-ctx.Done()

				return ctx.Err()
			}

			worker := replay.NewMemoryWorker(replayer, execute,
				replay.WithRetryPolicy(policy),
				// Well past the bound Stop is held to: a Stop that waited
				// for the attempts in flight would hang the test rather
				// than pass slowly.
				replay.WithExecuteTimeout(5*time.Second),
				replay.WithPollInterval(2*time.Millisecond),
				replay.WithWorkerMetrics(mc),
				replay.WithOnDrop(func(types.ReplayPayload, error) { dropped.Add(1) }),
			)
			enqueueN(t, replayer, perCluster, types.ClusterA)
			enqueueN(t, replayer, perCluster, types.ClusterB)
			require.NoError(t, worker.Start())

			// Both loops must be inside an attempt before Stop, so the
			// teardown has one in-flight and one queued payload to settle
			// on each cluster.
			seen := make(map[types.ClusterID]bool, 2)
			for len(seen) < 2 {
				select {
				case cluster := <-entered:
					seen[cluster] = true
				case <-time.After(2 * time.Second):
					t.Fatalf("only %v reached an attempt", seen)
				}
			}

			stopped := make(chan struct{})
			go func() {
				worker.Stop()
				close(stopped)
			}()
			select {
			case <-stopped:
			case <-time.After(2 * time.Second):
				t.Fatal("Stop waited for the attempts in flight instead of cancelling them")
			}

			// Stop joins both loops and the teardown they share, so nothing
			// below can still move.
			require.Equal(t, int32(2*perCluster), dropped.Load(), "every payload is reported exactly once")
			require.Equal(t, int32(2), executed.Load(),
				"one attempt per cluster ran, and the queued payloads never started one")
			require.Zero(t, replayer.Len(), "every payload released its slot")
			for _, cluster := range []types.ClusterID{types.ClusterA, types.ClusterB} {
				require.Equal(t, int64(perCluster),
					mc.GetReplayWorkerDropped(cluster, types.ReplayDropShutdown),
					"cluster %s reports its payloads as shutdown drops", cluster)
			}
		})
	}
}

// TestMemoryWorker_StopReportsWhatTheLastLoopParks proves the teardown
// waits for every dequeue loop rather than running when the first one
// leaves: a payload parked by the loop that is still running when the
// other has already exited is still reported and still releases its slot.
//
// Only one cluster holds a payload.
// The other loop has nothing to do and leaves its poll the moment Stop is
// called, which is the loop that would tear down early.
func TestMemoryWorker_StopReportsWhatTheLastLoopParks(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(2))
	// Buffered and sent to without blocking: the channel only reports that
	// a drop happened, and must never park the goroutine reporting it.
	reported := make(chan types.ClusterID, 4)
	var executed, dropped atomic.Int32

	held := make(chan struct{})
	release := make(chan struct{})
	// The gate permits the dequeue check and refuses the execute check that
	// follows it, which parks the payload.
	// Holding the worker inside that refusal keeps its loop running while
	// Stop takes the other one down.
	gate := closeOnceAtExecute(func() {
		close(held)
		<-release
	})

	worker := replay.NewMemoryWorker(replayer,
		func(context.Context, types.ReplayPayload) error {
			executed.Add(1)

			return errors.New("the gate refuses this payload, so it never runs")
		},
		replay.WithRetryPolicy(replay.RetryWhileRetained),
		replay.WithPollInterval(2*time.Millisecond),
		replay.WithClusterGate(gate),
		replay.WithOnDrop(func(payload types.ReplayPayload, _ error) {
			dropped.Add(1)
			select {
			case reported <- payload.TargetCluster:
			default:
			}
		}),
	)
	enqueueN(t, replayer, 1, types.ClusterA)
	startWorker(t, worker)
	// Registered after startWorker, so it runs before the Stop that
	// startWorker registered: a test that fails below must still let the
	// held loop out, or Stop would wait for it until the package timeout.
	var releaseOnce sync.Once
	releaseHeldLoop := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseHeldLoop)

	select {
	case <-held:
	case <-time.After(2 * time.Second):
		t.Fatal("the worker never reached the gate's execute check")
	}
	stopped := make(chan struct{})
	go func() {
		worker.Stop()
		close(stopped)
	}()

	// Nothing may be reported while a dequeue loop is still running: the
	// only payload is the held one, and the idle loop has nothing to
	// settle.
	// The wait is bounded because the claim is an absence.
	select {
	case cluster := <-reported:
		t.Fatalf("cluster %s was reported while a dequeue loop was still running", cluster)
	case <-time.After(150 * time.Millisecond):
	}

	releaseHeldLoop()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return after the held loop resumed")
	}

	require.Equal(t, int32(1), dropped.Load(), "the payload the last loop parked is still reported")
	require.Zero(t, replayer.Len(), "and still releases its slot")
	require.Zero(t, executed.Load(), "the gate refused the payload, so it never executed")
}
