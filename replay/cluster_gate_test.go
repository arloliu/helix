package replay_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
)

// gateSwitch is a cluster gate a test flips.
// checks counts how often the worker consulted it: the worker parks a refused
// payload for exactly one poll interval, so a rising count is the worker's own
// clock and stands in for the wall-clock sleeps these tests used to take.
type gateSwitch struct {
	open   atomic.Bool
	checks atomic.Int32
}

func (g *gateSwitch) allow(types.ClusterID) bool {
	g.checks.Add(1)

	return g.open.Load()
}

// awaitChecks blocks until the worker has consulted the gate n more times than
// it had at from.
func awaitChecks(t *testing.T, gate *gateSwitch, from int32, n int32, msg string) {
	t.Helper()
	require.Eventually(t, func() bool { return gate.checks.Load() >= from+n },
		5*time.Second, time.Millisecond, msg)
}

func countingExecute(executed *atomic.Int32) replay.ExecuteFunc {
	return func(context.Context, types.ReplayPayload) error {
		executed.Add(1)

		return nil
	}
}

func startWorker(t *testing.T, w *replay.Worker) {
	t.Helper()
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)
}

func TestMemoryWorker_RetainedGateParksWithoutSpendingWindow(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(8))
	var executed, dropped atomic.Int32
	gate := &gateSwitch{}
	worker := replay.NewMemoryWorker(replayer, countingExecute(&executed),
		replay.WithPollInterval(2*time.Millisecond),
		replay.WithRetryWindow(40*time.Millisecond),
		replay.WithClusterGate(gate.allow),
		replay.WithOnDrop(func(types.ReplayPayload, error) { dropped.Add(1) }),
	)
	enqueueN(t, replayer, 2, types.ClusterA)
	startWorker(t, worker)

	// Each refusal parks a payload for one poll interval, so gate checks measure
	// the gated stretch: 2 payloads at a 2ms poll means 50 checks put at least
	// one of them through 24 refusals, well past the 40ms retry window.
	awaitChecks(t, gate, 0, 50, "the worker must keep parking the payloads past the retry window")
	require.Zero(t, executed.Load(), "a gated cluster is never executed against")
	require.Equal(t, 2, replayer.PendingByCluster(types.ClusterA), "parked payloads keep their slots")

	gate.open.Store(true)
	require.Eventually(t, func() bool { return executed.Load() == 2 && replayer.Len() == 0 },
		time.Second, time.Millisecond,
		"both payloads execute once the gate opens, and release their slots")
	require.Zero(t, dropped.Load(), "time spent gated does not consume the retry window")
}

func TestMemoryWorker_RetainedGateParksRetries(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(8))
	var executed, dropped atomic.Int32
	gate := &gateSwitch{}
	gate.open.Store(true)
	var failFirst atomic.Bool
	failFirst.Store(true)
	execute := func(context.Context, types.ReplayPayload) error {
		executed.Add(1)
		if failFirst.CompareAndSwap(true, false) {
			return errors.New("first attempt fails")
		}

		return nil
	}
	// The retry delay is long enough that the gate closes well before the
	// retry is due, even on a loaded machine.
	worker := replay.NewMemoryWorker(replayer, execute,
		replay.WithPollInterval(2*time.Millisecond),
		replay.WithRetryDelay(200*time.Millisecond),
		replay.WithClusterGate(gate.allow),
		replay.WithOnDrop(func(types.ReplayPayload, error) { dropped.Add(1) }),
	)
	enqueueN(t, replayer, 1, types.ClusterA)
	startWorker(t, worker)

	require.Eventually(t, func() bool { return executed.Load() == 1 }, time.Second, time.Millisecond)
	gate.open.Store(false) // close before the retry is due
	// Nothing else is queued, so the next gate check can only come from the
	// retry falling due. Three of them prove the retry delay elapsed and the
	// closed gate turned the retry away each time.
	awaitChecks(t, gate, gate.checks.Load(), 3, "the retry must come due and be refused by the closed gate")
	require.Equal(t, int32(1), executed.Load(), "the retry waits while the gate is closed")

	gate.open.Store(true)
	require.Eventually(t, func() bool { return executed.Load() == 2 }, 2*time.Second, time.Millisecond)
	require.Zero(t, dropped.Load())
}

func TestMemoryWorker_BoundedGateWaitsBetweenAttempts(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(8))
	var executed, dropped atomic.Int32
	gate := &gateSwitch{}
	gate.open.Store(true)
	var failFirst atomic.Bool
	failFirst.Store(true)
	execute := func(context.Context, types.ReplayPayload) error {
		executed.Add(1)
		if failFirst.CompareAndSwap(true, false) {
			gate.open.Store(false) // the gate closes right after the first failure

			return errors.New("first attempt fails")
		}

		return nil
	}
	// Buffered with a non-blocking send: a payload that wrongly ran twice must
	// fail the assertion below, not panic on a second close.
	succeeded := make(chan struct{}, 1)
	worker := replay.NewMemoryWorker(replayer, execute,
		replay.WithRetryPolicy(replay.RetryBounded),
		replay.WithMaxAttempts(2),
		replay.WithPollInterval(2*time.Millisecond),
		replay.WithRetryDelay(time.Millisecond),
		replay.WithClusterGate(gate.allow),
		replay.WithOnDrop(func(types.ReplayPayload, error) { dropped.Add(1) }),
		replay.WithOnSuccess(func(types.ReplayPayload) {
			select {
			case succeeded <- struct{}{}:
			default:
			}
		}),
	)
	enqueueN(t, replayer, 1, types.ClusterA)
	startWorker(t, worker)

	require.Eventually(t, func() bool { return executed.Load() == 1 }, time.Second, time.Millisecond)
	// The retry waits behind the closed gate, one poll interval per refusal, so
	// three further checks are three intervals of the worker declining to spend
	// the last attempt.
	awaitChecks(t, gate, gate.checks.Load(), 3, "the worker must keep re-checking the closed gate")
	require.Equal(t, int32(1), executed.Load(), "the last attempt is not spent while gated")
	require.Zero(t, dropped.Load())

	gate.open.Store(true)
	require.Eventually(t, func() bool { return executed.Load() == 2 }, time.Second, time.Millisecond)
	<-succeeded // the payload is settled: no drop can follow a successful attempt
	require.Zero(t, dropped.Load(), "the retry succeeded once the gate opened")
}

func TestMemoryWorker_GateClosingBetweenDequeueAndExecuteRequeues(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(8))
	var executed atomic.Int32
	// Buffered with a non-blocking send: a payload that wrongly ran twice must
	// fail the assertion below, not panic on a second close.
	succeeded := make(chan struct{}, 1)
	worker := replay.NewMemoryWorker(replayer, countingExecute(&executed),
		replay.WithRetryPolicy(replay.RetryBounded),
		replay.WithPollInterval(2*time.Millisecond),
		replay.WithClusterGate(closeOnceAtExecute(nil)),
		replay.WithOnSuccess(func(types.ReplayPayload) {
			select {
			case succeeded <- struct{}{}:
			default:
			}
		}),
	)
	enqueueN(t, replayer, 1, types.ClusterA)
	startWorker(t, worker)

	require.Eventually(t, func() bool { return executed.Load() == 1 }, time.Second, time.Millisecond)
	// The success settles the payload and the empty queue leaves nothing to
	// dispatch again, so a second run is ruled out rather than merely late.
	<-succeeded
	require.Zero(t, replayer.Len(), "the requeued payload is not still waiting to run")
	require.Equal(t, int32(1), executed.Load(), "the requeued payload runs exactly once")
}

// closeOnceAtExecute builds a cluster A gate that permits the dequeue
// check, refuses the execute check that follows it (calling onClose first),
// then permits everything. Cluster B is always permitted.
func closeOnceAtExecute(onClose func()) func(types.ClusterID) bool {
	var calls atomic.Int32

	return func(c types.ClusterID) bool {
		if c != types.ClusterA {
			return true
		}
		if calls.Add(1) != 2 {
			return true
		}
		if onClose != nil {
			onClose()
		}

		return false
	}
}

// TestMemoryWorker_StopAfterGateRefusalDropsOnce proves a payload the gate
// refused after dequeue is reported as one shutdown drop and releases its
// slot when the worker stops while the gate is still closed.
func TestMemoryWorker_StopAfterGateRefusalDropsOnce(t *testing.T) {
	for _, policy := range []replay.ReplayRetryPolicy{replay.RetryBounded, replay.RetryWhileRetained} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(1))
			var executed, dropped atomic.Int32
			refused := make(chan struct{}, 1)
			// Permit the dequeue check, then refuse cluster A for good.
			var calls atomic.Int32
			gate := func(c types.ClusterID) bool {
				if c != types.ClusterA || calls.Add(1) == 1 {
					return true
				}
				select {
				case refused <- struct{}{}:
				default:
				}

				return false
			}
			worker := replay.NewMemoryWorker(replayer, countingExecute(&executed),
				replay.WithRetryPolicy(policy),
				replay.WithPollInterval(2*time.Millisecond),
				replay.WithClusterGate(gate),
				replay.WithOnDrop(func(_ types.ReplayPayload, err error) {
					dropped.Add(1)
					require.NoError(t, err, "a shutdown drop carries no execution error")
				}),
			)
			enqueueN(t, replayer, 1, types.ClusterA)
			require.NoError(t, worker.Start())
			<-refused
			worker.Stop()

			require.Zero(t, executed.Load())
			require.Equal(t, int32(1), dropped.Load(), "the requeued payload is dropped once at shutdown")
			require.Zero(t, replayer.Len(), "its slot is released")
		})
	}
}

func TestMemoryReplayer_PendingByClusterUnknownClusterIsZero(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(4))
	enqueueN(t, replayer, 2, types.ClusterB)
	require.Equal(t, 2, replayer.PendingByCluster(types.ClusterB))
	require.Zero(t, replayer.PendingByCluster("C"), "an unknown cluster does not alias B")
}

// TestMemoryWorker_GatedPayloadKeepsItsSlot proves a payload the gate
// closes on after dequeue still holds its capacity slot: a producer cannot
// take its place, it is never dropped, and it holds exactly one slot.
func TestMemoryWorker_GatedPayloadKeepsItsSlot(t *testing.T) {
	for _, policy := range []replay.ReplayRetryPolicy{replay.RetryBounded, replay.RetryWhileRetained} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(1))
			var executed, dropped atomic.Int32
			var producerErr error
			var pendingWhileGated int
			gate := closeOnceAtExecute(func() {
				producerErr = replayer.Enqueue(context.Background(), types.ReplayPayload{
					TargetCluster: types.ClusterA, Query: "INSERT producer", Timestamp: time.Now().UnixMicro(),
				})
				pendingWhileGated = replayer.PendingByCluster(types.ClusterA)
			})
			worker := replay.NewMemoryWorker(replayer, countingExecute(&executed),
				replay.WithRetryPolicy(policy),
				replay.WithPollInterval(2*time.Millisecond),
				replay.WithClusterGate(gate),
				replay.WithOnDrop(func(types.ReplayPayload, error) { dropped.Add(1) }),
			)
			enqueueN(t, replayer, 1, types.ClusterA)
			startWorker(t, worker)

			require.Eventually(t, func() bool { return executed.Load() == 1 }, time.Second, time.Millisecond)
			// The released slot is the payload's last act, so everything the
			// gate callback recorded on the way there has settled by then.
			require.Eventually(t, func() bool { return replayer.Len() == 0 }, time.Second, time.Millisecond,
				"the slot is released once the payload has run")
			require.ErrorIs(t, producerErr, types.ErrReplayQueueFull, "the gated payload still holds the only slot")
			require.Equal(t, 1, pendingWhileGated, "a requeue holds one slot, not two")
			require.Equal(t, int32(1), executed.Load())
			require.Zero(t, dropped.Load(), "a gated payload is never dropped")
		})
	}
}

func TestMemoryWorker_GateIsPerCluster(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(8))
	var executedB atomic.Int32
	execute := func(_ context.Context, p types.ReplayPayload) error {
		if p.TargetCluster == types.ClusterB {
			executedB.Add(1)
		}

		return nil
	}
	worker := replay.NewMemoryWorker(replayer, execute,
		replay.WithPollInterval(2*time.Millisecond),
		replay.WithClusterGate(func(c types.ClusterID) bool { return c == types.ClusterB }),
	)
	enqueueN(t, replayer, 2, types.ClusterA)
	enqueueN(t, replayer, 2, types.ClusterB)
	startWorker(t, worker)

	require.Eventually(t, func() bool { return executedB.Load() == 2 }, time.Second, time.Millisecond,
		"the ungated cluster drains while the other is parked")
	require.Equal(t, 2, replayer.PendingByCluster(types.ClusterA))
}
