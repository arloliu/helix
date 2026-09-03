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
type gateSwitch struct{ open atomic.Bool }

func (g *gateSwitch) allow(types.ClusterID) bool { return g.open.Load() }

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

// settle waits a few poll intervals so a gated worker had every chance to
// misbehave before the test asserts it did not.
func settle() { time.Sleep(30 * time.Millisecond) }

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

	time.Sleep(80 * time.Millisecond) // longer than the retry window
	require.Zero(t, executed.Load(), "a gated cluster is never executed against")
	require.Equal(t, 2, replayer.PendingByCluster(types.ClusterA), "parked payloads keep their slots")

	gate.open.Store(true)
	require.Eventually(t, func() bool { return executed.Load() == 2 }, time.Second, time.Millisecond,
		"both payloads execute once the gate opens")
	settle()
	require.Zero(t, dropped.Load(), "time spent gated does not consume the retry window")
	require.Zero(t, replayer.Len())
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
	worker := replay.NewMemoryWorker(replayer, execute,
		replay.WithPollInterval(2*time.Millisecond),
		replay.WithRetryDelay(2*time.Millisecond),
		replay.WithClusterGate(gate.allow),
		replay.WithOnDrop(func(types.ReplayPayload, error) { dropped.Add(1) }),
	)
	enqueueN(t, replayer, 1, types.ClusterA)
	startWorker(t, worker)

	require.Eventually(t, func() bool { return executed.Load() == 1 }, time.Second, time.Millisecond)
	gate.open.Store(false) // close before the retry is due
	settle()
	require.Equal(t, int32(1), executed.Load(), "the retry waits while the gate is closed")

	gate.open.Store(true)
	require.Eventually(t, func() bool { return executed.Load() == 2 }, time.Second, time.Millisecond)
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
	worker := replay.NewMemoryWorker(replayer, execute,
		replay.WithRetryPolicy(replay.RetryBounded),
		replay.WithMaxAttempts(2),
		replay.WithPollInterval(2*time.Millisecond),
		replay.WithRetryDelay(time.Millisecond),
		replay.WithClusterGate(gate.allow),
		replay.WithOnDrop(func(types.ReplayPayload, error) { dropped.Add(1) }),
	)
	enqueueN(t, replayer, 1, types.ClusterA)
	startWorker(t, worker)

	require.Eventually(t, func() bool { return executed.Load() == 1 }, time.Second, time.Millisecond)
	settle()
	require.Equal(t, int32(1), executed.Load(), "the last attempt is not spent while gated")
	require.Zero(t, dropped.Load())

	gate.open.Store(true)
	require.Eventually(t, func() bool { return executed.Load() == 2 }, time.Second, time.Millisecond)
	settle()
	require.Zero(t, dropped.Load(), "the retry succeeded once the gate opened")
}

func TestMemoryWorker_GateClosingBetweenDequeueAndExecuteRequeues(t *testing.T) {
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(8))
	var executed atomic.Int32
	worker := replay.NewMemoryWorker(replayer, countingExecute(&executed),
		replay.WithRetryPolicy(replay.RetryBounded),
		replay.WithPollInterval(2*time.Millisecond),
		replay.WithClusterGate(closeOnceAtExecute(nil)),
	)
	enqueueN(t, replayer, 1, types.ClusterA)
	startWorker(t, worker)

	require.Eventually(t, func() bool { return executed.Load() == 1 }, time.Second, time.Millisecond)
	settle()
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
			settle()
			require.ErrorIs(t, producerErr, types.ErrReplayQueueFull, "the gated payload still holds the only slot")
			require.Equal(t, 1, pendingWhileGated, "a requeue holds one slot, not two")
			require.Equal(t, int32(1), executed.Load())
			require.Zero(t, dropped.Load(), "a gated payload is never dropped")
			require.Eventually(t, func() bool { return replayer.Len() == 0 }, time.Second, time.Millisecond,
				"the slot is released once the payload has run")
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
