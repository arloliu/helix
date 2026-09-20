package helix

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// degradingStrategy is a write strategy whose degraded and latched state for
// cluster A the test sets directly.
// Driving a real strategy to its strike threshold would reach the same state
// through failing writes, slowly and with the strike thresholds as a second
// thing that can break the test.
// RecordProbeSuccess is deliberately inert, so a recovery probe tick cannot
// move state the test owns.
type degradingStrategy struct {
	*policy.ConcurrentDualWrite
	degraded atomic.Bool
	latched  atomic.Bool
}

func newDegradingStrategy() *degradingStrategy {
	return &degradingStrategy{ConcurrentDualWrite: policy.NewConcurrentDualWrite()}
}

func (s *degradingStrategy) IsDegraded(c ClusterID) bool  { return c == ClusterA && s.degraded.Load() }
func (s *degradingStrategy) IsLatched(c ClusterID) bool   { return c == ClusterA && s.latched.Load() }
func (s *degradingStrategy) RecordProbeSuccess(ClusterID) {}

// newGatedReplayClient builds a client whose cluster A is draining, so the
// first write is skipped on A and enqueued for replay, with the auto memory
// worker polling fast.
func newGatedReplayClient(t *testing.T, opts ...Option) (*CQLClient, *recordingSession) {
	t.Helper()
	sa, sb := newRecordingSession(nil), newRecordingSession(nil)
	client, err := NewCQLClient(sa, sb, append([]Option{
		WithAutoMemoryWorker(64, replay.WithPollInterval(2*time.Millisecond)),
	}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	return client, sa
}

func TestReplayGate_DrainHoldsReplayUntilLifted(t *testing.T) {
	client, sa := newGatedReplayClient(t)
	client.drainA.Store(true)

	require.NoError(t, client.Query("INSERT INTO t (k) VALUES (?)", 1).ExecContext(t.Context()),
		"cluster B acknowledges; A's leg is skipped and replayed")
	time.Sleep(30 * time.Millisecond)
	require.Zero(t, sa.execs.Load(), "replay never runs against a draining cluster")

	client.drainA.Store(false)
	waitForExecs(t, sa, 1)
}

func TestReplayGate_OperatorPredicateHoldsReplay(t *testing.T) {
	var quarantined atomic.Bool
	quarantined.Store(true)
	client, sa := newGatedReplayClient(t,
		WithReplayGate(func(c ClusterID) bool { return c != ClusterA || !quarantined.Load() }),
	)
	client.drainA.Store(true)
	require.NoError(t, client.Query("INSERT INTO t (k) VALUES (?)", 1).ExecContext(t.Context()))
	client.drainA.Store(false)

	time.Sleep(30 * time.Millisecond)
	require.Zero(t, sa.execs.Load(), "the operator's predicate holds replay back after the drain lifted")

	quarantined.Store(false)
	waitForExecs(t, sa, 1)
}

// enqueueReplayForA makes one write that cluster A misses, by draining A for
// the duration of that write, and lifts the drain again so the remaining gate
// inputs alone decide when the payload runs.
func enqueueReplayForA(t *testing.T, client *CQLClient) {
	t.Helper()
	client.drainA.Store(true)
	require.NoError(t, client.Query("INSERT INTO t (k) VALUES (?)", 1).ExecContext(t.Context()),
		"cluster B acknowledges; A's leg is skipped and replayed")
	client.drainA.Store(false)
}

func TestReplayGate_DegradedClusterHoldsReplay(t *testing.T) {
	strategy := newDegradingStrategy()
	strategy.degraded.Store(true)
	client, sa := newGatedReplayClient(t, WithWriteStrategy(strategy))
	enqueueReplayForA(t, client)

	time.Sleep(30 * time.Millisecond)
	require.Zero(t, sa.execs.Load(),
		"replay waits while the write strategy reports cluster A degraded, even with the drain lifted")
}

func TestReplayGate_StrategyRecoveryReleasesReplay(t *testing.T) {
	strategy := newDegradingStrategy()
	strategy.degraded.Store(true)
	client, sa := newGatedReplayClient(t, WithWriteStrategy(strategy))
	enqueueReplayForA(t, client)

	time.Sleep(30 * time.Millisecond)
	require.Zero(t, sa.execs.Load(), "the degraded cluster is held back first")

	strategy.degraded.Store(false)
	waitForExecs(t, sa, 1)
}

func TestReplayGate_LatchedClusterStillReceivesReplay(t *testing.T) {
	// The operator workflow latches a cluster, drains its backlog, and only
	// then recovers it by hand, so the latch must not hold the drain back.
	strategy := newDegradingStrategy()
	strategy.degraded.Store(true)
	strategy.latched.Store(true)
	client, sa := newGatedReplayClient(t, WithWriteStrategy(strategy))
	enqueueReplayForA(t, client)

	waitForExecs(t, sa, 1)
}

func TestReplayGate_StrategyWithoutDegradedStateNeverHoldsReplay(t *testing.T) {
	client, sa := newGatedReplayClient(t, WithWriteStrategy(policy.NewConcurrentDualWrite()))
	enqueueReplayForA(t, client)

	waitForExecs(t, sa, 1)
}

func TestReplayGate_DegradedClusterReplaysWithoutARecoveryProbe(t *testing.T) {
	// Without the probe nothing but an operator could lift the hold, so
	// these clients keep replaying to a degraded cluster.
	strategy := newDegradingStrategy()
	strategy.degraded.Store(true)
	client, sa := newGatedReplayClient(t, WithWriteStrategy(strategy), WithRecoveryProbeDisabled())
	enqueueReplayForA(t, client)

	waitForExecs(t, sa, 1)
}

func TestReplayGate_ComposesWithCallerWorkerGate(t *testing.T) {
	var callerGate, operatorGate atomic.Bool
	client, sa := newGatedReplayClient(t,
		WithAutoMemoryWorker(64,
			replay.WithPollInterval(2*time.Millisecond),
			replay.WithClusterGate(func(ClusterID) bool { return callerGate.Load() }),
		),
		WithReplayGate(func(ClusterID) bool { return operatorGate.Load() }),
	)
	client.drainA.Store(true)
	require.NoError(t, client.Query("INSERT INTO t (k) VALUES (?)", 1).ExecContext(t.Context()))

	// Every input must open: the drain, the caller's worker gate, and the
	// operator's predicate, in any order.
	time.Sleep(20 * time.Millisecond)
	require.Zero(t, sa.execs.Load())
	callerGate.Store(true)
	time.Sleep(20 * time.Millisecond)
	require.Zero(t, sa.execs.Load(), "the drain and the operator gate still hold")
	client.drainA.Store(false)
	time.Sleep(20 * time.Millisecond)
	require.Zero(t, sa.execs.Load(), "the operator gate still holds")
	operatorGate.Store(true)
	waitForExecs(t, sa, 1)
}

func TestReplayGate_SuppliedWorkerWarnsOnlyWhenGatingMatters(t *testing.T) {
	const warning = "replay gating could not be applied or verified for the supplied replay worker: " +
		"pass replay.WithClusterGate when building it so drain and WithReplayGate hold replay back"
	newSupplied := func(t *testing.T, opts ...Option) *captureLogger {
		t.Helper()
		logger := &captureLogger{}
		replayer := replay.NewMemoryReplayer()
		worker := replay.NewMemoryWorker(replayer, func(context.Context, types.ReplayPayload) error { return nil })
		client, err := NewCQLClient(newMockSession(), newMockSession(), append([]Option{
			WithLogger(logger),
			WithReplayer(replayer),
			WithReplayWorker(worker),
		}, opts...)...)
		require.NoError(t, err)
		t.Cleanup(client.Close)

		return logger
	}

	require.NotContains(t, warnings(newSupplied(t)), warning, "no drain and no gate: nothing to apply")
	require.Contains(t, warnings(newSupplied(t, WithReplayGate(func(ClusterID) bool { return true }))), warning)
}
