package helix

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

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
