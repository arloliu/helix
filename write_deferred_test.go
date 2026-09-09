package helix

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// writeDurationSpy records every write_duration sample, per cluster, in call order.
type writeDurationSpy struct {
	metrics.NopMetrics
	mu      sync.Mutex
	samples map[ClusterID][]float64
}

func newWriteDurationSpy() *writeDurationSpy {
	return &writeDurationSpy{samples: make(map[ClusterID][]float64)}
}

func (m *writeDurationSpy) ObserveWriteDuration(cluster types.ClusterID, seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.samples[cluster] = append(m.samples[cluster], seconds)
}

func (m *writeDurationSpy) samplesFor(cluster ClusterID) []float64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return append([]float64(nil), m.samples[cluster]...)
}

// TestAdaptiveWrite_DeferredFailureIsReplayed asserts that a fire-and-forget
// leg that fails in the background is enqueued for replay once, and that
// the write itself is still success because the other cluster acknowledged.
func TestAdaptiveWrite_DeferredFailureIsReplayed(t *testing.T) {
	failB := errors.New("cluster B rejected the background write")
	sa, sb := newRecordingSession(nil), newRecordingSession(failB)
	adaptive := policy.NewAdaptiveDualWrite()
	adaptive.ForceDegrade(ClusterB)
	replayer := &mockReplayer{}

	client, err := NewCQLClient(sa, sb,
		WithWriteStrategy(adaptive),
		WithRecoveryProbeDisabled(),
		WithReplayer(replayer),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	const writes = 2
	for i := range writes {
		require.NoError(t, client.Query("INSERT INTO t (k, v) VALUES (?, ?)", i, "v").ExecContext(t.Context()))
	}
	waitForExecs(t, sb, writes)

	require.Eventually(t, func() bool {
		replayer.Lock()
		defer replayer.Unlock()

		return len(replayer.payloads) == writes
	}, regressionWaitTimeout, time.Millisecond, "each failed background leg must be enqueued for replay once")

	replayer.Lock()
	defer replayer.Unlock()
	for _, p := range replayer.payloads {
		require.Equal(t, ClusterB, p.TargetCluster)
		require.Equal(t, "INSERT INTO t (k, v) VALUES (?, ?)", p.Query)
	}
}

// TestAdaptiveWrite_BackgroundLegObservesItsOwnDuration asserts that a fire-and-forget leg contributes exactly one write_duration sample,
// taken when the leg itself completes and covering the whole time the leg ran.
// A leg that is still in flight has no duration yet and must contribute nothing.
func TestAdaptiveWrite_BackgroundLegObservesItsOwnDuration(t *testing.T) {
	sa, sb := newRecordingSession(nil), newRecordingSession(nil)
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	sb.gate = release

	adaptive := policy.NewAdaptiveDualWrite()
	adaptive.ForceDegrade(ClusterB)
	spy := newWriteDurationSpy()

	client, err := NewCQLClient(sa, sb,
		WithWriteStrategy(adaptive),
		WithRecoveryProbeDisabled(),
		WithMetrics(spy),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	// Registered last so it runs first: Close waits for the background leg, which is holding the gate.
	t.Cleanup(releaseOnce)

	require.NoError(t, client.Query("INSERT INTO t (k, v) VALUES (?, ?)", 1, "v").ExecContext(t.Context()))
	waitForExecs(t, sb, 1) // the background leg is inside the session now, held on the gate

	require.Len(t, spy.samplesFor(ClusterA), 1, "the synchronous leg observes its duration once")
	require.Empty(t, spy.samplesFor(ClusterB), "a leg that is still running has no duration to observe yet")

	releaseOnce()

	require.Eventually(t, func() bool {
		return len(spy.samplesFor(ClusterB)) == 1
	}, regressionWaitTimeout, time.Millisecond, "the background leg must observe its duration once, when it completes")

	held := sb.heldFor()
	require.Positive(t, held, "the leg must have been held long enough to measure")
	require.GreaterOrEqual(t, spy.samplesFor(ClusterB)[0], held.Seconds(),
		"the sample must cover the time the leg itself spent running, not the time until the sibling leg returned")
	require.Len(t, spy.samplesFor(ClusterA), 1, "the synchronous leg is not observed a second time")
}

// TestAdaptiveWrite_DeferredFailureWithoutReplayerIsReported asserts that a
// background failure with no replayer is counted as a dropped replay.
func TestAdaptiveWrite_DeferredFailureWithoutReplayerIsReported(t *testing.T) {
	failB := errors.New("cluster B rejected the background write")
	sa, sb := newRecordingSession(nil), newRecordingSession(failB)
	adaptive := policy.NewAdaptiveDualWrite()
	adaptive.ForceDegrade(ClusterB)
	mc := newReplayDropCounter()
	dropped := make(chan types.ClusterEvent, 4)

	client, err := NewCQLClient(sa, sb,
		WithWriteStrategy(adaptive),
		WithRecoveryProbeDisabled(),
		WithMetrics(mc),
		WithOnClusterEvent(func(ev types.ClusterEvent) {
			if ev.Kind == types.EventReplayDropped {
				dropped <- ev
			}
		}),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.NoError(t, client.Query("INSERT INTO t (k, v) VALUES (?, ?)", 1, "v").ExecContext(t.Context()))
	waitForExecs(t, sb, 1)

	select {
	case ev := <-dropped:
		require.Equal(t, ClusterB, ev.Cluster)
		require.ErrorIs(t, ev.Err, types.ErrNoReplayer)
	case <-time.After(regressionWaitTimeout):
		t.Fatal("the failed background leg must be reported as a dropped replay")
	}
	require.Equal(t, int32(1), mc.dropped[ClusterB].Load())
}

// blockingReplayer is a Replayer whose Enqueue reports the payload and then
// waits for the test to release it.
// It stands in for a queue that is slow to admit, such as a NATS server that
// has not acknowledged the publish yet.
type blockingReplayer struct {
	entered chan types.ReplayPayload
	release chan struct{}
}

func (r *blockingReplayer) Enqueue(_ context.Context, p types.ReplayPayload) error {
	r.entered <- p
	<-r.release

	return nil
}

// TestAdaptiveWrite_FireForgetLimitBoundsPendingAdmissions asserts that the
// fire-and-forget limit bounds the background legs waiting in
// Replayer.Enqueue, not only the legs whose write is still running.
// A write that finds every slot held by a pending admission takes the
// over-limit path instead of starting one more.
func TestAdaptiveWrite_FireForgetLimitBoundsPendingAdmissions(t *testing.T) {
	failB := errors.New("cluster B rejected the background write")
	gate := make(chan struct{})
	sa, sb := newRecordingSession(nil), newRecordingSession(failB)
	sb.gate = gate

	adaptive := policy.NewAdaptiveDualWrite(policy.WithAdaptiveFireForgetLimit(2))
	adaptive.ForceDegrade(ClusterB)
	replayer := &blockingReplayer{
		entered: make(chan types.ReplayPayload, 4),
		release: make(chan struct{}),
	}
	mc := &mockMetricsCollector{}

	client, err := NewCQLClient(sa, sb,
		WithWriteStrategy(adaptive),
		WithRecoveryProbeDisabled(),
		WithReplayer(replayer),
		WithMetrics(mc),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	// Registered after Close so it runs before it: Close waits for the
	// background legs, which cannot finish while the replayer holds them.
	release := sync.OnceFunc(func() { close(replayer.release) })
	t.Cleanup(release)

	// Both writes return as soon as cluster A acknowledges.
	// Each leaves a background leg holding a slot, parked inside the gated session.
	const filled = 2
	for i := range filled {
		require.NoError(t, client.Query("INSERT INTO t (k, v) VALUES (?, ?)", i, "v").ExecContext(t.Context()))
	}
	waitForExecs(t, sb, filled)

	// Releasing the gate fails both legs, so each one moves from its write
	// into the replay admission that now blocks.
	close(gate)
	for range filled {
		select {
		case <-replayer.entered:
		case <-time.After(regressionWaitTimeout):
			t.Fatal("both failed background legs must reach the replayer")
		}
	}

	// The third write finds both slots held by those pending admissions.
	third := make(chan error, 1)
	go func() {
		third <- client.Query("INSERT INTO t (k, v) VALUES (?, ?)", filled, "v").ExecContext(t.Context())
	}()

	require.Eventually(t, func() bool {
		mc.Lock()
		defer mc.Unlock()

		return mc.writeDropped[ClusterB] == 1
	}, regressionWaitTimeout, time.Millisecond,
		"a write that cannot get a slot must be dropped, not start a third pending admission")
	require.Equal(t, int32(filled), sb.execs.Load(), "the dropped write is never attempted on cluster B")

	// The dropped leg is admitted on the caller's goroutine, so the third
	// write settles only once the replayer lets go of all three payloads.
	select {
	case <-replayer.entered:
	case <-time.After(regressionWaitTimeout):
		t.Fatal("the dropped leg must be admitted for replay by its caller")
	}
	release()

	select {
	case err := <-third:
		require.NoError(t, err, "cluster A acknowledged the write")
	case <-time.After(regressionWaitTimeout):
		t.Fatal("the third write must return once the replayer releases it")
	}
}
