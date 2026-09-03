package policy

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestActiveFailoverAlwaysFailsOver(t *testing.T) {
	policy := NewActiveFailover()

	require.True(t, policy.ShouldFailover(types.ClusterA, nil))
	require.True(t, policy.ShouldFailover(types.ClusterB, nil))
}

func TestCircuitBreakerThreshold(t *testing.T) {
	policy := NewCircuitBreaker(
		WithThreshold(3),
		WithResetTimeout(1*time.Hour),
	)

	// Should not failover before threshold
	require.False(t, policy.ShouldFailover(types.ClusterA, nil))

	// Record failures
	policy.RecordFailure(types.ClusterA)
	require.Equal(t, 1, policy.Failures(types.ClusterA))
	require.False(t, policy.ShouldFailover(types.ClusterA, nil))

	policy.RecordFailure(types.ClusterA)
	require.Equal(t, 2, policy.Failures(types.ClusterA))
	require.False(t, policy.ShouldFailover(types.ClusterA, nil))

	policy.RecordFailure(types.ClusterA)
	require.Equal(t, 3, policy.Failures(types.ClusterA))
	require.True(t, policy.ShouldFailover(types.ClusterA, nil))
}

// TestCircuitBreaker_ZeroValueDoesNotPanic verifies that a bare
// CircuitBreaker{} (bypassing NewCircuitBreaker / finalizeCircuitBreaker,
// which leaves threshold=0 and metrics/logger as nil interfaces) never
// panics. Before the fix, the first RecordFailure call crossed the
// zero-value threshold (1 >= 0) and unconditionally invoked the nil
// metrics/logger interfaces, panicking.
func TestCircuitBreaker_ZeroValueDoesNotPanic(t *testing.T) {
	var cb CircuitBreaker

	require.NotPanics(t, func() {
		cb.RecordFailure(types.ClusterA)
	}, "RecordFailure on zero-value CircuitBreaker must not panic")

	require.NotPanics(t, func() {
		cb.RecordFailure(types.ClusterB)
	}, "RecordFailure on zero-value CircuitBreaker must not panic (cluster B)")

	require.NotPanics(t, func() {
		cb.ShouldFailover(types.ClusterA, nil)
	}, "ShouldFailover on zero-value CircuitBreaker must not panic")

	require.NotPanics(t, func() {
		cb.RecordSuccess(types.ClusterA)
	}, "RecordSuccess on zero-value CircuitBreaker must not panic")

	require.NotPanics(t, func() {
		cb.RecordSuccess(types.ClusterB)
	}, "RecordSuccess on zero-value CircuitBreaker must not panic (cluster B)")
}

// TestCircuitBreaker_ZeroValueShouldFailoverReturnsFalse is a regression
// test for the `threshold <= 0` guard in ShouldFailover: a zero-value
// CircuitBreaker has threshold == 0, so the old `int(failures) < c.threshold`
// check (e.g. 0 < 0, or 1 < 0) is always false and falls through to
// `return true` — a zero-value breaker would report a failover after a
// single RecordFailure, or even with zero recorded failures, despite never
// having (or being able to) record a trip. TestCircuitBreaker_ZeroValueDoesNotPanic
// only asserts ShouldFailover does not panic; it does not capture the
// return value, so it would not catch this on its own.
func TestCircuitBreaker_ZeroValueShouldFailoverReturnsFalse(t *testing.T) {
	var cb CircuitBreaker

	assert.False(t, cb.ShouldFailover(types.ClusterA, nil),
		"zero-value CircuitBreaker must never report a failover with no recorded failures")

	cb.RecordFailure(types.ClusterA)
	assert.False(t, cb.ShouldFailover(types.ClusterA, nil),
		"zero-value CircuitBreaker (threshold=0) must never report a failover, even after RecordFailure")

	cb.RecordFailure(types.ClusterB)
	assert.False(t, cb.ShouldFailover(types.ClusterB, nil),
		"zero-value CircuitBreaker must never report a failover for cluster B either")
}

// TestClusterNameOrID verifies the shared clusterNameOrID helper: it falls
// back to the raw ClusterID when the atomic.Pointer[types.ClusterNames] has
// never been Store'd (the zero-value CircuitBreaker/AdaptiveDualWrite case,
// since neither struct's constructor has run), and defers to
// ClusterNames.Name once a value has been stored.
//
// This path is unreachable through CircuitBreaker's zero-value methods
// directly: RecordFailure/RecordSuccess only read clusterNames when
// justTripped/wasOpen is true, which requires threshold > 0, so a bare
// CircuitBreaker{} never reaches it. Testing the helper directly is the
// cheap, reachable way to lock in the fallback behavior.
func TestClusterNameOrID(t *testing.T) {
	var names atomic.Pointer[types.ClusterNames]

	assert.Equal(t, "A", clusterNameOrID(&names, types.ClusterA),
		"unset clusterNames must fall back to the raw ClusterID")
	assert.Equal(t, "B", clusterNameOrID(&names, types.ClusterB))

	names.Store(&types.ClusterNames{A: "primary", B: "secondary"})
	assert.Equal(t, "primary", clusterNameOrID(&names, types.ClusterA),
		"once clusterNames is set, the configured display name must win")
	assert.Equal(t, "secondary", clusterNameOrID(&names, types.ClusterB))
}

func TestCircuitBreaker_InvalidOptionsPreserveDefaults(t *testing.T) {
	policy := NewCircuitBreaker(
		WithThreshold(0),
		WithResetTimeout(-time.Second),
		WithCircuitBreakerMetrics(nil),
		WithCircuitBreakerLogger(nil),
		WithCircuitBreakerClusterNames(types.ClusterNames{A: "same", B: "same"}),
	)

	assert.Equal(t, 3, policy.threshold)
	assert.Equal(t, 30*time.Second, policy.resetTimeout)
	assert.False(t, policy.MetricsConfigured())
	assert.False(t, policy.LoggerConfigured())
	assert.Equal(t, "A", policy.clusterNames.Load().Name(types.ClusterA))

	policy.RecordFailure(types.ClusterA)
	policy.RecordFailure(types.ClusterA)
	assert.False(t, policy.ShouldFailover(types.ClusterA, nil),
		"invalid threshold must not turn CircuitBreaker into immediate failover")
}

func TestCircuitBreakerChecked_InvalidOptionsReturnJoinedErrors(t *testing.T) {
	policy, err := NewCircuitBreakerChecked(
		WithThreshold(0),
		WithResetTimeout(-time.Second),
		WithCircuitBreakerClusterNames(types.ClusterNames{A: "same", B: "same"}),
	)

	require.Nil(t, policy)
	require.Error(t, err)
	assert.True(t, types.IsOptionError(err))

	var optionErr *types.OptionError
	require.True(t, errors.As(err, &optionErr))
	assert.Equal(t, circuitBreakerComponent, optionErr.Component)

	assert.Contains(t, err.Error(), "WithThreshold")
	assert.Contains(t, err.Error(), "WithResetTimeout")
	assert.Contains(t, err.Error(), "WithCircuitBreakerClusterNames")
}

func TestCircuitBreakerChecked_ValidOptions(t *testing.T) {
	policy, err := NewCircuitBreakerChecked(
		WithThreshold(5),
		WithResetTimeout(2*time.Minute),
		WithCircuitBreakerClusterNames(types.ClusterNames{A: "primary", B: "secondary"}),
	)

	require.NoError(t, err)
	require.NotNil(t, policy)
	assert.Equal(t, 5, policy.threshold)
	assert.Equal(t, 2*time.Minute, policy.resetTimeout)
	assert.Equal(t, "primary", policy.clusterNames.Load().Name(types.ClusterA))
}

func TestCircuitBreaker_InvalidClusterNoop(t *testing.T) {
	policy := NewCircuitBreaker(WithThreshold(1))
	invalid := types.ClusterID("C")

	policy.RecordFailure(invalid)
	assert.Equal(t, 0, policy.Failures(types.ClusterA))
	assert.Equal(t, 0, policy.Failures(types.ClusterB))
	assert.Equal(t, 0, policy.Failures(invalid))
	assert.False(t, policy.ShouldFailover(invalid, nil))

	policy.RecordFailure(types.ClusterA)
	require.True(t, policy.ShouldFailover(types.ClusterA, nil))
	policy.RecordSuccess(invalid)
	assert.True(t, policy.ShouldFailover(types.ClusterA, nil),
		"invalid success must not close a real cluster's circuit")
}

func TestCircuitBreakerSuccessResets(t *testing.T) {
	policy := NewCircuitBreaker(WithThreshold(3))

	// Record some failures
	policy.RecordFailure(types.ClusterA)
	policy.RecordFailure(types.ClusterA)
	require.Equal(t, 2, policy.Failures(types.ClusterA))

	// Success should reset
	policy.RecordSuccess(types.ClusterA)
	require.Equal(t, 0, policy.Failures(types.ClusterA))
	require.False(t, policy.ShouldFailover(types.ClusterA, nil))
}

func TestCircuitBreakerIndependentClusters(t *testing.T) {
	policy := NewCircuitBreaker(WithThreshold(2))

	// Failures on A shouldn't affect B
	policy.RecordFailure(types.ClusterA)
	policy.RecordFailure(types.ClusterA)

	require.Equal(t, 2, policy.Failures(types.ClusterA))
	require.Equal(t, 0, policy.Failures(types.ClusterB))
	require.True(t, policy.ShouldFailover(types.ClusterA, nil))
	require.False(t, policy.ShouldFailover(types.ClusterB, nil))
}

func TestCircuitBreakerResetTimeout(t *testing.T) {
	cb := NewCircuitBreaker(
		WithThreshold(3),
		WithResetTimeout(1*time.Hour),
	)

	// Failures keep accumulating across a quiet gap: the reset timeout no
	// longer discards a stale count, it only decides when an open breaker
	// may be probed.
	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA)
	elapseResetTimeout(t, cb, types.ClusterA)
	_, ok := cb.TryBeginFailoverProbe(types.ClusterA)
	require.False(t, ok, "a closed breaker never reserves a probe")
	cb.RecordFailure(types.ClusterA)
	require.Equal(t, 3, cb.Failures(types.ClusterA))
	require.True(t, cb.ShouldFailover(types.ClusterA, nil), "the third failure trips the breaker")

	_, ok = cb.TryBeginFailoverProbe(types.ClusterA)
	require.False(t, ok, "the reset timeout has not elapsed since the last failure")
	elapseResetTimeout(t, cb, types.ClusterA)
	_, ok = cb.TryBeginFailoverProbe(types.ClusterA)
	require.True(t, ok, "an open breaker reserves a probe once the reset timeout has elapsed")
}

// TestCircuitBreaker_ZeroResetTimeout_AccumulatesFailures verifies that when
// ResetTimeout is set to 0 the breaker still trips at threshold, never
// reserves a probe, and stays open until an explicit RecordSuccess.
func TestCircuitBreaker_ZeroResetTimeout_AccumulatesFailures(t *testing.T) {
	policy := NewCircuitBreaker(
		WithThreshold(3),
		WithResetTimeout(0), // disable timed half-open
	)

	for i := 1; i <= 5; i++ {
		policy.RecordFailure(types.ClusterA)
		require.Equal(t, i, policy.Failures(types.ClusterA),
			"failures must accumulate; resetTimeout=0 disables timed transitions, not counting")
	}
	require.True(t, policy.ShouldFailover(types.ClusterA, nil),
		"breaker must trip at threshold even with resetTimeout=0")

	// Without an explicit RecordSuccess, the breaker stays open indefinitely:
	// a zero reset timeout never reserves a probe either.
	require.True(t, policy.ShouldFailover(types.ClusterA, nil))
	_, ok := policy.TryBeginFailoverProbe(types.ClusterA)
	require.False(t, ok, "resetTimeout=0 never reserves a probe")

	// Explicit success is the only path back to closed.
	policy.RecordSuccess(types.ClusterA)
	require.Equal(t, 0, policy.Failures(types.ClusterA))
	require.False(t, policy.ShouldFailover(types.ClusterA, nil))
}

// TestCircuitBreaker_ConcurrentRecordFailure_NoRace verifies that concurrent
// RecordFailure calls don't data-race on internal state. Run with -race.
func TestCircuitBreaker_ConcurrentRecordFailure_NoRace(t *testing.T) {
	cb := NewCircuitBreaker(
		WithThreshold(3),
		WithResetTimeout(1*time.Hour),
	)

	const goroutines = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			cb.RecordFailure(types.ClusterA)
		}()
	}
	wg.Wait()

	// At least one failure must have been recorded.
	assert.GreaterOrEqual(t, cb.Failures(types.ClusterA), 1)
}

// TestCircuitBreaker_ConcurrentSuccessAndFailure_NoRace verifies that interleaved
// RecordFailure and RecordSuccess calls are race-free. Run with -race.
func TestCircuitBreaker_ConcurrentSuccessAndFailure_NoRace(t *testing.T) {
	cb := NewCircuitBreaker(WithThreshold(3))

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)
	for range goroutines {
		go func() {
			defer wg.Done()
			cb.RecordFailure(types.ClusterA)
		}()
		go func() {
			defer wg.Done()
			cb.RecordSuccess(types.ClusterA)
		}()
	}
	wg.Wait()
	// Final state is non-deterministic but must be internally consistent
	// (the -race detector verifies no data races occurred).
}

// TestCircuitBreaker_MetricEmittedOnce verifies that the "circuit tripped" metric
// is emitted exactly once when the threshold is first crossed, and NOT emitted
// again on subsequent failures above the threshold.
func TestCircuitBreaker_MetricEmittedOnce(t *testing.T) {
	m := testutil.NewTestMetricsCollector()
	cb := NewCircuitBreaker(
		WithThreshold(3),
		WithResetTimeout(1*time.Hour),
		WithCircuitBreakerMetrics(m),
	)

	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA)
	assert.Equal(t, int64(0), m.CircuitBreakerTrips[types.ClusterA], "no trip before threshold")

	// Third failure crosses the threshold — metric fires exactly once.
	cb.RecordFailure(types.ClusterA)
	assert.Equal(t, int64(1), m.CircuitBreakerTrips[types.ClusterA], "trip fires at threshold")

	// Additional failures must not re-emit the trip metric.
	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA)
	assert.Equal(t, int64(1), m.CircuitBreakerTrips[types.ClusterA], "trip metric must not re-fire")

	// Success closes the circuit; state metric must reflect closed.
	cb.RecordSuccess(types.ClusterA)
	assert.Equal(t, 0, m.CircuitBreakerState[types.ClusterA], "circuit must be closed after success")

	// After recovery, a fresh trip sequence must emit the metric again.
	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA)
	assert.Equal(t, int64(2), m.CircuitBreakerTrips[types.ClusterA], "second trip after recovery must fire")
}

// TestCircuitBreaker_SetClusterNames_ThreadSafe verifies that concurrent
// SetClusterNames and RecordFailure/RecordSuccess calls are race-free. Run with -race.
func TestCircuitBreaker_SetClusterNames_ThreadSafe(t *testing.T) {
	cb := NewCircuitBreaker(WithThreshold(1))

	var wg sync.WaitGroup
	wg.Go(func() {
		for range 200 {
			cb.SetClusterNames(types.ClusterNames{A: "primary", B: "secondary"})
		}
	})
	wg.Go(func() {
		for range 200 {
			cb.RecordFailure(types.ClusterA)
		}
	})
	wg.Go(func() {
		for range 200 {
			cb.RecordSuccess(types.ClusterA)
		}
	})

	wg.Wait()
}

func TestCircuitBreaker_EmitsOpenAndClosedEvents(t *testing.T) {
	em := &recordingEmitter{}
	cb := NewCircuitBreaker(WithThreshold(2))
	cb.SetEventEmitter(em)

	cb.RecordFailure(types.ClusterA)
	require.Empty(t, em.kinds(), "below threshold: no event")

	cb.RecordFailure(types.ClusterA) // trips
	require.Equal(t, []types.ClusterEventKind{types.EventCircuitBreakerOpen}, em.kinds())
	openEv := em.snapshot()[0]
	require.Equal(t, types.ClusterA, openEv.Cluster)
	require.Equal(t, 2, openEv.Count)

	cb.RecordFailure(types.ClusterA) // still open: no duplicate
	require.Len(t, em.kinds(), 1)

	cb.RecordSuccess(types.ClusterA) // closes
	require.Equal(t,
		[]types.ClusterEventKind{types.EventCircuitBreakerOpen, types.EventCircuitBreakerClosed},
		em.kinds())

	cb.RecordSuccess(types.ClusterA) // already closed: no duplicate
	require.Len(t, em.kinds(), 2)
}

// TestCircuitBreaker_StalledEmitterCannotReorderTransitions proves the
// causal-order guarantee deterministically: the emitter is parked while
// delivering the Open event, and while it is parked a concurrent
// RecordSuccess performs the Closed transition. Because the transition
// enqueues the event under the state mutex and only delivers afterward,
// the Closed event queues behind Open and RecordSuccess returns without
// waiting on the stalled emitter. An implementation that emitted directly
// after unlocking (instead of queueing) would let Closed be delivered
// before Open, since Closed's own transition finishes and emits while
// Open's emission is still parked.
func TestCircuitBreaker_StalledEmitterCannotReorderTransitions(t *testing.T) {
	em := newGateEmitter()
	cb := NewCircuitBreaker(WithThreshold(1))
	cb.SetEventEmitter(em)

	// Unblock the parked emitter at cleanup even if an assertion below
	// fails first, so the goroutine started below is never left stuck.
	releaseOnce := sync.OnceFunc(func() { close(em.release) })
	t.Cleanup(releaseOnce)

	tripDone := make(chan struct{})
	go func() { cb.RecordFailure(types.ClusterA); close(tripDone) }()

	select {
	case <-em.entered: // drainer is parked inside emitting Open
	case <-time.After(5 * time.Second):
		t.Fatal("Open emission never started")
	}

	// RecordSuccess only needs to enqueue its Closed event and lose the
	// delivery race to the still-parked drainer, so it must return almost
	// immediately. A bounded wait turns a regression that instead runs
	// the emitter under the state mutex (and would therefore block here
	// until the gate is released) into a fast, attributable failure
	// rather than a silent hang.
	successDone := make(chan struct{})
	go func() { cb.RecordSuccess(types.ClusterA); close(successDone) }()
	select {
	case <-successDone:
	case <-time.After(2 * time.Second):
		t.Fatal("RecordSuccess blocked — emitter appears to run under the state mutex")
	}

	releaseOnce()
	select {
	case <-tripDone:
	case <-time.After(5 * time.Second):
		t.Fatal("RecordFailure did not return")
	}

	require.Equal(t,
		[]types.ClusterEventKind{types.EventCircuitBreakerOpen, types.EventCircuitBreakerClosed},
		em.kinds(), "Closed must never overtake its preceding Open")
}

// TestCircuitBreaker_ZeroValueSafeWithEventEmitterInstalled verifies that
// installing an event emitter on a zero-value CircuitBreaker, followed by
// RecordFailure / RecordSuccess, never panics. It does not exercise the
// event enqueue or drain paths: a zero-value breaker has threshold 0, so
// RecordFailure's trip latch is never entered and RecordSuccess never
// observes an open breaker. Reaching the event paths on a zero value is
// not possible through the exported API — this test covers the setter
// and the pre-existing zero-value guards only.
func TestCircuitBreaker_ZeroValueSafeWithEventEmitterInstalled(t *testing.T) {
	var cb CircuitBreaker
	require.NotPanics(t, func() {
		cb.SetEventEmitter(&recordingEmitter{})
		cb.RecordFailure(types.ClusterA)
		cb.RecordSuccess(types.ClusterA)
	})
}

// reentrantCloseEmitter closes the breaker from inside the Open
// emission, exercising the documented-safe reentrancy pattern: an
// emitter is allowed to call back into the policy it is observing.
type reentrantCloseEmitter struct {
	recordingEmitter
	cb   *CircuitBreaker
	once atomic.Bool
}

func (r *reentrantCloseEmitter) EmitClusterEvent(ev types.ClusterEvent) {
	r.recordingEmitter.EmitClusterEvent(ev)
	if ev.Kind == types.EventCircuitBreakerOpen && r.once.CompareAndSwap(false, true) {
		r.cb.RecordSuccess(types.ClusterA)
	}
}

// TestCircuitBreaker_ReentrantEmitterKeepsStateAndMetricConsistent
// verifies that delivering events as the last side-effect step pays off
// under reentrancy: by the time the Open event is handed to the emitter,
// RecordFailure has already written its metrics (gauge=open). The
// reentrant RecordSuccess triggered from inside that Open delivery then
// writes gauge=closed. So by the time both calls have returned, the
// gauge matches the final state (closed), and the events observed are
// Open followed by Closed — never the metric and the state disagreeing,
// and never Closed observed before Open.
func TestCircuitBreaker_ReentrantEmitterKeepsStateAndMetricConsistent(t *testing.T) {
	em := &reentrantCloseEmitter{}
	mc := testutil.NewTestMetricsCollector()
	cb := NewCircuitBreaker(WithThreshold(1), WithCircuitBreakerMetrics(mc))
	em.cb = cb
	cb.SetEventEmitter(em)

	cb.RecordFailure(types.ClusterA) // trips; emitter reenters RecordSuccess mid-drain

	require.Equal(t,
		[]types.ClusterEventKind{types.EventCircuitBreakerOpen, types.EventCircuitBreakerClosed},
		em.kinds(), "reentrant close must be delivered after the open, never before")
	require.Zero(t, cb.Failures(types.ClusterA), "final breaker state must be closed")
	require.EqualValues(t, 0, mc.CircuitBreakerState[types.ClusterA],
		"final gauge must agree with final state: closed (0), not open (2)")
}

// elapseResetTimeout backdates the cluster's last-failure timestamp far
// enough that the next TryBeginFailoverProbe sees an elapsed reset timeout.
// The breaker reads the wall clock directly and has no injectable clock, so
// rewinding the timestamp is what makes these tests deterministic — no
// wall-clock wait, and the configured timeout can stay long enough that a
// slow machine cannot reserve a probe by accident.
func elapseResetTimeout(t *testing.T, cb *CircuitBreaker, cluster types.ClusterID) {
	t.Helper()

	state := cb.stateFor(cluster)
	require.NotNil(t, state, "unknown cluster %q", cluster)
	require.NotZero(t, state.lastFailure.Load(), "no failure recorded yet: nothing to backdate")
	state.lastFailure.Store(time.Now().Add(-2 * cb.resetTimeout).UnixNano())
}

// reserveProbe trips nothing and only reserves: it backdates the last
// failure and returns the token of the reservation it then makes.
func reserveProbe(t *testing.T, cb *CircuitBreaker, cluster types.ClusterID) uint64 {
	t.Helper()

	elapseResetTimeout(t, cb, cluster)
	token, ok := cb.TryBeginFailoverProbe(cluster)
	require.True(t, ok, "an open breaker past its reset timeout must reserve a probe")

	return token
}

// TestCircuitBreaker_ProbeSucceededClosesBreaker covers open → half-open →
// closed through the client-driven probe: the reservation reports half-open
// on the gauge without an event, and the successful completion closes the
// breaker with its own reason.
func TestCircuitBreaker_ProbeSucceededClosesBreaker(t *testing.T) {
	em := &recordingEmitter{}
	mc := testutil.NewTestMetricsCollector()
	logged := make([]string, 0)
	cb := NewCircuitBreaker(
		WithThreshold(2),
		WithResetTimeout(1*time.Hour),
		WithCircuitBreakerMetrics(mc),
		WithCircuitBreakerLogger(&captureLogger{messages: &logged}),
	)
	cb.SetEventEmitter(em)

	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA) // trips
	require.Equal(t, []types.ClusterEventKind{types.EventCircuitBreakerOpen}, em.kinds())
	require.Equal(t, 2, mc.CircuitBreakerState[types.ClusterA], "gauge must report open after the trip")

	token := reserveProbe(t, cb, types.ClusterA)
	require.Equal(t, 1, mc.CircuitBreakerState[types.ClusterA], "a reservation reports half-open")
	require.Equal(t, []types.ClusterEventKind{types.EventCircuitBreakerOpen}, em.kinds(), "half-open emits no event")
	require.True(t, cb.ShouldFailover(types.ClusterA, nil), "half-open still fails over: the probe is the client's, not the caller's")
	_, again := cb.TryBeginFailoverProbe(types.ClusterA)
	require.False(t, again, "one reservation at a time")

	cb.CompleteFailoverProbe(types.ClusterA, token, types.ProbeSucceeded)
	require.Equal(t, int64(1), mc.CircuitBreakerProbes[types.ClusterA][types.BreakerProbeSucceeded])

	events := em.snapshot()
	require.Equal(t,
		[]types.ClusterEventKind{types.EventCircuitBreakerOpen, types.EventCircuitBreakerClosed},
		em.kinds(), "the open span must be closed exactly once")
	require.Equal(t, types.ClusterA, events[1].Cluster)
	require.Equal(t, "probe succeeded", events[1].Reason)
	require.Equal(t, 0, cb.Failures(types.ClusterA))
	require.False(t, cb.ShouldFailover(types.ClusterA, nil))
	require.Equal(t, 0, mc.CircuitBreakerState[types.ClusterA], "gauge must return to closed")
	require.EqualValues(t, 1, mc.CircuitBreakerTrips[types.ClusterA])
	require.Equal(t, []string{
		"circuit breaker tripped",
		"circuit breaker half-open: recovery probe reserved",
		"circuit breaker closed",
	}, logged)

	cb.CompleteFailoverProbe(types.ClusterA, token, types.ProbeFailed)
	require.Zero(t, mc.CircuitBreakerProbes[types.ClusterA][types.BreakerProbeFailed], "a stale token counts nothing")
	require.Equal(t, 0, mc.CircuitBreakerState[types.ClusterA], "a stale token has no effect")
	require.Len(t, em.kinds(), 2)
}

// TestCircuitBreaker_ProbeFailedReopensAndRestartsTimeout covers half-open →
// open: no event, no new trip, the gauge back at open, and the reset timeout
// restarted so the next reservation waits again.
func TestCircuitBreaker_ProbeFailedReopensAndRestartsTimeout(t *testing.T) {
	em := &recordingEmitter{}
	mc := testutil.NewTestMetricsCollector()
	cb := NewCircuitBreaker(
		WithThreshold(2),
		WithResetTimeout(1*time.Hour),
		WithCircuitBreakerMetrics(mc),
	)
	cb.SetEventEmitter(em)

	cb.RecordFailure(types.ClusterB)
	cb.RecordFailure(types.ClusterB) // trips
	token := reserveProbe(t, cb, types.ClusterB)

	cb.CompleteFailoverProbe(types.ClusterB, token, types.ProbeFailed)
	require.Equal(t, int64(1), mc.CircuitBreakerProbes[types.ClusterB][types.BreakerProbeFailed])

	require.Equal(t, []types.ClusterEventKind{types.EventCircuitBreakerOpen}, em.kinds(), "a failed probe emits nothing")
	require.Equal(t, 2, mc.CircuitBreakerState[types.ClusterB], "gauge returns to open")
	require.EqualValues(t, 1, mc.CircuitBreakerTrips[types.ClusterB], "a failed probe is not a new trip")
	require.True(t, cb.ShouldFailover(types.ClusterB, nil))
	_, ok := cb.TryBeginFailoverProbe(types.ClusterB)
	require.False(t, ok, "the failed probe restarted the reset timeout")

	// After the timeout elapses again the breaker can be probed again.
	reserveProbe(t, cb, types.ClusterB)
}

// TestCircuitBreaker_ProbeAbandonedReleasesReservation covers a probe the
// client cancelled: the breaker returns to open without counting a failure
// or restarting the timeout, so another client sharing it can reserve at once.
func TestCircuitBreaker_ProbeAbandonedReleasesReservation(t *testing.T) {
	mc := testutil.NewTestMetricsCollector()
	cb := NewCircuitBreaker(WithThreshold(1), WithResetTimeout(1*time.Hour), WithCircuitBreakerMetrics(mc))

	cb.RecordFailure(types.ClusterA) // trips
	token := reserveProbe(t, cb, types.ClusterA)
	failuresBefore := cb.Failures(types.ClusterA)

	cb.CompleteFailoverProbe(types.ClusterA, token, types.ProbeAbandoned)

	require.Equal(t, 2, mc.CircuitBreakerState[types.ClusterA], "gauge returns to open")
	require.Equal(t, failuresBefore, cb.Failures(types.ClusterA), "abandonment counts nothing")
	require.Equal(t, int64(1), mc.CircuitBreakerProbes[types.ClusterA][types.BreakerProbeAbandoned])
	_, ok := cb.TryBeginFailoverProbe(types.ClusterA)
	require.True(t, ok, "the timeout had already elapsed, so the breaker can be reserved again at once")
}

func TestCircuitBreaker_UnknownProbeOutcomeReleasesReservation(t *testing.T) {
	mc := testutil.NewTestMetricsCollector()
	cb := NewCircuitBreaker(WithThreshold(1), WithResetTimeout(1*time.Hour), WithCircuitBreakerMetrics(mc))

	cb.RecordFailure(types.ClusterA) // trips
	token := reserveProbe(t, cb, types.ClusterA)

	cb.CompleteFailoverProbe(types.ClusterA, token, types.ProbeOutcome(255))

	require.Equal(t, 2, mc.CircuitBreakerState[types.ClusterA], "gauge returns to open")
	require.Equal(t, int64(1), mc.CircuitBreakerProbes[types.ClusterA][types.BreakerProbeAbandoned],
		"an unknown outcome counts as abandoned")
	_, ok := cb.TryBeginFailoverProbe(types.ClusterA)
	require.True(t, ok, "an outcome the breaker does not know cannot leave it half-open")
}

// TestCircuitBreaker_OrdinaryObservationsDuringReservation pins what
// RecordFailure and RecordSuccess do while a probe is in flight: a failure
// keeps counting and leaves the reservation valid; a success closes the
// breaker and makes the reservation stale.
func TestCircuitBreaker_OrdinaryObservationsDuringReservation(t *testing.T) {
	t.Run("failure keeps the reservation", func(t *testing.T) {
		mc := testutil.NewTestMetricsCollector()
		cb := NewCircuitBreaker(WithThreshold(2), WithResetTimeout(1*time.Hour), WithCircuitBreakerMetrics(mc))
		cb.RecordFailure(types.ClusterA)
		cb.RecordFailure(types.ClusterA)
		token := reserveProbe(t, cb, types.ClusterA)

		cb.RecordFailure(types.ClusterA)
		require.Equal(t, 3, cb.Failures(types.ClusterA))
		require.Equal(t, 1, mc.CircuitBreakerState[types.ClusterA], "still half-open")

		cb.CompleteFailoverProbe(types.ClusterA, token, types.ProbeSucceeded)
		require.Equal(t, 0, mc.CircuitBreakerState[types.ClusterA], "the reservation was still valid")
		require.Equal(t, 0, cb.Failures(types.ClusterA))
	})
	t.Run("success closes and makes the reservation stale", func(t *testing.T) {
		em := &recordingEmitter{}
		mc := testutil.NewTestMetricsCollector()
		cb := NewCircuitBreaker(WithThreshold(2), WithResetTimeout(1*time.Hour), WithCircuitBreakerMetrics(mc))
		cb.SetEventEmitter(em)
		cb.RecordFailure(types.ClusterA)
		cb.RecordFailure(types.ClusterA)
		token := reserveProbe(t, cb, types.ClusterA)

		cb.RecordSuccess(types.ClusterA)
		events := em.snapshot()
		require.Equal(t, "operation succeeded", events[1].Reason)
		require.Equal(t, 0, mc.CircuitBreakerState[types.ClusterA])

		cb.CompleteFailoverProbe(types.ClusterA, token, types.ProbeFailed)
		require.Equal(t, 0, mc.CircuitBreakerState[types.ClusterA], "a stale completion cannot re-open a closed breaker")
		require.False(t, cb.ShouldFailover(types.ClusterA, nil))
		require.Len(t, em.kinds(), 2)
	})
}

// TestCircuitBreaker_NeverOpenBreakerNeverReserves guards against probing a
// breaker that is closed: a stale failure count is bookkeeping, not a state.
func TestCircuitBreaker_NeverOpenBreakerNeverReserves(t *testing.T) {
	mc := testutil.NewTestMetricsCollector()
	cb := NewCircuitBreaker(WithThreshold(3), WithResetTimeout(1*time.Hour), WithCircuitBreakerMetrics(mc))

	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA) // still below threshold: never open
	elapseResetTimeout(t, cb, types.ClusterA)
	_, ok := cb.TryBeginFailoverProbe(types.ClusterA)
	require.False(t, ok)
	require.NotContains(t, mc.CircuitBreakerState, types.ClusterA, "no state transition means no gauge write")
	cb.CompleteFailoverProbe(types.ClusterA, 0, types.ProbeSucceeded)
	require.Equal(t, 2, cb.Failures(types.ClusterA), "a completion without a reservation is ignored")
}

// TestCircuitBreaker_SuccessCloseCarriesReason pins the reason on the other
// close path, so the two causes stay distinguishable to a handler.
func TestCircuitBreaker_SuccessCloseCarriesReason(t *testing.T) {
	em := &recordingEmitter{}
	cb := NewCircuitBreaker(WithThreshold(1), WithResetTimeout(1*time.Hour))
	cb.SetEventEmitter(em)

	cb.RecordFailure(types.ClusterB) // trips
	cb.RecordSuccess(types.ClusterB) // closes

	events := em.snapshot()
	require.Equal(t,
		[]types.ClusterEventKind{types.EventCircuitBreakerOpen, types.EventCircuitBreakerClosed},
		em.kinds())
	require.Equal(t, types.ClusterB, events[1].Cluster)
	require.Equal(t, "operation succeeded", events[1].Reason)
}

// gatedStateCollector parks whichever goroutine writes the closed state
// gauge until the test releases it, and signals when a trip has reported its
// counter. Together those two signals let a test hold one transition between
// latching its state change and reporting it, land a second transition on the
// same cluster in that window, and know both happened before it lets the
// first one finish. Everything else delegates to the embedded collector.
//
// Both gates are armed only when the test asks, so the setup failures that
// build the breaker's starting state pass through untouched.
type gatedStateCollector struct {
	*testutil.TestMetricsCollector

	armed      atomic.Bool
	closeGate  atomic.Bool   // consumed by the first armed closed-state write
	tripSignal atomic.Bool   // consumed by the first armed trip counter write
	closing    chan struct{} // closed when the parked write is reached
	tripped    chan struct{} // closed when the second transition has latched
	release    chan struct{} // closed by the test to unpark the write
}

func newGatedStateCollector() *gatedStateCollector {
	return &gatedStateCollector{
		TestMetricsCollector: testutil.NewTestMetricsCollector(),
		closing:              make(chan struct{}),
		tripped:              make(chan struct{}),
		release:              make(chan struct{}),
	}
}

func (g *gatedStateCollector) SetCircuitBreakerState(cluster types.ClusterID, state int) {
	if state == 0 && g.armed.Load() && g.closeGate.CompareAndSwap(false, true) {
		close(g.closing)
		<-g.release
	}
	g.TestMetricsCollector.SetCircuitBreakerState(cluster, state)
}

func (g *gatedStateCollector) IncCircuitBreakerTrip(cluster types.ClusterID) {
	g.TestMetricsCollector.IncCircuitBreakerTrip(cluster)
	// Reported after the trip is latched and the state mutex is released, so
	// this is the earliest point at which the test can know the re-trip has
	// happened. It is also written before the state gauge, so it never
	// depends on the write the other goroutine is parked in.
	if g.armed.Load() && g.tripSignal.CompareAndSwap(false, true) {
		close(g.tripped)
	}
}

// TestCircuitBreaker_ConcurrentRetripKeepsTheStateGaugeOpen covers two
// same-cluster transitions whose post-unlock side effects overlap. A
// successful probe closes the breaker; that goroutine is parked inside the
// closed-state gauge write. While it is parked, two more failures reach the
// threshold and re-trip the breaker.
//
// The breaker ends up open, so the gauge must end at open too. Reporting
// each transition without checking whether a newer one has landed lets the
// parked goroutine write closed after the re-trip wrote open, leaving the
// gauge claiming closed for an open breaker — the inverse of the state the
// events correctly report.
func TestCircuitBreaker_ConcurrentRetripKeepsTheStateGaugeOpen(t *testing.T) {
	mc := newGatedStateCollector()
	em := &recordingEmitter{}
	cb := NewCircuitBreaker(
		WithThreshold(2),
		WithResetTimeout(1*time.Hour),
		WithCircuitBreakerMetrics(mc),
	)
	cb.SetEventEmitter(em)

	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA) // trips
	require.Equal(t, []types.ClusterEventKind{types.EventCircuitBreakerOpen}, em.kinds())
	require.Equal(t, 2, mc.CircuitBreakerState[types.ClusterA], "gauge must report open after the trip")
	token := reserveProbe(t, cb, types.ClusterA)

	// Unpark the gated write at cleanup even if an assertion below fails
	// first, so neither goroutine started here is ever left stuck.
	releaseOnce := sync.OnceFunc(func() { close(mc.release) })
	t.Cleanup(releaseOnce)
	mc.armed.Store(true)

	closeDone := make(chan struct{})
	go func() { cb.CompleteFailoverProbe(types.ClusterA, token, types.ProbeSucceeded); close(closeDone) }()

	select {
	case <-mc.closing:
	case <-time.After(5 * time.Second):
		t.Fatal("the close never reached the state gauge")
	}

	retripDone := make(chan struct{})
	go func() {
		cb.RecordFailure(types.ClusterA)
		cb.RecordFailure(types.ClusterA) // reaches the threshold again
		close(retripDone)
	}()

	select {
	case <-mc.tripped:
	case <-time.After(5 * time.Second):
		t.Fatal("the second pair of failures never re-tripped the breaker")
	}

	releaseOnce()
	for _, done := range []chan struct{}{closeDone, retripDone} {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("a breaker call did not return")
		}
	}

	require.Equal(t, []types.ClusterEventKind{
		types.EventCircuitBreakerOpen,
		types.EventCircuitBreakerClosed,
		types.EventCircuitBreakerOpen,
	}, em.kinds(), "both transitions must be delivered, in the order they happened")
	require.EqualValues(t, 2, mc.CircuitBreakerTrips[types.ClusterA],
		"a superseded report must still count its trip")
	require.Equal(t, 2, mc.CircuitBreakerState[types.ClusterA],
		"the breaker is open, so the gauge must be open: the parked close must not overwrite the re-trip")
}

// TestCircuitBreaker_SupersededTransitionSkipsItsStateReport covers the other
// half of the ordering rule, the half the parked-write test above cannot
// reach through the exported API: a goroutine that latches a transition and
// is descheduled before it reports anything, while a newer transition on the
// same cluster completes in full.
//
// Its report is then stale in both directions — the gauge would claim open
// for a closed breaker, and the log line would announce a trip the breaker
// has already come back from — so it writes neither. The trip counter is not
// state, it is a count of trips that happened, so it still increments.
func TestCircuitBreaker_SupersededTransitionSkipsItsStateReport(t *testing.T) {
	mc := testutil.NewTestMetricsCollector()
	logged := make([]string, 0)
	cb := NewCircuitBreaker(
		WithThreshold(1),
		WithResetTimeout(1*time.Hour),
		WithCircuitBreakerMetrics(mc),
		WithCircuitBreakerLogger(&captureLogger{messages: &logged}),
	)

	cb.RecordFailure(types.ClusterA) // trips
	tripSeq := cb.stateA.seq.Load()

	cb.RecordSuccess(types.ClusterA) // closes
	require.Equal(t, 0, mc.CircuitBreakerState[types.ClusterA])
	require.Equal(t, []string{"circuit breaker tripped", "circuit breaker closed"}, logged)

	// The call the trip would make once rescheduled, with the sequence it
	// captured while it held the state mutex.
	cb.report(&cb.stateA, types.ClusterA, transitionTripped, tripSeq)

	require.Equal(t, 0, mc.CircuitBreakerState[types.ClusterA],
		"the breaker is closed, so a superseded trip must not put the gauge back to open")
	require.Equal(t, []string{"circuit breaker tripped", "circuit breaker closed"}, logged,
		"a superseded trip must not log a trip the breaker has already come back from")
	require.EqualValues(t, 2, mc.CircuitBreakerTrips[types.ClusterA],
		"the trip counter is cumulative, so it counts even a superseded trip")
}

// reentrantTripEmitter re-trips the breaker from inside the Closed emission,
// the mirror of reentrantCloseEmitter for the other transition direction.
type reentrantTripEmitter struct {
	recordingEmitter
	cb   *CircuitBreaker
	once atomic.Bool
}

func (r *reentrantTripEmitter) EmitClusterEvent(ev types.ClusterEvent) {
	r.recordingEmitter.EmitClusterEvent(ev)
	if ev.Kind == types.EventCircuitBreakerClosed && r.once.CompareAndSwap(false, true) {
		r.cb.RecordFailure(types.ClusterA)
	}
}

// TestCircuitBreaker_ReentrantTripAfterProbeCloseKeepsOrder is the
// probe-close analogue of
// TestCircuitBreaker_ReentrantEmitterKeepsStateAndMetricConsistent. A
// successful probe closes the open span, and the handler re-trips the
// breaker from inside that Closed delivery.
//
// Delivering the event as the last side-effect step is what keeps this
// coherent: the close has already written its gauge and log line by the time
// the handler runs, so the reentrant trip's own writes land after them and
// the gauge ends at open.
func TestCircuitBreaker_ReentrantTripAfterProbeCloseKeepsOrder(t *testing.T) {
	em := &reentrantTripEmitter{}
	mc := testutil.NewTestMetricsCollector()
	logged := make([]string, 0)
	cb := NewCircuitBreaker(
		WithThreshold(1),
		WithResetTimeout(1*time.Hour),
		WithCircuitBreakerMetrics(mc),
		WithCircuitBreakerLogger(&captureLogger{messages: &logged}),
	)
	em.cb = cb
	cb.SetEventEmitter(em)

	cb.RecordFailure(types.ClusterA) // trips
	require.Equal(t, []string{"circuit breaker tripped"}, logged)
	token := reserveProbe(t, cb, types.ClusterA)

	cb.CompleteFailoverProbe(types.ClusterA, token, types.ProbeSucceeded) // closes; the handler re-trips mid-drain

	require.Equal(t, []types.ClusterEventKind{
		types.EventCircuitBreakerOpen,
		types.EventCircuitBreakerClosed,
		types.EventCircuitBreakerOpen,
	}, em.kinds(), "the reentrant re-open must be delivered after the close, never before")
	require.Equal(t, []string{
		"circuit breaker tripped",
		"circuit breaker half-open: recovery probe reserved",
		"circuit breaker closed",
		"circuit breaker tripped",
	}, logged, "the close must report before the handler that re-trips runs")
	require.Equal(t, 2, mc.CircuitBreakerState[types.ClusterA],
		"final gauge must agree with the final state: open")
	require.EqualValues(t, 2, mc.CircuitBreakerTrips[types.ClusterA])
}
