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
	policy := NewCircuitBreaker(
		WithThreshold(3),
		WithResetTimeout(10*time.Millisecond),
	)

	// Record failures
	policy.RecordFailure(types.ClusterA)
	policy.RecordFailure(types.ClusterA)
	require.Equal(t, 2, policy.Failures(types.ClusterA))

	// Wait for reset timeout
	time.Sleep(20 * time.Millisecond)

	// Next failure should reset counter
	policy.RecordFailure(types.ClusterA)
	require.Equal(t, 1, policy.Failures(types.ClusterA))
}

// TestCircuitBreaker_ZeroResetTimeout_AccumulatesFailures verifies that when
// ResetTimeout is set to 0, the timed half-open transition is disabled and
// the breaker still trips at threshold. ShouldFailover documents that
// resetTimeout=0 disables timed transitions, but RecordFailure must honor
// the same semantics — without this, every failure after the first resets
// failures to 1 and the breaker can never accumulate to threshold.
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

	// Without an explicit RecordSuccess, the breaker stays open indefinitely.
	require.True(t, policy.ShouldFailover(types.ClusterA, nil))

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
