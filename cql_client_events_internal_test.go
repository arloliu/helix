package helix

import (
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/types"
)

// internalEventRecorder collects events delivered to a WithOnClusterEvent
// handler for in-package tests. waitFor blocks until a matching event
// arrives or the timeout expires — no polling sleeps.
type internalEventRecorder struct {
	mu     sync.Mutex
	events []types.ClusterEvent
	notify chan struct{}
}

func newInternalEventRecorder() *internalEventRecorder {
	return &internalEventRecorder{notify: make(chan struct{}, 1)}
}

func (r *internalEventRecorder) handler(ev types.ClusterEvent) {
	r.mu.Lock()
	r.events = append(r.events, ev)
	r.mu.Unlock()
	select {
	case r.notify <- struct{}{}:
	default:
	}
}

func (r *internalEventRecorder) waitFor(t *testing.T, pred func(types.ClusterEvent) bool) types.ClusterEvent {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		r.mu.Lock()
		for _, ev := range r.events {
			if pred(ev) {
				r.mu.Unlock()

				return ev
			}
		}
		r.mu.Unlock()
		select {
		case <-r.notify:
		case <-deadline:
			t.Fatal("timeout waiting for matching cluster event")
		}
	}
}

// has reports whether any event matching pred has been delivered so far.
// Used for negative assertions after a bounded settle window.
func (r *internalEventRecorder) has(pred func(types.ClusterEvent) bool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.ContainsFunc(r.events, pred)
}

// emitterSpyPolicy wraps a real failover policy and records SetEventEmitter
// calls. Counting the injection calls is the direct probe: a dispatcher that
// was never started silently swallows events, so asserting only that the
// handler stayed silent cannot distinguish "the emitter was never injected"
// from "it was injected and the event was dropped".
type emitterSpyPolicy struct {
	FailoverPolicy // embed the interface; delegate behavior to a real policy

	setCalls atomic.Int32
}

func (s *emitterSpyPolicy) SetEventEmitter(types.ClusterEventEmitter) {
	s.setCalls.Add(1)
}

// unreachableKindsField returns the value logged under the
// "unreachableKinds" key of the construction-time unreachable-kinds Info
// line, or "" when no such line was logged. The contract is ONE concise
// line, so a second matching Info line fails the test rather than being
// silently ignored.
func (l *captureLogger) unreachableKindsField(t *testing.T) string {
	t.Helper()
	l.Lock()
	defer l.Unlock()
	var fields []string
	for i, msg := range l.infoMsgs {
		if !strings.Contains(msg, "unreachable") {
			continue
		}
		field := ""
		kvs := l.infoKVs[i]
		for j := 0; j+1 < len(kvs); j += 2 {
			if kvs[j] == "unreachableKinds" {
				field, _ = kvs[j+1].(string)
			}
		}
		fields = append(fields, field)
	}
	require.LessOrEqual(t, len(fields), 1,
		"the unreachable-kinds notice must be one Info line, not several")
	if len(fields) == 0 {
		return ""
	}

	return fields[0]
}

// TestClusterEvents_UnreachableKindsLoggedAtConstruction verifies that
// registering a handler while some kinds cannot fire produces one Info
// line at construction listing exactly the unreachable kinds, that
// configuring a producing component removes its kinds from the list, and
// that no line is logged without a handler.
func TestClusterEvents_UnreachableKindsLoggedAtConstruction(t *testing.T) {
	t.Run("dual cluster with only a handler lists every optional kind", func(t *testing.T) {
		logger := &captureLogger{}
		client, err := NewCQLClient(newMockSession(), newMockSession(),
			WithLogger(logger),
			WithOnClusterEvent(func(types.ClusterEvent) {}),
		)
		require.NoError(t, err)
		defer client.Close()

		// failover and read_divergence are reachable in dual-cluster mode
		// (read_divergence is a per-read runtime opt-in), so exactly the
		// component-gated kinds must be listed, in the documented order.
		require.Equal(t,
			"circuit_breaker_open,circuit_breaker_closed,"+
				"write_degraded,write_recovered,"+
				"drain_entered,drain_exited,"+
				"replay_dropped,mirror_replay_dropped,"+
				"session_refresh_attempt,session_refresh_success,session_refresh_error",
			logger.unreachableKindsField(t))
	})

	t.Run("single cluster with only a handler lists all kinds in documented order", func(t *testing.T) {
		logger := &captureLogger{}
		client, err := NewCQLClient(newMockSession(), nil,
			WithLogger(logger),
			WithOnClusterEvent(func(types.ClusterEvent) {}),
		)
		require.NoError(t, err)
		defer client.Close()

		// Single-cluster mode with no optional components makes every
		// kind unreachable, so this pins the COMPLETE documented order —
		// including the two leading kinds that are reachable in
		// dual-cluster mode.
		require.Equal(t,
			"failover,read_divergence,"+
				"circuit_breaker_open,circuit_breaker_closed,"+
				"write_degraded,write_recovered,"+
				"drain_entered,drain_exited,"+
				"replay_dropped,mirror_replay_dropped,"+
				"session_refresh_attempt,session_refresh_success,session_refresh_error",
			logger.unreachableKindsField(t))
	})

	t.Run("ActiveFailover does not count as a circuit breaker", func(t *testing.T) {
		logger := &captureLogger{}
		client, err := NewCQLClient(newMockSession(), newMockSession(),
			WithLogger(logger),
			WithFailoverPolicy(policy.NewActiveFailover()),
			WithOnClusterEvent(func(types.ClusterEvent) {}),
		)
		require.NoError(t, err)
		defer client.Close()

		// ActiveFailover does not implement SetEventEmitter, so the
		// circuit-breaker kinds must remain listed as unreachable.
		kinds := strings.Split(logger.unreachableKindsField(t), ",")
		require.Contains(t, kinds, string(types.EventCircuitBreakerOpen))
		require.Contains(t, kinds, string(types.EventCircuitBreakerClosed))
	})

	t.Run("mirror with a mirror replayer removes mirror_replay_dropped", func(t *testing.T) {
		mirrorTarget, err := NewCQLClient(newMockSession(), nil)
		require.NoError(t, err)
		defer mirrorTarget.Close()

		logger := &captureLogger{}
		client, err := NewCQLClient(newMockSession(), newMockSession(),
			WithLogger(logger),
			WithMirror(mirrorTarget),
			WithMirrorReplayer(&mockReplayer{}),
			WithOnClusterEvent(func(types.ClusterEvent) {}),
		)
		require.NoError(t, err)
		defer client.Close()

		kinds := strings.Split(logger.unreachableKindsField(t), ",")
		require.NotContains(t, kinds, string(types.EventMirrorReplayDropped))
		require.Contains(t, kinds, string(types.EventReplayDropped),
			"replay_dropped needs a Replayer and must remain listed")
	})

	t.Run("configured components remove their kinds", func(t *testing.T) {
		logger := &captureLogger{}
		client, err := NewCQLClient(newMockSession(), newMockSession(),
			WithLogger(logger),
			WithFailoverPolicy(policy.NewCircuitBreaker()),
			WithWriteStrategy(policy.NewAdaptiveDualWrite()),
			WithReplayer(&mockReplayer{}),
			WithOnClusterEvent(func(types.ClusterEvent) {}),
		)
		require.NoError(t, err)
		defer client.Close()

		// Split into exact kind names: "mirror_replay_dropped" contains
		// "replay_dropped" as a substring, so string containment checks
		// would misfire.
		kinds := strings.Split(logger.unreachableKindsField(t), ",")
		require.NotContains(t, kinds, string(types.EventCircuitBreakerOpen))
		require.NotContains(t, kinds, string(types.EventCircuitBreakerClosed))
		require.NotContains(t, kinds, string(types.EventWriteDegraded))
		require.NotContains(t, kinds, string(types.EventWriteRecovered))
		require.NotContains(t, kinds, string(types.EventReplayDropped))
		require.Contains(t, kinds, string(types.EventDrainEntered),
			"kinds whose component is still missing must remain listed")
		require.Contains(t, kinds, string(types.EventMirrorReplayDropped))
	})

	t.Run("no handler produces no line", func(t *testing.T) {
		logger := &captureLogger{}
		client, err := NewCQLClient(newMockSession(), newMockSession(),
			WithLogger(logger),
		)
		require.NoError(t, err)
		defer client.Close()

		require.Empty(t, logger.unreachableKindsField(t))
	})
}

// TestClusterEvents_ConstructorFailureLeaksNothing verifies the dispatcher's
// create-early / start-late lifecycle: when NewCQLClient fails while starting
// the replay worker, the dispatcher exists (it is created before mirror setup
// so mirror callbacks can capture it) but was never started, no delivery
// goroutine was spawned, the handler was never invoked, and SetEventEmitter
// was never called on the caller-owned policy.
func TestClusterEvents_ConstructorFailureLeaksNothing(t *testing.T) {
	handlerCalled := make(chan struct{}, 1)
	workerErr := errors.New("worker start failed")
	spy := &emitterSpyPolicy{FailoverPolicy: policy.NewActiveFailover()}

	client, err := buildCQLClient(
		newMockSession(), newMockSession(),
		WithFailoverPolicy(spy),
		WithReplayWorker(newMockReplayWorker(workerErr)),
		WithOnClusterEvent(func(types.ClusterEvent) {
			select {
			case handlerCalled <- struct{}{}:
			default:
			}
		}),
	)
	require.ErrorIs(t, err, workerErr, "constructor must fail when the replay worker cannot start")
	require.NotNil(t, client)
	events := client.runtime.events
	require.NotNil(t, events, "dispatcher must exist (created before mirror setup)")
	require.False(t, events.started.Load(), "dispatcher must not start on the failure path")
	require.Zero(t, spy.setCalls.Load(),
		"SetEventEmitter must never be called on a failed construction path")

	stopDone := make(chan struct{})
	go func() { events.stop(); close(stopDone) }()
	select {
	case <-stopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("stop() hung — unstarted dispatcher must not join a nonexistent goroutine")
	}
	select {
	case <-handlerCalled:
		t.Fatal("handler must never be invoked when construction fails before start/injection")
	default:
	}
}

// emitterSpyWriteStrategy is the write-strategy counterpart of
// emitterSpyPolicy. It additionally implements IsDegraded/RecordProbeSuccess
// so the recovery-probe goroutines start for it, and it parks the constructor
// inside SetEventEmitter so a test can inspect, at that exact moment, whether
// any probe goroutine is already running.
type emitterSpyWriteStrategy struct {
	WriteStrategy // embed the interface; delegate behavior to a real strategy

	setCalls   atomic.Int32
	emitterSet atomic.Bool
	violation  atomic.Bool
	probedOnce sync.Once
	probed     chan struct{}

	// parked is closed when SetEventEmitter is entered; the call then waits
	// for release before returning.
	parkedOnce sync.Once
	parked     chan struct{}
	release    chan struct{}
}

func (s *emitterSpyWriteStrategy) SetEventEmitter(types.ClusterEventEmitter) {
	s.setCalls.Add(1)
	// Park before recording the emitter, so any probe that runs while the
	// constructor is held here is registered as a violation.
	s.parkedOnce.Do(func() { close(s.parked) })
	<-s.release
	s.emitterSet.Store(true)
}

func (s *emitterSpyWriteStrategy) IsDegraded(types.ClusterID) bool {
	if !s.emitterSet.Load() {
		s.violation.Store(true)
	}
	s.probedOnce.Do(func() { close(s.probed) })

	return false
}

func (s *emitterSpyWriteStrategy) RecordProbeSuccess(types.ClusterID) {}

// TestClusterEvents_SuccessfulConstructionInjectsBothSlotsBeforeProbes is the
// success-path counterpart of the constructor-failure test: both policy slots
// must receive the emitter exactly once by the time NewCQLClient returns, and
// the recovery-probe goroutines must not exist yet while injection is
// happening.
//
// The constructor runs on its own goroutine and is parked inside the write
// strategy's SetEventEmitter. A 1 ms probe interval means that if the probe
// goroutines had already been launched, at least one of them would tick many
// times over during the park window and be observed — so the check does not
// depend on winning a race. The default interval is 2 s; starting from
// DefaultRecoveryProbe keeps Timeout and Probe at valid values.
func TestClusterEvents_SuccessfulConstructionInjectsBothSlotsBeforeProbes(t *testing.T) {
	fpSpy := &emitterSpyPolicy{FailoverPolicy: policy.NewActiveFailover()}
	wsSpy := &emitterSpyWriteStrategy{
		WriteStrategy: policy.NewSyncDualWrite(),
		probed:        make(chan struct{}),
		parked:        make(chan struct{}),
		release:       make(chan struct{}),
	}
	probe := DefaultRecoveryProbe()
	probe.Interval = time.Millisecond

	type constructed struct {
		client *CQLClient
		err    error
	}
	built := make(chan constructed, 1)
	go func() {
		client, err := NewCQLClient(
			newMockSession(), newMockSession(),
			WithFailoverPolicy(fpSpy),
			WithWriteStrategy(wsSpy),
			WithRecoveryProbe(probe),
			WithOnClusterEvent(func(types.ClusterEvent) {}),
		)
		built <- constructed{client: client, err: err}
	}()

	select {
	case <-wsSpy.parked:
	case <-time.After(5 * time.Second):
		t.Fatal("SetEventEmitter was never called on the write strategy")
	}

	// The constructor is held at the injection point. No probe may run yet.
	select {
	case <-wsSpy.probed:
		t.Fatal("a recovery probe ran before the emitter was installed — injection must precede probe launch")
	case <-time.After(50 * time.Millisecond):
	}
	close(wsSpy.release)

	res := <-built
	require.NoError(t, res.err)
	t.Cleanup(res.client.Close)

	require.Equal(t, int32(1), fpSpy.setCalls.Load(),
		"failover-policy slot must be injected exactly once before return")
	require.Equal(t, int32(1), wsSpy.setCalls.Load(),
		"write-strategy slot must be injected exactly once before return")

	select {
	case <-wsSpy.probed:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery probe never observed the strategy — check the probe interval option")
	}
	require.False(t, wsSpy.violation.Load(),
		"a recovery-probe observation ran before the emitter was installed — injection must precede probe launch")
}

// clusterEventMetricsSpy extends mockMetricsCollector with the optional
// types.ClusterEventMetrics method. Local spy: internal helix tests
// cannot import test/testutil (import cycle), the same workaround as
// mockMetricsCollector itself.
type clusterEventMetricsSpy struct {
	mockMetricsCollector

	eventsDropped atomic.Int64
}

func (m *clusterEventMetricsSpy) AddClusterEventsDropped(n int) {
	m.eventsDropped.Add(int64(n))
}

// TestClusterEvents_DropMetricWiredThroughConstructor proves the
// production wiring end to end: a collector passed via WithMetrics that
// implements types.ClusterEventMetrics must receive the dispatcher's drop
// total without the test ever assigning the dispatcher's metrics field —
// removing the createEventDispatcher attachment must fail this test. The
// handler is wedged so the dispatcher goroutine cannot reconcile, the
// buffer is overflowed, and Close performs the final reconcile.
func TestClusterEvents_DropMetricWiredThroughConstructor(t *testing.T) {
	m := &clusterEventMetricsSpy{}
	block := make(chan struct{})
	first := make(chan struct{})
	var once sync.Once

	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithMetrics(m),
		WithOnClusterEvent(func(types.ClusterEvent) {
			once.Do(func() { close(first) })
			<-block // wedge the consumer
		}),
	)
	require.NoError(t, err)
	d := client.runtime.events
	require.NotNil(t, d)

	d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventFailover})
	select {
	case <-first:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never entered")
	}
	for range eventBufferSize + 10 {
		d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventReplayDropped})
	}
	require.GreaterOrEqual(t, d.dropped.Load(), uint64(10), "overflow must be counted")
	require.Zero(t, m.eventsDropped.Load(),
		"the metric must not be written from the emit hot path")

	close(block)
	client.Close()
	require.Equal(t, d.dropped.Load(), uint64(m.eventsDropped.Load()),
		"Close must reconcile the full internal drop total into the WithMetrics collector")
}

// adaptiveMetricsSpy extends mockMetricsCollector with the optional
// types.AdaptiveWriteMetrics methods. Local spy for the same
// import-cycle reason as clusterEventMetricsSpy.
type adaptiveMetricsSpy struct {
	mockMetricsCollector

	mu            sync.Mutex
	degradedState map[types.ClusterID]bool
	degraded      map[types.ClusterID]int64
	recovered     map[types.ClusterID]int64
}

func (m *adaptiveMetricsSpy) SetWriteDegraded(cluster types.ClusterID, degraded bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.degradedState == nil {
		m.degradedState = make(map[types.ClusterID]bool)
	}
	m.degradedState[cluster] = degraded
}

func (m *adaptiveMetricsSpy) IncWriteDegraded(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.degraded == nil {
		m.degraded = make(map[types.ClusterID]int64)
	}
	m.degraded[cluster]++
}

func (m *adaptiveMetricsSpy) IncWriteRecovered(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.recovered == nil {
		m.recovered = make(map[types.ClusterID]int64)
	}
	m.recovered[cluster]++
}

func (m *adaptiveMetricsSpy) snapshot(cluster types.ClusterID) (state bool, degraded, recovered int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.degradedState[cluster], m.degraded[cluster], m.recovered[cluster]
}

// TestClusterEvents_AdaptiveWriteMetricsAutoInjectedThroughClient proves
// metric auto-injection through the production constructor: an
// AdaptiveDualWrite built WITHOUT WithAdaptiveMetrics must record its
// transitions into the WithMetrics collector after NewCQLClient threads
// it in via autoInjectMetricsAndLogger — not only when a collector is
// handed to the policy directly.
func TestClusterEvents_AdaptiveWriteMetricsAutoInjectedThroughClient(t *testing.T) {
	m := &adaptiveMetricsSpy{}
	strategy := policy.NewAdaptiveDualWrite()
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithWriteStrategy(strategy),
		WithMetrics(m),
	)
	require.NoError(t, err)
	defer client.Close()

	// ForceDegrade/ForceRecover record their metrics synchronously before
	// returning, so the assertions need no waiting.
	strategy.ForceDegrade(types.ClusterA)
	state, degraded, recovered := m.snapshot(types.ClusterA)
	require.True(t, state, "auto-injected collector must receive the degraded gauge")
	require.Equal(t, int64(1), degraded)
	require.Zero(t, recovered)

	strategy.ForceRecover(types.ClusterA)
	state, degraded, recovered = m.snapshot(types.ClusterA)
	require.False(t, state, "recovery must flip the gauge back to healthy")
	require.Equal(t, int64(1), degraded)
	require.Equal(t, int64(1), recovered)
}
