package helix

import (
	"errors"
	"slices"
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

// TestClusterEvents_ConstructorFailureLeaksNothing verifies the dispatcher's
// create-early / start-late lifecycle: when NewCQLClient fails while starting
// the replay worker, the dispatcher exists (it is created before mirror setup
// so mirror callbacks can capture it) but was never started, no delivery
// goroutine was spawned, the handler was never invoked, and SetEventEmitter
// was never called on the caller-owned policy.
func TestClusterEvents_ConstructorFailureLeaksNothing(t *testing.T) {
	var capturedCfg *ClientConfig
	handlerCalled := make(chan struct{}, 1)
	workerErr := errors.New("worker start failed")
	spy := &emitterSpyPolicy{FailoverPolicy: policy.NewActiveFailover()}

	_, err := NewCQLClient(
		newMockSession(), newMockSession(),
		WithFailoverPolicy(spy),
		WithReplayWorker(newMockReplayWorker(workerErr)),
		WithOnClusterEvent(func(types.ClusterEvent) {
			select {
			case handlerCalled <- struct{}{}:
			default:
			}
		}),
		WithConfigCaptureForTest(&capturedCfg),
	)
	require.ErrorIs(t, err, workerErr, "constructor must fail when the replay worker cannot start")
	require.NotNil(t, capturedCfg)
	require.NotNil(t, capturedCfg.events, "dispatcher must exist (created before mirror setup)")
	require.False(t, capturedCfg.events.started.Load(), "dispatcher must not start on the failure path")
	require.Zero(t, spy.setCalls.Load(),
		"SetEventEmitter must never be called on a failed construction path")

	stopDone := make(chan struct{})
	go func() { capturedCfg.events.stop(); close(stopDone) }()
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

	var capturedCfg *ClientConfig
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithMetrics(m),
		WithOnClusterEvent(func(types.ClusterEvent) {
			once.Do(func() { close(first) })
			<-block // wedge the consumer
		}),
		WithConfigCaptureForTest(&capturedCfg),
	)
	require.NoError(t, err)
	require.NotNil(t, capturedCfg)
	d := capturedCfg.events
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
