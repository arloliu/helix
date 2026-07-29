package policy

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/types"
)

// recordingEmitter is a synchronous test emitter. Shared by the circuit
// breaker and adaptive-write event tests in this package.
type recordingEmitter struct {
	mu     sync.Mutex
	events []types.ClusterEvent
}

func (r *recordingEmitter) EmitClusterEvent(ev types.ClusterEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

func (r *recordingEmitter) kinds() []types.ClusterEventKind {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]types.ClusterEventKind, 0, len(r.events))
	for _, ev := range r.events {
		out = append(out, ev.Kind)
	}
	return out
}

func (r *recordingEmitter) snapshot() []types.ClusterEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]types.ClusterEvent, len(r.events))
	copy(out, r.events)
	return out
}

// gateEmitter blocks its FIRST EmitClusterEvent invocation until
// released — used to prove ordering holds even when the drainer is
// stalled mid-emission. The gate uses an atomic first-call flag, NOT
// sync.Once: Once.Do would make a second concurrent invocation WAIT for
// the first to return, which would let a broken post-unlock
// direct-emission implementation hang instead of visibly reordering —
// the atomic flag lets later invocations proceed so a regression
// produces a failing order assertion, not a stuck test.
type gateEmitter struct {
	recordingEmitter
	entered chan struct{}
	release chan struct{}
	first   atomic.Bool
}

func newGateEmitter() *gateEmitter {
	return &gateEmitter{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (g *gateEmitter) EmitClusterEvent(ev types.ClusterEvent) {
	if g.first.CompareAndSwap(false, true) {
		close(g.entered)
		<-g.release
	}
	g.recordingEmitter.EmitClusterEvent(ev)
}

func TestEventOutbox_NoEmitterIsNoop(t *testing.T) {
	var o eventOutbox // zero value: no emitter
	require.NotPanics(t, func() {
		o.enqueue(types.ClusterEvent{Kind: types.EventCircuitBreakerOpen})
		o.drain()
	})
	require.False(t, o.enabled())
}

func TestEventOutbox_EmitsInEnqueueOrder(t *testing.T) {
	var o eventOutbox
	em := &recordingEmitter{}
	o.setEmitter(em)

	o.enqueue(types.ClusterEvent{Kind: types.EventCircuitBreakerOpen})
	o.enqueue(types.ClusterEvent{Kind: types.EventCircuitBreakerClosed})
	o.drain()

	require.Equal(t,
		[]types.ClusterEventKind{types.EventCircuitBreakerOpen, types.EventCircuitBreakerClosed},
		em.kinds())
}

// TestEventOutbox_StalledDrainerPreservesOrder: drainer 1 is parked
// inside the emitter while another goroutine enqueues + drains. The
// second drain must not emit (only one goroutine delivers events at a
// time), and after release, delivery order must match enqueue order.
func TestEventOutbox_StalledDrainerPreservesOrder(t *testing.T) {
	var o eventOutbox
	em := newGateEmitter()
	o.setEmitter(em)

	o.enqueue(types.ClusterEvent{Kind: types.EventCircuitBreakerOpen})
	drain1Done := make(chan struct{})
	go func() { o.drain(); close(drain1Done) }()

	select {
	case <-em.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("drainer never reached the emitter")
	}

	// Concurrent transition while drainer 1 is stalled.
	o.enqueue(types.ClusterEvent{Kind: types.EventCircuitBreakerClosed})
	o.drain() // CAS fails: drainer 1 owns emission; must return without emitting

	close(em.release)
	select {
	case <-drain1Done:
	case <-time.After(5 * time.Second):
		t.Fatal("drainer 1 did not finish")
	}

	require.Equal(t,
		[]types.ClusterEventKind{types.EventCircuitBreakerOpen, types.EventCircuitBreakerClosed},
		em.kinds(), "stalled drainer must still deliver in enqueue order, nothing stranded")
}

// TestEventOutbox_ReentrantEmitterDoesNotDeadlock: an emitter that calls
// back into the outbox (as a reentrant policy method would) must enqueue
// and return, not deadlock.
type reentrantEmitter struct {
	recordingEmitter
	outbox *eventOutbox
	once   sync.Once
}

func (r *reentrantEmitter) EmitClusterEvent(ev types.ClusterEvent) {
	r.once.Do(func() {
		r.outbox.enqueue(types.ClusterEvent{Kind: types.EventCircuitBreakerClosed})
		r.outbox.drain() // must return immediately (drainer already active)
	})
	r.recordingEmitter.EmitClusterEvent(ev)
}

func TestEventOutbox_ReentrantEmitterDoesNotDeadlock(t *testing.T) {
	var o eventOutbox
	em := &reentrantEmitter{outbox: &o}
	o.setEmitter(em)

	done := make(chan struct{})
	go func() {
		o.enqueue(types.ClusterEvent{Kind: types.EventCircuitBreakerOpen})
		o.drain()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reentrant emitter deadlocked the outbox")
	}
	require.Equal(t,
		[]types.ClusterEventKind{types.EventCircuitBreakerOpen, types.EventCircuitBreakerClosed},
		em.kinds(), "reentrant enqueue must be delivered after the current event")
}

func TestEventOutbox_TimestampStampedAtEnqueue(t *testing.T) {
	var o eventOutbox
	em := &recordingEmitter{}
	o.setEmitter(em)
	o.enqueue(types.ClusterEvent{Kind: types.EventWriteDegraded})
	o.drain()
	require.False(t, em.snapshot()[0].Timestamp.IsZero())
}

func TestEventOutbox_EnqueueReportsAppend(t *testing.T) {
	var o eventOutbox
	require.False(t, o.enqueue(types.ClusterEvent{Kind: types.EventWriteDegraded}),
		"disabled outbox must not append (callers skip drain entirely)")
	o.setEmitter(&recordingEmitter{})
	require.True(t, o.enqueue(types.ClusterEvent{Kind: types.EventWriteDegraded}))
}

// panicOnceEmitter panics on its first invocation only.
type panicOnceEmitter struct {
	recordingEmitter
	panicked atomic.Bool
}

func (p *panicOnceEmitter) EmitClusterEvent(ev types.ClusterEvent) {
	if p.panicked.CompareAndSwap(false, true) {
		panic("emitter boom")
	}
	p.recordingEmitter.EmitClusterEvent(ev)
}

// TestEventOutbox_PanickingEmitterDoesNotPoisonDraining: a panic inside
// the emitter must lose at most that one event (counted) — the draining
// flag must be released so future transitions still emit.
func TestEventOutbox_PanickingEmitterDoesNotPoisonDraining(t *testing.T) {
	var o eventOutbox
	em := &panicOnceEmitter{}
	o.setEmitter(em)

	o.enqueue(types.ClusterEvent{Kind: types.EventCircuitBreakerOpen})
	require.NotPanics(t, o.drain, "emitter panics must not propagate to the transition path")
	require.Equal(t, uint64(1), o.dropped.Load(), "the panicking event is counted as dropped")

	o.enqueue(types.ClusterEvent{Kind: types.EventCircuitBreakerClosed})
	o.drain()
	require.Equal(t, []types.ClusterEventKind{types.EventCircuitBreakerClosed}, em.kinds(),
		"the outbox must remain drainable after an emitter panic")
}

// TestEventOutbox_OverflowIsBoundedAndCounted: a wedged emitter must not
// let pending grow without bound — beyond outboxCap, newest events are
// dropped and counted.
func TestEventOutbox_OverflowIsBoundedAndCounted(t *testing.T) {
	var o eventOutbox
	o.setEmitter(&recordingEmitter{})
	// Enqueue without draining (simulates transitions piling up while a
	// wedged emitter holds delivery elsewhere).
	for range outboxCap + 5 {
		o.enqueue(types.ClusterEvent{Kind: types.EventCircuitBreakerOpen})
	}
	o.mu.Lock()
	pending := len(o.pending)
	o.mu.Unlock()
	require.Equal(t, outboxCap, pending, "pending must be capped at outboxCap")
	require.Equal(t, uint64(5), o.dropped.Load(), "overflow must be counted")
}
