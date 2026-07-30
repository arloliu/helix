package helix

import (
	"math"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/types"
)

// Compile-time assertion that the dispatcher satisfies the emitter
// interface injected into policies.
var _ types.ClusterEventEmitter = (*eventDispatcher)(nil)

// collectingHandler records delivered events and closes doneCh once
// `expect` events have arrived (event-driven wait, no sleeps).
type collectingHandler struct {
	mu     sync.Mutex
	events []types.ClusterEvent
	expect int
	doneCh chan struct{}
	once   sync.Once
}

func newCollectingHandler(expect int) *collectingHandler {
	return &collectingHandler{expect: expect, doneCh: make(chan struct{})}
}

func (h *collectingHandler) handle(ev types.ClusterEvent) {
	h.mu.Lock()
	h.events = append(h.events, ev)
	reached := len(h.events) >= h.expect
	h.mu.Unlock()
	if reached {
		h.once.Do(func() { close(h.doneCh) })
	}
}

func (h *collectingHandler) wait(t *testing.T) []types.ClusterEvent {
	t.Helper()
	select {
	case <-h.doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for events")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]types.ClusterEvent, len(h.events))
	copy(out, h.events)

	return out
}

// isClosed reports whether ch is already closed, without blocking. Used
// to sample lifecycle state at an exact instant instead of waiting.
func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// waitStopBlockedOnLifecycleLock returns once a goroutine that the
// calling test started is inside stop() waiting on a mutex. Observing
// that from the goroutine dump is what makes the caller's ordering exact:
// it proves stop() has run everything above the lock. No handshake inside
// stop() can prove the same thing, because any signal stop() could send
// would itself sit above the code being pinned down, leaving whatever
// follows it unconstrained. A mutex exposes nothing about its waiters, so
// the dump is the only source for this.
//
// Matching on the creating test keeps a stop goroutine from some other
// test out of the answer. The wait is bounded and fails loudly, so a
// future change to these runtime frame names cannot turn into a silent
// pass.
func waitStopBlockedOnLifecycleLock(t *testing.T) {
	t.Helper()
	createdBy := "created by github.com/arloliu/helix." + t.Name()
	deadline := time.Now().Add(5 * time.Second)
	buf := make([]byte, 64<<10)
	for time.Now().Before(deadline) {
		var n int
		for {
			n = runtime.Stack(buf, true)
			if n < len(buf) {
				break
			}
			buf = make([]byte, 2*len(buf)) // dump was truncated; retry bigger
		}
		// Goroutines are separated by a blank line in the dump.
		for g := range strings.SplitSeq(string(buf[:n]), "\n\n") {
			if strings.Contains(g, createdBy) &&
				strings.Contains(g, "(*eventDispatcher).stop(") &&
				strings.Contains(g, "sync.(*Mutex).Lock(") {
				return
			}
		}
		runtime.Gosched()
	}
	t.Fatal("stop() never reached the lifecycle lock")
}

// waitSignal receives one signal from ch or fails the test.
func waitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for %s", what)
	}
}

func TestEventDispatcher_DeliversInOrderAndStampsTimestamp(t *testing.T) {
	h := newCollectingHandler(3)
	d := newEventDispatcher(h.handle, logging.NewNopLogger())
	d.start()
	t.Cleanup(d.stop)

	d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventFailover})
	d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventDrainEntered})
	d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventDrainExited})

	got := h.wait(t)
	require.Len(t, got, 3)
	require.Equal(t, types.EventFailover, got[0].Kind)
	require.Equal(t, types.EventDrainEntered, got[1].Kind)
	require.Equal(t, types.EventDrainExited, got[2].Kind)
	for _, ev := range got {
		require.False(t, ev.Timestamp.IsZero(), "dispatcher must stamp Timestamp")
	}
}

func TestEventDispatcher_StopDrainsPendingEvents(t *testing.T) {
	h := newCollectingHandler(5)
	d := newEventDispatcher(h.handle, logging.NewNopLogger())

	// Emit BEFORE start: events buffer in the channel.
	for range 5 {
		d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventReplayDropped})
	}
	d.start()
	d.stop()

	// stop() must not return until the handler has received all 5 buffered
	// events, so doneCh must already be closed at this point — check
	// non-blockingly instead of through h.wait's 5s window, which would
	// also pass if stop returned early and delivery finished afterwards.
	select {
	case <-h.doneCh:
	default:
		t.Fatal("stop() returned before all buffered events were delivered")
	}

	got := h.wait(t)
	require.Len(t, got, 5)
}

func TestEventDispatcher_StopWithoutStartDoesNotHang(t *testing.T) {
	d := newEventDispatcher(func(types.ClusterEvent) {}, logging.NewNopLogger())
	d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventFailover}) // buffered, never delivered
	done := make(chan struct{})
	go func() { d.stop(); close(done) }()
	waitSignal(t, done, "stop() on never-started dispatcher")
	require.Equal(t, uint64(1), d.dropped.Load(),
		"undelivered buffered event must be counted as dropped")
}

func TestEventDispatcher_StartAfterStopIsNoop(t *testing.T) {
	d := newEventDispatcher(func(types.ClusterEvent) {}, logging.NewNopLogger())
	d.stop()
	d.start() // must not spawn a goroutine
	require.False(t, d.started.Load(), "start after stop must be a no-op")
}

func TestEventDispatcher_ConcurrentDoubleStopObservesCompletion(t *testing.T) {
	inHandler := make(chan struct{})
	release := make(chan struct{})
	var delivered sync.WaitGroup
	delivered.Add(1)
	d := newEventDispatcher(func(_ types.ClusterEvent) {
		close(inHandler)
		<-release
		delivered.Done()
	}, logging.NewNopLogger())
	d.start()
	d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventFailover})
	waitSignal(t, inHandler, "handler entry")

	stops := make(chan struct{}, 2)
	for range 2 {
		go func() { d.stop(); stops <- struct{}{} }()
	}
	close(release)
	// Both concurrent stop calls must return.
	waitSignal(t, stops, "first stop")
	waitSignal(t, stops, "second stop")
	delivered.Wait()
}

// TestEventDispatcher_PostStopEmitNeverJoinsInflight pins down the rule
// that makes stop()'s in-flight drain terminate: a caller that arrives
// after the stopped flag is set must return at the very first check,
// without registering in the in-flight counter. If it registered first
// and only then noticed the shutdown, a steady stream of such callers
// could hold the counter above zero and stop() could spin in its drain
// loop indefinitely.
//
// The rule is checked from both ends. First a stream of post-stop calls
// runs while the in-flight counter is sampled: a caller that registers
// before it checks holds the counter above zero for a few instructions
// of every call it makes, which is far too short to park but easy to
// catch when sampled alongside thousands of calls. Then the admission
// gate is armed and left closed, so a caller that gets past the check
// while registered is held at the gate and announces itself on the entry
// channel instead of slipping through.
//
// Emitting after stop() has returned exercises the same code path as
// emitting while stop() is still draining: the emit path only reads the
// stopped flag, which is already set in both cases.
func TestEventDispatcher_PostStopEmitNeverJoinsInflight(t *testing.T) {
	const postStopEmits = 10_000

	d := newEventDispatcher(func(types.ClusterEvent) {}, logging.NewNopLogger())
	d.start()
	d.stop()

	streamDone := make(chan struct{})
	go func() {
		for range postStopEmits {
			d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventFailover})
		}
		close(streamDone)
	}()
	var inflight int64
	for !isClosed(streamDone) {
		if n := d.emitting.Load(); n != 0 {
			inflight = n
			break
		}
	}
	waitSignal(t, streamDone, "the post-stop emission stream")
	require.Zero(t, inflight,
		"a post-stop caller registered in the in-flight counter instead of dropping at the stopped check")

	// Safe to arm now: the stream has been joined, the consumer is gone,
	// no emitter is in flight, and the goroutine below is created after
	// these writes.
	d.testEmitGate = make(chan struct{})
	d.testEmitEntered = make(chan struct{}, 1)
	var gateOnce sync.Once
	releaseGate := func() { gateOnce.Do(func() { close(d.testEmitGate) }) }

	emitReturned := make(chan struct{})
	go func() {
		d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventFailover})
		close(emitReturned)
	}()
	// Registered before any assertion below, so a failing run still frees
	// the emitter instead of leaving it parked at the gate.
	t.Cleanup(func() {
		releaseGate()
		select {
		case <-emitReturned:
		case <-time.After(5 * time.Second):
			t.Error("post-stop emitter never returned after the gate was released")
		}
	})

	select {
	case <-emitReturned:
		// expected: dropped at the stopped check, never registered.
	case <-d.testEmitEntered:
		t.Fatal("a post-stop caller registered as in flight instead of dropping at the stopped check")
	case <-time.After(5 * time.Second):
		t.Fatal("post-stop emit neither returned nor reached the admission gate")
	}

	require.Zero(t, d.emitting.Load(), "a post-stop caller must leave the in-flight counter at zero")
	require.Equal(t, uint64(postStopEmits+1), d.dropped.Load(),
		"every post-stop emission must be counted as dropped")
}

// TestEventDispatcher_StopCompletesUnderSustainedEmission runs the whole
// shutdown against a live stream of emissions: every emitter confirms it
// has completed a call and is looping before stop() is called, so the
// stream provably exists for the entire shutdown and callers keep
// arriving long after the stopped flag is set. stop() must still return,
// and the accounting must balance across the transition.
//
// This is the end-to-end scenario; the rule it depends on is proved
// directly in TestEventDispatcher_PostStopEmitNeverJoinsInflight.
func TestEventDispatcher_StopCompletesUnderSustainedEmission(t *testing.T) {
	const emitters = 64

	var emitted, delivered atomic.Int64
	d := newEventDispatcher(func(types.ClusterEvent) {
		delivered.Add(1)
	}, logging.NewNopLogger())
	d.start()

	quitEmitters := make(chan struct{})
	var quitOnce sync.Once
	stopEmitters := func() { quitOnce.Do(func() { close(quitEmitters) }) }
	var running sync.WaitGroup
	ready := make(chan struct{}, emitters)
	for range emitters {
		running.Go(func() {
			// Announce readiness only after a completed call, so waiting on
			// this channel proves the goroutine is emitting, not merely
			// scheduled.
			d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventFailover})
			emitted.Add(1)
			ready <- struct{}{}
			for {
				select {
				case <-quitEmitters:
					return
				default:
				}
				d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventFailover})
				emitted.Add(1)
			}
		})
	}
	// Registered before any assertion, so a failing run still winds the
	// emitters down instead of leaving them spinning.
	t.Cleanup(func() {
		stopEmitters()
		running.Wait()
	})
	for range emitters {
		waitSignal(t, ready, "every emitter to reach its loop")
	}

	stopDone := make(chan struct{})
	go func() { d.stop(); close(stopDone) }()

	// The emitters keep running until stop() reports completion, so the
	// post-stop arrival stream never lets up while stop is draining.
	select {
	case <-stopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("stop() did not complete while post-stop emissions kept arriving")
	}
	stopEmitters()
	running.Wait()

	// Accounting stays exact across the shutdown: the consumer was joined
	// before stop() returned, so the delivered count is final, and every
	// remaining emission — including the ones that arrived after the
	// shutdown flag was set — landed in the drop count.
	require.Equal(t, emitted.Load(), delivered.Load()+int64(d.dropped.Load()),
		"every emission must be delivered or counted as dropped")
}

// TestEventDispatcher_StopJoinsConsumerPublishedWhileWaitingForLock
// forces, in a single execution, the interleaving that stop()'s join
// depends on: a consumer becomes visible while stop() is already waiting
// for the lifecycle lock.
//
// The test takes the lifecycle lock itself, so the stop() it launches has
// to wait for it. Once the goroutine dump shows stop() parked on that
// lock — proof that it has run everything above it while no consumer
// existed — the test publishes a consumer under the lock exactly as
// start() does and lets that consumer park inside the handler. Only then
// does it release the lock.
//
// stop() reads the started flag under the lock, so it must now see the
// consumer and wait for it. Reading that flag anywhere before the lock
// would return the value from before the consumer was published and let
// stop() return with the consumer still inside the handler, which the
// blocked handler makes observable here. The concurrent path through the
// real start() is covered by
// TestEventDispatcher_StartRacingStopJoinsConsumer.
func TestEventDispatcher_StopJoinsConsumerPublishedWhileWaitingForLock(t *testing.T) {
	inHandler := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }

	var delivered atomic.Int64
	d := newEventDispatcher(func(types.ClusterEvent) {
		inHandler <- struct{}{}
		<-release
		delivered.Add(1)
	}, logging.NewNopLogger())
	// Registered before anything that can fail, so no run can leave the
	// consumer parked in the handler — including the failure path that
	// this test exists to catch.
	t.Cleanup(releaseHandler)

	// Buffered before either lifecycle call, so the consumer has an event
	// to pick up and will park in the handler.
	d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventFailover})

	d.lifecycleMu.Lock()
	var unlockOnce sync.Once
	unlockLifecycle := func() { unlockOnce.Do(d.lifecycleMu.Unlock) }

	stopDone := make(chan struct{})
	go func() { d.stop(); close(stopDone) }()
	// Everything below runs while the lifecycle lock is held, and any of
	// it can fail. Registered here so a failing run releases the lock and
	// the handler and then joins stop(), instead of abandoning that
	// goroutine parked on a mutex nobody will ever unlock. Cleanups run in
	// reverse order of registration, so this one goes first and the plain
	// handler release stays as a backstop behind it.
	t.Cleanup(func() {
		unlockLifecycle()
		releaseHandler()
		select {
		case <-stopDone:
		case <-time.After(5 * time.Second):
			t.Error("stop() never returned after the lifecycle lock and the handler were released")
		}
	})
	waitStopBlockedOnLifecycleLock(t)

	// Publish the consumer the way start() does, under the lock start()
	// would hold. start() itself cannot be called here: it takes the same
	// lock, which this test is holding to pin the ordering.
	d.started.Store(true)
	go d.run()
	waitSignal(t, inHandler, "consumer to reach the handler")
	unlockLifecycle()

	select {
	case <-stopDone:
		t.Fatal("stop() returned without joining the consumer published while it waited for the lock")
	case <-time.After(100 * time.Millisecond):
		// expected: negative assertion — stop is waiting on the join
	}

	releaseHandler()
	waitSignal(t, stopDone, "stop completion after the consumer was joined")
	require.True(t, isClosed(d.done), "stop() must not return before the consumer has exited")
	require.Equal(t, int64(1), delivered.Load(), "the buffered event must reach the handler")
	require.Zero(t, d.dropped.Load(), "a delivered event must not be counted as dropped")
}

// TestEventDispatcher_StartRacingStopJoinsConsumer runs the real,
// unforced start/stop race repeatedly. Which call takes the lifecycle
// lock first is up to the scheduler, so each round samples what actually
// happened and checks the invariant that holds either way: if a consumer
// was started, stop() must not have returned before that consumer
// exited. The sample is taken in the stop goroutine the moment stop()
// returns, while the handler is still parked, so a consumer that exists
// cannot have exited on its own — the done channel is closed at that
// instant only if stop() really joined it.
//
// If no consumer had been started when stop() read the flag, then stop()
// held the lifecycle lock first, so start() must go on to find the
// dispatcher already stopped and do nothing at all: no consumer may
// appear afterwards, and the event buffered before the race must end up
// swept into the drop count. Both calls have returned by the time that
// is checked, so it is the settled outcome of the race rather than
// another sample. Checking it is what makes a start() that decides
// whether to spawn on a stopped-flag value read before it takes the lock
// fail here: such a start() can spawn a consumer after stop() has
// already returned and swept the buffer, leaving the started flag set on
// a dispatcher that is finished.
//
// The rounds are bounded and cheap: no round waits on a timer. Without
// race instrumentation, stop() essentially always reaches lifecycleMu
// before the start goroutine is scheduled off its starting gate, so a run
// here may never observe start winning; that is expected and is reported
// with t.Skip naming the deterministic test above that forces the
// interleaving and proves the join property directly, rather than either
// failing the suite or passing while having asserted nothing about the
// start-won branch. Under the race detector the scheduler interleaves
// differently and start does go on to win at least one round, so that
// branch's assertions run for real there.
func TestEventDispatcher_StartRacingStopJoinsConsumer(t *testing.T) {
	const rounds = 64

	startWon := 0
	for range rounds {
		release := make(chan struct{})
		inHandler := make(chan struct{}, 1)
		d := newEventDispatcher(func(types.ClusterEvent) {
			inHandler <- struct{}{}
			<-release
		}, logging.NewNopLogger())

		// Buffered before either lifecycle call, so a consumer that does
		// run has an event to pick up and parks in the handler.
		d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventFailover})

		begin := make(chan struct{})
		startDone := make(chan struct{})
		stopDone := make(chan struct{})
		// Written by the stop goroutine before stopDone closes, read only
		// after waiting on it.
		var joined, wasStarted bool
		go func() { <-begin; d.start(); close(startDone) }()
		go func() {
			<-begin
			d.stop()
			joined = isClosed(d.done)
			wasStarted = d.started.Load()
			close(stopDone)
		}()
		close(begin)

		// Whichever happened, unpark the handler: either a consumer is
		// sitting in it, or stop() already finished without one.
		select {
		case <-inHandler:
		case <-stopDone:
		}
		close(release)

		waitSignal(t, stopDone, "stop completion")
		waitSignal(t, startDone, "start completion")
		if wasStarted {
			startWon++
			waitSignal(t, d.done, "consumer exit")
			require.True(t, joined,
				"stop() returned before the consumer started alongside it had exited")

			continue
		}

		// stop() won the lifecycle lock. Both calls have returned, so
		// these are final values, not a sample.
		require.False(t, d.started.Load(),
			"start() spawned a consumer even though stop() had already run to completion")
		require.Equal(t, uint64(1), d.dropped.Load(),
			"the buffered event must be counted as dropped when no consumer runs")
	}

	if startWon == 0 {
		t.Skipf("start never won any of %d rounds without the race detector; "+
			"the join property this branch exercises is proven directly by "+
			"TestEventDispatcher_StopJoinsConsumerPublishedWhileWaitingForLock", rounds)
	}
}

func TestEventDispatcher_NilReceiverSafe(t *testing.T) {
	var d *eventDispatcher
	require.NotPanics(t, func() {
		d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventFailover})
		d.start()
		d.stop()
	})
}

func TestEventDispatcher_EmitAfterStopIsCountedDrop(t *testing.T) {
	h := newCollectingHandler(1)
	d := newEventDispatcher(h.handle, logging.NewNopLogger())
	d.start()
	d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventFailover})
	_ = h.wait(t)
	d.stop()
	before := d.dropped.Load()
	require.NotPanics(t, func() {
		d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventDrainEntered})
	})
	require.Equal(t, before+1, d.dropped.Load(), "post-stop emissions must be counted as drops")
}

// TestEventDispatcher_EmitRacingStopIsDeliveredOrCounted exercises the
// exact admission window: an emitter paused between its stopped-check
// and its channel send (via the test gate) while stop() runs. stop()
// must wait for the in-flight emitter, then deliver-or-count its event.
func TestEventDispatcher_EmitRacingStopIsDeliveredOrCounted(t *testing.T) {
	var deliveredCount int
	var mu sync.Mutex
	d := newEventDispatcher(func(_ types.ClusterEvent) {
		mu.Lock()
		deliveredCount++
		mu.Unlock()
	}, logging.NewNopLogger())
	d.testEmitGate = make(chan struct{})
	d.testEmitEntered = make(chan struct{}, 1)
	d.start()

	emitDone := make(chan struct{})
	go func() {
		d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventFailover})
		close(emitDone)
	}()
	// Entry handshake: do not race stop() against an emitter that may not
	// have reached the gate yet — wait until it is provably parked inside
	// the admission window.
	waitSignal(t, d.testEmitEntered, "emitter to reach the admission gate")

	stopDone := make(chan struct{})
	go func() { d.stop(); close(stopDone) }()

	// stop() must NOT complete while an admitted emitter is in flight.
	select {
	case <-stopDone:
		t.Fatal("stop() returned while an admitted emitter was still in flight")
	case <-time.After(100 * time.Millisecond):
		// expected: stop is waiting on the emitting counter
	}

	close(d.testEmitGate) // release the emitter
	waitSignal(t, emitDone, "emit completion")
	waitSignal(t, stopDone, "stop completion")

	mu.Lock()
	got := deliveredCount
	mu.Unlock()
	// Exact accounting: the event was either delivered or counted, never lost.
	require.Equal(t, 1, got+int(d.dropped.Load()),
		"in-flight event must be delivered or counted as dropped")
}

func TestEventDispatcher_HandlerPanicIsRecovered(t *testing.T) {
	h := newCollectingHandler(1)
	d := newEventDispatcher(func(ev types.ClusterEvent) {
		if ev.Kind == types.EventFailover {
			panic("boom")
		}
		h.handle(ev)
	}, logging.NewNopLogger())
	d.start()
	t.Cleanup(d.stop)

	d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventFailover})     // panics inside handler
	d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventDrainEntered}) // must still arrive

	got := h.wait(t)
	require.Equal(t, types.EventDrainEntered, got[0].Kind)
}

// TestEventDispatcher_OverflowNeverBlocksEmit wedges the handler; every
// EmitClusterEvent past the full buffer must return promptly (overflow is
// an atomic count; no logging on the emit path).
func TestEventDispatcher_OverflowNeverBlocksEmit(t *testing.T) {
	block := make(chan struct{})
	first := make(chan struct{})
	var once sync.Once
	d := newEventDispatcher(func(_ types.ClusterEvent) {
		once.Do(func() { close(first) })
		<-block // wedge the consumer
	}, logging.NewNopLogger())
	d.start()
	t.Cleanup(func() {
		close(block)
		d.stop()
	})

	d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventFailover})
	waitSignal(t, first, "consumer wedge")

	emitDone := make(chan struct{})
	go func() {
		for range eventBufferSize + 10 {
			d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventReplayDropped})
		}
		close(emitDone)
	}()
	waitSignal(t, emitDone, "non-blocking emits")
	require.GreaterOrEqual(t, d.dropped.Load(), uint64(10), "overflow must be counted")
}

// fakeClusterEventMetrics records AddClusterEventsDropped deltas for
// assertions on the dispatcher's drop-metric reconciliation.
type fakeClusterEventMetrics struct {
	mu     sync.Mutex
	deltas []int
	total  atomic.Int64
}

func (f *fakeClusterEventMetrics) AddClusterEventsDropped(n int) {
	f.mu.Lock()
	f.deltas = append(f.deltas, n)
	f.mu.Unlock()
	f.total.Add(int64(n))
}

// TestEventDispatcher_DropTotalReconciledToMetric wedges the handler,
// overflows the buffer, and verifies two properties of the optional drop
// metric: it is never written from the emit hot path (the wedged handler
// means no dispatcher-goroutine sync has run, so the metric must still be
// zero right after the overflow), and stop() reconciles the full internal
// drop total into it, keeping the exact-accounting invariant visible to
// the collector.
func TestEventDispatcher_DropTotalReconciledToMetric(t *testing.T) {
	block := make(chan struct{})
	first := make(chan struct{})
	var once sync.Once
	m := &fakeClusterEventMetrics{}
	d := newEventDispatcher(func(_ types.ClusterEvent) {
		once.Do(func() { close(first) })
		<-block // wedge the consumer
	}, logging.NewNopLogger())
	d.metrics = m
	d.start()

	d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventFailover})
	waitSignal(t, first, "consumer wedge")

	for range eventBufferSize + 10 {
		d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventReplayDropped})
	}
	require.GreaterOrEqual(t, d.dropped.Load(), uint64(10), "overflow must be counted")
	require.Zero(t, m.total.Load(),
		"the metric must not be written from the emit hot path — only the dispatcher goroutine reconciles it")

	close(block)
	d.stop()
	require.Equal(t, d.dropped.Load(), uint64(m.total.Load()),
		"stop() must reconcile the full internal drop total into the metric")
}

// TestEventDispatcher_StopWithoutStartReconcilesDropMetric covers the
// never-started path: the final buffer sweep in stop() counts the
// undelivered event as dropped and must still reconcile it into the
// metric, even though no delivery goroutine ever ran.
func TestEventDispatcher_StopWithoutStartReconcilesDropMetric(t *testing.T) {
	m := &fakeClusterEventMetrics{}
	d := newEventDispatcher(func(types.ClusterEvent) {}, logging.NewNopLogger())
	d.metrics = m
	d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventFailover}) // buffered, never delivered
	d.stop()
	require.Equal(t, uint64(1), d.dropped.Load())
	require.Equal(t, int64(1), m.total.Load(),
		"stop() on a never-started dispatcher must reconcile the swept drop into the metric")
}

// TestEventDispatcher_ReconcileDropsCarriesOversizedRemainder pins the
// metric-cursor arithmetic for a drop delta above math.MaxInt. Such a
// delta is unreachable through real emission (it needs math.MaxInt drops
// between two reconciliations), so the counter is seeded directly on a
// never-started dispatcher, where the test goroutine is the only toucher.
// One reconcile reports at most math.MaxInt and must advance the cursor
// only by that amount; the next reconcile must report the remainder, so
// the excess converges into the metric rather than being lost.
func TestEventDispatcher_ReconcileDropsCarriesOversizedRemainder(t *testing.T) {
	m := &fakeClusterEventMetrics{}
	d := newEventDispatcher(func(types.ClusterEvent) {}, logging.NewNopLogger())
	d.metrics = m
	d.dropped.Store(uint64(math.MaxInt) + 5)

	d.reconcileDrops(false)
	d.reconcileDrops(false)

	m.mu.Lock()
	deltas := slices.Clone(m.deltas)
	m.mu.Unlock()
	require.Equal(t, []int{math.MaxInt, 5}, deltas,
		"the capped excess must be reported by the next reconciliation, not dropped")
	require.Equal(t, uint64(math.MaxInt)+5, d.droppedMetricReported,
		"the cursor must converge on the internal drop total")
}

// TestEventDispatcher_CloseWaitsForInflightHandler verifies the Close
// contract: stop() waits for the current handler invocation.
func TestEventDispatcher_CloseWaitsForInflightHandler(t *testing.T) {
	inHandler := make(chan struct{})
	release := make(chan struct{})
	d := newEventDispatcher(func(_ types.ClusterEvent) {
		close(inHandler)
		<-release
	}, logging.NewNopLogger())
	d.start()

	d.EmitClusterEvent(types.ClusterEvent{Kind: types.EventFailover})
	waitSignal(t, inHandler, "handler entry")

	stopDone := make(chan struct{})
	go func() { d.stop(); close(stopDone) }()

	select {
	case <-stopDone:
		t.Fatal("stop() returned while the handler was still running")
	case <-time.After(100 * time.Millisecond):
		// expected: negative assertion — stop is waiting
	}

	close(release)
	waitSignal(t, stopDone, "stop completion")
}
