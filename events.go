package helix

import (
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/helix/types"
)

// eventBufferSize bounds the dispatcher's pending-event buffer. Most
// kinds are state transitions, so their bursts are small and 128 absorbs
// them without measurable memory cost. EventFailover and
// EventReadDivergence are the exception: they fire once per affected read,
// so a sustained outage overruns this buffer and the excess is dropped.
// That is the accepted trade — the buffer exists to keep emission
// non-blocking, and rates for those two kinds are meant to be read from
// metrics, not reconstructed from events.
const eventBufferSize = 128

// ClusterEventHandler is called asynchronously for cluster-health events.
// Registered via [WithOnClusterEvent].
//
// The handler runs on a single dedicated goroutine: invocations never
// overlap. Circuit-breaker and adaptive-write events arrive in
// per-cluster transition order, per policy instance; events from
// independent producers arrive in enqueue order with no cross-kind causal
// guarantee. Delivery is best-effort: a slow handler never blocks
// read/write operations — when the internal buffer fills, newest events
// are dropped and counted. The drop total is logged from the dispatcher
// goroutine on the first drop, then only once it has at least doubled
// since the last line, plus once at shutdown. When the configured
// [types.MetricsCollector] also implements [types.ClusterEventMetrics],
// the total is additionally exposed as a counter (contrib/metrics/vm:
// {prefix}_cluster_events_dropped_total), reconciled from the dispatcher
// goroutine, so an application can alert on event loss.
//
// The handler MUST NOT call [CQLClient.Close] synchronously (Close waits
// for the in-flight handler invocation and would deadlock); trigger
// shutdown from another goroutine and return instead. Calling anything
// else on the client, or on a policy, from inside the handler is safe: no
// policy lock is held during delivery, and an event produced by such a
// reentrant call is enqueued with a non-blocking send.
//
// Parameters:
//   - event: The cluster event. See [types.ClusterEvent] for which fields
//     are populated per [types.ClusterEventKind].
type ClusterEventHandler func(event types.ClusterEvent)

// eventDispatcher fans cluster events from hot paths to the user handler
// on a dedicated goroutine.
//
// Hot-path contract: EmitClusterEvent performs ONLY an admission
// increment, a stopped check, a timestamp stamp, and a non-blocking
// buffered send (overflow = atomic count). No logging, no locks, no
// metric calls. Drop totals are reported to the log and reconciled into
// the optional metrics collector from the dispatcher goroutine, and
// finalized in stop().
//
// Lifecycle (serialized by lifecycleMu; never touched by the emit path):
// newEventDispatcher (no goroutine) -> start() (spawns delivery;
// idempotent; no-op after stop) -> stop() (halts intake, joins the
// consumer, waits out in-flight emitters, sweeps the buffer, reports).
// Concurrent stop callers block until shutdown completes. stop without
// start cannot hang. A nil *eventDispatcher no-ops on every method.
//
// Accounting invariant: every event passed to EmitClusterEvent is either
// delivered to the handler or counted in dropped — stop's emitting-drain
// plus final buffer sweep closes the check-then-send race window.
type eventDispatcher struct {
	handler ClusterEventHandler
	logger  types.Logger
	// metrics receives the drop total when the configured collector opts
	// in via types.ClusterEventMetrics; nil otherwise. Never called from
	// the emit hot path — see reconcileDrops.
	metrics types.ClusterEventMetrics
	ch      chan types.ClusterEvent
	quit    chan struct{}
	done    chan struct{}

	lifecycleMu sync.Mutex  // serializes start/stop
	started     atomic.Bool // written under lifecycleMu; read by tests
	stopped     atomic.Bool // read on the emit hot path

	emitting atomic.Int64  // in-flight EmitClusterEvent calls
	dropped  atomic.Uint64 // events not delivered to the handler

	// droppedReported tracks the last logged drop total. Touched only by
	// the dispatcher goroutine and by stop() after joining it.
	droppedReported uint64

	// droppedMetricReported tracks the drop total already reconciled into
	// the metrics collector. Same touch discipline as droppedReported.
	droppedMetricReported uint64

	// testEmitGate, when non-nil, parks emitters inside the admission
	// window (after registering in emitting, before the send); an emitter
	// signals testEmitEntered first so tests can handshake on entry.
	// Test-only.
	testEmitGate    chan struct{}
	testEmitEntered chan struct{}
}

// newEventDispatcher creates a dispatcher without starting delivery.
// Events emitted before start() buffer (up to eventBufferSize) and are
// delivered once it runs.
func newEventDispatcher(handler ClusterEventHandler, logger types.Logger) *eventDispatcher {
	return &eventDispatcher{
		handler: handler,
		logger:  logger,
		ch:      make(chan types.ClusterEvent, eventBufferSize),
		quit:    make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// EmitClusterEvent enqueues an event for asynchronous delivery. Never
// blocks; drops (and counts) the event when the buffer is full or the
// dispatcher is stopped. Nil-safe. Stamps Timestamp when left zero.
//
// Admission is check-increment-recheck: a caller arriving after stop
// drops at the first check WITHOUT joining the emitting counter, so
// sustained post-stop emissions cannot starve stop's counter drain; a
// caller that passed the first check pre-stop is counted in emitting,
// which stop waits out before its sweep — closing the check-then-send
// race exactly.
func (d *eventDispatcher) EmitClusterEvent(event types.ClusterEvent) {
	if d == nil {
		return
	}
	if d.stopped.Load() {
		d.dropped.Add(1)
		return
	}
	d.emitting.Add(1)
	defer d.emitting.Add(-1)
	if d.testEmitGate != nil {
		if d.testEmitEntered != nil {
			d.testEmitEntered <- struct{}{}
		}
		<-d.testEmitGate
	}
	if d.stopped.Load() {
		// stop() began after our admission; it is waiting on emitting,
		// so drop-and-count here keeps accounting exact.
		d.dropped.Add(1)
		return
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	select {
	case d.ch <- event:
	default:
		d.dropped.Add(1)
	}
}

// start spawns the delivery goroutine. Idempotent; no-op after stop.
func (d *eventDispatcher) start() {
	if d == nil {
		return
	}
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	if d.stopped.Load() || d.started.Load() {
		return
	}
	d.started.Store(true)
	go d.run()
}

// run delivers events until stop() is observed, then drains buffered
// events before exiting. Runs on its own goroutine.
func (d *eventDispatcher) run() {
	defer close(d.done)
	for {
		select {
		case ev := <-d.ch:
			d.invoke(ev)
			d.reconcileDrops(false)
		case <-d.quit:
			for {
				select {
				case ev := <-d.ch:
					d.invoke(ev)
				default:
					return
				}
			}
		}
	}
}

// invoke calls the handler, converting a handler panic into an error log
// so one bad handler cannot kill event delivery or the process.
func (d *eventDispatcher) invoke(ev types.ClusterEvent) {
	defer func() {
		if r := recover(); r != nil {
			d.logger.Error("cluster event handler panicked",
				"kind", string(ev.Kind),
				"panic", r,
			)
		}
	}()
	d.handler(ev)
}

// reconcileDrops reads the atomic drop total once and reconciles both
// observers of it. The optional metrics collector receives the delta
// since the last call, capped at math.MaxInt per call with the cursor
// advanced only by the amount actually reported, so any capped remainder
// carries into later calls and the exported counter converges on the
// internal count — equal at every sync point short of the unreachable
// case of math.MaxInt drops accumulating between two calls. The log
// reports on the first drop and then only
// each time the total has at least doubled since the last report, so log
// volume does not grow linearly with the drop count; final=true forces a
// report of any unreported remainder regardless of that threshold.
//
// Called only from the dispatcher goroutine or from stop() after the
// join — never from the emit hot path, so an arbitrary (possibly
// locking) collector implementation cannot slow emitters down. Drops
// that occur after stop's final call are counted internally but reach
// neither the metric nor the log.
func (d *eventDispatcher) reconcileDrops(final bool) {
	n := d.dropped.Load()
	if d.metrics != nil && n != d.droppedMetricReported {
		// The cap is unreachable in practice; it keeps the conversion
		// safe. The cursor advances only by what is reported, so any
		// capped excess is reported by later calls rather than lost.
		delta := min(n-d.droppedMetricReported, math.MaxInt)
		d.droppedMetricReported += delta
		d.metrics.AddClusterEventsDropped(int(delta)) //nolint:gosec // delta is capped at math.MaxInt above; gosec cannot see through min()
	}
	if n == 0 || n == d.droppedReported {
		return
	}
	if final || d.droppedReported == 0 || n >= d.droppedReported*2 {
		d.logger.Warn("cluster events dropped: handler not keeping up",
			"droppedTotal", n,
		)
		d.droppedReported = n
	}
}

// stop halts intake, joins the consumer (when started), waits for
// in-flight emitters to finish, sweeps undeliverable buffered events
// into the drop count, and reports the final total. Idempotent;
// concurrent callers block until shutdown completes; safe without
// start(); nil-safe. Must not be called from within the handler itself:
// stop() waits for the current handler invocation to return, so calling
// it from inside that invocation would wait on itself forever. Note: the
// final drop report and handler-panic recovery use the configured
// logger, so a wedged logger can block stop (and therefore
// CQLClient.Close). Events dropped after the final report (post-stop
// emissions from unjoined background goroutines) are counted but neither
// logged nor reconciled into the metric.
func (d *eventDispatcher) stop() {
	if d == nil {
		return
	}
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	if d.stopped.Load() {
		return
	}
	d.stopped.Store(true)
	close(d.quit)
	if d.started.Load() {
		<-d.done
	}
	// Wait for in-flight emitters. The population is bounded: stopped is
	// already set, so no new caller joins emitting (the first admission
	// check drops them) — only emitters admitted before stop remain, and
	// their critical section is lock-free and tiny (atomics plus one
	// select), so this spin terminates promptly even under sustained
	// emission load.
	for d.emitting.Load() != 0 {
		runtime.Gosched()
	}
	// Sweep events that landed after the consumer's final drain (or that
	// were buffered on a never-started dispatcher): they will never be
	// delivered, so count them as dropped. With the emitting counter at
	// zero, no further sends can occur — accounting is exact.
	for {
		select {
		case <-d.ch:
			d.dropped.Add(1)
		default:
			d.reconcileDrops(true)
			return
		}
	}
}
