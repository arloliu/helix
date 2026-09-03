package mirror

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
)

// ExecuteFunc executes a captured mirror write against the mirror destination.
//
// Implementations typically dispatch through a helix CQLClient bound to the
// new dual-cluster pair, preserving the original write timestamp via
// WithTimestamp(payload.Timestamp).
//
// ExecuteFunc is a type alias for [replay.ExecuteFunc] — the mirror engine's
// initial dispatch and a [replay.Worker] draining a replayer share
// the same execution shape.
type ExecuteFunc = replay.ExecuteFunc

// DropHandler is invoked when a captured write cannot be enqueued because
// the queue is full or the engine has stopped, and for every capture still
// queued when [WithDrainTimeout] cuts the shutdown drain short.
//
// The handler runs on the caller's goroutine (or on a worker goroutine
// during shutdown); keep it fast and non-blocking. It must not call the
// owning client's Close or the engine's Stop: Stop waits for the workers
// that run the shutdown drops, so a handler that calls it deadlocks.
type DropHandler func(payload types.ReplayPayload)

// ErrorHandler is invoked synchronously by a worker after [ExecuteFunc]
// returns a non-nil error. It receives the original payload and the error.
//
// Typical use is to push the failed payload onto a replayer (the helix
// Replayer interface) for durable retry; the helix client wires this
// internally when its WithMirrorReplayer option is configured. Custom handlers may also implement alerting, escalation,
// or alternative durability stores.
//
// The handler runs on a worker goroutine and blocks the worker until it
// returns. Keep it fast — slow handlers throttle mirror throughput. It must
// not call the owning client's Close or the engine's Stop: both wait for
// the worker that invoked it and would deadlock.
type ErrorHandler func(payload types.ReplayPayload, err error)

// Stats reports a snapshot of engine counters and queue state.
//
// Counters are monotonic for the engine's lifetime. QueueDepth and Enabled
// are point-in-time observations.
type Stats struct {
	Enqueued   uint64
	Dropped    uint64
	Success    uint64
	Error      uint64
	QueueDepth int
	Enabled    bool
}

// DefaultQueueSize is the default capacity of the in-memory mirror queue.
const DefaultQueueSize = 8192

// DefaultWorkers is the default number of mirror worker goroutines.
const DefaultWorkers = 4

// dropLogInterval is the minimum gap between drop warning logs. Counters
// still increment on every drop; the log is rate-limited so that a queue-
// full storm does not produce a log storm.
const dropLogInterval = time.Second

// Drop reasons reported via the engine's logger and (in Phase 4) metrics.
const (
	dropDisabled  = "disabled"
	dropStopped   = "stopped"
	dropQueueFull = "queue_full"
)

type config struct {
	queueSize    int
	workers      int
	enabled      bool
	onDrop       DropHandler
	onError      ErrorHandler
	logger       types.Logger
	metrics      types.MirrorMetrics
	drainTimeout time.Duration
}

// Option configures an [Engine].
type Option func(*config)

// WithQueueSize sets the capacity of the in-memory mirror queue. Values <= 0
// fall back to DefaultQueueSize.
func WithQueueSize(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.queueSize = n
		}
	}
}

// WithWorkers sets the number of worker goroutines that drain the queue.
// Values <= 0 fall back to DefaultWorkers.
func WithWorkers(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.workers = n
		}
	}
}

// WithEnabled sets the initial enabled state of the engine. Default: true.
func WithEnabled(enabled bool) Option {
	return func(c *config) { c.enabled = enabled }
}

// WithOnDrop installs a callback invoked for every dropped capture. See
// [DropHandler] for what it must not do.
func WithOnDrop(fn DropHandler) Option {
	return func(c *config) { c.onDrop = fn }
}

// WithOnError installs a callback invoked synchronously by a worker after
// [ExecuteFunc] returns a non-nil error. See [ErrorHandler] for semantics
// and what it must not do.
func WithOnError(fn ErrorHandler) Option {
	return func(c *config) { c.onError = fn }
}

// WithDrainTimeout bounds how long [Engine.Stop] keeps starting new
// executions for queued captures. Once d elapses the workers stop taking
// captures, every capture still queued is dropped exactly once (the
// [DropHandler], the dropped statistic, and
// [types.MirrorShutdownMetrics.AddMirrorDrainDropped] when the collector
// implements it), one Warn line reports the count, and Stop then waits for
// the captures already executing and their callbacks.
//
// d is the cutoff for starting new executions, not a bound on Stop: Stop,
// and the owning client's Close, still wait for every in-flight execution
// and every synchronous drop and error callback, whose delays add up.
// Zero (the default) keeps the unbounded drain, which in publisher mode
// can hold Close for minutes on a full queue. The dropped captures go to
// the [DropHandler] only, never to a mirror replayer; a handler that
// wants them durable must enqueue them itself.
//
// Parameters:
//   - d: The cutoff; zero or negative disables it
//
// Returns:
//   - Option: Configuration option
func WithDrainTimeout(d time.Duration) Option {
	return func(c *config) { c.drainTimeout = d }
}

// WithLogger sets the logger used for engine events. Defaults to a no-op
// logger if not provided.
func WithLogger(l types.Logger) Option {
	return func(c *config) {
		if l != nil {
			c.logger = l
		}
	}
}

// WithMetrics installs a [types.MirrorMetrics] collector that receives
// per-event counters and gauge updates. Defaults to a no-op collector.
//
// Helix's CQLClient auto-wires this when its configured
// [types.MetricsCollector] also implements [types.MirrorMetrics] — so
// using a bundled collector (e.g. contrib/metrics/vm) gives mirror
// observability for free.
func WithMetrics(m types.MirrorMetrics) Option {
	return func(c *config) {
		if m != nil {
			c.metrics = m
		}
	}
}

// Engine captures async mirror writes, holds them in a bounded queue, and
// drains them through a worker pool that calls [ExecuteFunc].
//
// The zero value is not usable; construct an Engine with [NewEngine].
type Engine struct {
	cfg     config
	execute ExecuteFunc

	enabled atomic.Bool
	queue   chan types.ReplayPayload

	startOnce sync.Once
	stopOnce  sync.Once

	// enqueueMu fences in-flight enqueues against Stop's close(queue) so
	// no goroutine can attempt a send on a closed channel. RLock is held
	// during the non-blocking send; Stop takes Lock before closing.
	enqueueMu sync.RWMutex
	stopped   atomic.Bool

	// lastDropLogNanos rate-limits the drop warning log to dropLogInterval.
	lastDropLogNanos atomic.Int64

	// drainCutoff is closed when the drain timeout elapses during Stop;
	// workers then drop instead of execute. drainDropped counts those.
	drainCutoff  chan struct{}
	drainDropped atomic.Uint64

	wg sync.WaitGroup

	enqueued atomic.Uint64
	dropped  atomic.Uint64
	success  atomic.Uint64
	errored  atomic.Uint64
}

// NewEngine constructs a mirror engine that dispatches captured writes via
// execute. The engine is created in the started=false state; helix's
// CQLClient calls Start during construction.
//
// Parameters:
//   - execute: Dispatches a captured write to the mirror destination. Must be non-nil.
//   - opts: Functional options (e.g., WithQueueSize, WithWorkers, WithOnError)
//
// Returns:
//   - *Engine: Ready-to-start mirror engine
//
// NewEngine panics if execute is nil, since a nil ExecuteFunc would only
// surface as an unrecovered panic in a worker goroutine on the first
// dequeued payload.
func NewEngine(execute ExecuteFunc, opts ...Option) *Engine {
	if execute == nil {
		panic("mirror: NewEngine requires a non-nil execute function")
	}

	cfg := config{
		queueSize: DefaultQueueSize,
		workers:   DefaultWorkers,
		enabled:   true,
		logger:    logging.NewNopLogger(),
		metrics:   nopMirrorMetrics{},
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	e := &Engine{
		cfg:         cfg,
		execute:     execute,
		queue:       make(chan types.ReplayPayload, cfg.queueSize),
		drainCutoff: make(chan struct{}),
	}
	e.enabled.Store(cfg.enabled)

	return e
}

// nopMirrorMetrics discards every metric. Used when no collector is
// configured to avoid nil checks on the hot path.
type nopMirrorMetrics struct{}

func (nopMirrorMetrics) IncMirrorEnqueueSuccess()          {}
func (nopMirrorMetrics) IncMirrorEnqueueDropped()          {}
func (nopMirrorMetrics) IncMirrorExecSuccess()             {}
func (nopMirrorMetrics) IncMirrorExecError()               {}
func (nopMirrorMetrics) ObserveMirrorExecDuration(float64) {}
func (nopMirrorMetrics) SetMirrorQueueDepth(int)           {}
func (nopMirrorMetrics) SetMirrorEnabled(bool)             {}

// Start spawns the worker pool. Calling Start more than once is a no-op.
func (e *Engine) Start() {
	e.startOnce.Do(func() {
		e.cfg.metrics.SetMirrorEnabled(e.enabled.Load())
		for i := 0; i < e.cfg.workers; i++ {
			e.wg.Add(1)
			go e.worker()
		}
	})
}

// Stop signals workers to drain remaining queued captures and exit, then
// waits for them. After Stop returns, TryEnqueue always drops.
// The drain is synchronous and unbounded unless [WithDrainTimeout] cuts
// it short.
func (e *Engine) Stop() {
	e.stopOnce.Do(func() {
		e.enqueueMu.Lock()
		e.stopped.Store(true)
		close(e.queue)
		e.enqueueMu.Unlock()
		if e.cfg.drainTimeout > 0 {
			timer := time.AfterFunc(e.cfg.drainTimeout, func() { close(e.drainCutoff) })
			defer timer.Stop()
		}
		e.wg.Wait()
		if n := e.drainDropped.Load(); n > 0 {
			e.reportDrainDropped(n)
		}
		// Workers write the gauge from len(queue) after their own dequeue;
		// with several workers the last write to land can be stale, so
		// the empty queue is reconciled here.
		e.cfg.metrics.SetMirrorQueueDepth(0)
		e.cfg.metrics.SetMirrorEnabled(false)
	})
}

// reportDrainDropped publishes the captures the drain timeout dropped.
func (e *Engine) reportDrainDropped(n uint64) {
	count := int(min(n, math.MaxInt)) //nolint:gosec // capped above
	if sm, ok := e.cfg.metrics.(types.MirrorShutdownMetrics); ok {
		sm.AddMirrorDrainDropped(count)
	}
	e.cfg.logger.Warn("mirror drain timeout elapsed: queued captures dropped", "count", count)
}

// dropAtShutdown drops a capture the drain timeout left queued.
func (e *Engine) dropAtShutdown(payload types.ReplayPayload) {
	e.dropped.Add(1)
	e.drainDropped.Add(1)
	if e.cfg.onDrop != nil {
		e.cfg.onDrop(payload)
	}
}

// Enable resumes accepting new captures. Items already in the queue are
// always processed regardless of the enabled flag.
func (e *Engine) Enable() {
	e.enabled.Store(true)
	e.cfg.metrics.SetMirrorEnabled(true)
}

// Disable stops accepting new captures. In-flight queued captures continue
// draining through the worker pool.
func (e *Engine) Disable() {
	e.enabled.Store(false)
	e.cfg.metrics.SetMirrorEnabled(false)
}

// Enabled reports the current enable state.
func (e *Engine) Enabled() bool { return e.enabled.Load() }

// Stats returns a snapshot of engine counters and queue state.
func (e *Engine) Stats() Stats {
	return Stats{
		Enqueued:   e.enqueued.Load(),
		Dropped:    e.dropped.Load(),
		Success:    e.success.Load(),
		Error:      e.errored.Load(),
		QueueDepth: len(e.queue),
		Enabled:    e.enabled.Load(),
	}
}

// TryEnqueue attempts a non-blocking enqueue. Returns false when the engine
// is disabled, stopped, or the queue is full; in those cases the optional
// drop handler is invoked.
func (e *Engine) TryEnqueue(payload types.ReplayPayload) bool {
	if !e.enabled.Load() {
		e.dropOne(payload, dropDisabled)
		return false
	}

	e.enqueueMu.RLock()
	defer e.enqueueMu.RUnlock()

	if e.stopped.Load() {
		e.dropOne(payload, dropStopped)
		return false
	}

	select {
	case e.queue <- payload:
		e.enqueued.Add(1)
		e.cfg.metrics.IncMirrorEnqueueSuccess()
		e.cfg.metrics.SetMirrorQueueDepth(len(e.queue))
		return true
	default:
		e.dropOne(payload, dropQueueFull)
		return false
	}
}

func (e *Engine) dropOne(payload types.ReplayPayload, reason string) {
	e.dropped.Add(1)
	e.cfg.metrics.IncMirrorEnqueueDropped()

	nowNs := time.Now().UnixNano()
	last := e.lastDropLogNanos.Load()
	if nowNs-last >= int64(dropLogInterval) && e.lastDropLogNanos.CompareAndSwap(last, nowNs) {
		e.cfg.logger.Warn("mirror capture dropped",
			"reason", reason,
			"isBatch", payload.IsBatch,
			"totalDropped", e.dropped.Load(),
		)
	}

	if e.cfg.onDrop != nil {
		e.cfg.onDrop(payload)
	}
}

func (e *Engine) worker() {
	defer e.wg.Done()

	// Mirror writes intentionally use a background context: they may run
	// long after the caller's request context is cancelled.
	ctx := context.Background()

	for payload := range e.queue {
		e.cfg.metrics.SetMirrorQueueDepth(len(e.queue))
		select {
		case <-e.drainCutoff:
			e.dropAtShutdown(payload)

			continue
		default:
		}

		start := time.Now()
		err := e.execute(ctx, payload)
		e.cfg.metrics.ObserveMirrorExecDuration(time.Since(start).Seconds())

		if err != nil {
			e.errored.Add(1)
			e.cfg.metrics.IncMirrorExecError()
			e.cfg.logger.Warn("mirror write failed",
				"error", err.Error(),
				"isBatch", payload.IsBatch,
			)
			if e.cfg.onError != nil {
				e.cfg.onError(payload, err)
			}

			continue
		}
		e.success.Add(1)
		e.cfg.metrics.IncMirrorExecSuccess()
	}
}
