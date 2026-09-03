package replay

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/internal/typeutil"
	"github.com/arloliu/helix/types"
)

// ExecuteFunc is a function that executes a replay payload against a target cluster.
// It receives the payload and should execute the query against the appropriate cluster.
// Returns nil on success, error on failure.
type ExecuteFunc func(ctx context.Context, payload types.ReplayPayload) error

// WorkerConfig configures the replay worker.
type WorkerConfig struct {
	// BatchSize is the number of messages to fetch per dequeue operation.
	// Default: 100
	BatchSize int

	// PollInterval is the interval between dequeue attempts when the queue is empty.
	// Default: 100ms
	PollInterval time.Duration

	// RetryDelay is the initial delay before retrying a failed replay.
	// The delay doubles after every failed attempt, without jitter, up to
	// MaxRetryDelay.
	// Default: 100ms
	RetryDelay time.Duration

	// MaxRetryDelay is the maximum delay between retries.
	// Default: 30 seconds
	MaxRetryDelay time.Duration

	// ExecuteTimeout is the timeout for each replay execution.
	// Default: 30 seconds
	ExecuteTimeout time.Duration

	// MaxAttempts caps the number of in-line retries for a single payload
	// in the memory backend. After MaxAttempts failures, the payload is
	// dropped and OnDrop is invoked with the final error.
	//
	// Under RetryWhileRetained it is the poison budget on both backends:
	// only attempts the classifier marks DispositionDeadLetter count toward
	// it.
	//
	// Under RetryBounded the NATS backend uses NATSReplayerConfig.MaxDeliver
	// instead and ignores this setting.
	//
	// Values <= 0 are treated as 1 (no retry beyond the initial attempt).
	// Default: 5
	MaxAttempts int

	// RetryPolicy selects how failed attempts are budgeted.
	// See ReplayRetryPolicy.
	// Default: RetryWhileRetained
	RetryPolicy ReplayRetryPolicy

	// RetryWindow bounds how long the memory backend keeps retrying a
	// payload under RetryWhileRetained, measured from its first attempt.
	// The NATS backend is bounded by the stream MaxAge instead.
	// Default: 24 hours
	RetryWindow time.Duration

	// Classifier maps an execution error to a ReplayDisposition under
	// RetryWhileRetained.
	// Default: DefaultReplayClassifier
	Classifier ReplayClassifier

	// HighPriorityRatio controls the ratio of high-priority to low-priority
	// processing. The exact unit depends on the backend:
	//
	//   - Memory backend: per-message ratio. For every N high-priority
	//     messages dequeued, 1 low-priority message is dequeued.
	//   - NATS backend: per-batch ratio. For every N high-priority batches
	//     fetched, 1 low-priority batch is fetched. With BatchSize=100 and
	//     ratio=10, this is roughly 1000 high messages : 100 low messages
	//     per scheduling cycle.
	//
	// This prevents low-priority starvation while ensuring high-priority
	// messages are preferred. Set to 0 for equal priority processing (1:1).
	// Default: 10
	HighPriorityRatio int

	// StrictPriority when true, drains all high-priority messages before processing any
	// low-priority messages. This provides absolute priority but may cause low-priority
	// starvation under continuous high-priority load.
	// When false (default), uses HighPriorityRatio for fair scheduling.
	// Default: false
	StrictPriority bool

	// Metrics is the metrics collector for recording replay statistics.
	// If nil, no metrics are recorded.
	Metrics types.MetricsCollector

	// backlog is Metrics narrowed to the optional backlog interface, or a
	// no-op collector when Metrics does not implement it.
	backlog types.ReplayBacklogMetrics

	// metricsExplicit is true when WithWorkerMetrics was called by the
	// caller. Used by [Worker.MetricsConfigured] so a parent (e.g. the
	// helix CQLClient) can detect "user did NOT pass WithWorkerMetrics"
	// and inject its own MetricsCollector to keep client+worker
	// instrumentation unified.
	metricsExplicit bool

	// Logger is the structured logger for replay worker events.
	// If nil, no logs are emitted.
	Logger types.Logger

	// ClusterNames holds custom display names for clusters in log messages.
	// Defaults to "A" and "B".
	ClusterNames types.ClusterNames

	// OnSuccess is called after a successful replay (optional).
	OnSuccess func(payload types.ReplayPayload)

	// OnError is called after a failed replay attempt (optional).
	// The error and attempt number are provided.
	OnError func(payload types.ReplayPayload, err error, attempt int)

	// OnDrop is called once when the worker permanently drops a payload
	// (optional). Both backends invoke it; the reason is reported through
	// the optional types.ReplayBacklogMetrics interface and the worker log.
	// A NATS message the stream evicts on its own (MaxAge expiry, or a
	// stream limit with DiscardOld) never reaches the worker and is not
	// reported here; watch JetStream's stream and consumer metrics for it.
	OnDrop func(payload types.ReplayPayload, err error)

	// ClusterGate answers whether replay to a cluster may execute now; nil
	// permits every cluster. Set via [WithClusterGate].
	ClusterGate func(cluster types.ClusterID) bool
}

// DefaultWorkerConfig returns the default worker configuration.
func DefaultWorkerConfig() WorkerConfig {
	return WorkerConfig{
		BatchSize:         100,
		PollInterval:      100 * time.Millisecond,
		RetryDelay:        100 * time.Millisecond,
		MaxRetryDelay:     30 * time.Second,
		ExecuteTimeout:    30 * time.Second,
		MaxAttempts:       5,
		RetryPolicy:       RetryWhileRetained,
		RetryWindow:       defaultRetryWindow,
		HighPriorityRatio: 10,
		StrictPriority:    false,
		ClusterNames:      types.DefaultClusterNames(),
	}
}

// WorkerOption configures a Worker.
type WorkerOption func(*WorkerConfig)

// WithBatchSize sets the batch size for dequeue operations.
func WithBatchSize(n int) WorkerOption {
	return func(c *WorkerConfig) {
		c.BatchSize = n
	}
}

// WithPollInterval sets the polling interval when queue is empty.
func WithPollInterval(d time.Duration) WorkerOption {
	return func(c *WorkerConfig) {
		c.PollInterval = d
	}
}

// WithRetryDelay sets the initial retry delay.
func WithRetryDelay(d time.Duration) WorkerOption {
	return func(c *WorkerConfig) {
		c.RetryDelay = d
	}
}

// WithMaxRetryDelay sets the maximum retry delay.
func WithMaxRetryDelay(d time.Duration) WorkerOption {
	return func(c *WorkerConfig) {
		c.MaxRetryDelay = d
	}
}

// WithExecuteTimeout sets the execution timeout per replay.
func WithExecuteTimeout(d time.Duration) WorkerOption {
	return func(c *WorkerConfig) {
		c.ExecuteTimeout = d
	}
}

// WithMaxAttempts sets the maximum number of attempts for a single payload
// in the memory backend before it is dropped via OnDrop.
// Under [RetryWhileRetained] it is the poison budget on both backends: only
// attempts the classifier marks [DispositionDeadLetter] count toward it.
//
// Under [RetryBounded] the NATS backend uses MaxDeliver on the consumer
// instead.
// Values <= 0 are treated as 1 (no retry beyond the initial attempt).
func WithMaxAttempts(n int) WorkerOption {
	return func(c *WorkerConfig) {
		c.MaxAttempts = n
	}
}

// WithHighPriorityRatio sets the ratio of high-priority to low-priority processing.
//
// For every N high-priority batches processed, 1 low-priority batch is processed.
// This prevents low-priority starvation while ensuring high-priority messages are preferred.
// Set to 0 for equal priority processing (1:1 ratio).
// Default: 10 (10:1 ratio)
//
// Parameters:
//   - n: Number of high-priority batches to process before 1 low-priority batch
//
// Returns:
//   - WorkerOption: Configuration option
func WithHighPriorityRatio(n int) WorkerOption {
	return func(c *WorkerConfig) {
		c.HighPriorityRatio = n
	}
}

// WithStrictPriority enables strict priority mode.
//
// When enabled, all high-priority messages are drained before processing any
// low-priority messages. This provides absolute priority but may cause low-priority
// starvation under continuous high-priority load.
//
// When disabled (default), uses HighPriorityRatio for fair scheduling.
//
// Parameters:
//   - strict: true to enable strict priority mode
//
// Returns:
//   - WorkerOption: Configuration option
func WithStrictPriority(strict bool) WorkerOption {
	return func(c *WorkerConfig) {
		c.StrictPriority = strict
	}
}

// WithOnSuccess sets the success callback.
func WithOnSuccess(fn func(types.ReplayPayload)) WorkerOption {
	return func(c *WorkerConfig) {
		c.OnSuccess = fn
	}
}

// WithOnError sets the error callback.
func WithOnError(fn func(types.ReplayPayload, error, int)) WorkerOption {
	return func(c *WorkerConfig) {
		c.OnError = fn
	}
}

// WithClusterGate installs a predicate that decides whether replay to a
// cluster may execute right now.
//
// The gate is consulted before every execution attempt, on both backends:
// the memory worker's first attempt and every retry, and every message of
// a fetched NATS batch. A gated payload is parked, not counted: the memory
// worker holds it without consuming an attempt or its retry window, and
// the NATS worker keeps the fetched batch in progress without NAKing, so a
// bounded consumer's delivery budget is never spent while gated. The
// memory dequeue and the NATS fetch also skip a gated cluster, so queued
// work stays queued or server-side. Reopening is observed within
// PollInterval.
//
// Repeated WithClusterGate options compose by logical AND: every gate must
// permit a cluster before replay runs, whichever order the options came in.
// A helix client that builds its own worker appends a gate driven by drain
// and its operator predicate (see helix.WithReplayGate).
//
// The predicate must be cheap, non-blocking, and safe for concurrent use.
// A gate that panics counts as closed.
//
// Parameters:
//   - gate: Returns true when replay to the cluster may execute; nil is
//     ignored
//
// Returns:
//   - WorkerOption: Configuration option
func WithClusterGate(gate func(cluster types.ClusterID) bool) WorkerOption {
	return func(c *WorkerConfig) {
		if gate == nil {
			return
		}
		if prev := c.ClusterGate; prev != nil {
			c.ClusterGate = func(cluster types.ClusterID) bool {
				return prev(cluster) && gate(cluster)
			}

			return
		}
		c.ClusterGate = gate
	}
}

// WithOnDrop sets the drop callback.
func WithOnDrop(fn func(types.ReplayPayload, error)) WorkerOption {
	return func(c *WorkerConfig) {
		c.OnDrop = fn
	}
}

// WithWorkerMetrics sets the metrics collector for the worker.
//
// Marks the configuration as "metrics explicitly set" so a parent caller
// (e.g. helix.NewCQLClient) does not auto-inject a different collector
// later via [Worker.SetMetrics] / [Worker.MetricsConfigured].
//
// A typed-nil m (e.g. a nil `*myCollector` assigned to the
// types.MetricsCollector variable) is treated the same as an untyped
// nil: it is ignored so [finalizeWorkerConfig] can still install the
// Nop fallback, instead of the config being left holding an interface
// value that panics on first use.
func WithWorkerMetrics(m types.MetricsCollector) WorkerOption {
	return func(c *WorkerConfig) {
		if typeutil.IsNilInterface(m) {
			return
		}
		c.Metrics = m
		c.metricsExplicit = true
	}
}

// WithWorkerLogger sets the logger for the worker.
//
// A typed-nil l is ignored for the same reason described in
// [WithWorkerMetrics]: it leaves the config free to fall back to the Nop
// logger instead of storing an unusable interface value.
func WithWorkerLogger(l types.Logger) WorkerOption {
	return func(c *WorkerConfig) {
		if typeutil.IsNilInterface(l) {
			return
		}
		c.Logger = l
	}
}

// WithWorkerClusterNames sets the cluster display names for log messages.
func WithWorkerClusterNames(names types.ClusterNames) WorkerOption {
	return func(c *WorkerConfig) {
		c.ClusterNames = names
	}
}

// Worker processes replay messages from a queue and re-executes failed writes.
//
// The worker uses a backend strategy pattern to support different queue implementations.
// It manages the lifecycle (Start/Stop) while delegating queue-specific processing to the backend.
type Worker struct {
	config     WorkerConfig
	execute    ExecuteFunc
	backend    workerBackend
	stopCh     chan struct{}
	wg         sync.WaitGroup
	startupErr error
	running    atomic.Bool
	stopped    atomic.Bool
}

// workerBackend abstracts queue-specific processing logic.
// This interface is unexported - users interact with Worker via NewMemoryWorker/NewNATSWorker.
type workerBackend interface {
	// start begins processing for a specific cluster.
	// For memory backend, cluster parameter is ignored (single worker processes all).
	// For NATS backend, one goroutine per cluster is started.
	// Must call wg.Done() when finished.
	start(cluster types.ClusterID)

	// numWorkers returns how many goroutines should be spawned.
	// Memory: 1, NATS: 2 (one per cluster)
	numWorkers() int

	// backendType returns a string identifier for debugging/logging.
	backendType() string
}

// Start begins processing replay messages.
//
// The number of worker goroutines depends on the backend:
//   - MemoryBackend: Single goroutine processing all messages
//   - NATSBackend: Two goroutines, one per cluster for parallel processing
//
// Returns:
//   - error: ErrWorkerAlreadyRunning if already started
func (w *Worker) Start() error {
	if w.startupErr != nil {
		return w.startupErr
	}
	if w.backend == nil {
		return errors.New("helix: worker is not initialized")
	}
	if w.stopped.Load() {
		return errors.New("helix: worker has been stopped")
	}
	if !w.running.CompareAndSwap(false, true) {
		return errors.New("helix: worker already running")
	}

	numWorkers := w.backend.numWorkers()
	w.wg.Add(numWorkers)

	if numWorkers == 1 {
		// Single worker (memory backend)
		go w.backend.start(types.ClusterA) // cluster param ignored for memory
	} else {
		// One worker per cluster (NATS backend)
		go w.backend.start(types.ClusterA)
		go w.backend.start(types.ClusterB)
	}

	return nil
}

// Stop gracefully stops the worker.
//
// It signals all goroutines to stop and waits for them to finish processing
// their current batch. This method blocks until all workers have stopped.
func (w *Worker) Stop() {
	if !w.running.CompareAndSwap(true, false) {
		return
	}

	w.stopped.Store(true)
	close(w.stopCh)
	w.wg.Wait()
}

// IsRunning returns whether the worker is currently running.
func (w *Worker) IsRunning() bool {
	return w.running.Load()
}

// SetClusterNames sets custom display names for clusters in log messages.
//
// This method is called by the client during initialization to propagate
// cluster names configured via WithClusterNames.
//
// Parameters:
//   - names: The cluster names to use in log messages
func (w *Worker) SetClusterNames(names types.ClusterNames) {
	w.config.ClusterNames = names
}

// MetricsConfigured reports whether the worker's metrics collector was
// explicitly set by the caller (via [WithWorkerMetrics]) at construction
// time.
//
// The helix CQLClient uses this to detect a worker that has not been
// given a metrics collector and inject its own (via [Worker.SetMetrics])
// so client and worker metrics share the same collector — preventing
// the surprise where worker-side IncReplaySuccess/Dropped/Error are
// silently invisible because the worker is using its internal NopMetrics.
//
// Returns:
//   - bool: true if WithWorkerMetrics was called at construction time
func (w *Worker) MetricsConfigured() bool {
	return w.config.metricsExplicit
}

// SetMetrics replaces the worker's metrics collector. No-op once
// [Worker.MetricsConfigured] returns true (the caller's explicit choice
// wins) or if m is nil. Successful injection marks the configuration
// as explicit so subsequent SetMetrics calls are no-ops.
//
// Intended for parent callers like helix.CQLClient that want to ensure
// client and worker metrics are unified without overwriting a deliberate
// per-worker choice.
//
// Parameters:
//   - m: The metrics collector to use
func (w *Worker) SetMetrics(m types.MetricsCollector) {
	if w.config.metricsExplicit {
		return
	}
	if typeutil.IsNilInterface(m) {
		return
	}
	w.config.Metrics = m
	w.config.metricsExplicit = true
	w.config.resolveBacklog()
}

// Metrics returns the worker's metrics collector. Useful for tests and
// for parent callers verifying metrics propagation.
func (w *Worker) Metrics() types.MetricsCollector {
	return w.config.Metrics
}

// BackendType returns the type of backend being used ("memory" or "nats").
// Useful for debugging and logging.
func (w *Worker) BackendType() string {
	return w.backend.backendType()
}

// resolveBacklog narrows Metrics to the optional backlog interface once so
// the workers can report without a type assertion per payload.
func (c *WorkerConfig) resolveBacklog() {
	c.backlog = c.backlogMetrics()
}

// backlogMetrics returns the resolved backlog collector, deriving it from
// Metrics for configurations built without finalizeWorkerConfig.
func (c *WorkerConfig) backlogMetrics() types.ReplayBacklogMetrics {
	if c.backlog != nil {
		return c.backlog
	}
	if bm, ok := c.Metrics.(types.ReplayBacklogMetrics); ok {
		return bm
	}

	return metrics.NewNopMetrics()
}

// observeAge publishes how old the payload about to be executed is,
// measured from its client-side write timestamp (microseconds).
func (c *WorkerConfig) observeAge(payload types.ReplayPayload) {
	if payload.Timestamp <= 0 {
		return
	}
	c.backlogMetrics().SetReplayOldestAge(payload.TargetCluster,
		max(float64(time.Now().UnixMicro()-payload.Timestamp)/1e6, 0))
}

// observeIdle resets the age gauge for a cluster with nothing pending.
func (c *WorkerConfig) observeIdle(cluster types.ClusterID) {
	c.backlogMetrics().SetReplayOldestAge(cluster, 0)
}

// allows reports whether replay to cluster may execute now. A nil gate
// permits everything; a gate that panics counts as closed.
func (c *WorkerConfig) allows(cluster types.ClusterID) (allowed bool) {
	if c.ClusterGate == nil {
		return true
	}
	defer func() {
		if r := recover(); r != nil {
			c.Logger.Error("replay cluster gate panicked; treating the cluster as gated",
				"cluster", c.ClusterNames.Name(cluster), "panic", r)
			allowed = false
		}
	}()

	return c.ClusterGate(cluster)
}

// observeDrop records one permanently dropped payload exactly once: the
// drop counters, the per-reason series, the log line, and OnDrop.
// reason is one of the types.ReplayDrop constants; attrs are extra log
// fields.
func (c *WorkerConfig) observeDrop(payload types.ReplayPayload, err error, reason string, attrs ...any) {
	cluster := payload.TargetCluster
	c.Metrics.IncReplayDropped(cluster)
	c.backlogMetrics().IncReplayWorkerDropped(cluster, reason)
	c.Logger.Error("replay message dropped", append([]any{
		"cluster", c.ClusterNames.Name(cluster),
		"reason", reason,
		"error", errString(err),
	}, attrs...)...)
	if c.OnDrop != nil {
		c.OnDrop(payload, err)
	}
}

// calculateBackoff calculates the backoff delay with exponential increase.
// Both backends use it to space out retries of a failed payload.
func calculateBackoff(attempt int, retryDelay, maxRetryDelay time.Duration) time.Duration {
	delay := retryDelay

	// Exponential backoff: delay * 2^(attempt-1)
	for i := 1; i < attempt && delay < maxRetryDelay; i++ {
		delay *= 2
	}

	if delay > maxRetryDelay {
		delay = maxRetryDelay
	}

	return delay
}
