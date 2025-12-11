package replay

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

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
	// Uses exponential backoff with jitter.
	// Default: 100ms
	RetryDelay time.Duration

	// MaxRetryDelay is the maximum delay between retries.
	// Default: 30 seconds
	MaxRetryDelay time.Duration

	// ExecuteTimeout is the timeout for each replay execution.
	// Default: 30 seconds
	ExecuteTimeout time.Duration

	// HighPriorityRatio controls the ratio of high-priority to low-priority message processing.
	// For every N high-priority batches processed, 1 low-priority batch is processed.
	// This prevents low-priority starvation while ensuring high-priority messages are preferred.
	// Set to 0 for equal priority processing (1:1 ratio).
	// Default: 10 (10:1 ratio - process 10 high-priority batches, then 1 low-priority)
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

	// OnDrop is called when a message exceeds max retries and is dropped (optional).
	// Only applicable for NATS replayer which has built-in max delivery.
	OnDrop func(payload types.ReplayPayload, err error)
}

// DefaultWorkerConfig returns the default worker configuration.
func DefaultWorkerConfig() WorkerConfig {
	return WorkerConfig{
		BatchSize:         100,
		PollInterval:      100 * time.Millisecond,
		RetryDelay:        100 * time.Millisecond,
		MaxRetryDelay:     30 * time.Second,
		ExecuteTimeout:    30 * time.Second,
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

// WithOnDrop sets the drop callback.
func WithOnDrop(fn func(types.ReplayPayload, error)) WorkerOption {
	return func(c *WorkerConfig) {
		c.OnDrop = fn
	}
}

// WithWorkerMetrics sets the metrics collector for the worker.
func WithWorkerMetrics(m types.MetricsCollector) WorkerOption {
	return func(c *WorkerConfig) {
		c.Metrics = m
	}
}

// WithWorkerLogger sets the logger for the worker.
func WithWorkerLogger(l types.Logger) WorkerOption {
	return func(c *WorkerConfig) {
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
	config  WorkerConfig
	execute ExecuteFunc
	backend workerBackend
	stopCh  chan struct{}
	wg      sync.WaitGroup
	running atomic.Bool
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

// BackendType returns the type of backend being used ("memory" or "nats").
// Useful for debugging and logging.
func (w *Worker) BackendType() string {
	return w.backend.backendType()
}

// calculateBackoff calculates the backoff delay with exponential increase.
// This is a standalone function used by memory backend for retry logic.
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
