package replay

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/types"
)

// ExecuteFunc is a function that executes a replay payload against a target cluster.
// It receives the payload and should execute the query against the appropriate cluster.
// Returns nil on success, error on failure.
type ExecuteFunc func(ctx context.Context, payload types.ReplayPayload) error

// WorkerConfig configures the replay worker.
type WorkerConfig struct {
	// BatchSize is the number of messages to fetch per dequeue operation.
	// Default: 10
	BatchSize int

	// PollInterval is the interval between dequeue attempts when the queue is empty.
	// Default: 1 second
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
		BatchSize:      100,
		PollInterval:   100 * time.Millisecond,
		RetryDelay:     100 * time.Millisecond,
		MaxRetryDelay:  30 * time.Second,
		ExecuteTimeout: 30 * time.Second,
		ClusterNames:   types.DefaultClusterNames(),
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
// The worker runs one goroutine per cluster to allow parallel processing.
// It supports both MemoryReplayer and NATSReplayer through the Replayer interface.
type Worker struct {
	config  WorkerConfig
	execute ExecuteFunc
	stopCh  chan struct{}
	wg      sync.WaitGroup
	running atomic.Bool

	// For MemoryReplayer
	memoryReplayer *MemoryReplayer

	// For NATSReplayer
	natsReplayer *NATSReplayer
}

// NewMemoryWorker creates a worker that processes messages from a MemoryReplayer.
//
// Parameters:
//   - replayer: The memory replayer to consume from
//   - execute: Function to execute replay payloads
//   - opts: Optional configuration options
//
// Returns:
//   - *Worker: A new worker instance
func NewMemoryWorker(replayer *MemoryReplayer, execute ExecuteFunc, opts ...WorkerOption) *Worker {
	config := DefaultWorkerConfig()
	for _, opt := range opts {
		opt(&config)
	}

	// Ensure metrics is never nil
	if config.Metrics == nil {
		config.Metrics = metrics.NewNopMetrics()
	}

	// Ensure logger is never nil
	if config.Logger == nil {
		config.Logger = logging.NewNopLogger()
	}

	return &Worker{
		config:         config,
		execute:        execute,
		stopCh:         make(chan struct{}),
		memoryReplayer: replayer,
	}
}

// NewNATSWorker creates a worker that processes messages from a NATSReplayer.
//
// Parameters:
//   - replayer: The NATS replayer to consume from
//   - execute: Function to execute replay payloads
//   - opts: Optional configuration options
//
// Returns:
//   - *Worker: A new worker instance
func NewNATSWorker(replayer *NATSReplayer, execute ExecuteFunc, opts ...WorkerOption) *Worker {
	config := DefaultWorkerConfig()
	for _, opt := range opts {
		opt(&config)
	}

	// Ensure metrics is never nil
	if config.Metrics == nil {
		config.Metrics = metrics.NewNopMetrics()
	}

	// Ensure logger is never nil
	if config.Logger == nil {
		config.Logger = logging.NewNopLogger()
	}

	return &Worker{
		config:       config,
		execute:      execute,
		stopCh:       make(chan struct{}),
		natsReplayer: replayer,
	}
}

// Start begins processing replay messages.
//
// For MemoryReplayer: Starts a single worker goroutine that processes all messages.
// For NATSReplayer: Starts two goroutines, one per cluster, for parallel processing.
//
// Returns:
//   - error: ErrWorkerAlreadyRunning if already started
func (w *Worker) Start() error {
	if !w.running.CompareAndSwap(false, true) {
		return errors.New("helix: worker already running")
	}

	if w.natsReplayer != nil {
		// Start one worker per cluster for NATS
		w.wg.Add(2)
		go w.processNATSCluster(types.ClusterA)
		go w.processNATSCluster(types.ClusterB)
	} else if w.memoryReplayer != nil {
		// Single worker for memory replayer
		w.wg.Add(1)
		go w.processMemory()
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

// processMemory processes messages from the MemoryReplayer.
func (w *Worker) processMemory() {
	defer w.wg.Done()

	for {
		select {
		case <-w.stopCh:
			return
		default:
		}

		// Try to dequeue a message
		payload, ok := w.memoryReplayer.TryDequeue()
		if !ok {
			// Queue is empty, wait before polling again
			select {
			case <-w.stopCh:
				return
			case <-time.After(w.config.PollInterval):
				continue
			}
		}

		// Execute the replay
		w.executeWithRetry(payload, 1)
	}
}

// processNATSCluster processes messages for a specific cluster from NATSReplayer.
func (w *Worker) processNATSCluster(cluster types.ClusterID) {
	defer w.wg.Done()

	for {
		select {
		case <-w.stopCh:
			return
		default:
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		msgs, err := w.natsReplayer.Dequeue(ctx, cluster, w.config.BatchSize)
		cancel()

		if err != nil {
			w.config.Logger.Error("failed to dequeue replay messages",
				"cluster", w.clusterName(cluster),
				"error", err.Error(),
			)
			// Wait before retrying
			select {
			case <-w.stopCh:
				return
			case <-time.After(w.config.PollInterval):
				continue
			}
		}

		if len(msgs) == 0 {
			// No messages, wait before polling again
			select {
			case <-w.stopCh:
				return
			case <-time.After(w.config.PollInterval):
				continue
			}
		}

		// Process each message
		for _, msg := range msgs {
			select {
			case <-w.stopCh:
				// Nak remaining messages for redelivery
				_ = msg.Nak()

				return
			default:
			}

			start := time.Now()
			err := w.executeOnce(msg.Payload)
			elapsed := time.Since(start).Seconds()

			if err != nil {
				// Nak for redelivery (NATS handles retry count via MaxDeliver)
				_ = msg.Nak()
				w.config.Metrics.IncReplayError(msg.Payload.TargetCluster)
				w.config.Metrics.ObserveReplayDuration(msg.Payload.TargetCluster, elapsed)
				w.config.Logger.Warn("replay execution failed, will retry",
					"cluster", w.clusterName(msg.Payload.TargetCluster),
					"error", err.Error(),
				)
				if w.config.OnError != nil {
					w.config.OnError(msg.Payload, err, 1)
				}
			} else {
				// Ack on success
				_ = msg.Ack()
				w.config.Metrics.IncReplaySuccess(msg.Payload.TargetCluster)
				w.config.Metrics.ObserveReplayDuration(msg.Payload.TargetCluster, elapsed)
				if w.config.OnSuccess != nil {
					w.config.OnSuccess(msg.Payload)
				}
			}
		}
	}
}

// executeWithRetry executes a replay with exponential backoff retry.
// Used for MemoryReplayer which doesn't have built-in retry.
func (w *Worker) executeWithRetry(payload types.ReplayPayload, attempt int) {
	start := time.Now()
	err := w.executeOnce(payload)
	elapsed := time.Since(start).Seconds()

	if err == nil {
		w.config.Metrics.IncReplaySuccess(payload.TargetCluster)
		w.config.Metrics.ObserveReplayDuration(payload.TargetCluster, elapsed)
		if w.config.OnSuccess != nil {
			w.config.OnSuccess(payload)
		}

		return
	}

	w.config.Metrics.IncReplayError(payload.TargetCluster)
	w.config.Metrics.ObserveReplayDuration(payload.TargetCluster, elapsed)
	w.config.Logger.Warn("replay execution failed",
		"cluster", w.clusterName(payload.TargetCluster),
		"attempt", attempt,
		"error", err.Error(),
	)
	if w.config.OnError != nil {
		w.config.OnError(payload, err, attempt)
	}

	// For memory replayer, re-enqueue with backoff
	// (since we can't put it back at front of queue, we just re-enqueue)
	delay := w.calculateBackoff(attempt)

	select {
	case <-w.stopCh:
		return
	case <-time.After(delay):
	}

	// Re-enqueue for retry
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if enqErr := w.memoryReplayer.Enqueue(ctx, payload); enqErr != nil {
		// Queue is full, drop the message
		w.config.Metrics.IncReplayDropped(payload.TargetCluster)
		w.config.Logger.Error("replay message dropped, queue full",
			"cluster", w.clusterName(payload.TargetCluster),
			"error", err.Error(),
		)
		if w.config.OnDrop != nil {
			w.config.OnDrop(payload, err)
		}
	}
}

// executeOnce executes a single replay attempt.
func (w *Worker) executeOnce(payload types.ReplayPayload) error {
	ctx, cancel := context.WithTimeout(context.Background(), w.config.ExecuteTimeout)
	defer cancel()

	return w.execute(ctx, payload)
}

// calculateBackoff calculates the backoff delay with exponential increase.
func (w *Worker) calculateBackoff(attempt int) time.Duration {
	delay := w.config.RetryDelay

	// Exponential backoff: delay * 2^(attempt-1)
	for i := 1; i < attempt && delay < w.config.MaxRetryDelay; i++ {
		delay *= 2
	}

	if delay > w.config.MaxRetryDelay {
		delay = w.config.MaxRetryDelay
	}

	return delay
}

// clusterName returns the display name for the given cluster.
func (w *Worker) clusterName(cluster types.ClusterID) string {
	return w.config.ClusterNames.Name(cluster)
}
