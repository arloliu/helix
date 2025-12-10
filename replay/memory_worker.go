package replay

import (
	"context"
	"time"

	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/types"
)

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
