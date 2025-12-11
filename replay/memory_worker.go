package replay

import (
	"context"
	"sync"
	"time"

	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/types"
)

// memoryBackend implements workerBackend for MemoryReplayer.
type memoryBackend struct {
	replayer *MemoryReplayer
	config   *WorkerConfig
	execute  ExecuteFunc
	stopCh   <-chan struct{}
	wg       *sync.WaitGroup
}

// Compile-time assertion that memoryBackend implements workerBackend.
var _ workerBackend = (*memoryBackend)(nil)

func (b *memoryBackend) numWorkers() int {
	return 1
}

func (b *memoryBackend) backendType() string {
	return "memory"
}

// start processes messages from the MemoryReplayer.
// The cluster parameter is ignored since memory backend uses a single worker.
func (b *memoryBackend) start(_ types.ClusterID) {
	defer b.wg.Done()

	for {
		select {
		case <-b.stopCh:
			return
		default:
		}

		// Try to dequeue a message
		payload, ok := b.replayer.TryDequeue()
		if !ok {
			// Queue is empty, wait before polling again
			select {
			case <-b.stopCh:
				return
			case <-time.After(b.config.PollInterval):
				continue
			}
		}

		// Execute the replay
		b.executeWithRetry(payload, 1)
	}
}

// executeWithRetry executes a replay with exponential backoff retry.
// Used for MemoryReplayer which doesn't have built-in retry.
func (b *memoryBackend) executeWithRetry(payload types.ReplayPayload, attempt int) {
	start := time.Now()
	err := b.executeOnce(payload)
	elapsed := time.Since(start).Seconds()

	if err == nil {
		b.config.Metrics.IncReplaySuccess(payload.TargetCluster)
		b.config.Metrics.ObserveReplayDuration(payload.TargetCluster, elapsed)
		if b.config.OnSuccess != nil {
			b.config.OnSuccess(payload)
		}

		return
	}

	b.config.Metrics.IncReplayError(payload.TargetCluster)
	b.config.Metrics.ObserveReplayDuration(payload.TargetCluster, elapsed)
	b.config.Logger.Warn("replay execution failed",
		"cluster", b.clusterName(payload.TargetCluster),
		"attempt", attempt,
		"error", err.Error(),
	)
	if b.config.OnError != nil {
		b.config.OnError(payload, err, attempt)
	}

	// For memory replayer, re-enqueue with backoff
	// (since we can't put it back at front of queue, we just re-enqueue)
	delay := calculateBackoff(attempt, b.config.RetryDelay, b.config.MaxRetryDelay)

	select {
	case <-b.stopCh:
		return
	case <-time.After(delay):
	}

	// Re-enqueue for retry
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if enqErr := b.replayer.Enqueue(ctx, payload); enqErr != nil {
		// Queue is full, drop the message
		b.config.Metrics.IncReplayDropped(payload.TargetCluster)
		b.config.Logger.Error("replay message dropped, queue full",
			"cluster", b.clusterName(payload.TargetCluster),
			"error", err.Error(),
		)
		if b.config.OnDrop != nil {
			b.config.OnDrop(payload, err)
		}
	}
}

// executeOnce executes a single replay attempt with timeout.
func (b *memoryBackend) executeOnce(payload types.ReplayPayload) error {
	ctx, cancel := context.WithTimeout(context.Background(), b.config.ExecuteTimeout)
	defer cancel()

	return b.execute(ctx, payload)
}

// clusterName returns the display name for the given cluster.
func (b *memoryBackend) clusterName(cluster types.ClusterID) string {
	return b.config.ClusterNames.Name(cluster)
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

	w := &Worker{
		config:  config,
		execute: execute,
		stopCh:  make(chan struct{}),
	}

	// Create and inject the memory backend
	w.backend = &memoryBackend{
		replayer: replayer,
		config:   &w.config,
		execute:  execute,
		stopCh:   w.stopCh,
		wg:       &w.wg,
	}

	return w
}
