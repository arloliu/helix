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
	defer b.drainAndDrop() // Drain remaining queue items and notify OnDrop on exit.

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

		// Execute the replay with inline bounded retry. Returns true when the
		// caller should exit (stopCh observed during backoff).
		if b.executeWithRetry(payload) {
			return
		}
	}
}

// drainAndDrop dequeues all remaining items from the queue and calls OnDrop
// for each. Invoked when the worker exits to ensure no payload is silently lost.
func (b *memoryBackend) drainAndDrop() {
	for {
		payload, ok := b.replayer.TryDequeue()
		if !ok {
			return
		}

		b.config.Metrics.IncReplayDropped(payload.TargetCluster)
		b.config.Logger.Warn("replay message dropped during shutdown",
			"cluster", b.clusterName(payload.TargetCluster),
		)

		if b.config.OnDrop != nil {
			b.config.OnDrop(payload, nil)
		}
	}
}

// executeWithRetry runs a payload through up to MaxAttempts with exponential
// backoff between attempts. Returns true when stopCh was observed during a
// backoff sleep (the caller should exit). Returns false after success or
// after the payload is dropped via OnDrop because attempts were exhausted.
//
// Retries are inline (not re-enqueued). The previous re-enqueue strategy
// re-entered start() with attempt always 1, which collapsed the documented
// exponential backoff into a fixed RetryDelay and let a permanently-broken
// cluster bounce the same payload through the queue forever. Inline retry
// keeps the backoff honest and bounds attempts at MaxAttempts.
func (b *memoryBackend) executeWithRetry(payload types.ReplayPayload) (stopped bool) {
	maxAttempts := b.config.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		start := time.Now()
		err := b.executeOnce(payload)
		elapsed := time.Since(start).Seconds()

		if err == nil {
			b.config.Metrics.IncReplaySuccess(payload.TargetCluster)
			b.config.Metrics.ObserveReplayDuration(payload.TargetCluster, elapsed)
			if b.config.OnSuccess != nil {
				b.config.OnSuccess(payload)
			}

			return false
		}

		lastErr = err
		b.config.Metrics.IncReplayError(payload.TargetCluster)
		b.config.Metrics.ObserveReplayDuration(payload.TargetCluster, elapsed)
		b.config.Logger.Warn("replay execution failed",
			"cluster", b.clusterName(payload.TargetCluster),
			"attempt", attempt,
			"maxAttempts", maxAttempts,
			"error", err.Error(),
		)
		if b.config.OnError != nil {
			b.config.OnError(payload, err, attempt)
		}

		// No more attempts — fall through to the drop path below.
		if attempt == maxAttempts {
			break
		}

		delay := calculateBackoff(attempt, b.config.RetryDelay, b.config.MaxRetryDelay)
		timer := time.NewTimer(delay)
		select {
		case <-b.stopCh:
			timer.Stop()
			// Payload dropped during shutdown — notify callback so the
			// user has visibility into what was lost.
			b.config.Metrics.IncReplayDropped(payload.TargetCluster)
			b.config.Logger.Warn("replay message dropped during shutdown",
				"cluster", b.clusterName(payload.TargetCluster),
			)
			if b.config.OnDrop != nil {
				b.config.OnDrop(payload, err)
			}

			return true
		case <-timer.C:
		}
	}

	// Attempts exhausted — drop the payload.
	b.config.Metrics.IncReplayDropped(payload.TargetCluster)
	b.config.Logger.Error("replay message dropped, max attempts exceeded",
		"cluster", b.clusterName(payload.TargetCluster),
		"maxAttempts", maxAttempts,
		"error", lastErr.Error(),
	)
	if b.config.OnDrop != nil {
		b.config.OnDrop(payload, lastErr)
	}

	return false
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
