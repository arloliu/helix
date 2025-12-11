package replay

import (
	"context"
	"sync"
	"time"

	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/types"
)

// natsBackend implements workerBackend for NATSReplayer.
type natsBackend struct {
	replayer *NATSReplayer
	config   *WorkerConfig
	execute  ExecuteFunc
	stopCh   <-chan struct{}
	wg       *sync.WaitGroup
}

// Compile-time assertion that natsBackend implements workerBackend.
var _ workerBackend = (*natsBackend)(nil)

func (b *natsBackend) numWorkers() int {
	return 2 // One per cluster
}

func (b *natsBackend) backendType() string {
	return "nats"
}

// natsDequeueResult holds the result of a priority-aware dequeue operation.
type natsDequeueResult struct {
	msgs          []ReplayMessage
	err           error
	highProcessed int
}

// start processes messages for a specific cluster from NATSReplayer.
// Uses priority-aware dequeue with configurable ratio-based fair scheduling.
func (b *natsBackend) start(cluster types.ClusterID) {
	defer b.wg.Done()

	highProcessed := 0
	ratio := b.config.HighPriorityRatio
	if ratio <= 0 {
		ratio = 1 // 1:1 equal priority
	}

	for {
		select {
		case <-b.stopCh:
			return
		default:
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		result := b.dequeueWithPriority(ctx, cluster, highProcessed, ratio)
		cancel()

		highProcessed = result.highProcessed

		if result.err != nil {
			b.config.Logger.Error("failed to dequeue replay messages",
				"cluster", b.clusterName(cluster),
				"error", result.err.Error(),
			)
			// Wait before retrying
			select {
			case <-b.stopCh:
				return
			case <-time.After(b.config.PollInterval):
				continue
			}
		}

		if len(result.msgs) == 0 {
			// No messages, wait before polling again
			select {
			case <-b.stopCh:
				return
			case <-time.After(b.config.PollInterval):
				continue
			}
		}

		// Process each message
		b.processMessages(result.msgs)
	}
}

// dequeueWithPriority fetches messages based on priority settings.
func (b *natsBackend) dequeueWithPriority(ctx context.Context, cluster types.ClusterID, highProcessed, ratio int) natsDequeueResult {
	if b.config.StrictPriority {
		return b.dequeueStrict(ctx, cluster)
	}

	return b.dequeueWithRatio(ctx, cluster, highProcessed, ratio)
}

// dequeueStrict drains high priority completely before low priority.
func (b *natsBackend) dequeueStrict(ctx context.Context, cluster types.ClusterID) natsDequeueResult {
	msgs, err := b.replayer.DequeueByPriority(ctx, cluster, types.PriorityHigh, b.config.BatchSize)
	if err != nil || len(msgs) > 0 {
		return natsDequeueResult{msgs: msgs, err: err}
	}
	// High queue empty, try low
	msgs, err = b.replayer.DequeueByPriority(ctx, cluster, types.PriorityLow, b.config.BatchSize)

	return natsDequeueResult{msgs: msgs, err: err}
}

// dequeueWithRatio uses ratio-based fair scheduling between priorities.
func (b *natsBackend) dequeueWithRatio(ctx context.Context, cluster types.ClusterID, highProcessed, ratio int) natsDequeueResult {
	shouldProcessLow := highProcessed >= ratio

	if shouldProcessLow {
		return b.dequeueLowFirst(ctx, cluster)
	}

	return b.dequeueHighFirst(ctx, cluster, highProcessed)
}

// dequeueLowFirst tries low priority first, falls back to high.
func (b *natsBackend) dequeueLowFirst(ctx context.Context, cluster types.ClusterID) natsDequeueResult {
	msgs, err := b.replayer.DequeueByPriority(ctx, cluster, types.PriorityLow, b.config.BatchSize)
	if err != nil {
		return natsDequeueResult{err: err}
	}
	if len(msgs) > 0 {
		return natsDequeueResult{msgs: msgs, highProcessed: 0} // Reset counter
	}
	// Low queue empty, fall back to high
	msgs, err = b.replayer.DequeueByPriority(ctx, cluster, types.PriorityHigh, b.config.BatchSize)
	newCount := 0
	if len(msgs) > 0 {
		newCount = 1
	}

	return natsDequeueResult{msgs: msgs, err: err, highProcessed: newCount}
}

// dequeueHighFirst tries high priority first, falls back to low.
func (b *natsBackend) dequeueHighFirst(ctx context.Context, cluster types.ClusterID, highProcessed int) natsDequeueResult {
	msgs, err := b.replayer.DequeueByPriority(ctx, cluster, types.PriorityHigh, b.config.BatchSize)
	if err != nil {
		return natsDequeueResult{err: err, highProcessed: highProcessed}
	}
	if len(msgs) > 0 {
		return natsDequeueResult{msgs: msgs, highProcessed: highProcessed + 1}
	}
	// High queue empty, try low (don't starve if high is empty)
	msgs, err = b.replayer.DequeueByPriority(ctx, cluster, types.PriorityLow, b.config.BatchSize)
	newCount := highProcessed
	if len(msgs) > 0 {
		newCount = 0 // Reset counter
	}

	return natsDequeueResult{msgs: msgs, err: err, highProcessed: newCount}
}

// processMessages processes a batch of NATS replay messages.
func (b *natsBackend) processMessages(msgs []ReplayMessage) {
	for _, msg := range msgs {
		select {
		case <-b.stopCh:
			// Nak remaining messages for redelivery
			_ = msg.Nak()

			return
		default:
		}

		start := time.Now()
		err := b.executeOnce(msg.Payload)
		elapsed := time.Since(start).Seconds()

		if err != nil {
			// Compare using uint64 to avoid integer overflow warning
			// MaxDeliver is always a small positive int (typically 1-10), so this is safe
			isLastAttempt := msg.MaxDeliver > 0 && msg.DeliveryCount >= uint64(msg.MaxDeliver)

			b.config.Metrics.IncReplayError(msg.Payload.TargetCluster)
			b.config.Metrics.ObserveReplayDuration(msg.Payload.TargetCluster, elapsed)

			if isLastAttempt {
				// This is the last attempt - message will be dropped
				// Use Term() to explicitly terminate, preventing any redelivery
				_ = msg.Term()
				b.config.Metrics.IncReplayDropped(msg.Payload.TargetCluster)
				b.config.Logger.Error("replay execution failed, max retries exceeded, message dropped",
					"cluster", b.clusterName(msg.Payload.TargetCluster),
					"attempt", msg.DeliveryCount,
					"maxDeliver", msg.MaxDeliver,
					"error", err.Error(),
				)
				if b.config.OnDrop != nil {
					b.config.OnDrop(msg.Payload, err)
				}
			} else {
				// Nak for redelivery
				_ = msg.Nak()
				b.config.Logger.Warn("replay execution failed, will retry",
					"cluster", b.clusterName(msg.Payload.TargetCluster),
					"attempt", msg.DeliveryCount,
					"maxDeliver", msg.MaxDeliver,
					"error", err.Error(),
				)
				if b.config.OnError != nil {
					// Safe cast: DeliveryCount is always a small positive number (typically 1-10)
					b.config.OnError(msg.Payload, err, int(msg.DeliveryCount)) //nolint:gosec // safe conversion for small values
				}
			}
		} else {
			// Ack on success
			_ = msg.Ack()
			b.config.Metrics.IncReplaySuccess(msg.Payload.TargetCluster)
			b.config.Metrics.ObserveReplayDuration(msg.Payload.TargetCluster, elapsed)
			if b.config.OnSuccess != nil {
				b.config.OnSuccess(msg.Payload)
			}
		}
	}
}

// executeOnce executes a single replay attempt with timeout.
func (b *natsBackend) executeOnce(payload types.ReplayPayload) error {
	ctx, cancel := context.WithTimeout(context.Background(), b.config.ExecuteTimeout)
	defer cancel()

	return b.execute(ctx, payload)
}

// clusterName returns the display name for the given cluster.
func (b *natsBackend) clusterName(cluster types.ClusterID) string {
	return b.config.ClusterNames.Name(cluster)
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

	w := &Worker{
		config:  config,
		execute: execute,
		stopCh:  make(chan struct{}),
	}

	// Create and inject the NATS backend
	w.backend = &natsBackend{
		replayer: replayer,
		config:   &w.config,
		execute:  execute,
		stopCh:   w.stopCh,
		wg:       &w.wg,
	}

	return w
}
