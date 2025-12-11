package replay

import (
	"context"
	"time"

	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/types"
)

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

// natsDequeueResult holds the result of a priority-aware dequeue operation.
type natsDequeueResult struct {
	msgs          []ReplayMessage
	err           error
	highProcessed int
}

// dequeueWithPriority fetches messages based on priority settings.
func (w *Worker) dequeueWithPriority(ctx context.Context, cluster types.ClusterID, highProcessed, ratio int) natsDequeueResult {
	if w.config.StrictPriority {
		return w.dequeueStrict(ctx, cluster)
	}
	return w.dequeueWithRatio(ctx, cluster, highProcessed, ratio)
}

// dequeueStrict drains high priority completely before low priority.
func (w *Worker) dequeueStrict(ctx context.Context, cluster types.ClusterID) natsDequeueResult {
	msgs, err := w.natsReplayer.DequeueByPriority(ctx, cluster, types.PriorityHigh, w.config.BatchSize)
	if err != nil || len(msgs) > 0 {
		return natsDequeueResult{msgs: msgs, err: err}
	}
	// High queue empty, try low
	msgs, err = w.natsReplayer.DequeueByPriority(ctx, cluster, types.PriorityLow, w.config.BatchSize)
	return natsDequeueResult{msgs: msgs, err: err}
}

// dequeueWithRatio uses ratio-based fair scheduling between priorities.
func (w *Worker) dequeueWithRatio(ctx context.Context, cluster types.ClusterID, highProcessed, ratio int) natsDequeueResult {
	shouldProcessLow := highProcessed >= ratio

	if shouldProcessLow {
		return w.dequeueLowFirst(ctx, cluster)
	}
	return w.dequeueHighFirst(ctx, cluster, highProcessed)
}

// dequeueLowFirst tries low priority first, falls back to high.
func (w *Worker) dequeueLowFirst(ctx context.Context, cluster types.ClusterID) natsDequeueResult {
	msgs, err := w.natsReplayer.DequeueByPriority(ctx, cluster, types.PriorityLow, w.config.BatchSize)
	if err != nil {
		return natsDequeueResult{err: err}
	}
	if len(msgs) > 0 {
		return natsDequeueResult{msgs: msgs, highProcessed: 0} // Reset counter
	}
	// Low queue empty, fall back to high
	msgs, err = w.natsReplayer.DequeueByPriority(ctx, cluster, types.PriorityHigh, w.config.BatchSize)
	newCount := 0
	if len(msgs) > 0 {
		newCount = 1
	}

	return natsDequeueResult{msgs: msgs, err: err, highProcessed: newCount}
}

// dequeueHighFirst tries high priority first, falls back to low.
func (w *Worker) dequeueHighFirst(ctx context.Context, cluster types.ClusterID, highProcessed int) natsDequeueResult {
	msgs, err := w.natsReplayer.DequeueByPriority(ctx, cluster, types.PriorityHigh, w.config.BatchSize)
	if err != nil {
		return natsDequeueResult{err: err, highProcessed: highProcessed}
	}
	if len(msgs) > 0 {
		return natsDequeueResult{msgs: msgs, highProcessed: highProcessed + 1}
	}
	// High queue empty, try low (don't starve if high is empty)
	msgs, err = w.natsReplayer.DequeueByPriority(ctx, cluster, types.PriorityLow, w.config.BatchSize)
	newCount := highProcessed
	if len(msgs) > 0 {
		newCount = 0 // Reset counter
	}

	return natsDequeueResult{msgs: msgs, err: err, highProcessed: newCount}
}

// processNATSCluster processes messages for a specific cluster from NATSReplayer.
// Uses priority-aware dequeue with configurable ratio-based fair scheduling.
func (w *Worker) processNATSCluster(cluster types.ClusterID) {
	defer w.wg.Done()

	highProcessed := 0
	ratio := w.config.HighPriorityRatio
	if ratio <= 0 {
		ratio = 1 // 1:1 equal priority
	}

	for {
		select {
		case <-w.stopCh:
			return
		default:
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		result := w.dequeueWithPriority(ctx, cluster, highProcessed, ratio)
		cancel()

		highProcessed = result.highProcessed

		if result.err != nil {
			w.config.Logger.Error("failed to dequeue replay messages",
				"cluster", w.clusterName(cluster),
				"error", result.err.Error(),
			)
			// Wait before retrying
			select {
			case <-w.stopCh:
				return
			case <-time.After(w.config.PollInterval):
				continue
			}
		}

		if len(result.msgs) == 0 {
			// No messages, wait before polling again
			select {
			case <-w.stopCh:
				return
			case <-time.After(w.config.PollInterval):
				continue
			}
		}

		// Process each message
		w.processNATSMessages(result.msgs)
	}
}

// processNATSMessages processes a batch of NATS replay messages.
func (w *Worker) processNATSMessages(msgs []ReplayMessage) {
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
			// Compare using uint64 to avoid integer overflow warning
			// MaxDeliver is always a small positive int (typically 1-10), so this is safe
			isLastAttempt := msg.MaxDeliver > 0 && msg.DeliveryCount >= uint64(msg.MaxDeliver)

			w.config.Metrics.IncReplayError(msg.Payload.TargetCluster)
			w.config.Metrics.ObserveReplayDuration(msg.Payload.TargetCluster, elapsed)

			if isLastAttempt {
				// This is the last attempt - message will be dropped
				// Use Term() to explicitly terminate, preventing any redelivery
				_ = msg.Term()
				w.config.Metrics.IncReplayDropped(msg.Payload.TargetCluster)
				w.config.Logger.Error("replay execution failed, max retries exceeded, message dropped",
					"cluster", w.clusterName(msg.Payload.TargetCluster),
					"attempt", msg.DeliveryCount,
					"maxDeliver", msg.MaxDeliver,
					"error", err.Error(),
				)
				if w.config.OnDrop != nil {
					w.config.OnDrop(msg.Payload, err)
				}
			} else {
				// Nak for redelivery
				_ = msg.Nak()
				w.config.Logger.Warn("replay execution failed, will retry",
					"cluster", w.clusterName(msg.Payload.TargetCluster),
					"attempt", msg.DeliveryCount,
					"maxDeliver", msg.MaxDeliver,
					"error", err.Error(),
				)
				if w.config.OnError != nil {
					// Safe cast: DeliveryCount is always a small positive number (typically 1-10)
					w.config.OnError(msg.Payload, err, int(msg.DeliveryCount)) //nolint:gosec // safe conversion for small values
				}
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
