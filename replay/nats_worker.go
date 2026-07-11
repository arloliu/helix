package replay

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/arloliu/helix/types"
)

// natsBackend implements workerBackend for NATSReplayer.
type natsBackend struct {
	replayer *NATSReplayer
	config   *WorkerConfig
	execute  ExecuteFunc
	stopCh   <-chan struct{}
	wg       *sync.WaitGroup

	// backoffWait is a test seam allowing deterministic observation of
	// backoff entry: when set, start() calls it instead of time.After to
	// obtain the channel it waits on before retrying a dequeue. Production
	// code always leaves this nil, in which case start() falls back to
	// time.After — zero behavior change outside of tests.
	backoffWait func(d time.Duration) <-chan time.Time
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
// msgs may be non-empty even when err is non-nil — a batch fetch interrupted
// mid-stream can still carry successfully fetched messages, and callers must
// forward/process those before handling err.
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

	wait := b.backoffWait
	if wait == nil {
		wait = time.After
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

		// A batch fetch that is interrupted mid-stream (consumer deleted,
		// ctx cancel, transport error) can still carry successfully fetched
		// messages alongside the error. Process those first so they are
		// Ack'd/Nak'd/Term'd before the error branch below backs off -
		// otherwise each occurrence would burn a JetStream delivery attempt
		// without execute() ever running on the fetched messages.
		if len(result.msgs) > 0 {
			b.processMessages(result.msgs)
		}

		if result.err != nil {
			b.config.Logger.Error("failed to dequeue replay messages",
				"cluster", b.clusterName(cluster),
				"error", result.err.Error(),
			)
			// Wait before retrying
			select {
			case <-b.stopCh:
				return
			case <-wait(b.config.PollInterval):
				continue
			}
		}

		if len(result.msgs) == 0 {
			// No messages, wait before polling again
			select {
			case <-b.stopCh:
				return
			case <-wait(b.config.PollInterval):
				continue
			}
		}
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
		// Forward messages fetched before the error; see natsDequeueResult.
		// highProcessed is left at its zero value here, matching the
		// success branch below: dequeueLowFirst has no incoming counter to
		// preserve (unlike dequeueHighFirst, it isn't passed one), and any
		// msgs forwarded alongside the error are still low-priority
		// progress, so resetting is the correct, consistent fairness
		// accounting whether or not msgs is empty.
		return natsDequeueResult{msgs: msgs, err: err}
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
		// Forward messages fetched before the error; see natsDequeueResult.
		// Only advance the counter when messages were actually forwarded:
		// start() processes and forwards result.msgs regardless of err, so
		// a forwarded batch is real high-priority progress and must count
		// toward HighPriorityRatio the same as the error-free success
		// branch below. A zero-message error means nothing was processed,
		// so the counter is left untouched — advancing it here would let
		// repeated empty-batch errors fabricate progress and unfairly
		// starve low priority once ratio-many errors accumulate.
		newCount := highProcessed
		if len(msgs) > 0 {
			newCount = highProcessed + 1
		}

		return natsDequeueResult{msgs: msgs, err: err, highProcessed: newCount}
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
//
// On shutdown, the current message AND all remaining messages in the batch
// are Nak'd so they redeliver immediately. Without naking the tail, those
// messages would sit unacknowledged until AckWait expires (default 30s)
// before a restarted worker could re-process them — a long visible delay
// during graceful restarts.
func (b *natsBackend) processMessages(msgs []ReplayMessage) {
	for i, msg := range msgs {
		select {
		case <-b.stopCh:
			// Nak the current message AND every unprocessed message left
			// in the batch. Each Nak is independent; an error on one does
			// not block the others.
			for j := i; j < len(msgs); j++ {
				b.nakMessage(msgs[j], "shutdown")
			}

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
				if b.termMessage(msg, "max retries exceeded") {
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
				}
			} else {
				// Nak for redelivery
				b.nakMessage(msg, "retry")
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
			if ackErr := b.ackMessage(msg); ackErr != nil {
				b.config.Metrics.IncReplayError(msg.Payload.TargetCluster)
				b.config.Metrics.ObserveReplayDuration(msg.Payload.TargetCluster, elapsed)
				if b.config.OnError != nil {
					b.config.OnError(msg.Payload, ackErr, int(msg.DeliveryCount)) //nolint:gosec // safe conversion for small values
				}

				continue
			}
			b.config.Metrics.IncReplaySuccess(msg.Payload.TargetCluster)
			b.config.Metrics.ObserveReplayDuration(msg.Payload.TargetCluster, elapsed)
			if b.config.OnSuccess != nil {
				b.config.OnSuccess(msg.Payload)
			}
		}
	}
}

func (b *natsBackend) ackMessage(msg ReplayMessage) error {
	err := msg.Ack()
	if err == nil {
		return nil
	}

	wrapped := fmt.Errorf("helix: failed to ack replay message after successful execution: %w", err)
	b.config.Logger.Error("failed to ack replay message after successful execution",
		"cluster", b.clusterName(msg.Payload.TargetCluster),
		"attempt", msg.DeliveryCount,
		"error", err.Error(),
	)

	return wrapped
}

func (b *natsBackend) nakMessage(msg ReplayMessage, reason string) {
	if err := msg.Nak(); err != nil {
		b.config.Logger.Error("failed to nak replay message",
			"cluster", b.clusterName(msg.Payload.TargetCluster),
			"reason", reason,
			"attempt", msg.DeliveryCount,
			"error", err.Error(),
		)
	}
}

func (b *natsBackend) termMessage(msg ReplayMessage, reason string) bool {
	if err := msg.Term(); err != nil {
		b.config.Logger.Error("failed to terminate replay message",
			"cluster", b.clusterName(msg.Payload.TargetCluster),
			"reason", reason,
			"attempt", msg.DeliveryCount,
			"maxDeliver", msg.MaxDeliver,
			"error", err.Error(),
		)

		return false
	}

	return true
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
//
// For production configuration that should fail fast on invalid inputs,
// use [NewNATSWorkerChecked].
func NewNATSWorker(replayer *NATSReplayer, execute ExecuteFunc, opts ...WorkerOption) *Worker {
	config := buildWorkerConfig(opts...)
	normalizeWorkerConfig(&config)
	finalizeWorkerConfig(&config)

	return newNATSWorkerWithConfig(config, replayer, execute,
		validateWorkerInputs(replayer != nil, execute))
}

// NewNATSWorkerChecked creates a worker that processes messages from a
// NATSReplayer and returns a validation error when constructor inputs or
// option values are invalid.
//
// Parameters:
//   - replayer: The NATS replayer to consume from
//   - execute: Function to execute replay payloads
//   - opts: Optional configuration options
//
// Returns:
//   - *Worker: A new worker instance
//   - error: Joined [types.OptionError] values when one or more inputs are invalid
func NewNATSWorkerChecked(replayer *NATSReplayer, execute ExecuteFunc, opts ...WorkerOption) (*Worker, error) {
	config := buildWorkerConfig(opts...)
	validationErr := joinValidationErrors(
		validateWorkerInputsForChecked(natsWorkerComponent, replayer != nil, execute),
		validateWorkerConfigForChecked(config, natsWorkerComponent),
	)
	if validationErr != nil {
		return nil, validationErr
	}
	finalizeWorkerConfig(&config)

	return newNATSWorkerWithConfig(config, replayer, execute, nil), nil
}

func newNATSWorkerWithConfig(
	config WorkerConfig,
	replayer *NATSReplayer,
	execute ExecuteFunc,
	startupErr error,
) *Worker {
	w := &Worker{
		config:     config,
		execute:    execute,
		stopCh:     make(chan struct{}),
		startupErr: startupErr,
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
