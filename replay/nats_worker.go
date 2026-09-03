package replay

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/arloliu/helix/types"
)

// depthRefreshInterval bounds how often a NATS worker goroutine asks the
// server for consumer info to publish the queue depth gauge.
const depthRefreshInterval = time.Second

// maxRedeliverySteps caps the length of the server-side redelivery schedule
// handed to a consumer under RetryWhileRetained.
const maxRedeliverySteps = 8

// natsBackend implements workerBackend for NATSReplayer.
type natsBackend struct {
	replayer *NATSReplayer
	config   *WorkerConfig
	execute  ExecuteFunc
	stopCh   <-chan struct{}
	wg       *sync.WaitGroup

	// deadLetters counts, per stream sequence, the attempts the classifier
	// marked dead-letter under RetryWhileRetained. Both cluster goroutines
	// share it; sequence numbers are unique across the stream. It is not
	// persisted, so a restart resets the poison budget.
	dlMu        sync.Mutex
	deadLetters map[uint64]int

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

	var nextDepthAt time.Time
	retained := b.config.RetryPolicy == RetryWhileRetained

	for {
		select {
		case <-b.stopCh:
			return
		default:
		}

		if !b.config.allows(cluster) {
			// Leave the cluster's messages server-side while it is gated.
			b.config.observeIdle(cluster)
			select {
			case <-b.stopCh:
				return
			case <-b.after(b.config.PollInterval):
				continue
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		result := b.dequeueWithPriority(ctx, cluster, highProcessed, ratio)
		if now := time.Now(); !now.Before(nextDepthAt) {
			b.reportDepth(ctx, cluster)
			nextDepthAt = now.Add(depthRefreshInterval)
		}
		cancel()

		highProcessed = result.highProcessed

		// A batch fetch that is interrupted mid-stream (consumer deleted,
		// ctx cancel, transport error) can still carry successfully fetched
		// messages alongside the error. Process those first so they are
		// Ack'd/Nak'd/Term'd before the error branch below backs off -
		// otherwise each occurrence would burn a JetStream delivery attempt
		// without execute() ever running on the fetched messages.
		if len(result.msgs) > 0 {
			b.processMessages(result.msgs, retained)
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
			case <-b.after(b.config.PollInterval):
				continue
			}
		}

		if len(result.msgs) == 0 {
			b.config.observeIdle(cluster)
			// No messages, wait before polling again
			select {
			case <-b.stopCh:
				return
			case <-b.after(b.config.PollInterval):
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
//
// Under RetryWhileRetained every message is marked in progress before it
// is executed, so a slow batch is not redelivered while it is still being
// worked through, and failures are settled by settleRetained.
func (b *natsBackend) processMessages(msgs []ReplayMessage, retained bool) {
	for i, msg := range msgs {
		select {
		case <-b.stopCh:
			b.nakTail(msgs[i:])

			return
		default:
		}
		if !b.holdWhileGated(msgs[i:]) {
			b.nakTail(msgs[i:])

			return
		}

		if retained {
			if progressErr := msg.InProgress(); progressErr != nil {
				b.config.Logger.Debug("failed to mark replay message in progress",
					"cluster", b.clusterName(msg.Payload.TargetCluster),
					"error", progressErr.Error(),
				)
			}
		}
		b.config.observeAge(msg.Payload)

		start := time.Now()
		err := b.executeOnce(msg.Payload)
		elapsed := time.Since(start).Seconds()

		if err != nil {
			b.config.Metrics.IncReplayError(msg.Payload.TargetCluster)
			b.config.Metrics.ObserveReplayDuration(msg.Payload.TargetCluster, elapsed)

			if retained {
				b.settleRetained(msg, err)

				continue
			}

			// Compare using uint64 to avoid integer overflow warning
			// MaxDeliver is always a small positive int (typically 1-10), so this is safe
			isLastAttempt := msg.MaxDeliver > 0 && msg.DeliveryCount >= uint64(msg.MaxDeliver)

			if isLastAttempt {
				// This is the last attempt: the server will not deliver
				// the message again whether or not it accepts the Term, so
				// the drop is recorded either way.
				b.termMessage(msg, "max retries exceeded")
				b.recordDrop(msg, err, types.ReplayDropMaxAttempts)
			} else {
				// Nak for redelivery
				b.nakMessage(msg, "retry", 0)
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
			if retained {
				b.forgetDeadLetters(msg.StreamSequence)
			}
			b.config.Metrics.IncReplaySuccess(msg.Payload.TargetCluster)
			b.config.Metrics.ObserveReplayDuration(msg.Payload.TargetCluster, elapsed)
			if b.config.OnSuccess != nil {
				b.config.OnSuccess(msg.Payload)
			}
		}
	}
}

// after is time.After, or the test seam installed in backoffWait.
func (b *natsBackend) after(d time.Duration) <-chan time.Time {
	if b.backoffWait != nil {
		return b.backoffWait(d)
	}

	return time.After(d)
}

// nakTail NAKs every message in tail; see processMessages for why a
// shutdown NAKs the unprocessed rest of a batch.
func (b *natsBackend) nakTail(tail []ReplayMessage) {
	for _, msg := range tail {
		b.nakMessage(msg, "shutdown", 0)
	}
}

// holdWhileGated keeps a fetched batch alive while the gate refuses its
// cluster: every unprocessed message is marked in progress at a fraction
// of the consumer's AckWait so nothing is redelivered or charged a delivery
// while parked. It returns true once the gate permits execution and false
// when the worker stops first.
func (b *natsBackend) holdWhileGated(tail []ReplayMessage) bool {
	cluster := tail[0].Payload.TargetCluster
	if b.config.allows(cluster) {
		return true
	}
	// The acknowledgement timers are refreshed once a third of AckWait
	// has passed, so a long quarantine does not turn into a stream of
	// InProgress calls, and the gate is polled at least that often so a
	// PollInterval longer than AckWait cannot let a lease expire.
	pollEvery := b.config.PollInterval
	refreshEvery := pollEvery
	if ackWait := b.replayer.config.AckWait; ackWait > 0 {
		refreshEvery = ackWait / 3
		pollEvery = min(pollEvery, refreshEvery)
	}
	var lastRefresh time.Time
	for {
		if time.Since(lastRefresh) >= refreshEvery {
			lastRefresh = time.Now()
			for _, msg := range tail {
				if err := msg.InProgress(); err != nil {
					b.config.Logger.Debug("failed to mark gated replay message in progress",
						"cluster", b.clusterName(cluster),
						"error", err.Error(),
					)
				}
			}
		}
		select {
		case <-b.stopCh:
			return false
		case <-b.after(pollEvery):
		}
		if b.config.allows(cluster) {
			return true
		}
	}
}

// settleRetained handles a failed attempt under RetryWhileRetained: a
// dead-letter disposition consumes the poison budget (MaxAttempts) and
// terminates the message once it is exhausted; every other disposition
// asks the server to redeliver after the backoff delay, for as long as the
// stream retains the message.
func (b *natsBackend) settleRetained(msg ReplayMessage, err error) {
	disposition := b.config.Classifier(err)
	attempt := int(min(msg.DeliveryCount, 1<<30)) //nolint:gosec // clamped before conversion

	if disposition == DispositionDeadLetter {
		count := b.countDeadLetter(msg.StreamSequence)
		if count >= b.config.MaxAttempts {
			if b.termMessage(msg, "dead-lettered") {
				b.recordDrop(msg, err, types.ReplayDropDeadLetter)
			}
			b.forgetDeadLetters(msg.StreamSequence)

			return
		}
		attempt = count
	}

	delay := calculateBackoff(attempt, b.config.RetryDelay, b.config.MaxRetryDelay)
	b.nakMessage(msg, "retry", delay)
	b.config.Logger.Warn("replay execution failed, will retry",
		"cluster", b.clusterName(msg.Payload.TargetCluster),
		"attempt", msg.DeliveryCount,
		"disposition", disposition.String(),
		"delay", delay.String(),
		"error", err.Error(),
	)
	if b.config.OnError != nil {
		b.config.OnError(msg.Payload, err, attempt)
	}
}

// countDeadLetter increments and returns the dead-letter count for a
// stream sequence.
func (b *natsBackend) countDeadLetter(seq uint64) int {
	b.dlMu.Lock()
	defer b.dlMu.Unlock()

	b.deadLetters[seq]++

	return b.deadLetters[seq]
}

// forgetDeadLetters clears the dead-letter count once a message leaves the
// stream.
func (b *natsBackend) forgetDeadLetters(seq uint64) {
	b.dlMu.Lock()
	delete(b.deadLetters, seq)
	b.dlMu.Unlock()
}

// recordDrop reports a permanently dropped message; the reason is one of
// the types.ReplayDrop constants.
func (b *natsBackend) recordDrop(msg ReplayMessage, err error, reason string) {
	b.config.observeDrop(msg.Payload, err, reason,
		"attempt", msg.DeliveryCount,
		"maxDeliver", msg.MaxDeliver,
	)
}

// reportDepth publishes the outstanding message count for a cluster as the
// queue depth gauge.
func (b *natsBackend) reportDepth(ctx context.Context, cluster types.ClusterID) {
	depth, err := b.replayer.PendingByCluster(ctx, cluster)
	if err != nil {
		b.config.Logger.Debug("failed to read replay backlog depth",
			"cluster", b.clusterName(cluster),
			"error", err.Error(),
		)

		return
	}
	b.config.Metrics.SetReplayQueueDepth(cluster, depth)
}

// redeliverySchedule builds the server-side redelivery delays for
// unacknowledged messages under RetryWhileRetained: it starts at ackWait,
// which the server also adopts as the consumer's AckWait, and doubles up to
// the larger of ackWait and maxDelay.
func redeliverySchedule(ackWait, maxDelay time.Duration) []time.Duration {
	if ackWait <= 0 {
		ackWait = DefaultNATSReplayerConfig().AckWait
	}
	limit := max(maxDelay, ackWait)
	schedule := make([]time.Duration, 0, maxRedeliverySteps)
	for d := ackWait; len(schedule) < maxRedeliverySteps && d < limit; d *= 2 {
		schedule = append(schedule, d)
	}
	if len(schedule) < maxRedeliverySteps {
		schedule = append(schedule, limit)
	}

	return schedule
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

// nakMessage requests redelivery, after delay when it is positive.
func (b *natsBackend) nakMessage(msg ReplayMessage, reason string, delay time.Duration) {
	// A message accepts one acknowledgement: a plain Nak followed by a
	// delayed one would redeliver immediately and fail the second call.
	var err error
	if delay > 0 {
		err = msg.NakWithDelay(delay)
	} else {
		err = msg.Nak()
	}
	if err != nil {
		b.config.Logger.Error("failed to nak replay message",
			"cluster", b.clusterName(msg.Payload.TargetCluster),
			"reason", reason,
			"attempt", msg.DeliveryCount,
			"error", err.Error(),
		)
	}
}

// observeCorrupt records a message the replayer terminated at decode; the
// worker owns the metrics and the logger, so it reports on the replayer's
// behalf.
func (b *natsBackend) observeCorrupt(cluster types.ClusterID, streamSeq uint64, decodeErr, termErr error) {
	stream := b.config.streamMetrics()
	stream.IncReplayCorrupt(cluster)
	attrs := []any{
		"cluster", b.clusterName(cluster),
		"streamSequence", streamSeq,
		"error", decodeErr.Error(),
	}
	if termErr != nil {
		stream.IncReplayTermFailed(cluster)
		attrs = append(attrs, "termError", termErr.Error())
	}
	b.config.Logger.Error("corrupt replay message terminated", attrs...)
}

// termMessage terminates a message the worker gave up on and reports
// whether the server accepted it. A refused Term is counted, because the
// message may be delivered again.
func (b *natsBackend) termMessage(msg ReplayMessage, reason string) bool {
	if err := msg.Term(); err != nil {
		b.config.streamMetrics().IncReplayTermFailed(msg.Payload.TargetCluster)
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
	backend := &natsBackend{
		replayer:    replayer,
		config:      &w.config,
		execute:     execute,
		stopCh:      w.stopCh,
		wg:          &w.wg,
		deadLetters: make(map[uint64]int),
	}

	w.backend = backend
	if replayer != nil {
		replayer.setCorruptObserver(backend.observeCorrupt)
		if config.RetryPolicy == RetryWhileRetained {
			replayer.enableRetainedDelivery(
				redeliverySchedule(replayer.config.AckWait, config.MaxRetryDelay),
			)
		}
	}

	return w
}
