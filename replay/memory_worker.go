package replay

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/types"
)

// defaultMemoryRetryConcurrency caps the number of in-flight retry
// goroutines a single memoryBackend will spawn. Each retry goroutine is
// idle most of its life (waiting on backoff timers), so the cost is
// dominated by stack pages, not CPU. 100 absorbs typical bursts without
// risking unbounded fan-out under sustained failure rates; once the cap
// is reached, further failures drop immediately rather than queue up.
const defaultMemoryRetryConcurrency = 100

// memoryBackend implements workerBackend for MemoryReplayer.
//
// Retries run in dedicated goroutines, not on the main dequeue loop, so a
// permanently-failing payload does not stall unrelated backlog. The
// retrySem semaphore caps concurrent in-flight retries so a sustained
// failure storm cannot blow up the goroutine count; once full, further
// failures drop immediately.
type memoryBackend struct {
	replayer *MemoryReplayer
	config   *WorkerConfig
	execute  ExecuteFunc
	stopCh   <-chan struct{}
	wg       *sync.WaitGroup

	retrySem chan struct{} // bounds concurrent retry goroutines
	retryWG  sync.WaitGroup
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
//
// The first execution attempt for each payload runs inline on the dequeue
// loop. On failure, retry attempts are dispatched to a bounded-concurrency
// goroutine pool so a permanently-failing payload cannot block dequeues
// for unrelated payloads — including those targeting a different cluster.
//
// The cluster parameter is ignored since memory backend uses a single
// dequeue worker.
func (b *memoryBackend) start(_ types.ClusterID) {
	defer b.wg.Done()
	// Order of teardown (innermost first):
	//   1. drainAndDrop — flush any payloads still in the queue.
	//   2. retryWG.Wait — wait for in-flight retry goroutines to finish.
	//      Each one observes stopCh during its backoff sleep and exits
	//      via the drop path with its accumulated error.
	defer b.retryWG.Wait()
	defer b.drainAndDrop()

	for {
		select {
		case <-b.stopCh:
			return
		default:
		}

		payload, ok := b.replayer.TryDequeue()
		if !ok {
			select {
			case <-b.stopCh:
				return
			case <-time.After(b.config.PollInterval):
				continue
			}
		}

		b.handleFirstAttempt(payload)
	}
}

// handleFirstAttempt runs the initial execution synchronously on the
// dequeue loop. On success or terminal drop, it returns immediately. On
// recoverable failure, it dispatches further attempts to a retry
// goroutine and returns control to the dequeue loop so the next payload
// can be picked up without waiting for the failing payload's backoff.
func (b *memoryBackend) handleFirstAttempt(payload types.ReplayPayload) {
	maxAttempts := b.config.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	err := b.runAttempt(payload, 1, maxAttempts)
	if err == nil {
		return
	}

	// No retries configured — drop now.
	if maxAttempts == 1 {
		b.dropPayload(payload, err, maxAttempts, "max attempts exceeded")
		return
	}

	// Try to acquire a retry slot. If the pool is saturated, drop now
	// rather than queue the payload behind in-flight retries; this
	// keeps the dequeue loop responsive and gives the caller (via
	// OnDrop + IncReplayDropped) immediate visibility into the
	// saturation event.
	select {
	case b.retrySem <- struct{}{}:
	default:
		b.dropPayload(payload, err, maxAttempts, "retry pool saturated")
		return
	}

	b.retryWG.Add(1)
	go b.retryAsync(payload, err, maxAttempts)
}

// retryAsync runs attempts 2..maxAttempts in a dedicated goroutine,
// sleeping the appropriate exponential backoff between each. Returns
// (and drops via OnDrop) when:
//   - an attempt succeeds (no drop);
//   - all attempts are exhausted (drop with the final error);
//   - stopCh fires during a backoff sleep (drop with the most recent error).
func (b *memoryBackend) retryAsync(payload types.ReplayPayload, prevErr error, maxAttempts int) {
	defer b.retryWG.Done()
	defer func() { <-b.retrySem }()

	for attempt := 2; attempt <= maxAttempts; attempt++ {
		// Backoff between attempts uses (attempt-1) so attempt 2's wait
		// is calculateBackoff(1) = RetryDelay, attempt 3's wait is
		// 2*RetryDelay, etc.
		delay := calculateBackoff(attempt-1, b.config.RetryDelay, b.config.MaxRetryDelay)
		timer := time.NewTimer(delay)
		select {
		case <-b.stopCh:
			timer.Stop()
			b.dropPayload(payload, prevErr, maxAttempts, "shutdown")
			return
		case <-timer.C:
		}

		err := b.runAttempt(payload, attempt, maxAttempts)
		if err == nil {
			return
		}
		prevErr = err
	}

	b.dropPayload(payload, prevErr, maxAttempts, "max attempts exceeded")
}

// runAttempt performs a single execution attempt and emits all the
// per-attempt observability (metrics, log, OnError/OnSuccess). Returns
// nil on success and the error otherwise. Used by both the inline first
// attempt and the background retry path so the two paths emit identical
// observability.
func (b *memoryBackend) runAttempt(payload types.ReplayPayload, attempt, maxAttempts int) error {
	start := time.Now()
	err := b.executeOnce(payload)
	elapsed := time.Since(start).Seconds()

	if err == nil {
		b.config.Metrics.IncReplaySuccess(payload.TargetCluster)
		b.config.Metrics.ObserveReplayDuration(payload.TargetCluster, elapsed)
		if b.config.OnSuccess != nil {
			b.config.OnSuccess(payload)
		}
		return nil
	}

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

	return err
}

// dropPayload records a single drop event with metrics, log, and the
// OnDrop callback. The reason string is attached to the log so an
// operator can distinguish exhaustion, shutdown, and saturation drops.
func (b *memoryBackend) dropPayload(payload types.ReplayPayload, err error, maxAttempts int, reason string) {
	b.config.Metrics.IncReplayDropped(payload.TargetCluster)
	b.config.Logger.Error("replay message dropped",
		"cluster", b.clusterName(payload.TargetCluster),
		"reason", reason,
		"maxAttempts", maxAttempts,
		"error", errString(err),
	)
	if b.config.OnDrop != nil {
		b.config.OnDrop(payload, err)
	}
}

// errString returns the error message or "<nil>" so the log emits a
// sensible value when drainAndDrop drops a payload that never executed.
func errString(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

// drainAndDrop dequeues all remaining items from the queue and calls
// OnDrop for each. Invoked when the worker exits to ensure no payload
// is silently lost. Items already dispatched to a retry goroutine are
// not in the queue and are handled by retryAsync's stopCh path.
func (b *memoryBackend) drainAndDrop() {
	for {
		payload, ok := b.replayer.TryDequeue()
		if !ok {
			return
		}
		b.dropPayload(payload, nil, b.config.MaxAttempts, "shutdown")
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
// # Retry model
//
// The first attempt for each payload runs synchronously on the dequeue
// loop. On failure, attempts 2..MaxAttempts run in a dedicated goroutine
// so the dequeue loop is never blocked behind a permanently-failing
// payload — including payloads targeting a different cluster.
//
// Concurrent in-flight retries are capped (default 100). When the cap is
// reached, further failures drop immediately via OnDrop with the reason
// "retry pool saturated" rather than queuing behind running retries.
//
// # Shutdown semantics
//
// On Stop, every pending payload still in the queue is dequeued and the
// configured OnDrop callback is invoked for each, with reason "shutdown".
// Any in-flight retry goroutines observe stopCh during their backoff
// sleep and also exit via OnDrop. High-throughput systems can therefore
// see a sudden burst of OnDrop callbacks at shutdown proportional to
// the queue depth + in-flight retry count. Size your OnDrop handler
// (and any synchronous fallback persistence) accordingly.
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
	normalizeWorkerConfig(&config)

	if config.Metrics == nil {
		config.Metrics = metrics.NewNopMetrics()
	}
	if config.Logger == nil {
		config.Logger = logging.NewNopLogger()
	}

	w := &Worker{
		config:     config,
		execute:    execute,
		stopCh:     make(chan struct{}),
		startupErr: validateWorkerInputs(replayer != nil, execute),
	}

	w.backend = &memoryBackend{
		replayer: replayer,
		config:   &w.config,
		execute:  execute,
		stopCh:   w.stopCh,
		wg:       &w.wg,
		retrySem: make(chan struct{}, defaultMemoryRetryConcurrency),
	}

	return w
}

func validateWorkerInputs(hasReplayer bool, execute ExecuteFunc) error {
	if !hasReplayer {
		return errors.New("helix: replay worker requires a non-nil replayer")
	}
	if execute == nil {
		return errors.New("helix: replay worker requires a non-nil execute function")
	}

	return nil
}

func normalizeWorkerConfig(config *WorkerConfig) {
	defaults := DefaultWorkerConfig()
	if config.BatchSize <= 0 {
		config.BatchSize = defaults.BatchSize
	}
	if config.PollInterval <= 0 {
		config.PollInterval = defaults.PollInterval
	}
	if config.RetryDelay <= 0 {
		config.RetryDelay = defaults.RetryDelay
	}
	if config.MaxRetryDelay <= 0 {
		config.MaxRetryDelay = defaults.MaxRetryDelay
	}
	if config.MaxRetryDelay < config.RetryDelay {
		config.MaxRetryDelay = config.RetryDelay
	}
	if config.ExecuteTimeout <= 0 {
		config.ExecuteTimeout = defaults.ExecuteTimeout
	}
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = 1
	}
}
