// Package replay provides replay queue implementations for failed write reconciliation.
package replay

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/arloliu/helix/types"
)

const (
	defaultMemoryQueueCapacity    = 10000
	defaultMemoryHighPriorityRate = 10
)

// MemoryReplayer implements an in-memory replay queue using priority-based buffered channels.
//
// # Priority Support
//
// The replayer uses separate channels for high and low priority messages,
// sharing a total enforced capacity. Each priority channel is sized to the
// full configured capacity so an all-high or all-low workload can use the
// entire budget; a shared atomic counter caps the combined pending across
// both queues. High-priority messages are preferred during dequeue operations,
// with configurable ratio-based fair scheduling to prevent low-priority
// starvation.
//
// # Memory Footprint
//
// Because both priority channels are sized to the full capacity, the replayer
// reserves channel buffer slots for ~2× capacity ReplayPayload structs (the
// enforced pending limit is still 1× capacity). Size WithQueueCapacity with
// that overhead in mind for high-capacity deployments.
//
// # Durability Warning
//
// Enqueued replays are LOST on process restart or client.Close().
// Use MemoryReplayer for:
//   - Development and testing
//   - Scenarios where replay loss is acceptable
//
// For production durability, use NATSReplayer with JetStream persistence.
//
// # Thread Safety
//
// All methods are safe for concurrent use. The Close method marks the replayer
// as closed but does not close the underlying channel, preventing panics from
// concurrent Enqueue calls during shutdown.
type MemoryReplayer struct {
	highQueue chan types.ReplayPayload
	lowQueue  chan types.ReplayPayload
	closed    atomic.Bool
	capacity  int
	pending   atomic.Int64
	pendingA  atomic.Int64 // slots held by payloads targeting cluster A
	pendingB  atomic.Int64 // slots held by payloads targeting cluster B

	// For priority-aware dequeue tracking
	mu                sync.Mutex
	highProcessed     int // Count of high-priority items processed since last low
	highPriorityRatio int // Process N high before 1 low (0 = equal priority)
	strictPriority    bool
}

// MemoryReplayerOption configures a MemoryReplayer.
type MemoryReplayerOption func(*MemoryReplayer)

// WithQueueCapacity sets the maximum number of pending replays across both
// priority queues combined.
//
// Values <= 0 are clamped to 1; there is no way to disable the queue via
// this option (callers who do not want a replayer should not configure one).
//
// Parameters:
//   - n: Total queue capacity (default: 10000)
//
// Returns:
//   - MemoryReplayerOption: Configuration option
func WithQueueCapacity(n int) MemoryReplayerOption {
	return func(m *MemoryReplayer) {
		m.capacity = n
	}
}

// WithMemoryHighPriorityRatio sets the ratio of high-priority to low-priority processing.
//
// For every N high-priority items dequeued, 1 low-priority item is dequeued.
// This prevents low-priority starvation while ensuring high-priority messages are preferred.
// Set to 0 for equal priority processing (1:1 ratio).
// Default: 10 (10:1 ratio)
//
// Parameters:
//   - n: Number of high-priority items to process before 1 low-priority item
//
// Returns:
//   - MemoryReplayerOption: Configuration option
func WithMemoryHighPriorityRatio(n int) MemoryReplayerOption {
	return func(m *MemoryReplayer) {
		m.highPriorityRatio = n
	}
}

// WithMemoryStrictPriority enables strict priority mode for memory replayer.
//
// When enabled, all high-priority messages are drained before processing any
// low-priority messages. This provides absolute priority but may cause low-priority
// starvation under continuous high-priority load.
//
// Parameters:
//   - strict: true to enable strict priority mode
//
// Returns:
//   - MemoryReplayerOption: Configuration option
func WithMemoryStrictPriority(strict bool) MemoryReplayerOption {
	return func(m *MemoryReplayer) {
		m.strictPriority = strict
	}
}

// NewMemoryReplayer creates a new in-memory replayer.
//
// The replayer uses separate buffered channels for high and low priority messages,
// while a shared capacity counter enforces the total pending-item limit across
// both queues. Default capacity is 10,000 items.
//
// Parameters:
//   - opts: Optional configuration options
//
// Returns:
//   - *MemoryReplayer: A new memory replayer
//
// For production configuration that should fail fast on invalid option values,
// use [NewMemoryReplayerChecked].
func NewMemoryReplayer(opts ...MemoryReplayerOption) *MemoryReplayer {
	m := newMemoryReplayerWithDefaults()
	applyMemoryReplayerOptions(m, opts...)
	normalizeMemoryReplayerForLegacy(m)
	initializeMemoryReplayerQueues(m)

	return m
}

// NewMemoryReplayerChecked creates a new in-memory replayer and returns a
// validation error when any option value is invalid.
//
// Parameters:
//   - opts: Optional configuration options
//
// Returns:
//   - *MemoryReplayer: A new memory replayer
//   - error: Joined [types.OptionError] values when one or more options are invalid
func NewMemoryReplayerChecked(opts ...MemoryReplayerOption) (*MemoryReplayer, error) {
	m := newMemoryReplayerWithDefaults()
	applyMemoryReplayerOptions(m, opts...)
	if err := validateMemoryReplayerForChecked(m); err != nil {
		return nil, err
	}
	initializeMemoryReplayerQueues(m)

	return m, nil
}

func newMemoryReplayerWithDefaults() *MemoryReplayer {
	return &MemoryReplayer{
		capacity:          defaultMemoryQueueCapacity,
		highPriorityRatio: defaultMemoryHighPriorityRate,
		strictPriority:    false,
	}
}

func applyMemoryReplayerOptions(m *MemoryReplayer, opts ...MemoryReplayerOption) {
	for _, opt := range opts {
		opt(m)
	}
}

func validateMemoryReplayerForChecked(m *MemoryReplayer) error {
	errList := make([]error, 0, 2)

	if m.capacity <= 0 {
		errList = append(errList, optionErrPositiveInt(memoryReplayerComponent, "WithQueueCapacity"))
	}
	if m.highPriorityRatio < 0 {
		errList = append(errList, optionErrNonNegativeInt(memoryReplayerComponent, "WithMemoryHighPriorityRatio"))
	}

	return joinValidationErrors(errList...)
}

func normalizeMemoryReplayerForLegacy(m *MemoryReplayer) {
	if m.capacity < 1 {
		m.capacity = 1
	}
}

func initializeMemoryReplayerQueues(m *MemoryReplayer) {
	m.highQueue = make(chan types.ReplayPayload, m.capacity)
	m.lowQueue = make(chan types.ReplayPayload, m.capacity)
}

// Enqueue adds a failed write to the replay queue.
//
// Messages are routed to the appropriate priority queue based on payload.Priority.
// If the target queue is full, returns ErrReplayQueueFull immediately (non-blocking).
// If the replayer is closed, returns ErrSessionClosed.
//
// Parameters:
//   - ctx: Context for cancellation
//   - payload: The write operation to replay
//
// Returns:
//   - error: nil on success;
//     [types.ErrSessionClosed] if Close has been called;
//     ctx.Err() if ctx is already cancelled or is cancelled while waiting;
//     [types.ErrReplayQueueFull] if the combined pending count is at capacity.
func (m *MemoryReplayer) Enqueue(ctx context.Context, payload types.ReplayPayload) error {
	if m.highQueue == nil || m.lowQueue == nil {
		return errors.New("helix: memory replayer not initialized, use NewMemoryReplayer")
	}
	if m.closed.Load() {
		return types.ErrSessionClosed
	}
	if err := validatePayloadArgs(payload); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if !m.tryReserveSlot(payload.TargetCluster) {
		return types.ErrReplayQueueFull
	}

	// Route to appropriate queue based on priority
	var targetQueue chan types.ReplayPayload
	if payload.Priority == types.PriorityHigh {
		targetQueue = m.highQueue
	} else {
		targetQueue = m.lowQueue
	}

	select {
	case <-ctx.Done():
		m.releaseSlot(payload.TargetCluster)
		return ctx.Err()
	case targetQueue <- payload:
		return nil
	default:
		m.releaseSlot(payload.TargetCluster)
		return types.ErrReplayQueueFull
	}
}

// Dequeue retrieves the next replay payload from the queue with priority awareness.
//
// Uses the configured priority ratio to balance high and low priority processing.
// High-priority messages are preferred, but low-priority messages are guaranteed
// to be processed based on the ratio (e.g., 10:1 means 1 low per 10 high).
//
// Blocks until a payload is available or the context is cancelled.
// Returns false if the context is cancelled or the replayer is closed and empty.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - types.ReplayPayload: The next payload to replay
//   - bool: true if a payload was retrieved, false if cancelled/closed
func (m *MemoryReplayer) Dequeue(ctx context.Context) (types.ReplayPayload, bool) {
	if m.highQueue == nil || m.lowQueue == nil {
		return types.ReplayPayload{}, false
	}
	for {
		// Check for stop condition first
		select {
		case <-ctx.Done():
			return types.ReplayPayload{}, false
		default:
		}

		// Determine which queue to try based on priority mode
		payload, ok := m.tryDequeueWithPriority()
		if ok {
			m.releaseSlot(payload.TargetCluster)
			return payload, true
		}

		// No messages available; if closed and drained, signal completion.
		if m.closed.Load() && m.Len() == 0 {
			return types.ReplayPayload{}, false
		}

		// Wait for any message or context cancellation
		select {
		case <-ctx.Done():
			return types.ReplayPayload{}, false
		case payload := <-m.highQueue:
			m.mu.Lock()
			m.highProcessed++
			m.mu.Unlock()
			m.releaseSlot(payload.TargetCluster)
			return payload, true
		case payload := <-m.lowQueue:
			m.mu.Lock()
			m.highProcessed = 0 // Reset counter after processing low
			m.mu.Unlock()
			m.releaseSlot(payload.TargetCluster)
			return payload, true
		}
	}
}

func (m *MemoryReplayer) tryReserveSlot(cluster types.ClusterID) bool {
	for {
		current := m.pending.Load()
		if current >= int64(m.capacity) {
			return false
		}
		if m.pending.CompareAndSwap(current, current+1) {
			m.clusterPending(cluster).Add(1)
			return true
		}
	}
}

// releaseSlot decrements the pending counter by one. It is paired with
// tryReserveSlot for capacity accounting. A defensive CAS loop refuses to
// drive the counter below zero so a regression that releases more than it
// reserved cannot inflate effective capacity on subsequent reservations.
func (m *MemoryReplayer) releaseSlot(cluster types.ClusterID) {
	for {
		current := m.pending.Load()
		if current <= 0 {
			return
		}
		if m.pending.CompareAndSwap(current, current-1) {
			m.clusterPending(cluster).Add(-1)
			return
		}
	}
}

// clusterPending returns the per-cluster slot counter.
// Enqueue has already rejected any target other than A or B.
func (m *MemoryReplayer) clusterPending(cluster types.ClusterID) *atomic.Int64 {
	if cluster == types.ClusterA {
		return &m.pendingA
	}

	return &m.pendingB
}

// tryDequeueRetained removes the next payload without releasing its
// capacity slot.
// The worker that took it must call releaseSlot once the payload has been
// replayed or dropped, so capacity keeps counting executing and waiting
// payloads.
func (m *MemoryReplayer) tryDequeueRetained() (types.ReplayPayload, bool) {
	if m.highQueue == nil || m.lowQueue == nil {
		return types.ReplayPayload{}, false
	}
	if m.closed.Load() && m.Len() == 0 {
		return types.ReplayPayload{}, false
	}

	return m.tryDequeueWithPriority()
}

// tryDequeueWithPriority attempts to dequeue based on priority settings.
// Returns immediately without blocking.
func (m *MemoryReplayer) tryDequeueWithPriority() (types.ReplayPayload, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Strict priority mode: drain high completely before low
	if m.strictPriority {
		select {
		case payload := <-m.highQueue:
			return payload, true
		default:
			// High queue empty, try low
			select {
			case payload := <-m.lowQueue:
				return payload, true
			default:
				return types.ReplayPayload{}, false
			}
		}
	}

	// Ratio-based fair scheduling
	ratio := m.highPriorityRatio
	if ratio <= 0 {
		ratio = 1 // 1:1 equal priority
	}

	// Check if we should process low priority (based on ratio)
	shouldProcessLow := m.highProcessed >= ratio

	if shouldProcessLow {
		// Try low priority first
		select {
		case payload := <-m.lowQueue:
			m.highProcessed = 0 // Reset counter
			return payload, true
		default:
			// Low queue empty, fall through to try high
		}
	}

	// Try high priority
	select {
	case payload := <-m.highQueue:
		m.highProcessed++
		return payload, true
	default:
		// High queue empty, try low (don't starve if high is empty)
		select {
		case payload := <-m.lowQueue:
			m.highProcessed = 0
			return payload, true
		default:
			return types.ReplayPayload{}, false
		}
	}
}

// TryDequeue attempts to retrieve a payload without blocking.
//
// Uses priority-aware selection based on the configured ratio.
//
// Returns:
//   - types.ReplayPayload: The payload if available
//   - bool: true if a payload was retrieved, false if queue is empty or closed
func (m *MemoryReplayer) TryDequeue() (types.ReplayPayload, bool) {
	payload, ok := m.tryDequeueRetained()
	if ok {
		m.releaseSlot(payload.TargetCluster)
	}

	return payload, ok
}

// Len returns the number of capacity slots currently held.
//
// Under the worker's default [RetryBounded] policy a slot is released as
// soon as a worker dequeues the payload, so Len reports queue depth only:
// payloads being attempted or sleeping between attempts are NOT counted,
// and a worker pulling a payload, failing, and retrying with backoff
// produces a transient `Len() == 0` window even though work remains.
// Do not use Len()==0 as a "replay drained" signal in tests or operational
// checks; instead observe an authoritative downstream signal (row counts on
// the destination, success-counter convergence, or worker-callback
// completion). Misuse of this contract was the root cause of the S1 e2e
// flake fixed in `test/e2e/cql/write_replay_test.go` — see the comment
// there.
//
// Under [RetryWhileRetained] the worker holds the slot until the payload is
// replayed or dropped, so Len counts queued, executing, and waiting payloads
// together and reaches 0 only when the backlog has converged.
//
// Returns:
//   - int: Number of slots held across both priority queues
func (m *MemoryReplayer) Len() int {
	return int(m.pending.Load())
}

// PendingByCluster returns the number of capacity slots held by payloads
// targeting one cluster.
// It follows the same slot lifecycle as [MemoryReplayer.Len].
//
// Parameters:
//   - cluster: The target cluster to count
//
// Returns:
//   - int: Slots held by payloads for that cluster
func (m *MemoryReplayer) PendingByCluster(cluster types.ClusterID) int {
	return int(m.clusterPending(cluster).Load())
}

// HighLen returns the current number of high-priority pending replays.
//
// Returns:
//   - int: Number of items in the high-priority queue
func (m *MemoryReplayer) HighLen() int {
	return len(m.highQueue)
}

// LowLen returns the current number of low-priority pending replays.
//
// Returns:
//   - int: Number of items in the low-priority queue
func (m *MemoryReplayer) LowLen() int {
	return len(m.lowQueue)
}

// Cap returns the total enforced queue capacity across both priority queues.
//
// Returns:
//   - int: Maximum combined pending across high and low priority queues
func (m *MemoryReplayer) Cap() int {
	return m.capacity
}

// Close marks the replay queue as closed.
//
// After Close is called, Enqueue will return ErrSessionClosed.
// The underlying channel is NOT closed to prevent panics from concurrent
// Enqueue calls. Use DrainAll to retrieve remaining items after Close.
//
// Close is safe to call multiple times.
func (m *MemoryReplayer) Close() {
	m.closed.Store(true)
}

// IsClosed returns whether the replayer has been closed.
//
// Returns:
//   - bool: true if Close has been called
func (m *MemoryReplayer) IsClosed() bool {
	return m.closed.Load()
}

// DrainAll returns all pending replays and empties both queues.
//
// This is useful for graceful shutdown scenarios where you want
// to persist pending replays before exiting. High-priority messages
// are returned first, followed by low-priority messages.
//
// Returns:
//   - []types.ReplayPayload: All pending replay payloads
func (m *MemoryReplayer) DrainAll() []types.ReplayPayload {
	var payloads []types.ReplayPayload
	if m.highQueue == nil || m.lowQueue == nil {
		return payloads
	}

	// Drain high priority first
	for {
		select {
		case payload := <-m.highQueue:
			m.releaseSlot(payload.TargetCluster)
			payloads = append(payloads, payload)
		default:
			goto drainLow
		}
	}

drainLow:
	// Then drain low priority
	for {
		select {
		case payload := <-m.lowQueue:
			m.releaseSlot(payload.TargetCluster)
			payloads = append(payloads, payload)
		default:
			return payloads
		}
	}
}
