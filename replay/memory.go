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

// MemoryReplayer implements an in-memory replay queue using per-cluster,
// priority-based buffered channels.
//
// # Per-Cluster Queues
//
// Each target cluster owns its own pair of priority channels, and dequeue
// alternates between the clusters that have payloads waiting.
// A long backlog for one cluster therefore never delays the other
// cluster's payloads, and each cluster's queue stays first-in first-out
// within a priority.
//
// # Priority Support
//
// Within a cluster, high and low priority messages use separate channels.
// High-priority messages are preferred during dequeue operations,
// with configurable ratio-based fair scheduling to prevent low-priority
// starvation.
//
// # Capacity
//
// All four channels share one enforced capacity: each channel is sized to
// the full configured capacity so a workload that is all one cluster and
// one priority can use the entire budget, and a shared atomic counter caps
// the combined pending count across every queue.
//
// # Memory Footprint
//
// Because every channel is sized to the full capacity, the replayer
// reserves channel buffer slots for ~4× capacity ReplayPayload structs (the
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
	queues   [2]clusterQueues // indexed by clusterIndex
	closed   atomic.Bool
	capacity int
	pending  atomic.Int64 // slots held across both clusters

	// Dequeue scheduling state: cluster rotation and priority ratio.
	mu                sync.Mutex
	nextQueue         int // Index of the cluster to try first on the next dequeue; guarded by mu
	highPriorityRatio int // Process N high before 1 low (0 = equal priority)
	strictPriority    bool
}

// clusterQueues holds one cluster's priority channels, its slot count, and
// its share of the ratio bookkeeping.
type clusterQueues struct {
	high          chan types.ReplayPayload
	low           chan types.ReplayPayload
	pending       atomic.Int64 // slots held by payloads targeting this cluster
	highProcessed int          // Count of high-priority items processed since last low; guarded by MemoryReplayer.mu
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
	for i := range m.queues {
		m.queues[i].high = make(chan types.ReplayPayload, m.capacity)
		m.queues[i].low = make(chan types.ReplayPayload, m.capacity)
	}
}

// clusterIndex maps a target cluster to its slot in MemoryReplayer.queues.
// Enqueue has already rejected any target other than A or B.
func clusterIndex(cluster types.ClusterID) int {
	if cluster == types.ClusterA {
		return 0
	}

	return 1
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
	if !m.initialized() {
		return errors.New("helix: memory replayer not initialized, use NewMemoryReplayer")
	}
	if m.closed.Load() {
		return types.ErrSessionClosed
	}
	if err := validatePayload(payload); err != nil {
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

	// Route to the target cluster's queue for the payload's priority.
	q := &m.queues[clusterIndex(payload.TargetCluster)]
	targetQueue := q.low
	if payload.Priority == types.PriorityHigh {
		targetQueue = q.high
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
	if !m.initialized() {
		return types.ReplayPayload{}, false
	}

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

	// Wait for any message or context cancellation.
	// Go's select picks a ready channel at random, so no cluster or
	// priority can starve the others while several have payloads; the
	// rotation and ratio bookkeeping resume from whichever was taken.
	var idx int
	var high bool
	select {
	case <-ctx.Done():
		return types.ReplayPayload{}, false
	case payload = <-m.queues[0].high:
		idx, high = 0, true
	case payload = <-m.queues[0].low:
		idx = 0
	case payload = <-m.queues[1].high:
		idx, high = 1, true
	case payload = <-m.queues[1].low:
		idx = 1
	}
	m.mu.Lock()
	m.noteDequeuedLocked(idx, high)
	m.mu.Unlock()
	m.releaseSlot(payload.TargetCluster)

	return payload, true
}

// initialized reports whether the queues were built by a constructor.
func (m *MemoryReplayer) initialized() bool {
	return m.queues[0].high != nil
}

// noteDequeuedLocked records that one payload left the queue at index idx:
// it advances that cluster's ratio counter and rotates to the other cluster.
// The caller holds m.mu.
func (m *MemoryReplayer) noteDequeuedLocked(idx int, high bool) {
	if high {
		m.queues[idx].highProcessed++
	} else {
		m.queues[idx].highProcessed = 0
	}
	m.nextQueue = 1 - idx
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
func (m *MemoryReplayer) clusterPending(cluster types.ClusterID) *atomic.Int64 {
	return &m.queues[clusterIndex(cluster)].pending
}

// tryDequeueRetained removes the next payload without releasing its
// capacity slot.
// The worker that took it must call releaseSlot once the payload has been
// replayed or dropped, so capacity keeps counting executing and waiting
// payloads.
func (m *MemoryReplayer) tryDequeueRetained() (types.ReplayPayload, bool) {
	if !m.initialized() {
		return types.ReplayPayload{}, false
	}
	if m.closed.Load() && m.Len() == 0 {
		return types.ReplayPayload{}, false
	}

	return m.tryDequeueWithPriority()
}

// tryDequeueWithPriority attempts to dequeue from the next cluster in the
// rotation, falling back to the other cluster when that one is empty.
// Returns immediately without blocking.
func (m *MemoryReplayer) tryDequeueWithPriority() (types.ReplayPayload, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range m.queues {
		idx := (m.nextQueue + i) % len(m.queues)
		payload, high, ok := m.tryDequeueCluster(&m.queues[idx])
		if ok {
			m.noteDequeuedLocked(idx, high)

			return payload, true
		}
	}

	return types.ReplayPayload{}, false
}

// tryDequeueCluster applies the priority settings to one cluster's queues
// and reports which priority the payload came from.
// The caller holds m.mu and records the outcome through noteDequeuedLocked.
func (m *MemoryReplayer) tryDequeueCluster(q *clusterQueues) (payload types.ReplayPayload, high, ok bool) {
	// Ratio-based scheduling lets low go first once enough high payloads
	// have been processed; strict priority never does.
	ratio := m.highPriorityRatio
	if ratio <= 0 {
		ratio = 1 // 1:1 equal priority
	}
	if !m.strictPriority && q.highProcessed >= ratio {
		select {
		case payload = <-q.low:
			return payload, false, true
		default:
			// Low queue empty, fall through to try high
		}
	}

	select {
	case payload = <-q.high:
		return payload, true, true
	default:
		// High queue empty, try low (don't starve if high is empty)
		select {
		case payload = <-q.low:
			return payload, false, true
		default:
			return types.ReplayPayload{}, false, false
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
// Under the worker's [RetryBounded] policy a slot is released as
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

// HighLen returns the current number of high-priority pending replays
// across both clusters.
//
// Returns:
//   - int: Number of items in the high-priority queues
func (m *MemoryReplayer) HighLen() int {
	return len(m.queues[0].high) + len(m.queues[1].high)
}

// LowLen returns the current number of low-priority pending replays
// across both clusters.
//
// Returns:
//   - int: Number of items in the low-priority queues
func (m *MemoryReplayer) LowLen() int {
	return len(m.queues[0].low) + len(m.queues[1].low)
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

// DrainAll returns all pending replays and empties every queue.
//
// This is useful for graceful shutdown scenarios where you want
// to persist pending replays before exiting. High-priority messages
// are returned first (cluster A, then cluster B), followed by
// low-priority messages in the same cluster order.
//
// Returns:
//   - []types.ReplayPayload: All pending replay payloads
func (m *MemoryReplayer) DrainAll() []types.ReplayPayload {
	var payloads []types.ReplayPayload
	if !m.initialized() {
		return payloads
	}

	for i := range m.queues {
		payloads = m.drainChannel(m.queues[i].high, payloads)
	}
	for i := range m.queues {
		payloads = m.drainChannel(m.queues[i].low, payloads)
	}

	return payloads
}

// drainChannel appends every payload buffered in ch to payloads,
// releasing each one's capacity slot.
func (m *MemoryReplayer) drainChannel(ch chan types.ReplayPayload, payloads []types.ReplayPayload) []types.ReplayPayload {
	for {
		select {
		case payload := <-ch:
			m.releaseSlot(payload.TargetCluster)
			payloads = append(payloads, payload)
		default:
			return payloads
		}
	}
}
