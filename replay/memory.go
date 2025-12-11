// Package replay provides replay queue implementations for failed write reconciliation.
package replay

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/arloliu/helix/types"
)

// MemoryReplayer implements an in-memory replay queue using priority-based buffered channels.
//
// # Priority Support
//
// The replayer uses separate channels for high and low priority messages, sharing
// a total capacity. High-priority messages are preferred during dequeue operations,
// with configurable ratio-based fair scheduling to prevent low-priority starvation.
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

	// For priority-aware dequeue tracking
	mu                sync.Mutex
	highProcessed     int // Count of high-priority items processed since last low
	highPriorityRatio int // Process N high before 1 low (0 = equal priority)
	strictPriority    bool
}

// MemoryReplayerOption configures a MemoryReplayer.
type MemoryReplayerOption func(*MemoryReplayer)

// WithQueueCapacity sets the maximum number of pending replays.
//
// The capacity is shared across both high and low priority queues.
// Each queue gets half the capacity.
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
// sharing a total capacity. Default capacity is 10,000 items (5,000 per priority).
//
// Parameters:
//   - opts: Optional configuration options
//
// Returns:
//   - *MemoryReplayer: A new memory replayer
func NewMemoryReplayer(opts ...MemoryReplayerOption) *MemoryReplayer {
	m := &MemoryReplayer{
		capacity:          10000,
		highPriorityRatio: 10, // Default 10:1 ratio
		strictPriority:    false,
	}

	for _, opt := range opts {
		opt(m)
	}

	// Split capacity between high and low priority queues
	halfCap := m.capacity / 2
	if halfCap < 1 {
		halfCap = 1
	}
	m.highQueue = make(chan types.ReplayPayload, halfCap)
	m.lowQueue = make(chan types.ReplayPayload, halfCap)

	return m
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
//   - error: nil on success, ErrReplayQueueFull if queue is at capacity
func (m *MemoryReplayer) Enqueue(ctx context.Context, payload types.ReplayPayload) error {
	if m.closed.Load() {
		return types.ErrSessionClosed
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
		return ctx.Err()
	case targetQueue <- payload:
		return nil
	default:
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
			return payload, true
		}

		// No messages available, wait for any message or context cancellation
		select {
		case <-ctx.Done():
			return types.ReplayPayload{}, false
		case payload := <-m.highQueue:
			m.mu.Lock()
			m.highProcessed++
			m.mu.Unlock()
			return payload, true
		case payload := <-m.lowQueue:
			m.mu.Lock()
			m.highProcessed = 0 // Reset counter after processing low
			m.mu.Unlock()
			return payload, true
		}
	}
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
	if m.closed.Load() && m.Len() == 0 {
		return types.ReplayPayload{}, false
	}

	return m.tryDequeueWithPriority()
}

// Len returns the current number of pending replays across both queues.
//
// Returns:
//   - int: Total number of items in high and low priority queues
func (m *MemoryReplayer) Len() int {
	return len(m.highQueue) + len(m.lowQueue)
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

// Cap returns the total queue capacity.
//
// Returns:
//   - int: Maximum total queue size (sum of high and low queue capacities)
func (m *MemoryReplayer) Cap() int {
	return cap(m.highQueue) + cap(m.lowQueue)
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

	// Drain high priority first
	for {
		select {
		case payload := <-m.highQueue:
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
			payloads = append(payloads, payload)
		default:
			return payloads
		}
	}
}
