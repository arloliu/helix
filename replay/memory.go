// Package replay provides replay queue implementations for failed write reconciliation.
package replay

import (
	"context"
	"sync/atomic"

	"github.com/arloliu/helix/types"
)

// MemoryReplayer implements an in-memory replay queue using a buffered channel.
//
// WARNING: Data in this queue is volatile and will be lost on process crash.
// For production use with durability requirements, use NATSReplayer (Phase N).
//
// Thread-safety: All methods are safe for concurrent use. The Close method
// marks the replayer as closed but does not close the underlying channel,
// preventing panics from concurrent Enqueue calls during shutdown.
type MemoryReplayer struct {
	queue    chan types.ReplayPayload
	closed   atomic.Bool
	capacity int
}

// MemoryReplayerOption configures a MemoryReplayer.
type MemoryReplayerOption func(*MemoryReplayer)

// WithQueueCapacity sets the maximum number of pending replays.
//
// Parameters:
//   - n: Queue capacity (default: 10000)
//
// Returns:
//   - MemoryReplayerOption: Configuration option
func WithQueueCapacity(n int) MemoryReplayerOption {
	return func(m *MemoryReplayer) {
		m.capacity = n
	}
}

// NewMemoryReplayer creates a new in-memory replayer.
//
// The replayer uses a buffered channel to store failed writes for later replay.
// Default capacity is 10,000 items.
//
// Parameters:
//   - opts: Optional configuration options
//
// Returns:
//   - *MemoryReplayer: A new memory replayer
func NewMemoryReplayer(opts ...MemoryReplayerOption) *MemoryReplayer {
	m := &MemoryReplayer{
		capacity: 10000,
	}

	for _, opt := range opts {
		opt(m)
	}

	m.queue = make(chan types.ReplayPayload, m.capacity)

	return m
}

// Enqueue adds a failed write to the replay queue.
//
// If the queue is full, returns ErrReplayQueueFull immediately (non-blocking).
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

	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.queue <- payload:
		return nil
	default:
		return types.ErrReplayQueueFull
	}
}

// Dequeue retrieves the next replay payload from the queue.
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
	select {
	case <-ctx.Done():
		return types.ReplayPayload{}, false
	case payload := <-m.queue:
		return payload, true
	}
}

// TryDequeue attempts to retrieve a payload without blocking.
//
// Returns:
//   - types.ReplayPayload: The payload if available
//   - bool: true if a payload was retrieved, false if queue is empty or closed
func (m *MemoryReplayer) TryDequeue() (types.ReplayPayload, bool) {
	if m.closed.Load() && len(m.queue) == 0 {
		return types.ReplayPayload{}, false
	}

	select {
	case payload := <-m.queue:
		return payload, true
	default:
		return types.ReplayPayload{}, false
	}
}

// Len returns the current number of pending replays.
//
// Returns:
//   - int: Number of items in the queue
func (m *MemoryReplayer) Len() int {
	return len(m.queue)
}

// Cap returns the queue capacity.
//
// Returns:
//   - int: Maximum queue size
func (m *MemoryReplayer) Cap() int {
	return cap(m.queue)
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

// DrainAll returns all pending replays and empties the queue.
//
// This is useful for graceful shutdown scenarios where you want
// to persist pending replays before exiting.
//
// Returns:
//   - []types.ReplayPayload: All pending replay payloads
func (m *MemoryReplayer) DrainAll() []types.ReplayPayload {
	var payloads []types.ReplayPayload
	for {
		select {
		case payload := <-m.queue:
			payloads = append(payloads, payload)
		default:
			return payloads
		}
	}
}
