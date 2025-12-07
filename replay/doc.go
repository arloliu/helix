// Package replay provides asynchronous reconciliation mechanisms for failed writes
// in the helix dual-database client.
//
// When a dual-write operation partially fails (one cluster succeeds, one fails),
// the failed operation is queued for later replay to maintain eventual consistency.
//
// # Replayer Interface
//
// The public [helix.Replayer] interface is minimal, requiring only Enqueue:
//
//	type Replayer interface {
//	    Enqueue(ctx context.Context, payload types.ReplayPayload) error
//	}
//
// This minimal interface allows different queue implementations (in-memory, NATS,
// Kafka, etc.) while the Helix client only needs enqueue capability.
//
// # Memory Replayer
//
// [MemoryReplayer] provides an in-memory queue implementation suitable for
// single-instance deployments or testing. It implements the helix.Replayer
// interface plus additional methods for queue management:
//
//   - Dequeue: Block until a payload is available
//   - TryDequeue: Non-blocking dequeue attempt
//   - DrainAll: Retrieve all pending payloads
//   - Len/Cap: Queue size information
//   - Close: Mark queue as closed
//   - IsClosed: Check closed state
//
// Example usage:
//
//	replayer := replay.NewMemoryReplayer(
//	    replay.WithQueueCapacity(1000),
//	)
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithReplayer(replayer),
//	)
//
// The memory replayer is bounded and will return [types.ErrReplayQueueFull]
// when capacity is reached.
//
// # NATS JetStream Replayer
//
// [NATSReplayer] provides a durable, distributed replay queue backed by NATS
// JetStream. It is suitable for production deployments requiring durability
// and multi-instance replay processing.
//
// # Replay Worker
//
// The [Worker] type processes replay payloads automatically in the background.
// Configure it with [helix.WithReplayWorker] and it will be started/stopped
// with the client lifecycle:
//
//	worker := replay.NewWorker(replayer,
//	    replay.WithExecutor(executorFunc),
//	    replay.WithRetryDelay(time.Second),
//	    replay.WithMaxRetries(3),
//	)
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithReplayer(replayer),
//	    helix.WithReplayWorker(worker),
//	)
package replay
