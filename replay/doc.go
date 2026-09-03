// Package replay provides asynchronous reconciliation mechanisms for failed writes
// in the helix dual-database client.
//
// When a dual-write operation partially fails (one cluster succeeds, one fails),
// the failed operation is queued for later replay to maintain eventual consistency.
//
// # Replay Ordering and Idempotency
//
// Chronological execution order of replays is NOT guaranteed. Priority queues,
// concurrent background retries, and network unreliability (e.g., JetStream Naks)
// can cause failed writes to be replayed out of order.
//
// To prevent older replays from overwriting newer data, every write carries
// a client-side timestamp: the client stamps writes with
// [helix.DefaultTimestampProvider] unless [helix.WithTimestampProvider]
// replaces it. That keeps timestamp-resolved (last-write-wins) mutations
// idempotent regardless of execution order; counter updates, collection
// appends, and CAS/LWT statements are not protected by a timestamp and
// must be excluded from replay with the client's NonIdempotent marker.
//
// # CAS / LWT Safety
//
// Lightweight Transactions (CAS) are inherently replay-unsafe. The conditional
// state of the cluster may drift during an outage, causing a blindly replayed
// CAS statement to fail or behave incorrectly. Do not use Helix for CAS/LWT
// operations.
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
// For fail-fast configuration validation, use
// [NewMemoryReplayerChecked], which returns joined [types.OptionError]
// values when options are invalid.
//
// # NATS JetStream Replayer
//
// [NATSReplayer] provides a durable, distributed replay queue backed by NATS
// JetStream. It is suitable for production deployments requiring durability
// and multi-instance replay processing.
//
// [NewNATSReplayer] returns an error on invalid configuration and reports
// option-value issues as joined [types.OptionError] values.
//
// By default, NATSReplayer uses JetStream's DiscardOld policy when MaxMsgs or
// MaxBytes is reached. This is an availability-first bounded replay window:
// new partial writes can continue to enqueue, and the newest replay work is
// retained, but older unreplayed messages may be evicted under sustained
// outage pressure. Use [WithRejectNewOnLimit] when preserving already-admitted
// replay messages is more important than keeping enqueue calls successful.
//
// # Replay Worker
//
// The [Worker] type processes replay payloads automatically in the background.
// Configure it with [helix.WithReplayWorker] and it will be started/stopped
// with the client lifecycle:
//
//	worker := replay.NewMemoryWorker(replayer, executorFunc,
//	    replay.WithRetryDelay(time.Second),
//	)
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithReplayer(replayer),
//	    helix.WithReplayWorker(worker),
//	)
//
// For fail-fast worker configuration validation, use
// [NewMemoryWorkerChecked] or [NewNATSWorkerChecked].
package replay
