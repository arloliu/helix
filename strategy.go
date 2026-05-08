package helix

import (
	"context"
	"time"
)

// AllowedClustersFunc returns the ordered list of clusters currently allowed
// for reads. The first element is the primary read target; subsequent elements
// are failover targets in priority order.
//
// Return values:
//
//   - []ClusterID{ClusterB}           — only B; A excluded, no failover
//   - []ClusterID{ClusterB, ClusterA} — B primary, A as failover
//   - nil or empty slice              — no override, normal strategy + drain behavior
//
// Duplicate entries are ignored (only the first occurrence counts).
//
// Fail-closed behavior: if the returned list contains only unknown cluster IDs
// (not ClusterA or ClusterB), or if all valid clusters are currently draining,
// the read fails with an error — it does NOT fall through to normal strategy
// routing. A panicking function also fails the read. This ensures operator
// intent is never silently ignored.
//
// The function must be non-blocking, safe for concurrent use from multiple
// goroutines, and cheap to call — it runs on every read operation. A slow
// function adds its latency to every read; if the function may block, cache
// the result in an atomic and read from the atomic.
type AllowedClustersFunc func() []ClusterID

// Replayer handles asynchronous reconciliation of failed writes.
//
// When a dual-write partially fails (one cluster succeeds, one fails),
// the failed write is enqueued for later replay.
//
// Implementations MUST be safe for concurrent use from multiple goroutines.
// Enqueue may be called concurrently from different write operations.
type Replayer interface {
	// Enqueue adds a failed write to the replay queue.
	//
	// Parameters:
	//   - ctx: Context for cancellation
	//   - payload: The write operation to replay
	//
	// Returns:
	//   - error: nil on success, error if enqueue fails
	Enqueue(ctx context.Context, payload ReplayPayload) error
}

// ReadStrategy defines how reads are routed to clusters.
//
// Implementations MUST be safe for concurrent use from multiple goroutines.
// All methods may be called concurrently from different operations.
type ReadStrategy interface {
	// Select chooses which cluster to read from.
	//
	// Parameters:
	//   - ctx: Context for the operation
	//
	// Returns:
	//   - ClusterID: The cluster to read from
	Select(ctx context.Context) ClusterID

	// OnSuccess is called when a read succeeds.
	//
	// Parameters:
	//   - cluster: The cluster that succeeded
	OnSuccess(cluster ClusterID)

	// OnFailure is called when a read fails.
	//
	// Parameters:
	//   - cluster: The cluster that failed
	//   - err: The error that occurred
	//
	// Returns:
	//   - ClusterID: Alternative cluster to try, or empty if no failover
	//   - bool: true if failover should be attempted
	OnFailure(cluster ClusterID, err error) (ClusterID, bool)
}

// WriteStrategy defines how writes are executed across clusters.
//
// Implementations MUST be safe for concurrent use from multiple goroutines.
// Execute may be called concurrently from different write operations.
type WriteStrategy interface {
	// Execute performs the write operation on both clusters.
	//
	// Parameters:
	//   - ctx: Context for the operation
	//   - writeA: Function to write to cluster A
	//   - writeB: Function to write to cluster B
	//
	// Returns:
	//   - resultA: Error from cluster A (nil if successful)
	//   - resultB: Error from cluster B (nil if successful)
	Execute(
		ctx context.Context,
		writeA func(context.Context) error,
		writeB func(context.Context) error,
	) (resultA, resultB error)
}

// FailoverPolicy controls when and how failover occurs.
//
// Implementations MUST be safe for concurrent use from multiple goroutines.
// RecordFailure/RecordSuccess may be called concurrently while ShouldFailover
// is evaluating the current state.
type FailoverPolicy interface {
	// ShouldFailover determines if failover should occur.
	//
	// Parameters:
	//   - cluster: The cluster that failed
	//   - err: The error that occurred
	//
	// Returns:
	//   - bool: true if failover should be attempted
	ShouldFailover(cluster ClusterID, err error) bool

	// RecordFailure records a failure for circuit breaker logic.
	//
	// Parameters:
	//   - cluster: The cluster that failed
	RecordFailure(cluster ClusterID)

	// RecordSuccess records a success to reset failure counters.
	//
	// Parameters:
	//   - cluster: The cluster that succeeded
	RecordSuccess(cluster ClusterID)
}

// StrictWriter is an optional interface for write strategies that support
// Strict() per-statement semantics.
//
// When a [Query] or [Batch] is marked with [Query.Strict]/[Batch.Strict],
// the client type-asserts the configured WriteStrategy to StrictWriter and
// calls ExecuteStrict instead of Execute. A nil WriteStrategy falls back to
// an inline concurrent write (matching the default non-strict path). A non-nil
// WriteStrategy that does not implement StrictWriter fails with
// [types.ErrStrictUnsupported].
//
// All built-in strategies ([policy.ConcurrentDualWrite], [policy.SyncDualWrite],
// [policy.AdaptiveDualWrite]) implement StrictWriter. Custom strategies may
// opt in by adding ExecuteStrict.
//
// ExecuteStrict MUST NOT fire-and-forget writes or enqueue replay for partial
// failures. On partial failure it returns the failing cluster's error directly
// so the caller can surface it as [types.PartialWriteError].
type StrictWriter interface {
	WriteStrategy

	// ExecuteStrict performs writes without fire-and-forget or replay enqueue.
	//
	// On AdaptiveDualWrite: if a cluster is currently degraded, ExecuteStrict
	// skips that cluster's write and returns [types.ErrClusterDegraded] for it
	// instead of dispatching a fire-and-forget goroutine.
	//
	// Returns the raw errors from each cluster (nil on success). The caller is
	// responsible for interpreting partial vs total failure.
	ExecuteStrict(
		ctx context.Context,
		writeA func(context.Context) error,
		writeB func(context.Context) error,
	) (errA, errB error)
}

// LatencyRecorder is an optional interface for failover policies that track latency.
//
// Policies implementing this interface will have RecordLatency called automatically
// by the client after successful operations. This enables latency-aware circuit
// breaking where slow responses are treated as "soft failures".
//
// Implementations MUST be safe for concurrent use from multiple goroutines.
//
// Example implementation: policy.LatencyCircuitBreaker
type LatencyRecorder interface {
	// RecordLatency records the latency of a successful operation.
	//
	// Implementations may treat slow responses (above a threshold) as failures
	// for circuit breaker purposes.
	//
	// Parameters:
	//   - cluster: The cluster that was accessed
	//   - latency: The operation duration
	RecordLatency(cluster ClusterID, latency time.Duration)
}

// TopologyWatcher monitors cluster topology changes.
//
// Implementations include topology.Local (in-memory) and topology.NATS (NATS KV backed).
type TopologyWatcher interface {
	// Watch returns a channel that receives topology updates.
	//
	// Parameters:
	//   - ctx: Context for cancellation
	//
	// Returns:
	//   - <-chan TopologyUpdate: Channel of topology changes
	Watch(ctx context.Context) <-chan TopologyUpdate
}

// TopologyOperator allows setting cluster drain states.
//
// This interface is typically used by operations tools and tests to control
// cluster availability. Implementations include topology.Local (in-memory).
type TopologyOperator interface {
	// SetDrain sets the drain state for a cluster.
	//
	// Parameters:
	//   - ctx: Context for cancellation/timeout
	//   - cluster: The cluster to update
	//   - draining: true to enable drain mode, false to disable
	//   - reason: Human-readable reason for the drain (only used when draining=true)
	//
	// Returns:
	//   - error: nil on success, error if the operation fails
	SetDrain(ctx context.Context, cluster ClusterID, draining bool, reason string) error
}

// ReplayWorker processes failed writes from a replay queue.
//
// Implementations include MemoryWorker and NATSWorker from the replay package.
type ReplayWorker interface {
	// Start begins processing replay messages in background goroutines.
	//
	// Returns:
	//   - error: ErrWorkerAlreadyRunning if already started
	Start() error

	// Stop gracefully stops the worker and waits for pending work to complete.
	Stop()

	// IsRunning returns whether the worker is currently running.
	IsRunning() bool
}

// TopologyUpdate represents a change in cluster topology.
type TopologyUpdate struct {
	// Cluster that was updated.
	Cluster ClusterID

	// Available indicates if the cluster is available.
	Available bool

	// DrainMode indicates if the cluster is in drain mode.
	DrainMode bool
}
