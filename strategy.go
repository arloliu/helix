package helix

import (
	"context"
	"time"
)

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
