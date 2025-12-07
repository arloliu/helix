// Package types provides shared types and errors for the Helix library.
//
// This is a "leaf" package with no imports from other helix packages,
// allowing it to be imported by any package without causing import cycles.
package types

import (
	"errors"
	"regexp"
)

// ClusterID identifies a cluster in the dual-cluster setup.
type ClusterID string

// String returns the string representation of the ClusterID.
func (c ClusterID) String() string {
	return string(c)
}

const (
	// ClusterA represents the first cluster (often called "primary").
	ClusterA ClusterID = "A"
	// ClusterB represents the second cluster (often called "secondary").
	ClusterB ClusterID = "B"
)

// clusterNameRegex validates cluster names for use in metrics labels.
// Must be Prometheus-compatible: [a-zA-Z_][a-zA-Z0-9_]*
var clusterNameRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// ClusterNames holds custom display names for clusters.
//
// These names are used in metrics labels and log messages instead of the
// default "A" and "B". Names must be:
//   - 1-32 characters long
//   - Prometheus-compatible: start with letter or underscore, contain only
//     alphanumeric characters and underscores
//   - Different from each other
//
// Example names: "us_east", "us_west", "primary", "secondary", "dc1", "dc2"
type ClusterNames struct {
	// A is the display name for ClusterA. Defaults to "A".
	A string

	// B is the display name for ClusterB. Defaults to "B".
	B string
}

// DefaultClusterNames returns the default cluster names ("A" and "B").
func DefaultClusterNames() ClusterNames {
	return ClusterNames{A: "A", B: "B"}
}

// Validate checks that the cluster names are valid for use in metrics.
//
// Returns:
//   - error: Validation error, or nil if valid
func (n ClusterNames) Validate() error {
	if err := validateClusterName(n.A, "A"); err != nil {
		return err
	}
	if err := validateClusterName(n.B, "B"); err != nil {
		return err
	}
	if n.A == n.B {
		return errors.New("helix: cluster names must be different")
	}

	return nil
}

// Name returns the display name for the given cluster ID.
func (n ClusterNames) Name(cluster ClusterID) string {
	if cluster == ClusterA {
		return n.A
	}
	return n.B
}

// ClusterNamer is an optional interface for components that can use custom cluster names.
//
// Components implementing this interface will have their cluster names set by the client
// after construction. This allows centralized configuration of cluster names at the
// client level, which are then propagated to metrics collectors, loggers, policies, etc.
//
// Example implementation:
//
//	type MyPolicy struct {
//	    clusterNames types.ClusterNames
//	}
//
//	func (p *MyPolicy) SetClusterNames(names types.ClusterNames) {
//	    p.clusterNames = names
//	}
type ClusterNamer interface {
	// SetClusterNames sets the display names for clusters.
	//
	// This method is called by the client after construction to propagate
	// custom cluster names configured via WithClusterNames.
	//
	// Parameters:
	//   - names: The cluster names to use for metrics and logging
	SetClusterNames(names ClusterNames)
}

// validateClusterName validates a single cluster name.
func validateClusterName(name, which string) error {
	if len(name) == 0 {
		return errors.New("helix: cluster " + which + " name cannot be empty")
	}
	if len(name) > 32 {
		return errors.New("helix: cluster " + which + " name cannot exceed 32 characters")
	}
	if !clusterNameRegex.MatchString(name) {
		return errors.New("helix: cluster " + which + " name must be alphanumeric with underscores, starting with letter or underscore")
	}

	return nil
}

// Consistency represents the Cassandra consistency level.
type Consistency uint16

// Common consistency levels matching gocql.
const (
	Any         Consistency = 0x00
	One         Consistency = 0x01
	Two         Consistency = 0x02
	Three       Consistency = 0x03
	Quorum      Consistency = 0x04
	All         Consistency = 0x05
	LocalQuorum Consistency = 0x06
	EachQuorum  Consistency = 0x07
	Serial      Consistency = 0x08
	LocalSerial Consistency = 0x09
	LocalOne    Consistency = 0x0A
)

// BatchType represents the type of batch operation.
type BatchType byte

// Batch types matching gocql.
//
// WARNING: CounterBatch operations are NOT idempotent. Counter updates
// (e.g., "UPDATE ... SET counter = counter + 1") are additive, so replaying
// them after a partial failure will cause double-counting. Do not use
// CounterBatch with the Helix Replay System if you require exactly-once
// semantics. Consider using a separate reconciliation strategy for counters.
const (
	LoggedBatch   BatchType = 0
	UnloggedBatch BatchType = 1
	CounterBatch  BatchType = 2
)

// PriorityLevel defines the priority for replay operations.
type PriorityLevel int

const (
	// PriorityHigh indicates critical writes that must be replayed ASAP.
	PriorityHigh PriorityLevel = iota
	// PriorityLow indicates best-effort writes that can be delayed.
	PriorityLow
)

// BatchStatement represents a single statement in a batch for replay.
type BatchStatement struct {
	// Query is the CQL statement.
	Query string

	// Args are the bound values for the query.
	Args []any
}

// ReplayPayload contains the information needed to replay a failed write.
type ReplayPayload struct {
	// TargetCluster identifies which cluster failed and needs replay.
	TargetCluster ClusterID

	// Query is the CQL statement to replay (for single-query writes).
	// Empty when IsBatch is true.
	Query string

	// Args are the bound values for the query.
	// Empty when IsBatch is true.
	Args []any

	// IsBatch indicates if this is a batch operation.
	IsBatch bool

	// BatchType is the type of batch (Logged, Unlogged, Counter).
	// Only used when IsBatch is true.
	BatchType BatchType

	// BatchStatements contains the statements in a batch.
	// Only used when IsBatch is true.
	BatchStatements []BatchStatement

	// Timestamp is the client-generated timestamp for idempotency.
	// This ensures replays don't overwrite newer data.
	Timestamp int64

	// Priority indicates the importance of this replay.
	Priority PriorityLevel
}

// Sentinel errors for common failure scenarios.
var (
	// ErrBothClustersFailed indicates that a write failed on both clusters.
	// This is returned to the caller as a hard failure.
	ErrBothClustersFailed = errors.New("helix: write failed on both clusters")

	// ErrNoAvailableCluster indicates that no cluster is available for reads.
	// Both clusters are either down or in drain mode.
	ErrNoAvailableCluster = errors.New("helix: no cluster available for read")

	// ErrBothClustersDraining indicates both clusters are in drain mode.
	// No writes can be performed until at least one cluster exits drain mode.
	ErrBothClustersDraining = errors.New("helix: both clusters are draining")

	// ErrSessionClosed indicates an operation was attempted on a closed session.
	ErrSessionClosed = errors.New("helix: session is closed")

	// ErrReplayQueueFull indicates the in-memory replay queue is at capacity.
	// The failed write could not be enqueued for later reconciliation.
	ErrReplayQueueFull = errors.New("helix: replay queue is full")

	// ErrInvalidBatchType indicates an unsupported batch type was specified.
	ErrInvalidBatchType = errors.New("helix: invalid batch type")

	// ErrNilSession indicates that a nil session was provided.
	ErrNilSession = errors.New("helix: session cannot be nil")

	// ErrWriteAsync indicates a write was sent asynchronously (fire-and-forget).
	// This is returned by AdaptiveDualWrite when a cluster is degraded.
	// The write is still attempted in the background, but the caller should
	// not wait for it. Replay system handles reconciliation if it fails.
	ErrWriteAsync = errors.New("helix: write sent asynchronously to degraded cluster")

	// ErrWriteDropped indicates a fire-and-forget write was dropped due to
	// concurrency limit. This protects the application from resource exhaustion
	// when a degraded cluster is slow. The replay system handles reconciliation.
	ErrWriteDropped = errors.New("helix: write dropped due to fire-and-forget concurrency limit")
)

// PartialWriteError represents a write that succeeded on one cluster but failed on another.
//
// This error is NOT returned to the caller. Partial writes are considered successful
// from the caller's perspective, and the failed write is enqueued for replay.
// This type is used internally for logging and metrics.
type PartialWriteError struct {
	// SucceededCluster is the cluster where the write succeeded.
	SucceededCluster string

	// FailedCluster is the cluster where the write failed.
	FailedCluster string

	// Cause is the underlying error from the failed cluster.
	Cause error
}

// Error implements the error interface.
func (e *PartialWriteError) Error() string {
	return "helix: partial write - succeeded on " + e.SucceededCluster +
		", failed on " + e.FailedCluster + ": " + e.Cause.Error()
}

// Unwrap returns the underlying cause for errors.Is/As compatibility.
func (e *PartialWriteError) Unwrap() error {
	return e.Cause
}

// ClusterError wraps an error from a specific cluster.
type ClusterError struct {
	// Cluster identifies which cluster the error came from.
	Cluster string

	// Operation describes what operation failed.
	Operation string

	// Cause is the underlying error.
	Cause error
}

// Error implements the error interface.
func (e *ClusterError) Error() string {
	return "helix: cluster " + e.Cluster + " " + e.Operation + " failed: " + e.Cause.Error()
}

// Unwrap returns the underlying cause for errors.Is/As compatibility.
func (e *ClusterError) Unwrap() error {
	return e.Cause
}

// DualClusterError represents failures from both clusters.
type DualClusterError struct {
	// ErrorA is the error from cluster A.
	ErrorA error

	// ErrorB is the error from cluster B.
	ErrorB error
}

// Error implements the error interface.
func (e *DualClusterError) Error() string {
	return "helix: both clusters failed - A: " + e.ErrorA.Error() + ", B: " + e.ErrorB.Error()
}

// Unwrap returns the wrapped errors for errors.Is/As compatibility.
// This allows checking for specific error types in either cluster's error.
func (e *DualClusterError) Unwrap() []error {
	return []error{ErrBothClustersFailed, e.ErrorA, e.ErrorB}
}
