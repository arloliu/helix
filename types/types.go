// Package types provides shared types and errors for the Helix library.
//
// This is a "leaf" package with no imports from other helix packages,
// allowing it to be imported by any package without causing import cycles.
package types

import (
	"errors"
	"fmt"
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
// CounterBatch operations are NOT idempotent: counter updates
// (e.g., "UPDATE ... SET counter = counter + 1") are additive, so replaying
// them after a partial failure would double-count. The Helix client treats
// a CounterBatch as non-idempotent automatically: it is never replayed and
// a partial failure is reported to the caller as a PartialWriteError.
const (
	LoggedBatch   BatchType = 0
	UnloggedBatch BatchType = 1
	CounterBatch  BatchType = 2
)

// Duration is a CQL duration value: months, days, and nanoseconds, the
// same three components the drivers' own duration types carry.
//
// Replay carries a duration argument as this type, and the bundled
// adapters convert it to the driver's duration when binding, so a caller
// may pass either a Duration or the driver's own type to a query.
type Duration struct {
	Months      int32
	Days        int32
	Nanoseconds int64
}

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

	// Consistency is the consistency level the original write used, or nil
	// when the write used the session default. A replay applies it so the
	// replayed write is acknowledged under the same rule as the original.
	Consistency *Consistency

	// SerialConsistency is the serial consistency level the original write
	// used, or nil when it used the session default.
	SerialConsistency *Consistency

	// NonIdempotent marks a statement that must not be applied twice, such
	// as a counter update (see the client's NonIdempotent option).
	// The write path never enqueues such a statement for replay; the flag
	// is carried so a mirror destination executes it on its strict path.
	NonIdempotent bool
}

// NoSynchronousAckError reports a dual-cluster write that no cluster
// acknowledged before the call returned: every leg was dispatched in the
// background ([ErrWriteAsync]), dropped ([ErrWriteDropped]), skipped
// ([ErrClusterDraining]), or failed.
//
// The write may still land through the replay queue: Replay is nil when
// every leg that needed replay was enqueued, and carries the enqueue error
// (or [ErrNoReplayer]) otherwise.
// A leg still running in the background counts as enqueued here
// provisionally: its failure, if any, is enqueued when it completes, and a
// failure to enqueue it then is reported only through the client's
// replay-dropped callback and event.
// Callers that accept a replay admission as success select that mode on
// the client instead of inspecting Replay.
//
// errors.Is(err, ErrNoSynchronousAck) matches every value of this type;
// errors.Is also reaches the individual leg results.
type NoSynchronousAckError struct {
	// ResultA is cluster A's leg result.
	ResultA error

	// ResultB is cluster B's leg result.
	ResultB error

	// Replay is nil when the write was admitted to the replay queue for
	// every leg that needed it, or a leg is still running in the background
	// and will be enqueued if it fails; otherwise the reason it was not.
	Replay error
}

// Error implements the error interface.
func (e *NoSynchronousAckError) Error() string {
	msg := ErrNoSynchronousAck.Error() + " (A: " + errString(e.ResultA) + ", B: " + errString(e.ResultB) + ")"
	if e.Replay != nil {
		msg += ", replay: " + e.Replay.Error()
	}

	return msg
}

// Unwrap exposes [ErrNoSynchronousAck], both leg results, and the replay
// error to errors.Is and errors.As.
func (e *NoSynchronousAckError) Unwrap() []error {
	errs := []error{ErrNoSynchronousAck}
	for _, err := range []error{e.ResultA, e.ResultB, e.Replay} {
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errs
}

func errString(err error) string {
	if err == nil {
		return "<nil>"
	}

	return err.Error()
}

// PartialWriteError indicates that a Strict() write was acknowledged by exactly
// one cluster. The other cluster did not respond OK before the deadline; the
// mutation MAY OR MAY NOT have applied there.
//
// Callers MUST NOT assume the unacknowledged cluster is in a known state.
// Compensating retries on non-idempotent operations (counters, list/set append)
// can double-apply.
type PartialWriteError struct {
	// Acknowledged is the cluster that returned OK.
	Acknowledged ClusterID

	// Unacknowledged is the cluster that did not ack (reason in Cause).
	Unacknowledged ClusterID

	// Cause is the underlying error: timeout, sentinel, or driver error.
	Cause error
}

// Error implements the error interface.
func (e *PartialWriteError) Error() string {
	return fmt.Sprintf("helix: strict write acknowledged on %s but not on %s: %v",
		e.Acknowledged, e.Unacknowledged, e.Cause)
}

// Unwrap returns the underlying cause for errors.Is/As compatibility.
func (e *PartialWriteError) Unwrap() error { return e.Cause }

// AsPartialWriteError extracts a *PartialWriteError from err using errors.As.
// Returns the error and true if found, or nil and false otherwise.
func AsPartialWriteError(err error) (*PartialWriteError, bool) {
	var pwe *PartialWriteError
	ok := errors.As(err, &pwe)
	return pwe, ok
}

// IsPartialWrite reports whether err contains a *PartialWriteError.
func IsPartialWrite(err error) bool {
	_, ok := AsPartialWriteError(err)
	return ok
}

// OptionError reports an invalid functional-option value.
type OptionError struct {
	// Component identifies where the option is used (for example,
	// "policy.AdaptiveDualWrite").
	Component string

	// Option is the option function name (for example,
	// "WithAdaptiveStrikeThreshold").
	Option string

	// Reason describes why the value is invalid.
	Reason string
}

// Error implements the error interface.
func (e *OptionError) Error() string {
	component := e.Component
	if component == "" {
		component = "unknown"
	}

	option := e.Option
	if option == "" {
		option = "unknown"
	}

	reason := e.Reason
	if reason == "" {
		reason = "invalid value"
	}

	return fmt.Sprintf("helix: invalid option %s.%s: %s", component, option, reason)
}

// AsOptionError extracts an OptionError from err using errors.As.
// Returns the error and true if found, or nil and false otherwise.
func AsOptionError(err error) (*OptionError, bool) {
	var optionErr *OptionError
	ok := errors.As(err, &optionErr)
	return optionErr, ok
}

// IsOptionError reports whether err contains an OptionError.
func IsOptionError(err error) bool {
	_, ok := AsOptionError(err)
	return ok
}

// Sentinel errors for common failure scenarios.
var (
	// ErrBothClustersFailed indicates that a write failed on both clusters.
	// This is returned to the caller as a hard failure.
	ErrBothClustersFailed = errors.New("helix: write failed on both clusters")

	// ErrUnsupportedReplayArg reports a query argument no replay backend can
	// carry (a struct or user-defined type, or a map with non-string keys).
	// Both replayers reject such a payload at enqueue, so the write is
	// reported as a dropped replay instead of failing later in a worker.
	ErrUnsupportedReplayArg = errors.New("helix: replay cannot carry an argument of this type")

	// ErrInvalidTimestamp reports a write timestamp of zero, either passed
	// through WithTimestamp or returned by the client's TimestampProvider.
	// The drivers treat zero as "assign the current time", which would give
	// a replayed write a newer timestamp than the original and let it
	// overwrite later data.
	ErrInvalidTimestamp = errors.New("helix: write timestamp must not be zero")

	// ErrNoSynchronousAck is wrapped by every [NoSynchronousAckError]: no
	// cluster acknowledged the write before the call returned.
	ErrNoSynchronousAck = errors.New("helix: write was not acknowledged synchronously by any cluster")

	// ErrNoReplayer reports that a write leg which needed replay could not
	// be enqueued because the client has no Replayer configured.
	ErrNoReplayer = errors.New("helix: no replayer configured, failed write cannot be replayed")

	// ErrBothClustersDraining indicates both clusters are in drain mode.
	// No writes can be performed until at least one cluster exits drain mode.
	ErrBothClustersDraining = errors.New("helix: both clusters are draining")

	// ErrSessionClosed indicates an operation was attempted on a closed session.
	ErrSessionClosed = errors.New("helix: session is closed")

	// ErrReplayQueueFull indicates the in-memory replay queue is at capacity.
	// The failed write could not be enqueued for later reconciliation.
	ErrReplayQueueFull = errors.New("helix: replay queue is full")

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

	// ErrNoValidClusters indicates that the allowed-clusters override and
	// drain state conflict, leaving no cluster available for reads.
	// The operator must resolve the conflict (adjust the override or clear drain).
	ErrNoValidClusters = errors.New("helix: no valid clusters for read — override and drain state conflict")

	// ErrInvalidClusterOverride indicates that the AllowedClustersFunc returned
	// only unknown ClusterIDs, or targeted an unconfigured cluster in
	// single-cluster mode. This is a fail-closed condition.
	ErrInvalidClusterOverride = errors.New("helix: invalid cluster override — no recognized clusters in returned list")

	// ErrClusterOverridePanic indicates that the AllowedClustersFunc panicked.
	// This is a fail-closed condition; the panic is recovered and the read fails.
	ErrClusterOverridePanic = errors.New("helix: cluster override function panicked")

	// ErrInvalidCluster indicates an operation referenced a cluster the client
	// is not configured for (e.g., SwapSession(ClusterB, …) on a single-cluster
	// client) or used an unknown ClusterID value.
	//
	// Distinct from ErrInvalidClusterOverride, which is read-override-specific.
	ErrInvalidCluster = errors.New("helix: invalid cluster for this client")

	// ErrClusterUnreachable marks a driver error that means the cluster could
	// not be reached at all: no connections in the pool, a closed session, a
	// connection dropped before it answered, or a coordinator reporting that
	// not enough replicas were alive.
	// It is distinct from an error that means the cluster rejected the
	// statement.
	//
	// The bundled adapters wrap such driver errors so that
	// errors.Is(err, ErrClusterUnreachable) holds while the original driver
	// error stays reachable through errors.Is and errors.As.
	// The replay worker uses it to keep retrying a payload for as long as it
	// is retained instead of counting the attempt against a poison budget.
	ErrClusterUnreachable = errors.New("helix: cluster unreachable")

	// ErrClusterTimeout marks an operation that a Helix-owned deadline
	// ended: a per-cluster write leg that exceeded WithClusterWriteTimeout,
	// or a recovery probe that exceeded its Timeout, while the caller's own
	// context was still live. The expiry describes the cluster, not the
	// caller, so it counts as a connectivity failure for auto-refresh; the
	// driver's error stays reachable through errors.Is and errors.As.
	ErrClusterTimeout = errors.New("helix: cluster timed out")

	// ErrSessionReplaced reports that RefreshSession found a different
	// session installed for the cluster than the one it set out to replace:
	// SwapSession or another refresh landed while the refresher ran. The
	// refresher's session is closed and the newer installed session kept.
	ErrSessionReplaced = errors.New("helix: session was replaced while the refresher ran")

	// ErrNoSessionRefresher indicates RefreshSession was called but no
	// SessionRefresher was registered via WithSessionRefresher. The caller
	// must either register one at construction or use the lower-level
	// SwapSession to provide a freshly-built session directly.
	ErrNoSessionRefresher = errors.New("helix: no session refresher configured")

	// ErrClusterDegraded indicates a Strict() write was skipped because the
	// cluster is currently flagged degraded by AdaptiveDualWrite. The write
	// was not sent to that cluster and was not enqueued for replay.
	ErrClusterDegraded = errors.New("helix: cluster is degraded; strict write skipped")

	// ErrClusterDraining indicates a Strict() write was skipped because the
	// cluster is currently in topology drain mode. The write was not sent to
	// that cluster and was not enqueued for replay.
	ErrClusterDraining = errors.New("helix: cluster is draining; strict write skipped")

	// ErrStrictUnsupported indicates the configured WriteStrategy does not
	// implement StrictWriter and a Strict() statement was attempted. Switch to
	// ConcurrentDualWrite, SyncDualWrite, or AdaptiveDualWrite to use Strict().
	ErrStrictUnsupported = errors.New("helix: configured WriteStrategy does not support Strict() writes")

	// ErrStrictMirrorUnsupported indicates Strict() and Mirror() were combined
	// on one statement. The combination is rejected before attempting the write:
	// strict writes are commonly replay-unsafe, and mirror destinations may
	// retry or replay failed dispatches.
	ErrStrictMirrorUnsupported = errors.New("helix: Strict() and Mirror() cannot be combined")

	// ErrMirrorModeConflict indicates that both WithMirror and
	// WithMirrorPublisher were configured. The two mirror modes are
	// mutually exclusive: target mode dispatches writes from this process,
	// publisher mode publishes captures for an out-of-process consumer.
	ErrMirrorModeConflict = errors.New("helix: WithMirror and WithMirrorPublisher are mutually exclusive")

	// ErrNilMirrorTarget indicates that NewMirrorWorker or NewCQLClient
	// (with WithMirror) was called with a nil mirror destination CQLClient.
	ErrNilMirrorTarget = errors.New("helix: mirror target cannot be nil")

	// ErrNilMirrorPublisher indicates that NewCQLClient was called with
	// WithMirrorPublisher and a nil Replayer.
	ErrNilMirrorPublisher = errors.New("helix: mirror publisher cannot be nil")

	// ErrNotFound indicates that a query returned zero rows.
	//
	// This is the Helix sentinel for "not found" results, mapped from
	// gocql.ErrNotFound at the adapter layer. It is NOT treated as a cluster
	// health failure — Helix never records this as a read error or failover trigger.
	//
	// Use [IsNotFound] to check for this error, or errors.Is(err, ErrNotFound).
	ErrNotFound = errors.New("helix: not found")

	// ErrRowLimitExceeded indicates that a bounded multi-row read exceeded
	// its row limit (per-query MaxRows or Config.DefaultMaxRows).
	//
	// This is an application-level cap, not a cluster fault. Like [ErrNotFound],
	// it is NOT treated as a cluster health failure: Helix never records it
	// as a read error, never advances circuit-breaker / auto-refresh state,
	// and never triggers FallbackRead empty-retry. It is propagated to the
	// caller as-is across both clusters, including the FallbackRead alt path.
	//
	// Use [IsRowLimitExceeded] to check for this error, or
	// errors.Is(err, ErrRowLimitExceeded).
	ErrRowLimitExceeded = errors.New("helix: row limit exceeded")
)

// IsNotFound reports whether err is a "not found" result.
//
// Returns true for [ErrNotFound] and any error wrapping it.
// Use this instead of errors.Is(err, gocql.ErrNotFound) — the adapter layer
// maps the driver-specific error to this sentinel.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// IsRowLimitExceeded reports whether err is a row-limit-exceeded result.
//
// Returns true for [ErrRowLimitExceeded] and any error wrapping it.
func IsRowLimitExceeded(err error) bool {
	return errors.Is(err, ErrRowLimitExceeded)
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
//
// If Cause is nil, "<nil>" is substituted in its place rather than panicking.
func (e *ClusterError) Error() string {
	return "helix: cluster " + e.Cluster + " " + e.Operation + " failed: " + errString(e.Cause)
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
//
// If either ErrorA or ErrorB is nil, the corresponding part is omitted from
// the message rather than panicking.
func (e *DualClusterError) Error() string {
	return "helix: both clusters failed - A: " + errString(e.ErrorA) + ", B: " + errString(e.ErrorB)
}

// Unwrap returns the wrapped errors for errors.Is/As compatibility.
// This allows checking for specific error types in either cluster's error.
// Nil cluster errors are excluded from the returned slice.
func (e *DualClusterError) Unwrap() []error {
	errs := []error{ErrBothClustersFailed}
	if e.ErrorA != nil {
		errs = append(errs, e.ErrorA)
	}
	if e.ErrorB != nil {
		errs = append(errs, e.ErrorB)
	}

	return errs
}
