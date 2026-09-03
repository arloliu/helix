package helix

import (
	"context"
	"time"

	"github.com/arloliu/helix/types"
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

// ProbeReporter is an optional interface for write strategies that track
// per-cluster degradation, such as [policy.AdaptiveDualWrite].
//
// When the configured [WriteStrategy] implements it, a dual-cluster client
// runs a background recovery probe against each degraded cluster (see
// [WithRecoveryProbe]) and credits every successful probe through
// RecordProbeSuccess. A strategy that does not implement it is never probed.
//
// Implementations MUST be safe for concurrent use from multiple goroutines.
type ProbeReporter interface {
	// IsDegraded reports whether the strategy currently treats cluster as
	// degraded, that is, whether a probe should run against it.
	IsDegraded(cluster ClusterID) bool

	// RecordProbeSuccess credits one successful probe against cluster.
	RecordProbeSuccess(cluster ClusterID)
}

// ProbeLatencyReporter is an optional interface for write strategies that
// judge a recovery probe by how long it took, such as
// [policy.AdaptiveDualWrite].
//
// When the configured [WriteStrategy] implements it, the recovery probe
// reports each successful probe through RecordProbeLatency instead of
// [ProbeReporter.RecordProbeSuccess], so a cluster that answers the probe
// but too slowly earns no recovery credit.
//
// Implementations MUST be safe for concurrent use from multiple goroutines.
type ProbeLatencyReporter interface {
	// RecordProbeLatency reports one successful probe against cluster and
	// how long it took.
	RecordProbeLatency(cluster ClusterID, latency time.Duration)
}

// LatchReporter is an optional interface for write strategies whose
// degraded state can be latched by an operator, such as
// [policy.AdaptiveDualWrite] after ForceDegrade.
//
// A dual-cluster client skips the recovery probe for a latched cluster:
// a probe could not restore it, and reporting probe successes against it
// would misrepresent the operator's decision as pending recovery.
//
// Implementations MUST be safe for concurrent use from multiple goroutines.
type LatchReporter interface {
	// IsLatched reports whether cluster is held degraded by the operator
	// until an explicit manual recovery.
	IsLatched(cluster ClusterID) bool
}

// EventEmitterSetter is an optional interface for read strategies, write
// strategies, and failover policies that emit cluster events (see
// [WithOnClusterEvent]).
//
// When the configured strategy or policy implements it, [NewCQLClient] installs
// the client's dispatcher before any background goroutine starts, so the
// component's events reach the registered handler. All built-in policies that
// emit events implement it.
type EventEmitterSetter interface {
	// SetEventEmitter installs the emitter the component reports events to.
	// A nil emitter disables emission.
	SetEventEmitter(emitter types.ClusterEventEmitter)
}

// Instrumentable is an optional interface for components that can adopt the
// client's [MetricsCollector]: read strategies, write strategies, failover
// policies, and replay workers.
//
// [NewCQLClient] calls SetMetrics with the client's collector when
// MetricsConfigured reports false, so a component built without its own
// collector shares the client's; a collector set through the component's
// own option is left untouched.
type Instrumentable interface {
	// MetricsConfigured reports whether a collector was explicitly set on
	// the component.
	MetricsConfigured() bool

	// SetMetrics installs the collector the component records to.
	SetMetrics(collector types.MetricsCollector)
}

// LoggerSetter is an optional interface for write strategies and failover
// policies that can adopt the client's [types.Logger].
//
// [NewCQLClient] calls SetLogger with the client's logger on every
// configured strategy and policy that implements it.
type LoggerSetter interface {
	// SetLogger installs the logger the component writes to.
	SetLogger(logger types.Logger)
}

// DeferredWriteResult is the optional interface on the error a
// [WriteStrategy] returns for a leg it completes in the background.
//
// A strategy that dispatches a leg without waiting returns
// [types.ErrWriteAsync] for it. When that error also implements this
// interface, the client defers the leg's replay: it snapshots the write
// and enqueues it for replay only if the background leg later reports a
// failure, so a statement whose background attempt succeeds is not
// applied a second time by an eager replay. An ambiguous failure such as
// a timeout can still lead to a replay of a statement the cluster
// applied; mark such statements [Query.NonIdempotent].
// A plain [types.ErrWriteAsync] without this interface is enqueued for
// replay immediately, as a safety net. [policy.AdaptiveDualWrite]
// implements it for its fire-and-forget legs.
type DeferredWriteResult interface {
	error

	// OnComplete registers fn to run exactly once with the leg's final
	// error, nil on success. If the leg has already completed, fn runs
	// immediately on the caller's goroutine; otherwise it runs on the
	// goroutine that completes the leg, so it must be quick and must not
	// block on the strategy.
	// The leg must complete within a bounded time: [CQLClient.Close]
	// waits for every registered leg before stopping the replay worker.
	OnComplete(fn func(err error))
}

// LatencyRecorder is an optional interface for failover policies that track latency.
//
// For a policy that implements it, the client calls RecordLatency in place
// of [FailoverPolicy.RecordSuccess] after every successful read: RecordLatency
// is the success signal, and the implementation decides whether the sample
// resets the failure count (a fast read) or counts as a soft failure (a slow
// read). Calling both would let a fast RecordSuccess erase the slow-read
// count a latency breaker accumulates. This enables latency-aware circuit
// breaking where slow responses are treated as "soft failures".
//
// Implementations MUST be safe for concurrent use from multiple goroutines.
//
// Example implementation: policy.LatencyCircuitBreaker
type LatencyRecorder interface {
	// RecordLatency records the latency of a successful operation and
	// stands in for RecordSuccess on the read path.
	//
	// Implementations must reset the failure counter for a fast sample and
	// may treat slow responses (above a threshold) as failures for circuit
	// breaker purposes.
	//
	// Parameters:
	//   - cluster: The cluster that was accessed
	//   - latency: The operation duration
	RecordLatency(cluster ClusterID, latency time.Duration)
}

// FailoverProbeReporter is an optional interface for failover policies
// whose open breaker can be probed for recovery by the client, such as
// [policy.CircuitBreaker] and [policy.LatencyCircuitBreaker].
//
// A dual-cluster client asks TryBeginFailoverProbe on every recovery-probe
// tick (see [WithRecoveryProbe]); when it returns true the client runs the
// probe against the cluster's live session and reports the result through
// CompleteFailoverProbe with the same token, so a breaker closes on a
// probe the client ran rather than on a caller's read sacrificed to it.
// A probe the client cancelled (for example on Close) completes as
// [types.ProbeAbandoned].
//
// A write strategy that implements [ProbeReporter] and a failover policy
// that implements this interface share one physical probe per tick.
//
// Implementations MUST be safe for concurrent use from multiple goroutines.
type FailoverProbeReporter interface {
	// TryBeginFailoverProbe reserves the breaker for one probe and returns
	// the reservation token, or false when no probe should run now.
	TryBeginFailoverProbe(cluster ClusterID) (token uint64, ok bool)

	// CompleteFailoverProbe settles the reservation identified by token.
	CompleteFailoverProbe(cluster ClusterID, token uint64, outcome types.ProbeOutcome)
}

// FailoverBelowThresholdReporter is an optional interface for failover
// policies that own a below-threshold failover setting, such as
// [policy.WithFailoverBelowThreshold]. The client logs a startup warning
// while the setting is off, the legacy v1 default.
type FailoverBelowThresholdReporter interface {
	// FailoverBelowThreshold reports whether a failed read below the
	// policy's threshold may fail over.
	FailoverBelowThreshold() bool
}

// RouteVeto is an optional interface for failover policies that can steer
// future reads away from a cluster, such as [policy.LatencyCircuitBreaker]
// while its breaker is open.
//
// It is consulted only when [WithRouteVeto] is enabled, after
// [ReadStrategy.Select] and only for an ordinary read: one that is not
// pinned to a paging cursor, not preserving a legacy paging token, and
// not under an [AllowedClusters] override. The veto is advisory: a vetoed
// selection moves to the other cluster only when that cluster is neither
// draining nor vetoed, and otherwise the selection stands. A rerouted read
// never calls [ReadStrategy.OnFailure]; the strategy's ordinary
// [ReadStrategy.OnSuccess] for the cluster that served the read is
// unchanged. A FallbackRead probe treats a vetoed alternative like a
// draining one and returns not-found without asking it.
//
// Implementations MUST be non-blocking and safe for concurrent use from
// multiple goroutines: VetoRoute runs on every ordinary read.
type RouteVeto interface {
	// VetoRoute reports whether reads should be routed away from cluster.
	VetoRoute(cluster ClusterID) bool
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
