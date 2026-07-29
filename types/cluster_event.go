package types

import "time"

// ClusterEventKind identifies the category of a [ClusterEvent].
//
// Values are stable snake_case strings suitable for direct use as log
// fields or alert labels.
type ClusterEventKind string

const (
	// EventFailover fires when a read fails on the selected cluster and is
	// retried on the alternative cluster. FromCluster/ToCluster identify
	// the direction; Err carries the error that triggered the failover.
	EventFailover ClusterEventKind = "failover"

	// EventReadDivergence fires when a fallback read finds a row on the
	// alternative cluster after the selected cluster returned not-found.
	// Cluster identifies the cluster that was missing the row (replay lag).
	EventReadDivergence ClusterEventKind = "read_divergence"

	// EventCircuitBreakerOpen fires when a circuit breaker trips open for
	// a cluster. Count carries the consecutive-failure count at trip time.
	EventCircuitBreakerOpen ClusterEventKind = "circuit_breaker_open"

	// EventCircuitBreakerClosed fires when a previously open circuit
	// breaker closes after a successful operation.
	EventCircuitBreakerClosed ClusterEventKind = "circuit_breaker_closed"

	// EventWriteDegraded fires when AdaptiveDualWrite transitions a cluster
	// into degraded (fire-and-forget) mode. Count carries the slow-strike
	// count; Reason distinguishes threshold-based from manual transitions.
	EventWriteDegraded ClusterEventKind = "write_degraded"

	// EventWriteRecovered fires when AdaptiveDualWrite transitions a
	// cluster back to healthy. Reason distinguishes fast-strike recovery
	// from manual recovery ("manual", "manual reset").
	EventWriteRecovered ClusterEventKind = "write_recovered"

	// EventDrainEntered fires when a cluster enters drain mode via the
	// topology watcher.
	EventDrainEntered ClusterEventKind = "drain_entered"

	// EventDrainExited fires when a cluster exits drain mode.
	EventDrainExited ClusterEventKind = "drain_exited"

	// EventReplayDropped fires when a failed write cannot be enqueued for
	// replay (queue full or unavailable) — potential data loss. Cluster is
	// the replay target; Err carries the enqueue error. For payload
	// access, use helix.WithOnReplayDropped.
	EventReplayDropped ClusterEventKind = "replay_dropped"

	// EventMirrorReplayDropped fires when a failed mirror write cannot be
	// enqueued for mirror replay — potential mirror-target data loss. Err
	// carries the enqueue error. Cluster is unset: mirror payloads target
	// a logical sink, not one of this client's clusters. This event fires
	// only while Helix's internal mirror error handler is installed — a
	// caller-supplied mirror.WithOnError replaces that handler (existing
	// "caller options win" semantics) and with it this event.
	EventMirrorReplayDropped ClusterEventKind = "mirror_replay_dropped"

	// EventSessionRefreshAttempt fires when the auto-refresh detector
	// decides a cluster's session is permanently dead and invokes the
	// SessionRefresher. Count carries the qualifying consecutive-failure
	// count observed at the trigger decision.
	EventSessionRefreshAttempt ClusterEventKind = "session_refresh_attempt"

	// EventSessionRefreshSuccess fires after a successful session refresh.
	EventSessionRefreshSuccess ClusterEventKind = "session_refresh_success"

	// EventSessionRefreshError fires when a session refresh attempt fails.
	// Err carries the refresh error.
	EventSessionRefreshError ClusterEventKind = "session_refresh_error"
)

// ClusterEvent describes an operationally significant cluster-health
// transition observed by a Helix client or one of its policies.
//
// Delivery is asynchronous and best-effort: a bounded buffer absorbs
// bursts, and events are dropped (and counted) rather than ever blocking
// a read/write operation. Treat this as an alerting/notification stream,
// not a durable audit log — metrics remain the authoritative source for
// rates and current state.
//
// Ordering: events produced by circuit-breaker and adaptive-write state
// transitions are delivered in per-cluster transition order. Events from
// independent producers (failover, read divergence, replay drops, drain
// transitions, session refresh) are delivered in enqueue order with no
// cross-kind causal guarantee. Metric updates and log lines may become
// visible before or after the corresponding handler invocation.
//
// Only the fields relevant to a given Kind are populated; unpopulated
// fields hold zero values.
//
// Field population by Kind:
//   - EventFailover: FromCluster, ToCluster, Cluster (= ToCluster), Err
//   - EventReadDivergence: Cluster (cluster missing the row), Reason
//   - EventCircuitBreakerOpen: Cluster, Count (failures at trip)
//   - EventCircuitBreakerClosed: Cluster
//   - EventWriteDegraded: Cluster, Count (slow strikes), Reason
//   - EventWriteRecovered: Cluster, Reason
//   - EventDrainEntered / EventDrainExited: Cluster
//   - EventReplayDropped: Cluster (replay target), Err (enqueue error)
//   - EventMirrorReplayDropped: Err (enqueue error), Reason; Cluster unset
//   - EventSessionRefreshAttempt: Cluster, Count (qualifying failures)
//   - EventSessionRefreshSuccess: Cluster
//   - EventSessionRefreshError: Cluster, Err
type ClusterEvent struct {
	// Kind identifies the transition category.
	Kind ClusterEventKind

	// Cluster is the cluster this event primarily concerns.
	Cluster ClusterID

	// FromCluster and ToCluster describe direction for EventFailover.
	FromCluster ClusterID
	ToCluster   ClusterID

	// Timestamp is when the event was recorded (stamped at transition
	// time for policy events, at emission for client events).
	Timestamp time.Time

	// Err is the error that triggered the event, when applicable.
	Err error

	// Reason is a short human-readable cause (e.g. "slow-strike threshold
	// reached", "manual"), when applicable.
	Reason string

	// Count is a kind-specific counter (failure count, strike count),
	// when applicable.
	Count int
}

// ClusterEventEmitter delivers ClusterEvents to a registered handler.
//
// Contract: implementations must be safe for concurrent use and should
// return quickly. Helix never invokes an emitter while holding policy
// state locks (policy transitions enqueue to an internal outbox and the
// emitter runs after the locks are released), so a slow emitter cannot
// deadlock or stall policy state — but it does delay the read/write
// goroutine that performed the transition, exactly like a slow Logger.
// Reentrant calls from an emitter back into the emitting policy are safe
// (they enqueue and return) but discouraged. Helix's internal dispatcher
// (registered via helix.WithOnClusterEvent) is non-blocking: atomics
// plus a buffered non-blocking send.
type ClusterEventEmitter interface {
	// EmitClusterEvent delivers one event. Should not block.
	EmitClusterEvent(event ClusterEvent)
}
