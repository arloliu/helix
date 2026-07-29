package types

// MetricsCollector defines methods for collecting operational metrics.
//
// All cluster-scoped methods accept a ClusterID parameter for labeling.
// Implementations should be thread-safe as methods may be called concurrently.
//
// Example usage with VictoriaMetrics (via contrib/metrics/vm):
//
//	import vmmetrics "github.com/arloliu/helix/contrib/metrics/vm"
//
//	collector := vmmetrics.New(vmmetrics.WithPrefix("myapp"))
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithMetrics(collector),
//	)
//
//	// Expose metrics via HTTP
//	http.HandleFunc("/metrics", collector.Handler)
type MetricsCollector interface {
	// ----------------------
	// Read Operations
	// ----------------------

	// IncReadTotal increments the total read operations counter.
	// Metric: [prefix]_read_total{cluster="..."}
	IncReadTotal(cluster ClusterID)

	// IncReadError increments the read error counter.
	// Metric: [prefix]_read_errors_total{cluster="..."}
	IncReadError(cluster ClusterID)

	// ObserveReadDuration records a read operation duration in seconds.
	// Metric: [prefix]_read_duration_seconds{cluster="..."}
	ObserveReadDuration(cluster ClusterID, seconds float64)

	// IncReadDivergence increments the counter when a FallbackRead finds data
	// on the alternative cluster after the selected cluster returned not-found.
	// The cluster parameter is the cluster that was missing the row, allowing
	// operators to correlate divergence with replay lag on a specific cluster.
	// Metric: [prefix]_read_divergence_total{cluster="..."}
	IncReadDivergence(cluster ClusterID)

	// ----------------------
	// Write Operations
	// ----------------------

	// IncWriteTotal increments the total write operations counter.
	// Metric: [prefix]_write_total{cluster="..."}
	IncWriteTotal(cluster ClusterID)

	// IncWriteError increments the write error counter.
	// Metric: [prefix]_write_errors_total{cluster="..."}
	IncWriteError(cluster ClusterID)

	// IncWriteAsync increments the counter when a write is dispatched asynchronously
	// to a degraded cluster via fire-and-forget (AdaptiveDualWrite only).
	// This is an operational state, not a cluster error.
	// Metric: [prefix]_write_async_total{cluster="..."}
	IncWriteAsync(cluster ClusterID)

	// IncWriteDropped increments the counter when a fire-and-forget write is dropped
	// because the concurrency limit (semaphore) is full (AdaptiveDualWrite only).
	// The replay system handles reconciliation for dropped writes.
	// Metric: [prefix]_write_dropped_total{cluster="..."}
	IncWriteDropped(cluster ClusterID)

	// ObserveWriteDuration records a write operation duration in seconds.
	// Metric: [prefix]_write_duration_seconds{cluster="..."}
	ObserveWriteDuration(cluster ClusterID, seconds float64)

	// ----------------------
	// Failover
	// ----------------------

	// IncFailoverTotal increments the failover event counter.
	// Called when a read operation fails over from one cluster to another.
	// Metric: [prefix]_failover_total{from="...",to="..."}
	IncFailoverTotal(fromCluster, toCluster ClusterID)

	// ----------------------
	// Circuit Breaker
	// ----------------------

	// SetCircuitBreakerState sets the circuit breaker state gauge.
	// State values: 0=closed, 1=half-open (reserved; no policy in this
	// module emits it today — a breaker admitting a probe after its reset
	// timeout still reports 2 until the probe's outcome closes or re-opens
	// it), 2=open.
	// Metric: [prefix]_circuit_breaker_state{cluster="..."}
	SetCircuitBreakerState(cluster ClusterID, state int)

	// IncCircuitBreakerTrip increments the counter when circuit breaker trips to open.
	// Metric: [prefix]_circuit_breaker_trips_total{cluster="..."}
	IncCircuitBreakerTrip(cluster ClusterID)

	// ----------------------
	// Replay Queue
	// ----------------------

	// IncReplayEnqueued increments the counter when a write is enqueued for replay.
	// Metric: [prefix]_replay_enqueued_total{cluster="..."}
	IncReplayEnqueued(cluster ClusterID)

	// IncReplaySuccess increments the counter when a replay operation succeeds.
	// Metric: [prefix]_replay_success_total{cluster="..."}
	IncReplaySuccess(cluster ClusterID)

	// IncReplayError increments the counter when a replay operation fails.
	// Metric: [prefix]_replay_errors_total{cluster="..."}
	IncReplayError(cluster ClusterID)

	// IncReplayDropped increments the counter when a replay payload cannot be enqueued.
	// This indicates potential data loss if the replay queue is full or unavailable.
	// Metric: [prefix]_replay_dropped_total{cluster="..."}
	IncReplayDropped(cluster ClusterID)

	// SetReplayQueueDepth sets the current replay queue depth gauge.
	// Metric: [prefix]_replay_queue_depth{cluster="..."}
	SetReplayQueueDepth(cluster ClusterID, depth int)

	// ObserveReplayDuration records a replay operation duration in seconds.
	// Metric: [prefix]_replay_duration_seconds{cluster="..."}
	ObserveReplayDuration(cluster ClusterID, seconds float64)

	// ----------------------
	// Cluster Health
	// ----------------------

	// SetClusterDraining sets the drain status gauge for a cluster.
	// Value: 1 if draining, 0 if healthy.
	// Metric: [prefix]_cluster_draining{cluster="..."}
	SetClusterDraining(cluster ClusterID, draining bool)

	// IncDrainModeEntered increments the counter when a cluster enters drain mode.
	// Metric: [prefix]_drain_mode_entered_total{cluster="..."}
	IncDrainModeEntered(cluster ClusterID)

	// IncDrainModeExited increments the counter when a cluster exits drain mode.
	// Metric: [prefix]_drain_mode_exited_total{cluster="..."}
	IncDrainModeExited(cluster ClusterID)
}
