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
	IncReadTotal(cluster ClusterID)

	// IncReadError increments the read error counter.
	IncReadError(cluster ClusterID)

	// ObserveReadDuration records a read operation duration in seconds.
	ObserveReadDuration(cluster ClusterID, seconds float64)

	// ----------------------
	// Write Operations
	// ----------------------

	// IncWriteTotal increments the total write operations counter.
	IncWriteTotal(cluster ClusterID)

	// IncWriteError increments the write error counter.
	IncWriteError(cluster ClusterID)

	// ObserveWriteDuration records a write operation duration in seconds.
	ObserveWriteDuration(cluster ClusterID, seconds float64)

	// ----------------------
	// Failover
	// ----------------------

	// IncFailoverTotal increments the failover event counter.
	// Called when a read operation fails over from one cluster to another.
	IncFailoverTotal(fromCluster, toCluster ClusterID)

	// ----------------------
	// Circuit Breaker
	// ----------------------

	// SetCircuitBreakerState sets the circuit breaker state gauge.
	// State values: 0=closed, 1=half-open, 2=open.
	SetCircuitBreakerState(cluster ClusterID, state int)

	// IncCircuitBreakerTrip increments the counter when circuit breaker trips to open.
	IncCircuitBreakerTrip(cluster ClusterID)

	// ----------------------
	// Replay Queue
	// ----------------------

	// IncReplayEnqueued increments the counter when a write is enqueued for replay.
	IncReplayEnqueued(cluster ClusterID)

	// IncReplaySuccess increments the counter when a replay operation succeeds.
	IncReplaySuccess(cluster ClusterID)

	// IncReplayError increments the counter when a replay operation fails.
	IncReplayError(cluster ClusterID)

	// IncReplayDropped increments the counter when a replay payload cannot be enqueued.
	// This indicates potential data loss if the replay queue is full or unavailable.
	IncReplayDropped(cluster ClusterID)

	// SetReplayQueueDepth sets the current replay queue depth gauge.
	SetReplayQueueDepth(cluster ClusterID, depth int)

	// ObserveReplayDuration records a replay operation duration in seconds.
	ObserveReplayDuration(cluster ClusterID, seconds float64)

	// ----------------------
	// Cluster Health
	// ----------------------

	// SetClusterDraining sets the drain status gauge for a cluster.
	// Value: 1 if draining, 0 if healthy.
	SetClusterDraining(cluster ClusterID, draining bool)

	// IncDrainModeEntered increments the counter when a cluster enters drain mode.
	IncDrainModeEntered(cluster ClusterID)

	// IncDrainModeExited increments the counter when a cluster exits drain mode.
	IncDrainModeExited(cluster ClusterID)
}
