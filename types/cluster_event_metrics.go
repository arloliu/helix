package types

// ClusterEventMetrics is an OPTIONAL interface that [MetricsCollector]
// implementations may satisfy to receive the cluster event dispatcher's
// drop total (see helix.WithOnClusterEvent).
//
// The dispatcher type-asserts on this interface at client construction
// and silently no-ops if the configured collector does not implement it.
// By-hand [MetricsCollector] implementations stay source-compatible
// across this release and may opt in later by adding the method. Bundled
// collectors (e.g. contrib/metrics/vm) implement this interface directly.
//
// Call discipline: the method is never called from the read/write hot
// path. The dispatcher counts drops with an atomic and reconciles the
// counter into the metric from its own delivery goroutine (after each
// delivered event) and once more at shutdown, so the metric can lag the
// internal count while the handler is blocked. Drops that occur after
// the shutdown reconciliation (post-Close emissions from unjoined
// background goroutines) are counted internally but not reflected in
// the metric.
type ClusterEventMetrics interface {
	// AddClusterEventsDropped adds n (always > 0) to the dropped-event
	// counter. Called with the delta accumulated since the previous call.
	// Metric: [prefix]_cluster_events_dropped_total
	AddClusterEventsDropped(n int)
}
