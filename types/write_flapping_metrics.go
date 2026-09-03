package types

// WriteFlappingMetrics is an OPTIONAL interface that [MetricsCollector]
// implementations may satisfy to count the [EventWriteFlapping]
// transitions of policy.AdaptiveDualWrite: a strike-driven degrade that
// follows a recovery so soon that the re-degrade backoff reached its cap.
//
// The strategy type-asserts on this interface and silently no-ops if the
// configured collector does not implement it.
// By-hand [MetricsCollector] implementations stay source-compatible and
// may opt in by adding the method below.
// Bundled collectors (e.g. contrib/metrics/vm) implement this interface
// directly.
type WriteFlappingMetrics interface {
	// IncWriteFlapping is called once per run of re-degrades, when the
	// backoff cap is first reached, at the same transition that emits
	// [EventWriteFlapping].
	// Metric: [prefix]_write_flapping_total{cluster}
	IncWriteFlapping(cluster ClusterID)
}
