package types

// StrictMetrics is an OPTIONAL interface that [MetricsCollector]
// implementations may satisfy to receive counters from [Strict] write paths.
//
// Helix's strict write orchestrator type-asserts on this interface and silently
// no-ops if the configured collector does not implement it. By-hand
// [MetricsCollector] implementations stay source-compatible and may opt in by
// adding the method below. Bundled collectors (e.g. contrib/metrics/vm)
// implement this interface directly.
//
// Counter semantics:
//   - IncWriteSkipped: the cluster was not written to because it was degraded
//     (AdaptiveDualWrite) or draining (drain mode). This is an operational
//     state, not a cluster error — [MetricsCollector.IncWriteError] is NOT
//     incremented for skipped writes.
type StrictMetrics interface {
	// IncWriteSkipped is called when a cluster is skipped by a Strict() write
	// due to its degraded or draining state.
	IncWriteSkipped(cluster ClusterID)
}
