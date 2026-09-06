package types

// StrictMetrics is an OPTIONAL interface that [MetricsCollector]
// implementations may satisfy to receive the skipped-write counter.
//
// Helix's write path type-asserts on this interface and silently
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
//
// Despite the interface name, the counter is not confined to Strict() writes.
// A degraded cluster is skipped only by a Strict() write, but a draining
// cluster's leg is skipped by every write, so an ordinary write to a draining
// cluster increments it too.
type StrictMetrics interface {
	// IncWriteSkipped is called when a cluster's write leg is skipped because
	// the cluster is degraded (Strict() writes only) or draining (any write).
	IncWriteSkipped(cluster ClusterID)
}
