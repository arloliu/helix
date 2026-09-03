package types

// ReadRouteMetrics is an OPTIONAL interface that [MetricsCollector]
// implementations may satisfy to receive the read strategy's current
// preference as a gauge.
//
// The read strategies that keep a preferred cluster (policy.StickyRead and
// policy.PrimaryOnlyRead) type-assert on this interface and silently no-op
// if the configured collector does not implement it.
// By-hand [MetricsCollector] implementations stay source-compatible and may
// opt in by adding the method below.
// Bundled collectors (e.g. contrib/metrics/vm) implement this interface
// directly.
//
// Gauge semantics ([prefix]_read_preferred{cluster}):
//   - SetReadPreferred is called for both clusters whenever the preference
//     moves, and once when the collector is installed, so the gauge reads 1
//     for the cluster the strategy prefers and 0 for the other from startup.
//   - The gauge follows the strategy's preference only.
//     A route the client overrides for one request (a veto, a draining
//     cluster, an AllowedClusters override) does not move it.
type ReadRouteMetrics interface {
	// SetReadPreferred records whether cluster is the read strategy's
	// preferred cluster.
	SetReadPreferred(cluster ClusterID, preferred bool)
}
