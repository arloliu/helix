package types

// AdaptiveWriteMetrics is an OPTIONAL interface that [MetricsCollector]
// implementations may satisfy to receive AdaptiveDualWrite health-state
// transitions (see policy.AdaptiveDualWrite).
//
// The strategy type-asserts on this interface and silently no-ops if the
// configured collector does not implement it. By-hand [MetricsCollector]
// implementations stay source-compatible across this release and may opt
// in to the new metrics later by adding the three methods. Bundled
// collectors (e.g. contrib/metrics/vm) implement this interface directly.
//
// All three methods are called after the cluster's state mutex has been
// released, once per actual transition — never per write. A cluster that
// is already degraded produces no further calls until it recovers.
//
// Semantics:
//   - SetWriteDegraded: gauge of the cluster's current write mode
//     (true = degraded fire-and-forget, false = healthy synchronous).
//     Gauge writes are sequenced per cluster: a transition report that
//     another transition has already superseded skips the write, so the
//     gauge always ends at the newest latched state.
//   - IncWriteDegraded: incremented once per healthy-to-degraded
//     transition (threshold-based or ForceDegrade), including superseded
//     ones — the counter is cumulative.
//   - IncWriteRecovered: incremented once per degraded-to-healthy
//     transition (fast-strike recovery, ForceRecover, or Reset).
type AdaptiveWriteMetrics interface {
	// SetWriteDegraded sets the degraded-state gauge for a cluster.
	// Metric: [prefix]_write_degraded{cluster="..."} (1=degraded, 0=healthy)
	SetWriteDegraded(cluster ClusterID, degraded bool)

	// IncWriteDegraded increments the healthy-to-degraded transition counter.
	// Metric: [prefix]_write_degraded_total{cluster="..."}
	IncWriteDegraded(cluster ClusterID)

	// IncWriteRecovered increments the degraded-to-healthy transition counter.
	// Metric: [prefix]_write_recovered_total{cluster="..."}
	IncWriteRecovered(cluster ClusterID)
}
