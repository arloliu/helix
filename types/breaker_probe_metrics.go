package types

// Outcome labels of [BreakerProbeMetrics.IncCircuitBreakerProbe].
const (
	// BreakerProbeSucceeded: the probe reached the cluster and the breaker closed.
	BreakerProbeSucceeded = "succeeded"
	// BreakerProbeFailed: the probe failed and the breaker reopened with a fresh reset timeout.
	BreakerProbeFailed = "failed"
	// BreakerProbeAbandoned: the probe never reported (the client closed) and the reservation was released.
	BreakerProbeAbandoned = "abandoned"
)

// BreakerProbeMetrics is an OPTIONAL interface that [MetricsCollector]
// implementations may satisfy to count how the recovery probes a circuit
// breaker reserved ended.
//
// The breaker type-asserts on this interface and silently no-ops if the
// configured collector does not implement it.
// By-hand [MetricsCollector] implementations stay source-compatible and
// may opt in by adding the method below.
// Bundled collectors (e.g. contrib/metrics/vm) implement this interface
// directly.
type BreakerProbeMetrics interface {
	// IncCircuitBreakerProbe is called once per completed reservation with
	// one of the BreakerProbe outcome constants; an outcome the breaker
	// does not know counts as abandoned, and a completion with a stale
	// token records nothing.
	// Metric: [prefix]_circuit_breaker_probe_total{cluster,outcome}
	IncCircuitBreakerProbe(cluster ClusterID, outcome string)
}
