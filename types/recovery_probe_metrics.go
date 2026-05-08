package types

// RecoveryProbeMetrics is an OPTIONAL interface that [MetricsCollector]
// implementations may satisfy to receive recovery-probe counters from the
// CQLClient's background probe goroutines (see helix.WithRecoveryProbe).
//
// Helix's probe loop type-asserts on this interface and silently no-ops if
// the configured collector does not implement it. By-hand [MetricsCollector]
// implementations stay source-compatible and may opt in by adding the two
// methods below. Bundled collectors (e.g. contrib/metrics/vm) implement
// this interface directly.
//
// Counter semantics:
//   - IncRecoveryProbeSuccess: the probe returned nil; the cluster has been
//     credited with one recovery point via [AdaptiveDualWrite.RecordProbeSuccess].
//   - IncRecoveryProbeFailure: the probe returned a non-nil error; the cluster
//     remains degraded and no recovery point was credited.
//
// Probes run only against degraded clusters, so a healthy cluster produces
// neither counter.
type RecoveryProbeMetrics interface {
	// IncRecoveryProbeSuccess is called after a successful probe (err == nil)
	// against a degraded cluster.
	IncRecoveryProbeSuccess(cluster ClusterID)

	// IncRecoveryProbeFailure is called after a failing probe (err != nil)
	// against a degraded cluster.
	IncRecoveryProbeFailure(cluster ClusterID)
}
