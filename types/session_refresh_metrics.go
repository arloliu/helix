package types

// SessionRefreshMetrics is an OPTIONAL interface that [MetricsCollector]
// implementations may satisfy to receive auto-refresh counters from the
// CQLClient's auto-refresh detector (see helix.WithAutoRefresh).
//
// Helix's auto-refresh path type-asserts on this interface and silently
// no-ops if the configured collector does not implement it. External
// callers who implement [MetricsCollector] via embedding of
// internal/metrics.NopMetrics get the new methods for free; external
// callers who implement [MetricsCollector] by hand stay source-
// compatible across the v2 release and may opt in to the new metrics
// later by adding the three methods.
//
// Counter semantics:
//   - IncSessionRefreshAttempt: incremented every time the auto-refresh
//     detector decides a cluster's session is permanently dead and is
//     about to invoke the SessionRefresher. Stamped before the refresher
//     call so monitoring sees attempts even if the refresher hangs.
//   - IncSessionRefreshSuccess: incremented after a successful
//     RefreshSession (refresher returned a non-nil session and the swap
//     installed it). Old session has been closed at this point.
//   - IncSessionRefreshError: incremented when the refresher returned an
//     error, returned a nil session, or the swap failed (e.g., client
//     was closed mid-operation).
//
// On every attempt: Attempt is incremented exactly once; either Success
// XOR Error is incremented exactly once afterward. Sum of (Success + Error)
// always equals Attempt.
type SessionRefreshMetrics interface {
	// IncSessionRefreshAttempt is called by the auto-refresh detector
	// just before invoking the SessionRefresher.
	IncSessionRefreshAttempt(cluster ClusterID)
	// IncSessionRefreshSuccess is called after a successful auto-refresh
	// (refresher returned a non-nil session and the swap installed it).
	IncSessionRefreshSuccess(cluster ClusterID)
	// IncSessionRefreshError is called when the refresher returned an
	// error, returned a nil session, or the swap failed.
	IncSessionRefreshError(cluster ClusterID)
}
