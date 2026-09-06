package types

// CallerContextMetrics is an OPTIONAL interface that [MetricsCollector]
// implementations may satisfy to receive the caller-expired counters.
//
// Helix's read and write paths type-assert on this interface and silently
// no-op if the configured collector does not implement it.
// By-hand [MetricsCollector] implementations stay source-compatible and may
// opt in by adding the methods below.
// Bundled collectors (e.g. contrib/metrics/vm) implement this interface
// directly.
//
// # Why the counters exist
//
// A leg whose error arrives after the caller's context was already
// cancelled or past its deadline is attributed to the caller rather than to
// the cluster: no failover, no FailoverPolicy.RecordFailure, no
// [MetricsCollector.IncReadError] or [MetricsCollector.IncWriteError], and
// no session-liveness failure.
// The attribution is right — the caller gave up first — but it leaves a
// frozen cluster invisible.
//
// A leg carries no deadline of its own unless a dual-cluster client sets
// helix.WithClusterReadTimeout / helix.WithClusterWriteTimeout, so every
// request against a cluster that accepts connections and then never answers
// ends exactly that way: the caller's context expires, the error is
// attributed to the caller, and the cluster's error and health counters stay
// clean while every caller times out.
// These counters are the only trace such a leg leaves.
//
// A cluster whose caller-expired counter climbs while its error counter
// stays flat is the signature of that failure.
// For a dual-cluster client the fix is a leg deadline: with one configured
// the leg expires on Helix's own deadline while the caller is still waiting,
// which surfaces as [ErrClusterTimeout] and does count against the cluster.
//
// Counter semantics:
//   - IncReadCallerExpired: one read attempt against one cluster ended after
//     the caller's context was done. Recorded once per attempt, on the
//     cluster the attempt targeted, so a read counts at most once per
//     operation: a read that fails over on a cluster error and then expires
//     on the alternative is counted only against the alternative.
//   - IncWriteCallerExpired: one write leg was classified as cancelled by
//     the caller. Recorded once per leg per cluster, and only for a leg that
//     was dispatched: a leg a write strategy skipped without sending it
//     (SyncDualWrite returns the caller's context error for the second leg
//     once the first has spent the budget) never reached the cluster and
//     says nothing about whether it is answering.
//
// Both counters are per-cluster facts rather than health signals, so they
// are recorded in single-cluster mode as well as dual-cluster mode.
// Neither option applies to a single-cluster leg — there is no alternative
// cluster to preserve budget for — so the counters are the only signal
// available in that mode and the remedy there is a driver-level request
// timeout on the session instead.
//
// The late result of a background write leg (see helix.DeferredWriteResult)
// is deliberately never counted: it is classified against a context that
// carries the caller's values but none of its cancellation, so it is always
// either an acknowledged or a genuinely failed leg, never a caller-expired
// one.
type CallerContextMetrics interface {
	// IncReadCallerExpired is called when a read leg on cluster returned an
	// error after the caller's context was already cancelled or past its
	// deadline.
	IncReadCallerExpired(cluster ClusterID)

	// IncWriteCallerExpired is called when a write leg on cluster was
	// classified as cancelled by the caller.
	IncWriteCallerExpired(cluster ClusterID)
}
