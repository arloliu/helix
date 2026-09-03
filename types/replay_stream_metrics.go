package types

// ReplayStreamMetrics is an OPTIONAL interface that [MetricsCollector]
// implementations may satisfy to receive the NATS replay worker's
// stream-level signals: messages it could not decode, terminations the
// server refused, and messages the stream removed without the worker's
// acknowledgement.
//
// The worker type-asserts on this interface and silently no-ops if the
// configured collector does not implement it.
// By-hand [MetricsCollector] implementations stay source-compatible and
// may opt in by adding the methods below.
// Bundled collectors (e.g. contrib/metrics/vm) implement this interface
// directly.
//
// Counter semantics:
//   - IncReplayCorrupt ([prefix]_replay_corrupt_total{cluster}): a fetched
//     message did not decode (bad bytes, an unknown target cluster, or
//     undecodable arguments) and was terminated; cluster is the consumer's
//     cluster, which is known even when the payload is not.
//   - IncReplayTermFailed ([prefix]_replay_term_failed_total{cluster}): the
//     server refused a Term, so a message the worker gave up on may be
//     delivered again.
//   - AddReplayEvicted ([prefix]_replay_evicted_total): messages the stream
//     removed without an acknowledgement from this process, as counted by
//     the worker's opt-in eviction watch; the stream does not say which
//     cluster they targeted.
type ReplayStreamMetrics interface {
	// IncReplayCorrupt is called once per message terminated at decode.
	IncReplayCorrupt(cluster ClusterID)

	// IncReplayTermFailed is called once per Term the server refused.
	IncReplayTermFailed(cluster ClusterID)

	// AddReplayEvicted adds n (always > 0) messages the stream removed
	// without this process acknowledging them.
	AddReplayEvicted(n int)
}
