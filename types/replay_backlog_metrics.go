package types

// Reasons a replay worker gives when it permanently drops a payload.
// They are the values of the reason label on the per-reason drop series
// reported through [ReplayBacklogMetrics].
const (
	// ReplayDropMaxAttempts: the bounded retry budget was exhausted.
	ReplayDropMaxAttempts = "max_attempts"

	// ReplayDropRetryPoolSaturated: the memory worker's in-flight retry pool
	// was full under the bounded policy, so the payload was dropped instead of
	// queued behind running retries.
	ReplayDropRetryPoolSaturated = "retry_pool_saturated"

	// ReplayDropShutdown: the worker stopped while the payload was still
	// queued or waiting to retry.
	ReplayDropShutdown = "shutdown"

	// ReplayDropDeadLetter: the payload was classified as unprocessable often
	// enough to exhaust the poison budget.
	ReplayDropDeadLetter = "dead_letter"

	// ReplayDropRetryWindowExpired: the memory worker's retention window
	// elapsed before the payload could be replayed.
	ReplayDropRetryWindowExpired = "retry_window_expired"

	// ReplayDropRequeueFailed: the memory worker could not put a payload
	// back in its queue after the cluster gate closed between dequeue and
	// execution. The payload still holds its capacity slot and the queues
	// are sized to the full capacity, so this reason marks a broken
	// accounting invariant rather than queue pressure.
	ReplayDropRequeueFailed = "requeue_failed"
)

// ReplayBacklogMetrics is an OPTIONAL interface that [MetricsCollector]
// implementations may satisfy to receive per-cluster replay backlog signals
// from the bundled replay workers.
//
// The workers type-assert on this interface and silently no-op if the
// configured collector does not implement it, so by-hand [MetricsCollector]
// implementations stay source-compatible.
// Bundled collectors (for example contrib/metrics/vm) implement it directly.
//
// Semantics:
//   - SetReplayOldestAge: age of the payload the worker most recently took
//     for execution, computed from the payload's client timestamp.
//     Payloads leave the queue oldest first, so this approximates the age of
//     the backlog head.
//     Reset to 0 when the worker finds the queue empty.
//   - IncReplayWorkerDropped: incremented once per payload the worker gives
//     up on, labelled with one of the ReplayDrop reason constants.
//     Unlike the replay_dropped_total series it never counts enqueue
//     failures.
type ReplayBacklogMetrics interface {
	// SetReplayOldestAge sets the backlog-head age gauge for a cluster.
	// Metric: [prefix]_replay_oldest_age_seconds{cluster="..."}
	SetReplayOldestAge(cluster ClusterID, seconds float64)

	// IncReplayWorkerDropped increments the per-reason worker drop counter.
	// Metric: [prefix]_replay_worker_dropped_total{cluster="...",reason="..."}
	IncReplayWorkerDropped(cluster ClusterID, reason string)
}
