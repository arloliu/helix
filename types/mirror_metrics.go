package types

// MirrorMetrics is an OPTIONAL interface that [MetricsCollector]
// implementations may satisfy to receive async-mirror counters and gauges.
//
// Helix's mirror engine type-asserts on this interface (when configured
// via [WithMirror] or [WithMirrorPublisher]) and silently no-ops if the
// configured collector does not implement it. By-hand [MetricsCollector]
// implementations stay source-compatible across this release and may opt
// in by adding the methods below. Bundled collectors (e.g.
// contrib/metrics/vm) implement this interface directly.
//
// Mirror metrics are NOT cluster-scoped. Mirroring targets a separate
// helix dual-cluster pair (or a publisher); the per-cluster routing
// happens inside the mirror destination's own write path and is recorded
// against that destination's own metrics namespace, not the source
// client's. The methods below report only the source client's view of
// the mirror engine.
//
// Counter semantics:
//   - IncMirrorEnqueueSuccess: a captured write was accepted into the
//     engine's bounded queue.
//   - IncMirrorEnqueueDropped: a captured write was rejected because the
//     engine was disabled, stopped, or its queue was full. Use the
//     engine's drop log / [mirror.WithOnDrop] callback to disambiguate
//     reasons.
//   - IncMirrorExecSuccess: the engine successfully dispatched a captured
//     write. In target mode this means the mirror destination's Exec
//     returned nil; in publisher mode it means publisher.Enqueue
//     returned nil.
//   - IncMirrorExecError: the engine's dispatch returned an error. In
//     target mode pair with [WithMirrorReplayer] for durable retry; in
//     publisher mode pair with [mirror.WithOnError] for observability.
//
// Gauges:
//   - SetMirrorQueueDepth: current queue depth, updated after enqueue and
//     after each dequeue.
//   - SetMirrorEnabled: 1 when accepting new enqueues, 0 when disabled.
//     Updated on engine start, on Enable/Disable transitions, and on Stop.
type MirrorMetrics interface {
	// IncMirrorEnqueueSuccess increments when a captured write enters the
	// engine's bounded queue.
	IncMirrorEnqueueSuccess()

	// IncMirrorEnqueueDropped increments when a captured write is rejected
	// (engine disabled, stopped, or queue full).
	IncMirrorEnqueueDropped()

	// IncMirrorExecSuccess increments when the engine's execute returned
	// nil (write landed at target, or publish.Enqueue returned nil).
	IncMirrorExecSuccess()

	// IncMirrorExecError increments when the engine's execute returned an
	// error.
	IncMirrorExecError()

	// ObserveMirrorExecDuration records execute duration in seconds. Called
	// for both success and error paths.
	ObserveMirrorExecDuration(seconds float64)

	// SetMirrorQueueDepth updates the gauge with the current queue depth.
	SetMirrorQueueDepth(depth int)

	// SetMirrorEnabled updates the gauge: true = accepting new captures,
	// false = disabled or stopped.
	SetMirrorEnabled(enabled bool)
}

// MirrorReplayMetrics is an OPTIONAL interface that [MetricsCollector]
// implementations may satisfy to receive mirror replay-enqueue drop
// counts (see helix.WithMirrorReplayer).
//
// It is separate from [MirrorMetrics] so existing implementations of
// that interface stay source-compatible. Helix's internal mirror error
// handler type-asserts on this interface and silently no-ops if the
// configured collector does not implement it; a caller-supplied
// mirror.WithOnError replaces that handler and with it this metric,
// exactly as it does [EventMirrorReplayDropped].
//
// Like the [MirrorMetrics] methods, the counter is not cluster-scoped:
// mirror payloads target a logical sink, not one of this client's
// clusters.
type MirrorReplayMetrics interface {
	// IncMirrorReplayDropped increments when a failed mirror write cannot
	// be enqueued for mirror replay — potential mirror-target data loss.
	// Metric: [prefix]_mirror_replay_dropped_total
	IncMirrorReplayDropped()
}
