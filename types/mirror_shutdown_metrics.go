package types

// MirrorShutdownMetrics is an OPTIONAL interface that [MetricsCollector]
// implementations may satisfy to count the mirror captures the engine
// dropped when its drain timeout cut the shutdown drain short (see
// mirror.WithDrainTimeout).
//
// The engine type-asserts on this interface when it stops and silently
// no-ops if the configured collector does not implement it.
// By-hand [MetricsCollector] implementations stay source-compatible and
// may opt in by adding the method below.
// Bundled collectors (e.g. contrib/metrics/vm) implement this interface
// directly.
//
// Like the [MirrorMetrics] methods, the counter is not cluster-scoped:
// mirror payloads target a logical sink, not one of this client's
// clusters.
type MirrorShutdownMetrics interface {
	// AddMirrorDrainDropped adds n (always > 0) captures still queued when
	// the drain timeout elapsed; each was also handed to the engine's drop
	// handler.
	// Metric: [prefix]_mirror_drain_dropped_total
	AddMirrorDrainDropped(n int)
}
