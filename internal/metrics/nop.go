// Package metrics provides internal metrics utilities for Helix.
package metrics

import "github.com/arloliu/helix/types"

// NopMetrics is a no-op metrics collector that discards all metrics.
//
// This is used as the default metrics collector when no collector is configured,
// avoiding nil checks throughout the codebase.
type NopMetrics struct{}

// Compile-time assertion that NopMetrics implements types.MetricsCollector.
var _ types.MetricsCollector = (*NopMetrics)(nil)

// NewNopMetrics creates a new no-op metrics collector.
//
// Returns:
//   - *NopMetrics: A collector that discards all metrics
func NewNopMetrics() *NopMetrics {
	return &NopMetrics{}
}

// ----------------------
// Read Operations
// ----------------------

// IncReadTotal discards the metric.
func (m *NopMetrics) IncReadTotal(_ types.ClusterID) {}

// IncReadError discards the metric.
func (m *NopMetrics) IncReadError(_ types.ClusterID) {}

// ObserveReadDuration discards the metric.
func (m *NopMetrics) ObserveReadDuration(_ types.ClusterID, _ float64) {}

// IncReadDivergence discards the metric.
func (m *NopMetrics) IncReadDivergence(_ types.ClusterID) {}

// ----------------------
// Write Operations
// ----------------------

// IncWriteTotal discards the metric.
func (m *NopMetrics) IncWriteTotal(_ types.ClusterID) {}

// IncWriteError discards the metric.
func (m *NopMetrics) IncWriteError(_ types.ClusterID) {}

// IncWriteAsync discards the metric.
func (m *NopMetrics) IncWriteAsync(_ types.ClusterID) {}

// IncWriteDropped discards the metric.
func (m *NopMetrics) IncWriteDropped(_ types.ClusterID) {}

// ObserveWriteDuration discards the metric.
func (m *NopMetrics) ObserveWriteDuration(_ types.ClusterID, _ float64) {}

// ----------------------
// Failover
// ----------------------

// IncFailoverTotal discards the metric.
func (m *NopMetrics) IncFailoverTotal(_, _ types.ClusterID) {}

// ----------------------
// Circuit Breaker
// ----------------------

// SetCircuitBreakerState discards the metric.
func (m *NopMetrics) SetCircuitBreakerState(_ types.ClusterID, _ int) {}

// IncCircuitBreakerTrip discards the metric.
func (m *NopMetrics) IncCircuitBreakerTrip(_ types.ClusterID) {}

// ----------------------
// Replay Queue
// ----------------------

// IncReplayEnqueued discards the metric.
func (m *NopMetrics) IncReplayEnqueued(_ types.ClusterID) {}

// IncReplaySuccess discards the metric.
func (m *NopMetrics) IncReplaySuccess(_ types.ClusterID) {}

// IncReplayError discards the metric.
func (m *NopMetrics) IncReplayError(_ types.ClusterID) {}

// IncReplayDropped discards the metric.
func (m *NopMetrics) IncReplayDropped(_ types.ClusterID) {}

// SetReplayQueueDepth discards the metric.
func (m *NopMetrics) SetReplayQueueDepth(_ types.ClusterID, _ int) {}

// ObserveReplayDuration discards the metric.
func (m *NopMetrics) ObserveReplayDuration(_ types.ClusterID, _ float64) {}

// ----------------------
// Cluster Health
// ----------------------

// SetClusterDraining discards the metric.
func (m *NopMetrics) SetClusterDraining(_ types.ClusterID, _ bool) {}

// IncDrainModeEntered discards the metric.
func (m *NopMetrics) IncDrainModeEntered(_ types.ClusterID) {}

// IncDrainModeExited discards the metric.
func (m *NopMetrics) IncDrainModeExited(_ types.ClusterID) {}

// ----------------------
// Session Refresh (optional types.SessionRefreshMetrics)
// ----------------------

// IncSessionRefreshAttempt discards the metric.
func (m *NopMetrics) IncSessionRefreshAttempt(_ types.ClusterID) {}

// IncSessionRefreshSuccess discards the metric.
func (m *NopMetrics) IncSessionRefreshSuccess(_ types.ClusterID) {}

// IncSessionRefreshError discards the metric.
func (m *NopMetrics) IncSessionRefreshError(_ types.ClusterID) {}

// Compile-time assertion that NopMetrics implements the optional
// types.SessionRefreshMetrics interface, so embedders pick up the
// no-op coverage automatically.
var _ types.SessionRefreshMetrics = (*NopMetrics)(nil)

// ----------------------
// Mirror (optional types.MirrorMetrics)
// ----------------------

// IncMirrorEnqueueSuccess discards the metric.
func (m *NopMetrics) IncMirrorEnqueueSuccess() {}

// IncMirrorEnqueueDropped discards the metric.
func (m *NopMetrics) IncMirrorEnqueueDropped() {}

// IncMirrorExecSuccess discards the metric.
func (m *NopMetrics) IncMirrorExecSuccess() {}

// IncMirrorExecError discards the metric.
func (m *NopMetrics) IncMirrorExecError() {}

// ObserveMirrorExecDuration discards the metric.
func (m *NopMetrics) ObserveMirrorExecDuration(_ float64) {}

// SetMirrorQueueDepth discards the metric.
func (m *NopMetrics) SetMirrorQueueDepth(_ int) {}

// SetMirrorEnabled discards the metric.
func (m *NopMetrics) SetMirrorEnabled(_ bool) {}

// Compile-time assertion that NopMetrics implements the optional
// types.MirrorMetrics interface so embedders pick up no-op coverage.
var _ types.MirrorMetrics = (*NopMetrics)(nil)

// ----------------------
// Strict writes (optional types.StrictMetrics)
// ----------------------

// IncWriteSkipped discards the metric.
func (m *NopMetrics) IncWriteSkipped(_ types.ClusterID) {}

// Compile-time assertion that NopMetrics implements the optional
// types.StrictMetrics interface so embedders pick up no-op coverage.
var _ types.StrictMetrics = (*NopMetrics)(nil)

// ----------------------
// Adaptive write transitions (optional types.AdaptiveWriteMetrics)
// ----------------------

// SetWriteDegraded discards the metric.
func (m *NopMetrics) SetWriteDegraded(_ types.ClusterID, _ bool) {}

// IncWriteDegraded discards the metric.
func (m *NopMetrics) IncWriteDegraded(_ types.ClusterID) {}

// IncWriteRecovered discards the metric.
func (m *NopMetrics) IncWriteRecovered(_ types.ClusterID) {}

// Compile-time assertion that NopMetrics implements the optional
// types.AdaptiveWriteMetrics interface so embedders pick up no-op coverage.
var _ types.AdaptiveWriteMetrics = (*NopMetrics)(nil)

// ----------------------
// Cluster event dispatcher (optional types.ClusterEventMetrics)
// ----------------------

// AddClusterEventsDropped discards the metric.
func (m *NopMetrics) AddClusterEventsDropped(_ int) {}

// Compile-time assertion that NopMetrics implements the optional
// types.ClusterEventMetrics interface so embedders pick up no-op coverage.
var _ types.ClusterEventMetrics = (*NopMetrics)(nil)

// ----------------------
// Mirror replay (optional types.MirrorReplayMetrics)
// ----------------------

// IncMirrorReplayDropped discards the metric.
func (m *NopMetrics) IncMirrorReplayDropped() {}

// Compile-time assertion that NopMetrics implements the optional
// types.MirrorReplayMetrics interface so embedders pick up no-op coverage.
var _ types.MirrorReplayMetrics = (*NopMetrics)(nil)

// ----------------------
// Recovery probe (optional types.RecoveryProbeMetrics)
// ----------------------

// IncRecoveryProbeSuccess discards the metric.
func (m *NopMetrics) IncRecoveryProbeSuccess(_ types.ClusterID) {}

// IncRecoveryProbeFailure discards the metric.
func (m *NopMetrics) IncRecoveryProbeFailure(_ types.ClusterID) {}

// Compile-time assertion that NopMetrics implements the optional
// types.RecoveryProbeMetrics interface so embedders pick up no-op coverage.
var _ types.RecoveryProbeMetrics = (*NopMetrics)(nil)
