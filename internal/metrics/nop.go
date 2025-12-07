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

// ----------------------
// Write Operations
// ----------------------

// IncWriteTotal discards the metric.
func (m *NopMetrics) IncWriteTotal(_ types.ClusterID) {}

// IncWriteError discards the metric.
func (m *NopMetrics) IncWriteError(_ types.ClusterID) {}

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
