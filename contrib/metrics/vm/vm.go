package vm

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sync/atomic"

	"github.com/VictoriaMetrics/metrics"
	"github.com/arloliu/helix/types"
)

// Option configures a Collector.
type Option func(*Collector)

// defaultDurationBuckets is the canonical default for all
// *_duration_seconds histograms. It is never handed out directly: New
// copies it into each collector and DefaultDurationBuckets returns a
// fresh copy on every call, so no caller can mutate it and corrupt
// buckets for a collector built afterward.
var defaultDurationBuckets = []float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
}

// DefaultDurationBuckets returns a copy of the default histogram upper
// bounds (seconds) used for all *_duration_seconds histograms. They
// target CQL operation latency: healthy reads/writes land in the
// 1-50 ms buckets, while the 0.25-10 s tail captures degraded clusters,
// retries, and driver timeouts (default driver timeouts fall between
// 600 ms and 12 s). Values above 10 s land in the implicit +Inf bucket.
//
// Override per collector with [WithDurationBuckets].
//
// Returns:
//   - []float64: A fresh copy of the default bucket upper bounds
func DefaultDurationBuckets() []float64 {
	return append([]float64(nil), defaultDurationBuckets...)
}

// validDurationBuckets reports whether buckets is non-empty, strictly
// increasing, and contains only finite positive values. VictoriaMetrics
// only rejects non-increasing bounds by panicking at histogram
// construction; it does not validate for NaN, infinite, zero, or negative
// values, so this check covers those cases too before anything reaches
// the histogram constructor.
func validDurationBuckets(buckets []float64) bool {
	if len(buckets) == 0 {
		return false
	}
	prev := 0.0
	for _, b := range buckets {
		if math.IsNaN(b) || math.IsInf(b, 0) || b <= prev {
			return false
		}
		prev = b
	}

	return true
}

// WithPrefix sets the metric name prefix.
//
// Default: "helix"
//
// Parameters:
//   - prefix: The prefix to use for all metric names
//
// Returns:
//   - Option: A configuration option
func WithPrefix(prefix string) Option {
	return func(c *Collector) {
		c.prefix = prefix
	}
}

// WithClusterNames sets custom display names for clusters in metric labels.
//
// Default: "A" and "B"
//
// Parameters:
//   - names: The cluster names to use in metric labels
//
// Returns:
//   - Option: A configuration option
//
// Example:
//
//	collector := vm.New(
//	    vm.WithClusterNames(types.ClusterNames{A: "us_east", B: "us_west"}),
//	)
func WithClusterNames(names types.ClusterNames) Option {
	return func(c *Collector) {
		c.clusterNames = names
	}
}

// WithDurationBuckets overrides the default buckets (see
// [DefaultDurationBuckets]) for every duration histogram (read, write,
// replay, mirror exec).
//
// Buckets must be strictly increasing, finite, and positive; the +Inf
// bucket is added implicitly. Invalid input is ignored and the collector
// keeps whatever bounds it already has — the defaults, or the bounds set
// by an earlier valid WithDurationBuckets in the same [New] call
// (VictoriaMetrics would otherwise panic at construction). The slice is
// copied; later mutation by the caller has no effect.
//
// Rejection is silent: no error is returned, nothing is logged, and the
// Collector exposes no accessor for its effective bounds. A typo therefore
// produces a working collector with unintended buckets. Validate the input
// yourself with a known-good literal, or check the exposed
// {prefix}_..._duration_seconds_bucket series after startup.
//
// Parameters:
//   - buckets: Histogram upper bounds in seconds, strictly increasing
//
// Returns:
//   - Option: A configuration option
//
// Example:
//
//	collector := vm.New(
//	    vm.WithDurationBuckets([]float64{0.005, 0.05, 0.5, 5}),
//	)
func WithDurationBuckets(buckets []float64) Option {
	return func(c *Collector) {
		if !validDurationBuckets(buckets) {
			return
		}
		c.durationBuckets = append([]float64(nil), buckets...)
	}
}

// WithMetricsSet sets the metrics set to use.
//
// If provided, the collector will register metrics with this set instead of
// creating a new one. The caller is responsible for exposing this set
// (e.g., via metrics.WritePrometheus or a custom handler).
//
// Parameters:
//   - set: The metrics set to use
//
// Returns:
//   - Option: A configuration option
func WithMetricsSet(set *metrics.Set) Option {
	return func(c *Collector) {
		c.set = set
	}
}

// Collector implements types.MetricsCollector using VictoriaMetrics.
//
// All metrics are pre-created at initialization time for optimal performance.
// Thread-safe for concurrent use.
type Collector struct {
	set             *metrics.Set
	prefix          string
	clusterNames    types.ClusterNames
	durationBuckets []float64

	// Read metrics
	readTotalA      *metrics.Counter
	readTotalB      *metrics.Counter
	readErrorsA     *metrics.Counter
	readErrorsB     *metrics.Counter
	readDurationA   *metrics.PrometheusHistogram
	readDurationB   *metrics.PrometheusHistogram
	readDivergenceA *metrics.Counter
	readDivergenceB *metrics.Counter

	// Write metrics
	writeTotalA    *metrics.Counter
	writeTotalB    *metrics.Counter
	writeErrorsA   *metrics.Counter
	writeErrorsB   *metrics.Counter
	writeAsyncA    *metrics.Counter
	writeAsyncB    *metrics.Counter
	writeDroppedA  *metrics.Counter
	writeDroppedB  *metrics.Counter
	writeDurationA *metrics.PrometheusHistogram
	writeDurationB *metrics.PrometheusHistogram

	// Failover metrics
	failoverAToB *metrics.Counter
	failoverBToA *metrics.Counter

	// Circuit breaker metrics
	circuitStateA atomic.Int64
	circuitStateB atomic.Int64
	circuitTripsA *metrics.Counter
	circuitTripsB *metrics.Counter

	// Replay metrics
	replayEnqueuedA   *metrics.Counter
	replayEnqueuedB   *metrics.Counter
	replaySuccessA    *metrics.Counter
	replaySuccessB    *metrics.Counter
	replayErrorsA     *metrics.Counter
	replayErrorsB     *metrics.Counter
	replayDroppedA    *metrics.Counter
	replayDroppedB    *metrics.Counter
	replayQueueDepthA atomic.Int64
	replayQueueDepthB atomic.Int64
	replayDurationA   *metrics.PrometheusHistogram
	replayDurationB   *metrics.PrometheusHistogram

	// Cluster health metrics
	clusterDrainingA  atomic.Int64
	clusterDrainingB  atomic.Int64
	drainModeEnteredA *metrics.Counter
	drainModeEnteredB *metrics.Counter
	drainModeExitedA  *metrics.Counter
	drainModeExitedB  *metrics.Counter

	// Session refresh metrics (optional types.SessionRefreshMetrics)
	sessionRefreshAttemptA *metrics.Counter
	sessionRefreshAttemptB *metrics.Counter
	sessionRefreshSuccessA *metrics.Counter
	sessionRefreshSuccessB *metrics.Counter
	sessionRefreshErrorA   *metrics.Counter
	sessionRefreshErrorB   *metrics.Counter

	// Mirror metrics (optional types.MirrorMetrics)
	mirrorEnqueueSuccess *metrics.Counter
	mirrorEnqueueDropped *metrics.Counter
	mirrorExecSuccess    *metrics.Counter
	mirrorExecError      *metrics.Counter
	mirrorExecDuration   *metrics.PrometheusHistogram
	mirrorQueueDepth     atomic.Int64
	mirrorEnabled        atomic.Int64
}

// New creates a new VictoriaMetrics-based metrics collector.
//
// The collector creates its own metrics.Set and registers it globally.
// All metrics are pre-created at initialization for optimal performance.
//
// Parameters:
//   - opts: Configuration options (e.g., WithPrefix)
//
// Returns:
//   - *Collector: A new metrics collector ready for use
//
// Example:
//
//	collector := vm.New(vm.WithPrefix("myapp"))
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithMetrics(collector),
//	)
func New(opts ...Option) *Collector {
	c := &Collector{
		prefix:       "helix",
		clusterNames: types.DefaultClusterNames(),
	}

	for _, opt := range opts {
		opt(c)
	}

	// If no set is provided, create a new one and register it globally.
	// If a set is provided, we assume the caller manages it.
	if c.set == nil {
		c.set = metrics.NewSet()
		metrics.RegisterSet(c.set)
	}

	if c.durationBuckets == nil {
		c.durationBuckets = append([]float64(nil), defaultDurationBuckets...)
	}

	c.initMetrics()

	return c
}

// initMetrics pre-creates all metrics with the configured prefix.
func (c *Collector) initMetrics() {
	p := c.prefix
	nA := c.clusterNames.A
	nB := c.clusterNames.B

	// Read metrics
	c.readTotalA = c.set.NewCounter(fmt.Sprintf(`%s_read_total{cluster="%s"}`, p, nA))
	c.readTotalB = c.set.NewCounter(fmt.Sprintf(`%s_read_total{cluster="%s"}`, p, nB))
	c.readErrorsA = c.set.NewCounter(fmt.Sprintf(`%s_read_errors_total{cluster="%s"}`, p, nA))
	c.readErrorsB = c.set.NewCounter(fmt.Sprintf(`%s_read_errors_total{cluster="%s"}`, p, nB))
	c.readDurationA = c.set.NewPrometheusHistogramExt(fmt.Sprintf(`%s_read_duration_seconds{cluster="%s"}`, p, nA), c.durationBuckets)
	c.readDurationB = c.set.NewPrometheusHistogramExt(fmt.Sprintf(`%s_read_duration_seconds{cluster="%s"}`, p, nB), c.durationBuckets)
	c.readDivergenceA = c.set.NewCounter(fmt.Sprintf(`%s_read_divergence_total{cluster="%s"}`, p, nA))
	c.readDivergenceB = c.set.NewCounter(fmt.Sprintf(`%s_read_divergence_total{cluster="%s"}`, p, nB))

	// Write metrics
	c.writeTotalA = c.set.NewCounter(fmt.Sprintf(`%s_write_total{cluster="%s"}`, p, nA))
	c.writeTotalB = c.set.NewCounter(fmt.Sprintf(`%s_write_total{cluster="%s"}`, p, nB))
	c.writeErrorsA = c.set.NewCounter(fmt.Sprintf(`%s_write_errors_total{cluster="%s"}`, p, nA))
	c.writeErrorsB = c.set.NewCounter(fmt.Sprintf(`%s_write_errors_total{cluster="%s"}`, p, nB))
	c.writeAsyncA = c.set.NewCounter(fmt.Sprintf(`%s_write_async_total{cluster="%s"}`, p, nA))
	c.writeAsyncB = c.set.NewCounter(fmt.Sprintf(`%s_write_async_total{cluster="%s"}`, p, nB))
	c.writeDroppedA = c.set.NewCounter(fmt.Sprintf(`%s_write_dropped_total{cluster="%s"}`, p, nA))
	c.writeDroppedB = c.set.NewCounter(fmt.Sprintf(`%s_write_dropped_total{cluster="%s"}`, p, nB))
	c.writeDurationA = c.set.NewPrometheusHistogramExt(fmt.Sprintf(`%s_write_duration_seconds{cluster="%s"}`, p, nA), c.durationBuckets)
	c.writeDurationB = c.set.NewPrometheusHistogramExt(fmt.Sprintf(`%s_write_duration_seconds{cluster="%s"}`, p, nB), c.durationBuckets)

	// Failover metrics
	c.failoverAToB = c.set.NewCounter(fmt.Sprintf(`%s_failover_total{from="%s",to="%s"}`, p, nA, nB))
	c.failoverBToA = c.set.NewCounter(fmt.Sprintf(`%s_failover_total{from="%s",to="%s"}`, p, nB, nA))

	// Circuit breaker metrics - use gauges with callbacks
	c.set.NewGauge(fmt.Sprintf(`%s_circuit_breaker_state{cluster="%s"}`, p, nA), func() float64 {
		return float64(c.circuitStateA.Load())
	})
	c.set.NewGauge(fmt.Sprintf(`%s_circuit_breaker_state{cluster="%s"}`, p, nB), func() float64 {
		return float64(c.circuitStateB.Load())
	})
	c.circuitTripsA = c.set.NewCounter(fmt.Sprintf(`%s_circuit_breaker_trips_total{cluster="%s"}`, p, nA))
	c.circuitTripsB = c.set.NewCounter(fmt.Sprintf(`%s_circuit_breaker_trips_total{cluster="%s"}`, p, nB))

	// Replay metrics
	c.replayEnqueuedA = c.set.NewCounter(fmt.Sprintf(`%s_replay_enqueued_total{cluster="%s"}`, p, nA))
	c.replayEnqueuedB = c.set.NewCounter(fmt.Sprintf(`%s_replay_enqueued_total{cluster="%s"}`, p, nB))
	c.replaySuccessA = c.set.NewCounter(fmt.Sprintf(`%s_replay_success_total{cluster="%s"}`, p, nA))
	c.replaySuccessB = c.set.NewCounter(fmt.Sprintf(`%s_replay_success_total{cluster="%s"}`, p, nB))
	c.replayErrorsA = c.set.NewCounter(fmt.Sprintf(`%s_replay_errors_total{cluster="%s"}`, p, nA))
	c.replayErrorsB = c.set.NewCounter(fmt.Sprintf(`%s_replay_errors_total{cluster="%s"}`, p, nB))
	c.replayDroppedA = c.set.NewCounter(fmt.Sprintf(`%s_replay_dropped_total{cluster="%s"}`, p, nA))
	c.replayDroppedB = c.set.NewCounter(fmt.Sprintf(`%s_replay_dropped_total{cluster="%s"}`, p, nB))
	c.set.NewGauge(fmt.Sprintf(`%s_replay_queue_depth{cluster="%s"}`, p, nA), func() float64 {
		return float64(c.replayQueueDepthA.Load())
	})
	c.set.NewGauge(fmt.Sprintf(`%s_replay_queue_depth{cluster="%s"}`, p, nB), func() float64 {
		return float64(c.replayQueueDepthB.Load())
	})
	c.replayDurationA = c.set.NewPrometheusHistogramExt(fmt.Sprintf(`%s_replay_duration_seconds{cluster="%s"}`, p, nA), c.durationBuckets)
	c.replayDurationB = c.set.NewPrometheusHistogramExt(fmt.Sprintf(`%s_replay_duration_seconds{cluster="%s"}`, p, nB), c.durationBuckets)

	// Cluster health metrics
	c.set.NewGauge(fmt.Sprintf(`%s_cluster_draining{cluster="%s"}`, p, nA), func() float64 {
		return float64(c.clusterDrainingA.Load())
	})
	c.set.NewGauge(fmt.Sprintf(`%s_cluster_draining{cluster="%s"}`, p, nB), func() float64 {
		return float64(c.clusterDrainingB.Load())
	})
	c.drainModeEnteredA = c.set.NewCounter(fmt.Sprintf(`%s_drain_mode_entered_total{cluster="%s"}`, p, nA))
	c.drainModeEnteredB = c.set.NewCounter(fmt.Sprintf(`%s_drain_mode_entered_total{cluster="%s"}`, p, nB))
	c.drainModeExitedA = c.set.NewCounter(fmt.Sprintf(`%s_drain_mode_exited_total{cluster="%s"}`, p, nA))
	c.drainModeExitedB = c.set.NewCounter(fmt.Sprintf(`%s_drain_mode_exited_total{cluster="%s"}`, p, nB))

	// Session refresh metrics (optional types.SessionRefreshMetrics)
	c.sessionRefreshAttemptA = c.set.NewCounter(fmt.Sprintf(`%s_session_refresh_attempt_total{cluster="%s"}`, p, nA))
	c.sessionRefreshAttemptB = c.set.NewCounter(fmt.Sprintf(`%s_session_refresh_attempt_total{cluster="%s"}`, p, nB))
	c.sessionRefreshSuccessA = c.set.NewCounter(fmt.Sprintf(`%s_session_refresh_success_total{cluster="%s"}`, p, nA))
	c.sessionRefreshSuccessB = c.set.NewCounter(fmt.Sprintf(`%s_session_refresh_success_total{cluster="%s"}`, p, nB))
	c.sessionRefreshErrorA = c.set.NewCounter(fmt.Sprintf(`%s_session_refresh_error_total{cluster="%s"}`, p, nA))
	c.sessionRefreshErrorB = c.set.NewCounter(fmt.Sprintf(`%s_session_refresh_error_total{cluster="%s"}`, p, nB))

	// Mirror metrics (optional types.MirrorMetrics)
	c.mirrorEnqueueSuccess = c.set.NewCounter(fmt.Sprintf(`%s_mirror_enqueue_success_total`, p))
	c.mirrorEnqueueDropped = c.set.NewCounter(fmt.Sprintf(`%s_mirror_enqueue_dropped_total`, p))
	c.mirrorExecSuccess = c.set.NewCounter(fmt.Sprintf(`%s_mirror_exec_success_total`, p))
	c.mirrorExecError = c.set.NewCounter(fmt.Sprintf(`%s_mirror_exec_errors_total`, p))
	c.mirrorExecDuration = c.set.NewPrometheusHistogramExt(fmt.Sprintf(`%s_mirror_exec_duration_seconds`, p), c.durationBuckets)
	c.set.NewGauge(fmt.Sprintf(`%s_mirror_queue_depth`, p), func() float64 {
		return float64(c.mirrorQueueDepth.Load())
	})
	c.set.NewGauge(fmt.Sprintf(`%s_mirror_enabled`, p), func() float64 {
		return float64(c.mirrorEnabled.Load())
	})
}

func (c *Collector) Set() *metrics.Set {
	return c.set
}

// Handler returns an HTTP handler that exposes metrics in Prometheus format.
//
// Example:
//
//	http.HandleFunc("/metrics", collector.Handler)
func (c *Collector) Handler(w http.ResponseWriter, _ *http.Request) {
	c.set.WritePrometheus(w)
}

// WritePrometheus writes all metrics in Prometheus format to the given writer.
//
// Parameters:
//   - w: The writer to write metrics to
func (c *Collector) WritePrometheus(w io.Writer) {
	c.set.WritePrometheus(w)
}

// ----------------------
// Read Operations
// ----------------------

// IncReadTotal increments the total read operations counter.
func (c *Collector) IncReadTotal(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		c.readTotalA.Inc()
	} else {
		c.readTotalB.Inc()
	}
}

// IncReadError increments the read error counter.
func (c *Collector) IncReadError(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		c.readErrorsA.Inc()
	} else {
		c.readErrorsB.Inc()
	}
}

// ObserveReadDuration records a read operation duration in seconds.
func (c *Collector) ObserveReadDuration(cluster types.ClusterID, seconds float64) {
	if cluster == types.ClusterA {
		c.readDurationA.Update(seconds)
	} else {
		c.readDurationB.Update(seconds)
	}
}

// IncReadDivergence increments the counter when a FallbackRead finds data on
// the alternative cluster after the selected cluster returned not-found.
func (c *Collector) IncReadDivergence(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		c.readDivergenceA.Inc()
	} else {
		c.readDivergenceB.Inc()
	}
}

// ----------------------
// Write Operations
// ----------------------

// IncWriteTotal increments the total write operations counter.
func (c *Collector) IncWriteTotal(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		c.writeTotalA.Inc()
	} else {
		c.writeTotalB.Inc()
	}
}

// IncWriteError increments the write error counter.
func (c *Collector) IncWriteError(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		c.writeErrorsA.Inc()
	} else {
		c.writeErrorsB.Inc()
	}
}

// IncWriteAsync increments the counter for fire-and-forget writes to degraded clusters.
func (c *Collector) IncWriteAsync(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		c.writeAsyncA.Inc()
	} else {
		c.writeAsyncB.Inc()
	}
}

// IncWriteDropped increments the counter for writes dropped due to the concurrency limit.
func (c *Collector) IncWriteDropped(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		c.writeDroppedA.Inc()
	} else {
		c.writeDroppedB.Inc()
	}
}

// ObserveWriteDuration records a write operation duration in seconds.
func (c *Collector) ObserveWriteDuration(cluster types.ClusterID, seconds float64) {
	if cluster == types.ClusterA {
		c.writeDurationA.Update(seconds)
	} else {
		c.writeDurationB.Update(seconds)
	}
}

// ----------------------
// Failover
// ----------------------

// IncFailoverTotal increments the failover event counter.
func (c *Collector) IncFailoverTotal(fromCluster, toCluster types.ClusterID) {
	if fromCluster == types.ClusterA && toCluster == types.ClusterB {
		c.failoverAToB.Inc()
	} else if fromCluster == types.ClusterB && toCluster == types.ClusterA {
		c.failoverBToA.Inc()
	}
}

// ----------------------
// Circuit Breaker
// ----------------------

// SetCircuitBreakerState sets the circuit breaker state gauge.
func (c *Collector) SetCircuitBreakerState(cluster types.ClusterID, state int) {
	if cluster == types.ClusterA {
		c.circuitStateA.Store(int64(state))
	} else {
		c.circuitStateB.Store(int64(state))
	}
}

// IncCircuitBreakerTrip increments the counter when circuit breaker trips to open.
func (c *Collector) IncCircuitBreakerTrip(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		c.circuitTripsA.Inc()
	} else {
		c.circuitTripsB.Inc()
	}
}

// ----------------------
// Replay Queue
// ----------------------

// IncReplayEnqueued increments the counter when a write is enqueued for replay.
func (c *Collector) IncReplayEnqueued(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		c.replayEnqueuedA.Inc()
	} else {
		c.replayEnqueuedB.Inc()
	}
}

// IncReplaySuccess increments the counter when a replay operation succeeds.
func (c *Collector) IncReplaySuccess(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		c.replaySuccessA.Inc()
	} else {
		c.replaySuccessB.Inc()
	}
}

// IncReplayError increments the counter when a replay operation fails.
func (c *Collector) IncReplayError(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		c.replayErrorsA.Inc()
	} else {
		c.replayErrorsB.Inc()
	}
}

// IncReplayDropped increments the counter when a replay payload cannot be enqueued.
func (c *Collector) IncReplayDropped(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		c.replayDroppedA.Inc()
	} else {
		c.replayDroppedB.Inc()
	}
}

// SetReplayQueueDepth sets the current replay queue depth gauge.
func (c *Collector) SetReplayQueueDepth(cluster types.ClusterID, depth int) {
	if cluster == types.ClusterA {
		c.replayQueueDepthA.Store(int64(depth))
	} else {
		c.replayQueueDepthB.Store(int64(depth))
	}
}

// ObserveReplayDuration records a replay operation duration in seconds.
func (c *Collector) ObserveReplayDuration(cluster types.ClusterID, seconds float64) {
	if cluster == types.ClusterA {
		c.replayDurationA.Update(seconds)
	} else {
		c.replayDurationB.Update(seconds)
	}
}

// ----------------------
// Cluster Health
// ----------------------

// SetClusterDraining sets the drain status gauge for a cluster.
func (c *Collector) SetClusterDraining(cluster types.ClusterID, draining bool) {
	val := int64(0)
	if draining {
		val = 1
	}
	if cluster == types.ClusterA {
		c.clusterDrainingA.Store(val)
	} else {
		c.clusterDrainingB.Store(val)
	}
}

// IncDrainModeEntered increments the counter when a cluster enters drain mode.
func (c *Collector) IncDrainModeEntered(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		c.drainModeEnteredA.Inc()
	} else {
		c.drainModeEnteredB.Inc()
	}
}

// IncDrainModeExited increments the counter when a cluster exits drain mode.
func (c *Collector) IncDrainModeExited(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		c.drainModeExitedA.Inc()
	} else {
		c.drainModeExitedB.Inc()
	}
}

// IncSessionRefreshAttempt increments the counter when the auto-refresh
// detector decides a cluster's session is permanently dead and is about
// to invoke the SessionRefresher. Part of the optional
// types.SessionRefreshMetrics interface.
func (c *Collector) IncSessionRefreshAttempt(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		c.sessionRefreshAttemptA.Inc()
	} else {
		c.sessionRefreshAttemptB.Inc()
	}
}

// IncSessionRefreshSuccess increments the counter after a successful
// auto-refresh (refresher returned a non-nil session and the swap installed it).
func (c *Collector) IncSessionRefreshSuccess(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		c.sessionRefreshSuccessA.Inc()
	} else {
		c.sessionRefreshSuccessB.Inc()
	}
}

// IncSessionRefreshError increments the counter when an auto-refresh
// attempt failed (refresher errored, returned nil, or swap failed).
func (c *Collector) IncSessionRefreshError(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		c.sessionRefreshErrorA.Inc()
	} else {
		c.sessionRefreshErrorB.Inc()
	}
}

// Compile-time assertion that *Collector implements the optional
// types.SessionRefreshMetrics interface.
var _ types.SessionRefreshMetrics = (*Collector)(nil)

// ----------------------
// Mirror metrics (optional types.MirrorMetrics)
// ----------------------

// IncMirrorEnqueueSuccess increments the mirror enqueue-success counter.
func (c *Collector) IncMirrorEnqueueSuccess() { c.mirrorEnqueueSuccess.Inc() }

// IncMirrorEnqueueDropped increments the mirror enqueue-dropped counter.
func (c *Collector) IncMirrorEnqueueDropped() { c.mirrorEnqueueDropped.Inc() }

// IncMirrorExecSuccess increments the mirror exec-success counter.
func (c *Collector) IncMirrorExecSuccess() { c.mirrorExecSuccess.Inc() }

// IncMirrorExecError increments the mirror exec-error counter.
func (c *Collector) IncMirrorExecError() { c.mirrorExecError.Inc() }

// ObserveMirrorExecDuration records the mirror exec duration.
func (c *Collector) ObserveMirrorExecDuration(seconds float64) {
	c.mirrorExecDuration.Update(seconds)
}

// SetMirrorQueueDepth updates the mirror queue depth gauge.
func (c *Collector) SetMirrorQueueDepth(depth int) {
	c.mirrorQueueDepth.Store(int64(depth))
}

// SetMirrorEnabled updates the mirror enabled gauge (1 = accepting,
// 0 = disabled or stopped).
func (c *Collector) SetMirrorEnabled(enabled bool) {
	if enabled {
		c.mirrorEnabled.Store(1)
	} else {
		c.mirrorEnabled.Store(0)
	}
}

// Compile-time assertion that *Collector implements the optional
// types.MirrorMetrics interface.
var _ types.MirrorMetrics = (*Collector)(nil)
