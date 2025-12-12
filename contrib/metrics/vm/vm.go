package vm

import (
	"fmt"
	"io"
	"net/http"
	"sync/atomic"

	"github.com/VictoriaMetrics/metrics"
	"github.com/arloliu/helix/types"
)

// Option configures a Collector.
type Option func(*Collector)

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
	set          *metrics.Set
	prefix       string
	clusterNames types.ClusterNames

	// Read metrics
	readTotalA    *metrics.Counter
	readTotalB    *metrics.Counter
	readErrorsA   *metrics.Counter
	readErrorsB   *metrics.Counter
	readDurationA *metrics.Histogram
	readDurationB *metrics.Histogram

	// Write metrics
	writeTotalA    *metrics.Counter
	writeTotalB    *metrics.Counter
	writeErrorsA   *metrics.Counter
	writeErrorsB   *metrics.Counter
	writeDurationA *metrics.Histogram
	writeDurationB *metrics.Histogram

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
	replayDurationA   *metrics.Histogram
	replayDurationB   *metrics.Histogram

	// Cluster health metrics
	clusterDrainingA  atomic.Int64
	clusterDrainingB  atomic.Int64
	drainModeEnteredA *metrics.Counter
	drainModeEnteredB *metrics.Counter
	drainModeExitedA  *metrics.Counter
	drainModeExitedB  *metrics.Counter
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
	c.readDurationA = c.set.NewHistogram(fmt.Sprintf(`%s_read_duration_seconds{cluster="%s"}`, p, nA))
	c.readDurationB = c.set.NewHistogram(fmt.Sprintf(`%s_read_duration_seconds{cluster="%s"}`, p, nB))

	// Write metrics
	c.writeTotalA = c.set.NewCounter(fmt.Sprintf(`%s_write_total{cluster="%s"}`, p, nA))
	c.writeTotalB = c.set.NewCounter(fmt.Sprintf(`%s_write_total{cluster="%s"}`, p, nB))
	c.writeErrorsA = c.set.NewCounter(fmt.Sprintf(`%s_write_errors_total{cluster="%s"}`, p, nA))
	c.writeErrorsB = c.set.NewCounter(fmt.Sprintf(`%s_write_errors_total{cluster="%s"}`, p, nB))
	c.writeDurationA = c.set.NewHistogram(fmt.Sprintf(`%s_write_duration_seconds{cluster="%s"}`, p, nA))
	c.writeDurationB = c.set.NewHistogram(fmt.Sprintf(`%s_write_duration_seconds{cluster="%s"}`, p, nB))

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
	c.replayDurationA = c.set.NewHistogram(fmt.Sprintf(`%s_replay_duration_seconds{cluster="%s"}`, p, nA))
	c.replayDurationB = c.set.NewHistogram(fmt.Sprintf(`%s_replay_duration_seconds{cluster="%s"}`, p, nB))

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
