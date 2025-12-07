// Package vm provides a VictoriaMetrics-based implementation of the MetricsCollector interface.
//
// This package uses github.com/VictoriaMetrics/metrics for lightweight,
// high-performance Prometheus-compatible metrics collection.
//
// # Basic Usage
//
// Create a collector with default prefix "helix":
//
//	collector := vm.New()
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithMetrics(collector),
//	)
//
// # Custom Prefix
//
// Use WithPrefix to customize the metric name prefix:
//
//	collector := vm.New(vm.WithPrefix("myapp"))
//
// This produces metrics like:
//   - myapp_read_total{cluster="A"}
//   - myapp_write_duration_seconds{cluster="B"}
//
// # Exposing Metrics
//
// Use the Handler method to expose metrics via HTTP:
//
//	http.HandleFunc("/metrics", collector.Handler)
//	http.ListenAndServe(":8080", nil)
//
// Or use WritePrometheus to write metrics to a custom writer:
//
//	collector.WritePrometheus(w)
//
// # Metrics Provided
//
// Read operations:
//   - {prefix}_read_total{cluster} - Counter of read operations
//   - {prefix}_read_errors_total{cluster} - Counter of read errors
//   - {prefix}_read_duration_seconds{cluster} - Histogram of read latencies
//
// Write operations:
//   - {prefix}_write_total{cluster} - Counter of write operations
//   - {prefix}_write_errors_total{cluster} - Counter of write errors
//   - {prefix}_write_duration_seconds{cluster} - Histogram of write latencies
//
// Failover:
//   - {prefix}_failover_total{from,to} - Counter of failover events
//
// Circuit breaker:
//   - {prefix}_circuit_breaker_state{cluster} - Gauge of circuit state (0=closed, 1=half-open, 2=open)
//   - {prefix}_circuit_breaker_trips_total{cluster} - Counter of circuit trips
//
// Replay queue:
//   - {prefix}_replay_enqueued_total{cluster} - Counter of enqueued replays
//   - {prefix}_replay_success_total{cluster} - Counter of successful replays
//   - {prefix}_replay_errors_total{cluster} - Counter of failed replays
//   - {prefix}_replay_queue_depth{cluster} - Gauge of current queue depth
//   - {prefix}_replay_duration_seconds{cluster} - Histogram of replay latencies
//
// Cluster health:
//   - {prefix}_cluster_draining{cluster} - Gauge (1=draining, 0=healthy)
//   - {prefix}_drain_mode_entered_total{cluster} - Counter of drain entries
//   - {prefix}_drain_mode_exited_total{cluster} - Counter of drain exits
//
// Connection pool:
//   - {prefix}_active_connections{cluster} - Gauge of active connections
//
// Latency:
//   - {prefix}_p99_latency_seconds{cluster} - Gauge of observed P99 latency
//
// # Performance Notes
//
// This implementation pre-creates all metrics at initialization time
// using the NewXXX pattern (instead of GetOrCreateXXX) for optimal
// performance in hot paths, as recommended by the VictoriaMetrics documentation.
//
// The metrics are registered with a dedicated Set that is registered
// globally, allowing standard Prometheus scraping.
package vm
