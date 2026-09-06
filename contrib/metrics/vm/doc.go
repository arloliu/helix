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
//   - {prefix}_read_divergence_total{cluster} - Counter of fallback reads that
//     found the row on the alternative cluster (cluster = the one missing it)
//   - {prefix}_read_duration_seconds{cluster} - Histogram of read latencies
//
// Write operations:
//   - {prefix}_write_total{cluster} - Counter of write operations
//   - {prefix}_write_errors_total{cluster} - Counter of write errors
//   - {prefix}_write_async_total{cluster} - Counter of fire-and-forget writes
//     dispatched to a degraded cluster
//   - {prefix}_write_dropped_total{cluster} - Counter of fire-and-forget writes
//     never attempted because the concurrency limit was full
//   - {prefix}_write_degraded{cluster} - Gauge of AdaptiveDualWrite's write
//     mode (1=degraded fire-and-forget, 0=healthy synchronous)
//   - {prefix}_write_degraded_total{cluster} - Counter of healthy-to-degraded
//     transitions
//   - {prefix}_write_recovered_total{cluster} - Counter of degraded-to-healthy
//     transitions
//   - {prefix}_write_duration_seconds{cluster} - Histogram of write latencies
//   - {prefix}_write_flapping_total{cluster} - Counter of re-degrades that
//     reached the backoff cap (optional types.WriteFlappingMetrics)
//
// Failover:
//   - {prefix}_failover_total{from,to} - Counter of failover events
//   - {prefix}_read_preferred{cluster} - Gauge of the read strategy's
//     preferred cluster (1 for the preferred cluster, 0 for the other)
//
// Circuit breaker:
//   - {prefix}_circuit_breaker_state{cluster} - Gauge of circuit state
//     (0=closed, 2=open). The breaker admits a probe once its reset timeout
//     elapses, but writes no distinct gauge value for that window: the gauge
//     still reads 2 until the probe's outcome closes or re-opens the breaker.
//   - {prefix}_circuit_breaker_trips_total{cluster} - Counter of circuit trips
//   - {prefix}_circuit_breaker_probe_total{cluster,outcome} - Counter of
//     completed probe reservations by outcome (succeeded, failed,
//     abandoned; optional types.BreakerProbeMetrics)
//
// Replay queue:
//   - {prefix}_replay_enqueued_total{cluster} - Counter of enqueued replays
//   - {prefix}_replay_success_total{cluster} - Counter of successful replays
//   - {prefix}_replay_errors_total{cluster} - Counter of failed replays
//   - {prefix}_replay_dropped_total{cluster} - Counter of payloads that could
//     not be enqueued by the client, plus payloads permanently dropped by a
//     replay worker after exhausting its retry budget
//   - {prefix}_replay_queue_depth{cluster} - Gauge of payloads not yet
//     replayed, published by the bundled workers: slots held per cluster
//     for the memory backend, undelivered plus unacknowledged messages per
//     cluster for the NATS backend
//   - {prefix}_replay_duration_seconds{cluster} - Histogram of replay latencies
//
// Replay backlog (optional types.ReplayBacklogMetrics):
//   - {prefix}_replay_oldest_age_seconds{cluster} - Gauge of the age of the
//     payload most recently taken for execution, measured from its write
//     timestamp; 0 when nothing is pending
//   - {prefix}_replay_worker_dropped_total{cluster,reason} - Counter of
//     payloads a worker gave up on, by reason (max_attempts,
//     retry_pool_saturated, shutdown, dead_letter, retry_window_expired);
//     unlike replay_dropped_total it never counts enqueue failures
//
// Replay stream (optional types.ReplayStreamMetrics, NATS worker only):
//   - {prefix}_replay_corrupt_total{cluster} - Counter of messages
//     terminated because they did not decode
//   - {prefix}_replay_term_failed_total{cluster} - Counter of terminations
//     the server refused
//   - {prefix}_replay_evicted_total - Counter of messages the stream removed
//     without this process's acknowledgement (the worker's eviction watch)
//
// Cluster health:
//   - {prefix}_cluster_draining{cluster} - Gauge (1=draining, 0=healthy)
//   - {prefix}_drain_mode_entered_total{cluster} - Counter of drain entries
//   - {prefix}_drain_mode_exited_total{cluster} - Counter of drain exits
//
// Session refresh (recorded when the client's auto-refresh detector runs):
//   - {prefix}_session_refresh_attempt_total{cluster} - Counter of refresh attempts
//   - {prefix}_session_refresh_success_total{cluster} - Counter of successful refreshes
//   - {prefix}_session_refresh_error_total{cluster} - Counter of failed refreshes
//
// Recovery probe (recorded when helix.WithRecoveryProbe probes a degraded
// cluster; a healthy cluster produces neither counter):
//   - {prefix}_recovery_probe_success_total{cluster} - Counter of probes that
//     returned nil and credited the cluster with a recovery point (optional
//     types.RecoveryProbeMetrics)
//   - {prefix}_recovery_probe_failure_total{cluster} - Counter of probes that
//     returned an error and left the cluster degraded (optional
//     types.RecoveryProbeMetrics)
//
// Skipped write legs:
//   - {prefix}_write_skipped_total{cluster} - Counter of write legs skipped
//     because the cluster was degraded (Strict() writes only) or draining
//     (any write). A skip is an operational state, so
//     {prefix}_write_errors_total is not incremented alongside it (optional
//     types.StrictMetrics)
//
// Caller-expired legs:
//   - {prefix}_read_caller_expired_total{cluster} - Counter of read attempts
//     that returned an error after the caller's context was already
//     cancelled or past its deadline (optional
//     types.CallerContextMetrics)
//   - {prefix}_write_caller_expired_total{cluster} - Counter of write legs
//     classified as cancelled by the caller (optional
//     types.CallerContextMetrics)
//
// Both are attributed to the caller rather than to the cluster, so
// {prefix}_read_errors_total / {prefix}_write_errors_total are not
// incremented alongside them and the cluster's health is untouched. A leg
// has no deadline of its own unless a dual-cluster client sets
// helix.WithClusterReadTimeout / helix.WithClusterWriteTimeout, so a cluster
// that accepts connections and never answers shows up here and nowhere else:
// a counter climbing while the matching errors_total stays flat is that
// cluster. Both options are inert in single-cluster mode, where these
// counters are the only signal and a driver-level request timeout is the
// remedy.
//
// A CAS iterator (from ExecCASContext or MapExecCASContext) whose Close
// reports a caller-expired error increments the read counter: an iterator's
// outcome is reported through the read path, which does not distinguish CAS
// from an ordinary read.
//
// Mirror (recorded when mirroring is configured):
//   - {prefix}_mirror_enqueue_success_total - Counter of captures accepted by the engine queue
//   - {prefix}_mirror_enqueue_dropped_total - Counter of captures rejected by a full engine queue
//   - {prefix}_mirror_drain_dropped_total - Counter of captures dropped when
//     mirror.WithDrainTimeout cut the shutdown drain short (optional
//     types.MirrorShutdownMetrics)
//   - {prefix}_mirror_exec_success_total - Counter of successful mirror writes
//   - {prefix}_mirror_exec_errors_total - Counter of failed mirror writes
//   - {prefix}_mirror_exec_duration_seconds - Histogram of mirror write latencies
//   - {prefix}_mirror_queue_depth - Gauge of current engine queue depth
//   - {prefix}_mirror_enabled - Gauge (1=mirroring active, 0=inactive)
//   - {prefix}_mirror_replay_dropped_total - Counter of failed mirror writes
//     that could not be enqueued for mirror replay (no cluster label: mirror
//     payloads target a logical sink)
//
// Cluster events (recorded when helix.WithOnClusterEvent is registered):
//   - {prefix}_cluster_events_dropped_total - Counter of cluster events
//     dropped by the dispatcher (handler too slow, or emission racing Close).
//     Reconciled from the dispatcher goroutine, so it can briefly lag the
//     internal count while the handler is blocked.
//
// Every cluster event kind has a metric counterpart; see
// docs/cluster-events.md for the kind-to-metric table.
//
// # Duration Histograms
//
// All *_duration_seconds metrics (read, write, replay, and mirror exec) are
// classic Prometheus histograms with explicit upper bounds, exposed as
// {prefix}_..._duration_seconds_bucket{...,le="<bound>"}, plus the matching
// _sum and _count series. This makes them compatible with
// histogram_quantile() in vanilla Prometheus, unlike VictoriaMetrics-native
// histograms which expose vmrange buckets instead:
//
//	histogram_quantile(0.99, sum(rate(helix_read_duration_seconds_bucket[5m])) by (le))
//
// Queries built on the _sum and _count series are unaffected by the change
// from vmrange buckets; only quantile queries need rewriting.
//
// The default bucket bounds are available via DefaultDurationBuckets() and
// can be overridden per collector with WithDurationBuckets.
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
