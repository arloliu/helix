package vm

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/metrics"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/types"
)

// Adding a recorder?
// Find its signature shape below and add one row to the matching table.
// TestCollector_RecorderTablesCoverEveryRecorder fails the test run if you
// forget.
//
//	func(cluster) 1 series, +1 per call          -> clusterCounterRecorderCases
//	func(cluster, string) 1 series per string     -> stringDimensionRecorderNames + its own t.Run block
//	func(cluster, bool) 1 gauge, 0/1               -> clusterBoolGaugeRecorderCases
//	func(cluster, int/float64) 1 gauge, any value -> clusterValueGaugeRecorderNames + its own t.Run block
//	func() 1 series, +1 per call                   -> nonClusterCounterRecorderCases
//	func(int/bool) 1 gauge, no cluster             -> nonClusterGaugeRecorderNames + its own t.Run block
//	func(cluster, seconds float64) histogram       -> histogramClusterRecorderCases
//	func(n int) 1 series, += n                     -> addRecorderCases
//	func(fromCluster, toCluster)                   -> TestCollector_IncFailoverTotal (the only one of this shape)
//
// Every table row also needs its metric to be exercised in
// TestCollector_WithPrefixRenamesEveryEmittedSeries and, if the recorder
// builds its label from c.clusterNames at call time rather than through a
// pre-created field, in TestCollector_WithClusterNamesChangesLabelValues
// too.

// Compile-time assertions that *Collector satisfies the base
// types.MetricsCollector interface plus every optional metrics interface it
// claims to implement (docs/plans/test-weakness-analysis.md S4).
// Each optional interface is matched at runtime by a type assertion inside
// helix, replay, and mirror — breaking one here (a renamed method, a
// changed parameter type) does not fail a build anywhere else;
// it just makes that assertion stop matching, and the metric it guards
// silently stops being emitted.
// Two of these (types.SessionRefreshMetrics, types.MirrorMetrics) are also
// asserted next to the code they describe, at vm.go:837 and vm.go:947;
// this block is what pins the other thirteen, and is the single place a
// reader checks for the full set.
var (
	_ types.MetricsCollector      = (*Collector)(nil)
	_ types.AdaptiveWriteMetrics  = (*Collector)(nil)
	_ types.BreakerProbeMetrics   = (*Collector)(nil)
	_ types.CallerContextMetrics  = (*Collector)(nil)
	_ types.ClusterEventMetrics   = (*Collector)(nil)
	_ types.MirrorMetrics         = (*Collector)(nil)
	_ types.MirrorReplayMetrics   = (*Collector)(nil)
	_ types.MirrorShutdownMetrics = (*Collector)(nil)
	_ types.ReadRouteMetrics      = (*Collector)(nil)
	_ types.RecoveryProbeMetrics  = (*Collector)(nil)
	_ types.ReplayBacklogMetrics  = (*Collector)(nil)
	_ types.ReplayStreamMetrics   = (*Collector)(nil)
	_ types.SessionRefreshMetrics = (*Collector)(nil)
	_ types.StrictMetrics         = (*Collector)(nil)
	_ types.WriteFlappingMetrics  = (*Collector)(nil)
)

// newRecorderCollector builds an isolated collector for a single case: a
// fresh metrics.Set so one test's series never leak into another's scrape.
func newRecorderCollector() *Collector {
	return New(WithMetricsSet(metrics.NewSet()))
}

// scrapeSeries renders a collector's current state and parses it into a
// map keyed by the full series string (metric name plus its label set),
// which is exactly what a Prometheus scraper keys on.
func scrapeSeries(t *testing.T, c *Collector) map[string]float64 {
	t.Helper()

	var buf bytes.Buffer
	c.WritePrometheus(&buf)

	return parseSeries(t, buf.String())
}

// parseSeries turns Prometheus exposition text into series -> value.
// Each non-comment line is "<name>{labels} <value>"; the value is always
// the last whitespace-delimited token, so splitting on the final space
// handles both labelled and unlabelled series without a full grammar.
func parseSeries(t *testing.T, text string) map[string]float64 {
	t.Helper()

	out := make(map[string]float64)
	for line := range strings.SplitSeq(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		idx := strings.LastIndex(line, " ")
		require.Greater(t, idx, 0, "malformed prometheus line: %q", line)

		name := line[:idx]
		val, err := strconv.ParseFloat(line[idx+1:], 64)
		require.NoError(t, err, "value in line %q", line)

		out[name] = val
	}

	return out
}

// assertOnlyExpectedSeriesChanged is the cross-check the recorder table
// relies on: expected pins the series that must land at a specific value,
// allowedPrefixes exempts a bounded family from the check (a histogram's
// own bucket lines, which a single Observe legitimately moves several of),
// and everything else in the union of before and after must be bit-for-bit
// unchanged.
// A recorder wired to the wrong field, or one that also nudges a second
// field, shows up here as an unexpected change to a series this case did
// not name.
// Its boundary: it only ever scrapes c.set, so a recorder that wrote to the
// package-level default registry via the free-standing
// metrics.GetOrCreateCounter — same name and signature as the c.set. method
// form — in addition to its own field would not be caught here.
// A recorder that wrote only to the package-level registry, substituting
// it for c.set entirely, is still caught: the expected series would go
// missing from this collector's own scrape.
func assertOnlyExpectedSeriesChanged(
	t *testing.T,
	before, after map[string]float64,
	expected map[string]float64,
	allowedPrefixes ...string,
) {
	t.Helper()

	for series, want := range expected {
		got, ok := after[series]
		require.True(t, ok, "expected series %q not present after the call", series)
		require.InDelta(t, want, got, 1e-9, "series %q", series)
	}

	all := make(map[string]struct{}, len(before)+len(after))
	for k := range before {
		all[k] = struct{}{}
	}
	for k := range after {
		all[k] = struct{}{}
	}

	for k := range all {
		if _, ok := expected[k]; ok {
			continue
		}

		allowed := false
		for _, p := range allowedPrefixes {
			if strings.HasPrefix(k, p) {
				allowed = true

				break
			}
		}
		if allowed {
			continue
		}

		require.InDelta(t, before[k], after[k], 1e-9,
			"series %q changed but was not named by this case (before=%v after=%v)", k, before[k], after[k])
	}
}

// clusterName maps a ClusterID to the label value New's default
// types.DefaultClusterNames() gives it ("A", "B"). Every helper below that
// builds a series string against a freshly built collector funnels through
// this one place, instead of re-deriving the if/else independently.
func clusterName(cluster types.ClusterID) string {
	if cluster == types.ClusterB {
		return "B"
	}

	return "A"
}

// clusterSeries formats a "<metric>{cluster=\"<name>\"}" series string.
func clusterSeries(metric string, cluster types.ClusterID) string {
	return `helix_` + metric + `{cluster="` + clusterName(cluster) + `"}`
}

// clusterLabel returns the bare `cluster="A"` label fragment used to scope
// a bucket-family prefix to one cluster.
func clusterLabel(cluster types.ClusterID) string {
	return `cluster="` + clusterName(cluster) + `"`
}

// clusterDimSeries formats a series carrying both the cluster label and one
// extra string-valued dimension label, e.g.
// `helix_circuit_breaker_probe_total{cluster="A",outcome="committed"}`.
func clusterDimSeries(metric string, cluster types.ClusterID, dimKey, dimVal string) string {
	return `helix_` + metric + `{cluster="` + clusterName(cluster) + `",` + dimKey + `="` + dimVal + `"}`
}

// counterRecorderCase is a table row for the simple shape shared by most of
// the package: increment one cluster-scoped counter by exactly 1, leave
// every other series untouched.
// It covers both the counters New() pre-creates and the lazily created
// ones (IncReplayCorrupt, IncReplayTermFailed, IncWriteFlapping all call
// metrics.Set.GetOrCreateCounter directly instead of using a struct
// field), since both shapes are asserted identically here.
type counterRecorderCase struct {
	name   string
	metric string // series name without the cluster label, e.g. "read_total"
	call   func(c *Collector, cluster types.ClusterID)
}

// clusterCounterRecorderCases is the table for finding S4 asks for: every
// cluster-scoped "Inc*" recorder, called once on each cluster, with the
// cross-check confirming nothing else in the set moved.
// It is package-level, not local to TestCollector_ClusterCounterRecorders,
// so TestCollector_RecorderTablesCoverEveryRecorder can check its names
// against reflection without a second, independently maintained list.
// Add a row here for any new cluster-scoped counter recorder.
var clusterCounterRecorderCases = []counterRecorderCase{
	{"IncReadTotal", "read_total", func(c *Collector, cl types.ClusterID) { c.IncReadTotal(cl) }},
	{"IncReadError", "read_errors_total", func(c *Collector, cl types.ClusterID) { c.IncReadError(cl) }},
	{"IncReadDivergence", "read_divergence_total", func(c *Collector, cl types.ClusterID) { c.IncReadDivergence(cl) }},
	{"IncWriteTotal", "write_total", func(c *Collector, cl types.ClusterID) { c.IncWriteTotal(cl) }},
	{"IncWriteError", "write_errors_total", func(c *Collector, cl types.ClusterID) { c.IncWriteError(cl) }},
	{"IncWriteAsync", "write_async_total", func(c *Collector, cl types.ClusterID) { c.IncWriteAsync(cl) }},
	{"IncWriteDropped", "write_dropped_total", func(c *Collector, cl types.ClusterID) { c.IncWriteDropped(cl) }},
	{"IncWriteDegraded", "write_degraded_total", func(c *Collector, cl types.ClusterID) { c.IncWriteDegraded(cl) }},
	{"IncWriteRecovered", "write_recovered_total", func(c *Collector, cl types.ClusterID) { c.IncWriteRecovered(cl) }},
	{"IncCircuitBreakerTrip", "circuit_breaker_trips_total", func(c *Collector, cl types.ClusterID) { c.IncCircuitBreakerTrip(cl) }},
	{"IncReplayEnqueued", "replay_enqueued_total", func(c *Collector, cl types.ClusterID) { c.IncReplayEnqueued(cl) }},
	{"IncReplaySuccess", "replay_success_total", func(c *Collector, cl types.ClusterID) { c.IncReplaySuccess(cl) }},
	{"IncReplayError", "replay_errors_total", func(c *Collector, cl types.ClusterID) { c.IncReplayError(cl) }},
	{"IncReplayDropped", "replay_dropped_total", func(c *Collector, cl types.ClusterID) { c.IncReplayDropped(cl) }},
	{"IncDrainModeEntered", "drain_mode_entered_total", func(c *Collector, cl types.ClusterID) { c.IncDrainModeEntered(cl) }},
	{"IncDrainModeExited", "drain_mode_exited_total", func(c *Collector, cl types.ClusterID) { c.IncDrainModeExited(cl) }},
	{"IncSessionRefreshAttempt", "session_refresh_attempt_total", func(c *Collector, cl types.ClusterID) { c.IncSessionRefreshAttempt(cl) }},
	{"IncSessionRefreshSuccess", "session_refresh_success_total", func(c *Collector, cl types.ClusterID) { c.IncSessionRefreshSuccess(cl) }},
	{"IncSessionRefreshError", "session_refresh_error_total", func(c *Collector, cl types.ClusterID) { c.IncSessionRefreshError(cl) }},
	{"IncRecoveryProbeSuccess", "recovery_probe_success_total", func(c *Collector, cl types.ClusterID) { c.IncRecoveryProbeSuccess(cl) }},
	{"IncRecoveryProbeFailure", "recovery_probe_failure_total", func(c *Collector, cl types.ClusterID) { c.IncRecoveryProbeFailure(cl) }},
	{"IncWriteSkipped", "write_skipped_total", func(c *Collector, cl types.ClusterID) { c.IncWriteSkipped(cl) }},
	{"IncReadCallerExpired", "read_caller_expired_total", func(c *Collector, cl types.ClusterID) { c.IncReadCallerExpired(cl) }},
	{"IncWriteCallerExpired", "write_caller_expired_total", func(c *Collector, cl types.ClusterID) { c.IncWriteCallerExpired(cl) }},
	{"IncReplayCorrupt", "replay_corrupt_total", func(c *Collector, cl types.ClusterID) { c.IncReplayCorrupt(cl) }},
	{"IncReplayTermFailed", "replay_term_failed_total", func(c *Collector, cl types.ClusterID) { c.IncReplayTermFailed(cl) }},
	{"IncWriteFlapping", "write_flapping_total", func(c *Collector, cl types.ClusterID) { c.IncWriteFlapping(cl) }},
}

// TestCollector_ClusterCounterRecorders runs clusterCounterRecorderCases.
func TestCollector_ClusterCounterRecorders(t *testing.T) {
	for _, tt := range clusterCounterRecorderCases {
		t.Run(tt.name, func(t *testing.T) {
			for _, cluster := range []types.ClusterID{types.ClusterA, types.ClusterB} {
				t.Run(string(cluster), func(t *testing.T) {
					c := newRecorderCollector()
					before := scrapeSeries(t, c)

					tt.call(c, cluster)

					after := scrapeSeries(t, c)
					series := clusterSeries(tt.metric, cluster)
					assertOnlyExpectedSeriesChanged(t, before, after, map[string]float64{series: 1})
				})
			}
		})
	}
}

// TestCollector_StringDimensionRecorders covers the two recorders that
// take a free-form string dimension in addition to the cluster: two
// distinct values must land as two distinct series rather than merging
// into one, which is the failure mode a shared-field regression would
// produce.
// Both clusters are covered for each recorder, since the dimension and the
// cluster branch are independent failure surfaces.
var stringDimensionRecorderNames = []string{"IncCircuitBreakerProbe", "IncReplayWorkerDropped"}

func TestCollector_StringDimensionRecorders(t *testing.T) {
	t.Run("IncCircuitBreakerProbe", func(t *testing.T) {
		for _, cluster := range []types.ClusterID{types.ClusterA, types.ClusterB} {
			t.Run(string(cluster), func(t *testing.T) {
				c := newRecorderCollector()
				before := scrapeSeries(t, c)

				c.IncCircuitBreakerProbe(cluster, "committed")
				c.IncCircuitBreakerProbe(cluster, "abandoned")

				after := scrapeSeries(t, c)
				assertOnlyExpectedSeriesChanged(t, before, after, map[string]float64{
					clusterDimSeries("circuit_breaker_probe_total", cluster, "outcome", "committed"): 1,
					clusterDimSeries("circuit_breaker_probe_total", cluster, "outcome", "abandoned"): 1,
				})
			})
		}
	})

	t.Run("IncReplayWorkerDropped", func(t *testing.T) {
		for _, cluster := range []types.ClusterID{types.ClusterA, types.ClusterB} {
			t.Run(string(cluster), func(t *testing.T) {
				c := newRecorderCollector()
				before := scrapeSeries(t, c)

				c.IncReplayWorkerDropped(cluster, "queue_full")
				c.IncReplayWorkerDropped(cluster, "stopped")

				after := scrapeSeries(t, c)
				assertOnlyExpectedSeriesChanged(t, before, after, map[string]float64{
					clusterDimSeries("replay_worker_dropped_total", cluster, "reason", "queue_full"): 1,
					clusterDimSeries("replay_worker_dropped_total", cluster, "reason", "stopped"):    1,
				})
			})
		}
	})
}

// TestCollector_ClusterBoolGaugeRecorders covers the three cluster-scoped
// gauges that take a bool: each must be asserted in both directions, since
// a recorder that only ever sets 1 (or only ever sets 0) would still pass
// a single-direction check.
type boolGaugeRecorderCase struct {
	name   string
	metric string
	call   func(c *Collector, cluster types.ClusterID, on bool)
}

var clusterBoolGaugeRecorderCases = []boolGaugeRecorderCase{
	{"SetReadPreferred", "read_preferred", func(c *Collector, cl types.ClusterID, on bool) { c.SetReadPreferred(cl, on) }},
	{"SetWriteDegraded", "write_degraded", func(c *Collector, cl types.ClusterID, on bool) { c.SetWriteDegraded(cl, on) }},
	{"SetClusterDraining", "cluster_draining", func(c *Collector, cl types.ClusterID, on bool) { c.SetClusterDraining(cl, on) }},
}

func TestCollector_ClusterBoolGaugeRecorders(t *testing.T) {
	for _, tt := range clusterBoolGaugeRecorderCases {
		t.Run(tt.name, func(t *testing.T) {
			for _, cluster := range []types.ClusterID{types.ClusterA, types.ClusterB} {
				t.Run(string(cluster), func(t *testing.T) {
					c := newRecorderCollector()
					series := clusterSeries(tt.metric, cluster)
					baseline := scrapeSeries(t, c)

					tt.call(c, cluster, true)
					onState := scrapeSeries(t, c)
					assertOnlyExpectedSeriesChanged(t, baseline, onState, map[string]float64{series: 1})

					tt.call(c, cluster, false)
					offState := scrapeSeries(t, c)
					assertOnlyExpectedSeriesChanged(t, onState, offState, map[string]float64{series: 0})
				})
			}
		})
	}
}

// TestCollector_ClusterValueGaugeRecorders covers the cluster-scoped
// gauges that take a non-bool value.
var clusterValueGaugeRecorderNames = []string{
	"SetCircuitBreakerState", "SetReplayQueueDepth", "SetReplayOldestAge",
}

func TestCollector_ClusterValueGaugeRecorders(t *testing.T) {
	t.Run("SetCircuitBreakerState", func(t *testing.T) {
		for _, cluster := range []types.ClusterID{types.ClusterA, types.ClusterB} {
			t.Run(string(cluster), func(t *testing.T) {
				c := newRecorderCollector()
				before := scrapeSeries(t, c)
				c.SetCircuitBreakerState(cluster, 2)
				after := scrapeSeries(t, c)
				assertOnlyExpectedSeriesChanged(t, before, after,
					map[string]float64{clusterSeries("circuit_breaker_state", cluster): 2})
			})
		}
	})

	t.Run("SetReplayQueueDepth", func(t *testing.T) {
		for _, cluster := range []types.ClusterID{types.ClusterA, types.ClusterB} {
			t.Run(string(cluster), func(t *testing.T) {
				c := newRecorderCollector()
				before := scrapeSeries(t, c)
				c.SetReplayQueueDepth(cluster, 5)
				after := scrapeSeries(t, c)
				assertOnlyExpectedSeriesChanged(t, before, after,
					map[string]float64{clusterSeries("replay_queue_depth", cluster): 5})
			})
		}
	})

	t.Run("SetReplayOldestAge", func(t *testing.T) {
		for _, cluster := range []types.ClusterID{types.ClusterA, types.ClusterB} {
			t.Run(string(cluster), func(t *testing.T) {
				c := newRecorderCollector()
				before := scrapeSeries(t, c)
				c.SetReplayOldestAge(cluster, 12.5)
				after := scrapeSeries(t, c)
				assertOnlyExpectedSeriesChanged(t, before, after,
					map[string]float64{clusterSeries("replay_oldest_age_seconds", cluster): 12.5})
			})
		}
	})
}

// TestCollector_NonClusterCounterRecorders covers the mirror counters that
// take no cluster argument at all.
type simpleCounterRecorderCase struct {
	name   string
	series string
	call   func(c *Collector)
}

var nonClusterCounterRecorderCases = []simpleCounterRecorderCase{
	{"IncMirrorEnqueueSuccess", `helix_mirror_enqueue_success_total`, func(c *Collector) { c.IncMirrorEnqueueSuccess() }},
	{"IncMirrorEnqueueDropped", `helix_mirror_enqueue_dropped_total`, func(c *Collector) { c.IncMirrorEnqueueDropped() }},
	{"IncMirrorExecSuccess", `helix_mirror_exec_success_total`, func(c *Collector) { c.IncMirrorExecSuccess() }},
	{"IncMirrorExecError", `helix_mirror_exec_errors_total`, func(c *Collector) { c.IncMirrorExecError() }},
	{"IncMirrorReplayDropped", `helix_mirror_replay_dropped_total`, func(c *Collector) { c.IncMirrorReplayDropped() }},
}

func TestCollector_NonClusterCounterRecorders(t *testing.T) {
	for _, tt := range nonClusterCounterRecorderCases {
		t.Run(tt.name, func(t *testing.T) {
			c := newRecorderCollector()
			before := scrapeSeries(t, c)
			tt.call(c)
			after := scrapeSeries(t, c)
			assertOnlyExpectedSeriesChanged(t, before, after, map[string]float64{tt.series: 1})
		})
	}
}

// TestCollector_NonClusterGaugeRecorders covers the two mirror gauges that
// take no cluster argument.
var nonClusterGaugeRecorderNames = []string{"SetMirrorQueueDepth", "SetMirrorEnabled"}

func TestCollector_NonClusterGaugeRecorders(t *testing.T) {
	t.Run("SetMirrorQueueDepth", func(t *testing.T) {
		c := newRecorderCollector()
		before := scrapeSeries(t, c)
		c.SetMirrorQueueDepth(9)
		after := scrapeSeries(t, c)
		assertOnlyExpectedSeriesChanged(t, before, after, map[string]float64{`helix_mirror_queue_depth`: 9})
	})

	t.Run("SetMirrorEnabled", func(t *testing.T) {
		c := newRecorderCollector()
		baseline := scrapeSeries(t, c)

		c.SetMirrorEnabled(true)
		onState := scrapeSeries(t, c)
		assertOnlyExpectedSeriesChanged(t, baseline, onState, map[string]float64{`helix_mirror_enabled`: 1})

		c.SetMirrorEnabled(false)
		offState := scrapeSeries(t, c)
		assertOnlyExpectedSeriesChanged(t, onState, offState, map[string]float64{`helix_mirror_enabled`: 0})
	})
}

// TestCollector_HistogramRecorders covers the four duration histograms.
// Observing a duration legitimately moves several bucket series at once
// (every le upper bound at or above the sample), so the cross-check
// exempts this metric's own bucket family via allowedPrefixes and asserts
// only the _count and _sum series exactly, which is what the finding asks
// for.
type histogramRecorderCase struct {
	name string
	base string
	call func(c *Collector, cluster types.ClusterID, seconds float64)
}

var histogramClusterRecorderCases = []histogramRecorderCase{
	{"ObserveReadDuration", "read_duration_seconds", func(c *Collector, cl types.ClusterID, s float64) { c.ObserveReadDuration(cl, s) }},
	{"ObserveWriteDuration", "write_duration_seconds", func(c *Collector, cl types.ClusterID, s float64) { c.ObserveWriteDuration(cl, s) }},
	{"ObserveReplayDuration", "replay_duration_seconds", func(c *Collector, cl types.ClusterID, s float64) { c.ObserveReplayDuration(cl, s) }},
}

func TestCollector_HistogramRecorders(t *testing.T) {
	for _, tt := range histogramClusterRecorderCases {
		t.Run(tt.name, func(t *testing.T) {
			for _, cluster := range []types.ClusterID{types.ClusterA, types.ClusterB} {
				t.Run(string(cluster), func(t *testing.T) {
					c := newRecorderCollector()
					before := scrapeSeries(t, c)

					tt.call(c, cluster, 0.2)

					after := scrapeSeries(t, c)
					countSeries := clusterSeries(tt.base+"_count", cluster)
					sumSeries := clusterSeries(tt.base+"_sum", cluster)
					bucketPrefix := `helix_` + tt.base + `_bucket{` + clusterLabel(cluster)
					assertOnlyExpectedSeriesChanged(t, before, after,
						map[string]float64{countSeries: 1, sumSeries: 0.2}, bucketPrefix)
				})
			}
		})
	}

	t.Run("ObserveMirrorExecDuration", func(t *testing.T) {
		c := newRecorderCollector()
		before := scrapeSeries(t, c)

		c.ObserveMirrorExecDuration(0.03)

		after := scrapeSeries(t, c)
		assertOnlyExpectedSeriesChanged(t, before, after,
			map[string]float64{
				`helix_mirror_exec_duration_seconds_count`: 1,
				`helix_mirror_exec_duration_seconds_sum`:   0.03,
			},
			`helix_mirror_exec_duration_seconds_bucket{`)
	})
}

// TestCollector_AddRecorders covers the three "Add" recorders, whose live
// defect class (per docs/plans/test-weakness-analysis.md S4) is being
// downgraded to an Inc that only ever adds 1. n=7 makes that distinguishable
// from a hardcoded 1.
type addRecorderCase struct {
	name   string
	series string
	call   func(c *Collector, n int)
}

var addRecorderCases = []addRecorderCase{
	{"AddReplayEvicted", `helix_replay_evicted_total`, func(c *Collector, n int) { c.AddReplayEvicted(n) }},
	{"AddMirrorDrainDropped", `helix_mirror_drain_dropped_total`, func(c *Collector, n int) { c.AddMirrorDrainDropped(n) }},
	{"AddClusterEventsDropped", `helix_cluster_events_dropped_total`, func(c *Collector, n int) { c.AddClusterEventsDropped(n) }},
}

func TestCollector_AddRecorders(t *testing.T) {
	for _, tt := range addRecorderCases {
		t.Run(tt.name, func(t *testing.T) {
			c := newRecorderCollector()
			before := scrapeSeries(t, c)
			tt.call(c, 7)
			after := scrapeSeries(t, c)
			assertOnlyExpectedSeriesChanged(t, before, after, map[string]float64{tt.series: 7})
		})
	}
}

// TestCollector_IncFailoverTotal covers the one recorder that takes two
// cluster arguments: the from/to pair selects between two disjoint
// counters, so both directions must land on their own series.
func TestCollector_IncFailoverTotal(t *testing.T) {
	t.Run("A to B", func(t *testing.T) {
		c := newRecorderCollector()
		before := scrapeSeries(t, c)
		c.IncFailoverTotal(types.ClusterA, types.ClusterB)
		after := scrapeSeries(t, c)
		assertOnlyExpectedSeriesChanged(t, before, after,
			map[string]float64{`helix_failover_total{from="A",to="B"}`: 1})
	})

	t.Run("B to A", func(t *testing.T) {
		c := newRecorderCollector()
		before := scrapeSeries(t, c)
		c.IncFailoverTotal(types.ClusterB, types.ClusterA)
		after := scrapeSeries(t, c)
		assertOnlyExpectedSeriesChanged(t, before, after,
			map[string]float64{`helix_failover_total{from="B",to="A"}`: 1})
	})
}

// TestCollector_WithPrefixRenamesEveryEmittedSeries shows what a scrape
// actually keys on: every series carries the configured prefix, not just
// one sampled metric.
//
// Seven recorders build their series name with fmt.Sprintf against
// c.prefix at call time instead of going through a struct field New()
// pre-created (IncCircuitBreakerProbe, IncReplayWorkerDropped,
// IncReplayCorrupt, IncReplayTermFailed, IncWriteFlapping,
// AddReplayEvicted, AddMirrorDrainDropped); their series simply do not
// exist in a scrape until called, so a hardcoded "helix" literal inside
// one of them cannot be caught unless the test calls it.
// All seven are called here for exactly that reason.
func TestCollector_WithPrefixRenamesEveryEmittedSeries(t *testing.T) {
	c := New(WithMetricsSet(metrics.NewSet()), WithPrefix("myapp"))
	c.IncReadTotal(types.ClusterA)
	c.IncWriteTotal(types.ClusterB)
	c.IncMirrorEnqueueSuccess()
	c.IncCircuitBreakerProbe(types.ClusterA, "committed")
	c.IncReplayWorkerDropped(types.ClusterA, "queue_full")
	c.IncReplayCorrupt(types.ClusterA)
	c.IncReplayTermFailed(types.ClusterA)
	c.IncWriteFlapping(types.ClusterA)
	c.AddReplayEvicted(1)
	c.AddMirrorDrainDropped(1)

	series := scrapeSeries(t, c)
	require.NotEmpty(t, series)
	for name := range series {
		require.True(t, strings.HasPrefix(name, "myapp_"),
			"series %q must carry the configured prefix, not the default", name)
		require.False(t, strings.HasPrefix(name, "helix_"),
			"default prefix must not leak through when overridden: %q", name)
	}
}

// TestCollector_WithClusterNamesChangesLabelValues shows the other option
// a scrape keys on: the cluster label value, not just the metric name.
// IncReplayCorrupt is included because it is one of the lazy recorders
// that builds its label with c.clusterNames.Name(cluster) at call time
// (vm.go:750) rather than through a field New() pre-created with the
// configured names baked in; a hardcoded "A"/"B" there needs its own
// cluster-scoped call to surface.
func TestCollector_WithClusterNamesChangesLabelValues(t *testing.T) {
	c := New(WithMetricsSet(metrics.NewSet()), WithClusterNames(types.ClusterNames{A: "us_east", B: "us_west"}))
	c.IncReadTotal(types.ClusterA)
	c.IncReadTotal(types.ClusterB)
	c.IncReplayCorrupt(types.ClusterA)
	c.IncReplayCorrupt(types.ClusterB)

	series := scrapeSeries(t, c)
	require.Contains(t, series, `helix_read_total{cluster="us_east"}`)
	require.Contains(t, series, `helix_read_total{cluster="us_west"}`)
	require.NotContains(t, series, `helix_read_total{cluster="A"}`)
	require.NotContains(t, series, `helix_read_total{cluster="B"}`)
	require.Contains(t, series, `helix_replay_corrupt_total{cluster="us_east"}`)
	require.Contains(t, series, `helix_replay_corrupt_total{cluster="us_west"}`)
	require.NotContains(t, series, `helix_replay_corrupt_total{cluster="A"}`)
	require.NotContains(t, series, `helix_replay_corrupt_total{cluster="B"}`)
}

// TestCollector_NewRegistersGloballyWhenNoSetProvided covers the branch
// every real caller of New (without WithMetricsSet) actually takes:
// New builds its own metrics.Set and registers it with the package-level
// default set via metrics.RegisterSet, so the collector's series show up
// on the global metrics.WritePrometheus a caller wires to "/metrics"
// without the caller ever touching a *metrics.Set itself.
//
// This test cannot use WithMetricsSet (that is the branch under test), so
// it scrapes the process-wide registry instead of an isolated one; a
// prefix unique to this test keeps its series from colliding with any
// other collector's. The t.Cleanup unregister is not just hygiene here:
// metrics.WritePrometheus concatenates every registered set into one
// stream, and parseSeries' map assignment means a second collector
// created later with the same series name would silently overwrite this
// one's value on any later scrape sharing the process registry, so the
// set must be torn down before another test can register the same names.
func TestCollector_NewRegistersGloballyWhenNoSetProvided(t *testing.T) {
	c := New(WithPrefix("helix_global_registration_test"))
	t.Cleanup(func() { metrics.UnregisterSet(c.Set(), true) })

	c.IncReadTotal(types.ClusterA)

	var buf bytes.Buffer
	metrics.WritePrometheus(&buf, false)
	require.Contains(t, buf.String(), `helix_global_registration_test_read_total{cluster="A"} 1`)
}

// recorderMethodPattern matches the four verb prefixes every recorder in
// this package uses (IncReadTotal, SetReadPreferred, AddReplayEvicted,
// ObserveReadDuration). It deliberately also matches Collector.Set, the
// *metrics.Set accessor, which testedRecorderNames excludes by exact name
// since it is a recorder table's namesake shape without being a recorder.
var recorderMethodPattern = regexp.MustCompile(`^(Inc|Set|Add|Observe)[A-Z]`)

// testedRecorderNames unions every table and standalone-name list this file
// defines, which is exactly the set of recorder names a table-driven test
// somewhere in this file exercises.
// It is built from the tables themselves, not typed out a second time, so
// adding a row to a table also registers it here for free; only a
// genuinely new recorder — one with no row anywhere — is missing.
func testedRecorderNames() map[string]bool {
	names := make(map[string]bool)
	for _, c := range clusterCounterRecorderCases {
		names[c.name] = true
	}
	for _, n := range stringDimensionRecorderNames {
		names[n] = true
	}
	for _, c := range clusterBoolGaugeRecorderCases {
		names[c.name] = true
	}
	for _, n := range clusterValueGaugeRecorderNames {
		names[n] = true
	}
	for _, c := range nonClusterCounterRecorderCases {
		names[c.name] = true
	}
	for _, n := range nonClusterGaugeRecorderNames {
		names[n] = true
	}
	for _, c := range histogramClusterRecorderCases {
		names[c.name] = true
	}
	names["ObserveMirrorExecDuration"] = true
	for _, c := range addRecorderCases {
		names[c.name] = true
	}
	names["IncFailoverTotal"] = true

	return names
}

// TestCollector_RecorderTablesCoverEveryRecorder is the guard against
// table rot: it lists *Collector's exported Inc*/Set*/Add*/Observe*
// methods by reflection and fails by name for any that no table in this
// file exercises.
// Without this, a recorder added to vm.go without a matching table row
// would sit at 0% coverage silently — exactly the state finding S4 closed
// this file to fix.
func TestCollector_RecorderTablesCoverEveryRecorder(t *testing.T) {
	tested := testedRecorderNames()

	for method := range reflect.TypeFor[*Collector]().Methods() {
		name := method.Name
		if name == "Set" {
			continue // *metrics.Set accessor, covered by TestCollector_Set, not a recorder
		}
		if !recorderMethodPattern.MatchString(name) {
			continue
		}

		require.True(t, tested[name], "recorder %s has no row in any table", name)
	}
}

// TestCollector_Set confirms the accessor returns the exact set the
// collector was built with, so a caller-supplied set (WithMetricsSet) is
// the one that ends up registered, not a copy or a fresh one.
func TestCollector_Set(t *testing.T) {
	set := metrics.NewSet()
	c := New(WithMetricsSet(set))
	require.Same(t, set, c.Set())
}

// TestCollector_Handler confirms Handler exposes the same content
// WritePrometheus does, through the http.HandlerFunc shape callers wire to
// their mux.
func TestCollector_Handler(t *testing.T) {
	c := newRecorderCollector()
	c.IncReadTotal(types.ClusterA)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	c.Handler(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `helix_read_total{cluster="A"} 1`)
}
