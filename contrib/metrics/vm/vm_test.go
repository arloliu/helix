package vm

import (
	"bytes"
	"math"
	"testing"

	"github.com/VictoriaMetrics/metrics"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/types"
)

// Compile-time assertions that *Collector implements the optional
// types.AdaptiveWriteMetrics, types.MirrorReplayMetrics,
// types.ClusterEventMetrics, types.RecoveryProbeMetrics, and
// types.StrictMetrics interfaces. Placed in a _test.go file per the
// public-package assertion convention.
var (
	_ types.AdaptiveWriteMetrics = (*Collector)(nil)
	_ types.MirrorReplayMetrics  = (*Collector)(nil)
	_ types.ClusterEventMetrics  = (*Collector)(nil)
	_ types.RecoveryProbeMetrics = (*Collector)(nil)
	_ types.StrictMetrics        = (*Collector)(nil)
)

func TestCollector_DurationHistogramsUsePrometheusBuckets(t *testing.T) {
	c := New(WithMetricsSet(metrics.NewSet()))

	c.ObserveReadDuration(types.ClusterA, 0.004) // -> le="0.005"
	c.ObserveReadDuration(types.ClusterB, 0.004)
	c.ObserveWriteDuration(types.ClusterA, 0.2) // -> le="0.25"
	c.ObserveWriteDuration(types.ClusterB, 0.2)
	c.ObserveReplayDuration(types.ClusterA, 3.0) // -> le="5"
	c.ObserveReplayDuration(types.ClusterB, 3.0)
	c.ObserveMirrorExecDuration(0.03) // -> le="0.05"

	var buf bytes.Buffer
	c.WritePrometheus(&buf)
	out := buf.String()

	// All seven histograms, both cluster labels, classic le buckets.
	require.Contains(t, out, `helix_read_duration_seconds_bucket{cluster="A",le="0.005"} 1`)
	require.Contains(t, out, `helix_read_duration_seconds_bucket{cluster="B",le="0.005"} 1`)
	require.Contains(t, out, `helix_write_duration_seconds_bucket{cluster="A",le="0.25"} 1`)
	require.Contains(t, out, `helix_write_duration_seconds_bucket{cluster="B",le="0.25"} 1`)
	require.Contains(t, out, `helix_replay_duration_seconds_bucket{cluster="A",le="5"} 1`)
	require.Contains(t, out, `helix_replay_duration_seconds_bucket{cluster="B",le="5"} 1`)
	require.Contains(t, out, `helix_mirror_exec_duration_seconds_bucket{le="0.05"} 1`)
	require.Contains(t, out, `helix_read_duration_seconds_bucket{cluster="A",le="+Inf"} 1`)
	require.Contains(t, out, `helix_read_duration_seconds_sum`)
	require.Contains(t, out, `helix_read_duration_seconds_count`)
	require.NotContains(t, out, "vmrange", "VM-native buckets must be fully replaced")
}

func TestCollector_WithDurationBucketsOverride(t *testing.T) {
	c := New(
		WithMetricsSet(metrics.NewSet()),
		WithDurationBuckets([]float64{0.1, 1.0}),
	)
	c.ObserveReadDuration(types.ClusterA, 0.05)

	var buf bytes.Buffer
	c.WritePrometheus(&buf)
	out := buf.String()

	require.Contains(t, out, `le="0.1"`)
	require.NotContains(t, out, `le="0.005"`, "defaults must not appear when overridden")
}

// VM panics at construction on invalid bounds, so the option must reject
// (ignore) anything not strictly increasing, finite, and positive.
func TestCollector_WithDurationBucketsRejectsInvalidInput(t *testing.T) {
	cases := []struct {
		name    string
		buckets []float64
	}{
		{"empty", nil},
		{"duplicate adjacent", []float64{0.1, 0.1, 1}},
		{"descending", []float64{1, 0.1}},
		{"zero bound", []float64{0, 1}},
		{"negative bound", []float64{-1, 1}},
		{"NaN", []float64{0.1, math.NaN()}},
		{"+Inf", []float64{0.1, math.Inf(1)}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var c *Collector
			require.NotPanics(t, func() {
				c = New(
					WithMetricsSet(metrics.NewSet()),
					WithDurationBuckets(tt.buckets),
				)
			})
			c.ObserveReadDuration(types.ClusterA, 0.004)
			var buf bytes.Buffer
			c.WritePrometheus(&buf)
			require.Contains(t, buf.String(), `le="0.005"`,
				"invalid input must fall back to the canonical defaults")
		})
	}
}

// VM retains the slice it is given, so the collector must always pass a
// private copy: mutating the caller's slice after construction must not
// change live buckets.
func TestCollector_CallerBucketSliceIsNotAliased(t *testing.T) {
	caller := []float64{0.1, 1.0}
	c := New(WithMetricsSet(metrics.NewSet()), WithDurationBuckets(caller))
	caller[0] = 99 // mutate after construction
	c.ObserveReadDuration(types.ClusterA, 0.05)
	var buf bytes.Buffer
	c.WritePrometheus(&buf)
	require.Contains(t, buf.String(), `le="0.1"`, "mutation must not leak into live buckets")
}

// DefaultDurationBuckets is an accessor returning a fresh copy — callers
// cannot corrupt the canonical defaults for future collectors.
func TestDefaultDurationBuckets_ReturnsUnaliasedCopy(t *testing.T) {
	b1 := DefaultDurationBuckets()
	b1[0] = 99 // mutating the returned copy...
	c := New(WithMetricsSet(metrics.NewSet()))
	c.ObserveReadDuration(types.ClusterA, 0.0005)
	var buf bytes.Buffer
	c.WritePrometheus(&buf)
	require.Contains(t, buf.String(), `le="0.001"`,
		"...must not affect collectors built afterwards")

	b2 := DefaultDurationBuckets()
	require.NotEqual(t, 99.0, b2[0], "each call returns a fresh copy")
}

func TestDefaultDurationBuckets_StrictlyIncreasingAndFinite(t *testing.T) {
	b := DefaultDurationBuckets()
	require.NotEmpty(t, b)
	for i, v := range b {
		require.False(t, math.IsNaN(v) || math.IsInf(v, 0))
		require.Positive(t, v)
		if i > 0 {
			require.Greater(t, v, b[i-1])
		}
	}
}

// The adaptive-write, mirror-replay, and cluster-event metrics are
// pre-created at zero and recorded through the optional interface
// methods; the degraded gauge must follow the last SetWriteDegraded.
func TestCollector_AdaptiveMirrorAndClusterEventMetrics(t *testing.T) {
	c := New(WithMetricsSet(metrics.NewSet()))

	var buf bytes.Buffer
	c.WritePrometheus(&buf)
	out := buf.String()
	require.Contains(t, out, `helix_write_degraded{cluster="A"} 0`, "gauge must be pre-created at healthy")
	require.Contains(t, out, `helix_write_degraded_total{cluster="A"} 0`)
	require.Contains(t, out, `helix_write_recovered_total{cluster="B"} 0`)
	require.Contains(t, out, `helix_mirror_replay_dropped_total 0`)
	require.Contains(t, out, `helix_cluster_events_dropped_total 0`)

	c.SetWriteDegraded(types.ClusterA, true)
	c.IncWriteDegraded(types.ClusterA)
	c.IncWriteRecovered(types.ClusterB)
	c.IncMirrorReplayDropped()
	c.AddClusterEventsDropped(3)

	buf.Reset()
	c.WritePrometheus(&buf)
	out = buf.String()
	require.Contains(t, out, `helix_write_degraded{cluster="A"} 1`)
	require.Contains(t, out, `helix_write_degraded_total{cluster="A"} 1`)
	require.Contains(t, out, `helix_write_recovered_total{cluster="B"} 1`)
	require.Contains(t, out, `helix_mirror_replay_dropped_total 1`)
	require.Contains(t, out, `helix_cluster_events_dropped_total 3`,
		"AddClusterEventsDropped must add the full delta")

	c.SetWriteDegraded(types.ClusterA, false)
	buf.Reset()
	c.WritePrometheus(&buf)
	require.Contains(t, buf.String(), `helix_write_degraded{cluster="A"} 0`,
		"recovery must return the gauge to healthy")
}

func TestCollector_RecoveryProbeAndStrictWriteMetrics(t *testing.T) {
	c := New(WithMetricsSet(metrics.NewSet()))

	var buf bytes.Buffer
	c.WritePrometheus(&buf)
	out := buf.String()
	require.Contains(t, out, `helix_recovery_probe_success_total{cluster="A"} 0`,
		"counters must be pre-created so a scrape sees them before the first probe")
	require.Contains(t, out, `helix_recovery_probe_failure_total{cluster="B"} 0`)
	require.Contains(t, out, `helix_write_skipped_total{cluster="A"} 0`)

	c.IncRecoveryProbeSuccess(types.ClusterA)
	c.IncRecoveryProbeFailure(types.ClusterB)
	c.IncRecoveryProbeFailure(types.ClusterB)
	c.IncWriteSkipped(types.ClusterA)
	c.IncWriteSkipped(types.ClusterB)

	buf.Reset()
	c.WritePrometheus(&buf)
	out = buf.String()
	require.Contains(t, out, `helix_recovery_probe_success_total{cluster="A"} 1`)
	require.Contains(t, out, `helix_recovery_probe_success_total{cluster="B"} 0`,
		"a probe on one cluster must not credit the other")
	require.Contains(t, out, `helix_recovery_probe_failure_total{cluster="B"} 2`)
	require.Contains(t, out, `helix_recovery_probe_failure_total{cluster="A"} 0`)
	require.Contains(t, out, `helix_write_skipped_total{cluster="A"} 1`)
	require.Contains(t, out, `helix_write_skipped_total{cluster="B"} 1`)
	require.Contains(t, out, `helix_write_errors_total{cluster="A"} 0`,
		"a skip is an operational state, not a cluster write error")
}
