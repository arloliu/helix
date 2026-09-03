package policy

import (
	"errors"
	"testing"
	"time"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// manualClock drives the strategy's hysteresis clock from the test.
type manualClock struct{ nanos int64 }

func (c *manualClock) now() int64              { return c.nanos }
func (c *manualClock) advance(d time.Duration) { c.nanos += int64(d) }

func newHysteresisStrategy(clock *manualClock, opts ...AdaptiveDualWriteOption) *AdaptiveDualWrite {
	a := NewAdaptiveDualWrite(append([]AdaptiveDualWriteOption{
		WithAdaptiveStrikeThreshold(1),
		WithAdaptiveRecoveryThreshold(1),
	}, opts...)...)
	a.now = clock.now

	return a
}

func TestAdaptiveDualWrite_ProbeLatencyCreditsOnlyFastProbes(t *testing.T) {
	a := NewAdaptiveDualWrite(
		WithAdaptiveStrikeThreshold(1),
		WithAdaptiveRecoveryThreshold(1),
		WithAdaptiveAbsoluteMax(time.Second),
		WithAdaptiveDeltaThreshold(100*time.Millisecond),
		WithAdaptiveMinFloor(50*time.Millisecond),
	)
	degradeByStrikes(a, types.ClusterA)

	// No sibling baseline: only a probe under the minimum floor counts.
	a.RecordProbeLatency(types.ClusterA, 80*time.Millisecond)
	require.True(t, a.IsDegraded(types.ClusterA), "a probe above the floor earns nothing without a sibling baseline")
	a.RecordProbeLatency(types.ClusterA, 10*time.Millisecond)
	require.False(t, a.IsDegraded(types.ClusterA))

	// With a sibling baseline the probe must be within the delta threshold.
	degradeByStrikes(a, types.ClusterA)
	a.stateB.lastLatency.Store(int64(20 * time.Millisecond))
	a.RecordProbeLatency(types.ClusterA, 500*time.Millisecond)
	require.True(t, a.IsDegraded(types.ClusterA), "a probe far slower than the sibling earns nothing")
	a.RecordProbeLatency(types.ClusterA, 2*time.Second)
	require.True(t, a.IsDegraded(types.ClusterA), "a probe over the absolute cap earns nothing")
	a.RecordProbeLatency(types.ClusterA, 90*time.Millisecond)
	require.False(t, a.IsDegraded(types.ClusterA))
	require.Zero(t, a.stateA.lastLatency.Load(), "a probe is not a write latency sample")

	a.RecordProbeLatency("unknown", time.Millisecond) // ignored
}

func TestAdaptiveDualWrite_MinDegradedDwellHoldsRecovery(t *testing.T) {
	clock := &manualClock{}
	a := newHysteresisStrategy(clock, WithAdaptiveMinDegradedDwell(time.Minute))
	degradeByStrikes(a, types.ClusterA)

	for range 3 {
		a.recordFast(&a.stateA)
	}
	require.True(t, a.IsDegraded(types.ClusterA), "recovery credit alone cannot end a span before the dwell")

	clock.advance(time.Minute)
	a.recordFast(&a.stateA)
	require.False(t, a.IsDegraded(types.ClusterA), "the first fast observation after the dwell recovers")
}

func TestAdaptiveDualWrite_RedegradeBackoffDoublesDwellAndReportsFlapping(t *testing.T) {
	clock := &manualClock{}
	em := &recordingEmitter{}
	a := newHysteresisStrategy(clock,
		WithAdaptiveMinDegradedDwell(10*time.Second),
		WithAdaptiveRedegradeBackoff(time.Hour, 40*time.Second),
	)
	a.SetEventEmitter(em)

	recoverAfter := func(d time.Duration) {
		clock.advance(d)
		a.recordFast(&a.stateA)
		require.False(t, a.IsDegraded(types.ClusterA))
	}
	degradeByStrikes(a, types.ClusterA)
	require.Equal(t, 10*time.Second, a.stateA.dwell)
	recoverAfter(10 * time.Second)

	clock.advance(time.Second)
	degradeByStrikes(a, types.ClusterA) // first re-degrade: 20 s
	require.Equal(t, 20*time.Second, a.stateA.dwell)
	require.NotContains(t, em.kinds(), types.EventWriteFlapping)
	recoverAfter(20 * time.Second)

	clock.advance(time.Second)
	degradeByStrikes(a, types.ClusterA) // second: 40 s, the cap
	require.Equal(t, 40*time.Second, a.stateA.dwell)
	flapping := 0
	for _, ev := range em.snapshot() {
		if ev.Kind == types.EventWriteFlapping {
			flapping++
			require.Equal(t, types.ClusterA, ev.Cluster)
			require.Equal(t, 2, ev.Count)
		}
	}
	require.Equal(t, 1, flapping, "the cap is reported once")
	recoverAfter(40 * time.Second)

	clock.advance(time.Second)
	degradeByStrikes(a, types.ClusterA) // third: still at the cap, no second event
	require.Equal(t, 40*time.Second, a.stateA.dwell)
	require.Equal(t, 1, countKind(em, types.EventWriteFlapping))
	recoverAfter(40 * time.Second)

	// A recovery that holds past the window resets the backoff.
	clock.advance(2 * time.Hour)
	degradeByStrikes(a, types.ClusterA)
	require.Equal(t, 10*time.Second, a.stateA.dwell)
	require.Zero(t, a.stateA.redegrades)
}

func TestAdaptiveDualWrite_ManualRecoveryResetsBackoff(t *testing.T) {
	clock := &manualClock{}
	a := newHysteresisStrategy(clock,
		WithAdaptiveMinDegradedDwell(10*time.Second),
		WithAdaptiveRedegradeBackoff(time.Hour, 40*time.Second),
	)
	degradeByStrikes(a, types.ClusterA)
	clock.advance(10 * time.Second)
	a.recordFast(&a.stateA)
	clock.advance(time.Second)
	degradeByStrikes(a, types.ClusterA)
	require.Equal(t, 20*time.Second, a.stateA.dwell)

	a.ForceRecover(types.ClusterA)
	clock.advance(time.Second)
	degradeByStrikes(a, types.ClusterA)
	require.Equal(t, 10*time.Second, a.stateA.dwell, "a manual recovery starts the backoff over")

	// ForceDegrade never counts as a re-degrade.
	a.ForceRecover(types.ClusterA)
	a.ForceDegrade(types.ClusterA)
	require.Zero(t, a.stateA.redegrades)
}

func TestAdaptiveDualWrite_HysteresisOptionValidation(t *testing.T) {
	_, err := NewAdaptiveDualWriteChecked(WithAdaptiveMinDegradedDwell(-time.Second))
	var optErr *types.OptionError
	require.True(t, errors.As(err, &optErr))
	require.Equal(t, "WithAdaptiveMinDegradedDwell", optErr.Option)

	_, err = NewAdaptiveDualWriteChecked(WithAdaptiveRedegradeBackoff(time.Hour, time.Minute))
	require.True(t, errors.As(err, &optErr))
	require.Equal(t, "WithAdaptiveRedegradeBackoff", optErr.Option, "backoff needs a dwell to double")

	_, err = NewAdaptiveDualWriteChecked(
		WithAdaptiveMinDegradedDwell(time.Minute),
		WithAdaptiveRedegradeBackoff(time.Hour, time.Second),
	)
	require.True(t, errors.As(err, &optErr))
	require.Equal(t, "WithAdaptiveRedegradeBackoff", optErr.Option, "the cap must not be below the dwell")

	a, err := NewAdaptiveDualWriteChecked(
		WithAdaptiveMinDegradedDwell(time.Minute),
		WithAdaptiveRedegradeBackoff(time.Hour, 8*time.Minute),
	)
	require.NoError(t, err)
	require.Equal(t, time.Hour, a.redegradeWindow)

	// The legacy constructor drops a backoff it cannot apply.
	legacy := NewAdaptiveDualWrite(WithAdaptiveRedegradeBackoff(time.Hour, time.Minute))
	require.Zero(t, legacy.redegradeWindow)
}

func countKind(em *recordingEmitter, kind types.ClusterEventKind) int {
	n := 0
	for _, k := range em.kinds() {
		if k == kind {
			n++
		}
	}

	return n
}
