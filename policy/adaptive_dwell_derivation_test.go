package policy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/types"
)

// TestAdaptiveDualWrite_DwellDerivation pins what each way of configuring the
// degraded dwell leaves behind.
// A minimum dwell on its own has to enable the re-degrade backoff: with
// redegradeWindow at zero the backoff never doubles a dwell, so neither
// write_flapping_total nor EventWriteFlapping can fire and a caller who set
// only the minimum silently lost the whole subsystem.
func TestAdaptiveDualWrite_DwellDerivation(t *testing.T) {
	tests := []struct {
		name        string
		opts        []AdaptiveDualWriteOption
		minDwell    time.Duration
		maxDwell    time.Duration
		window      time.Duration
		checkedFail bool
	}{
		{
			name: "no dwell leaves the backoff off",
		},
		{
			name:     "a minimum dwell alone derives the cap and the window",
			opts:     []AdaptiveDualWriteOption{WithAdaptiveMinDegradedDwell(time.Minute)},
			minDwell: time.Minute,
			maxDwell: 4 * time.Minute,
			window:   4 * time.Minute,
		},
		{
			name: "an explicit cap and window are kept as given",
			opts: []AdaptiveDualWriteOption{
				WithAdaptiveMinDegradedDwell(10 * time.Second),
				WithAdaptiveRedegradeBackoff(time.Hour, 40*time.Second),
			},
			minDwell: 10 * time.Second,
			maxDwell: 40 * time.Second,
			window:   time.Hour,
		},
		{
			name: "an unset cap is derived while the window is kept",
			opts: []AdaptiveDualWriteOption{
				WithAdaptiveMinDegradedDwell(10 * time.Second),
				WithAdaptiveRedegradeBackoff(time.Hour, 0),
			},
			minDwell: 10 * time.Second,
			maxDwell: 40 * time.Second,
			window:   time.Hour,
		},
		{
			name:        "a backoff with no dwell to double stays off",
			opts:        []AdaptiveDualWriteOption{WithAdaptiveRedegradeBackoff(time.Hour, time.Minute)},
			checkedFail: true,
		},
		{
			name: "a cap below the dwell is dropped and derived instead",
			opts: []AdaptiveDualWriteOption{
				WithAdaptiveMinDegradedDwell(time.Minute),
				WithAdaptiveRedegradeBackoff(time.Hour, time.Second),
			},
			minDwell:    time.Minute,
			maxDwell:    4 * time.Minute,
			window:      4 * time.Minute,
			checkedFail: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			legacy := NewAdaptiveDualWrite(tt.opts...)
			require.Equal(t, tt.minDwell, legacy.minDegradedDwell)
			require.Equal(t, tt.maxDwell, legacy.maxDegradedDwell)
			require.Equal(t, tt.window, legacy.redegradeWindow)

			// Both constructors must agree: the checked one never runs the
			// legacy normaliser, so a derivation placed there alone would
			// leave it with the subsystem off.
			checked, err := NewAdaptiveDualWriteChecked(tt.opts...)
			if tt.checkedFail {
				require.Error(t, err)
				require.True(t, types.IsOptionError(err))

				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.minDwell, checked.minDegradedDwell)
			require.Equal(t, tt.maxDwell, checked.maxDegradedDwell)
			require.Equal(t, tt.window, checked.redegradeWindow)
		})
	}
}

// TestAdaptiveDualWrite_MinimumDwellAloneReportsFlapping drives the subsystem
// the derivation switches on: a cluster that degrades again inside the derived
// window doubles its dwell and reports the derived cap.
func TestAdaptiveDualWrite_MinimumDwellAloneReportsFlapping(t *testing.T) {
	clock := &manualClock{}
	em := &recordingEmitter{}
	a := newHysteresisStrategy(clock, WithAdaptiveMinDegradedDwell(10*time.Second))
	a.SetEventEmitter(em)

	recoverAfter := func(d time.Duration) {
		clock.advance(d)
		a.recordFast(&a.stateA)
		require.False(t, a.IsDegraded(types.ClusterA))
	}
	flappingEvents := func() int {
		n := 0
		for _, kind := range em.kinds() {
			if kind == types.EventWriteFlapping {
				n++
			}
		}

		return n
	}

	degradeByStrikes(a, types.ClusterA)
	require.Equal(t, 10*time.Second, a.stateA.dwell)
	recoverAfter(10 * time.Second)

	clock.advance(time.Second)
	degradeByStrikes(a, types.ClusterA) // first re-degrade inside the derived 40s window
	require.Equal(t, 20*time.Second, a.stateA.dwell)
	require.Zero(t, flappingEvents())
	recoverAfter(20 * time.Second)

	clock.advance(time.Second)
	degradeByStrikes(a, types.ClusterA) // second: the derived 40s cap
	require.Equal(t, 40*time.Second, a.stateA.dwell)
	require.Equal(t, 1, flappingEvents(),
		"a minimum dwell alone must be able to report flapping")
}
