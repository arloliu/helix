package policy

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

const (
	walkAdaptiveAbsoluteMax = time.Second
	walkAdaptiveDelta       = 100 * time.Millisecond
	walkAdaptiveFloor       = 50 * time.Millisecond
	walkMinDwell            = 10 * time.Second
	walkMaxDwell            = 40 * time.Second
	// walkRedegradeWindow sits half a second off every whole second,
	// so no walk lands exactly on the window's edge,
	// which the Godoc leaves open ("within window" against "holds for longer than window").
	walkRedegradeWindow = 40*time.Second + 500*time.Millisecond
)

// walkAdaptiveAdvances land on the dwell's edge on purpose:
// the Godoc says a span lasts at least the dwell, so exactly the dwell may end it.
var walkAdaptiveAdvances = []time.Duration{time.Second, 5 * time.Second, 10 * time.Second, 40 * time.Second, 60 * time.Second}

// walkProbeLatencies straddle the floor, the delta threshold and the absolute cap.
var walkProbeLatencies = []time.Duration{time.Millisecond, 30 * time.Millisecond, 300 * time.Millisecond, 2 * time.Second}

var errWalkWrite = errors.New("walk: write failed")

// legShape is what one leg of a walk write does.
type legShape int

const (
	legFast      legShape = iota // 1ms, under the floor
	legDeltaSlow                 // 300ms, past the delta threshold and under the cap
	legCapSlow                   // 3s, past the absolute cap
	legError                     // fails
)

var legShapeNames = [...]string{"fast", "delta-slow", "cap-slow", "error"}

// adaptiveOp names what a step did, for the transition checks.
type adaptiveOp int

const (
	adaptiveOpWrite adaptiveOp = iota
	adaptiveOpFastWrite
	adaptiveOpProbe
	adaptiveOpForceDegrade
	adaptiveOpForceRecover
	adaptiveOpReset
	adaptiveOpAdvance
	adaptiveOpUnknown
)

// adaptiveSnap is one cluster's state, read under its mutex.
type adaptiveSnap struct {
	degraded    bool
	latched     bool
	slow        int32
	fast        int32
	dwell       time.Duration
	degradedAt  int64
	recoveredAt int64
	redegrades  int32
}

// adaptiveTally is test-side bookkeeping for one cluster.
// Both counts are upper bounds: they count every operation that could have produced a strike or a recovery credit,
// so a transition that needed more than the count allows is wrong whatever the operations actually did.
type adaptiveTally struct {
	strikes int // legs that could have struck since the slow-strike count was last certainly cleared
	credits int // operations that could have credited recovery since the current degraded span began
}

// adaptiveWalk drives an AdaptiveDualWrite in strict mode.
//
// ExecuteStrict runs every leg on the caller's goroutine or joins it before returning,
// so each step settles before the next; Execute's fire-and-forget legs would mutate state between steps.
// Recovery credit therefore comes from RecordFastWrite and RecordProbeLatency, the strict-mode recovery path.
type adaptiveWalk struct {
	w        *walk
	a        *AdaptiveDualWrite
	clock    *manualClock
	legs     *legClock
	mc       *testutil.TestMetricsCollector
	em       *recordingEmitter
	strike   int32
	recovery int32
	minDwell time.Duration
	tally    map[types.ClusterID]*adaptiveTally
	ops      []walkOp

	// Set by the operation a step ran.
	op     adaptiveOp
	target types.ClusterID
	shapes map[types.ClusterID]legShape
}

// TestAdaptiveDualWrite_RandomWalk drives an AdaptiveDualWrite through seeded random sequences of
// strict writes with fast, slow and failing legs, recovery credits, manual degrade, recover and reset,
// and hysteresis clock advances.
// Every step checks the degrade and recover rules, the dwell and its re-degrade backoff,
// the operator latch, and that the gauge and the events agree with the state.
func TestAdaptiveDualWrite_RandomWalk(t *testing.T) {
	runWalk(t, func(w *walk) walkMachine {
		strike := int32(1 + w.intn(3))
		recovery := int32(1 + w.intn(3))
		minDwell := walkMinDwell
		if w.chance(4) {
			minDwell = 0
		}
		clock := &manualClock{nanos: int64(time.Hour)}
		legs := &legClock{}
		mc := testutil.NewTestMetricsCollector()
		em := &recordingEmitter{}
		a := NewAdaptiveDualWrite(
			WithAdaptiveStrikeThreshold(int(strike)),
			WithAdaptiveRecoveryThreshold(int(recovery)),
			WithAdaptiveAbsoluteMax(walkAdaptiveAbsoluteMax),
			WithAdaptiveDeltaThreshold(walkAdaptiveDelta),
			WithAdaptiveMinFloor(walkAdaptiveFloor),
			WithAdaptiveMinDegradedDwell(minDwell),
			WithAdaptiveRedegradeBackoff(walkRedegradeWindow, walkMaxDwell),
			WithAdaptiveMetrics(mc),
		)
		a.now = clock.now
		a.latencyNow = legs.now
		a.SetEventEmitter(em)
		w.notef("new AdaptiveDualWrite strike=%d recovery=%d minDwell=%v", strike, recovery, minDwell)

		aw := &adaptiveWalk{
			w: w, a: a, clock: clock, legs: legs, mc: mc, em: em,
			strike: strike, recovery: recovery, minDwell: minDwell,
			tally: map[types.ClusterID]*adaptiveTally{types.ClusterA: {}, types.ClusterB: {}},
		}
		aw.ops = []walkOp{
			{weight: 40, run: aw.opWrite},
			{weight: 15, run: aw.opFastWrite},
			{weight: 10, run: aw.opProbe},
			{weight: 3, run: aw.opForceDegrade},
			{weight: 4, run: aw.opForceRecover},
			{weight: 1, run: aw.opReset},
			{weight: 20, run: aw.opAdvance},
			{weight: 2, run: aw.opUnknownCluster},
		}

		return aw
	})
}

func (aw *adaptiveWalk) step() {
	before := aw.snapAll()
	aw.w.run(aw.ops)
	after := aw.snapAll()
	for _, cluster := range []types.ClusterID{types.ClusterA, types.ClusterB} {
		aw.checkState(cluster, after[cluster])
		aw.checkTransition(cluster, before[cluster], after[cluster])
	}
}

func (aw *adaptiveWalk) finish() {}

func (aw *adaptiveWalk) snapAll() map[types.ClusterID]adaptiveSnap {
	return map[types.ClusterID]adaptiveSnap{
		types.ClusterA: aw.snap(&aw.a.stateA),
		types.ClusterB: aw.snap(&aw.a.stateB),
	}
}

func (aw *adaptiveWalk) snap(state *clusterWriteState) adaptiveSnap {
	state.mu.Lock()
	defer state.mu.Unlock()

	return adaptiveSnap{
		degraded:    state.isDegraded.Load(),
		latched:     state.latched.Load(),
		slow:        state.slowStrikes,
		fast:        state.fastStrikes,
		dwell:       state.dwell,
		degradedAt:  state.degradedAt,
		recoveredAt: state.recoveredAt,
		redegrades:  state.redegrades,
	}
}

func (aw *adaptiveWalk) begin(op adaptiveOp, target types.ClusterID) {
	aw.op, aw.target = op, target
	aw.shapes = nil
}

// leg returns a write that takes the shape's latency on the cluster's leg clock.
func (aw *adaptiveWalk) leg(cluster types.ClusterID, shape legShape) func(context.Context) error {
	latency := map[legShape]time.Duration{
		legFast: time.Millisecond, legDeltaSlow: 300 * time.Millisecond,
		legCapSlow: 3 * time.Second, legError: time.Millisecond,
	}[shape]

	return func(_ context.Context) error {
		aw.legs.advance(cluster, latency)
		if shape == legError {
			return errWalkWrite
		}

		return nil
	}
}

func (aw *adaptiveWalk) drawShape() legShape {
	if aw.w.chance(2) {
		return legFast
	}

	return legShape(1 + aw.w.intn(3))
}

func (aw *adaptiveWalk) opWrite() {
	aw.begin(adaptiveOpWrite, "")
	aw.shapes = map[types.ClusterID]legShape{types.ClusterA: aw.drawShape(), types.ClusterB: aw.drawShape()}
	aw.w.notef("ExecuteStrict(A %s, B %s)", legShapeNames[aw.shapes[types.ClusterA]], legShapeNames[aw.shapes[types.ClusterB]])

	healthy := map[types.ClusterID]bool{
		types.ClusterA: !aw.a.IsDegraded(types.ClusterA),
		types.ClusterB: !aw.a.IsDegraded(types.ClusterB),
	}
	errA, errB := aw.a.ExecuteStrict(aw.w.t.Context(),
		aw.leg(types.ClusterA, aw.shapes[types.ClusterA]), aw.leg(types.ClusterB, aw.shapes[types.ClusterB]))

	for cluster, err := range map[types.ClusterID]error{types.ClusterA: errA, types.ClusterB: errB} {
		if !healthy[cluster] {
			require.ErrorIs(aw.w.t, err, types.ErrClusterDegraded, "a degraded cluster's strict leg is skipped")

			continue
		}
		if aw.shapes[cluster] == legError {
			require.ErrorIs(aw.w.t, err, errWalkWrite)
			aw.tally[cluster].strikes++

			continue
		}
		require.NoError(aw.w.t, err)
		if aw.shapes[cluster] == legFast {
			aw.tally[cluster].strikes = 0
		} else {
			aw.tally[cluster].strikes++
		}
	}
}

func (aw *adaptiveWalk) opFastWrite() {
	cluster := aw.w.cluster()
	aw.begin(adaptiveOpFastWrite, cluster)
	aw.w.notef("RecordFastWrite(%s)", cluster)
	aw.a.RecordFastWrite(cluster)
	aw.tally[cluster].credits++
	aw.tally[cluster].strikes = 0
}

func (aw *adaptiveWalk) opProbe() {
	cluster := aw.w.cluster()
	latency := walkProbeLatencies[aw.w.intn(len(walkProbeLatencies))]
	aw.begin(adaptiveOpProbe, cluster)
	aw.w.notef("RecordProbeLatency(%s, %v)", cluster, latency)
	aw.a.RecordProbeLatency(cluster, latency)
	aw.tally[cluster].credits++
}

func (aw *adaptiveWalk) opForceDegrade() {
	cluster := aw.w.cluster()
	aw.begin(adaptiveOpForceDegrade, cluster)
	aw.w.notef("ForceDegrade(%s)", cluster)
	aw.a.ForceDegrade(cluster)
}

func (aw *adaptiveWalk) opForceRecover() {
	cluster := aw.w.cluster()
	aw.begin(adaptiveOpForceRecover, cluster)
	aw.w.notef("ForceRecover(%s)", cluster)
	aw.a.ForceRecover(cluster)
	*aw.tally[cluster] = adaptiveTally{}
}

func (aw *adaptiveWalk) opReset() {
	aw.begin(adaptiveOpReset, "")
	aw.w.notef("Reset()")
	aw.a.Reset()
	for _, tally := range aw.tally {
		*tally = adaptiveTally{}
	}
}

func (aw *adaptiveWalk) opAdvance() {
	d := walkAdaptiveAdvances[aw.w.intn(len(walkAdaptiveAdvances))]
	aw.begin(adaptiveOpAdvance, "")
	aw.w.notef("advance %v", d)
	aw.clock.advance(d)
}

func (aw *adaptiveWalk) opUnknownCluster() {
	t := aw.w.t
	aw.begin(adaptiveOpUnknown, "")
	aw.w.notef("operations on unknown cluster %s", unknownCluster)
	aw.a.ForceDegrade(unknownCluster)
	aw.a.RecordFastWrite(unknownCluster)
	aw.a.RecordProbeLatency(unknownCluster, time.Millisecond)
	require.False(t, aw.a.IsDegraded(unknownCluster), "an unknown cluster is never degraded")
	require.False(t, aw.a.IsLatched(unknownCluster), "an unknown cluster is never latched")
	aw.a.ForceRecover(unknownCluster)
}

// checkState checks what must hold of any single state, whatever led to it.
func (aw *adaptiveWalk) checkState(cluster types.ClusterID, s adaptiveSnap) {
	t := aw.w.t

	require.Equal(t, s.degraded, aw.a.IsDegraded(cluster), "IsDegraded(%s)", cluster)
	require.Equal(t, s.latched, aw.a.IsLatched(cluster), "IsLatched(%s)", cluster)
	require.False(t, s.latched && !s.degraded, "%s is latched but not degraded", cluster)
	if !s.degraded {
		require.Zero(t, s.fast, "a healthy %s holds recovery credit", cluster)
		require.Less(t, s.slow, aw.strike, "a healthy %s holds %d slow strikes, the threshold is %d", cluster, s.slow, aw.strike)
	} else {
		require.LessOrEqual(t, s.fast, aw.recovery, "a degraded %s holds more recovery credit than the threshold", cluster)
	}
	switch {
	case aw.minDwell == 0:
		require.Zero(t, s.dwell, "without a minimum dwell %s has no dwell", cluster)
	case s.degraded:
		require.GreaterOrEqual(t, s.dwell, aw.minDwell, "a degraded %s dwells less than the minimum", cluster)
		require.LessOrEqual(t, s.dwell, walkMaxDwell, "a degraded %s dwells past the cap", cluster)
	}

	require.Equal(t, s.degraded, aw.mc.WriteDegradedState[cluster], "degraded gauge of %s", cluster)
	degrades := countEvents(aw.em, types.EventWriteDegraded, cluster)
	recovers := countEvents(aw.em, types.EventWriteRecovered, cluster)
	balance := 0
	if s.degraded {
		balance = 1
	}
	require.Equal(t, balance, degrades-recovers, "degrade and recover events of %s must alternate", cluster)
	require.EqualValues(t, degrades, aw.mc.WriteDegraded[cluster], "degrade counter of %s", cluster)
	require.EqualValues(t, recovers, aw.mc.WriteRecovered[cluster], "recover counter of %s", cluster)
}

// checkTransition checks what the step's operation may have done to one cluster.
func (aw *adaptiveWalk) checkTransition(cluster types.ClusterID, b, a adaptiveSnap) {
	t := aw.w.t
	manualRecover := aw.op == adaptiveOpReset || (aw.op == adaptiveOpForceRecover && aw.target == cluster)
	manualDegrade := aw.op == adaptiveOpForceDegrade && aw.target == cluster

	switch {
	case manualRecover:
		a.degradedAt = 0 // meaningless once healthy, and left as it was
		require.Equal(t, adaptiveSnap{}, a, "a manual recovery of %s must clear its state and its backoff", cluster)
	case manualDegrade:
		aw.checkForceDegrade(cluster, b, a)
	case aw.op == adaptiveOpAdvance || aw.op == adaptiveOpUnknown:
		require.Equal(t, b, a, "%v changed %s", aw.op, cluster)
	case b.latched:
		// Every fast observation clears the slow strikes, latch or not; nothing else may move.
		require.Contains(t, []int32{b.slow, 0}, a.slow, "slow strikes of latched %s", cluster)
		a.slow = b.slow
		require.Equal(t, b, a, "only a manual recovery may touch latched %s", cluster)
	case b.degraded && !a.degraded:
		aw.checkAutoRecovery(cluster, b, a)
	case !b.degraded && a.degraded:
		aw.checkAutoDegrade(cluster, b, a)
	}
	if aw.op == adaptiveOpWrite {
		aw.checkWriteLeg(cluster, b, a)
	}
}

func (aw *adaptiveWalk) checkForceDegrade(cluster types.ClusterID, b, a adaptiveSnap) {
	t := aw.w.t
	require.True(t, a.degraded && a.latched, "ForceDegrade(%s) must degrade and latch", cluster)
	require.Zero(t, a.fast, "ForceDegrade(%s) must clear recovery credit", cluster)
	require.Equal(t, b.slow, a.slow, "ForceDegrade(%s) keeps the slow strikes", cluster)
	if b.degraded {
		require.Equal(t, b.dwell, a.dwell, "ForceDegrade on degraded %s must not restart its span", cluster)
		require.Equal(t, b.degradedAt, a.degradedAt, "ForceDegrade on degraded %s must not restart its span", cluster)

		return
	}
	require.Equal(t, aw.minDwell, a.dwell, "a manual degrade of %s starts at the minimum dwell", cluster)
	require.Equal(t, aw.clock.nanos, a.degradedAt, "a manual degrade of %s starts its span now", cluster)
	aw.tally[cluster].credits = 0
}

func (aw *adaptiveWalk) checkAutoRecovery(cluster types.ClusterID, b, a adaptiveSnap) {
	t := aw.w.t
	tally := aw.tally[cluster]
	require.True(t, (aw.op == adaptiveOpFastWrite || aw.op == adaptiveOpProbe) && aw.target == cluster,
		"%s recovered on an operation that credits no recovery to it", cluster)
	spent := time.Duration(aw.clock.nanos - b.degradedAt)
	require.GreaterOrEqual(t, spent, b.dwell, "%s recovered after %v, before its dwell of %v", cluster, spent, b.dwell)
	require.GreaterOrEqual(t, tally.credits, int(aw.recovery),
		"%s recovered after at most %d credits, the threshold is %d", cluster, tally.credits, aw.recovery)
	require.Equal(t, aw.clock.nanos, a.recoveredAt, "the recovery of %s is stamped now", cluster)
	require.Equal(t, b.dwell, a.dwell, "the dwell of %s survives a recovery, for the backoff", cluster)
	*tally = adaptiveTally{}
}

func (aw *adaptiveWalk) checkAutoDegrade(cluster types.ClusterID, b, a adaptiveSnap) {
	t := aw.w.t
	tally := aw.tally[cluster]
	require.Equal(t, adaptiveOpWrite, aw.op, "%s degraded on an operation that strikes nothing", cluster)
	require.GreaterOrEqual(t, tally.strikes, int(aw.strike),
		"%s degraded after at most %d strikes, the threshold is %d", cluster, tally.strikes, aw.strike)
	require.False(t, a.latched, "a strike-driven degrade of %s must not latch", cluster)
	require.Equal(t, aw.clock.nanos, a.degradedAt, "a degrade of %s starts its span now", cluster)

	want := aw.minDwell
	redegrade := aw.minDwell > 0 && b.recoveredAt != 0 && aw.clock.nanos-b.recoveredAt < int64(walkRedegradeWindow)
	if redegrade {
		want = min(2*b.dwell, walkMaxDwell)
		require.Equal(t, b.redegrades+1, a.redegrades, "a re-degrade of %s is counted", cluster)
	} else {
		require.Zero(t, a.redegrades, "a degrade of %s outside the window resets the backoff", cluster)
	}
	require.Equal(t, want, a.dwell,
		"degrade of %s: previous dwell %v, recovered %v ago", cluster, b.dwell, time.Duration(aw.clock.nanos-b.recoveredAt))
	tally.credits = 0
}

// checkWriteLeg checks the slow-strike arithmetic of one leg of a strict write.
func (aw *adaptiveWalk) checkWriteLeg(cluster types.ClusterID, b, a adaptiveSnap) {
	t := aw.w.t
	if b.degraded {
		require.Equal(t, b, a, "a strict write must skip degraded %s and leave its state alone", cluster)

		return
	}
	switch aw.shapes[cluster] {
	case legFast:
		require.Zero(t, a.slow, "a fast leg must clear the slow strikes of %s", cluster)
	case legCapSlow, legError:
		require.Equal(t, b.slow+1, a.slow, "a %s leg strikes %s exactly once", legShapeNames[aw.shapes[cluster]], cluster)
	case legDeltaSlow:
	}
}
