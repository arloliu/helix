package policy

import (
	"testing"
	"time"

	"github.com/arloliu/helix/internal/test/testutil"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// walkCooldown sits half a minute off every whole minute, for the reason walkResetTimeout does:
// StickyRead reads the wall clock, and a walk advances time by backdating the last move in whole minutes.
const walkCooldown = 5*time.Minute + 30*time.Second

// walkStickyAdvances straddle walkCooldown: 5 minutes is inside it and 6 is past it.
var walkStickyAdvances = []time.Duration{time.Minute, 3 * time.Minute, 5 * time.Minute, 6 * time.Minute, 20 * time.Minute}

// stickyModel is the reference state of a StickyRead, as its Godoc describes it.
type stickyModel struct {
	initial   types.ClusterID
	preferred types.ClusterID
	knownBad  map[types.ClusterID]bool
	stamped   bool          // a cooldown is running from the last move or SetPreferred
	since     time.Duration // virtual time since that stamp
	moves     int
}

// inCooldown reports whether a move or SetPreferred happened less than the cooldown ago.
func (m *stickyModel) inCooldown(cooldown time.Duration) bool {
	return m.stamped && m.since < cooldown
}

// stickyWalk drives a StickyRead against stickyModel.
type stickyWalk struct {
	w        *walk
	s        *StickyRead
	cooldown time.Duration
	model    *stickyModel
	mc       *testutil.TestMetricsCollector
	em       *recordingEmitter
	ops      []walkOp
}

// TestStickyRead_RandomWalk drives a StickyRead through seeded random sequences of
// read failures and successes on either cluster, manual moves, resets and clock advances,
// and checks every step against a reference model of the documented preference rules.
//
// The model encodes the no-oscillation rule directly: inside the cooldown a failure on the preferred cluster
// moves the preference only when the other cluster has succeeded since it last failed,
// so two clusters failing in turn never swap the preference back and forth.
func TestStickyRead_RandomWalk(t *testing.T) {
	runWalk(t, func(w *walk) walkMachine {
		initial := w.cluster()
		cooldown := walkCooldown
		if w.chance(4) {
			cooldown = 0
		}
		s := NewStickyRead(WithPreferredCluster(initial), WithStickyReadCooldown(cooldown))
		mc := testutil.NewTestMetricsCollector()
		em := &recordingEmitter{}
		s.SetMetrics(mc)
		s.SetEventEmitter(em)
		w.notef("new StickyRead preferred=%s cooldown=%v", initial, cooldown)

		sw := &stickyWalk{
			w: w, s: s, cooldown: cooldown, mc: mc, em: em,
			model: &stickyModel{initial: initial, preferred: initial, knownBad: map[types.ClusterID]bool{}},
		}
		sw.ops = []walkOp{
			{weight: 30, run: sw.opFailure},
			{weight: 15, run: sw.opSuccess},
			{weight: 5, run: sw.opSetPreferred},
			{weight: 2, run: sw.opReset},
			{weight: 20, run: sw.opAdvance},
			{weight: 3, run: sw.opUnknownCluster},
		}

		return sw
	})
}

func (sw *stickyWalk) step() {
	sw.w.run(sw.ops)
	sw.check()
}

func (sw *stickyWalk) finish() {}

func (sw *stickyWalk) opFailure() {
	t := sw.w.t
	m := sw.model
	cluster := sw.w.cluster()
	alt, ok := sw.s.OnFailure(cluster, nil)
	sw.w.notef("OnFailure(%s) = %q, %v", cluster, alt, ok)

	m.knownBad[cluster] = true
	if cluster != m.preferred {
		require.False(t, ok, "a failure on the cluster that is not preferred must not fail over")
		require.Empty(t, alt)

		return
	}
	other := otherCluster(cluster)
	require.True(t, ok, "a failure on the preferred cluster must fail over")
	require.Equal(t, other, alt, "the failover target is the other cluster")
	if m.inCooldown(sw.cooldown) && m.knownBad[other] {
		return
	}
	m.preferred = other
	m.stamped, m.since = true, 0
	m.moves++
}

func (sw *stickyWalk) opSuccess() {
	cluster := sw.w.cluster()
	sw.w.notef("OnSuccess(%s)", cluster)
	sw.s.OnSuccess(cluster)
	sw.model.knownBad[cluster] = false
}

func (sw *stickyWalk) opSetPreferred() {
	m := sw.model
	cluster := sw.w.cluster()
	sw.w.notef("SetPreferred(%s)", cluster)
	sw.s.SetPreferred(cluster)
	if cluster != m.preferred {
		m.preferred = cluster
		m.moves++
	}
	m.stamped, m.since = true, 0
}

func (sw *stickyWalk) opReset() {
	m := sw.model
	sw.w.notef("Reset()")
	sw.s.Reset()
	if m.preferred != m.initial {
		m.preferred = m.initial
		m.moves++
	}
	m.stamped, m.since = false, 0
	clear(m.knownBad)
}

// opAdvance moves virtual time forward by backdating the last move.
func (sw *stickyWalk) opAdvance() {
	d := walkStickyAdvances[sw.w.intn(len(walkStickyAdvances))]
	sw.w.notef("advance %v", d)
	sw.s.mu.Lock()
	if !sw.s.lastFailoverTime.IsZero() {
		sw.s.lastFailoverTime = sw.s.lastFailoverTime.Add(-d)
	}
	sw.s.mu.Unlock()
	if sw.model.stamped {
		sw.model.since += d
	}
}

func (sw *stickyWalk) opUnknownCluster() {
	t := sw.w.t
	sw.w.notef("operations on unknown cluster %s", unknownCluster)
	alt, ok := sw.s.OnFailure(unknownCluster, nil)
	require.False(t, ok, "an unknown cluster never fails over")
	require.Empty(t, alt)
	sw.s.OnSuccess(unknownCluster)
	sw.s.SetPreferred(unknownCluster)
}

func (sw *stickyWalk) check() {
	t := sw.w.t
	m := sw.model

	require.Equal(t, m.preferred, sw.s.Preferred(), "preferred cluster")
	require.Equal(t, m.preferred, sw.s.Select(t.Context()), "Select must return the preferred cluster")
	for _, cluster := range []types.ClusterID{types.ClusterA, types.ClusterB} {
		require.Equal(t, m.knownBad[cluster], sw.s.knownBadSlot(cluster).Load(), "known-bad mark of %s", cluster)
		require.Equal(t, m.preferred == cluster, sw.mc.ReadPreferred[cluster], "preference gauge of %s", cluster)
	}
	moves := countEvents(sw.em, types.EventReadRouteChanged, types.ClusterA) +
		countEvents(sw.em, types.EventReadRouteChanged, types.ClusterB)
	require.Equal(t, m.moves, moves, "one route-changed event per move")
}
