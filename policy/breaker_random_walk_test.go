package policy

import (
	"testing"
	"time"

	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// walkResetTimeout sits half a minute off every whole minute.
// The breaker reads the wall clock, so a walk advances time by backdating the last-failure stamp in whole minutes.
// The real time the test itself takes, well under a second,
// can then never carry the virtual elapsed time across the timeout.
const walkResetTimeout = 59*time.Minute + 30*time.Second

// walkAbsoluteMax is the latency breaker's soft-failure bound in the walks.
const walkAbsoluteMax = 100 * time.Millisecond

// walkBreakerAdvances straddle walkResetTimeout: 59 minutes is inside it and 60 is past it.
var walkBreakerAdvances = []time.Duration{
	time.Minute, 30 * time.Minute, 59 * time.Minute, 60 * time.Minute, 120 * time.Minute,
}

// walkLatencies straddle walkAbsoluteMax, which itself is not a soft failure.
var walkLatencies = []time.Duration{
	time.Millisecond, walkAbsoluteMax - time.Nanosecond, walkAbsoluteMax, walkAbsoluteMax + time.Nanosecond, time.Second,
}

// walkProbeOutcomes includes a value the package does not know, which the Godoc says is treated as abandoned.
var walkProbeOutcomes = []types.ProbeOutcome{
	types.ProbeSucceeded, types.ProbeFailed, types.ProbeAbandoned, types.ProbeOutcome(99),
}

// breakerPolicy is the surface CircuitBreaker and LatencyCircuitBreaker share.
type breakerPolicy interface {
	ShouldFailover(cluster types.ClusterID, err error) bool
	RecordFailure(cluster types.ClusterID)
	RecordSuccess(cluster types.ClusterID)
	Failures(cluster types.ClusterID) int
	TryBeginFailoverProbe(cluster types.ClusterID) (uint64, bool)
	CompleteFailoverProbe(cluster types.ClusterID, token uint64, outcome types.ProbeOutcome)
}

// breakerModel is the reference state of one cluster's breaker, as the Godoc of CircuitBreaker describes it.
type breakerModel struct {
	failures int
	tripped  bool          // open or half-open
	halfOpen bool          // a reservation is live
	stamped  bool          // a last-failure stamp exists
	since    time.Duration // virtual time since the stamp
	token    uint64        // the live reservation's token
	issued   []uint64      // every token handed out, for stale completions
	trips    int
	closes   int
}

// fail applies a failure: count, restamp, and trip at the threshold.
func (m *breakerModel) fail(threshold int) {
	m.failures++
	m.stamped = true
	m.since = 0
	if threshold > 0 && m.failures >= threshold && !m.tripped {
		m.tripped = true
		m.trips++
	}
}

// succeed applies a success, which closes an open or half-open breaker.
func (m *breakerModel) succeed() {
	if m.tripped {
		m.closes++
	}
	m.failures = 0
	m.stamped = false
	m.since = 0
	m.tripped = false
	m.halfOpen = false
}

// reservable reports whether a probe reservation should succeed now.
func (m *breakerModel) reservable() bool {
	return m.tripped && !m.halfOpen && m.stamped && m.since > walkResetTimeout
}

// breakerWalk drives a CircuitBreaker or a LatencyCircuitBreaker against breakerModel.
type breakerWalk struct {
	w         *walk
	policy    breakerPolicy
	core      *CircuitBreaker
	latency   *LatencyCircuitBreaker // nil when the walk drives a plain CircuitBreaker
	threshold int
	fbt       bool
	mc        *testutil.TestMetricsCollector
	em        *recordingEmitter
	models    map[types.ClusterID]*breakerModel
	ops       []walkOp
}

// TestCircuitBreaker_RandomWalk drives a CircuitBreaker through seeded random sequences of
// failures, successes, probe reservations and completions, and clock advances,
// and checks every step against a reference model of the documented state machine.
func TestCircuitBreaker_RandomWalk(t *testing.T) {
	runWalk(t, func(w *walk) walkMachine {
		threshold := 1 + w.intn(4)
		fbt := w.chance(2)
		mc := testutil.NewTestMetricsCollector()
		cb := NewCircuitBreaker(
			WithThreshold(threshold),
			WithResetTimeout(walkResetTimeout),
			WithFailoverBelowThreshold(fbt),
			WithCircuitBreakerMetrics(mc),
		)
		w.notef("new CircuitBreaker threshold=%d failoverBelowThreshold=%v", threshold, fbt)

		return newBreakerWalk(w, cb, cb, nil, threshold, fbt, mc)
	})
}

// TestLatencyCircuitBreaker_RandomWalk is TestCircuitBreaker_RandomWalk with latency samples
// either side of the absolute maximum mixed in, and the route veto checked alongside the failover answer.
func TestLatencyCircuitBreaker_RandomWalk(t *testing.T) {
	runWalk(t, func(w *walk) walkMachine {
		threshold := 1 + w.intn(4)
		fbt := w.chance(2)
		mc := testutil.NewTestMetricsCollector()
		lcb := NewLatencyCircuitBreaker(
			WithLatencyThreshold(threshold),
			WithLatencyResetTimeout(walkResetTimeout),
			WithLatencyAbsoluteMax(walkAbsoluteMax),
			WithLatencyFailoverBelowThreshold(fbt),
			WithLatencyMetrics(mc),
		)
		w.notef("new LatencyCircuitBreaker threshold=%d failoverBelowThreshold=%v", threshold, fbt)

		return newBreakerWalk(w, lcb, lcb.CircuitBreaker, lcb, threshold, fbt, mc)
	})
}

func newBreakerWalk(
	w *walk, policy breakerPolicy, core *CircuitBreaker, latency *LatencyCircuitBreaker,
	threshold int, fbt bool, mc *testutil.TestMetricsCollector,
) *breakerWalk {
	em := &recordingEmitter{}
	core.SetEventEmitter(em)
	b := &breakerWalk{
		w: w, policy: policy, core: core, latency: latency,
		threshold: threshold, fbt: fbt, mc: mc, em: em,
		models: map[types.ClusterID]*breakerModel{types.ClusterA: {}, types.ClusterB: {}},
	}
	b.ops = []walkOp{
		{weight: 25, run: b.opFailure},
		{weight: 6, run: b.opSuccess},
		{weight: 15, run: b.opTryBegin},
		{weight: 15, run: b.opComplete},
		{weight: 15, run: b.opAdvance},
		{weight: 3, run: b.opUnknownCluster},
	}
	if latency != nil {
		b.ops = append(b.ops, walkOp{weight: 15, run: b.opLatency})
	}

	return b
}

func (b *breakerWalk) step() {
	b.w.run(b.ops)
	b.check()
}

// finish checks liveness: once no failure arrives for longer than the reset timeout,
// a probe can reserve an open breaker and its success closes it.
func (b *breakerWalk) finish() {
	t := b.w.t
	for _, cluster := range []types.ClusterID{types.ClusterA, types.ClusterB} {
		m := b.models[cluster]
		if m.halfOpen {
			b.complete(cluster, m.token, types.ProbeAbandoned)
		}
		if !m.tripped {
			continue
		}
		b.advance(2 * walkResetTimeout)
		token, ok := b.policy.TryBeginFailoverProbe(cluster)
		require.True(t, ok, "an open breaker with no failure for twice the reset timeout must reserve a probe on %s", cluster)
		m.halfOpen, m.token = true, token
		b.complete(cluster, token, types.ProbeSucceeded)
		require.False(t, b.policy.ShouldFailover(cluster, nil) && !b.fbt, "a successful probe must close %s", cluster)
	}
	b.check()
}

func (b *breakerWalk) opFailure() {
	cluster := b.w.cluster()
	b.w.notef("RecordFailure(%s)", cluster)
	b.policy.RecordFailure(cluster)
	b.models[cluster].fail(b.threshold)
}

func (b *breakerWalk) opSuccess() {
	cluster := b.w.cluster()
	b.w.notef("RecordSuccess(%s)", cluster)
	b.policy.RecordSuccess(cluster)
	b.models[cluster].succeed()
}

func (b *breakerWalk) opLatency() {
	cluster := b.w.cluster()
	latency := walkLatencies[b.w.intn(len(walkLatencies))]
	b.w.notef("RecordLatency(%s, %v)", cluster, latency)
	b.latency.RecordLatency(cluster, latency)
	if latency > walkAbsoluteMax {
		b.models[cluster].fail(b.threshold)
	} else {
		b.models[cluster].succeed()
	}
}

func (b *breakerWalk) opTryBegin() {
	cluster := b.w.cluster()
	m := b.models[cluster]
	token, ok := b.policy.TryBeginFailoverProbe(cluster)
	b.w.notef("TryBeginFailoverProbe(%s) = %d, %v", cluster, token, ok)
	if m.halfOpen {
		require.False(b.w.t, ok, "a second probe was reserved on %s while one is in flight", cluster)
	}
	require.Equal(b.w.t, m.reservable(), ok,
		"reservation on %s: tripped=%v halfOpen=%v since=%v", cluster, m.tripped, m.halfOpen, m.since)
	if !ok {
		return
	}
	require.NotContains(b.w.t, m.issued, token, "a reservation token was handed out twice on %s", cluster)
	m.halfOpen, m.token = true, token
	m.issued = append(m.issued, token)
}

func (b *breakerWalk) opComplete() {
	cluster := b.w.cluster()
	m := b.models[cluster]
	var token uint64
	switch {
	case m.halfOpen && !b.w.chance(3):
		token = m.token
	case len(m.issued) > 0 && !b.w.chance(4):
		token = m.issued[b.w.intn(len(m.issued))]
	}
	b.complete(cluster, token, walkProbeOutcomes[b.w.intn(len(walkProbeOutcomes))])
}

// complete settles a reservation on the breaker and on the model.
// Only the live token of a half-open breaker settles anything.
func (b *breakerWalk) complete(cluster types.ClusterID, token uint64, outcome types.ProbeOutcome) {
	b.w.notef("CompleteFailoverProbe(%s, %d, %d)", cluster, token, outcome)
	b.policy.CompleteFailoverProbe(cluster, token, outcome)
	m := b.models[cluster]
	if !m.halfOpen || token != m.token {
		return
	}
	switch outcome { //nolint:exhaustive // every other outcome releases the reservation
	case types.ProbeSucceeded:
		m.succeed()
	case types.ProbeFailed:
		m.halfOpen = false
		m.stamped = true
		m.since = 0
	default:
		m.halfOpen = false
	}
}

func (b *breakerWalk) opAdvance() {
	b.advance(walkBreakerAdvances[b.w.intn(len(walkBreakerAdvances))])
}

// advance moves virtual time forward by backdating each cluster's last-failure stamp.
// An absent stamp stays absent, as a later clock would leave it.
func (b *breakerWalk) advance(d time.Duration) {
	b.w.notef("advance %v", d)
	for cluster, m := range b.models {
		state := b.core.stateFor(cluster)
		if last := state.lastFailure.Load(); last != 0 {
			state.lastFailure.Store(last - int64(d))
		}
		if m.stamped {
			m.since += d
		}
	}
}

func (b *breakerWalk) opUnknownCluster() {
	t := b.w.t
	b.w.notef("operations on unknown cluster %s", unknownCluster)
	b.policy.RecordFailure(unknownCluster)
	b.policy.RecordSuccess(unknownCluster)
	b.policy.CompleteFailoverProbe(unknownCluster, 1, types.ProbeSucceeded)
	_, ok := b.policy.TryBeginFailoverProbe(unknownCluster)
	require.False(t, ok, "an unknown cluster never reserves a probe")
	require.False(t, b.policy.ShouldFailover(unknownCluster, nil), "an unknown cluster never fails over")
	require.Zero(t, b.policy.Failures(unknownCluster), "an unknown cluster has no failures")
}

// check compares both clusters with the model and with the invariants the Godoc promises.
func (b *breakerWalk) check() {
	for _, cluster := range []types.ClusterID{types.ClusterA, types.ClusterB} {
		b.checkCluster(cluster)
	}
}

func (b *breakerWalk) checkCluster(cluster types.ClusterID) {
	t := b.w.t
	m := b.models[cluster]

	failures := b.policy.Failures(cluster)
	require.Equal(t, m.failures, failures, "failure count of %s", cluster)
	should := b.policy.ShouldFailover(cluster, nil)
	require.Equal(t, m.tripped || b.fbt, should, "ShouldFailover(%s)", cluster)
	if should && !b.fbt {
		require.GreaterOrEqual(t, failures, b.threshold,
			"%s fails over with %d failures, below the threshold %d", cluster, failures, b.threshold)
	}
	if b.latency != nil {
		require.Equal(t, m.tripped, b.latency.VetoRoute(cluster), "VetoRoute(%s) must follow the open state", cluster)
	}

	state := b.core.stateFor(cluster)
	state.mu.Lock()
	tripped, halfOpen := state.tripped, state.halfOpen
	state.mu.Unlock()
	require.Equal(t, m.tripped, tripped, "tripped state of %s", cluster)
	require.Equal(t, m.halfOpen, halfOpen, "half-open state of %s", cluster)
	require.False(t, halfOpen && !tripped, "%s is half-open without being open", cluster)

	gauge := 0
	switch {
	case m.halfOpen:
		gauge = 1
	case m.tripped:
		gauge = 2
	}
	require.Equal(t, gauge, b.mc.CircuitBreakerState[cluster], "state gauge of %s", cluster)
	require.EqualValues(t, m.trips, b.mc.CircuitBreakerTrips[cluster], "trip counter of %s", cluster)
	require.Equal(t, m.trips, countEvents(b.em, types.EventCircuitBreakerOpen, cluster), "open events of %s", cluster)
	require.Equal(t, m.closes, countEvents(b.em, types.EventCircuitBreakerClosed, cluster), "close events of %s", cluster)
}

// TestBreakers_ZeroValueRandomWalk calls every exported method of a zero-value CircuitBreaker
// and LatencyCircuitBreaker in random order.
// The Godoc promises neither panics and neither is a functioning breaker:
// neither ever fails over or reserves a probe.
func TestBreakers_ZeroValueRandomWalk(t *testing.T) {
	runWalk(t, func(w *walk) walkMachine {
		return &zeroBreakerWalk{w: w, cb: &CircuitBreaker{}, lcb: &LatencyCircuitBreaker{}}
	})
}

type zeroBreakerWalk struct {
	w   *walk
	cb  *CircuitBreaker
	lcb *LatencyCircuitBreaker
}

func (z *zeroBreakerWalk) step() {
	cluster := []types.ClusterID{types.ClusterA, types.ClusterB, unknownCluster}[z.w.intn(3)]
	targets := []breakerPolicy{z.cb, z.lcb}
	target := targets[z.w.intn(len(targets))]
	ops := []walkOp{
		{weight: 1, run: func() { target.RecordFailure(cluster) }},
		{weight: 1, run: func() { target.RecordSuccess(cluster) }},
		{weight: 1, run: func() { target.CompleteFailoverProbe(cluster, uint64(z.w.intn(4)), types.ProbeSucceeded) }},
		{weight: 1, run: func() { z.lcb.RecordLatency(cluster, walkLatencies[z.w.intn(len(walkLatencies))]) }},
		{weight: 1, run: func() { z.setters(target) }},
		{weight: 1, run: func() { z.getters(target, cluster) }},
	}
	z.w.notef("zero-value %T on %s", target, cluster)
	z.w.run(ops)

	for _, p := range targets {
		_, ok := p.TryBeginFailoverProbe(cluster)
		require.False(z.w.t, ok, "a zero-value %T never reserves a probe", p)
		require.False(z.w.t, p.ShouldFailover(cluster, nil), "a zero-value %T never fails over", p)
	}
	require.False(z.w.t, z.lcb.VetoRoute(cluster), "a zero-value LatencyCircuitBreaker never vetoes")
	require.Zero(z.w.t, z.lcb.Failures(cluster), "a zero-value LatencyCircuitBreaker counts nothing")
}

func (z *zeroBreakerWalk) finish() {}

func (z *zeroBreakerWalk) setters(target breakerPolicy) {
	type setter interface {
		SetMetrics(m types.MetricsCollector)
		SetLogger(l types.Logger)
		SetEventEmitter(em types.ClusterEventEmitter)
		SetClusterNames(names types.ClusterNames)
	}
	s, ok := target.(setter)
	require.True(z.w.t, ok)
	switch z.w.intn(5) {
	case 0:
		s.SetMetrics(testutil.NewTestMetricsCollector())
	case 1:
		s.SetMetrics(nil)
	case 2:
		s.SetLogger(nil)
	case 3:
		s.SetEventEmitter(&recordingEmitter{})
	default:
		s.SetClusterNames(types.DefaultClusterNames())
	}
}

func (z *zeroBreakerWalk) getters(target breakerPolicy, cluster types.ClusterID) {
	type getter interface {
		MetricsConfigured() bool
		LoggerConfigured() bool
		ProbeScheduled() bool
		FailoverBelowThreshold() bool
	}
	g, ok := target.(getter)
	require.True(z.w.t, ok)
	_ = g.MetricsConfigured()
	_ = g.LoggerConfigured()
	require.False(z.w.t, g.ProbeScheduled(), "a zero-value breaker schedules no probe")
	require.False(z.w.t, g.FailoverBelowThreshold(), "a zero-value breaker does not fail over below the threshold")
	_ = target.Failures(cluster)
	_ = z.lcb.AbsoluteMax()
}
