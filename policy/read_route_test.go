package policy

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
)

// routeEvents returns the read-route events an emitter recorded.
func routeEvents(em *recordingEmitter) []types.ClusterEvent {
	var out []types.ClusterEvent
	for _, ev := range em.snapshot() {
		if ev.Kind == types.EventReadRouteChanged {
			out = append(out, ev)
		}
	}

	return out
}

func requireRoute(t *testing.T, ev types.ClusterEvent, from, to types.ClusterID, reason string) {
	t.Helper()
	require.Equal(t, types.EventReadRouteChanged, ev.Kind)
	require.Equal(t, from, ev.FromCluster)
	require.Equal(t, to, ev.ToCluster)
	require.Equal(t, to, ev.Cluster)
	require.Equal(t, reason, ev.Reason)
	require.False(t, ev.Timestamp.IsZero())
}

func requirePreferredGauge(t *testing.T, mc *testutil.TestMetricsCollector, preferred types.ClusterID) {
	t.Helper()
	require.Equal(t, preferred == types.ClusterA, mc.ReadPreferred[types.ClusterA])
	require.Equal(t, preferred == types.ClusterB, mc.ReadPreferred[types.ClusterB])
}

func TestStickyRead_ReportsEveryRouteReason(t *testing.T) {
	s := NewStickyRead(WithPreferredCluster(types.ClusterA), WithStickyReadCooldown(time.Hour))
	mc := testutil.NewTestMetricsCollector()
	em := &recordingEmitter{}
	require.False(t, s.MetricsConfigured())
	s.SetMetrics(mc)
	require.True(t, s.MetricsConfigured())
	requirePreferredGauge(t, mc, types.ClusterA)
	s.SetEventEmitter(em)

	_, _ = s.OnFailure(types.ClusterA, nil) // A down: failover to B
	requirePreferredGauge(t, mc, types.ClusterB)
	_, _ = s.OnFailure(types.ClusterB, nil) // B down, A still known bad: no move
	s.OnSuccess(types.ClusterA)
	_, _ = s.OnFailure(types.ClusterB, nil) // A proven good: swap inside the cooldown
	requirePreferredGauge(t, mc, types.ClusterA)
	s.SetPreferred(types.ClusterA) // already preferred: no move
	s.SetPreferred(types.ClusterB)
	requirePreferredGauge(t, mc, types.ClusterB)
	s.Reset()
	requirePreferredGauge(t, mc, types.ClusterA)
	s.Reset() // already at the initial preference: no move

	events := routeEvents(em)
	require.Len(t, events, 4)
	requireRoute(t, events[0], types.ClusterA, types.ClusterB, routeReasonFailover)
	requireRoute(t, events[1], types.ClusterB, types.ClusterA, routeReasonKnownGood)
	requireRoute(t, events[2], types.ClusterA, types.ClusterB, routeReasonManual)
	requireRoute(t, events[3], types.ClusterB, types.ClusterA, routeReasonManual)
}

func TestStickyRead_ConcurrentFailuresMoveOnce(t *testing.T) {
	s := NewStickyRead(WithPreferredCluster(types.ClusterA), WithStickyReadCooldown(time.Hour))
	mc := testutil.NewTestMetricsCollector()
	em := &recordingEmitter{}
	s.SetMetrics(mc)
	s.SetEventEmitter(em)

	const callers = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	var failedOver atomic.Int32
	for range callers {
		wg.Go(func() {
			<-start
			// A caller that arrives after the move sees A as a
			// non-preferred cluster and gets no failover, as before.
			if alt, ok := s.OnFailure(types.ClusterA, nil); ok {
				failedOver.Add(1)
				require.Equal(t, types.ClusterB, alt)
			}
		})
	}
	close(start)
	wg.Wait()

	require.GreaterOrEqual(t, failedOver.Load(), int32(1))
	require.Len(t, routeEvents(em), 1, "the preference moves once")
	require.Equal(t, types.ClusterB, s.Preferred())
	requirePreferredGauge(t, mc, types.ClusterB)
}

// blockingRouteCollector blocks the first gauge write after the install
// until released, so a transition can be forced to report while an older
// report is stuck.
type blockingRouteCollector struct {
	*testutil.TestMetricsCollector
	writes  atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (b *blockingRouteCollector) SetReadPreferred(cluster types.ClusterID, preferred bool) {
	if b.writes.Add(1) == 3 { // the install writes both clusters first
		close(b.entered)
		<-b.release
	}
	b.TestMetricsCollector.SetReadPreferred(cluster, preferred)
}

func TestStickyRead_BlockedGaugeDoesNotStallTransitions(t *testing.T) {
	s := NewStickyRead(WithPreferredCluster(types.ClusterA))
	mc := &blockingRouteCollector{
		TestMetricsCollector: testutil.NewTestMetricsCollector(),
		entered:              make(chan struct{}),
		release:              make(chan struct{}),
	}
	em := &recordingEmitter{}
	s.SetEventEmitter(em)
	s.SetMetrics(mc)

	done := make(chan struct{})
	go func() {
		s.SetPreferred(types.ClusterB) // its gauge write blocks
		close(done)
	}()
	<-mc.entered

	s.SetPreferred(types.ClusterA) // completes while the older report is stuck
	require.Equal(t, types.ClusterA, s.Preferred())
	require.Len(t, routeEvents(em), 2, "both transitions were recorded in order")

	close(mc.release)
	<-done
	requirePreferredGauge(t, mc.TestMetricsCollector, types.ClusterA)
}

func TestPrimaryOnlyRead_ReportsEveryRouteReason(t *testing.T) {
	p := NewPrimaryOnlyRead()
	mc := testutil.NewTestMetricsCollector()
	em := &recordingEmitter{}
	p.SetMetrics(mc)
	requirePreferredGauge(t, mc, types.ClusterA)
	p.SetEventEmitter(em)

	_, _ = p.OnFailure(types.ClusterA, nil) // failover
	requirePreferredGauge(t, mc, types.ClusterB)
	_, _ = p.OnFailure(types.ClusterA, nil) // a failed probe: no move
	p.OnSuccess(types.ClusterA)             // recovered
	requirePreferredGauge(t, mc, types.ClusterA)
	p.OnSuccess(types.ClusterA) // already on A: no move
	_, _ = p.OnFailure(types.ClusterA, nil)
	p.Reset() // manual
	p.Reset() // no move

	events := routeEvents(em)
	require.Len(t, events, 4)
	requireRoute(t, events[0], types.ClusterA, types.ClusterB, routeReasonFailover)
	requireRoute(t, events[1], types.ClusterB, types.ClusterA, routeReasonRecovered)
	requireRoute(t, events[2], types.ClusterA, types.ClusterB, routeReasonFailover)
	requireRoute(t, events[3], types.ClusterB, types.ClusterA, routeReasonManual)
}

func TestPrimaryOnlyRead_ConcurrentTransitionsAlternate(t *testing.T) {
	p := NewPrimaryOnlyRead()
	mc := testutil.NewTestMetricsCollector()
	em := &reportingEmitter{}
	p.SetMetrics(mc)
	p.SetEventEmitter(em)

	// 8 goroutines x 3 rounds is at most 48 moves, comfortably below the
	// outbox's capacity. The burst has to stay under it: past outboxCap the
	// outbox drops the newest event by design, which would leave the
	// delivered events a subsequence of the transitions and their order
	// unverifiable. The assertion below is about delivery order, so the test
	// keeps the outbox from overflowing and checks that it did not.
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 3 {
				_, _ = p.OnFailure(types.ClusterA, nil)
				p.OnSuccess(types.ClusterA)
			}
		})
	}
	wg.Wait()

	require.Zero(t, em.dropped.Load(), "the burst must stay inside the outbox")
	events := routeEvents(&em.recordingEmitter)
	require.NotEmpty(t, events)
	for i, ev := range events {
		want := types.ClusterB
		if i%2 == 1 {
			want = types.ClusterA
		}
		require.Equal(t, want, ev.ToCluster, "event %d: transitions alternate, never two moves the same way", i)
	}
	requirePreferredGauge(t, mc, p.preferred())
}

func TestRoundRobinRead_ReportsNoRoute(t *testing.T) {
	var rr any = NewRoundRobinRead()
	_, hasMetrics := rr.(interface{ SetMetrics(types.MetricsCollector) })
	_, hasEmitter := rr.(interface {
		SetEventEmitter(types.ClusterEventEmitter)
	})
	require.False(t, hasMetrics)
	require.False(t, hasEmitter)
}
