package policy

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// legClock drives one simulated latency counter per cluster.
//
// One counter per cluster, rather than one shared counter, because the
// two legs run concurrently: a single counter could not attribute a
// duration to one of them, since the other's advance would land between
// that leg's two reads.
// Separate fields rather than a map keyed by cluster, because a map
// written from two goroutines aborts the process outright.
//
// The counters are atomic even though each is only ever advanced by its
// own cluster's leg. A fire-and-forget leg outlives the Execute that
// started it, so a test that does not await its legs can have two rounds'
// legs for one cluster in flight at once, and a bare += would be a race
// between them.
type legClock struct{ a, b atomic.Int64 }

func (c *legClock) counter(cluster types.ClusterID) *atomic.Int64 {
	if cluster == types.ClusterA {
		return &c.a
	}

	return &c.b
}

func (c *legClock) now(cluster types.ClusterID) int64 {
	return c.counter(cluster).Load()
}

func (c *legClock) advance(cluster types.ClusterID, d time.Duration) {
	c.counter(cluster).Add(int64(d))
}

// latencyStep returns a write function that advances that cluster's
// latency counter by d instead of sleeping for it.
// Simulating latency by sleeping makes every threshold assertion a race
// against the machine running it, and costs the suite the wall clock it
// pretends to spend.
func latencyStep(clock *legClock, cluster types.ClusterID, d time.Duration) func(context.Context) error {
	return func(_ context.Context) error {
		clock.advance(cluster, d)

		return nil
	}
}

// TestAdaptiveDualWrite_LatencyClockDrivesDegrade pins the latency seam:
// a cluster crosses the delta threshold because the injected clock says
// it did, with no sleep anywhere.
//
// The strategy already injects its hysteresis clock through a.now.
// Latency sampling read time.Now directly, so this test cannot compile
// without the second seam.
func TestAdaptiveDualWrite_LatencyClockDrivesDegrade(t *testing.T) {
	clock := &legClock{}
	a := NewAdaptiveDualWrite(
		WithAdaptiveDeltaThreshold(10*time.Millisecond),
		WithAdaptiveMinFloor(1*time.Millisecond),
		WithAdaptiveStrikeThreshold(2),
	)
	a.latencyNow = clock.now

	ctx := t.Context()
	for range 3 {
		_, _ = a.Execute(ctx,
			latencyStep(clock, types.ClusterA, 1*time.Millisecond),
			latencyStep(clock, types.ClusterB, 60*time.Millisecond),
		)
	}

	require.False(t, a.IsDegraded(types.ClusterA), "the fast cluster must stay healthy")
	require.True(t, a.IsDegraded(types.ClusterB), "the slow cluster must degrade on the injected latency alone")
}

// TestAdaptiveDualWrite_LatencyClockDrivesStrictDegrade pins the seam on
// the strict path.
//
// ExecuteStrict samples latency through the same latencyNanos calls that
// Execute does, but every clock-driven test in adaptive_write_test.go
// goes through Execute, which would leave half the seam unexercised.
func TestAdaptiveDualWrite_LatencyClockDrivesStrictDegrade(t *testing.T) {
	clock := &legClock{}
	a := NewAdaptiveDualWrite(
		WithAdaptiveDeltaThreshold(10*time.Millisecond),
		WithAdaptiveMinFloor(1*time.Millisecond),
		WithAdaptiveStrikeThreshold(2),
	)
	a.latencyNow = clock.now

	ctx := t.Context()

	// Two rounds reach the strike threshold, and both legs still run
	// while B is healthy.
	for range 2 {
		errA, errB := a.ExecuteStrict(ctx,
			latencyStep(clock, types.ClusterA, 1*time.Millisecond),
			latencyStep(clock, types.ClusterB, 60*time.Millisecond),
		)
		require.NoError(t, errA)
		require.NoError(t, errB)
	}

	require.False(t, a.IsDegraded(types.ClusterA), "the fast cluster must stay healthy")
	require.True(t, a.IsDegraded(types.ClusterB), "the slow cluster must degrade on the injected latency alone")

	// A degraded cluster is skipped rather than written, which is the
	// strict path's whole point and the reason its samples stop arriving.
	errA, errB := a.ExecuteStrict(ctx,
		latencyStep(clock, types.ClusterA, 1*time.Millisecond),
		latencyStep(clock, types.ClusterB, 60*time.Millisecond),
	)
	require.NoError(t, errA)
	require.ErrorIs(t, errB, types.ErrClusterDegraded)
}
