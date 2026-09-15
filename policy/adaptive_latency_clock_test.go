package policy

import (
	"context"
	"testing"
	"time"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// legClock drives one simulated latency counter per cluster.
//
// Two fields rather than a map keyed by cluster: the two legs advance
// concurrently, and separate addresses are both race-clean and
// deterministic, where a map write from two goroutines aborts the
// process outright.
type legClock struct{ a, b int64 }

func (c *legClock) now(cluster types.ClusterID) int64 {
	if cluster == types.ClusterA {
		return c.a
	}

	return c.b
}

func (c *legClock) advance(cluster types.ClusterID, d time.Duration) {
	if cluster == types.ClusterA {
		c.a += int64(d)

		return
	}

	c.b += int64(d)
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
