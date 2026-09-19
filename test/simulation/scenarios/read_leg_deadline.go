package scenarios

import (
	"context"
	"fmt"
	"time"

	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/test/simulation/types"
	htypes "github.com/arloliu/helix/types"
)

// ReadLegDeadlineTrip verifies that a slow-but-alive cluster trips the breaker through read-leg timeouts
// when the client bounds each read leg with WithClusterReadTimeout.
//
// It requires a client configured with a 500ms read-leg deadline,
// reads pinned to ClusterA,
// and a LatencyCircuitBreaker whose AbsoluteMax (2s) is far above that deadline.
// Every read on the slowed cluster ends at the leg deadline as a read error,
// before any latency sample reaches the breaker,
// so only the error path can trip it; a plain CircuitBreaker would behave the same here.
// The latency path is covered by the latency-cb group,
// which runs without a read-leg deadline.
type ReadLegDeadlineTrip struct{}

func (s *ReadLegDeadlineTrip) Name() string {
	return "read-leg-deadline-trip"
}

func (s *ReadLegDeadlineTrip) Description() string {
	return "Verifies read-leg timeouts on a slow cluster trip the breaker, reroute reads, and the breaker closes after recovery"
}

func (s *ReadLegDeadlineTrip) Run(ctx context.Context, env *types.Environment) error {
	env.Logger.Info("Starting ReadLegDeadlineTrip scenario")

	lcb, _ := env.Client.Config().FailoverPolicy.(*policy.LatencyCircuitBreaker)
	if lcb == nil {
		return fmt.Errorf("LatencyCircuitBreaker not configured: got %T", env.Client.Config().FailoverPolicy)
	}

	readErrsBefore := env.Metrics.GetReadErrors(htypes.ClusterA)

	// 1. Slow Cluster A well past both the 500ms read-leg deadline
	// and the recovery probe's 1s timeout, so neither a read nor a probe succeeds.
	env.Logger.Info("Injecting 2s latency on Cluster A (above the read-leg deadline)")
	env.ChaosA.SetLatency(2 * time.Second)

	// 2. Wait for the breaker to trip.
	err := waitUntil(ctx, 20*time.Second, func() bool {
		return lcb.ShouldFailover(htypes.ClusterA, nil)
	})
	if err != nil {
		return fmt.Errorf("breaker did not trip within timeout: failures=%d readErrorsA=%d: %w",
			lcb.Failures(htypes.ClusterA), env.Metrics.GetReadErrors(htypes.ClusterA)-readErrsBefore, err)
	}

	// 3. The trip came from read errors, not latency samples:
	// a latency trip would leave Cluster A's read error count where it was.
	readErrs := env.Metrics.GetReadErrors(htypes.ClusterA) - readErrsBefore
	env.Logger.Info("Breaker tripped", "failures", lcb.Failures(htypes.ClusterA), "readErrorsA", readErrs)
	if readErrs < 1 {
		return fmt.Errorf("breaker tripped with no read errors on Cluster A (delta=%d)", readErrs)
	}

	// 4. Reads are rerouted to Cluster B while A's breaker is open.
	_, scansB, _ := env.ChaosB.Counters()
	err = waitUntil(ctx, 10*time.Second, func() bool {
		_, scans, _ := env.ChaosB.Counters()
		return scans > scansB
	})
	if err != nil {
		return fmt.Errorf("no reads reached Cluster B after the trip: %w", err)
	}

	// 5. Recover Cluster A.
	// The breaker stays open until the reset timeout elapses and a recovery probe succeeds.
	env.Logger.Info("Recovering Cluster A (breaker should stay open)")
	env.ChaosA.SetLatency(0)

	env.Logger.Info("Waiting for the breaker reset timeout (15s) and a recovery probe...")
	err = waitUntil(ctx, 30*time.Second, func() bool {
		return !lcb.ShouldFailover(htypes.ClusterA, nil)
	})
	if err != nil {
		return fmt.Errorf("breaker did not close after recovery: failures=%d: %w", lcb.Failures(htypes.ClusterA), err)
	}
	env.Logger.Info("Breaker closed")

	// 6. Writes and reads resume.
	writesBefore := env.Tracker.TotalWrites()
	readsBefore := env.Stats.ReadOK.Load()
	err = waitUntil(ctx, 10*time.Second, func() bool {
		return env.Tracker.TotalWrites() >= writesBefore+30 && env.Stats.ReadOK.Load() > readsBefore
	})
	if err != nil {
		return fmt.Errorf("traffic did not resume after the breaker closed: writes=+%d reads=+%d: %w",
			env.Tracker.TotalWrites()-writesBefore, env.Stats.ReadOK.Load()-readsBefore, err)
	}

	env.Logger.Info("ReadLegDeadlineTrip scenario completed")

	return nil
}
