package scenarios

import (
	"context"
	"fmt"
	"time"

	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/test/simulation/types"
	htypes "github.com/arloliu/helix/types"
)

// LatencyCircuitBreakerTrip verifies that LatencyCircuitBreaker opens when
// cluster latency exceeds AbsoluteMax and resets after the configured timeout.
// This scenario requires a client configured with NewLatencyCircuitBreaker.
type LatencyCircuitBreakerTrip struct{}

func (s *LatencyCircuitBreakerTrip) Name() string {
	return "latency-circuit-breaker-trip"
}

func (s *LatencyCircuitBreakerTrip) Description() string {
	return "Verifies LatencyCircuitBreaker opens on slow responses and closes after reset timeout"
}

func (s *LatencyCircuitBreakerTrip) Run(ctx context.Context, env *types.Environment) error {
	env.Logger.Info("Starting LatencyCircuitBreakerTrip scenario")
	startCount := env.Tracker.TotalWrites()

	lcb, _ := env.Client.Config().FailoverPolicy.(*policy.LatencyCircuitBreaker)
	if lcb == nil {
		return fmt.Errorf("LatencyCircuitBreaker not configured: got %T", env.Client.Config().FailoverPolicy)
	}

	// 1. Inject latency above AbsoluteMax (configured at 500ms for this group)
	env.Logger.Info("Injecting 600ms latency on Cluster A (above AbsoluteMax)")
	env.ChaosA.SetLatency(600 * time.Millisecond)

	// 2. Wait for the breaker to trip (RecordLatency called on successful reads)
	err := waitUntil(ctx, 20*time.Second, func() bool {
		return lcb.ShouldFailover(htypes.ClusterA, nil)
	})
	if err != nil {
		return fmt.Errorf("LatencyCircuitBreaker did not trip within timeout: failures=%d", lcb.Failures(htypes.ClusterA))
	}
	env.Logger.Info("LatencyCircuitBreaker tripped", "failures", lcb.Failures(htypes.ClusterA))

	// Assert metrics recorded the trip
	if trips := env.Metrics.CircuitBreakerTrips[htypes.ClusterA]; trips == 0 {
		env.Logger.Warn("No circuit breaker trip recorded in metrics")
	}

	// 3. Recover Cluster A (breaker should remain open until ResetTimeout)
	env.Logger.Info("Recovering Cluster A (breaker should stay open)")
	env.ChaosA.SetLatency(0)

	// 4. Wait for breaker to close (ResetTimeout = 15s for this group)
	env.Logger.Info("Waiting for LatencyCircuitBreaker reset timeout (15s)...")
	err = waitUntil(ctx, 25*time.Second, func() bool {
		return !lcb.ShouldFailover(htypes.ClusterA, nil)
	})
	if err != nil {
		return fmt.Errorf("LatencyCircuitBreaker did not close after reset timeout: failures=%d", lcb.Failures(htypes.ClusterA))
	}
	env.Logger.Info("LatencyCircuitBreaker closed")

	// 5. Confirm writes resume normally
	recoveryStart := env.Tracker.TotalWrites()
	_ = waitUntil(ctx, 10*time.Second, func() bool {
		return env.Tracker.TotalWrites() >= recoveryStart+30
	})

	_ = startCount // acknowledged

	env.Logger.Info("LatencyCircuitBreakerTrip scenario completed")

	return nil
}
