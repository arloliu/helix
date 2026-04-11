package scenarios

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/test/simulation/types"
	htypes "github.com/arloliu/helix/types"
)

type CircuitBreakerTrip struct{}

func (s *CircuitBreakerTrip) Name() string {
	return "circuit-breaker-trip"
}

func (s *CircuitBreakerTrip) Description() string {
	return "Verifies circuit breaker opens after failures and resets after timeout"
}

func (s *CircuitBreakerTrip) Run(ctx context.Context, env *types.Environment) error {
	env.Logger.Info("Starting CircuitBreakerTrip scenario")

	cb, ok := env.Client.Config().FailoverPolicy.(*policy.CircuitBreaker)
	if !ok || cb == nil {
		return errors.New("CircuitBreakerTrip requires a CircuitBreaker failover policy")
	}

	// 1. Induce failures in Cluster A to trip the breaker
	env.Logger.Info("Inducing failures in Cluster A to trip breaker")
	env.ChaosA.SetErrorRate(1.0)

	// 2. Wait for the breaker to trip (ShouldFailover returns true)
	err := waitUntil(ctx, 10*time.Second, func() bool {
		return cb.ShouldFailover(htypes.ClusterA, nil)
	})
	if err != nil {
		return fmt.Errorf("circuit breaker did not trip within timeout: failures=%d", cb.Failures(htypes.ClusterA))
	}
	env.Logger.Info("Circuit breaker tripped", "failures", cb.Failures(htypes.ClusterA))

	// 3. Assert metrics recorded the trip
	if trips := env.Metrics.CircuitBreakerTrips[htypes.ClusterA]; trips == 0 {
		return errors.New("CircuitBreaker tripped but no trip recorded in metrics")
	}

	// 4. Recover Cluster A (breaker should remain open until reset timeout)
	env.Logger.Info("Recovering Cluster A (breaker should still be open)")
	env.ChaosA.SetErrorRate(0.0)

	// 5. Wait for the breaker to close (ShouldFailover returns false)
	env.Logger.Info("Waiting for Circuit Breaker reset timeout...")
	err = waitUntil(ctx, 45*time.Second, func() bool {
		return !cb.ShouldFailover(htypes.ClusterA, nil)
	})
	if err != nil {
		return fmt.Errorf("circuit breaker did not close after reset timeout: failures=%d", cb.Failures(htypes.ClusterA))
	}
	env.Logger.Info("Circuit breaker closed")

	env.Logger.Info("CircuitBreakerTrip scenario completed")

	return nil
}
