package scenarios

import (
	"context"
	"time"

	"github.com/arloliu/helix/test/simulation/types"
)

// CompleteFailure simulates a complete failure of one cluster.
type CompleteFailure struct{}

func (s *CompleteFailure) Name() string {
	return "complete-failure"
}

func (s *CompleteFailure) Description() string {
	return "Simulates complete failure of one cluster to verify availability"
}

func (s *CompleteFailure) Run(ctx context.Context, env *types.Environment) error {
	env.Logger.Info("Starting CompleteFailure scenario")

	// Kill Cluster A completely
	env.Logger.Info("Killing Cluster A")
	env.ChaosA.SetErrorRate(1.0)

	// Run for duration
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(15 * time.Second):
	}

	// Recover
	env.Logger.Info("Recovering Cluster A")
	env.ChaosA.SetErrorRate(0.0)

	time.Sleep(5 * time.Second)
	env.Logger.Info("CompleteFailure scenario completed")

	return nil
}
