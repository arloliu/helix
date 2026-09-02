package scenarios

import (
	"context"
	"fmt"
	"time"

	"github.com/arloliu/helix/test/simulation/types"
	htypes "github.com/arloliu/helix/types"
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
	startCount := env.Tracker.TotalWrites()

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
	_ = waitUntil(ctx, 10*time.Second, func() bool {
		return env.Tracker.TotalWrites() > startCount
	})

	// Every write acknowledged during the outage was backed only by the
	// replay queue; a dropped payload is a row that never reaches A.
	if dropped := env.Metrics.GetReplayDropped(htypes.ClusterA); dropped > 0 {
		return fmt.Errorf("replay worker dropped %d payloads for cluster A during a 15s outage", dropped)
	}
	env.Logger.Info("CompleteFailure scenario completed")

	return nil
}
