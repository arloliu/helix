package scenarios

import (
	"context"
	"time"

	"github.com/arloliu/helix/test/simulation/types"
)

type ReplaySaturation struct{}

func (s *ReplaySaturation) Name() string {
	return "replay-saturation"
}

func (s *ReplaySaturation) Description() string {
	return "Simulates high failure rate to saturate replay buffer"
}

func (s *ReplaySaturation) Run(ctx context.Context, env *types.Environment) error {
	env.Logger.Info("Starting ReplaySaturation scenario")

	// 1. Disconnect Cluster B
	env.Logger.Info("Disconnecting Cluster B")
	env.ChaosB.SetErrorRate(1.0)

	// 2. Wait for buffer to fill
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(30 * time.Second):
	}

	// 3. Recover
	env.Logger.Info("Recovering Cluster B")
	env.ChaosB.SetErrorRate(0.0)

	// 4. Wait for drain
	time.Sleep(10 * time.Second)

	env.Logger.Info("ReplaySaturation scenario completed")

	return nil
}
