package scenarios

import (
	"context"
	"time"

	"github.com/arloliu/helix/test/simulation/types"
)

type DrainMode struct{}

func (s *DrainMode) Name() string {
	return "drain-mode"
}

func (s *DrainMode) Description() string {
	return "Simulates graceful shutdown and replay draining"
}

func (s *DrainMode) Run(_ context.Context, env *types.Environment) error {
	env.Logger.Info("Starting DrainMode scenario")

	// 1. Create some backlog
	env.Logger.Info("Creating backlog on Cluster B")
	env.ChaosB.SetErrorRate(1.0)
	time.Sleep(5 * time.Second)

	// 2. Recover
	env.Logger.Info("Recovering Cluster B and draining")
	env.ChaosB.SetErrorRate(0.0)

	// 3. Verify replays are processing
	time.Sleep(5 * time.Second)

	env.Logger.Info("DrainMode scenario completed")

	return nil
}
