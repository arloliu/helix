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
	startCount := env.Tracker.Count()

	// 1. Disconnect Cluster B
	env.Logger.Info("Disconnecting Cluster B")
	env.ChaosB.SetErrorRate(1.0)

	// 2. Wait for buffer to fill
	_ = waitUntil(ctx, 30*time.Second, func() bool {
		// Best-effort: ensure workload is still producing successful writes.
		return env.Tracker.Count() >= startCount+250
	})

	// 3. Recover
	env.Logger.Info("Recovering Cluster B")
	env.ChaosB.SetErrorRate(0.0)

	// 4. Wait for drain
	_ = waitUntil(ctx, 30*time.Second, func() bool {
		// After recovery, expect additional successful writes to resume.
		return env.Tracker.Count() >= startCount+500
	})

	env.Logger.Info("ReplaySaturation scenario completed")

	return nil
}
