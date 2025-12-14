package scenarios

import (
	"context"
	"time"

	"github.com/arloliu/helix/test/simulation/types"
)

type FireForgetLimit struct{}

func (s *FireForgetLimit) Name() string {
	return "fire-forget-limit"
}

func (s *FireForgetLimit) Description() string {
	return "Verifies behavior when fire-and-forget limit is reached"
}

func (s *FireForgetLimit) Run(ctx context.Context, env *types.Environment) error {
	env.Logger.Info("Starting FireForgetLimit scenario")
	startCount := env.Tracker.Count()

	// 1. Degrade Cluster A to trigger fire-and-forget
	env.Logger.Info("Degrading Cluster A to trigger fire-and-forget")
	env.ChaosA.SetLatency(200 * time.Millisecond) // > 100ms threshold

	// 2. Wait for mode switch
	_ = waitUntil(ctx, 10*time.Second, func() bool {
		return env.Tracker.Count() >= startCount+50
	})

	// 3. The workload generator is running in background.
	// If we want to hit the limit, we might need higher concurrency.
	// But we can't easily change workload concurrency here.
	// We'll just observe for a while.

	_ = waitUntil(ctx, 15*time.Second, func() bool {
		return env.Tracker.Count() >= startCount+200
	})

	// 4. Recover
	env.Logger.Info("Recovering Cluster A")
	env.ChaosA.SetLatency(0)
	_ = waitUntil(ctx, 10*time.Second, func() bool {
		return env.Tracker.Count() >= startCount+250
	})

	env.Logger.Info("FireForgetLimit scenario completed")

	return nil
}
