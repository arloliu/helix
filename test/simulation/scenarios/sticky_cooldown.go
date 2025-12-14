package scenarios

import (
	"context"
	"time"

	"github.com/arloliu/helix/test/simulation/types"
)

type StickyCooldown struct{}

func (s *StickyCooldown) Name() string {
	return "sticky-cooldown"
}

func (s *StickyCooldown) Description() string {
	return "Verifies sticky read cooldown behavior"
}

func (s *StickyCooldown) Run(ctx context.Context, env *types.Environment) error {
	env.Logger.Info("Starting StickyCooldown scenario")
	startCount := env.Tracker.Count()

	// 1. Degrade Cluster A to force switch to B
	env.Logger.Info("Degrading Cluster A")
	env.ChaosA.SetLatency(500 * time.Millisecond) // High latency

	_ = waitUntil(ctx, 15*time.Second, func() bool {
		return env.Tracker.Count() >= startCount+100
	})

	// 2. Recover Cluster A
	env.Logger.Info("Recovering Cluster A")
	env.ChaosA.SetLatency(0)

	// 3. Wait during cooldown (traffic should stay on B)
	env.Logger.Info("Waiting during cooldown (traffic should stay on B)")
	_ = waitUntil(ctx, 35*time.Second, func() bool {
		return env.Tracker.Count() >= startCount+300
	})

	// 4. Wait for cooldown to expire (default 1m in quick.yaml)
	env.Logger.Info("Waiting for cooldown to expire...")
	_ = waitUntil(ctx, 45*time.Second, func() bool {
		return env.Tracker.Count() >= startCount+450
	})

	env.Logger.Info("StickyCooldown scenario completed")

	return nil
}
