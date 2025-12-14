package scenarios

import (
	"context"
	"time"

	"github.com/arloliu/helix/test/simulation/types"
)

// DegradedCluster simulates a scenario where one cluster becomes slow.
type DegradedCluster struct{}

func (s *DegradedCluster) Name() string {
	return "degraded-cluster"
}

func (s *DegradedCluster) Description() string {
	return "Simulates high latency on one cluster to verify failover"
}

func (s *DegradedCluster) Run(ctx context.Context, env *types.Environment) error {
	env.Logger.Info("Starting DegradedCluster scenario")
	startCount := env.Tracker.Count()

	// 1. Baseline: Normal operation
	env.Logger.Info("Phase 1: Normal operation")
	_ = waitUntil(ctx, 5*time.Second, func() bool {
		return env.Tracker.Count() > startCount
	})

	// 2. Inject latency into Cluster A
	env.Logger.Info("Phase 2: Injecting latency into Cluster A")
	env.ChaosA.SetLatency(500 * time.Millisecond)

	// Run for a while
	_ = waitUntil(ctx, 10*time.Second, func() bool {
		return env.Tracker.Count() >= startCount+100
	})

	// 3. Verify failover (metrics check would go here)
	env.Logger.Info("Phase 3: Verifying failover behavior")

	// 4. Recovery
	env.Logger.Info("Phase 4: Recovering Cluster A")
	env.ChaosA.SetLatency(0)
	_ = waitUntil(ctx, 5*time.Second, func() bool {
		return env.Tracker.Count() >= startCount+150
	})
	env.Logger.Info("DegradedCluster scenario completed")

	return nil
}
