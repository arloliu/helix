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

	adw, _ := env.Client.Config().WriteStrategy.(*policy.AdaptiveDualWrite)

	// 1. Baseline: Normal operation
	env.Logger.Info("Phase 1: Normal operation")
	_ = waitUntil(ctx, 5*time.Second, func() bool {
		return env.Tracker.Count() > startCount
	})

	// 2. Inject latency into Cluster A and snapshot counters
	env.Logger.Info("Phase 2: Injecting latency into Cluster A")
	env.ChaosA.SetLatency(500 * time.Millisecond)
	beforeExecA, _, _ := env.ChaosA.Counters()
	beforeExecB, _, _ := env.ChaosB.Counters()

	if err := waitUntil(ctx, 15*time.Second, func() bool {
		return env.Tracker.Count() >= startCount+100
	}); err != nil {
		return fmt.Errorf("write volume gate not reached before degradation check: %w", err)
	}

	// 3. Assert failover happened
	env.Logger.Info("Phase 3: Verifying failover behavior")
	afterExecA, _, _ := env.ChaosA.Counters()
	afterExecB, _, _ := env.ChaosB.Counters()
	deltaA := afterExecA - beforeExecA
	deltaB := afterExecB - beforeExecB
	env.Logger.Info("Exec delta during degradation", "cluster_a", deltaA, "cluster_b", deltaB)

	// Use the canonical degradation API rather than exec-count comparison.
	// Count-based checks are timing-sensitive: AdaptiveDualWrite fire-and-forget
	// goroutines for Cluster A complete asynchronously, so A and B often accumulate
	// similar counts within any fixed observation window.
	if adw != nil {
		if !adw.IsDegraded(htypes.ClusterA) {
			return errors.New("AdaptiveDualWrite did not degrade Cluster A after sustained high latency")
		}
		env.Logger.Info("Cluster A marked degraded (fire-and-forget mode active)")
	}

	// 4. Recovery
	env.Logger.Info("Phase 4: Recovering Cluster A")
	env.ChaosA.SetLatency(0)
	recoveryStart := env.Tracker.Count()
	_ = waitUntil(ctx, 10*time.Second, func() bool {
		return env.Tracker.Count() >= recoveryStart+50
	})
	if adw != nil && adw.IsDegraded(htypes.ClusterA) {
		env.Logger.Warn("AdaptiveDualWrite: Cluster A still degraded after recovery")
	}

	env.Logger.Info("DegradedCluster scenario completed")

	return nil
}
