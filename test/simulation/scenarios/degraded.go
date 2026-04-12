package scenarios

import (
	"context"
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

	// 2. Inject latency into Cluster A and snapshot counters for later diagnostics.
	env.Logger.Info("Phase 2: Injecting latency into Cluster A")
	env.ChaosA.SetLatency(500 * time.Millisecond)
	beforeExecA, _, _ := env.ChaosA.Counters()
	beforeExecB, _, _ := env.ChaosB.Counters()

	// 3. Wait for AdaptiveDualWrite to detect and degrade Cluster A.
	//
	// Poll the degradation flag directly instead of waiting for a fixed write count.
	// A write-volume gate is sensitive to Cassandra cold-start latency: on a fresh
	// container, Cluster B writes can take 100–300ms (not the assumed ~10ms), which
	// reduces post-degradation throughput to ~3–10 writes/sec and makes any fixed
	// count threshold unreliable. The degradation flag is the authoritative signal.
	env.Logger.Info("Phase 3: Waiting for AdaptiveDualWrite to degrade Cluster A")
	if adw != nil {
		if err := waitUntil(ctx, 30*time.Second, func() bool {
			return adw.IsDegraded(htypes.ClusterA)
		}); err != nil {
			afterExecA, _, _ := env.ChaosA.Counters()
			afterExecB, _, _ := env.ChaosB.Counters()
			env.Logger.Error("Cluster A not degraded within timeout",
				"delta_a", afterExecA-beforeExecA,
				"delta_b", afterExecB-beforeExecB,
			)
			return fmt.Errorf("AdaptiveDualWrite did not degrade Cluster A after sustained high latency: %w", err)
		}
		afterExecA, _, _ := env.ChaosA.Counters()
		afterExecB, _, _ := env.ChaosB.Counters()
		env.Logger.Info("Cluster A marked degraded (fire-and-forget mode active)",
			"delta_a", afterExecA-beforeExecA,
			"delta_b", afterExecB-beforeExecB,
		)
	} else {
		afterExecA, _, _ := env.ChaosA.Counters()
		afterExecB, _, _ := env.ChaosB.Counters()
		env.Logger.Info("Exec delta during degradation window",
			"cluster_a", afterExecA-beforeExecA,
			"cluster_b", afterExecB-beforeExecB,
		)
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
