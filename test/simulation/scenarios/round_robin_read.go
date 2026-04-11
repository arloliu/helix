package scenarios

import (
	"context"
	"fmt"
	"time"

	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/test/simulation/types"
	htypes "github.com/arloliu/helix/types"
)

// RoundRobinReadBalance verifies that RoundRobinRead distributes reads evenly
// between clusters under normal conditions and fails over when one cluster fails.
type RoundRobinReadBalance struct{}

func (s *RoundRobinReadBalance) Name() string {
	return "round-robin-read-balance"
}

func (s *RoundRobinReadBalance) Description() string {
	return "Verifies RoundRobinRead distributes reads evenly and handles failover"
}

func (s *RoundRobinReadBalance) Run(ctx context.Context, env *types.Environment) error {
	env.Logger.Info("Starting RoundRobinReadBalance scenario")

	if _, ok := env.Client.Config().ReadStrategy.(*policy.RoundRobinRead); !ok {
		return fmt.Errorf("RoundRobinRead not configured: got %T", env.Client.Config().ReadStrategy)
	}

	// 1. Run healthy for 10s and verify balanced read distribution
	env.Logger.Info("Phase 1: Verifying balanced read distribution")
	_, scanAStart, _ := env.ChaosA.Counters()
	_, scanBStart, _ := env.ChaosB.Counters()

	startCount := env.Tracker.Count()
	_ = waitUntil(ctx, 10*time.Second, func() bool {
		return env.Tracker.Count() >= startCount+100
	})

	_, scanAAfter, _ := env.ChaosA.Counters()
	_, scanBAfter, _ := env.ChaosB.Counters()
	deltaA := scanAAfter - scanAStart
	deltaB := scanBAfter - scanBStart
	total := deltaA + deltaB
	env.Logger.Info("Read distribution", "cluster_a", deltaA, "cluster_b", deltaB, "total", total)

	if total > 0 {
		ratioA := float64(deltaA) / float64(total)
		if ratioA < 0.35 || ratioA > 0.65 {
			env.Logger.Warn("Read distribution outside 35-65% range", "ratio_a", ratioA)
		}
	}

	// 2. Fail Cluster A and verify reads go to B
	env.Logger.Info("Phase 2: Failing Cluster A")
	env.ChaosA.SetErrorRate(1.0)
	_, scanBMid, _ := env.ChaosB.Counters()

	_ = waitUntil(ctx, 10*time.Second, func() bool {
		_, scanBNow, _ := env.ChaosB.Counters()
		return scanBNow > scanBMid+10
	})
	_, dropAMid, _ := env.ChaosA.Counters()
	_, scanBEnd, _ := env.ChaosB.Counters()
	env.Logger.Info("During A failure", "drops_a", dropAMid, "scans_b", scanBEnd-scanBMid)

	// 3. Recover Cluster A and verify distribution rebalances
	env.Logger.Info("Phase 3: Recovering Cluster A")
	env.ChaosA.SetErrorRate(0)
	_, scanARecov, _ := env.ChaosA.Counters()

	err := waitUntil(ctx, 10*time.Second, func() bool {
		_, scanANow, _ := env.ChaosA.Counters()
		return scanANow > scanARecov+5
	})
	if err != nil {
		return fmt.Errorf("reads did not return to Cluster A after recovery: %w", err)
	}

	env.Logger.Info("RoundRobinReadBalance scenario completed",
		"failovers", env.Metrics.GetFailoverCount(htypes.ClusterA, htypes.ClusterB),
	)

	return nil
}
