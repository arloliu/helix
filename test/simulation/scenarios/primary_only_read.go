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

// PrimaryOnlyReadRecovery verifies that PrimaryOnlyRead fails over to Cluster B
// when Cluster A fails, and automatically returns to Cluster A after the
// configured recovery timeout.
type PrimaryOnlyReadRecovery struct{}

func (s *PrimaryOnlyReadRecovery) Name() string {
	return "primary-only-read-recovery"
}

func (s *PrimaryOnlyReadRecovery) Description() string {
	return "Verifies PrimaryOnlyRead fails over to B and auto-recovers to A after timeout"
}

func (s *PrimaryOnlyReadRecovery) Run(ctx context.Context, env *types.Environment) error {
	env.Logger.Info("Starting PrimaryOnlyReadRecovery scenario")
	startCount := env.Tracker.TotalWrites()

	por, _ := env.Client.Config().ReadStrategy.(*policy.PrimaryOnlyRead)
	if por == nil {
		return fmt.Errorf("PrimaryOnlyRead not configured: got %T", env.Client.Config().ReadStrategy)
	}

	// 1. Baseline: reads should go to Cluster A (primary)
	_ = waitUntil(ctx, 5*time.Second, func() bool {
		return env.Tracker.TotalWrites() > startCount
	})
	_, scanABefore, _ := env.ChaosA.Counters()
	env.Logger.Info("Baseline scan count on A", "scan_a", scanABefore)

	// 2. Fail Cluster A completely
	env.Logger.Info("Failing Cluster A (100% errors)")
	env.ChaosA.SetErrorRate(1.0)

	// 3. Wait for reads to shift to B
	_, scanBBefore, _ := env.ChaosB.Counters()
	err := waitUntil(ctx, 10*time.Second, func() bool {
		_, scanBNow, _ := env.ChaosB.Counters()
		return scanBNow > scanBBefore+5
	})
	if err != nil {
		return errors.New("reads did not shift to Cluster B after Cluster A failure")
	}
	env.Logger.Info("Reads shifted to Cluster B")

	// 4. Recover Cluster A
	env.Logger.Info("Recovering Cluster A")
	env.ChaosA.SetErrorRate(0)

	// 5. Wait for the recovery timeout (configured at 10s for this strategy group)
	env.Logger.Info("Waiting for PrimaryOnlyRead recovery timeout (10s)...")
	_, scanAMid, _ := env.ChaosA.Counters()
	err = waitUntil(ctx, 20*time.Second, func() bool {
		_, scanANow, _ := env.ChaosA.Counters()
		return scanANow > scanAMid+5
	})
	if err != nil {
		return errors.New("reads did not return to Cluster A after recovery timeout")
	}

	// Assert the PrimaryOnlyRead state is cleared
	if por.Select(ctx) != htypes.ClusterA {
		return errors.New("PrimaryOnlyRead still routing to B after recovery")
	}

	env.Logger.Info("PrimaryOnlyReadRecovery scenario completed")

	return nil
}
