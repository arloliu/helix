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

// AdaptiveRecovery simulates a flapping cluster to test adaptive recovery.
type AdaptiveRecovery struct{}

func (s *AdaptiveRecovery) Name() string {
	return "adaptive-recovery"
}

func (s *AdaptiveRecovery) Description() string {
	return "Simulates a flapping cluster to verify adaptive recovery mechanisms"
}

func (s *AdaptiveRecovery) Run(ctx context.Context, env *types.Environment) error {
	env.Logger.Info("Starting AdaptiveRecovery scenario")

	// Flap Cluster B — keep each down phase long enough for the adaptive write
	// strategy to accumulate its strike threshold (default 3).
	for i := 0; i < 3; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		env.Logger.Info(fmt.Sprintf("Flapping iteration %d: Cluster B DOWN", i+1))
		env.ChaosB.SetErrorRate(1.0) // 100% errors
		downStart := env.Tracker.Count()
		// Wait until at least 20 write attempts have been made while B is down,
		// giving the adaptive strategy enough samples to react.
		if err := waitUntil(ctx, 5*time.Second, func() bool {
			return env.Tracker.Count() >= downStart+20
		}); err != nil {
			return fmt.Errorf("flap iteration %d: down-phase write gate not reached: %w", i+1, err)
		}

		env.Logger.Info(fmt.Sprintf("Flapping iteration %d: Cluster B UP", i+1))
		env.ChaosB.SetErrorRate(0.0)
		upStart := env.Tracker.Count()
		_ = waitUntil(ctx, 10*time.Second, func() bool {
			return env.Tracker.Count() > upStart
		})
	}

	// Assert Cluster B recovered from degradation after the final UP cycle
	adw, _ := env.Client.Config().WriteStrategy.(*policy.AdaptiveDualWrite)
	if adw != nil && adw.IsDegraded(htypes.ClusterB) {
		return errors.New("cluster B still degraded after final recovery cycle")
	}

	env.Logger.Info("AdaptiveRecovery scenario completed")

	return nil
}
