package scenarios

import (
	"context"
	"fmt"
	"time"

	"github.com/arloliu/helix/test/simulation/types"
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

	// Flap Cluster B
	for i := 0; i < 3; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		env.Logger.Info(fmt.Sprintf("Flapping iteration %d: Cluster B DOWN", i+1))
		env.ChaosB.SetErrorRate(1.0) // 100% errors

		time.Sleep(3 * time.Second)

		env.Logger.Info(fmt.Sprintf("Flapping iteration %d: Cluster B UP", i+1))
		env.ChaosB.SetErrorRate(0.0)

		time.Sleep(5 * time.Second)
	}

	env.Logger.Info("AdaptiveRecovery scenario completed")

	return nil
}
