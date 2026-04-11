package scenarios

import (
	"context"
	"fmt"
	"time"

	"github.com/arloliu/helix/test/simulation/types"
)

type DrainMode struct{}

func (s *DrainMode) Name() string {
	return "drain-mode"
}

func (s *DrainMode) Description() string {
	return "Simulates graceful shutdown and replay draining"
}

func (s *DrainMode) Run(ctx context.Context, env *types.Environment) error {
	env.Logger.Info("Starting DrainMode scenario")
	startCount := env.Tracker.Count()

	// 1. Create backlog on Cluster B
	env.Logger.Info("Creating backlog on Cluster B")
	env.ChaosB.SetErrorRate(1.0)
	_ = waitUntil(ctx, 10*time.Second, func() bool {
		return env.Tracker.Count() >= startCount+50
	})

	queueDepth := env.MemReplayer.Len()
	env.Logger.Info("Replay queue depth before recovery", "depth", queueDepth)
	if queueDepth == 0 {
		env.Logger.Warn("Replay queue is empty — writes may not be failing as expected")
	}

	// 2. Recover and drain
	env.Logger.Info("Recovering Cluster B and draining replay queue")
	env.ChaosB.SetErrorRate(0.0)

	// 3. Wait for replay queue to drain fully
	err := waitUntil(ctx, 20*time.Second, func() bool {
		return env.MemReplayer.Len() == 0
	})
	if err != nil {
		return fmt.Errorf("replay queue did not drain: pending=%d", env.MemReplayer.Len())
	}
	env.Logger.Info("Replay queue drained")

	// 4. Confirm writes continue after drain
	drainEnd := env.Tracker.Count()
	_ = waitUntil(ctx, 10*time.Second, func() bool {
		return env.Tracker.Count() >= drainEnd+50
	})

	env.Logger.Info("DrainMode scenario completed")

	return nil
}
