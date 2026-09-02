package scenarios

import (
	"context"
	"fmt"
	"time"

	"github.com/arloliu/helix/test/simulation/types"
	htypes "github.com/arloliu/helix/types"
)

type ReplaySaturation struct{}

func (s *ReplaySaturation) Name() string {
	return "replay-saturation"
}

func (s *ReplaySaturation) Description() string {
	return "Simulates high failure rate to saturate replay buffer"
}

func (s *ReplaySaturation) Run(ctx context.Context, env *types.Environment) error {
	env.Logger.Info("Starting ReplaySaturation scenario")
	startCount := env.Tracker.TotalWrites()

	// 1. Disconnect Cluster B
	env.Logger.Info("Disconnecting Cluster B")
	env.ChaosB.SetErrorRate(1.0)

	// 2. Wait for writes to accumulate and replay queue to grow
	if err := waitUntil(ctx, 30*time.Second, func() bool {
		return env.Tracker.TotalWrites() >= startCount+250
	}); err != nil {
		return fmt.Errorf("write volume gate not reached before replay check: %w", err)
	}

	queueDepth := env.MemReplayer.Len()
	env.Logger.Info("Replay queue depth during outage", "depth", queueDepth)
	if queueDepth == 0 {
		env.Logger.Warn("Replay queue is empty during Cluster B outage — writes may not be failing as expected")
	}

	// 3. Recover Cluster B
	env.Logger.Info("Recovering Cluster B")
	env.ChaosB.SetErrorRate(0.0)

	// 4. Wait for replay queue to drain fully
	err := waitUntil(ctx, 30*time.Second, func() bool {
		return env.MemReplayer.Len() == 0
	})
	if err != nil {
		return fmt.Errorf("replay queue did not drain after recovery: pending=%d", env.MemReplayer.Len())
	}

	env.Logger.Info("Replay queue drained", "total_replayed", env.Metrics.GetTotalReplaySuccess())

	// An empty queue is not proof of convergence,
	// because the worker also empties it by giving up on payloads.
	// Any drop during the outage is data loss.
	if dropped := env.Metrics.GetReplayDropped(htypes.ClusterB); dropped > 0 {
		return fmt.Errorf("replay worker dropped %d payloads for cluster B during the outage", dropped)
	}

	// 5. Wait for additional writes to confirm throughput resumed
	drainEnd := env.Tracker.TotalWrites()
	_ = waitUntil(ctx, 15*time.Second, func() bool {
		return env.Tracker.TotalWrites() >= drainEnd+50
	})

	env.Logger.Info("ReplaySaturation scenario completed")

	return nil
}
