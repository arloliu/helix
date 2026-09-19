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
	return "Flaps Cluster B mid-drain to verify adaptive recovery and lossless replay"
}

// Run flaps Cluster B three times.
// Each UP phase lasts until replay to B has made progress,
// so the next DOWN fails B while payloads from the previous outage may still be draining.
// After the last UP it requires the replay backlog to converge with no payload dropped
// and every payload enqueued for B replayed.
func (s *AdaptiveRecovery) Run(ctx context.Context, env *types.Environment) error {
	env.Logger.Info("Starting AdaptiveRecovery scenario")

	enqueuedBase := env.Metrics.GetReplayEnqueued(htypes.ClusterB)
	successBase := env.Metrics.GetReplaySuccess(htypes.ClusterB)
	droppedBase := env.Metrics.GetReplayDropped(htypes.ClusterB)

	// Flap Cluster B — keep each down phase long enough for the adaptive write
	// strategy to accumulate its strike threshold (default 3).
	for i := range 3 {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		env.Logger.Info(fmt.Sprintf("Flapping iteration %d: Cluster B DOWN", i+1),
			"replay_pending", env.MemReplayer.Len())
		env.ChaosB.SetErrorRate(1.0) // 100% errors
		downStart := env.Tracker.TotalWrites()
		// Wait until at least 20 write attempts have been made while B is down,
		// giving the adaptive strategy enough samples to react.
		if err := waitUntil(ctx, 5*time.Second, func() bool {
			return env.Tracker.TotalWrites() >= downStart+20
		}); err != nil {
			return fmt.Errorf("flap iteration %d: down-phase write gate not reached: %w", i+1, err)
		}

		env.Logger.Info(fmt.Sprintf("Flapping iteration %d: Cluster B UP", i+1))
		successAtUp := env.Metrics.GetReplaySuccess(htypes.ClusterB)
		env.ChaosB.SetErrorRate(0.0)
		// Stay up only until replay to B makes progress,
		// so the next outage lands on a drain that has started.
		if err := waitUntil(ctx, 10*time.Second, func() bool {
			return env.Metrics.GetReplaySuccess(htypes.ClusterB) > successAtUp
		}); err != nil {
			return fmt.Errorf("flap iteration %d: no replay to cluster B succeeded after recovery: %w", i+1, err)
		}
	}

	// Under the retained retry policy the queue holds a payload until it is replayed or dropped,
	// so an empty queue means the backlog has converged.
	if err := waitUntil(ctx, 30*time.Second, func() bool {
		return env.MemReplayer.Len() == 0
	}); err != nil {
		return fmt.Errorf("replay queue did not drain after the final recovery: pending=%d", env.MemReplayer.Len())
	}

	enqueued := env.Metrics.GetReplayEnqueued(htypes.ClusterB) - enqueuedBase
	succeeded := env.Metrics.GetReplaySuccess(htypes.ClusterB) - successBase
	dropped := env.Metrics.GetReplayDropped(htypes.ClusterB) - droppedBase
	env.Logger.Info("Replay to cluster B after flapping",
		"enqueued", enqueued, "succeeded", succeeded, "dropped", dropped)

	if dropped != 0 {
		return fmt.Errorf("replay dropped %d payloads for cluster B while it flapped", dropped)
	}
	if enqueued == 0 {
		return errors.New("no write to cluster B was enqueued for replay: the outages did not reach the write path")
	}
	if succeeded != enqueued {
		return fmt.Errorf("replay to cluster B succeeded %d times for %d enqueued payloads", succeeded, enqueued)
	}

	// Assert Cluster B recovered from degradation after the final UP cycle
	adw, _ := env.Client.Config().WriteStrategy.(*policy.AdaptiveDualWrite)
	if adw != nil && adw.IsDegraded(htypes.ClusterB) {
		return errors.New("cluster B still degraded after final recovery cycle")
	}

	env.Logger.Info("AdaptiveRecovery scenario completed")

	return nil
}
