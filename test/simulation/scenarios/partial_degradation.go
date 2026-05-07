package scenarios

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/arloliu/helix/test/simulation/types"
)

// PartialDegradation simulates a sustained partial failure rate on one cluster.
// This is the first scenario to use non-binary chaos (30% drop rate vs. 0% or 100%).
// It verifies that the replay system compensates for intermittent failures and
// eventual consistency is maintained.
type PartialDegradation struct{}

func (s *PartialDegradation) Name() string {
	return "partial-degradation"
}

func (s *PartialDegradation) Description() string {
	return "Injects a 30% drop rate on Cluster A to verify replay compensates for partial failures"
}

func (s *PartialDegradation) Run(ctx context.Context, env *types.Environment) error {
	env.Logger.Info("Starting PartialDegradation scenario")
	startCount := env.Tracker.TotalWrites()
	_, _, dropsBefore := env.ChaosA.Counters()

	// 1. Inject 30% drop rate on Cluster A
	env.Logger.Info("Injecting 30% drop rate on Cluster A")
	env.ChaosA.SetErrorRate(0.3)

	// 2. Run for long enough to accumulate both drops and replay entries
	if err := waitUntil(ctx, 20*time.Second, func() bool {
		return env.Tracker.TotalWrites() >= startCount+200
	}); err != nil {
		return fmt.Errorf("write volume gate not reached before drop check: %w", err)
	}

	_, _, dropsAfter := env.ChaosA.Counters()
	drops := dropsAfter - dropsBefore
	env.Logger.Info("Partial degradation stats", "drops", drops, "replay_queued", env.MemReplayer.Len())

	if drops == 0 {
		return errors.New("expected some drops with 30% error rate on Cluster A, got 0")
	}

	// 3. Recover Cluster A
	env.Logger.Info("Recovering Cluster A")
	env.ChaosA.SetErrorRate(0)

	// 4. Wait for replay queue to drain fully
	err := waitUntil(ctx, 30*time.Second, func() bool {
		return env.MemReplayer.Len() == 0
	})
	if err != nil {
		return fmt.Errorf("replay queue did not drain after partial recovery: pending=%d", env.MemReplayer.Len())
	}

	env.Logger.Info("PartialDegradation scenario completed",
		"drops", drops,
		"total_replayed", env.Metrics.GetTotalReplaySuccess(),
	)

	return nil
}
