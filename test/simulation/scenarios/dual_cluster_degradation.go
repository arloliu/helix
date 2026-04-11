package scenarios

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/arloliu/helix/test/simulation/types"
)

// DualClusterDegradation simulates simultaneous partial failures on both clusters.
// This exercises the DualClusterError path and surfaces the known P0 bug in
// AdaptiveDualWrite where both-async writes return DualClusterError with no
// replay enqueued.
type DualClusterDegradation struct{}

func (s *DualClusterDegradation) Name() string {
	return "dual-cluster-degradation"
}

func (s *DualClusterDegradation) Description() string {
	return "Degrades both clusters simultaneously to exercise the DualClusterError path"
}

func (s *DualClusterDegradation) Run(ctx context.Context, env *types.Environment) error {
	env.Logger.Info("Starting DualClusterDegradation scenario")

	// 1. Degrade both clusters with different partial error rates
	env.Logger.Info("Degrading both clusters simultaneously", "cluster_a_rate", 0.3, "cluster_b_rate", 0.5)
	env.ChaosA.SetErrorRate(0.3)
	env.ChaosB.SetErrorRate(0.5)

	// 2. Run under dual degradation. Use a fresh baseline so the wait gate
	//    measures writes that flow through the error-injected sessions, not
	//    pre-injection writes already accumulated.
	injectCount := env.Tracker.Count()
	_ = waitUntil(ctx, 15*time.Second, func() bool {
		return env.Tracker.Count() >= injectCount+200
	})

	dualErrors := env.Stats.DualClusterErr.Load()
	_, _, dropsA := env.ChaosA.Counters()
	_, _, dropsB := env.ChaosB.Counters()
	env.Logger.Info("Dual degradation stats",
		"dual_cluster_errors", dualErrors,
		"drops_a", dropsA,
		"drops_b", dropsB,
		"replay_queued", env.MemReplayer.Len(),
	)

	// With 30% and 50% drop rates on A and B simultaneously, the both-sync-fail
	// path must have triggered at least once (both clusters healthy, both writes
	// fail). Fail fast if it didn't — it means the DualClusterError path was
	// never exercised.
	if dualErrors == 0 {
		return errors.New("expected DualClusterError > 0 with both clusters degraded simultaneously")
	}

	// 3. Recover both clusters
	env.Logger.Info("Recovering both clusters")
	env.ChaosA.SetErrorRate(0)
	env.ChaosB.SetErrorRate(0)

	// 4. Wait for replay queue to drain
	if err := waitUntil(ctx, 30*time.Second, func() bool {
		return env.MemReplayer.Len() == 0
	}); err != nil {
		return fmt.Errorf("replay queue did not drain after dual-cluster recovery: pending=%d", env.MemReplayer.Len())
	}

	env.Logger.Info("DualClusterDegradation scenario completed",
		"total_replayed", env.Metrics.GetTotalReplaySuccess(),
	)

	return nil
}
