package scenarios

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/arloliu/helix/internal/test/simulation/types"
	"github.com/arloliu/helix/policy"
	htypes "github.com/arloliu/helix/types"
)

// StickyCooldown verifies the sticky read failover cooldown mechanism using a 3-phase test:
//  1. Fail Cluster A — StickyRead must switch preferred to B.
//  2. Keep A failing and fail Cluster B inside the cooldown window —
//     preferred must stay on B while reads keep failing over to A.
//  3. Wait for cooldown to expire, fail Cluster B again — StickyRead must switch back to A.
//
// Phase 2 asserts what the cooldown guarantees:
// two clusters failing in turn do not swap the preference inside the cooldown.
// The cooldown does not hold the preference against a known-good other cluster.
// A cluster that has served a read since its own last failure is known good,
// and a failure on the preferred cluster moves the preference to it even inside the cooldown.
// That is why A stays failing through phase 2:
// a healthy A would serve B's failover reads and become known good.
//
// This scenario must run inside a strategy group that configures StickyRead with
// WithPreferredCluster(ClusterA) and a short WithStickyReadCooldown, otherwise
// phase 2 and 3 will not behave deterministically.
type StickyCooldown struct{}

func (s *StickyCooldown) Name() string {
	return "sticky-cooldown"
}

func (s *StickyCooldown) Description() string {
	return "Verifies sticky read cooldown: failover triggers, cooldown blocks re-failover, expires and re-failover succeeds"
}

func (s *StickyCooldown) Run(ctx context.Context, env *types.Environment) error {
	env.Logger.Info("Starting StickyCooldown scenario")

	// Matches WithStickyReadCooldown in the sticky-cooldown group.
	const cooldown = 10 * time.Second

	sr, ok := env.Client.Config().ReadStrategy.(*policy.StickyRead)
	if !ok || sr == nil {
		return errors.New("sticky-cooldown requires a StickyRead read strategy")
	}

	// Phase 1: Fail Cluster A — preferred must switch from A → B.
	env.Logger.Info("Phase 1: failing Cluster A to trigger StickyRead failover to B")
	env.ChaosA.SetErrorRate(1.0)

	if err := waitUntil(ctx, 15*time.Second, func() bool {
		return sr.Preferred() == htypes.ClusterB
	}); err != nil {
		env.ChaosA.SetErrorRate(0)
		return fmt.Errorf("StickyRead did not failover to ClusterB after ClusterA errors: %w", err)
	}
	switchTime := time.Now()
	env.Logger.Info("Phase 1: StickyRead switched to ClusterB")

	// Phase 2: fail Cluster B inside the cooldown window while A is still failing.
	// Neither cluster is known good, so the preference must stay on B.
	env.Logger.Info("Phase 2: failing Cluster B during cooldown with A still failing — preferred must stay on B")
	clearChaos := func() {
		env.ChaosA.SetErrorRate(0)
		env.ChaosB.SetErrorRate(0)
	}
	readErrBBefore := env.Metrics.GetReadErrors(htypes.ClusterB)
	readErrABefore := env.Metrics.GetReadErrors(htypes.ClusterA)
	env.ChaosB.SetErrorRate(1.0)

	// Allow reads to fail and call OnFailure several times while cooldown is active.
	select {
	case <-ctx.Done():
		clearChaos()
		return ctx.Err()
	case <-time.After(3 * time.Second):
	}

	if sr.Preferred() != htypes.ClusterB {
		clearChaos()
		return errors.New("StickyRead switched away from ClusterB during cooldown window while both clusters failed")
	}
	// The hold means nothing unless reads failed on B and failed over to A during the window.
	readErrB := env.Metrics.GetReadErrors(htypes.ClusterB) - readErrBBefore
	readErrA := env.Metrics.GetReadErrors(htypes.ClusterA) - readErrABefore
	if readErrB == 0 || readErrA == 0 {
		clearChaos()
		return fmt.Errorf("phase 2 did not exercise failover: read errors on B=%d, on A=%d", readErrB, readErrA)
	}
	if time.Since(switchTime) >= cooldown {
		clearChaos()
		return errors.New("phase 2 outlasted the cooldown window; the hold was not observed inside it")
	}
	env.Logger.Info("Phase 2: preferred correctly held on ClusterB during cooldown",
		"read_errors_b", readErrB, "read_errors_a", readErrA)
	clearChaos()

	// Phase 3: Wait for the cooldown window to expire, then fail Cluster B again.
	// StickyRead must switch back to A.
	if remaining := (cooldown + time.Second) - time.Since(switchTime); remaining > 0 {
		env.Logger.Info("Phase 3: waiting for cooldown to expire", "wait", remaining)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(remaining):
		}
	}

	env.Logger.Info("Phase 3: failing Cluster B after cooldown — StickyRead must switch to A")
	env.ChaosB.SetErrorRate(1.0)

	if err := waitUntil(ctx, 15*time.Second, func() bool {
		return sr.Preferred() == htypes.ClusterA
	}); err != nil {
		env.ChaosB.SetErrorRate(0)
		return fmt.Errorf("StickyRead did not switch back to ClusterA after cooldown expired: %w", err)
	}
	env.ChaosB.SetErrorRate(0)

	env.Logger.Info("StickyCooldown scenario completed successfully")

	return nil
}
