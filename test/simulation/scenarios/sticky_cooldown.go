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

// StickyCooldown verifies the sticky read failover cooldown mechanism using a 3-phase test:
//  1. Fail Cluster A — StickyRead must switch preferred to B.
//  2. Immediately fail Cluster B inside the cooldown window — preferred must stay on B.
//  3. Wait for cooldown to expire, fail Cluster B again — StickyRead must switch back to A.
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
	env.ChaosA.SetErrorRate(0)

	// Phase 2: Immediately fail Cluster B while still inside the cooldown window.
	// StickyRead must stay on B because the cooldown has not expired.
	env.Logger.Info("Phase 2: failing Cluster B during cooldown — preferred must stay on B")
	env.ChaosB.SetErrorRate(1.0)

	// Allow reads to fail and call OnFailure several times while cooldown is active.
	select {
	case <-ctx.Done():
		env.ChaosB.SetErrorRate(0)
		return ctx.Err()
	case <-time.After(3 * time.Second):
	}

	if sr.Preferred() != htypes.ClusterB {
		env.ChaosB.SetErrorRate(0)
		return errors.New("StickyRead switched away from ClusterB during cooldown window")
	}
	env.Logger.Info("Phase 2: preferred correctly held on ClusterB during cooldown")
	env.ChaosB.SetErrorRate(0)

	// Phase 3: Wait for the cooldown window to expire, then fail Cluster B again.
	// StickyRead must switch back to A.
	const cooldown = 10 * time.Second
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
