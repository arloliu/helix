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

// PrimaryOnlyReadFailoverBack verifies that PrimaryOnlyRead recovers when the
// failover target (Cluster B) also fails and the original primary (Cluster A)
// has come back online.
//
// Phases:
//  1. Baseline — reads go to Cluster A.
//  2. Fail A — reads failover to B.
//  3. Recover A, then fail B — reads must failover back to A.
//  4. Recover B — steady-state resumes on A.
type PrimaryOnlyReadFailoverBack struct{}

func (s *PrimaryOnlyReadFailoverBack) Name() string {
	return "primary-only-read-failover-back"
}

func (s *PrimaryOnlyReadFailoverBack) Description() string {
	return "Verifies PrimaryOnlyRead fails over to B, then back to A when B fails and A recovered"
}

func (s *PrimaryOnlyReadFailoverBack) Run(ctx context.Context, env *types.Environment) error {
	env.Logger.Info("Starting PrimaryOnlyReadFailoverBack scenario")

	por, ok := env.Client.Config().ReadStrategy.(*policy.PrimaryOnlyRead)
	if !ok || por == nil {
		return fmt.Errorf("PrimaryOnlyRead not configured: got %T", env.Client.Config().ReadStrategy)
	}

	// Phase 1: Baseline — reads go to A.
	_, scanABefore, _ := env.ChaosA.Counters()
	env.Logger.Info("Phase 1: Baseline reads on Cluster A", "scan_a", scanABefore)

	err := waitUntil(ctx, 5*time.Second, func() bool {
		_, scanANow, _ := env.ChaosA.Counters()
		return scanANow > scanABefore+3
	})
	if err != nil {
		return errors.New("baseline: reads not arriving at Cluster A")
	}

	// Phase 2: Fail A — reads should shift to B.
	env.Logger.Info("Phase 2: Failing Cluster A")
	env.ChaosA.SetErrorRate(1.0)

	_, scanBBefore, _ := env.ChaosB.Counters()
	err = waitUntil(ctx, 10*time.Second, func() bool {
		_, scanBNow, _ := env.ChaosB.Counters()
		return scanBNow > scanBBefore+5
	})
	if err != nil {
		env.ChaosA.SetErrorRate(0)
		return errors.New("reads did not shift to Cluster B after Cluster A failure")
	}
	env.Logger.Info("Phase 2: Reads shifted to Cluster B")

	// Phase 3: Recover A, then fail B — reads must failover back to A.
	env.Logger.Info("Phase 3: Recovering A, then failing B")
	env.ChaosA.SetErrorRate(0)
	env.ChaosB.SetErrorRate(1.0)

	_, scanAMid, _ := env.ChaosA.Counters()
	err = waitUntil(ctx, 10*time.Second, func() bool {
		_, scanANow, _ := env.ChaosA.Counters()
		return scanANow > scanAMid+5
	})
	if err != nil {
		env.ChaosB.SetErrorRate(0)
		return errors.New("reads did not failover back to Cluster A after B failed")
	}

	// Verify strategy state is back to normal (not failed-over).
	if por.Select(ctx) != htypes.ClusterA {
		env.ChaosB.SetErrorRate(0)
		return errors.New("PrimaryOnlyRead still routing to B after failover-back to A")
	}

	env.Logger.Info("Phase 3: Reads successfully failed back to Cluster A")

	// Phase 4: Recover B — steady-state.
	env.ChaosB.SetErrorRate(0)

	env.Logger.Info("PrimaryOnlyReadFailoverBack scenario completed")

	return nil
}

// StickyReadFailoverBack verifies that StickyRead provides a failover target
// when the current preferred cluster fails within the cooldown window.
//
// Phases:
//  1. Baseline — reads go to the preferred cluster (A).
//  2. Fail A — StickyRead switches preferred to B, reads go to B.
//  3. Immediately fail B (within cooldown) — reads must still succeed by
//     falling back to A, even though preferred stays on B (cooldown blocks
//     the state change).
//  4. Recover both — reads go to B (still preferred from step 2).
type StickyReadFailoverBack struct{}

func (s *StickyReadFailoverBack) Name() string {
	return "sticky-read-failover-back"
}

func (s *StickyReadFailoverBack) Description() string {
	return "Verifies StickyRead provides failover target within cooldown when preferred cluster fails"
}

func (s *StickyReadFailoverBack) Run(ctx context.Context, env *types.Environment) error {
	env.Logger.Info("Starting StickyReadFailoverBack scenario")

	sr, ok := env.Client.Config().ReadStrategy.(*policy.StickyRead)
	if !ok || sr == nil {
		return errors.New("sticky-read-failover-back requires a StickyRead read strategy")
	}

	// Phase 1: Baseline — reads on preferred cluster (A).
	if sr.Preferred() != htypes.ClusterA {
		return fmt.Errorf("expected initial preferred to be ClusterA, got %s", sr.Preferred())
	}
	_, scanABefore, _ := env.ChaosA.Counters()
	err := waitUntil(ctx, 5*time.Second, func() bool {
		_, scanANow, _ := env.ChaosA.Counters()
		return scanANow > scanABefore+3
	})
	if err != nil {
		return errors.New("baseline: reads not arriving at Cluster A")
	}
	env.Logger.Info("Phase 1: Baseline reads on Cluster A confirmed")

	// Phase 2: Fail A — StickyRead must switch preferred to B.
	env.Logger.Info("Phase 2: Failing Cluster A")
	env.ChaosA.SetErrorRate(1.0)

	err = waitUntil(ctx, 15*time.Second, func() bool {
		return sr.Preferred() == htypes.ClusterB
	})
	if err != nil {
		env.ChaosA.SetErrorRate(0)
		return errors.New("StickyRead did not failover to ClusterB after ClusterA errors")
	}
	env.Logger.Info("Phase 2: StickyRead switched preferred to ClusterB")
	env.ChaosA.SetErrorRate(0)

	// Phase 3: Immediately fail B within cooldown — reads must still succeed.
	// The fix ensures OnFailure returns (ClusterA, true) even within cooldown,
	// allowing reads to fall back to A without changing preferred.
	env.Logger.Info("Phase 3: Failing Cluster B within cooldown (preferred stays B)")
	env.ChaosB.SetErrorRate(1.0)

	// Reads should fall back to A. Verify A receives reads.
	_, scanAMid, _ := env.ChaosA.Counters()
	err = waitUntil(ctx, 10*time.Second, func() bool {
		_, scanANow, _ := env.ChaosA.Counters()
		return scanANow > scanAMid+5
	})
	if err != nil {
		env.ChaosB.SetErrorRate(0)
		return errors.New("reads did not fall back to Cluster A when B failed within cooldown")
	}

	// Preferred must stay on B — cooldown blocks the state change.
	if sr.Preferred() != htypes.ClusterB {
		env.ChaosB.SetErrorRate(0)
		return fmt.Errorf("preferred should remain ClusterB during cooldown, got %s", sr.Preferred())
	}
	env.Logger.Info("Phase 3: Reads fell back to A, preferred correctly held on B")

	// Phase 4: Recover B — steady-state on B (still preferred).
	env.ChaosB.SetErrorRate(0)

	_, scanBRecov, _ := env.ChaosB.Counters()
	err = waitUntil(ctx, 10*time.Second, func() bool {
		_, scanBNow, _ := env.ChaosB.Counters()
		return scanBNow > scanBRecov+3
	})
	if err != nil {
		return errors.New("reads did not return to Cluster B after recovery")
	}

	env.Logger.Info("StickyReadFailoverBack scenario completed successfully")

	return nil
}
