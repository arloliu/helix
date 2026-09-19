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
//  3. Keep A failing and fail B within cooldown — each failed read on B
//     still fails over to A, and preferred stays on B.
//  4. Recover B, then A — reads go to B (still preferred from step 2).
//
// Phase 3 asserts what the cooldown guarantees:
// two clusters failing in turn do not swap the preference inside the cooldown,
// yet each request is still offered the other cluster.
// The cooldown does not hold the preference against a known-good other cluster.
// A cluster that has served a read since its own last failure is known good,
// and a failure on the preferred cluster moves the preference to it even inside the cooldown.
// That is why A stays failing through phase 3 and recovers only after B:
// a healthy A would serve B's failover reads and become known good.
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

	// Phase 3: fail B within cooldown while A is still failing.
	// OnFailure still returns A for each request,
	// but neither cluster is known good, so preferred must stay on B.
	env.Logger.Info("Phase 3: Failing Cluster B within cooldown with A still failing (preferred stays B)")
	clearChaos := func() {
		env.ChaosA.SetErrorRate(0)
		env.ChaosB.SetErrorRate(0)
	}
	readErrAMid := env.Metrics.GetReadErrors(htypes.ClusterA)
	env.ChaosB.SetErrorRate(1.0)

	// Reads failing on B must still be offered A: A's read errors keep rising.
	err = waitUntil(ctx, 5*time.Second, func() bool {
		return env.Metrics.GetReadErrors(htypes.ClusterA) > readErrAMid+5
	})
	if err != nil {
		clearChaos()
		return errors.New("reads did not fail over to Cluster A when B failed within cooldown")
	}

	// Preferred must stay on B — two failing clusters do not swap it inside the cooldown.
	if sr.Preferred() != htypes.ClusterB {
		clearChaos()
		return fmt.Errorf("preferred should remain ClusterB during cooldown, got %s", sr.Preferred())
	}
	env.Logger.Info("Phase 3: Reads failed over to A, preferred correctly held on B")

	// Phase 4: Recover B, then A — steady-state on B (still preferred).
	// B recovers first so that no read fails over to a healthy A.
	env.ChaosB.SetErrorRate(0)

	_, scanBRecov, _ := env.ChaosB.Counters()
	err = waitUntil(ctx, 10*time.Second, func() bool {
		_, scanBNow, _ := env.ChaosB.Counters()
		return scanBNow > scanBRecov+3
	})
	env.ChaosA.SetErrorRate(0)
	if err != nil {
		return errors.New("reads did not return to Cluster B after recovery")
	}
	if sr.Preferred() != htypes.ClusterB {
		return fmt.Errorf("preferred should remain ClusterB after recovery, got %s", sr.Preferred())
	}

	env.Logger.Info("StickyReadFailoverBack scenario completed successfully")

	return nil
}
