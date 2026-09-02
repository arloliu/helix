package helix

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/types"
)

// probeReporter is the capability subset of [policy.AdaptiveDualWrite] that
// the recovery probe goroutines need. Defined as a local interface so the root
// package does not import the policy package, and so custom write strategies
// that also implement IsDegraded + RecordProbeSuccess benefit from probing.
type probeReporter interface {
	IsDegraded(cluster types.ClusterID) bool
	RecordProbeSuccess(cluster types.ClusterID)
}

// startRecoveryProbes starts one background goroutine per cluster when the
// write strategy implements [probeReporter] (i.e. AdaptiveDualWrite or a
// custom strategy with equivalent methods) and the recovery probe has not
// been explicitly disabled via [WithRecoveryProbeDisabled]. Single-cluster
// mode is skipped because there is no second cluster to recover.
//
// If no RecoveryProbe is configured, the default probe (system.local read)
// is used. The goroutines are stopped by [CQLClient.Close].
func (c *CQLClient) startRecoveryProbes() {
	if c.config.recoveryProbeOff || c.singleCluster {
		return
	}
	pr, ok := c.config.WriteStrategy.(probeReporter)
	if !ok {
		return
	}
	p := c.config.RecoveryProbe
	if p == nil {
		def := DefaultRecoveryProbe()
		p = &def
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.recoveryProbeCtx = ctx
	c.recoveryProbeClose = cancel
	for _, cluster := range []ClusterID{ClusterA, ClusterB} {
		c.recoveryProbeWG.Go(func() { c.recoveryProbeLoop(cluster, pr, p) })
	}
}

// recoveryProbeLoop ticks at p.Interval and, while the cluster is degraded,
// executes the probe against its live session. A successful probe credits one
// recovery point; a failing probe is logged at debug and the cluster stays
// degraded. The loop exits when recoveryProbeCtx is cancelled (i.e. on Close).
func (c *CQLClient) recoveryProbeLoop(cluster ClusterID, pr probeReporter, p *RecoveryProbe) {
	// Metrics is immutable after construction, so resolve the optional
	// interface once. rpm is nil for collectors that do not opt in.
	rpm, _ := c.config.Metrics.(types.RecoveryProbeMetrics)

	ticker := time.NewTicker(p.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.recoveryProbeCtx.Done():
			return
		case <-ticker.C:
			if !pr.IsDegraded(cluster) {
				continue
			}
			ctx, cancel := context.WithTimeout(c.recoveryProbeCtx, p.Timeout)
			err := safeProbe(ctx, p.Probe, c.getSession(cluster))
			cancel()
			if err == nil {
				pr.RecordProbeSuccess(cluster)
				if rpm != nil {
					rpm.IncRecoveryProbeSuccess(cluster)
				}
				continue
			}
			if rpm != nil {
				rpm.IncRecoveryProbeFailure(cluster)
			}
			c.config.Logger.Debug("recovery probe failed",
				"cluster", c.clusterName(cluster), "error", err)
		}
	}
}

// safeProbe calls probe and recovers from panics, converting them to errors so
// recoveryProbeLoop can increment IncRecoveryProbeFailure without crashing.
func safeProbe(ctx context.Context, probe func(context.Context, cql.Session) error, session cql.Session) (err error) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			err = fmt.Errorf("helix: panic in recovery probe: %v\n%s", r, buf[:n])
		}
	}()

	return probe(ctx, session)
}
