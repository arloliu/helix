package helix

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/types"
)

// startRecoveryProbes starts one background goroutine per cluster when the
// write strategy implements [ProbeReporter] (i.e. AdaptiveDualWrite or a
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
	pr, ok := c.config.WriteStrategy.(ProbeReporter)
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
// recovery point; a failing probe leaves the cluster degraded.
// Consecutive failures are logged at Warn on the first failure and on every
// power-of-two count, and at Debug otherwise, so a long outage stays visible
// without a line per tick; the first success after failures is logged at Info.
// The loop exits when recoveryProbeCtx is cancelled (i.e. on Close).
func (c *CQLClient) recoveryProbeLoop(cluster ClusterID, pr ProbeReporter, p *RecoveryProbe) {
	// Metrics is immutable after construction, so resolve the optional
	// interface once. rpm is nil for collectors that do not opt in.
	rpm, _ := c.config.Metrics.(types.RecoveryProbeMetrics)
	// A latched cluster is the operator's decision; probing it is pointless.
	latch, _ := pr.(LatchReporter)
	// A strategy that judges probes by latency receives the elapsed time.
	byLatency, _ := pr.(ProbeLatencyReporter)

	var failures uint64
	ticker := time.NewTicker(p.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.recoveryProbeCtx.Done():
			return
		case <-ticker.C:
			if !pr.IsDegraded(cluster) || (latch != nil && latch.IsLatched(cluster)) {
				continue
			}
			holder := c.holderFor(cluster)
			ctx, cancel := context.WithTimeout(c.recoveryProbeCtx, p.Timeout)
			started := time.Now()
			err := safeProbe(ctx, p.Probe, holder.s)
			cancel()
			if err != nil && c.recoveryProbeCtx.Err() != nil {
				// The client cancelled the probe (Close): not a health
				// observation for anyone.
				c.health.probe(holder, probeCanceled, err)

				continue
			}
			// The probe's own deadline expiring is a connectivity failure
			// by provenance, like an expired write leg.
			err = clusterTimeoutIfExpired(ctx, c.recoveryProbeCtx, err)
			if err == nil {
				c.health.probe(holder, probeOK, nil)
				if byLatency != nil {
					byLatency.RecordProbeLatency(cluster, time.Since(started))
				} else {
					pr.RecordProbeSuccess(cluster)
				}
				if rpm != nil {
					rpm.IncRecoveryProbeSuccess(cluster)
				}
				if failures > 0 {
					c.config.Logger.Info("recovery probe succeeded",
						"cluster", c.clusterName(cluster), "failedProbes", failures)
					failures = 0
				}

				continue
			}
			c.health.probe(holder, probeFailed, err)
			if rpm != nil {
				rpm.IncRecoveryProbeFailure(cluster)
			}
			failures++
			c.logProbeFailure(cluster, err, failures)
		}
	}
}

// logProbeFailure reports one failed probe at Warn when the consecutive
// count is escalated (see [logging.Escalate]) and at Debug otherwise.
func (c *CQLClient) logProbeFailure(cluster ClusterID, err error, failures uint64) {
	log := c.config.Logger.Debug
	if logging.Escalate(failures) {
		log = c.config.Logger.Warn
	}
	log("recovery probe failed",
		"cluster", c.clusterName(cluster), "consecutiveFailures", failures, "error", err)
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
