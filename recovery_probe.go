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
// custom strategy with equivalent methods) or the failover policy implements
// [FailoverProbeReporter] (the built-in breakers), and the recovery probe
// has not been explicitly disabled via [WithRecoveryProbeDisabled].
// Single-cluster mode is skipped because there is no second cluster to
// recover.
//
// If no RecoveryProbe is configured, the default probe (system.local read)
// is used. The goroutines are stopped by [CQLClient.Close].
func (c *CQLClient) startRecoveryProbes() {
	if c.config.recoveryProbeOff || c.singleCluster {
		return
	}
	pr, fp := probeReporters(c.config)
	if pr == nil && fp == nil {
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
		c.recoveryProbeWG.Go(func() { c.recoveryProbeLoop(cluster, pr, fp, p) })
	}
}

// probeReporters resolves the two authorities a recovery probe can serve:
// the write strategy's degraded reporter and the failover policy's probe
// reservation; either may be nil.
func probeReporters(config *ClientConfig) (ProbeReporter, FailoverProbeReporter) {
	pr, _ := config.WriteStrategy.(ProbeReporter)
	fp, _ := config.FailoverPolicy.(FailoverProbeReporter)

	return pr, fp
}

// probeDemand records which authorities asked for one probe tick, so the
// outcome is reported only to those.
type probeDemand struct {
	write  bool   // the write strategy wants recovery credit
	policy bool   // the failover policy reserved the breaker
	token  uint64 // the policy's reservation token
}

// recoveryProbeLoop ticks at p.Interval and, whenever the write strategy
// reports the cluster degraded (and not latched by the operator) or the
// failover policy reserves a probe, runs one probe against the cluster's
// live session and reports the outcome to the authorities that asked.
// Consecutive failures are logged at Warn on the first failure and on every
// power-of-two count, and at Debug otherwise, so a long outage stays visible
// without a line per tick; the first success after failures is logged at Info.
// A probe the client cancels (on Close) is abandoned: no authority hears a
// failure and no health observation is recorded.
// The loop exits when recoveryProbeCtx is cancelled (i.e. on Close).
func (c *CQLClient) recoveryProbeLoop(cluster ClusterID, pr ProbeReporter, fp FailoverProbeReporter, p *RecoveryProbe) {
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
		}
		var demand probeDemand
		if pr != nil && pr.IsDegraded(cluster) && (latch == nil || !latch.IsLatched(cluster)) {
			demand.write = true
		}
		if fp != nil {
			demand.token, demand.policy = fp.TryBeginFailoverProbe(cluster)
		}
		if !demand.write && !demand.policy {
			continue
		}

		holder := c.holderFor(cluster)
		ctx, cancel := context.WithTimeout(c.recoveryProbeCtx, p.Timeout)
		started := time.Now()
		err := safeProbe(ctx, p.Probe, holder.s)
		// Classify before cancel, which would make every error look like
		// an expired context. The probe's own deadline expiring is a
		// connectivity failure by provenance, like an expired write leg.
		canceled := err != nil && c.recoveryProbeCtx.Err() != nil
		err = clusterTimeoutIfExpired(ctx, c.recoveryProbeCtx, err)
		cancel()
		if canceled {
			// The client cancelled the probe (Close): not a health
			// observation for anyone, and the breaker gets its
			// reservation back.
			if demand.policy {
				fp.CompleteFailoverProbe(cluster, demand.token, types.ProbeAbandoned)
			}
			c.health.probe(holder, probeCanceled, err)

			continue
		}
		if err == nil {
			if demand.write {
				if byLatency != nil {
					byLatency.RecordProbeLatency(cluster, time.Since(started))
				} else {
					pr.RecordProbeSuccess(cluster)
				}
			}
			if demand.policy {
				fp.CompleteFailoverProbe(cluster, demand.token, types.ProbeSucceeded)
			}
			c.health.probe(holder, probeOK, nil)
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
		if demand.policy {
			fp.CompleteFailoverProbe(cluster, demand.token, types.ProbeFailed)
		}
		c.health.probe(holder, probeFailed, err)
		if rpm != nil {
			rpm.IncRecoveryProbeFailure(cluster)
		}
		failures++
		c.logProbeFailure(cluster, err, failures)
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
