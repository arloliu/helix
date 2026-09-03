package helix

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/arloliu/helix/types"
)

// clusterHealth is the observation hub: every health observation about a
// cluster enters through one of its typed entry points, and it is the only
// writer of the session-liveness stats kept on each [sessionHolder].
//
// Each entry point forwards the observation to the authorities that own a
// decision about it (read strategy, failover policy, latency recorder,
// session liveness) in a fixed order and adds no translation between them:
// a write failure never reaches the failover policy, and a breaker opening
// never degrades writes. Classification happens before an observation
// arrives, where the caller still holds the live context, so the hub sees
// an already classified kind next to the original error.
//
// Stats live on the holder an attempt used, so a report that lands after a
// session swap updates the replaced holder and never the installed one;
// the routing authorities still hear it, because they describe the
// cluster rather than the session.
//
// Deliberately outside the hub: immediate query CAS (reports no health
// today), source-side mirror execution (its payloads name a logical sink,
// not a root cluster), and [ReadStrategy.OnFailure], which is a routing
// decision taken in the failover flow.
//
// Single-cluster exception: with no second cluster, a read success or
// failure updates only the stats, while an iterator's clean close still
// reaches the read strategy (as it always has) and nothing reaches the
// failover policy. The three entry points keep these historical rules
// rather than one uniform gate.
type clusterHealth struct {
	strategy ReadStrategy
	policy   FailoverPolicy
	latency  LatencyRecorder // policy as a LatencyRecorder, or nil
	metrics  types.MetricsCollector
	dual     bool         // false in single-cluster mode: no strategy or policy calls
	now      func() int64 // the client's NowProvider; see deferredWriteLeg for the one entry point that must not call it

	// countsForRefresh decides whether a failure is a connectivity failure
	// worth counting toward auto-refresh; see AutoRefreshConfig.FailureClassifier.
	countsForRefresh func(error) bool
}

// probeKind classifies a recovery probe outcome for the hub.
type probeKind uint8

const (
	probeOK       probeKind = iota
	probeFailed             // a cluster error, or the probe's own timeout
	probeCanceled           // the client's probe context ended (Close)
)

// newClusterHealth resolves the authorities once at construction.
func newClusterHealth(config *ClientConfig, dual bool) clusterHealth {
	h := clusterHealth{
		strategy:         config.ReadStrategy,
		policy:           config.FailoverPolicy,
		metrics:          config.Metrics,
		dual:             dual,
		now:              config.NowProvider,
		countsForRefresh: config.AutoRefresh.FailureClassifier,
	}
	if h.countsForRefresh == nil {
		h.countsForRefresh = DefaultAutoRefreshFailureClassifier
	}
	if recorder, ok := config.FailoverPolicy.(LatencyRecorder); ok {
		h.latency = recorder
	}

	return h
}

// readSucceeded reports a successful read attempt on cluster.
//
// Order: [ReadStrategy.OnSuccess] unless an override froze the strategy;
// then [LatencyRecorder.RecordLatency] when the policy implements it
// (the sample is its success signal, and calling RecordSuccess as well
// would erase the slow-read count a latency breaker keeps), otherwise
// [FailoverPolicy.RecordSuccess]; then the holder's stats.
// A single-cluster client updates only the stats.
func (h *clusterHealth) readSucceeded(holder *sessionHolder, cluster ClusterID, overrideActive bool, elapsed float64) {
	if h.dual {
		if !overrideActive && h.strategy != nil {
			h.strategy.OnSuccess(cluster)
		}
		switch {
		case h.latency != nil:
			h.latency.RecordLatency(cluster, time.Duration(elapsed*float64(time.Second)))
		case h.policy != nil:
			h.policy.RecordSuccess(cluster)
		}
	}
	holder.stats.succeeded(h.now())
}

// readFailed reports a read attempt on cluster that ended in a cluster
// error err of the given kind.
//
// Order: the read error metric; the holder's stats when kind is a cluster
// error; then [FailoverPolicy.RecordFailure]. A single-cluster client
// records no policy failure, because there is no cluster to fail over to.
// Data sentinels and caller-context errors never reach this entry point.
func (h *clusterHealth) readFailed(holder *sessionHolder, cluster ClusterID, kind readErrKind, err error) {
	h.metrics.IncReadError(cluster)
	if kind == readClusterErr {
		h.failedNow(holder, err)
	}
	if h.dual && h.policy != nil {
		h.policy.RecordFailure(cluster)
	}
}

// iterClosed reports the outcome of closing an iterator that read from
// cluster: a clean close is a success for the strategy and the policy, a
// cluster error is a failure for both, and data sentinels or a
// caller-context error are neither.
//
// Order (unchanged from before the hub existed): the holder's stats for
// every outcome except a caller-context error, then the strategy and the
// policy. The policy always receives RecordSuccess here, never
// RecordLatency, because an iterator has no single latency sample.
// The strategy's suggested alternative on failure is ignored: an iterator
// cannot be retried. A single-cluster client reports a clean close to the
// strategy but nothing to the policy, and reports failures to neither.
func (h *clusterHealth) iterClosed(holder *sessionHolder, cluster ClusterID, kind readErrKind, err error, overrideActive bool) {
	switch kind {
	case readOK:
		holder.stats.succeeded(h.now())
	case readClusterErr:
		h.failedNow(holder, err)
	case readNotFound, readRowLimit, readCallerNotFound, readCtxErr:
	}
	switch kind {
	case readOK:
		// A single-cluster client still tells the strategy about a clean
		// close (as it always has) but has no policy to credit.
		if !overrideActive && h.strategy != nil {
			h.strategy.OnSuccess(cluster)
		}
		if h.dual && h.policy != nil {
			h.policy.RecordSuccess(cluster)
		}
	case readClusterErr:
		if !h.dual {
			return
		}
		if h.policy != nil {
			h.policy.RecordFailure(cluster)
		}
		if !overrideActive && h.strategy != nil {
			h.strategy.OnFailure(cluster, err)
		}
	case readNotFound, readRowLimit, readCallerNotFound, readCtxErr:
	}
}

// writeLeg reports one leg of a write on cluster to the holder's stats at
// the caller's captured clock nowNano. An acknowledged leg is a success
// and a failed leg is a failure; an async, dropped, skipped, or
// caller-cancelled leg is neither. A nil holder means the leg never
// contacted a session.
func (h *clusterHealth) writeLeg(holder *sessionHolder, kind writeLegErrKind, err error, nowNano int64) {
	if holder == nil {
		return
	}
	switch kind {
	case legOK:
		holder.stats.succeeded(nowNano)
	case legFailed:
		h.failed(holder, err, nowNano)
	case legAsync, legDropped, legDraining, legSkipped, legCanceled:
	}
}

// deferredWriteLeg reports the late result of a background write leg (see
// [DeferredWriteResult]) to the holder's stats. It runs before the leg's
// deferred registration is released, which Close waits for, so it takes
// its timestamp from the process clock and never calls the configurable
// NowProvider or any other user code.
func (h *clusterHealth) deferredWriteLeg(holder *sessionHolder, kind writeLegErrKind, err error) {
	h.writeLeg(holder, kind, err, time.Now().UnixNano())
}

// probe reports a recovery probe outcome to the holder's stats: a
// successful probe is a success, a failed probe (including one ended by
// its own timeout) is a failure, and a probe the client cancelled records
// nothing.
func (h *clusterHealth) probe(holder *sessionHolder, kind probeKind, err error) {
	switch kind {
	case probeOK:
		holder.stats.succeeded(h.now())
	case probeFailed:
		h.failedNow(holder, err)
	case probeCanceled:
	}
}

// clusterTimeoutIfExpired wraps err with [types.ErrClusterTimeout] when
// ctx's own deadline ended the operation while parent is still live: a
// Helix-owned timeout, not the caller's or the client's cancellation.
func clusterTimeoutIfExpired(ctx, parent context.Context, err error) error {
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) && parent.Err() == nil {
		return fmt.Errorf("%w: %w", types.ErrClusterTimeout, err)
	}

	return err
}

// failed records a cluster failure at nowNano when the classifier counts
// it as a connectivity failure; a failure that proves the session
// reachable (a schema or query error) leaves the stats untouched.
func (h *clusterHealth) failed(holder *sessionHolder, err error, nowNano int64) {
	if h.countsForRefresh(err) {
		holder.stats.failed(err, nowNano)
	}
}

// failedNow is failed with the client clock, sampled only once the
// classifier has accepted the error.
func (h *clusterHealth) failedNow(holder *sessionHolder, err error) {
	if h.countsForRefresh(err) {
		holder.stats.failed(err, h.now())
	}
}

// succeeded records a successful operation against the session.
func (s *clusterStats) succeeded(nowNano int64) {
	s.consecutiveFailures.Store(0)
	s.lastSuccessNanos.Store(nowNano)
	// Steady-state lastErr is already nil; skip the redundant Store.
	if s.lastErr.Load() != nil {
		s.lastErr.Store(nil)
	}
}

// failed records a cluster failure against the session. err is kept for
// the SessionRefresher's lastErr.
func (s *clusterStats) failed(err error, nowNano int64) {
	s.consecutiveFailures.Add(1)
	s.lastFailureNanos.Store(nowNano)
	// Stable heap pointer for atomic.Pointer; the err interface itself is
	// two words and can't be stored directly.
	errCopy := err
	s.lastErr.Store(&errCopy)
}
