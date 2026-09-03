package helix

import (
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
	now      func() int64 // the client's NowProvider
}

// newClusterHealth resolves the authorities once at construction.
func newClusterHealth(config *ClientConfig, dual bool) clusterHealth {
	h := clusterHealth{
		strategy: config.ReadStrategy,
		policy:   config.FailoverPolicy,
		metrics:  config.Metrics,
		dual:     dual,
		now:      config.NowProvider,
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
		holder.stats.failed(err, h.now())
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
		holder.stats.failed(err, h.now())
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
		holder.stats.failed(err, nowNano)
	case legAsync, legDropped, legDraining, legSkipped, legCanceled:
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
