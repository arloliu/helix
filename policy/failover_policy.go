package policy

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/types"
)

// ActiveFailover implements an aggressive failover policy.
//
// On any failure, immediately attempts failover to the secondary cluster.
type ActiveFailover struct{}

// NewActiveFailover creates a new ActiveFailover policy.
//
// Returns:
//   - *ActiveFailover: A new active failover policy
func NewActiveFailover() *ActiveFailover {
	return &ActiveFailover{}
}

// ShouldFailover always returns true for active failover.
//
// Parameters:
//   - cluster: The cluster that failed (unused)
//   - err: The error (unused)
//
// Returns:
//   - bool: Always true
func (a *ActiveFailover) ShouldFailover(_ types.ClusterID, _ error) bool {
	return true
}

// RecordFailure is a no-op for active failover.
//
// Parameters:
//   - cluster: The cluster that failed (unused)
func (a *ActiveFailover) RecordFailure(_ types.ClusterID) {}

// RecordSuccess is a no-op for active failover.
//
// Parameters:
//   - cluster: The cluster that succeeded (unused)
func (a *ActiveFailover) RecordSuccess(_ types.ClusterID) {}

// CircuitBreaker implements a conservative failover policy.
//
// Tracks consecutive failures and only triggers failover after a threshold
// is reached. This prevents flapping on transient errors.
//
// Concurrency model:
//   - muA / muB serialize all compound operations on cluster A / B state
//     (load lastFailure → decide reset-or-add → store failures → store timestamp).
//     Without this serialization the three atomic ops form a TOCTOU sequence
//     that can lose counts or emit duplicate metrics under concurrent callers.
//   - failuresA / failuresB remain atomic.Int32 so ShouldFailover can read
//     them lock-free on the hot path; a one-call lag on a transition is fine.
//   - trippedA / trippedB track whether the circuit has already tripped to
//     open; they are plain bools guarded by muA / muB respectively, ensuring
//     the "circuit opened" metric fires exactly once per trip.
//   - clusterNames is stored in an atomic.Pointer so SetClusterNames is safe
//     to call concurrently with RecordFailure / RecordSuccess.
type CircuitBreaker struct {
	threshold    int
	resetTimeout time.Duration
	metrics      types.MetricsCollector
	logger       types.Logger
	clusterNames atomic.Pointer[types.ClusterNames]

	muA          sync.Mutex   // serializes compound ops for cluster A
	trippedA     bool         // circuit open for A; guarded by muA
	failuresA    atomic.Int32 // lock-free for ShouldFailover; written under muA
	lastFailureA atomic.Int64 // Unix nano; written under muA

	muB          sync.Mutex   // serializes compound ops for cluster B
	trippedB     bool         // circuit open for B; guarded by muB
	failuresB    atomic.Int32 // lock-free for ShouldFailover; written under muB
	lastFailureB atomic.Int64 // Unix nano; written under muB
}

// CircuitBreakerOption configures a CircuitBreaker policy.
type CircuitBreakerOption func(*CircuitBreaker)

// WithThreshold sets the number of consecutive failures before failover.
//
// Parameters:
//   - n: Number of failures required
//
// Returns:
//   - CircuitBreakerOption: Configuration option
func WithThreshold(n int) CircuitBreakerOption {
	return func(c *CircuitBreaker) {
		c.threshold = n
	}
}

// WithResetTimeout sets the duration after which failure count resets.
//
// Parameters:
//   - d: Reset timeout duration
//
// Returns:
//   - CircuitBreakerOption: Configuration option
func WithResetTimeout(d time.Duration) CircuitBreakerOption {
	return func(c *CircuitBreaker) {
		c.resetTimeout = d
	}
}

// WithCircuitBreakerMetrics sets the metrics collector for the circuit breaker.
//
// Parameters:
//   - m: The metrics collector
//
// Returns:
//   - CircuitBreakerOption: Configuration option
func WithCircuitBreakerMetrics(m types.MetricsCollector) CircuitBreakerOption {
	return func(c *CircuitBreaker) {
		c.metrics = m
	}
}

// WithCircuitBreakerLogger sets the logger for the circuit breaker.
//
// Parameters:
//   - l: The logger
//
// Returns:
//   - CircuitBreakerOption: Configuration option
func WithCircuitBreakerLogger(l types.Logger) CircuitBreakerOption {
	return func(c *CircuitBreaker) {
		c.logger = l
	}
}

// WithCircuitBreakerClusterNames sets the cluster display names for log messages.
//
// Parameters:
//   - names: The cluster names
//
// Returns:
//   - CircuitBreakerOption: Configuration option
func WithCircuitBreakerClusterNames(names types.ClusterNames) CircuitBreakerOption {
	return func(c *CircuitBreaker) {
		c.clusterNames.Store(&names)
	}
}

// NewCircuitBreaker creates a new CircuitBreaker policy.
//
// Defaults: threshold=3, resetTimeout=30s
//
// Parameters:
//   - opts: Optional configuration options
//
// Returns:
//   - *CircuitBreaker: A new circuit breaker policy
func NewCircuitBreaker(opts ...CircuitBreakerOption) *CircuitBreaker {
	c := &CircuitBreaker{
		threshold:    3,
		resetTimeout: 30 * time.Second,
	}
	defaultNames := types.DefaultClusterNames()
	c.clusterNames.Store(&defaultNames)

	for _, opt := range opts {
		opt(c)
	}

	// Ensure metrics is never nil
	if c.metrics == nil {
		c.metrics = metrics.NewNopMetrics()
	}

	// Ensure logger is never nil
	if c.logger == nil {
		c.logger = logging.NewNopLogger()
	}

	return c
}

// ShouldFailover returns true if the failure threshold has been reached.
//
// Parameters:
//   - cluster: The cluster that failed
//   - err: The error (unused)
//
// Returns:
//   - bool: true if consecutive failures >= threshold
func (c *CircuitBreaker) ShouldFailover(cluster types.ClusterID, _ error) bool {
	var failures int32
	if cluster == types.ClusterA {
		failures = c.failuresA.Load()
	} else {
		failures = c.failuresB.Load()
	}
	return int(failures) >= c.threshold
}

// RecordFailure increments the failure counter for a cluster.
//
// If the reset timeout has passed since the last failure, the counter
// is reset to 1 instead of incrementing. The compound load-check-store
// sequence is serialized by a per-cluster mutex to prevent TOCTOU races
// under concurrent callers. The "circuit opened" metric is emitted at most
// once per trip (guarded by the tripped flag inside the mutex).
//
// Parameters:
//   - cluster: The cluster that failed
func (c *CircuitBreaker) RecordFailure(cluster types.ClusterID) {
	now := time.Now().UnixNano()
	var newFailures int32
	var justTripped bool

	if cluster == types.ClusterA {
		c.muA.Lock()
		lastFailure := c.lastFailureA.Load()
		if lastFailure > 0 && time.Duration(now-lastFailure) > c.resetTimeout {
			c.failuresA.Store(1)
			newFailures = 1
		} else {
			newFailures = c.failuresA.Add(1)
		}
		c.lastFailureA.Store(now)
		if int(newFailures) >= c.threshold && !c.trippedA {
			c.trippedA = true
			justTripped = true
		}
		c.muA.Unlock()
	} else {
		c.muB.Lock()
		lastFailure := c.lastFailureB.Load()
		if lastFailure > 0 && time.Duration(now-lastFailure) > c.resetTimeout {
			c.failuresB.Store(1)
			newFailures = 1
		} else {
			newFailures = c.failuresB.Add(1)
		}
		c.lastFailureB.Store(now)
		if int(newFailures) >= c.threshold && !c.trippedB {
			c.trippedB = true
			justTripped = true
		}
		c.muB.Unlock()
	}

	if justTripped {
		c.metrics.IncCircuitBreakerTrip(cluster)
		c.metrics.SetCircuitBreakerState(cluster, 2) // 2 = open
		c.logger.Warn("circuit breaker tripped",
			"cluster", c.clusterNames.Load().Name(cluster),
			"threshold", c.threshold,
		)
	}
}

// RecordSuccess resets the failure counter for a cluster.
//
// Parameters:
//   - cluster: The cluster that succeeded
func (c *CircuitBreaker) RecordSuccess(cluster types.ClusterID) {
	var wasOpen bool

	if cluster == types.ClusterA {
		c.muA.Lock()
		wasOpen = c.trippedA
		c.failuresA.Store(0)
		c.lastFailureA.Store(0)
		c.trippedA = false
		c.muA.Unlock()
	} else {
		c.muB.Lock()
		wasOpen = c.trippedB
		c.failuresB.Store(0)
		c.lastFailureB.Store(0)
		c.trippedB = false
		c.muB.Unlock()
	}

	// Record metrics when circuit closes
	if wasOpen {
		c.metrics.SetCircuitBreakerState(cluster, 0) // 0 = closed
		c.logger.Info("circuit breaker closed",
			"cluster", c.clusterNames.Load().Name(cluster),
		)
	}
}

// Failures returns the current failure count for a cluster.
//
// Parameters:
//   - cluster: The cluster to check
//
// Returns:
//   - int: Number of consecutive failures
func (c *CircuitBreaker) Failures(cluster types.ClusterID) int {
	if cluster == types.ClusterA {
		return int(c.failuresA.Load())
	}
	return int(c.failuresB.Load())
}

// SetClusterNames sets custom display names for clusters in log messages.
//
// This method is called by the client during initialization to propagate
// cluster names configured via WithClusterNames. It is safe to call
// concurrently with RecordFailure and RecordSuccess.
//
// Parameters:
//   - names: The cluster names to use in log messages
func (c *CircuitBreaker) SetClusterNames(names types.ClusterNames) {
	c.clusterNames.Store(&names)
}
