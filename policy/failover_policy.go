package policy

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/types"
)

const (
	defaultCircuitBreakerThreshold    = 3
	defaultCircuitBreakerResetTimeout = 30 * time.Second
)

// ActiveFailover implements an aggressive failover policy.
//
// On any failure, immediately attempts failover to the secondary cluster.
// ShouldFailover always returns true — every error triggers a failover attempt,
// with no delay, threshold, or backoff.
//
// WARNING: Oscillation risk. If both clusters are intermittently failing,
// ActiveFailover will flip-flop between them on every request, producing
// rapid and noisy failover transitions. In flaky dual-cluster scenarios,
// prefer [CircuitBreaker] or [LatencyCircuitBreaker] which require a
// threshold of consecutive failures before opening, dampening oscillation.
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
// Every error triggers an immediate failover attempt regardless of cause or
// frequency. See the [ActiveFailover] type documentation for the oscillation
// risk this implies when both clusters are degraded.
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
//
// Zero value: a bare CircuitBreaker{} never panics — every method is safe to
// call — but it is not functionally a circuit breaker: threshold is 0 and
// metrics/logger are nil interfaces until finalizeCircuitBreaker runs, which
// only happens inside NewCircuitBreaker / NewCircuitBreakerChecked. Use one
// of those constructors to get a fully configured, functional CircuitBreaker.
type CircuitBreaker struct {
	threshold    int
	resetTimeout time.Duration
	metrics      types.MetricsCollector
	logger       types.Logger
	clusterNames atomic.Pointer[types.ClusterNames]

	// metricsExplicit / loggerExplicit track whether the caller passed
	// WithCircuitBreakerMetrics / WithCircuitBreakerLogger (or the
	// LatencyCircuitBreaker equivalents). Without these flags, helix's
	// auto-inject pass would overwrite a caller's explicit choice with
	// the client-level collector / logger.
	metricsExplicit bool
	loggerExplicit  bool

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
		if n > maxInt32 {
			c.threshold = 0
			return
		}
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
		if m == nil {
			return
		}
		c.metrics = m
		c.metricsExplicit = true
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
		if l == nil {
			return
		}
		c.logger = l
		c.loggerExplicit = true
	}
}

// MetricsConfigured reports whether the metrics collector was explicitly
// set via [WithCircuitBreakerMetrics] (or [WithLatencyMetrics] for the
// embedded LatencyCircuitBreaker case). Helix's CQLClient uses this to
// detect a caller-passed policy without metrics and inject its own
// collector so circuit-breaker trips participate in unified instrumentation.
func (c *CircuitBreaker) MetricsConfigured() bool {
	return c.metricsExplicit
}

// SetMetrics replaces the metrics collector. No-op once
// [CircuitBreaker.MetricsConfigured] returns true (caller's explicit
// choice wins) or if m is nil.
func (c *CircuitBreaker) SetMetrics(m types.MetricsCollector) {
	if c.metricsExplicit {
		return
	}
	if m == nil {
		return
	}
	c.metrics = m
	c.metricsExplicit = true
}

// LoggerConfigured reports whether the logger was explicitly set via
// [WithCircuitBreakerLogger] / [WithLatencyLogger]. Mirrors
// MetricsConfigured so the helix client can use the same auto-injection
// guard for both knobs.
func (c *CircuitBreaker) LoggerConfigured() bool {
	return c.loggerExplicit
}

// SetLogger replaces the logger. No-op once
// [CircuitBreaker.LoggerConfigured] returns true (caller's explicit
// choice wins) or if l is nil.
func (c *CircuitBreaker) SetLogger(l types.Logger) {
	if c.loggerExplicit {
		return
	}
	if l == nil {
		return
	}
	c.logger = l
	c.loggerExplicit = true
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
//
// For production configuration that should fail fast on invalid option values,
// use [NewCircuitBreakerChecked].
func NewCircuitBreaker(opts ...CircuitBreakerOption) *CircuitBreaker {
	c := newCircuitBreakerWithDefaults()
	applyCircuitBreakerOptions(c, opts...)
	normalizeCircuitBreakerForLegacy(c)
	finalizeCircuitBreaker(c)

	return c
}

// NewCircuitBreakerChecked creates a new CircuitBreaker policy and returns a
// validation error when any option value is invalid.
//
// Parameters:
//   - opts: Optional configuration options
//
// Returns:
//   - *CircuitBreaker: A new circuit breaker policy
//   - error: Joined [types.OptionError] values when one or more options are invalid
func NewCircuitBreakerChecked(opts ...CircuitBreakerOption) (*CircuitBreaker, error) {
	c := newCircuitBreakerWithDefaults()
	applyCircuitBreakerOptions(c, opts...)
	if err := validateCircuitBreaker(c); err != nil {
		return nil, err
	}
	finalizeCircuitBreaker(c)

	return c, nil
}

func newCircuitBreakerWithDefaults() *CircuitBreaker {
	c := &CircuitBreaker{}
	initCircuitBreakerDefaults(c)

	return c
}

// initCircuitBreakerDefaults populates c's default field values in place.
//
// Initialization always happens through a pointer: CircuitBreaker holds
// sync.Mutex fields, so constructing a standalone value and copying it in
// would trip `go vet` copylocks. Any future caller that embeds
// CircuitBreaker by value must likewise initialize through a pointer to the
// embedded field rather than by assignment.
func initCircuitBreakerDefaults(c *CircuitBreaker) {
	c.threshold = defaultCircuitBreakerThreshold
	c.resetTimeout = defaultCircuitBreakerResetTimeout
	defaultNames := types.DefaultClusterNames()
	c.clusterNames.Store(&defaultNames)
}

func applyCircuitBreakerOptions(c *CircuitBreaker, opts ...CircuitBreakerOption) {
	for _, opt := range opts {
		opt(c)
	}
}

func validateCircuitBreaker(c *CircuitBreaker) error {
	errList := make([]error, 0, 3)

	if c.threshold <= 0 || c.threshold > maxInt32 {
		errList = append(errList, optionErrInt32Range(circuitBreakerComponent, "WithThreshold"))
	}
	if c.resetTimeout < 0 {
		errList = append(errList, optionErrNonNegativeDuration(circuitBreakerComponent, "WithResetTimeout"))
	}
	if names := c.clusterNames.Load(); names == nil {
		errList = append(errList, newOptionError(circuitBreakerComponent, "WithCircuitBreakerClusterNames", "cluster names cannot be nil"))
	} else if err := names.Validate(); err != nil {
		errList = append(errList, optionErrReasonFromErr(circuitBreakerComponent, "WithCircuitBreakerClusterNames", err))
	}

	return joinValidationErrors(errList)
}

func normalizeCircuitBreakerForLegacy(c *CircuitBreaker) {
	if c.threshold <= 0 || c.threshold > maxInt32 {
		c.threshold = defaultCircuitBreakerThreshold
	}
	if c.resetTimeout < 0 {
		c.resetTimeout = defaultCircuitBreakerResetTimeout
	}
	if names := c.clusterNames.Load(); names == nil || names.Validate() != nil {
		defaultNames := types.DefaultClusterNames()
		c.clusterNames.Store(&defaultNames)
	}
}

func finalizeCircuitBreaker(c *CircuitBreaker) {
	if c.metrics == nil {
		c.metrics = metrics.NewNopMetrics()
	}
	if c.logger == nil {
		c.logger = logging.NewNopLogger()
	}
}

// ShouldFailover returns true if the failure threshold has been reached
// AND the reset timeout has not yet elapsed since the last failure.
//
// Once the reset timeout passes, ShouldFailover returns false to allow a
// half-open probe attempt: the next operation will be routed to the failed
// cluster, and its outcome (RecordSuccess closes the breaker; RecordFailure
// resets the counter to 1 and accumulates again) determines what happens
// next. Without this transition, a tripped breaker stays open indefinitely
// for any caller that stops sending traffic to the failed cluster (e.g.
// StickyRead routing all reads to the survivor) — there is no path to
// closure because no probe ever fires.
//
// Note: this is "leaky" half-open — concurrent callers may all see false
// during the probe window and all be routed to the failed cluster. This is
// intentional: the per-cluster mutex in RecordFailure / RecordSuccess
// serializes the outcome, and at most (threshold) operations can fail
// against a still-broken cluster before the breaker re-trips.
//
// Parameters:
//   - cluster: The cluster that failed
//   - err: The error (unused)
//
// Returns:
//   - bool: true if failover should occur
func (c *CircuitBreaker) ShouldFailover(cluster types.ClusterID, _ error) bool {
	if !isKnownCluster(cluster) {
		return false
	}

	var failures int32
	var lastFailure int64
	if cluster == types.ClusterA {
		failures = c.failuresA.Load()
		lastFailure = c.lastFailureA.Load()
	} else {
		failures = c.failuresB.Load()
		lastFailure = c.lastFailureB.Load()
	}
	// threshold <= 0 means "never configured" (zero-value CircuitBreaker) or
	// an explicitly disabled breaker; either way it must never report a
	// failover, matching RecordFailure's `c.threshold > 0` guard on the trip
	// latch below — otherwise a zero-value breaker would report true here
	// despite never having (or being able to) record a trip.
	if c.threshold <= 0 || int(failures) < c.threshold {
		return false
	}
	// Half-open: allow a probe after resetTimeout elapses. resetTimeout=0
	// disables the timed transition (breaker stays open until explicit
	// RecordSuccess).
	if c.resetTimeout > 0 && lastFailure > 0 &&
		time.Duration(time.Now().UnixNano()-lastFailure) > c.resetTimeout {
		return false
	}

	return true
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
	if !isKnownCluster(cluster) {
		return
	}

	nowNs := time.Now().UnixNano()
	var newFailures int32
	var justTripped bool

	if cluster == types.ClusterA {
		c.muA.Lock()
		lastFailure := c.lastFailureA.Load()
		if c.resetTimeout > 0 && lastFailure > 0 && time.Duration(nowNs-lastFailure) > c.resetTimeout {
			// Half-open window expired without a recovery — counter resets
			// to 1 AND the trip latch clears so a re-trip on this cycle
			// fires IncCircuitBreakerTrip again. Without clearing trippedA,
			// observability undercounts trips across multi-cycle outages.
			//
			// resetTimeout=0 disables the timed transition (matches the
			// guard in ShouldFailover) — the breaker stays open until an
			// explicit RecordSuccess, so failures must keep accumulating.
			c.failuresA.Store(1)
			newFailures = 1
			c.trippedA = false
		} else {
			newFailures = c.failuresA.Add(1)
		}
		c.lastFailureA.Store(nowNs)
		if c.threshold > 0 && int(newFailures) >= c.threshold && !c.trippedA {
			c.trippedA = true
			justTripped = true
		}
		c.muA.Unlock()
	} else {
		c.muB.Lock()
		lastFailure := c.lastFailureB.Load()
		if c.resetTimeout > 0 && lastFailure > 0 && time.Duration(nowNs-lastFailure) > c.resetTimeout {
			c.failuresB.Store(1)
			newFailures = 1
			c.trippedB = false
		} else {
			newFailures = c.failuresB.Add(1)
		}
		c.lastFailureB.Store(nowNs)
		if c.threshold > 0 && int(newFailures) >= c.threshold && !c.trippedB {
			c.trippedB = true
			justTripped = true
		}
		c.muB.Unlock()
	}

	if justTripped {
		// Guarded: a zero-value CircuitBreaker (metrics/logger nil until
		// finalizeCircuitBreaker runs) must never reach here because
		// justTripped requires threshold > 0 above, but the nil checks stay
		// as a defensive backstop for any other path that sets threshold
		// without going through the constructors.
		if c.metrics != nil {
			c.metrics.IncCircuitBreakerTrip(cluster)
			c.metrics.SetCircuitBreakerState(cluster, 2) // 2 = open
		}
		if c.logger != nil {
			c.logger.Warn("circuit breaker tripped",
				"cluster", c.clusterName(cluster),
				"threshold", c.threshold,
			)
		}
	}
}

// RecordSuccess resets the failure counter for a cluster.
//
// Parameters:
//   - cluster: The cluster that succeeded
func (c *CircuitBreaker) RecordSuccess(cluster types.ClusterID) {
	if !isKnownCluster(cluster) {
		return
	}

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

	// Same defensive backstop as RecordFailure's trip block; see there for rationale.
	if wasOpen {
		if c.metrics != nil {
			c.metrics.SetCircuitBreakerState(cluster, 0) // 0 = closed
		}
		if c.logger != nil {
			c.logger.Info("circuit breaker closed",
				"cluster", c.clusterName(cluster),
			)
		}
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
	if cluster != types.ClusterB {
		return 0
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

// clusterName returns the display name for cluster, falling back to the raw
// ClusterID when clusterNames has not been initialized — i.e. a zero-value
// CircuitBreaker that never went through NewCircuitBreaker.
func (c *CircuitBreaker) clusterName(cluster types.ClusterID) string {
	return clusterNameOrID(&c.clusterNames, cluster)
}

// clusterNameOrID returns the display name for cluster from names, falling
// back to the raw ClusterID when names has not been initialized yet — i.e.
// a zero-value policy struct that never went through its constructor.
func clusterNameOrID(names *atomic.Pointer[types.ClusterNames], cluster types.ClusterID) string {
	if n := names.Load(); n != nil {
		return n.Name(cluster)
	}

	return string(cluster)
}
