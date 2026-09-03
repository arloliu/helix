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
// States, per cluster:
//   - closed: failures are counted; at threshold the breaker trips open.
//   - open: ShouldFailover is true. The breaker closes on a successful
//     operation against the cluster. Once resetTimeout has elapsed since
//     the last failure a client's recovery probe may reserve the breaker
//     (see TryBeginFailoverProbe).
//   - half-open: a probe reservation is in flight; ShouldFailover stays
//     true and ordinary observations keep counting. CompleteFailoverProbe
//     closes the breaker, returns it to open, or releases the reservation.
//
// A breaker whose client never probes it (resetTimeout 0, probe disabled,
// single-cluster mode) stays open until a successful operation closes it.
//
// Concurrency model:
//   - Each cluster's breakerState.mu serializes every compound operation
//     on that cluster (load, decide, store). Without it the atomic ops form
//     a TOCTOU sequence that can lose counts or emit duplicate metrics.
//   - failures and open are atomics so ShouldFailover and VetoRoute read
//     them lock-free on the hot path; a one-call lag on a transition is fine.
//   - seq counts latched transitions per cluster and is incremented under
//     mu. A call captures the value its own transition produced and, once
//     the state mutex is released, writes the state gauge and the log line
//     only while that value is still current, so a goroutine descheduled
//     between latching and reporting cannot overwrite a fresher state.
//     A probe reservation is the same value: a completion settles the
//     breaker only while its reservation is the latest transition.
//   - reportMu makes that check and those writes a single step. It is
//     separate from mu on purpose: a caller-supplied metrics collector or
//     logger never runs while a state mutex is held, so a slow one cannot
//     stall state mutation or routing decisions. It is always released
//     before events are delivered. It is a plain sync.Mutex, not reentrant:
//     a collector or logger that synchronously calls back into the breaker
//     for the same cluster from inside the report deadlocks.
//   - clusterNames is stored in an atomic.Pointer so SetClusterNames is safe
//     to call concurrently with RecordFailure / RecordSuccess.
//
// Zero value: a bare CircuitBreaker{} never panics — every method is safe to
// call — but it is not functionally a circuit breaker: threshold is 0 and
// metrics/logger are nil interfaces until finalizeCircuitBreaker runs, which
// only happens inside NewCircuitBreaker / NewCircuitBreakerChecked. Use one
// of those constructors to get a fully configured, functional CircuitBreaker.
type CircuitBreaker struct {
	threshold              int
	resetTimeout           time.Duration
	failoverBelowThreshold bool
	metrics                types.MetricsCollector
	logger                 types.Logger
	clusterNames           atomic.Pointer[types.ClusterNames]

	// metricsExplicit / loggerExplicit track whether the caller passed
	// WithCircuitBreakerMetrics / WithCircuitBreakerLogger (or the
	// LatencyCircuitBreaker equivalents). Without these flags, helix's
	// auto-inject pass would overwrite a caller's explicit choice with
	// the client-level collector / logger.
	metricsExplicit bool
	loggerExplicit  bool

	stateA breakerState
	stateB breakerState

	// events queues open/close transitions for delivery to an optional
	// cluster event emitter. Its zero value is disabled. enqueue is
	// called only from inside a real transition, so a call that does not
	// change state never reaches it. On a real transition with no emitter
	// installed, the cost is enqueue's single atomic load plus drain()'s
	// CompareAndSwap and pending-queue check, since drain runs
	// unconditionally after every transition regardless of whether
	// enqueue appended anything.
	events eventOutbox
}

// breakerState is one cluster's breaker state. See the concurrency model
// on CircuitBreaker.
type breakerState struct {
	mu          sync.Mutex    // serializes compound ops
	tripped     bool          // open or half-open; guarded by mu
	halfOpen    bool          // a probe reservation is in flight; guarded by mu
	open        atomic.Bool   // lock-free snapshot of tripped for ShouldFailover and VetoRoute
	failures    atomic.Int32  // lock-free for Failures; written under mu
	lastFailure atomic.Int64  // Unix nano; written under mu
	seq         atomic.Uint64 // latched transitions; incremented under mu
	reportMu    sync.Mutex    // orders state-derived metric and log writes
}

// breakerTransition names what one call latched, for the report step.
type breakerTransition uint8

const (
	transitionNone     breakerTransition = iota
	transitionTripped                    // closed → open
	transitionClosed                     // open or half-open → closed
	transitionHalfOpen                   // open → half-open
	transitionReopened                   // half-open → open
)

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

// WithResetTimeout sets how long an open breaker waits after its last
// failure before the client's recovery probe may test the cluster (see
// [CircuitBreaker.TryBeginFailoverProbe]).
//
// Default: 30s
//
// Parameters:
//   - d: Reset timeout duration; 0 disables probing, so the breaker stays
//     open until a successful operation
//
// Returns:
//   - CircuitBreakerOption: Configuration option
func WithResetTimeout(d time.Duration) CircuitBreakerOption {
	return func(c *CircuitBreaker) {
		c.resetTimeout = d
	}
}

// WithFailoverBelowThreshold lets a failed read retry on the other cluster
// while the breaker is still closed.
//
// By default the first threshold-1 failures on a cluster reach the caller
// without a retry: the breaker only permits failover once it has tripped.
// With this option [CircuitBreaker.ShouldFailover] returns true for every
// failed read, so the caller sees a result from the healthy cluster
// instead of the failing cluster's error, while the threshold still
// governs when the breaker opens. A true result follows the ordinary
// failover path, including the read strategy's OnFailure, so a sticky
// read preference may move on the first failure.
//
// Default: false (the legacy v1 behaviour; a client logs a startup warning
// while it is in effect).
//
// Parameters:
//   - enabled: true to fail over below the threshold
//
// Returns:
//   - CircuitBreakerOption: Configuration option
func WithFailoverBelowThreshold(enabled bool) CircuitBreakerOption {
	return func(c *CircuitBreaker) {
		c.failoverBelowThreshold = enabled
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
//
// m's methods must not synchronously call back into RecordFailure or
// RecordSuccess on this breaker for the cluster they were invoked for —
// doing so deadlocks on the breaker's internal per-cluster report lock.
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
//
// l's methods must not synchronously call back into RecordFailure or
// RecordSuccess on this breaker for the cluster they were invoked for —
// doing so deadlocks on the breaker's internal per-cluster report lock.
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

// SetEventEmitter sets the cluster event emitter used for circuit
// breaker open/close notifications. helix.NewCQLClient injects this
// automatically when a WithOnClusterEvent handler is registered;
// standalone users may call it directly at any time, since the emitter
// reference is swapped atomically. Delivery is best-effort: a transition
// racing exactly with an emitter install or removal may not reach
// either the old or the new emitter.
//
// Emission ordering: an open or close transition is recorded, and its
// event appended to an internal queue, under that cluster's state
// mutex, so events for one cluster are delivered in the order their
// transitions occurred. The emitter itself is always invoked with no
// breaker locks held — see [types.ClusterEventEmitter] for the full
// (relaxed) contract an emitter must satisfy.
//
// Parameters:
//   - em: The event emitter; nil disables emission
func (c *CircuitBreaker) SetEventEmitter(em types.ClusterEventEmitter) {
	c.events.setEmitter(em)
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

// ShouldFailover reports whether a read that just failed on cluster may
// retry on the other cluster.
//
// It is side-effect free: unknown cluster → false; open or half-open →
// true; closed and below the threshold → the value of
// [WithFailoverBelowThreshold] (false by default, so the first
// threshold-1 errors reach the caller). A zero-value CircuitBreaker
// (threshold 0) never trips and reports false unless the option is set.
//
// Parameters:
//   - cluster: The cluster that failed
//   - err: The error (unused)
//
// Returns:
//   - bool: true if this request may fail over
func (c *CircuitBreaker) ShouldFailover(cluster types.ClusterID, _ error) bool {
	state := c.stateFor(cluster)
	if state == nil {
		return false
	}

	return state.open.Load() || c.failoverBelowThreshold
}

// FailoverBelowThreshold reports the [WithFailoverBelowThreshold] setting,
// so a client can warn while the legacy default is in effect.
//
// Returns:
//   - bool: true when a failed read below the threshold may fail over
func (c *CircuitBreaker) FailoverBelowThreshold() bool {
	return c.failoverBelowThreshold
}

// stateFor returns the per-cluster state, or nil for an unknown cluster.
func (c *CircuitBreaker) stateFor(cluster types.ClusterID) *breakerState {
	switch cluster {
	case types.ClusterA:
		return &c.stateA
	case types.ClusterB:
		return &c.stateB
	default:
		return nil
	}
}

// RecordFailure increments the failure counter for a cluster.
//
// At the threshold the breaker trips open, emitting
// [types.EventCircuitBreakerOpen] once per trip. An open or half-open
// breaker keeps counting and stamps the last failure, which restarts the
// reset timeout a probe reservation waits for; no other transition happens
// here. The compound load-check-store sequence is serialized by the
// cluster's mutex to prevent TOCTOU races under concurrent callers.
//
// Under concurrent callers the state gauge and the log line always describe
// the newest transition on that cluster: a call whose transition is
// superseded before it reports skips both, rather than putting back a state
// the breaker has already left. Its event is still delivered, in order.
//
// Parameters:
//   - cluster: The cluster that failed
func (c *CircuitBreaker) RecordFailure(cluster types.ClusterID) {
	state := c.stateFor(cluster)
	if state == nil {
		return
	}

	state.mu.Lock()
	failures := state.failures.Add(1)
	state.lastFailure.Store(time.Now().UnixNano())
	transition := transitionNone
	var seq uint64
	if c.threshold > 0 && int(failures) >= c.threshold && !state.tripped {
		state.tripped = true
		state.open.Store(true)
		transition = transitionTripped
		c.events.enqueue(types.ClusterEvent{
			Kind:    types.EventCircuitBreakerOpen,
			Cluster: cluster,
			Count:   int(failures),
		})
		seq = state.seq.Add(1)
	}
	state.mu.Unlock()

	c.report(state, cluster, transition, seq)
}

// RecordSuccess resets the failure counter for a cluster and closes an
// open or half-open breaker, emitting [types.EventCircuitBreakerClosed]
// with Reason "operation succeeded". A probe reservation that was in
// flight becomes stale and its completion is ignored.
//
// Parameters:
//   - cluster: The cluster that succeeded
func (c *CircuitBreaker) RecordSuccess(cluster types.ClusterID) {
	state := c.stateFor(cluster)
	if state == nil {
		return
	}
	if state.failures.Load() == 0 && !state.open.Load() {
		// Steady healthy state: nothing to reset and nothing to close. A
		// concurrent failure racing this check is at most one lost
		// success, the same one-call lag every lock-free read accepts.
		return
	}

	state.mu.Lock()
	transition, seq := c.closeLocked(state, cluster, "operation succeeded")
	state.mu.Unlock()

	c.report(state, cluster, transition, seq)
}

// closeLocked resets the counters and closes the breaker when it was open
// or half-open. The caller holds state.mu.
func (c *CircuitBreaker) closeLocked(state *breakerState, cluster types.ClusterID, reason string) (breakerTransition, uint64) {
	wasOpen := state.tripped
	state.failures.Store(0)
	state.lastFailure.Store(0)
	state.tripped = false
	state.halfOpen = false
	state.open.Store(false)
	if !wasOpen {
		return transitionNone, 0
	}
	c.events.enqueue(types.ClusterEvent{
		Kind:    types.EventCircuitBreakerClosed,
		Cluster: cluster,
		Reason:  reason,
	})

	return transitionClosed, state.seq.Add(1)
}

// TryBeginFailoverProbe reserves an open breaker for one recovery probe.
//
// The reservation succeeds when the breaker is open, no probe is already
// in flight, the reset timeout is positive, and it has elapsed since the
// last recorded failure. The breaker then reports half-open (gauge 1, a
// log line, no event) and returns a token the probe must pass to
// [CircuitBreaker.CompleteFailoverProbe]. A reset timeout of 0 never
// reserves: the breaker stays open until a successful operation.
//
// Parameters:
//   - cluster: The cluster to probe
//
// Returns:
//   - uint64: The reservation token
//   - bool: true when a probe should run now
func (c *CircuitBreaker) TryBeginFailoverProbe(cluster types.ClusterID) (uint64, bool) {
	state := c.stateFor(cluster)
	if state == nil || c.resetTimeout <= 0 || !state.open.Load() {
		// A closed breaker never reserves; the atomic check keeps the
		// steady-state tick lock-free.
		return 0, false
	}

	state.mu.Lock()
	elapsed := time.Duration(time.Now().UnixNano() - state.lastFailure.Load())
	if !state.tripped || state.halfOpen || elapsed <= c.resetTimeout {
		state.mu.Unlock()

		return 0, false
	}
	state.halfOpen = true
	seq := state.seq.Add(1)
	state.mu.Unlock()

	c.report(state, cluster, transitionHalfOpen, seq)

	return seq, true
}

// CompleteFailoverProbe settles the reservation token from
// [CircuitBreaker.TryBeginFailoverProbe].
//
// A stale token (the breaker moved on, for example an operation closed it)
// is ignored. [types.ProbeSucceeded] closes the breaker with Reason
// "probe succeeded". [types.ProbeFailed] returns it to open and restarts
// the reset timeout, emitting no event and counting no trip.
// [types.ProbeAbandoned] returns it to open without touching the counters
// or the timeout, so another client sharing the breaker may reserve it
// at once; an outcome this package does not know is treated the same way.
//
// Parameters:
//   - cluster: The cluster that was probed
//   - token: The reservation token
//   - outcome: What the probe found
func (c *CircuitBreaker) CompleteFailoverProbe(cluster types.ClusterID, token uint64, outcome types.ProbeOutcome) {
	state := c.stateFor(cluster)
	if state == nil {
		return
	}

	state.mu.Lock()
	if !state.halfOpen || state.seq.Load() != token {
		state.mu.Unlock()

		return
	}
	var transition breakerTransition
	var seq uint64
	switch outcome { //nolint:exhaustive // an unknown outcome is released like ProbeAbandoned
	case types.ProbeSucceeded:
		transition, seq = c.closeLocked(state, cluster, "probe succeeded")
	case types.ProbeFailed:
		state.halfOpen = false
		state.lastFailure.Store(time.Now().UnixNano())
		transition, seq = transitionReopened, state.seq.Add(1)
	default:
		// ProbeAbandoned, and any value this package does not know,
		// releases the reservation so the breaker cannot stay half-open.
		state.halfOpen = false
		transition, seq = transitionReopened, state.seq.Add(1)
	}
	state.mu.Unlock()

	c.report(state, cluster, transition, seq)
}

// report writes the metrics and log side effects of the transition a call
// latched, then delivers the queued events last, so a handler that
// reenters and changes the breaker mid-delivery cannot observe a metrics
// gauge that disagrees with the state both calls settle on.
//
// The trip counter is cumulative rather than state-derived, so it is
// incremented even by a superseded call: the trip did happen, and skipping
// it would undercount trips across a burst.
//
// The state gauge and the log line are skipped when the sequence has moved
// on, because another goroutine has latched a newer transition for this
// cluster and its own report describes where the breaker ended up. The
// report mutex makes the check and the writes one step; it is not the
// state mutex, so a caller-supplied collector or logger never blocks state
// mutation, and it is released before events are delivered so a reentrant
// emitter cannot deadlock on it.
//
// The nil checks are a defensive backstop for a zero-value CircuitBreaker
// (metrics/logger nil until finalizeCircuitBreaker runs).
func (c *CircuitBreaker) report(state *breakerState, cluster types.ClusterID, transition breakerTransition, seq uint64) {
	if transition == transitionNone {
		return
	}
	if transition == transitionTripped && c.metrics != nil {
		c.metrics.IncCircuitBreakerTrip(cluster)
	}

	state.reportMu.Lock()
	if state.seq.Load() == seq {
		c.reportTransition(cluster, transition)
	}
	state.reportMu.Unlock()

	c.events.drain()
}

// reportTransition writes the state gauge and log line for one transition.
// MUST be called from report, which holds the cluster's report mutex and
// has confirmed the transition is still the newest one.
func (c *CircuitBreaker) reportTransition(cluster types.ClusterID, transition breakerTransition) {
	switch transition {
	case transitionTripped:
		if c.metrics != nil {
			c.metrics.SetCircuitBreakerState(cluster, 2) // 2 = open
		}
		if c.logger != nil {
			c.logger.Warn("circuit breaker tripped",
				"cluster", c.clusterName(cluster),
				"threshold", c.threshold,
			)
		}
	case transitionClosed:
		if c.metrics != nil {
			c.metrics.SetCircuitBreakerState(cluster, 0) // 0 = closed
		}
		if c.logger != nil {
			c.logger.Info("circuit breaker closed",
				"cluster", c.clusterName(cluster),
			)
		}
	case transitionHalfOpen:
		if c.metrics != nil {
			c.metrics.SetCircuitBreakerState(cluster, 1) // 1 = half-open
		}
		if c.logger != nil {
			c.logger.Info("circuit breaker half-open: recovery probe reserved",
				"cluster", c.clusterName(cluster),
			)
		}
	case transitionReopened:
		if c.metrics != nil {
			c.metrics.SetCircuitBreakerState(cluster, 2) // 2 = open
		}
		if c.logger != nil {
			c.logger.Info("circuit breaker stays open: recovery probe did not succeed",
				"cluster", c.clusterName(cluster),
			)
		}
	case transitionNone:
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
	state := c.stateFor(cluster)
	if state == nil {
		return 0
	}

	return int(state.failures.Load())
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
