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
//   - seqA / seqB count latched transitions per cluster and are incremented
//     under muA / muB. A call captures the value its own transitions produced
//     and, once the state mutex is released, writes the state gauge and the
//     transition log line only while that value is still current. Without it
//     a goroutine descheduled between latching a transition and reporting it
//     could overwrite a fresher state with a stale one.
//   - reportMuA / reportMuB make that check and those writes a single step.
//     They are separate from muA / muB on purpose: a caller-supplied metrics
//     collector or logger never runs while a state mutex is held, so a slow
//     one cannot stall state mutation or routing decisions. They are always
//     released before events are delivered. They are plain sync.Mutex values,
//     not reentrant: a metrics collector or logger that synchronously calls
//     back into RecordFailure or RecordSuccess for the same cluster, from
//     inside the very call the report mutex is serializing, deadlocks trying
//     to reacquire it.
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

	muA          sync.Mutex    // serializes compound ops for cluster A
	trippedA     bool          // circuit open for A; guarded by muA
	openA        atomic.Bool   // lock-free snapshot of trippedA, read by LatencyCircuitBreaker.VetoRoute
	failuresA    atomic.Int32  // lock-free for ShouldFailover; written under muA
	lastFailureA atomic.Int64  // Unix nano; written under muA
	seqA         atomic.Uint64 // latched transitions for A; incremented under muA
	reportMuA    sync.Mutex    // orders A's state-derived metric and log writes

	muB          sync.Mutex    // serializes compound ops for cluster B
	trippedB     bool          // circuit open for B; guarded by muB
	openB        atomic.Bool   // lock-free snapshot of trippedB, read by LatencyCircuitBreaker.VetoRoute
	failuresB    atomic.Int32  // lock-free for ShouldFailover; written under muB
	lastFailureB atomic.Int64  // Unix nano; written under muB
	seqB         atomic.Uint64 // latched transitions for B; incremented under muB
	reportMuB    sync.Mutex    // orders B's state-derived metric and log writes

	// events queues open/close transitions for delivery to an optional
	// cluster event emitter. Its zero value is disabled. enqueue is
	// called only from inside the trip and close branches of
	// RecordFailure / RecordSuccess, so a call that does not actually
	// trip or close the breaker never reaches it. On a real transition
	// with no emitter installed, the cost is enqueue's single atomic
	// load plus drain()'s CompareAndSwap and pending-queue check, since
	// drain runs unconditionally after every transition regardless of
	// whether enqueue appended anything.
	events eventOutbox
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

// setTripped changes a cluster's open latch and its lock-free snapshot
// together, so VetoRoute always mirrors the latched state.
// The caller holds the cluster's mutex.
func setTripped(tripped *bool, open *atomic.Bool, v bool) {
	*tripped = v
	open.Store(v)
}

// RecordFailure increments the failure counter for a cluster.
//
// If the reset timeout has passed since the last failure, the counter
// is reset to 1 instead of incrementing. The compound load-check-store
// sequence is serialized by a per-cluster mutex to prevent TOCTOU races
// under concurrent callers. The "circuit opened" metric is emitted at most
// once per trip (guarded by the tripped flag inside the mutex).
//
// A breaker that was open when the reset timeout elapsed closes on that
// same call: it emits [types.EventCircuitBreakerClosed] with Reason
// "reset timeout elapsed" and returns the state gauge to closed. With
// threshold 1 the reset also brings the counter straight back to the
// threshold, so the call closes and re-opens, emitting Closed then Open
// and leaving the gauge open.
//
// Under concurrent callers the state gauge and the log line always describe
// the newest transition on that cluster: a call whose transition is
// superseded before it reports skips both, rather than putting back a state
// the breaker has already left. Its event is still delivered, in order.
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
	var justClosed bool
	var seq uint64

	if cluster == types.ClusterA {
		c.muA.Lock()
		lastFailure := c.lastFailureA.Load()
		if c.resetTimeout > 0 && lastFailure > 0 && time.Duration(nowNs-lastFailure) > c.resetTimeout {
			// Half-open window expired without a recovery — counter resets
			// to 1 AND the trip latch clears so a re-trip on this cycle
			// fires IncCircuitBreakerTrip again. Without clearing trippedA,
			// observability undercounts trips across multi-cycle outages.
			//
			// Clearing the latch also ends an open span. A breaker that was
			// genuinely tripped closes here, so its Open event gets a matching
			// Closed and the state gauge stops reporting open; a breaker whose
			// stale failure count merely aged out was never open and closes
			// nothing, which is why the latch is read before it is cleared.
			//
			// resetTimeout=0 disables the timed transition (matches the
			// guard in ShouldFailover) — the breaker stays open until an
			// explicit RecordSuccess, so failures must keep accumulating.
			c.failuresA.Store(1)
			newFailures = 1
			justClosed = c.trippedA
			setTripped(&c.trippedA, &c.openA, false)
			seq = c.enqueueTimeoutClose(cluster, justClosed, seq)
		} else {
			newFailures = c.failuresA.Add(1)
		}
		c.lastFailureA.Store(nowNs)
		if c.threshold > 0 && int(newFailures) >= c.threshold && !c.trippedA {
			setTripped(&c.trippedA, &c.openA, true)
			justTripped = true
			c.events.enqueue(types.ClusterEvent{
				Kind:    types.EventCircuitBreakerOpen,
				Cluster: cluster,
				Count:   int(newFailures),
			})
			seq = c.bumpTransitionSeq(cluster)
		}
		c.muA.Unlock()
	} else {
		c.muB.Lock()
		lastFailure := c.lastFailureB.Load()
		if c.resetTimeout > 0 && lastFailure > 0 && time.Duration(nowNs-lastFailure) > c.resetTimeout {
			c.failuresB.Store(1)
			newFailures = 1
			justClosed = c.trippedB
			setTripped(&c.trippedB, &c.openB, false)
			seq = c.enqueueTimeoutClose(cluster, justClosed, seq)
		} else {
			newFailures = c.failuresB.Add(1)
		}
		c.lastFailureB.Store(nowNs)
		if c.threshold > 0 && int(newFailures) >= c.threshold && !c.trippedB {
			setTripped(&c.trippedB, &c.openB, true)
			justTripped = true
			c.events.enqueue(types.ClusterEvent{
				Kind:    types.EventCircuitBreakerOpen,
				Cluster: cluster,
				Count:   int(newFailures),
			})
			seq = c.bumpTransitionSeq(cluster)
		}
		c.muB.Unlock()
	}

	// Write the metrics and log side effects, then deliver the queued
	// events last. This runs only when a close, a trip, or both actually
	// happened, so a handler that reenters and changes the breaker
	// mid-delivery cannot observe (or leave behind) a metrics gauge that
	// disagrees with the state both calls settle on. One drain covers both
	// transitions when a single call closes the breaker and re-trips it.
	if justClosed || justTripped {
		c.applyTransitionReports(cluster, seq, justClosed, justTripped)
		c.events.drain()
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
	var seq uint64

	if cluster == types.ClusterA {
		c.muA.Lock()
		wasOpen = c.trippedA
		c.failuresA.Store(0)
		c.lastFailureA.Store(0)
		setTripped(&c.trippedA, &c.openA, false)
		if wasOpen {
			c.events.enqueue(types.ClusterEvent{
				Kind:    types.EventCircuitBreakerClosed,
				Cluster: cluster,
				Reason:  "operation succeeded",
			})
			seq = c.bumpTransitionSeq(cluster)
		}
		c.muA.Unlock()
	} else {
		c.muB.Lock()
		wasOpen = c.trippedB
		c.failuresB.Store(0)
		c.lastFailureB.Store(0)
		setTripped(&c.trippedB, &c.openB, false)
		if wasOpen {
			c.events.enqueue(types.ClusterEvent{
				Kind:    types.EventCircuitBreakerClosed,
				Cluster: cluster,
				Reason:  "operation succeeded",
			})
			seq = c.bumpTransitionSeq(cluster)
		}
		c.muB.Unlock()
	}

	if wasOpen {
		c.applyTransitionReports(cluster, seq, true, false)
		// Deliver the queued Closed event last, after the metrics and log
		// updates above, for the same reason as RecordFailure's trip
		// block: this only runs on a real close, and delivering last
		// keeps the metrics gauge consistent with the breaker's state
		// even if a reentrant handler changes it again mid-delivery.
		c.events.drain()
	}
}

// enqueueTimeoutClose appends the Closed event for a breaker whose open span
// ended because its reset timeout elapsed, and returns the cluster's
// transition sequence after latching it. wasOpen is the trip latch as it
// stood before the reset branch cleared it: a breaker whose stale failure
// count merely aged out was never open, closes nothing, and gets seq back
// unchanged.
//
// MUST be called with the cluster's state mutex held, so that append order
// equals transition order.
func (c *CircuitBreaker) enqueueTimeoutClose(cluster types.ClusterID, wasOpen bool, seq uint64) uint64 {
	if !wasOpen {
		return seq
	}
	c.events.enqueue(types.ClusterEvent{
		Kind:    types.EventCircuitBreakerClosed,
		Cluster: cluster,
		Reason:  "reset timeout elapsed",
	})

	return c.bumpTransitionSeq(cluster)
}

// bumpTransitionSeq increments the cluster's transition sequence and returns
// the new value, which identifies the transition just latched.
//
// MUST be called with the cluster's state mutex held: that is what makes the
// sequence agree with the order in which transitions were latched, and what
// lets a later reporter tell that it has been superseded.
func (c *CircuitBreaker) bumpTransitionSeq(cluster types.ClusterID) uint64 {
	if cluster == types.ClusterB {
		return c.seqB.Add(1)
	}

	return c.seqA.Add(1)
}

// transitionReportState returns the cluster's report mutex and transition
// sequence. Cluster A is the fallback for anything that is not cluster B;
// every caller has already passed isKnownCluster.
func (c *CircuitBreaker) transitionReportState(cluster types.ClusterID) (*sync.Mutex, *atomic.Uint64) {
	if cluster == types.ClusterB {
		return &c.reportMuB, &c.seqB
	}

	return &c.reportMuA, &c.seqA
}

// applyTransitionReports writes the metrics and log side effects for the
// transitions a single call latched: a close, a trip, or a close followed by
// a trip. seq is the cluster's transition sequence as it stood when the call
// released the state mutex.
//
// The state gauge and the transition log lines are skipped when the sequence
// has moved on, because another goroutine has latched a newer transition for
// this cluster and its own report is the one that describes where the breaker
// ended up. Writing them anyway would leave the gauge, and the log, reporting
// a state the breaker has already left. The transition itself is not lost:
// its event is queued in order and still delivered.
//
// The report mutex makes the check and the writes one step, so two calls
// cannot interleave their gauge writes; the newest transition always writes
// last. It is not the state mutex, so a caller-supplied collector or logger
// never blocks state mutation, and it is released before events are
// delivered so a reentrant emitter cannot deadlock on it.
//
// The trip counter is cumulative rather than state-derived, so it is
// incremented even by a superseded call: the trip did happen, and skipping it
// would undercount trips across a burst.
//
// Callers must have released the cluster's state mutex, and must deliver
// queued events only after this returns.
//
// The nil checks are a defensive backstop for a zero-value CircuitBreaker
// (metrics/logger nil until finalizeCircuitBreaker runs). No exported path
// reaches here on one — a trip requires threshold > 0, and a close requires
// an earlier trip — but any future path that sets threshold without going
// through the constructors stays safe.
func (c *CircuitBreaker) applyTransitionReports(cluster types.ClusterID, seq uint64, closed bool, tripped bool) {
	if tripped && c.metrics != nil {
		c.metrics.IncCircuitBreakerTrip(cluster)
	}

	mu, current := c.transitionReportState(cluster)
	mu.Lock()
	defer mu.Unlock()

	if current.Load() != seq {
		return
	}

	if closed {
		c.reportClose(cluster)
	}
	if tripped {
		c.reportTrip(cluster)
	}
}

// reportClose writes the state gauge and log line for a breaker whose open
// span just ended, on either close path: a successful operation, or the reset
// timeout elapsing. MUST be called from applyTransitionReports, which holds
// the cluster's report mutex and has confirmed the transition is still the
// newest one.
func (c *CircuitBreaker) reportClose(cluster types.ClusterID) {
	if c.metrics != nil {
		c.metrics.SetCircuitBreakerState(cluster, 0) // 0 = closed
	}
	if c.logger != nil {
		c.logger.Info("circuit breaker closed",
			"cluster", c.clusterName(cluster),
		)
	}
}

// reportTrip writes the state gauge and log line for a breaker that just
// tripped open. The trip counter is not written here — see
// applyTransitionReports, which is also the only permitted caller.
func (c *CircuitBreaker) reportTrip(cluster types.ClusterID) {
	if c.metrics != nil {
		c.metrics.SetCircuitBreakerState(cluster, 2) // 2 = open
	}
	if c.logger != nil {
		c.logger.Warn("circuit breaker tripped",
			"cluster", c.clusterName(cluster),
			"threshold", c.threshold,
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
