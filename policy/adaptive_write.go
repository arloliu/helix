package policy

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/types"
)

// maxInt32 is the maximum value for int32, used for bounds checking.
const maxInt32 = math.MaxInt32

const (
	defaultAdaptiveDeltaThreshold    = 300 * time.Millisecond
	defaultAdaptiveAbsoluteMax       = 2 * time.Second
	defaultAdaptiveMinFloor          = 100 * time.Millisecond
	defaultAdaptiveStrikeThreshold   = int32(3)
	defaultAdaptiveRecoveryThreshold = int32(5)
	defaultAdaptiveFireForgetTimeout = 30 * time.Second
	defaultAdaptiveFireForgetLimit   = int32(100)
)

// AdaptiveDualWrite implements a latency-aware concurrent dual-write strategy.
//
// This strategy monitors the relative performance of both clusters and adapts
// its behavior accordingly:
//   - Healthy cluster: Wait for completion, record latency
//   - Degraded cluster: Fire-and-forget (don't block), rely on replay
//
// A cluster is marked as degraded when:
//  1. Its latency exceeds absoluteMax (e.g., 2s), OR
//  2. It is consistently slower than its sibling by more than deltaThreshold
//     (e.g., 150ms) for strikeThreshold consecutive writes
//
// The "min floor" filter ignores relative differences when both clusters
// are fast (< minFloor), preventing false positives from minor variations.
//
// Example:
//
//	strategy := policy.NewAdaptiveDualWrite(
//	    policy.WithAdaptiveDeltaThreshold(150 * time.Millisecond),
//	    policy.WithAdaptiveAbsoluteMax(2 * time.Second),
//	    policy.WithAdaptiveStrikeThreshold(3),
//	)
//
// Zero value: a bare AdaptiveDualWrite{} never panics and never silently
// drops writes — but it is not functionally adaptive: strikeThreshold is 0
// (so recordStrike never degrades a cluster) and fireForgetSem is nil (so a
// cluster degraded some other way, e.g. ForceDegrade, falls back to a
// synchronous write in fireAndForget instead of true fire-and-forget). Use
// [NewAdaptiveDualWrite] or [NewAdaptiveDualWriteChecked] for the fully
// configured, adaptive behavior described above.
type AdaptiveDualWrite struct {
	// Configuration
	deltaThreshold    time.Duration // Relative difference threshold
	absoluteMax       time.Duration // Absolute latency cap
	minFloor          time.Duration // Ignore delta if both faster than this
	strikeThreshold   int32         // Consecutive slow writes before degraded
	recoveryThreshold int32         // Consecutive fast writes to recover
	fireForgetTimeout time.Duration // Timeout for fire-and-forget writes
	fireForgetLimit   int32         // Max concurrent fire-and-forget writes
	fireForgetSem     chan struct{} // Semaphore for limiting concurrent fire-and-forget

	// Observability for fire-and-forget background writes. The
	// foreground caller already records metrics for the synchronous
	// path; without these, real errors against a degraded cluster are
	// invisible because the goroutine returns silently.
	metrics         types.MetricsCollector
	logger          types.Logger
	metricsExplicit bool
	loggerExplicit  bool
	clusterNames    atomic.Pointer[types.ClusterNames]

	// Ordered queue for degrade/recover notifications. An
	// AdaptiveDualWrite that never calls SetEventEmitter pays nothing on
	// the write path: enqueue is reached only on a real health
	// transition, and it costs a single atomic load when no emitter is
	// installed.
	events eventOutbox

	// Per-cluster state
	stateA clusterWriteState
	stateB clusterWriteState
}

// clusterWriteState tracks the health state of a single cluster for writes.
//
// Concurrency model:
//   - isDegraded and lastLatency are atomic.Bool / atomic.Int64 so Execute can read
//     them lock-free on the hot path.
//   - slowStrikes and fastStrikes are plain int32 values protected by mu. The two
//     counters must be updated as a compound operation to prevent races where a
//     concurrent strike resets fastStrikes mid-recovery (or vice-versa), which would
//     make the effective strikeThreshold / recoveryThreshold unpredictable.
//   - isDegraded.Store is always called while mu is held, keeping the transition
//     atomic with the counter changes. Reads of isDegraded in Execute are still
//     lock-free; a one-call lag on a degradation transition is acceptable.
type clusterWriteState struct {
	mu          sync.Mutex   // Protects slowStrikes and fastStrikes as a unit.
	slowStrikes int32        // Consecutive slow writes; guarded by mu.
	fastStrikes int32        // Consecutive fast writes for recovery; guarded by mu.
	isDegraded  atomic.Bool  // Lock-free read in Execute fast path; written under mu.
	lastLatency atomic.Int64 // Last write latency in nanoseconds; written from goroutines.
}

// AdaptiveDualWriteOption configures an AdaptiveDualWrite strategy.
type AdaptiveDualWriteOption func(*AdaptiveDualWrite)

// WithAdaptiveDeltaThreshold sets the relative latency difference threshold.
//
// If one cluster is slower than the other by more than this amount
// (and both are above minFloor), the slower one accumulates strikes.
//
// Default: 300ms (tuned for Cassandra latency characteristics)
//
// Parameters:
//   - d: Latency difference threshold
//
// Returns:
//   - AdaptiveDualWriteOption: Configuration option
func WithAdaptiveDeltaThreshold(d time.Duration) AdaptiveDualWriteOption {
	return func(a *AdaptiveDualWrite) {
		a.deltaThreshold = d
	}
}

// WithAdaptiveAbsoluteMax sets the absolute latency cap.
//
// If a cluster's latency exceeds this threshold, it is immediately
// considered for degradation (regardless of the other cluster's latency).
//
// Default: 2s
//
// Parameters:
//   - d: Maximum acceptable latency
//
// Returns:
//   - AdaptiveDualWriteOption: Configuration option
func WithAdaptiveAbsoluteMax(d time.Duration) AdaptiveDualWriteOption {
	return func(a *AdaptiveDualWrite) {
		a.absoluteMax = d
	}
}

// WithAdaptiveMinFloor sets the minimum latency floor.
//
// Relative delta comparisons are ignored if both clusters respond
// faster than this threshold. This filters out noise from minor
// variations when both clusters are performing well.
//
// Default: 100ms (accommodates typical Cassandra GC pauses and minor jitter)
//
// Parameters:
//   - d: Minimum latency floor
//
// Returns:
//   - AdaptiveDualWriteOption: Configuration option
func WithAdaptiveMinFloor(d time.Duration) AdaptiveDualWriteOption {
	return func(a *AdaptiveDualWrite) {
		a.minFloor = d
	}
}

// WithAdaptiveStrikeThreshold sets the consecutive slow writes before degradation.
//
// A cluster must be slow for this many consecutive writes before it is
// marked as degraded and switched to fire-and-forget mode.
//
// Default: 3
//
// Parameters:
//   - n: Number of consecutive slow writes required (must be positive, max 2^31-1)
//
// Returns:
//   - AdaptiveDualWriteOption: Configuration option
func WithAdaptiveStrikeThreshold(n int) AdaptiveDualWriteOption {
	return func(a *AdaptiveDualWrite) {
		if n <= 0 || n > maxInt32 {
			a.strikeThreshold = 0
			return
		}
		a.strikeThreshold = int32(n)
	}
}

// WithAdaptiveRecoveryThreshold sets the consecutive fast writes to recover.
//
// A degraded cluster must be fast for this many consecutive writes
// before it is restored to healthy status.
//
// Default: 5
//
// Parameters:
//   - n: Number of consecutive fast writes required (must be positive, max 2^31-1)
//
// Returns:
//   - AdaptiveDualWriteOption: Configuration option
func WithAdaptiveRecoveryThreshold(n int) AdaptiveDualWriteOption {
	return func(a *AdaptiveDualWrite) {
		if n <= 0 || n > maxInt32 {
			a.recoveryThreshold = 0
			return
		}
		a.recoveryThreshold = int32(n)
	}
}

// WithAdaptiveFireForgetTimeout sets the timeout for fire-and-forget writes.
//
// When a cluster is degraded, writes are sent in a background goroutine
// with this timeout. This prevents resource leaks from hanging connections.
//
// Default: 30s
//
// Parameters:
//   - d: Timeout for background writes
//
// Returns:
//   - AdaptiveDualWriteOption: Configuration option
func WithAdaptiveFireForgetTimeout(d time.Duration) AdaptiveDualWriteOption {
	return func(a *AdaptiveDualWrite) {
		a.fireForgetTimeout = d
	}
}

// WithAdaptiveMetrics sets the metrics collector for fire-and-forget background
// writes. Without this option (or auto-injection by [helix.NewCQLClient]),
// real errors against a degraded cluster's background write are not surfaced
// to metrics — only the foreground ErrWriteAsync result is.
//
// Parameters:
//   - m: The metrics collector
//
// Returns:
//   - AdaptiveDualWriteOption: Configuration option
func WithAdaptiveMetrics(m types.MetricsCollector) AdaptiveDualWriteOption {
	return func(a *AdaptiveDualWrite) {
		if m == nil {
			return
		}
		a.metrics = m
		a.metricsExplicit = true
	}
}

// WithAdaptiveLogger sets the logger for fire-and-forget background writes.
//
// Marks the configuration as "logger explicitly set" so a parent caller
// (e.g. helix.NewCQLClient) does not auto-inject a different logger
// later via [AdaptiveDualWrite.SetLogger]. This mirrors the
// [WithAdaptiveMetrics] / explicit-wins contract.
//
// Parameters:
//   - l: The logger
//
// Returns:
//   - AdaptiveDualWriteOption: Configuration option
func WithAdaptiveLogger(l types.Logger) AdaptiveDualWriteOption {
	return func(a *AdaptiveDualWrite) {
		if l == nil {
			return
		}
		a.logger = l
		a.loggerExplicit = true
	}
}

// WithAdaptiveClusterNames sets the display names for clusters in log
// messages emitted by the fire-and-forget background path.
//
// Parameters:
//   - names: The cluster names
//
// Returns:
//   - AdaptiveDualWriteOption: Configuration option
func WithAdaptiveClusterNames(names types.ClusterNames) AdaptiveDualWriteOption {
	return func(a *AdaptiveDualWrite) {
		a.clusterNames.Store(&names)
	}
}

// WithAdaptiveFireForgetLimit sets the maximum concurrent fire-and-forget writes.
//
// When a cluster is degraded and writes are sent via fire-and-forget, this limit
// prevents resource exhaustion from too many pending goroutines. If the limit
// is reached, new fire-and-forget writes are dropped (returning ErrWriteDropped)
// and the replay system handles reconciliation.
//
// Default: 100
//
// Parameters:
//   - n: Maximum concurrent fire-and-forget writes (must be positive, max 2^31-1)
//
// Returns:
//   - AdaptiveDualWriteOption: Configuration option
func WithAdaptiveFireForgetLimit(n int) AdaptiveDualWriteOption {
	return func(a *AdaptiveDualWrite) {
		if n <= 0 || n > maxInt32 {
			a.fireForgetLimit = 0
			return
		}
		a.fireForgetLimit = int32(n)
	}
}

// NewAdaptiveDualWrite creates a new AdaptiveDualWrite strategy.
//
// Defaults:
//   - deltaThreshold: 300ms (tuned for Cassandra: normal p99 jitter is ~3-20ms,
//     GC pauses can cause 50-100ms spikes, so 300ms indicates real degradation)
//   - absoluteMax: 2s
//   - minFloor: 100ms (ignores noise when both clusters are fast)
//   - strikeThreshold: 3
//   - recoveryThreshold: 5
//   - fireForgetTimeout: 30s
//   - fireForgetLimit: 100
//
// Parameters:
//   - opts: Optional configuration options
//
// Returns:
//   - *AdaptiveDualWrite: A new adaptive dual-write strategy
//
// For production configuration that should fail fast on invalid option values,
// use [NewAdaptiveDualWriteChecked].
func NewAdaptiveDualWrite(opts ...AdaptiveDualWriteOption) *AdaptiveDualWrite {
	a := newAdaptiveDualWriteWithDefaults()
	applyAdaptiveDualWriteOptions(a, opts...)
	normalizeAdaptiveDualWriteForLegacy(a)
	finalizeAdaptiveDualWrite(a)

	return a
}

// NewAdaptiveDualWriteChecked creates a new AdaptiveDualWrite strategy and
// returns a validation error when any option value is invalid.
//
// Parameters:
//   - opts: Optional configuration options
//
// Returns:
//   - *AdaptiveDualWrite: A new adaptive dual-write strategy
//   - error: Joined [types.OptionError] values when one or more options are invalid
func NewAdaptiveDualWriteChecked(opts ...AdaptiveDualWriteOption) (*AdaptiveDualWrite, error) {
	a := newAdaptiveDualWriteWithDefaults()
	applyAdaptiveDualWriteOptions(a, opts...)
	if err := validateAdaptiveDualWrite(a); err != nil {
		return nil, err
	}
	finalizeAdaptiveDualWrite(a)

	return a, nil
}

func newAdaptiveDualWriteWithDefaults() *AdaptiveDualWrite {
	a := &AdaptiveDualWrite{
		deltaThreshold:    defaultAdaptiveDeltaThreshold,
		absoluteMax:       defaultAdaptiveAbsoluteMax,
		minFloor:          defaultAdaptiveMinFloor,
		strikeThreshold:   defaultAdaptiveStrikeThreshold,
		recoveryThreshold: defaultAdaptiveRecoveryThreshold,
		fireForgetTimeout: defaultAdaptiveFireForgetTimeout,
		fireForgetLimit:   defaultAdaptiveFireForgetLimit,
	}
	defaultNames := types.DefaultClusterNames()
	a.clusterNames.Store(&defaultNames)

	return a
}

func applyAdaptiveDualWriteOptions(a *AdaptiveDualWrite, opts ...AdaptiveDualWriteOption) {
	for _, opt := range opts {
		opt(a)
	}
}

func validateAdaptiveDualWrite(a *AdaptiveDualWrite) error {
	errList := make([]error, 0, 8)

	if a.deltaThreshold <= 0 {
		errList = append(errList, optionErrPositiveDuration(adaptiveDualWriteComponent, "WithAdaptiveDeltaThreshold"))
	}
	if a.absoluteMax <= 0 {
		errList = append(errList, optionErrPositiveDuration(adaptiveDualWriteComponent, "WithAdaptiveAbsoluteMax"))
	}
	if a.minFloor < 0 {
		errList = append(errList, optionErrNonNegativeDuration(adaptiveDualWriteComponent, "WithAdaptiveMinFloor"))
	}
	if a.strikeThreshold <= 0 {
		errList = append(errList, optionErrInt32Range(adaptiveDualWriteComponent, "WithAdaptiveStrikeThreshold"))
	}
	if a.recoveryThreshold <= 0 {
		errList = append(errList, optionErrInt32Range(adaptiveDualWriteComponent, "WithAdaptiveRecoveryThreshold"))
	}
	if a.fireForgetTimeout <= 0 {
		errList = append(errList, optionErrPositiveDuration(adaptiveDualWriteComponent, "WithAdaptiveFireForgetTimeout"))
	}
	if a.fireForgetLimit <= 0 {
		errList = append(errList, optionErrInt32Range(adaptiveDualWriteComponent, "WithAdaptiveFireForgetLimit"))
	}
	if names := a.clusterNames.Load(); names == nil {
		errList = append(errList, newOptionError(adaptiveDualWriteComponent, "WithAdaptiveClusterNames", "cluster names cannot be nil"))
	} else if err := names.Validate(); err != nil {
		errList = append(errList, optionErrReasonFromErr(adaptiveDualWriteComponent, "WithAdaptiveClusterNames", err))
	}

	return joinValidationErrors(errList)
}

func normalizeAdaptiveDualWriteForLegacy(a *AdaptiveDualWrite) {
	if a.deltaThreshold <= 0 {
		a.deltaThreshold = defaultAdaptiveDeltaThreshold
	}
	if a.absoluteMax <= 0 {
		a.absoluteMax = defaultAdaptiveAbsoluteMax
	}
	if a.minFloor < 0 {
		a.minFloor = defaultAdaptiveMinFloor
	}
	if a.strikeThreshold <= 0 {
		a.strikeThreshold = defaultAdaptiveStrikeThreshold
	}
	if a.recoveryThreshold <= 0 {
		a.recoveryThreshold = defaultAdaptiveRecoveryThreshold
	}
	if a.fireForgetTimeout <= 0 {
		a.fireForgetTimeout = defaultAdaptiveFireForgetTimeout
	}
	if a.fireForgetLimit <= 0 {
		a.fireForgetLimit = defaultAdaptiveFireForgetLimit
	}
	if names := a.clusterNames.Load(); names == nil || names.Validate() != nil {
		defaultNames := types.DefaultClusterNames()
		a.clusterNames.Store(&defaultNames)
	}
}

func finalizeAdaptiveDualWrite(a *AdaptiveDualWrite) {
	if a.metrics == nil {
		a.metrics = metrics.NewNopMetrics()
	}
	if a.logger == nil {
		a.logger = logging.NewNopLogger()
	}
	a.fireForgetSem = make(chan struct{}, int(a.fireForgetLimit))
}

// SetClusterNames implements [types.ClusterNamer] so the helix client can
// propagate cluster names configured via WithClusterNames into log messages.
//
// Safe to call concurrently with Execute.
func (a *AdaptiveDualWrite) SetClusterNames(names types.ClusterNames) {
	a.clusterNames.Store(&names)
}

// MetricsConfigured reports whether the metrics collector was explicitly
// set via WithAdaptiveMetrics. The helix client uses this to detect a
// caller-passed strategy without metrics and inject its own collector so
// the strategy's fire-and-forget path participates in unified instrumentation.
func (a *AdaptiveDualWrite) MetricsConfigured() bool {
	return a.metricsExplicit
}

// SetMetrics replaces the metrics collector. No-op once MetricsConfigured
// returns true (caller's explicit choice wins) or if m is nil.
func (a *AdaptiveDualWrite) SetMetrics(m types.MetricsCollector) {
	if a.metricsExplicit {
		return
	}
	if m == nil {
		return
	}
	a.metrics = m
	a.metricsExplicit = true
}

// LoggerConfigured reports whether the logger was explicitly set via
// WithAdaptiveLogger. Mirrors MetricsConfigured so the helix client
// can use the same auto-injection guard for both knobs.
func (a *AdaptiveDualWrite) LoggerConfigured() bool {
	return a.loggerExplicit
}

// SetLogger replaces the logger. No-op once LoggerConfigured returns
// true (caller's explicit choice wins) or if l is nil. Used by
// helix.NewCQLClient to propagate the client logger into the strategy
// when the caller did not provide one.
func (a *AdaptiveDualWrite) SetLogger(l types.Logger) {
	if a.loggerExplicit {
		return
	}
	if l == nil {
		return
	}
	a.logger = l
	a.loggerExplicit = true
}

// SetEventEmitter sets the cluster event emitter used for degrade and
// recover notifications. helix.NewCQLClient injects this automatically
// when a WithOnClusterEvent handler is registered; standalone users may
// call it directly at any time, since the emitter reference is swapped
// atomically and recovery probes may already be running. Delivery is
// best-effort: a transition racing exactly with an emitter install or
// removal may not reach either the old or the new emitter.
//
// Emission ordering: a degrade or recover transition is recorded, and
// its event appended to an internal queue, under that cluster's state
// mutex, so events for one cluster are delivered in the order their
// transitions occurred. The emitter itself is always invoked with no
// policy locks held — see [types.ClusterEventEmitter] for the full
// contract an emitter must satisfy.
//
// Parameters:
//   - em: The event emitter; nil disables emission
func (a *AdaptiveDualWrite) SetEventEmitter(em types.ClusterEventEmitter) {
	a.events.setEmitter(em)
}

// clusterName returns the display name for the given cluster.
func (a *AdaptiveDualWrite) clusterName(cluster types.ClusterID) string {
	return clusterNameOrID(&a.clusterNames, cluster)
}

// Execute performs adaptive concurrent writes to both clusters.
//
// For healthy clusters, writes are executed concurrently and waited upon.
// For degraded clusters, writes are fire-and-forget (background goroutine).
// Fire-and-forget writes use a dedicated context.Background() with fireForgetTimeout,
// independent of the caller's ctx — the caller's context cancellation does not
// cancel in-flight background writes.
//
// After execution, latencies are compared to update cluster health state.
//
// Parameters:
//   - ctx: Context for the operation
//   - writeA: Function to write to cluster A
//   - writeB: Function to write to cluster B
//
// Returns:
//   - resultA: Error from cluster A (nil if successful, ErrWriteAsync if fire-and-forget)
//   - resultB: Error from cluster B (nil if successful, ErrWriteAsync if fire-and-forget)
func (a *AdaptiveDualWrite) Execute(
	ctx context.Context,
	writeA func(context.Context) error,
	writeB func(context.Context) error,
) (resultA, resultB error) {
	degradedA := a.stateA.isDegraded.Load()
	degradedB := a.stateB.isDegraded.Load()

	var wg sync.WaitGroup
	var latencyA, latencyB time.Duration
	var errA, errB error

	if !degradedA && !degradedB {
		// Both clusters healthy: spawn one goroutine for B and run A inline
		// on the calling goroutine instead of spawning two goroutines for
		// this common case. Both writes still execute concurrently.
		wg.Go(func() {
			start := time.Now()
			errB = safeWrite(ctx, writeB, "B")
			latencyB = time.Since(start)
		})

		start := time.Now()
		errA = safeWrite(ctx, writeA, "A")
		latencyA = time.Since(start)
	} else {
		// Cluster A
		if !degradedA {
			wg.Go(func() {
				start := time.Now()
				errA = safeWrite(ctx, writeA, "A")
				latencyA = time.Since(start)
			})
		} else {
			errA = a.fireAndForget(types.ClusterA, writeA, &a.stateA, &a.stateB)
		}

		// Cluster B
		if !degradedB {
			wg.Go(func() {
				start := time.Now()
				errB = safeWrite(ctx, writeB, "B")
				latencyB = time.Since(start)
			})
		} else {
			errB = a.fireAndForget(types.ClusterB, writeB, &a.stateB, &a.stateA)
		}
	}

	// Wait for healthy clusters to complete
	wg.Wait()

	// Assign results after all goroutines complete (no race)
	resultA = errA
	resultB = errB

	// Store latencies for healthy clusters (used for delta-based recovery)
	if errA == nil {
		a.stateA.lastLatency.Store(latencyA.Nanoseconds())
	}
	if errB == nil {
		a.stateB.lastLatency.Store(latencyB.Nanoseconds())
	}

	// Update health state based on results
	a.updateHealthState(latencyA, latencyB, resultA, resultB, degradedA, degradedB)

	return resultA, resultB
}

// fireAndForget executes a write in a background goroutine with its own timeout.
// It tracks latency to enable recovery of degraded clusters via delta comparison.
//
// On a real error (not ErrWriteAsync/ErrWriteDropped, which are not raised
// here anyway), the goroutine emits IncWriteError + a Warn log so the
// background failure is visible. The replay safety net was already enqueued
// by the caller based on the foreground ErrWriteAsync result; this is the
// observability complement.
//
// Returns:
//   - types.ErrWriteAsync if the write was accepted for background execution
//   - types.ErrWriteDropped if the concurrency limit was reached
//   - the write's own result if fireForgetSem is nil — only possible on an
//     uninitialized zero-value AdaptiveDualWrite{}; see the type doc
func (a *AdaptiveDualWrite) fireAndForget(
	cluster types.ClusterID,
	write func(context.Context) error,
	state *clusterWriteState,
	siblingState *clusterWriteState,
) error {
	if a.fireForgetSem == nil {
		// fireForgetSem is only created by finalizeAdaptiveDualWrite (called
		// from the constructors). On a zero-value AdaptiveDualWrite{} that
		// somehow reached a degraded state (e.g. via ForceDegrade), the
		// select below would always hit its nil-channel default case,
		// permanently dropping this write with no recovery path. Fall back
		// to executing it synchronously instead of dropping it.
		return safeWrite(context.Background(), write, a.clusterName(cluster))
	}

	// Try to acquire semaphore (non-blocking)
	select {
	case a.fireForgetSem <- struct{}{}:
		// Acquired semaphore slot
	default:
		// Concurrency limit reached - drop this write
		// The replay system will handle reconciliation
		return types.ErrWriteDropped
	}

	go func() {
		defer func() { <-a.fireForgetSem }() // Release semaphore

		ctx, cancel := context.WithTimeout(context.Background(), a.fireForgetTimeout)
		defer cancel()

		start := time.Now()
		err := safeWrite(ctx, write, a.clusterName(cluster))
		latency := time.Since(start)

		// Track latency for potential recovery
		if err != nil {
			// Surface the background failure: IncWriteError makes it
			// visible to dashboards, and the Warn log gives operators a
			// breadcrumb that "the cluster you flagged degraded is
			// actually erroring on its background writes." Replay
			// reconciliation has already been enqueued by the caller.
			a.metrics.IncWriteError(cluster)
			a.logger.Warn("adaptive: fire-and-forget write failed on degraded cluster",
				"cluster", a.clusterName(cluster),
				"error", err.Error(),
			)
			return
		}
		if latency >= a.absoluteMax {
			// Write succeeded but was too slow — no recovery credit.
			return
		}

		// Check delta against sibling for recovery
		// If sibling is also degraded (lastLatency == 0), fall back to absoluteMax-only check
		siblingLatencyNs := siblingState.lastLatency.Load()
		if siblingLatencyNs > 0 {
			// Sibling has valid latency - require delta check for recovery
			siblingLatency := time.Duration(siblingLatencyNs)
			delta := latency - siblingLatency
			if delta < 0 {
				delta = -delta
			}
			if delta > a.deltaThreshold {
				// Still significantly slower than sibling - no recovery credit
				return
			}
		} else if latency >= a.minFloor {
			// Sibling has no baseline yet (also degraded or never written to).
			// Use minFloor as a conservative substitute for the delta check:
			// only grant recovery credit if this write was comfortably fast on
			// its own. Without this guard, any sub-absoluteMax write would
			// trigger recovery even when the sibling's true cost is unknown,
			// potentially bouncing the cluster in/out of degradation.
			return
		}

		// Write succeeded, was fast, and within delta of sibling - record for recovery.
		// Update lastLatency so subsequent fire-and-forget cycles and the sibling's
		// delta check see a fresh observation rather than a stale pre-degradation value.
		state.lastLatency.Store(latency.Nanoseconds())
		a.recordFast(state)
	}()

	return types.ErrWriteAsync
}

// updateHealthState updates cluster health based on write results.
func (a *AdaptiveDualWrite) updateHealthState(
	latencyA, latencyB time.Duration,
	errA, errB error,
	_, _ bool, // degradedA, degradedB - reserved for future use
) {
	// Handle errors as strikes (but not ErrWriteAsync which is expected for degraded clusters)
	a.handleErrors(errA, errB)

	// Track which clusters have valid latency data
	hasLatencyA := errA == nil
	hasLatencyB := errB == nil

	// Check absolute cap for clusters with valid latency
	capViolationA, capViolationB := a.checkAbsoluteCap(hasLatencyA, hasLatencyB, latencyA, latencyB)

	// Skip relative delta comparison if we don't have both latencies.
	// Still record fast-write credit for any healthy cluster that has valid
	// latency and no cap violation, so that stale slowStrikes accumulated
	// before the sibling degraded are cleared.
	if !hasLatencyA || !hasLatencyB {
		if hasLatencyA {
			a.recordFastIfNoViolation(&a.stateA, capViolationA)
		}
		if hasLatencyB {
			a.recordFastIfNoViolation(&a.stateB, capViolationB)
		}
		return
	}

	// Process latency comparison
	a.processLatencies(latencyA, latencyB, capViolationA, capViolationB)
}

// handleErrors records strikes for write errors.
// ErrWriteAsync, ErrWriteDropped, ErrClusterDegraded, and ErrClusterDraining are
// excluded: they are expected operational states, not genuine write failures.
func (a *AdaptiveDualWrite) handleErrors(errA, errB error) {
	if errA != nil && !isSkippedErr(errA) {
		a.recordStrike(&a.stateA)
	}
	if errB != nil && !isSkippedErr(errB) {
		a.recordStrike(&a.stateB)
	}
}

// isSkippedErr reports whether err is an expected operational state that should
// not count as a write strike.
func isSkippedErr(err error) bool {
	return errors.Is(err, types.ErrWriteAsync) ||
		errors.Is(err, types.ErrWriteDropped) ||
		errors.Is(err, types.ErrClusterDegraded) ||
		errors.Is(err, types.ErrClusterDraining)
}

// checkAbsoluteCap checks if latencies exceed the absolute max and records strikes.
func (a *AdaptiveDualWrite) checkAbsoluteCap(hasA, hasB bool, latA, latB time.Duration) (violationA, violationB bool) {
	violationA = hasA && latA > a.absoluteMax
	violationB = hasB && latB > a.absoluteMax

	if violationA {
		a.recordStrike(&a.stateA)
	}
	if violationB {
		a.recordStrike(&a.stateB)
	}

	return violationA, violationB
}

// processLatencies compares latencies and records fast/slow status.
func (a *AdaptiveDualWrite) processLatencies(latA, latB time.Duration, capViolationA, capViolationB bool) {
	// Skip relative delta if both are fast (noise filter)
	if latA < a.minFloor && latB < a.minFloor {
		a.recordFastIfNoViolation(&a.stateA, capViolationA)
		a.recordFastIfNoViolation(&a.stateB, capViolationB)
		return
	}

	// Calculate relative delta
	delta := latA - latB
	if delta < 0 {
		delta = -delta
	}

	if delta > a.deltaThreshold {
		// One is significantly slower than the other.
		// Skip the delta strike if checkAbsoluteCap already issued a strike for
		// this cluster in the same execution round, to prevent double-counting.
		if latA > latB {
			if !capViolationA {
				a.recordStrike(&a.stateA)
			}
			a.recordFastIfNoViolation(&a.stateB, capViolationB)
		} else {
			if !capViolationB {
				a.recordStrike(&a.stateB)
			}
			a.recordFastIfNoViolation(&a.stateA, capViolationA)
		}
	} else {
		// Both are similar speed - record as fast if no cap violation
		a.recordFastIfNoViolation(&a.stateA, capViolationA)
		a.recordFastIfNoViolation(&a.stateB, capViolationB)
	}
}

// recordFastIfNoViolation records a fast write if there's no cap violation.
func (a *AdaptiveDualWrite) recordFastIfNoViolation(state *clusterWriteState, hasViolation bool) {
	if !hasViolation {
		a.recordFast(state)
	}
}

// stateCluster maps a state pointer back to its cluster ID. Deriving the
// identity from pointer comparison rather than a stored field keeps
// zero-value AdaptiveDualWrite instances correct: they never run
// constructor initialization, so a stored field would be empty.
func (a *AdaptiveDualWrite) stateCluster(state *clusterWriteState) types.ClusterID {
	if state == &a.stateB {
		return types.ClusterB
	}

	return types.ClusterA
}

// logWriteDegraded logs a healthy-to-degraded transition. Must be called
// after the state mutex is released: a caller-supplied logger may block, and
// no per-cluster lock may be held while it runs. Log lines are diagnostics —
// their order across concurrent transitions is not guaranteed, unlike the
// events. The nil check keeps zero-value instances safe, since the logger is
// only installed by the constructors.
func (a *AdaptiveDualWrite) logWriteDegraded(cluster types.ClusterID, reason string, strikes int) {
	if a.logger == nil {
		return
	}

	a.logger.Warn("cluster degraded to fire-and-forget writes",
		"cluster", a.clusterName(cluster),
		"reason", reason,
		"slowStrikes", strikes,
	)
}

// logWriteRecovered logs a degraded-to-healthy transition. See
// [AdaptiveDualWrite.logWriteDegraded] for the calling requirements.
func (a *AdaptiveDualWrite) logWriteRecovered(cluster types.ClusterID, reason string) {
	if a.logger == nil {
		return
	}

	a.logger.Info("cluster recovered to synchronous writes",
		"cluster", a.clusterName(cluster),
		"reason", reason,
	)
}

// recordStrike increments the slow strike counter and potentially degrades the cluster.
// Must be called with state.mu unlocked.
func (a *AdaptiveDualWrite) recordStrike(state *clusterWriteState) {
	// Only the transition branch below needs the cluster identity (for the
	// event and the log line), so it is derived there rather than on every
	// strike.
	var cluster types.ClusterID

	state.mu.Lock()
	state.fastStrikes = 0
	state.slowStrikes++
	strikes := int(state.slowStrikes)
	// strikeThreshold > 0 guards the zero-value AdaptiveDualWrite{}: without
	// it, a single failure (1 >= 0) would degrade the cluster, and every
	// subsequent write would route through fireAndForget and be silently
	// dropped forever (see fireAndForget's nil-fireForgetSem fallback).
	// Constructors always normalize strikeThreshold to >= 1, so this has no
	// effect on properly constructed instances.
	//
	// The isDegraded check makes this branch fire only on the healthy →
	// degraded transition: storing true over an already-true value is a
	// no-op, so the check exists only to emit the log line and event once
	// per transition instead of once per strike. isDegraded is only
	// written under mu, so the read is consistent with the store that
	// follows.
	justDegraded := false
	if a.strikeThreshold > 0 && state.slowStrikes >= a.strikeThreshold && !state.isDegraded.Load() {
		state.isDegraded.Store(true)
		justDegraded = true
		cluster = a.stateCluster(state)
		a.events.enqueue(types.ClusterEvent{
			Kind:    types.EventWriteDegraded,
			Cluster: cluster,
			Reason:  "slow-strike threshold reached",
			Count:   strikes,
		})
	}
	state.mu.Unlock()

	if justDegraded {
		a.logWriteDegraded(cluster, "slow-strike threshold reached", strikes)
		// Deliver the queued event last, after the log line above, so a
		// handler that inspects or changes state from inside the callback
		// cannot observe diagnostics that contradict the final state. Only
		// reached on a real transition.
		a.events.drain()
	}
}

// recordFast resets the slow-strike counter and, if the cluster is currently
// degraded, increments the fast counter toward recovery.
//
// slowStrikes is always cleared — even on a healthy cluster — so that a
// slow→fast→slow sequence does not let stale strikes accumulate across the
// gap. The isDegraded check is performed inside the lock so that a concurrent
// ForceRecover or Reset cannot change the degraded state between the check
// and the fastStrikes increment.
//
// Must be called with state.mu unlocked.
func (a *AdaptiveDualWrite) recordFast(state *clusterWriteState) {
	// A healthy cluster returns below without ever needing the cluster
	// identity, so it is derived only in the recovery branch that publishes
	// the event and the log line.
	var cluster types.ClusterID

	state.mu.Lock()
	state.slowStrikes = 0 // Always clear, regardless of degraded state.
	if !state.isDegraded.Load() {
		state.mu.Unlock()
		return // Healthy — no recovery credit needed.
	}
	state.fastStrikes++
	justRecovered := false
	if state.fastStrikes >= a.recoveryThreshold {
		state.isDegraded.Store(false)
		state.fastStrikes = 0
		justRecovered = true
		cluster = a.stateCluster(state)
		a.events.enqueue(types.ClusterEvent{
			Kind:    types.EventWriteRecovered,
			Cluster: cluster,
			Reason:  "fast-strike recovery",
		})
	}
	state.mu.Unlock()

	if justRecovered {
		a.logWriteRecovered(cluster, "fast-strike recovery")
		// Same ordering as recordStrike: the event is the last side effect,
		// and only a real transition reaches here.
		a.events.drain()
	}
}

// IsDegraded returns whether a cluster is currently in degraded (fire-and-forget) mode.
//
// Parameters:
//   - cluster: The cluster to check
//
// Returns:
//   - bool: true if the cluster is degraded
func (a *AdaptiveDualWrite) IsDegraded(cluster types.ClusterID) bool {
	if cluster == types.ClusterA {
		return a.stateA.isDegraded.Load()
	}
	if cluster != types.ClusterB {
		return false
	}
	return a.stateB.isDegraded.Load()
}

// Reset clears all health state, returning both clusters to healthy.
//
// This is useful for testing or manual intervention. Each cluster that was
// actually degraded transitions back to healthy, so [types.EventWriteRecovered]
// with Reason "manual reset" is emitted for it; a cluster that was already
// healthy produces no event. The two clusters are reset one after the other,
// each under its own mutex — the first is fully released before the second is
// taken, so Reset never holds both locks at once.
func (a *AdaptiveDualWrite) Reset() {
	for _, state := range []*clusterWriteState{&a.stateA, &a.stateB} {
		cluster := a.stateCluster(state)

		state.mu.Lock()
		wasDegraded := state.isDegraded.Load()
		state.slowStrikes = 0
		state.fastStrikes = 0
		state.isDegraded.Store(false)
		state.lastLatency.Store(0)
		if wasDegraded {
			a.events.enqueue(types.ClusterEvent{
				Kind:    types.EventWriteRecovered,
				Cluster: cluster,
				Reason:  "manual reset",
			})
		}
		state.mu.Unlock()

		if wasDegraded {
			a.logWriteRecovered(cluster, "manual reset")
			a.events.drain()
		}
	}
}

// ForceDegrade manually marks a cluster as degraded.
//
// This is useful for testing or manual intervention. The call acquires the
// cluster's mutex so that fastStrikes is reset atomically with the degraded
// transition — preventing a stale fast-strike accumulation from immediately
// recovering the cluster on the very next recordFast call.
//
// Emits [types.EventWriteDegraded] with Reason "manual" when this call performs
// the transition. Calling it on an already-degraded cluster still clears
// fastStrikes but emits nothing.
//
// Parameters:
//   - cluster: The cluster to degrade
func (a *AdaptiveDualWrite) ForceDegrade(cluster types.ClusterID) {
	var state *clusterWriteState
	switch cluster {
	case types.ClusterA:
		state = &a.stateA
	case types.ClusterB:
		state = &a.stateB
	default:
		return
	}

	state.mu.Lock()
	wasDegraded := state.isDegraded.Load()
	state.fastStrikes = 0
	state.isDegraded.Store(true)
	strikes := int(state.slowStrikes)
	if !wasDegraded {
		a.events.enqueue(types.ClusterEvent{
			Kind:    types.EventWriteDegraded,
			Cluster: cluster,
			Reason:  "manual",
		})
	}
	state.mu.Unlock()

	if !wasDegraded {
		// A manual degrade clears fastStrikes but leaves slowStrikes
		// intact, so the log line reports the live counter rather than
		// asserting it is zero.
		a.logWriteDegraded(cluster, "manual", strikes)
		a.events.drain()
	}
}

// ForceRecover manually marks a cluster as healthy.
//
// This is useful for testing or manual intervention when you know
// a cluster has recovered (e.g., from external health checks).
//
// Emits [types.EventWriteRecovered] with Reason "manual" when this call
// performs the transition. Calling it on an already-healthy cluster still
// clears the strike counters and last latency but emits nothing.
//
// Parameters:
//   - cluster: The cluster to recover
func (a *AdaptiveDualWrite) ForceRecover(cluster types.ClusterID) {
	var state *clusterWriteState
	switch cluster {
	case types.ClusterA:
		state = &a.stateA
	case types.ClusterB:
		state = &a.stateB
	default:
		return
	}

	state.mu.Lock()
	wasDegraded := state.isDegraded.Load()
	state.isDegraded.Store(false)
	state.slowStrikes = 0
	state.fastStrikes = 0
	state.lastLatency.Store(0)
	if wasDegraded {
		a.events.enqueue(types.ClusterEvent{
			Kind:    types.EventWriteRecovered,
			Cluster: cluster,
			Reason:  "manual",
		})
	}
	state.mu.Unlock()

	if wasDegraded {
		a.logWriteRecovered(cluster, "manual")
		a.events.drain()
	}
}

// ExecuteStrict performs adaptive concurrent writes without fire-and-forget dispatch.
//
// Unlike Execute, ExecuteStrict never spawns background goroutines. If a cluster
// is currently degraded, its write is skipped and [types.ErrClusterDegraded] is
// returned for that cluster — the caller receives the skip signal rather than the
// fire-and-forget [types.ErrWriteAsync]. Healthy clusters are written synchronously
// as in [AdaptiveDualWrite.Execute].
//
// Health state (strikes, recovery credit) is updated for clusters that actually
// run; degraded-and-skipped clusters are not penalised further. Recovery of
// degraded clusters in strict-only workloads is driven by the recovery probe
// configured via WithRecoveryProbe.
func (a *AdaptiveDualWrite) ExecuteStrict(
	ctx context.Context,
	writeA func(context.Context) error,
	writeB func(context.Context) error,
) (resultA, resultB error) {
	degradedA := a.stateA.isDegraded.Load()
	degradedB := a.stateB.isDegraded.Load()

	if degradedA && degradedB {
		return types.ErrClusterDegraded, types.ErrClusterDegraded
	}

	var wg sync.WaitGroup
	var latencyA, latencyB time.Duration
	var errA, errB error

	if !degradedA && !degradedB {
		// Both clusters healthy: spawn one goroutine for B and run A inline
		// on the calling goroutine instead of spawning two goroutines for
		// this common case. Both writes still execute concurrently.
		wg.Go(func() {
			start := time.Now()
			errB = safeWrite(ctx, writeB, "B")
			latencyB = time.Since(start)
		})

		start := time.Now()
		errA = safeWrite(ctx, writeA, "A")
		latencyA = time.Since(start)
	} else {
		if !degradedA {
			wg.Go(func() {
				start := time.Now()
				errA = safeWrite(ctx, writeA, "A")
				latencyA = time.Since(start)
			})
		} else {
			errA = types.ErrClusterDegraded
		}

		if !degradedB {
			wg.Go(func() {
				start := time.Now()
				errB = safeWrite(ctx, writeB, "B")
				latencyB = time.Since(start)
			})
		} else {
			errB = types.ErrClusterDegraded
		}
	}

	wg.Wait()

	if errA == nil {
		a.stateA.lastLatency.Store(latencyA.Nanoseconds())
	}
	if errB == nil {
		a.stateB.lastLatency.Store(latencyB.Nanoseconds())
	}

	// Pass ErrClusterDegraded for skipped clusters — handleErrors excludes it
	// from strike accounting, and updateHealthState treats non-nil as no latency.
	a.updateHealthState(latencyA, latencyB, errA, errB, degradedA, degradedB)

	return errA, errB
}

// RecordProbeSuccess credits one successful recovery probe against the cluster.
// After the existing consecutive-fast threshold is reached, the cluster
// transitions back to healthy. Safe to call when the cluster is not degraded
// (no-op in that case).
//
// This feeds the same recovery counter as natural fast writes (via
// [AdaptiveDualWrite.RecordFastWrite]) so strict-only workloads still benefit
// from Helix's auto-healing principle even when no write-side recovery signal
// is generated.
func (a *AdaptiveDualWrite) RecordProbeSuccess(cluster types.ClusterID) {
	a.RecordFastWrite(cluster)
}

// RecordFastWrite manually records a fast write for a cluster.
//
// This is useful for external health probes or testing recovery.
// Call this when you know a cluster responded quickly (e.g., from a
// separate health check mechanism).
//
// Parameters:
//   - cluster: The cluster to record the fast write for
func (a *AdaptiveDualWrite) RecordFastWrite(cluster types.ClusterID) {
	switch cluster {
	case types.ClusterA:
		a.recordFast(&a.stateA)
	case types.ClusterB:
		a.recordFast(&a.stateB)
	}
}
