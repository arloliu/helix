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
	clusterNames    atomic.Pointer[types.ClusterNames]

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
		if n > 0 && n <= maxInt32 {
			a.strikeThreshold = int32(n)
		}
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
		if n > 0 && n <= maxInt32 {
			a.recoveryThreshold = int32(n)
		}
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
		a.metrics = m
		a.metricsExplicit = true
	}
}

// WithAdaptiveLogger sets the logger for fire-and-forget background writes.
//
// Parameters:
//   - l: The logger
//
// Returns:
//   - AdaptiveDualWriteOption: Configuration option
func WithAdaptiveLogger(l types.Logger) AdaptiveDualWriteOption {
	return func(a *AdaptiveDualWrite) {
		a.logger = l
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
		if n > 0 && n <= maxInt32 {
			a.fireForgetLimit = int32(n)
		}
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
func NewAdaptiveDualWrite(opts ...AdaptiveDualWriteOption) *AdaptiveDualWrite {
	a := &AdaptiveDualWrite{
		deltaThreshold:    300 * time.Millisecond,
		absoluteMax:       2 * time.Second,
		minFloor:          100 * time.Millisecond,
		strikeThreshold:   3,
		recoveryThreshold: 5,
		fireForgetTimeout: 30 * time.Second,
		fireForgetLimit:   100,
	}
	defaultNames := types.DefaultClusterNames()
	a.clusterNames.Store(&defaultNames)

	for _, opt := range opts {
		opt(a)
	}

	// Ensure metrics and logger are never nil — fireAndForget calls them
	// from a background goroutine and a nil deref there would crash the
	// process under load.
	if a.metrics == nil {
		a.metrics = metrics.NewNopMetrics()
	}
	if a.logger == nil {
		a.logger = logging.NewNopLogger()
	}

	// Initialize semaphore after options are applied
	a.fireForgetSem = make(chan struct{}, a.fireForgetLimit)

	return a
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

// SetLogger replaces the logger. Used by helix.NewCQLClient to propagate
// the client logger into the strategy if the caller didn't provide one.
func (a *AdaptiveDualWrite) SetLogger(l types.Logger) {
	if l == nil {
		return
	}
	a.logger = l
}

// clusterName returns the display name for the given cluster.
func (a *AdaptiveDualWrite) clusterName(cluster types.ClusterID) string {
	if names := a.clusterNames.Load(); names != nil {
		return names.Name(cluster)
	}
	return string(cluster)
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

	// Cluster A
	if !degradedA {
		wg.Go(func() {
			start := time.Now()
			errA = writeA(ctx)
			latencyA = time.Since(start)
		})
	} else {
		errA = a.fireAndForget(types.ClusterA, writeA, &a.stateA, &a.stateB)
	}

	// Cluster B
	if !degradedB {
		wg.Go(func() {
			start := time.Now()
			errB = writeB(ctx)
			latencyB = time.Since(start)
		})
	} else {
		errB = a.fireAndForget(types.ClusterB, writeB, &a.stateB, &a.stateA)
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
func (a *AdaptiveDualWrite) fireAndForget(
	cluster types.ClusterID,
	write func(context.Context) error,
	state *clusterWriteState,
	siblingState *clusterWriteState,
) error {
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
		err := write(ctx)
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
// ErrWriteAsync and ErrWriteDropped are excluded: both are expected operational
// states for degraded clusters, not performance signals.
func (a *AdaptiveDualWrite) handleErrors(errA, errB error) {
	if errA != nil && !errors.Is(errA, types.ErrWriteAsync) && !errors.Is(errA, types.ErrWriteDropped) {
		a.recordStrike(&a.stateA)
	}
	if errB != nil && !errors.Is(errB, types.ErrWriteAsync) && !errors.Is(errB, types.ErrWriteDropped) {
		a.recordStrike(&a.stateB)
	}
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

// recordStrike increments the slow strike counter and potentially degrades the cluster.
// Must be called with state.mu unlocked.
func (a *AdaptiveDualWrite) recordStrike(state *clusterWriteState) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.fastStrikes = 0
	state.slowStrikes++
	if state.slowStrikes >= a.strikeThreshold {
		state.isDegraded.Store(true)
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
	state.mu.Lock()
	defer state.mu.Unlock()
	state.slowStrikes = 0 // Always clear, regardless of degraded state.
	if !state.isDegraded.Load() {
		return // Healthy — no recovery credit needed.
	}
	state.fastStrikes++
	if state.fastStrikes >= a.recoveryThreshold {
		state.isDegraded.Store(false)
		state.fastStrikes = 0
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
	return a.stateB.isDegraded.Load()
}

// Reset clears all health state, returning both clusters to healthy.
//
// This is useful for testing or manual intervention.
func (a *AdaptiveDualWrite) Reset() {
	a.stateA.mu.Lock()
	a.stateA.slowStrikes = 0
	a.stateA.fastStrikes = 0
	a.stateA.isDegraded.Store(false)
	a.stateA.mu.Unlock()

	a.stateB.mu.Lock()
	a.stateB.slowStrikes = 0
	a.stateB.fastStrikes = 0
	a.stateB.isDegraded.Store(false)
	a.stateB.mu.Unlock()
}

// ForceDegrade manually marks a cluster as degraded.
//
// This is useful for testing or manual intervention. The call acquires the
// cluster's mutex so that fastStrikes is reset atomically with the degraded
// transition — preventing a stale fast-strike accumulation from immediately
// recovering the cluster on the very next recordFast call.
//
// Parameters:
//   - cluster: The cluster to degrade
func (a *AdaptiveDualWrite) ForceDegrade(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		a.stateA.mu.Lock()
		a.stateA.fastStrikes = 0
		a.stateA.isDegraded.Store(true)
		a.stateA.mu.Unlock()
	} else {
		a.stateB.mu.Lock()
		a.stateB.fastStrikes = 0
		a.stateB.isDegraded.Store(true)
		a.stateB.mu.Unlock()
	}
}

// ForceRecover manually marks a cluster as healthy.
//
// This is useful for testing or manual intervention when you know
// a cluster has recovered (e.g., from external health checks).
//
// Parameters:
//   - cluster: The cluster to recover
func (a *AdaptiveDualWrite) ForceRecover(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		a.stateA.mu.Lock()
		a.stateA.isDegraded.Store(false)
		a.stateA.slowStrikes = 0
		a.stateA.fastStrikes = 0
		a.stateA.mu.Unlock()
	} else {
		a.stateB.mu.Lock()
		a.stateB.isDegraded.Store(false)
		a.stateB.slowStrikes = 0
		a.stateB.fastStrikes = 0
		a.stateB.mu.Unlock()
	}
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
	if cluster == types.ClusterA {
		a.recordFast(&a.stateA)
	} else {
		a.recordFast(&a.stateB)
	}
}
