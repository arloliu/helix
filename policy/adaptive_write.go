package policy

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

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

	// Per-cluster state
	stateA clusterWriteState
	stateB clusterWriteState
}

// clusterWriteState tracks the health state of a single cluster for writes.
type clusterWriteState struct {
	slowStrikes atomic.Int32 // Consecutive slow writes
	fastStrikes atomic.Int32 // Consecutive fast writes (for recovery)
	isDegraded  atomic.Bool  // Fire-and-forget mode
	lastLatency atomic.Int64 // Last successful write latency in nanoseconds
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

	for _, opt := range opts {
		opt(a)
	}

	// Initialize semaphore after options are applied
	a.fireForgetSem = make(chan struct{}, a.fireForgetLimit)

	return a
}

// Execute performs adaptive concurrent writes to both clusters.
//
// For healthy clusters, writes are executed concurrently and waited upon.
// For degraded clusters, writes are fire-and-forget (background goroutine).
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
		errA = a.fireAndForget(writeA, &a.stateA, &a.stateB)
	}

	// Cluster B
	if !degradedB {
		wg.Go(func() {
			start := time.Now()
			errB = writeB(ctx)
			latencyB = time.Since(start)
		})
	} else {
		errB = a.fireAndForget(writeB, &a.stateB, &a.stateA)
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
// Returns:
//   - types.ErrWriteAsync if the write was accepted for background execution
//   - types.ErrWriteDropped if the concurrency limit was reached
func (a *AdaptiveDualWrite) fireAndForget(
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
		if err != nil || latency >= a.absoluteMax {
			// Write failed or was too slow - no recovery credit
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
		}

		// Write succeeded, was fast, and within delta of sibling - record for recovery
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

	// Skip relative delta comparison if we don't have both latencies
	if !hasLatencyA || !hasLatencyB {
		return
	}

	// Process latency comparison
	a.processLatencies(latencyA, latencyB, capViolationA, capViolationB)
}

// handleErrors records strikes for write errors (excluding async writes).
func (a *AdaptiveDualWrite) handleErrors(errA, errB error) {
	if errA != nil && !errors.Is(errA, types.ErrWriteAsync) {
		a.recordStrike(&a.stateA)
	}
	if errB != nil && !errors.Is(errB, types.ErrWriteAsync) {
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
		// One is significantly slower
		if latA > latB {
			a.recordStrike(&a.stateA)
			a.recordFastIfNoViolation(&a.stateB, capViolationB)
		} else {
			a.recordStrike(&a.stateB)
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
func (a *AdaptiveDualWrite) recordStrike(state *clusterWriteState) {
	state.fastStrikes.Store(0) // Reset fast counter

	strikes := state.slowStrikes.Add(1)
	if strikes >= a.strikeThreshold {
		state.isDegraded.Store(true)
	}
}

// recordFast increments the fast counter and potentially recovers the cluster.
func (a *AdaptiveDualWrite) recordFast(state *clusterWriteState) {
	state.slowStrikes.Store(0) // Reset slow counter

	if !state.isDegraded.Load() {
		return // Already healthy, nothing to do
	}

	fast := state.fastStrikes.Add(1)
	if fast >= a.recoveryThreshold {
		state.isDegraded.Store(false)
		state.fastStrikes.Store(0)
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
	a.stateA.slowStrikes.Store(0)
	a.stateA.fastStrikes.Store(0)
	a.stateA.isDegraded.Store(false)

	a.stateB.slowStrikes.Store(0)
	a.stateB.fastStrikes.Store(0)
	a.stateB.isDegraded.Store(false)
}

// ForceDegrade manually marks a cluster as degraded.
//
// This is useful for testing or manual intervention.
//
// Parameters:
//   - cluster: The cluster to degrade
func (a *AdaptiveDualWrite) ForceDegrade(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		a.stateA.isDegraded.Store(true)
	} else {
		a.stateB.isDegraded.Store(true)
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
		a.stateA.isDegraded.Store(false)
		a.stateA.slowStrikes.Store(0)
		a.stateA.fastStrikes.Store(0)
	} else {
		a.stateB.isDegraded.Store(false)
		a.stateB.slowStrikes.Store(0)
		a.stateB.fastStrikes.Store(0)
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
