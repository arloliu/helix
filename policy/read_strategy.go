// Package policy provides read and write strategies for Helix dual-cluster operations.
package policy

import (
	"context"
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/helix/types"
)

// StickyRead implements a sticky read strategy that routes reads to a preferred cluster.
//
// The preferred cluster is randomly selected at initialization and sticks to it
// to maximize cache hits. On failure, it can fail over to the secondary cluster.
// Cooldown only gates future state changes; it does not trigger passive probing
// back to a recovered cluster in the absence of another read failure.
//
// StickyRead has no programmatic way to return the preferred cluster to its
// original choice once it has failed over (unlike PrimaryOnlyRead.Reset).
// To force preferred back to a specific cluster, the operator must:
//   - wait for the current preferred to fail (failover will swap them again,
//     subject to cooldown), or
//   - reconstruct a new StickyRead with WithPreferredCluster and rebuild the
//     CQLClient.
type StickyRead struct {
	preferred        atomic.Value // types.ClusterID
	mu               sync.RWMutex
	lastFailoverTime time.Time
	failoverCooldown time.Duration
}

// StickyReadOption configures a StickyRead strategy.
type StickyReadOption func(*StickyRead)

// WithFailoverCooldown sets the cooldown period after a failover.
//
// Parameters:
//   - d: Duration to wait before allowing another failover
//
// Returns:
//   - StickyReadOption: Configuration option
func WithStickyReadCooldown(d time.Duration) StickyReadOption {
	return func(s *StickyRead) {
		s.failoverCooldown = d
	}
}

// WithPreferredCluster sets the initial preferred cluster.
//
// Parameters:
//   - cluster: The cluster to prefer initially
//
// Returns:
//   - StickyReadOption: Configuration option
func WithPreferredCluster(cluster types.ClusterID) StickyReadOption {
	return func(s *StickyRead) {
		s.preferred.Store(cluster)
	}
}

// NewStickyRead creates a new StickyRead strategy.
//
// By default, the preferred cluster is randomly selected (50/50 between A and B)
// and the failover cooldown is 5 minutes.
//
// Parameters:
//   - opts: Optional configuration options
//
// Returns:
//   - *StickyRead: A new sticky read strategy
func NewStickyRead(opts ...StickyReadOption) *StickyRead {
	s := &StickyRead{
		failoverCooldown: 5 * time.Minute,
	}

	// Random initial selection for load distribution
	// Use crypto/rand for secure randomness
	n, err := rand.Int(rand.Reader, big.NewInt(2))
	if err != nil || n.Int64() == 0 {
		s.preferred.Store(types.ClusterA)
	} else {
		s.preferred.Store(types.ClusterB)
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Select returns the preferred cluster for reading.
//
// Parameters:
//   - ctx: Context (unused but required by interface)
//
// Returns:
//   - types.ClusterID: The preferred cluster
func (s *StickyRead) Select(_ context.Context) types.ClusterID {
	if v, ok := s.preferred.Load().(types.ClusterID); ok {
		return v
	}

	return types.ClusterA
}

// OnSuccess is called when a read succeeds.
//
// Parameters:
//   - cluster: The cluster that succeeded (unused for sticky reads)
func (s *StickyRead) OnSuccess(_ types.ClusterID) {
	// Nothing to do for sticky reads on success
}

// OnFailure handles read failures and determines failover.
//
// If the failed cluster is the preferred one, returns the alternative cluster
// for failover. When cooldown has passed, the preferred cluster is also
// switched. When cooldown is still active, the alternative is returned for the
// current request but preferred is not changed — this prevents reads from
// failing entirely while still avoiding rapid preferred-cluster oscillation.
// Cooldown expiry alone does not change the preferred cluster; a later failure
// on the current preferred cluster is still required to switch back.
//
// Parameters:
//   - cluster: The cluster that failed
//   - err: The error (unused)
//
// Returns:
//   - types.ClusterID: Alternative cluster to try
//   - bool: true if failover should be attempted
func (s *StickyRead) OnFailure(cluster types.ClusterID, _ error) (types.ClusterID, bool) {
	preferred, ok := s.preferred.Load().(types.ClusterID)
	if !ok {
		return "", false
	}

	// Only failover if the preferred cluster failed
	if cluster != preferred {
		return "", false
	}

	// Determine alternative
	var alternative types.ClusterID
	if preferred == types.ClusterA {
		alternative = types.ClusterB
	} else {
		alternative = types.ClusterA
	}

	// Check cooldown — if still within cooldown, return the alternative for
	// this request but do NOT change preferred. This prevents reads from
	// failing entirely when the current preferred is down and cooldown blocks
	// a state change (e.g., A failed → switched to B → B fails within cooldown).
	s.mu.RLock()
	if time.Since(s.lastFailoverTime) < s.failoverCooldown {
		s.mu.RUnlock()
		return alternative, true
	}
	s.mu.RUnlock()

	// Update preferred cluster and record failover time
	s.mu.Lock()
	// Double-check cooldown under write lock
	if time.Since(s.lastFailoverTime) < s.failoverCooldown {
		s.mu.Unlock()
		// Still return alternative for this request even under the write-lock re-check
		return alternative, true
	}
	s.preferred.Store(alternative)
	s.lastFailoverTime = time.Now()
	s.mu.Unlock()

	return alternative, true
}

// Preferred returns the current preferred cluster.
//
// Returns:
//   - types.ClusterID: The current preferred cluster
func (s *StickyRead) Preferred() types.ClusterID {
	if v, ok := s.preferred.Load().(types.ClusterID); ok {
		return v
	}

	return types.ClusterA
}

// PrimaryOnlyRead implements a read strategy that always reads from Cluster A.
//
// Cluster B is only used for writes and as a failover target. Once Cluster A
// fails, reads are redirected to Cluster B until one of:
//   - [PrimaryOnlyRead.Reset] is called manually, or
//   - The optional recovery timeout (set via [WithPrimaryOnlyRecoveryTimeout])
//     elapses, after which Select will probe ClusterA again on the next call, or
//   - Cluster B itself fails while in the failed-over state, at which point
//     OnFailure returns ClusterA as a probe; if that probe succeeds,
//     [PrimaryOnlyRead.OnSuccess] resets the failover state.
//
// Without a recovery timeout, failover is permanent until Reset is called or
// Cluster B also fails — if only ClusterA recovers while B stays healthy, the
// client stays on ClusterB indefinitely.
type PrimaryOnlyRead struct {
	failedOver      atomic.Bool
	failoverTime    atomic.Int64 // Unix nano of last failover; 0 = not failed over
	recoveryTimeout time.Duration
}

// PrimaryOnlyReadOption configures a PrimaryOnlyRead strategy.
type PrimaryOnlyReadOption func(*PrimaryOnlyRead)

// WithPrimaryOnlyRecoveryTimeout sets the duration after which a failed-over
// PrimaryOnlyRead will automatically attempt to return reads to ClusterA.
//
// When the timeout elapses, the next Select call returns ClusterA as a probe.
// If that read succeeds (OnSuccess called), the strategy remains on ClusterA.
// If it fails again (OnFailure called), the failover timer resets.
//
// A zero or negative value disables auto-recovery (default: disabled).
//
// Parameters:
//   - d: Recovery timeout duration
//
// Returns:
//   - PrimaryOnlyReadOption: Configuration option
func WithPrimaryOnlyRecoveryTimeout(d time.Duration) PrimaryOnlyReadOption {
	return func(p *PrimaryOnlyRead) {
		p.recoveryTimeout = d
	}
}

// NewPrimaryOnlyRead creates a new PrimaryOnlyRead strategy.
//
// Parameters:
//   - opts: Optional configuration options
//
// Returns:
//   - *PrimaryOnlyRead: A new primary-only read strategy
func NewPrimaryOnlyRead(opts ...PrimaryOnlyReadOption) *PrimaryOnlyRead {
	p := &PrimaryOnlyRead{}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Select returns ClusterA unless it has failed over.
//
// If a recovery timeout is configured and has elapsed since the last failover,
// ClusterA is returned as a probe — allowing the caller to re-evaluate whether
// ClusterA has recovered.
//
// Parameters:
//   - ctx: Context (unused)
//
// Returns:
//   - types.ClusterID: ClusterA or ClusterB if failed over
func (p *PrimaryOnlyRead) Select(_ context.Context) types.ClusterID {
	if !p.failedOver.Load() {
		return types.ClusterA
	}
	// Auto-recovery: if recovery timeout is set and has elapsed, probe ClusterA.
	if p.recoveryTimeout > 0 {
		failoverAt := p.failoverTime.Load()
		if failoverAt > 0 && time.Duration(time.Now().UnixNano()-failoverAt) >= p.recoveryTimeout {
			return types.ClusterA
		}
	}

	return types.ClusterB
}

// OnSuccess is called when a read succeeds.
//
// If ClusterA succeeds while in failed-over state, the strategy resets back
// to ClusterA — completing auto-recovery.
//
// Parameters:
//   - cluster: The cluster that succeeded
func (p *PrimaryOnlyRead) OnSuccess(cluster types.ClusterID) {
	if cluster == types.ClusterA && p.failedOver.Load() {
		p.failedOver.Store(false)
		p.failoverTime.Store(0)
	}
}

// OnFailure handles read failures.
//
// If ClusterA fails and has not yet failed over, routes to ClusterB.
// If ClusterA fails during an auto-recovery probe, resets the recovery timer
// so another probe will only occur after another full recovery timeout.
// If ClusterB fails while in the failed-over state, returns ClusterA as a
// probe without resetting state. If the caller's retry on ClusterA succeeds,
// OnSuccess clears the failover flag. If ClusterA is also down, the state
// stays failed-over to B, avoiding request-level A/B flipping.
//
// Parameters:
//   - cluster: The cluster that failed
//   - err: The error
//
// Returns:
//   - types.ClusterID: Alternative cluster to try, or empty if no failover
//   - bool: true if failover should be attempted
func (p *PrimaryOnlyRead) OnFailure(cluster types.ClusterID, _ error) (types.ClusterID, bool) {
	if cluster == types.ClusterA {
		p.failedOver.Store(true)
		p.failoverTime.Store(time.Now().UnixNano())
		return types.ClusterB, true
	}

	// ClusterB failed while in the failed-over state — probe ClusterA.
	// This handles the scenario where A failed, we switched to B, A recovered,
	// and now B fails. Without this, reads would fail entirely even though A
	// may be available again.
	//
	// State is NOT reset here. If the probe succeeds, the caller invokes
	// OnSuccess(ClusterA) which clears failedOver/failoverTime. If A is also
	// down, both attempts fail (DualClusterError) and the next Select() still
	// returns ClusterB — avoiding request-level A/B flipping.
	//
	// Guard: only probe A when genuinely failed-over. The caller may route to B
	// due to drain-state override even when not failed over; in that case we
	// must not suggest A (the drain-excluded cluster) as an alternative.
	if cluster == types.ClusterB && p.failedOver.Load() {
		return types.ClusterA, true
	}

	return "", false
}

// Reset resets the failover state back to primary immediately.
func (p *PrimaryOnlyRead) Reset() {
	p.failedOver.Store(false)
	p.failoverTime.Store(0)
}

// RoundRobinRead implements a read strategy that alternates between clusters.
//
// This provides even load distribution but lower cache efficiency.
type RoundRobinRead struct {
	counter atomic.Uint64
}

// NewRoundRobinRead creates a new RoundRobinRead strategy.
//
// Returns:
//   - *RoundRobinRead: A new round-robin read strategy
func NewRoundRobinRead() *RoundRobinRead {
	return &RoundRobinRead{}
}

// Select returns alternating clusters on each call.
//
// Parameters:
//   - ctx: Context (unused)
//
// Returns:
//   - types.ClusterID: Alternating between ClusterA and ClusterB
func (r *RoundRobinRead) Select(_ context.Context) types.ClusterID {
	count := r.counter.Add(1)
	if count%2 == 0 {
		return types.ClusterA
	}
	return types.ClusterB
}

// OnSuccess is called when a read succeeds.
//
// Parameters:
//   - cluster: The cluster that succeeded (unused)
func (r *RoundRobinRead) OnSuccess(_ types.ClusterID) {
	// Nothing to do
}

// OnFailure handles read failures.
//
// Parameters:
//   - cluster: The cluster that failed
//   - err: The error
//
// Returns:
//   - types.ClusterID: The other cluster
//   - bool: true (always attempt failover)
func (r *RoundRobinRead) OnFailure(cluster types.ClusterID, _ error) (types.ClusterID, bool) {
	if cluster == types.ClusterA {
		return types.ClusterB, true
	}
	return types.ClusterA, true
}
