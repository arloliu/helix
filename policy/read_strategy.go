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
// Within the cooldown the preferred cluster still moves when the current
// preferred fails and the other cluster is known good: it has reported a
// success (OnSuccess) since its own last failure, for example through the
// per-request failover that ran on it. Two clusters that keep failing in
// turn therefore do not oscillate, while a preferred that is hard down after
// a single blip on the other is abandoned on the second failed read.
//
// [StickyRead.SetPreferred] and [StickyRead.Reset] let an operator move the
// preference by hand.
type StickyRead struct {
	preferred        atomic.Value // types.ClusterID
	initial          types.ClusterID
	mu               sync.RWMutex
	lastFailoverTime time.Time
	failoverCooldown time.Duration
	// knownBad marks a cluster that failed and has not succeeded since;
	// index 0 is cluster A and index 1 is cluster B.
	knownBad [2]atomic.Bool
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
		if d < 0 {
			return
		}
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
		if !isKnownCluster(cluster) {
			return
		}
		s.initial = cluster
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
		s.initial = types.ClusterA
	} else {
		s.initial = types.ClusterB
	}

	for _, opt := range opts {
		opt(s)
	}
	s.preferred.Store(s.initial)

	return s
}

// knownBadSlot returns the known-bad mark of a cluster, or nil for an
// unknown cluster.
func (s *StickyRead) knownBadSlot(cluster types.ClusterID) *atomic.Bool {
	switch cluster {
	case types.ClusterA:
		return &s.knownBad[0]
	case types.ClusterB:
		return &s.knownBad[1]
	default:
		return nil
	}
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
//   - cluster: The cluster that succeeded; its known-bad mark is cleared
func (s *StickyRead) OnSuccess(cluster types.ClusterID) {
	// Store only on the transition so the steady state stays read-only.
	if bad := s.knownBadSlot(cluster); bad != nil && bad.Load() {
		bad.Store(false)
	}
}

// OnFailure handles read failures and determines failover.
//
// If the failed cluster is the preferred one, returns the alternative cluster
// for failover. When cooldown has passed, the preferred cluster is also
// switched. When cooldown is still active, the preferred cluster is switched
// only if the alternative is known good (see the type documentation);
// otherwise the alternative is returned for the current request but preferred
// is not changed — this prevents reads from failing entirely while still
// avoiding rapid preferred-cluster oscillation.
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
	if bad := s.knownBadSlot(cluster); bad != nil {
		bad.Store(true)
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

	// Within the cooldown the preference moves only when the alternative is
	// known good; otherwise the alternative serves this request alone, so
	// reads still succeed while two flapping clusters cannot swap the
	// preference back and forth.
	alternativeBad := s.knownBadSlot(alternative).Load()
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if now.Sub(s.lastFailoverTime) < s.failoverCooldown && alternativeBad {
		return alternative, true
	}
	s.preferred.Store(alternative)
	s.lastFailoverTime = now

	return alternative, true
}

// SetPreferred moves the preferred cluster by hand and restarts the
// cooldown, for an operator who knows which cluster should serve reads.
// An unknown cluster is ignored.
//
// Parameters:
//   - cluster: The cluster to prefer from now on
func (s *StickyRead) SetPreferred(cluster types.ClusterID) {
	if !isKnownCluster(cluster) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.preferred.Store(cluster)
	s.lastFailoverTime = time.Now()
}

// Reset returns the preference to the cluster chosen at construction (by
// [WithPreferredCluster] or at random), clears the cooldown, and forgets
// which clusters are known bad.
func (s *StickyRead) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.preferred.Store(s.initial)
	s.lastFailoverTime = time.Time{}
	for i := range s.knownBad {
		s.knownBad[i].Store(false)
	}
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
//     elapses, after which Select hands ClusterA to one caller as the probe, or
//   - Cluster B itself fails while in the failed-over state, at which point
//     OnFailure returns ClusterA as a probe; if that probe succeeds,
//     [PrimaryOnlyRead.OnSuccess] resets the failover state.
//
// Without a recovery timeout, failover is permanent until Reset is called or
// Cluster B also fails — if only ClusterA recovers while B stays healthy, the
// client stays on ClusterB indefinitely.
type PrimaryOnlyRead struct {
	failedOver atomic.Bool
	// nextProbeAt is the Unix nano after which one caller may be handed
	// ClusterA as the recovery probe; 0 = not failed over.
	// The Select that wins the probe re-arms it, so an unreported probe
	// expires after another recovery timeout.
	nextProbeAt     atomic.Int64
	recoveryTimeout time.Duration
}

// PrimaryOnlyReadOption configures a PrimaryOnlyRead strategy.
type PrimaryOnlyReadOption func(*PrimaryOnlyRead)

// WithPrimaryOnlyRecoveryTimeout sets the duration after which a failed-over
// PrimaryOnlyRead will automatically attempt to return reads to ClusterA.
//
// When the timeout elapses, one Select call returns ClusterA as the probe;
// the other callers keep reading ClusterB until that probe reports, and a
// probe that never reports expires after another timeout.
// If the probe read succeeds (OnSuccess called), the strategy returns to
// ClusterA. If it fails again (OnFailure called), the failover timer resets.
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
	// Auto-recovery: once the recovery timeout has elapsed, hand cluster A
	// to exactly one caller as the probe; everyone else keeps reading B
	// until that probe reports. A probe whose caller never reports (its
	// context ended) expires after another recovery timeout.
	if p.recoveryTimeout > 0 {
		nowNs := time.Now().UnixNano()
		next := p.nextProbeAt.Load()
		if next > 0 && nowNs >= next && p.nextProbeAt.CompareAndSwap(next, nowNs+int64(p.recoveryTimeout)) {
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
		p.Reset()
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
		p.nextProbeAt.Store(time.Now().Add(p.recoveryTimeout).UnixNano())
		return types.ClusterB, true
	}

	// ClusterB failed while in the failed-over state — probe ClusterA.
	// This handles the scenario where A failed, we switched to B, A recovered,
	// and now B fails. Without this, reads would fail entirely even though A
	// may be available again.
	//
	// State is NOT reset here. If the probe succeeds, the caller invokes
	// OnSuccess(ClusterA) which resets the failover state. If A is also
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
	p.nextProbeAt.Store(0)
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
	if cluster != types.ClusterB {
		return "", false
	}
	return types.ClusterA, true
}
