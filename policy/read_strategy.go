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
// to maximize cache hits. On failure, it can failover to the secondary cluster.
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
// If the failed cluster is the preferred one and cooldown has passed,
// returns the alternative cluster for failover.
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

	// Check cooldown
	s.mu.RLock()
	if time.Since(s.lastFailoverTime) < s.failoverCooldown {
		s.mu.RUnlock()
		return "", false
	}
	s.mu.RUnlock()

	// Determine alternative
	var alternative types.ClusterID
	if preferred == types.ClusterA {
		alternative = types.ClusterB
	} else {
		alternative = types.ClusterA
	}

	// Update preferred cluster and record failover time
	s.mu.Lock()
	// Double-check cooldown under write lock
	if time.Since(s.lastFailoverTime) < s.failoverCooldown {
		s.mu.Unlock()
		return "", false
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
// Cluster B is only used for writes and as a failover target.
type PrimaryOnlyRead struct {
	failedOver atomic.Bool
}

// NewPrimaryOnlyRead creates a new PrimaryOnlyRead strategy.
//
// Returns:
//   - *PrimaryOnlyRead: A new primary-only read strategy
func NewPrimaryOnlyRead() *PrimaryOnlyRead {
	return &PrimaryOnlyRead{}
}

// Select returns ClusterA unless it has failed over.
//
// Parameters:
//   - ctx: Context (unused)
//
// Returns:
//   - types.ClusterID: ClusterA or ClusterB if failed over
func (p *PrimaryOnlyRead) Select(_ context.Context) types.ClusterID {
	if p.failedOver.Load() {
		return types.ClusterB
	}
	return types.ClusterA
}

// OnSuccess is called when a read succeeds.
//
// Parameters:
//   - cluster: The cluster that succeeded (unused)
func (p *PrimaryOnlyRead) OnSuccess(_ types.ClusterID) {
	// Nothing to do
}

// OnFailure handles read failures.
//
// Parameters:
//   - cluster: The cluster that failed
//   - err: The error
//
// Returns:
//   - types.ClusterID: ClusterB for failover
//   - bool: true if failover should be attempted
func (p *PrimaryOnlyRead) OnFailure(cluster types.ClusterID, _ error) (types.ClusterID, bool) {
	if cluster == types.ClusterA && !p.failedOver.Load() {
		p.failedOver.Store(true)
		return types.ClusterB, true
	}
	return "", false
}

// Reset resets the failover state back to primary.
func (p *PrimaryOnlyRead) Reset() {
	p.failedOver.Store(false)
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
