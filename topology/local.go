package topology

import (
	"context"
	"sync"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/types"
)

// Local provides an in-memory topology watcher and operator for testing.
//
// Unlike NATS, this implementation allows programmatic control
// of drain states, making it ideal for unit tests and demos.
// It implements both TopologyWatcher (for observing) and TopologyOperator
// (for controlling drain states).
type Local struct {
	drainA      bool
	drainB      bool
	drainReason string
	mu          sync.RWMutex

	updates       chan helix.TopologyUpdate
	done          chan struct{}
	closed        bool
	updatesClosed bool
}

var (
	_ helix.TopologyWatcher  = (*Local)(nil)
	_ helix.TopologyOperator = (*Local)(nil)
)

// NewLocal creates a new in-memory topology watcher/operator.
//
// Returns:
//   - *Local: A new local topology instance
func NewLocal() *Local {
	return &Local{
		updates: make(chan helix.TopologyUpdate, 10),
		done:    make(chan struct{}),
	}
}

// Watch returns a channel that receives topology updates.
//
// Updates are emitted when SetDrain is called. The channel is closed
// when Close() is called or the context is cancelled.
//
// Multiple calls to Watch return the same channel; only the first call's
// context controls the watch lifecycle.
//
// Parameters:
//   - ctx: Context for cancellation (only used on first call)
//
// Returns:
//   - <-chan helix.TopologyUpdate: Channel of topology changes
func (l *Local) Watch(ctx context.Context) <-chan helix.TopologyUpdate {
	go l.waitForClose(ctx)
	return l.updates
}

// SetDrain sets the drain state for a cluster.
//
// This method emits a TopologyUpdate if the state changes.
//
// Parameters:
//   - ctx: Context for cancellation. For the local in-memory implementation,
//     this parameter is accepted for interface compliance but not used.
//   - cluster: The cluster to update
//   - draining: true to enable drain mode, false to disable
//   - reason: Human-readable reason for the drain (only used when draining=true)
//
// Returns:
//   - error: Always nil for local implementation
func (l *Local) SetDrain(_ context.Context, cluster types.ClusterID, draining bool, reason string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed || l.updatesClosed {
		return nil
	}

	var current *bool
	switch cluster {
	case types.ClusterA:
		current = &l.drainA
	case types.ClusterB:
		current = &l.drainB
	default:
		return nil
	}

	// Only emit if state changed
	if *current == draining {
		return nil
	}

	*current = draining
	if draining {
		l.drainReason = reason
	} else if !l.drainA && !l.drainB {
		// Clear reason only when no clusters are draining
		l.drainReason = ""
	}

	// Emit update (non-blocking)
	select {
	case l.updates <- helix.TopologyUpdate{
		Cluster:   cluster,
		Available: !draining,
		DrainMode: draining,
	}:
	default:
		// Channel full, skip update
	}

	return nil
}

// IsDraining returns whether the specified cluster is currently in drain mode.
//
// Parameters:
//   - cluster: The cluster to check
//
// Returns:
//   - bool: true if the cluster is being drained
func (l *Local) IsDraining(cluster types.ClusterID) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	switch cluster {
	case types.ClusterA:
		return l.drainA
	case types.ClusterB:
		return l.drainB
	default:
		return false
	}
}

// GetDrainReason returns the current drain reason, if any.
//
// Returns:
//   - string: The drain reason, or empty string if not draining
func (l *Local) GetDrainReason() string {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.drainReason
}

// Close stops the watcher and releases resources.
func (l *Local) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}

	l.closed = true
	close(l.done)

	return nil
}

// waitForClose waits for context cancellation or close signal.
func (l *Local) waitForClose(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-l.done:
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.updatesClosed {
		l.updatesClosed = true
		close(l.updates)
	}
}
