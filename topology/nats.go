package topology

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/types"
)

// NATS monitors a NATS KV bucket for drain mode configuration.
//
// It watches a configurable key and emits TopologyUpdate events when the
// drain status of any cluster changes. This enables operations teams to
// gracefully drain traffic before cluster maintenance.
//
// Watch() should be called once per instance. Subsequent calls return the
// same channel. The channel is closed when Close() is called or the context
// is cancelled.
type NATS struct {
	kv     jetstream.KeyValue
	config WatcherConfig

	// Current drain state
	drainA      bool
	drainB      bool
	drainReason string
	mu          sync.RWMutex

	// Lifecycle
	updates      chan helix.TopologyUpdate
	done         chan struct{}
	closed       bool
	watchStarted bool
	closeOnce    sync.Once
}

var _ helix.TopologyWatcher = (*NATS)(nil)

// NewNATS creates a new NATS KV topology watcher.
//
// The watcher will begin monitoring the KV bucket for drain configuration
// when Watch() is called.
//
// Parameters:
//   - kv: A NATS JetStream KeyValue store
//   - opts: Optional configuration options
//
// Returns:
//   - *NATS: A new watcher instance
//   - error: Error if kv is nil
//
// Example:
//
//	nc, _ := nats.Connect("nats://localhost:4222")
//	js, _ := jetstream.New(nc)
//	kv, _ := js.KeyValue(ctx, "helix-config")
//
//	watcher, _ := topology.NewNATS(kv,
//	    topology.WithKey("topology.drain"),
//	    topology.WithPollInterval(10*time.Second),
//	)
func NewNATS(kv jetstream.KeyValue, opts ...WatcherOption) (*NATS, error) {
	if kv == nil {
		return nil, errors.New("helix/topology: KeyValue store is nil")
	}

	config := DefaultWatcherConfig()
	for _, opt := range opts {
		opt(&config)
	}

	return &NATS{
		kv:      kv,
		config:  config,
		updates: make(chan helix.TopologyUpdate, 10),
		done:    make(chan struct{}),
	}, nil
}

// Watch returns a channel that receives topology updates.
//
// The watcher spawns a background goroutine that monitors the NATS KV key.
// When the drain configuration changes, it emits TopologyUpdate events
// for each affected cluster.
//
// The channel is closed when Close() is called or the context is cancelled.
// Multiple calls to Watch return the same channel; only the first call's
// context controls the watch lifecycle.
//
// Parameters:
//   - ctx: Context for cancellation (only used on first call)
//
// Returns:
//   - <-chan helix.TopologyUpdate: Channel of topology changes
func (n *NATS) Watch(ctx context.Context) <-chan helix.TopologyUpdate {
	n.mu.Lock()
	if n.watchStarted {
		n.mu.Unlock()

		return n.updates
	}
	n.watchStarted = true
	n.mu.Unlock()

	go n.watchLoop(ctx)

	return n.updates
}

// Close stops the watcher and releases resources.
//
// This method is safe to call multiple times.
func (n *NATS) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.closed {
		return nil
	}

	n.closed = true
	close(n.done)

	return nil
}

// IsDraining returns whether the specified cluster is currently in drain mode.
//
// This provides a synchronous way to check drain status without waiting
// for channel updates.
//
// Parameters:
//   - cluster: The cluster to check
//
// Returns:
//   - bool: true if the cluster is being drained
func (n *NATS) IsDraining(cluster types.ClusterID) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	switch cluster {
	case types.ClusterA:
		return n.drainA
	case types.ClusterB:
		return n.drainB
	}

	return false
}

// Config returns the watcher configuration.
//
// This method is primarily useful for testing to verify configuration options.
//
// Returns:
//   - WatcherConfig: The current watcher configuration
func (n *NATS) Config() WatcherConfig {
	return n.config
}

// GetDrainReason returns the current drain reason, if any.
//
// This returns the cached reason from the last processed KV entry.
// It does not perform a live KV fetch.
//
// Returns:
//   - string: The drain reason, or empty if not draining
func (n *NATS) GetDrainReason() string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.drainReason
}

// watchLoop is the main watch loop that monitors the NATS KV key.
func (n *NATS) watchLoop(ctx context.Context) {
	defer n.closeOnce.Do(func() { close(n.updates) })

	// Initial fetch
	n.fetchAndEmit(ctx)

	// Start watching
	watcher, err := n.kv.Watch(ctx, n.config.Key)
	if err != nil {
		// Fall back to polling if watch fails
		n.pollLoop(ctx)
		return
	}
	defer func() { _ = watcher.Stop() }()

	for {
		select {
		case <-ctx.Done():
			return
		case <-n.done:
			return
		case entry, ok := <-watcher.Updates():
			if !ok {
				// Watcher channel closed, fall back to polling
				n.pollLoop(ctx)
				return
			}
			if entry == nil {
				// Initial nil entry, skip
				continue
			}
			n.processEntry(entry)
		}
	}
}

// pollLoop is a fallback polling loop when watch fails.
func (n *NATS) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(n.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-n.done:
			return
		case <-ticker.C:
			n.fetchAndEmit(ctx)
		}
	}
}

// fetchAndEmit fetches the current KV value and emits updates if changed.
func (n *NATS) fetchAndEmit(ctx context.Context) {
	fetchCtx, cancel := context.WithTimeout(ctx, n.config.InitialFetchTimeout)
	defer cancel()

	entry, err := n.kv.Get(fetchCtx, n.config.Key)
	if err != nil {
		// Key doesn't exist or error - treat as no drain
		n.handleNoDrain()
		return
	}

	n.processEntry(entry)
}

// processEntry parses a KV entry and emits topology updates.
func (n *NATS) processEntry(entry jetstream.KeyValueEntry) {
	// Handle deletion
	if entry.Operation() == jetstream.KeyValueDelete || entry.Operation() == jetstream.KeyValuePurge {
		n.handleNoDrain()
		return
	}

	var config DrainConfig
	if err := json.Unmarshal(entry.Value(), &config); err != nil {
		// Invalid JSON - treat as no drain
		n.handleNoDrain()
		return
	}

	// Cache the drain reason
	n.mu.Lock()
	n.drainReason = config.Reason
	n.mu.Unlock()

	// Calculate new drain states
	newDrainA := config.ContainsCluster(types.ClusterA)
	newDrainB := config.ContainsCluster(types.ClusterB)

	n.updateDrainState(types.ClusterA, newDrainA)
	n.updateDrainState(types.ClusterB, newDrainB)
}

// handleNoDrain clears all drain states and reason.
func (n *NATS) handleNoDrain() {
	n.mu.Lock()
	n.drainReason = ""
	n.mu.Unlock()

	n.updateDrainState(types.ClusterA, false)
	n.updateDrainState(types.ClusterB, false)
}

// updateDrainState updates the drain state for a cluster and emits an update if changed.
func (n *NATS) updateDrainState(cluster types.ClusterID, draining bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	var current *bool
	switch cluster {
	case types.ClusterA:
		current = &n.drainA
	case types.ClusterB:
		current = &n.drainB
	default:
		return
	}

	// Only emit if state changed
	if *current == draining {
		return
	}

	*current = draining

	// Emit update (non-blocking)
	select {
	case n.updates <- helix.TopologyUpdate{
		Cluster:   cluster,
		Available: !draining,
		DrainMode: draining,
	}:
	default:
		// Channel full, skip update (older updates are stale anyway)
	}
}
