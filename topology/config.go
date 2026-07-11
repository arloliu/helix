package topology

import (
	"slices"
	"time"

	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/internal/typeutil"
	"github.com/arloliu/helix/types"
)

// DrainConfig represents the drain mode configuration stored in NATS KV.
//
// This is the JSON structure that operations teams PUT to the KV store
// to signal cluster maintenance.
type DrainConfig struct {
	// Drain lists the clusters currently being drained.
	// Valid values: "A", "B", or both.
	Drain []types.ClusterID `json:"drain"`

	// Reason is a human-readable explanation for the drain.
	// Example: "OS Patching", "Scaling", "Upgrade to v4.1"
	Reason string `json:"reason,omitempty"`
}

// ContainsCluster returns true if the given cluster is in the drain list.
//
// Parameters:
//   - cluster: The cluster ID to check
//
// Returns:
//   - bool: true if the cluster is being drained
func (d *DrainConfig) ContainsCluster(cluster types.ClusterID) bool {
	return slices.Contains(d.Drain, cluster)
}

// WatcherConfig holds configuration for topology watchers.
type WatcherConfig struct {
	// Key is the NATS KV key to watch for drain configuration.
	// Default: "helix.topology.drain"
	Key string

	// PollInterval is the fallback polling interval if watch fails.
	// Default: 5 seconds
	PollInterval time.Duration

	// InitialFetchTimeout is the timeout for the initial KV fetch.
	// Default: 10 seconds
	InitialFetchTimeout time.Duration

	// Logger receives diagnostic messages about watcher lifecycle events,
	// such as falling back to polling when the underlying watch mechanism
	// fails. Default: a no-op logger that discards all messages.
	Logger types.Logger
}

// DefaultWatcherConfig returns a WatcherConfig with sensible defaults.
//
// Returns:
//   - WatcherConfig: Default configuration
func DefaultWatcherConfig() WatcherConfig {
	return WatcherConfig{
		Key:                 "helix.topology.drain",
		PollInterval:        5 * time.Second,
		InitialFetchTimeout: 10 * time.Second,
		Logger:              logging.NewNopLogger(),
	}
}

// WatcherOption configures a topology watcher.
type WatcherOption func(*WatcherConfig)

// WithKey sets the NATS KV key to watch.
//
// Parameters:
//   - key: The key name (e.g., "storage.topology.maintenance")
//
// Returns:
//   - WatcherOption: Configuration option
func WithKey(key string) WatcherOption {
	return func(c *WatcherConfig) {
		c.Key = key
	}
}

// WithPollInterval sets the fallback polling interval.
//
// If the NATS watch fails or disconnects, the watcher falls back to
// polling at this interval.
//
// Parameters:
//   - d: Polling interval duration
//
// Returns:
//   - WatcherOption: Configuration option
func WithPollInterval(d time.Duration) WatcherOption {
	return func(c *WatcherConfig) {
		c.PollInterval = d
	}
}

// WithInitialFetchTimeout sets the timeout for the initial KV fetch.
//
// Parameters:
//   - d: Timeout duration
//
// Returns:
//   - WatcherOption: Configuration option
func WithInitialFetchTimeout(d time.Duration) WatcherOption {
	return func(c *WatcherConfig) {
		c.InitialFetchTimeout = d
	}
}

// WithLogger sets the logger used for watcher diagnostic messages.
//
// If not set (or set to nil, including a typed nil such as a nil `*myLogger`
// assigned to the types.Logger parameter), a no-op logger is used that
// discards all messages. Without normalizing the typed-nil case here, a
// caller-supplied nil concrete pointer would survive as a non-nil
// types.Logger interface value and panic the first time a background
// watch/fetch/parse goroutine invokes it.
//
// Parameters:
//   - logger: The logger implementation
//
// Returns:
//   - WatcherOption: Configuration option
func WithLogger(logger types.Logger) WatcherOption {
	return func(c *WatcherConfig) {
		if typeutil.IsNilInterface(logger) {
			return
		}
		c.Logger = logger
	}
}
