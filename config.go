package helix

import (
	"time"

	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/types"
)

// TimestampProvider generates timestamps for write operations.
//
// The default provider uses time.Now().UnixMicro().
type TimestampProvider func() int64

// DefaultTimestampProvider returns the current time in microseconds.
func DefaultTimestampProvider() int64 {
	return time.Now().UnixMicro()
}

// ReplayDroppedHandler is called when a replay payload cannot be enqueued.
// This callback allows applications to handle potential data loss scenarios.
//
// Parameters:
//   - payload: The payload that could not be enqueued
//   - err: The error from the enqueue attempt
type ReplayDroppedHandler func(payload types.ReplayPayload, err error)

// ClientConfig holds configuration for Helix clients.
type ClientConfig struct {
	ReadStrategy      ReadStrategy
	WriteStrategy     WriteStrategy
	FailoverPolicy    FailoverPolicy
	Replayer          Replayer
	ReplayWorker      ReplayWorker
	TimestampProvider TimestampProvider
	TopologyWatcher   TopologyWatcher
	Metrics           MetricsCollector
	Logger            types.Logger
	ClusterNames      types.ClusterNames
	OnReplayDropped   ReplayDroppedHandler
}

// DefaultConfig returns a ClientConfig with sensible defaults.
//
// Default strategies:
//   - ReadStrategy: nil (falls back to ClusterA, use policy.NewStickyRead() for production)
//   - WriteStrategy: nil (concurrent dual-write, use policy.NewAdaptiveDualWrite() for production)
//   - FailoverPolicy: nil (no circuit breaker, use policy.NewActiveFailover() for immediate failover)
//
// Returns:
//   - *ClientConfig: Configuration with default settings
func DefaultConfig() *ClientConfig {
	return &ClientConfig{
		TimestampProvider: DefaultTimestampProvider,
		Metrics:           metrics.NewNopMetrics(),
		Logger:            logging.NewNopLogger(),
		ClusterNames:      types.DefaultClusterNames(),
	}
}

// Option configures a ClientConfig.
type Option func(*ClientConfig)

// WithReadStrategy sets the read routing strategy.
//
// Parameters:
//   - strategy: The read strategy to use (e.g., StickyRead)
//
// Returns:
//   - Option: Configuration option
func WithReadStrategy(strategy ReadStrategy) Option {
	return func(c *ClientConfig) {
		c.ReadStrategy = strategy
	}
}

// WithWriteStrategy sets the write execution strategy.
//
// Parameters:
//   - strategy: The write strategy to use (e.g., ConcurrentDualWrite)
//
// Returns:
//   - Option: Configuration option
func WithWriteStrategy(strategy WriteStrategy) Option {
	return func(c *ClientConfig) {
		c.WriteStrategy = strategy
	}
}

// WithFailoverPolicy sets the failover policy for reads.
//
// Parameters:
//   - policy: The failover policy to use
//
// Returns:
//   - Option: Configuration option
func WithFailoverPolicy(policy FailoverPolicy) Option {
	return func(c *ClientConfig) {
		c.FailoverPolicy = policy
	}
}

// WithReplayer sets the replayer for failed writes.
//
// Parameters:
//   - replayer: The replayer implementation
//
// Returns:
//   - Option: Configuration option
func WithReplayer(replayer Replayer) Option {
	return func(c *ClientConfig) {
		c.Replayer = replayer
	}
}

// WithReplayWorker sets the replay worker for processing failed writes.
//
// The worker will be started automatically when the client is created
// and stopped when the client is closed.
//
// Parameters:
//   - worker: The replay worker implementation (e.g., MemoryWorker, NATSWorker)
//
// Returns:
//   - Option: Configuration option
func WithReplayWorker(worker ReplayWorker) Option {
	return func(c *ClientConfig) {
		c.ReplayWorker = worker
	}
}

// WithOnReplayDropped sets a callback for when replay payloads cannot be enqueued.
//
// This callback is invoked when a write succeeds on one cluster but fails on the other,
// and the failed write cannot be enqueued for replay (e.g., queue is full).
// Use this to implement custom alerting or fallback persistence strategies.
//
// Parameters:
//   - handler: Function called with the dropped payload and the enqueue error
//
// Returns:
//   - Option: Configuration option
//
// Example:
//
//	helix.WithOnReplayDropped(func(payload types.ReplayPayload, err error) {
//	    log.Error("replay queue full, potential data loss",
//	        "cluster", payload.TargetCluster,
//	        "query", payload.Query,
//	        "error", err,
//	    )
//	    // Optionally persist to a fallback store
//	    fallbackStore.Save(payload)
//	})
func WithOnReplayDropped(handler ReplayDroppedHandler) Option {
	return func(c *ClientConfig) {
		c.OnReplayDropped = handler
	}
}

// WithTimestampProvider sets the timestamp generator.
//
// Parameters:
//   - fn: Function that returns current timestamp in microseconds
//
// Returns:
//   - Option: Configuration option
func WithTimestampProvider(fn TimestampProvider) Option {
	return func(c *ClientConfig) {
		c.TimestampProvider = fn
	}
}

// WithTopologyWatcher sets the topology watcher for drain mode support.
//
// Parameters:
//   - watcher: The topology watcher implementation
//
// Returns:
//   - Option: Configuration option
func WithTopologyWatcher(watcher TopologyWatcher) Option {
	return func(c *ClientConfig) {
		c.TopologyWatcher = watcher
	}
}

// WithMetrics sets the metrics collector.
//
// If not set, a no-op collector is used that discards all metrics.
// Use contrib/metrics/vm.New() for VictoriaMetrics integration.
//
// Parameters:
//   - collector: The metrics collector implementation
//
// Returns:
//   - Option: Configuration option
//
// Example:
//
//	import vmmetrics "github.com/arloliu/helix/contrib/metrics/vm"
//
//	collector := vmmetrics.New(vmmetrics.WithPrefix("myapp"))
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithMetrics(collector),
//	)
func WithMetrics(collector MetricsCollector) Option {
	return func(c *ClientConfig) {
		c.Metrics = collector
	}
}

// WithLogger sets the structured logger.
//
// If not set, a no-op logger is used that discards all messages.
// The logger interface is compatible with zap.SugaredLogger.
//
// Parameters:
//   - logger: The logger implementation
//
// Returns:
//   - Option: Configuration option
//
// Example:
//
//	logger, _ := zap.NewProduction()
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithLogger(logger.Sugar()),
//	)
func WithLogger(logger types.Logger) Option {
	return func(c *ClientConfig) {
		c.Logger = logger
	}
}

// WithClusterNames sets custom display names for the clusters.
//
// These names are used in metrics labels and log messages instead of the
// default "A" and "B". Names must be Prometheus-compatible (alphanumeric
// with underscores, starting with letter or underscore, max 32 chars).
//
// If not set, defaults to "A" and "B".
//
// Parameters:
//   - nameA: Display name for cluster A (e.g., "us_east", "primary", "dc1")
//   - nameB: Display name for cluster B (e.g., "us_west", "secondary", "dc2")
//
// Returns:
//   - Option: Configuration option
//
// Example:
//
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithClusterNames("us_east", "us_west"),
//	)
//
// This will produce metrics like:
//
//	helix_read_total{cluster="us_east"} 100
//	helix_read_total{cluster="us_west"} 95
func WithClusterNames(nameA, nameB string) Option {
	return func(c *ClientConfig) {
		c.ClusterNames = types.ClusterNames{A: nameA, B: nameB}
	}
}

// propagateClusterNames sets cluster names on components that implement ClusterNamer.
//
// This function is called during client initialization to propagate cluster names
// configured via WithClusterNames to all components that support custom naming.
func propagateClusterNames(c *ClientConfig) {
	names := c.ClusterNames

	// Propagate to metrics collector
	if namer, ok := c.Metrics.(types.ClusterNamer); ok {
		namer.SetClusterNames(names)
	}

	// Propagate to failover policy
	if namer, ok := c.FailoverPolicy.(types.ClusterNamer); ok {
		namer.SetClusterNames(names)
	}

	// Propagate to replay worker
	if namer, ok := c.ReplayWorker.(types.ClusterNamer); ok {
		namer.SetClusterNames(names)
	}

	// Propagate to write strategy
	if namer, ok := c.WriteStrategy.(types.ClusterNamer); ok {
		namer.SetClusterNames(names)
	}

	// Propagate to read strategy
	if namer, ok := c.ReadStrategy.(types.ClusterNamer); ok {
		namer.SetClusterNames(names)
	}
}
