package helix

import (
	"context"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/replay"
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

	// AllowedClusters, when set, overrides automatic read routing with an
	// operator-controlled cluster list. This is the external control for
	// preventing reads from a recovering cluster whose replay backfill has
	// not yet completed.
	//
	// When the function returns a non-empty list, the read strategy's Select()
	// is NOT called — the list directly controls routing. The strategy's
	// internal state (OnSuccess/OnFailure) is frozen until the override is
	// removed.
	//
	// Iterator paths defer override errors to Close(). Always call Close()
	// and check its error.
	//
	// CAS operations (ScanCAS, MapScanCAS, batch ExecCAS) are not affected
	// by this override — they are write-like operations controlled by
	// ForceDegrade/ForceRecover.
	//
	// See [AllowedClustersFunc] for return-value semantics.
	AllowedClusters AllowedClustersFunc

	// DefaultFallbackRead enables FallbackRead for every Scan and MapScan query
	// on this client when true. Equivalent to calling [Query.FallbackRead] on
	// every query.
	//
	// FallbackRead is best-effort: if the alternative cluster is unreachable,
	// callers receive [ErrNotFound] (not the network error). See
	// [Query.FallbackRead] for full semantics.
	//
	// Default: false (opt-in per query or per context).
	DefaultFallbackRead bool

	// AutoMemoryWorker enables automatic in-process replay with MemoryReplayer.
	// When true, a MemoryReplayer and Worker are created automatically.
	AutoMemoryWorker bool

	// AutoMemoryCapacity is the queue capacity for auto-created MemoryReplayer.
	// Default: 10000
	AutoMemoryCapacity int

	// AutoMemoryWorkerOpts are options passed to the auto-created Worker.
	AutoMemoryWorkerOpts []replay.WorkerOption

	// NowProvider returns the current time as Unix nanoseconds. Defaults to
	// DefaultNowProvider (which calls time.Now().UnixNano()). Tests can
	// substitute a deterministic clock so the auto-refresh detector's
	// time-based conditions can be exercised without wall-clock dependence.
	//
	// Mirrors the [TimestampProvider] pattern above.
	NowProvider NowProvider

	// SessionRefresher is an optional caller-supplied factory used by
	// [CQLClient.RefreshSession] to build a replacement [cql.Session] for a
	// cluster whose live session is broken (e.g., the cluster restarted at a
	// different endpoint and the existing session cannot reconnect).
	//
	// Helix is decoupled from any specific gocql driver version, so it cannot
	// build a session itself — only the caller knows whether to wrap a
	// gocql v1, gocql v2, chaos-injecting, or test-mock implementation. The
	// refresher receives the target ClusterID and the most recently observed
	// error (currently always nil in v1; reserved for future per-cluster
	// last-error tracking) and returns a fresh session.
	//
	// If unset, [CQLClient.RefreshSession] returns [types.ErrNoSessionRefresher].
	// The lower-level [CQLClient.SwapSession] does not require a refresher
	// because the caller passes the new session directly.
	SessionRefresher SessionRefresher
}

// SessionRefresher builds a fresh [cql.Session] for the given cluster.
//
// Implementations are caller-provided and are responsible for choosing the
// concrete adapter (cqlv1.NewSession, cqlv2.NewSession, etc.) — Helix never
// imports a specific gocql driver and so cannot construct a session itself.
//
// lastErr is currently always nil; reserved for v2 per-cluster last-error
// tracking that will let refreshers tailor reconnection strategy to the
// observed failure mode.
type SessionRefresher func(ctx context.Context, cluster ClusterID, lastErr error) (cql.Session, error)

// DefaultConfig returns a ClientConfig with sensible defaults.
//
// The default configuration provides minimal, non-nil infrastructure:
//   - TimestampProvider: Uses time.Now().UnixMicro() for idempotent writes
//   - Metrics: No-op collector (silent, no overhead)
//   - Logger: No-op logger (silent, no overhead)
//   - ClusterNames: "ClusterA" and "ClusterB"
//
// Strategy and policy defaults (all nil):
//   - ReadStrategy: nil - Falls back to ClusterA only (no load balancing)
//   - WriteStrategy: nil - Concurrent dual-write to both clusters
//   - FailoverPolicy: nil - Always attempts failover on read failure
//   - Replayer: nil - CAUTION: Partial write failures will be lost!
//
// Production recommendations:
//   - ReadStrategy: policy.NewStickyRead() for cache-efficient reads
//   - WriteStrategy: policy.NewAdaptiveDualWrite() for latency-aware writes
//   - FailoverPolicy: policy.NewActiveFailover() for immediate failover
//   - Replayer: replay.NewNATSReplayer() for durable failure recovery
//
// A warning is logged during client creation if dual-cluster mode is used
// without a Replayer configured.
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

// WithSessionRefresher registers a factory used by [CQLClient.RefreshSession]
// to build a replacement [cql.Session] for a cluster whose live session has
// become permanently unrecoverable. Without this option, RefreshSession
// returns [types.ErrNoSessionRefresher]; the lower-level [CQLClient.SwapSession]
// remains available without it.
//
// The factory is invoked synchronously by RefreshSession on the calling
// goroutine; long-running connection establishment should respect the passed
// context.
//
// Parameters:
//   - fn: The session factory; receives the target cluster and (in v1) a nil
//     lastErr. Reserved for future per-cluster last-error threading.
//
// Returns:
//   - Option: Configuration option
func WithSessionRefresher(fn SessionRefresher) Option {
	return func(c *ClientConfig) {
		c.SessionRefresher = fn
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

// WithAutoMemoryWorker enables automatic in-process replay with MemoryReplayer.
//
// This is a convenience option that creates both a MemoryReplayer and a Worker
// automatically, eliminating boilerplate setup code. The worker uses the client's
// DefaultExecuteFunc() to route replays to the correct cluster.
//
// Use this for:
//   - Development and testing environments
//   - Simple deployments where in-process replay is acceptable
//
// For production with durable replay, use WithReplayer() with NATSReplayer instead.
// NATS workers typically run as separate consumer services.
//
// Parameters:
//   - queueCapacity: Maximum pending replays (0 uses default of 10000)
//   - workerOpts: Optional worker configuration (poll interval, callbacks, etc.)
//
// Returns:
//   - Option: Configuration option
//
// Example:
//
//	// Simple setup with defaults
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithAutoMemoryWorker(0),
//	)
//
//	// Custom capacity and callbacks
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithAutoMemoryWorker(50000,
//	        replay.WithPollInterval(50*time.Millisecond),
//	        replay.WithOnSuccess(func(p types.ReplayPayload) {
//	            log.Printf("Replay succeeded: cluster=%s", p.TargetCluster)
//	        }),
//	    ),
//	)
func WithAutoMemoryWorker(queueCapacity int, workerOpts ...replay.WorkerOption) Option {
	return func(c *ClientConfig) {
		c.AutoMemoryWorker = true
		if queueCapacity > 0 {
			c.AutoMemoryCapacity = queueCapacity
		} else {
			c.AutoMemoryCapacity = 10000 // default
		}
		c.AutoMemoryWorkerOpts = workerOpts
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

// WithAllowedClusters sets an operator-driven function that controls which
// clusters are eligible for reads.
//
// When the function returns a non-empty list, the read strategy is bypassed
// and the list directly controls read routing. The first element is the
// primary read target; subsequent elements are failover candidates in order.
// The read strategy's internal state (OnSuccess/OnFailure) is frozen during
// the override so it resumes cleanly when the override is removed.
//
// When the function returns nil or an empty slice, normal strategy + drain
// behavior applies.
//
// Fail-closed: if the list contains only unknown cluster IDs, or all valid
// clusters are draining, the read returns an error (ErrInvalidClusterOverride
// or ErrNoValidClusters). A panicking function returns ErrClusterOverridePanic.
// This prevents silent misrouting from stale or misconfigured flags.
//
// CAS operations (ScanCAS, MapScanCAS, batch ExecCAS/MapExecCAS) are NOT
// affected by this override — they are single-cluster, write-like operations
// controlled by ForceDegrade/ForceRecover on the write side.
//
// Iterator paths (IterContext) defer override errors to Close(). Always call
// Close() and check its error.
//
// Parameters:
//   - fn: Function returning the ordered list of allowed clusters for reads
//
// Returns:
//   - Option: Configuration option
//
// Example:
//
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithReadStrategy(policy.NewStickyRead(...)),
//	    helix.WithAllowedClusters(func() []helix.ClusterID {
//	        if featureFlag.IsClusterExcluded("A") {
//	            return []helix.ClusterID{helix.ClusterB}
//	        }
//	        return nil // all allowed, normal strategy behavior
//	    }),
//	)
func WithAllowedClusters(fn AllowedClustersFunc) Option {
	return func(c *ClientConfig) {
		c.AllowedClusters = fn
	}
}

// WithDefaultFallbackRead enables FallbackRead for all Scan and MapScan queries
// on this client when enabled is true.
//
// When true, every query executed on this client behaves as if [Query.FallbackRead]
// was called on it: a not-found result on the selected cluster triggers a silent
// best-effort attempt on the other cluster.
//
// Important: FallbackRead is best-effort. If the alternative cluster is
// unreachable, the caller receives [ErrNotFound] (the healthy cluster's answer),
// not the network error. Health metrics and failure tracking are still recorded
// on the unreachable cluster internally. See [Query.FallbackRead] for details.
//
// Use this when the majority of reads on this client are critical read-after-write
// operations. For mixed workloads (e.g., critical user data + bulk time-series),
// use per-query [Query.FallbackRead] or context-level [WithFallbackRead] instead,
// or create separate clients with different configurations.
//
// Default: false (opt-in per query or per context).
//
// Parameters:
//   - enabled: Whether to enable FallbackRead for all queries on this client
//
// Returns:
//   - Option: Configuration option
func WithDefaultFallbackRead(enabled bool) Option {
	return func(c *ClientConfig) {
		c.DefaultFallbackRead = enabled
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
