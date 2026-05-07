package helix

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
)

// CQLClient is the main Helix CQL client for single or dual-cluster operations.
//
// It wraps one or two CQL sessions and orchestrates reads and writes according
// to configured strategies. When only one session is provided (sessionB is nil),
// the client operates in single-cluster mode with pass-through behavior.
//
// Single-cluster mode is ideal for:
//   - Migrating existing applications to Helix incrementally
//   - Development and testing environments
//   - Applications that don't need dual-cluster redundancy yet
//
// # Thread Safety
//
// CQLClient is safe for concurrent use from multiple goroutines. A single client
// instance can be shared across your application:
//
//	// Create once, share everywhere
//	client, err := helix.NewCQLClient(sessionA, sessionB, ...)
//	defer client.Close()
//
//	// Use from multiple goroutines safely
//	go func() { client.Query("INSERT ...").Exec() }()
//	go func() { client.Query("SELECT ...").Scan(&result) }()
//
// All internal state is protected by atomic operations or appropriate locking.
//
// # Lifecycle
//
// Create a client with NewCQLClient() and clean up resources with Close():
//
//	client, err := helix.NewCQLClient(sessionA, sessionB, opts...)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()  // Always close to release resources
//
// After Close() is called:
//   - All ongoing operations complete or are cancelled
//   - Replay worker is stopped (enqueued replays are lost if using MemoryReplayer)
//   - Topology watcher is stopped
//   - Underlying sessions are closed
//   - The client cannot be reused (operations return ErrSessionClosed)
type CQLClient struct {
	// sessionA / sessionB hold the live cql.Session references behind an
	// atomic.Pointer so they can be replaced at runtime via SwapSession or
	// RefreshSession without locking the read path. The wrapper struct is
	// required because cql.Session is an interface — atomic.Pointer needs a
	// concrete type, and atomic.Value would panic when successive Stores
	// have different dynamic types (which we explicitly support, e.g.
	// swapping a chaos-wrapped session for a plain one).
	//
	// Both pointers always reference a non-nil holder after NewCQLClient.
	// In single-cluster mode, sessionB's holder wraps a nil cql.Session;
	// callers should consult IsSingleCluster (or the singleCluster bool) to
	// distinguish modes rather than nil-checking the loaded session.
	sessionA atomic.Pointer[sessionHolder]
	sessionB atomic.Pointer[sessionHolder]

	// singleCluster captures the construction-time mode and is immutable
	// for the lifetime of the client. SwapSession / RefreshSession cannot
	// promote a single-cluster client to dual-cluster.
	singleCluster bool

	config *ClientConfig
	closed atomic.Bool

	// Drain mode state
	drainA        atomic.Bool
	drainB        atomic.Bool
	topologyCtx   context.Context
	topologyClose context.CancelFunc

	// overrideErrSeq counts consecutive override errors for power-of-2 log backoff.
	// Prevents log storms when the AllowedClusters provider is misconfigured.
	overrideErrSeq atomic.Uint64

	// statsA / statsB track per-cluster op outcomes for the auto-refresh
	// detector (see WithAutoRefresh). Updated by recordOpOutcome on every
	// hot-path op. When auto-refresh is disabled the fields are still
	// updated (the cost is negligible) so toggling the option after
	// construction would just-work in v2.x if/when we add it.
	statsA clusterStats
	statsB clusterStats

	// nowFunc returns the current time as Unix nanoseconds. Defaults to
	// DefaultNowProvider; tests substitute a deterministic clock so
	// millisecond-scale time-based assertions don't depend on wall clock.
	// Mirrors the TimestampProvider pattern in config.go.
	nowFunc NowProvider

	// autoRefreshCtx / autoRefreshClose control the auto-refresh
	// background goroutine's lifetime. Nil if WithAutoRefresh was not
	// configured (or if no SessionRefresher was registered, since the
	// detector cannot do anything useful without a refresher).
	autoRefreshCtx   context.Context
	autoRefreshClose context.CancelFunc
}

// sessionHolder wraps a cql.Session so it can live in an atomic.Pointer.
// The indirection is required because cql.Session is an interface.
type sessionHolder struct {
	s cql.Session
}

// clusterStats holds per-cluster op-outcome counters. All fields are
// atomics — no lock needed on the read or write side.
//
// The auto-refresh detector (see [WithAutoRefresh]) reads these fields
// to decide when a cluster's session is permanently dead and needs a
// RefreshSession call. lastRefreshNanos is the throttle: stamped before
// each refresh attempt so a hung refresher cannot cause re-entrant
// double-fire on the next detector tick. lastErr captures the most
// recently observed failure error and is threaded through to the
// SessionRefresher's lastErr parameter so refreshers can tailor
// reconnection strategy to the observed failure mode.
type clusterStats struct {
	consecutiveFailures atomic.Int32
	lastSuccessNanos    atomic.Int64
	lastFailureNanos    atomic.Int64
	lastRefreshNanos    atomic.Int64
	lastErr             atomic.Pointer[error]
}

// NowProvider returns the current time as Unix nanoseconds.
//
// The default is [DefaultNowProvider] (which calls time.Now().UnixNano()).
// Tests use a deterministic provider so the auto-refresh detector's
// time-based conditions can be exercised without wall-clock dependence.
//
// Mirrors the [TimestampProvider] pattern in config.go.
type NowProvider func() int64

// DefaultNowProvider returns time.Now().UnixNano().
func DefaultNowProvider() int64 {
	return time.Now().UnixNano()
}

// loadSessionA returns the live session for cluster A. It is wait-free; the
// returned session reference is stable for the duration of the calling
// goroutine's use, but a concurrent SwapSession may install a new session
// for the next caller.
func (c *CQLClient) loadSessionA() cql.Session {
	return c.sessionA.Load().s
}

// loadSessionB returns the live session for cluster B, or nil if the client
// was constructed in single-cluster mode.
func (c *CQLClient) loadSessionB() cql.Session {
	h := c.sessionB.Load()
	if h == nil {
		return nil
	}

	return h.s
}

// storeSessionA installs s as the cluster A session. Used by NewCQLClient
// during construction and by SwapSession at runtime.
func (c *CQLClient) storeSessionA(s cql.Session) {
	c.sessionA.Store(&sessionHolder{s: s})
}

// storeSessionB installs s as the cluster B session. In single-cluster mode
// it stores a holder wrapping nil so loadSessionB() returns nil safely
// without a nil-pointer-deref on the holder pointer.
func (c *CQLClient) storeSessionB(s cql.Session) {
	c.sessionB.Store(&sessionHolder{s: s})
}

// statsForCluster returns the stats container for the given cluster.
// In single-cluster mode, only statsA is meaningful; statsB exists but
// is never read by the auto-refresh detector.
func (c *CQLClient) statsForCluster(cluster ClusterID) *clusterStats {
	if cluster == ClusterB {
		return &c.statsB
	}

	return &c.statsA
}

// autoRefreshLoop runs in a background goroutine when WithAutoRefresh is
// enabled and a SessionRefresher is registered. It ticks every
// AutoRefresh.CheckInterval and asks maybeAutoRefresh to evaluate each
// cluster.
//
// Started by NewCQLClient, stopped by Close.
func (c *CQLClient) autoRefreshLoop() {
	t := time.NewTicker(c.config.AutoRefresh.CheckInterval)
	defer t.Stop()

	for {
		select {
		case <-c.autoRefreshCtx.Done():
			return
		case <-t.C:
			c.maybeAutoRefresh(ClusterA)
			if !c.singleCluster {
				c.maybeAutoRefresh(ClusterB)
			}
		}
	}
}

// maybeAutoRefresh evaluates the trigger condition for the given cluster
// and invokes RefreshSession if all three predicates hold:
//
//  1. consecutiveFailures >= AutoRefresh.FailureThreshold
//  2. now - lastSuccess  >= AutoRefresh.SustainedFailureWindow
//  3. now - lastRefresh  >= AutoRefresh.MinRetryInterval (throttle)
//
// Each predicate is evaluated against atomic loads — no lock. The
// throttle stamp (lastRefreshNanos) is set BEFORE invoking the refresher
// so a hung refresher cannot cause re-entrant double-fire on the next
// tick. On successful refresh, consecutiveFailures is reset (so a
// fresh-session-also-dead scenario must re-accumulate threshold-many
// failures before re-triggering); lastSuccessNanos is left stale until
// the next genuinely successful op proves the new session is healthy.
//
// Exported for tests via direct invocation; production callers should
// not call this — the goroutine drives it.
func (c *CQLClient) maybeAutoRefresh(cluster ClusterID) {
	if c.closed.Load() {
		return
	}
	if c.config.SessionRefresher == nil {
		return // can't refresh; the loop is moot for this cluster
	}

	s := c.statsForCluster(cluster)
	now := c.nowFunc()
	cfg := c.config.AutoRefresh

	// Compare via int64 to dodge gosec's int → int32 conversion warning;
	// FailureThreshold can in principle exceed int32 max but in practice
	// is always single/double digits.
	if int64(s.consecutiveFailures.Load()) < int64(cfg.FailureThreshold) {
		return
	}
	if now-s.lastSuccessNanos.Load() < int64(cfg.SustainedFailureWindow) {
		return
	}
	if now-s.lastRefreshNanos.Load() < int64(cfg.MinRetryInterval) {
		return
	}

	// Throttle stamp first — any future tick within MinRetryInterval will
	// see this and bail at the predicate above, even if RefreshSession
	// hangs or panics.
	s.lastRefreshNanos.Store(now)

	if m, ok := c.config.Metrics.(types.SessionRefreshMetrics); ok {
		m.IncSessionRefreshAttempt(cluster)
	}
	c.config.Logger.Info("auto-refresh: session believed dead, invoking refresher",
		"cluster", c.clusterName(cluster),
		"consecutiveFailures", s.consecutiveFailures.Load(),
		"secondsSinceLastSuccess", (now-s.lastSuccessNanos.Load())/int64(time.Second),
	)

	ctx, cancel := context.WithTimeout(c.autoRefreshCtx, cfg.RefreshTimeout)
	defer cancel()

	if err := c.RefreshSession(ctx, cluster); err != nil {
		if m, ok := c.config.Metrics.(types.SessionRefreshMetrics); ok {
			m.IncSessionRefreshError(cluster)
		}
		c.config.Logger.Warn("auto-refresh: session refresh failed",
			"cluster", c.clusterName(cluster),
			"error", err.Error(),
		)

		return
	}

	if m, ok := c.config.Metrics.(types.SessionRefreshMetrics); ok {
		m.IncSessionRefreshSuccess(cluster)
	}
	c.config.Logger.Info("auto-refresh: session refreshed",
		"cluster", c.clusterName(cluster),
	)

	// Reset failure counters: the new session deserves a fresh start.
	// Leave lastSuccessNanos stale — we want the detector to require a
	// genuinely successful op against the new session before considering
	// it healthy. Without this, an immediate re-tick would see threshold
	// failures already met against a now-replaced session and could
	// (modulo MinRetryInterval) re-fire spuriously.
	s.consecutiveFailures.Store(0)
	s.lastFailureNanos.Store(0)
}

// recordOpOutcome updates the per-cluster stats based on a single op's
// result for that cluster. Designed to be invoked PER cluster — never
// gated on "all clusters succeeded" — so a dual-write where A succeeds
// and B fails records success on A AND failure on B independently.
//
// err == nil counts as success. ErrWriteAsync, ErrWriteDropped, and
// ErrNotFound are operational/data states (not health signals) and MUST
// NOT count as failure. All other errors increment consecutiveFailures.
//
// The ErrNotFound exclusion depends on Helix's adapter normalization
// contract: gocql v1's gocql.ErrNotFound and gocql v2's equivalent are
// both translated to types.ErrNotFound at the adapter boundary
// (verified in adapter/cql/v{1,2}/adapter.go:14). If a future adapter
// surfaces a driver-native not-found instead of the Helix sentinel,
// recordOpOutcome will count those as failures and a workload with
// many genuine not-founds could trigger spurious auto-refreshes.
// Revisit this helper if the adapter contract changes.
func (c *CQLClient) recordOpOutcome(cluster ClusterID, err error) {
	s := c.statsForCluster(cluster)
	now := c.nowFunc()

	switch {
	case err == nil:
		s.consecutiveFailures.Store(0)
		s.lastSuccessNanos.Store(now)
		s.lastErr.Store(nil)
	case errors.Is(err, types.ErrWriteAsync),
		errors.Is(err, types.ErrWriteDropped),
		errors.Is(err, types.ErrNotFound):
		// Operational/data state; not a health signal.
		return
	default:
		s.consecutiveFailures.Add(1)
		s.lastFailureNanos.Store(now)
		// Stable heap pointer for atomic.Pointer; the err interface
		// itself is two words and can't be stored directly.
		errCopy := err
		s.lastErr.Store(&errCopy)
	}
}

// Compile-time assertion that CQLClient implements CQLSession.
var _ CQLSession = (*CQLClient)(nil)

// shouldLogOverrideErr returns true on the first occurrence and on every
// power-of-2 occurrence thereafter (1, 2, 4, 8, 16 …). This prevents a
// misconfigured AllowedClusters provider from flooding the log at high QPS
// while still ensuring the error is visible immediately and periodically.
func (c *CQLClient) shouldLogOverrideErr() bool {
	seq := c.overrideErrSeq.Add(1)
	return seq == 1 || seq&(seq-1) == 0
}

// NewCQLClient creates a new Helix CQL client.
//
// The client supports two modes:
//   - Single-cluster mode: Pass sessionB as nil. Operations are executed directly
//     on sessionA without dual-write or failover logic. This provides a drop-in
//     replacement for existing single-cluster applications.
//   - Dual-cluster mode: Pass both sessions. Operations use configured strategies
//     for dual writes, sticky reads, and failover.
//
// If a ReplayWorker is configured, it will be started automatically.
// The worker will be stopped when Close() is called.
//
// If a TopologyWatcher is configured, it will be started automatically to
// monitor drain mode signals. Writes to draining clusters are skipped and
// enqueued for replay. Reads are failed over away from draining clusters.
//
// Parameters:
//   - sessionA: CQL session for cluster A (required)
//   - sessionB: CQL session for cluster B (optional, nil for single-cluster mode)
//   - opts: Optional configuration options
//
// Returns:
//   - *CQLClient: A new CQL client
//   - error: ErrNilSession if sessionA is nil, or error from worker start
func NewCQLClient(sessionA, sessionB cql.Session, opts ...Option) (*CQLClient, error) {
	if sessionA == nil {
		return nil, types.ErrNilSession
	}

	config := DefaultConfig()
	for _, opt := range opts {
		opt(config)
	}

	// Ensure metrics is never nil
	if config.Metrics == nil {
		config.Metrics = metrics.NewNopMetrics()
	}

	// Ensure logger is never nil
	if config.Logger == nil {
		config.Logger = logging.NewNopLogger()
	}

	// Ensure timestamp provider is never nil
	if config.TimestampProvider == nil {
		config.TimestampProvider = DefaultTimestampProvider
	}

	// Propagate cluster names to components that support it
	propagateClusterNames(config)

	// Warn about missing Replayer in dual-cluster mode
	if sessionB != nil && config.Replayer == nil && !config.AutoMemoryWorker {
		config.Logger.Warn("dual-cluster mode with no Replayer configured - partial write failures will be lost and cannot be reconciled")
	}

	// Ensure NowProvider is never nil — auto-refresh + stats helpers
	// dereference it on every recorded op outcome.
	if config.NowProvider == nil {
		config.NowProvider = DefaultNowProvider
	}

	client := &CQLClient{
		config:        config,
		singleCluster: sessionB == nil,
		nowFunc:       config.NowProvider,
	}
	client.storeSessionA(sessionA)
	// Store sessionB even if nil; in single-cluster mode the holder wraps a
	// nil cql.Session so loadSessionB() returns nil safely without a
	// nil-pointer-deref-risk on the holder pointer itself.
	client.storeSessionB(sessionB)

	// Create auto memory worker if configured. The auto-created worker
	// inherits the client's metrics collector by default so client and
	// worker metrics are unified — this prevents the gotcha where
	// worker-side IncReplaySuccess/Dropped/Error go into a separate
	// NopMetrics and are silently invisible to the client's collector.
	// Caller-supplied AutoMemoryWorkerOpts that include WithWorkerMetrics
	// override this default (last-option-wins).
	if config.AutoMemoryWorker {
		memReplayer := replay.NewMemoryReplayer(
			replay.WithQueueCapacity(config.AutoMemoryCapacity),
		)
		config.Replayer = memReplayer
		opts := append(
			[]replay.WorkerOption{replay.WithWorkerMetrics(config.Metrics)},
			config.AutoMemoryWorkerOpts...,
		)
		config.ReplayWorker = replay.NewMemoryWorker(
			memReplayer,
			client.DefaultExecuteFunc(),
			opts...,
		)
	}

	// For caller-supplied workers (via WithReplayWorker), inject the
	// client's metrics collector if the worker doesn't already have one
	// explicitly set. Type-assertion-based so the public ReplayWorker
	// interface is unchanged; workers that opt in (replay.Worker does)
	// expose MetricsConfigured + SetMetrics; others are left alone.
	if config.ReplayWorker != nil && config.Metrics != nil {
		type metricsAware interface {
			MetricsConfigured() bool
			SetMetrics(types.MetricsCollector)
		}
		if mw, ok := config.ReplayWorker.(metricsAware); ok && !mw.MetricsConfigured() {
			mw.SetMetrics(config.Metrics)
		}
	}

	// Start topology watcher if configured. The cancel function is stashed so
	// that it is called on any subsequent initialization error, preventing the
	// watchTopology goroutine from leaking.
	if config.TopologyWatcher != nil {
		ctx, cancel := context.WithCancel(context.Background())
		client.topologyCtx = ctx
		client.topologyClose = cancel
		go client.watchTopology()
	}

	// Start replay worker if configured. On failure, clean up the topology
	// watcher goroutine that may already be running.
	if config.ReplayWorker != nil {
		if err := config.ReplayWorker.Start(); err != nil {
			if client.topologyClose != nil {
				client.topologyClose()
			}
			return nil, err
		}
	}

	// Start the auto-refresh detector if enabled AND a refresher is
	// registered. Without a refresher the detector cannot do anything
	// useful, so we skip the goroutine entirely.
	if config.AutoRefresh.Enabled && config.SessionRefresher != nil {
		ctx, cancel := context.WithCancel(context.Background())
		client.autoRefreshCtx = ctx
		client.autoRefreshClose = cancel
		go client.autoRefreshLoop()
	}

	return client, nil
}

// watchTopology monitors topology updates and updates drain state.
func (c *CQLClient) watchTopology() {
	updates := c.config.TopologyWatcher.Watch(c.topologyCtx)
	for update := range updates {
		var previousDrain bool
		switch update.Cluster {
		case ClusterA:
			previousDrain = c.drainA.Swap(update.DrainMode)
		case ClusterB:
			previousDrain = c.drainB.Swap(update.DrainMode)
		}

		// Record drain mode transitions
		if !previousDrain && update.DrainMode {
			c.config.Metrics.IncDrainModeEntered(update.Cluster)
			c.config.Metrics.SetClusterDraining(update.Cluster, true)
			c.config.Logger.Warn("cluster entering drain mode",
				"cluster", c.clusterName(update.Cluster),
			)
		} else if previousDrain && !update.DrainMode {
			c.config.Metrics.IncDrainModeExited(update.Cluster)
			c.config.Metrics.SetClusterDraining(update.Cluster, false)
			c.config.Logger.Info("cluster exiting drain mode",
				"cluster", c.clusterName(update.Cluster),
			)
		}
	}
}

// IsDraining returns whether the specified cluster is currently in drain mode.
//
// When a cluster is draining:
//   - Writes to the cluster are skipped and enqueued for replay
//   - Reads are failed over to the non-draining cluster
//
// Parameters:
//   - cluster: The cluster to check
//
// Returns:
//   - bool: true if the cluster is being drained
func (c *CQLClient) IsDraining(cluster ClusterID) bool {
	switch cluster {
	case ClusterA:
		return c.drainA.Load()
	case ClusterB:
		return c.drainB.Load()
	}

	return false
}

// Query creates a new Query for the given statement.
//
// The method called on the returned Query determines the strategy:
//   - Exec/ExecContext → Write Strategy (Dual Write)
//   - Scan/Iter/MapScan → Read Strategy (Sticky Read)
//
// Parameters:
//   - stmt: CQL statement with ? placeholders
//   - values: Values to bind to placeholders
//
// Returns:
//   - Query: A query builder for further configuration
func (c *CQLClient) Query(stmt string, values ...any) Query {
	return &cqlQuery{
		client:    c,
		statement: stmt,
		values:    values,
	}
}

// Batch creates a new Batch for grouping multiple mutations.
//
// WARNING: CounterBatch operations are NOT idempotent. If a counter update
// partially fails (succeeds on one cluster, fails on another), or if
// AdaptiveDualWrite returns ErrWriteAsync and the client eagerly enqueues a replay
// safety net while the background write also succeeds, the Replay System can
// apply the same increment twice. Avoid using CounterBatch with dual-cluster mode
// if you require exactly-once semantics.
//
// Parameters:
//   - kind: Type of batch (Logged, Unlogged, or Counter)
//
// Returns:
//   - Batch: A batch builder for adding statements
func (c *CQLClient) Batch(kind BatchType) Batch {
	return &cqlBatch{
		client:  c,
		kind:    kind,
		entries: make([]batchEntry, 0),
	}
}

// NewBatch creates a new Batch for grouping multiple mutations.
//
// Deprecated: Use Batch() instead for the modern fluent API.
// This method is provided for compatibility with gocql v1 users.
//
// Parameters:
//   - kind: Type of batch (Logged, Unlogged, or Counter)
//
// Returns:
//   - Batch: A batch builder for adding statements
func (c *CQLClient) NewBatch(kind BatchType) Batch {
	return c.Batch(kind)
}

// ExecuteBatch executes a batch operation.
//
// Deprecated: Use batch.Exec() instead for the modern fluent API.
// This method is provided for compatibility with gocql v1 users.
//
// Parameters:
//   - batch: The batch to execute
//
// Returns:
//   - error: Any execution error
func (c *CQLClient) ExecuteBatch(batch Batch) error {
	return batch.Exec()
}

// ExecuteBatchCAS executes a batch with lightweight transaction semantics.
//
// Deprecated: Use batch.ExecCAS() instead for the modern fluent API.
// This method is provided for compatibility with gocql v1 users.
//
// Parameters:
//   - batch: The batch to execute
//   - dest: Optional destination for result columns
//
// Returns:
//   - applied: true if the transaction was applied
//   - iter: Iterator for result rows if not applied
//   - err: Any execution error
func (c *CQLClient) ExecuteBatchCAS(batch Batch, dest ...any) (applied bool, iter Iter, err error) {
	return batch.ExecCAS(dest...)
}

// MapExecuteBatchCAS executes a batch CAS operation and maps results to a map.
//
// Deprecated: Use batch.MapExecCAS() instead for the modern fluent API.
// This method is provided for compatibility with gocql v1 users.
//
// Parameters:
//   - batch: The batch to execute
//   - dest: Map to receive result columns
//
// Returns:
//   - applied: true if the transaction was applied
//   - iter: Iterator for result rows if not applied
//   - err: Any execution error
func (c *CQLClient) MapExecuteBatchCAS(batch Batch, dest map[string]any) (applied bool, iter Iter, err error) {
	return batch.MapExecCAS(dest)
}

// Close terminates connections to cluster(s) and stops the replay worker.
//
// The topology watcher and replay worker are stopped first. After Close is
// called, the client cannot be reused; further public operations including
// SwapSession and RefreshSession return [types.ErrSessionClosed].
//
// Calling Close concurrently with SwapSession or RefreshSession is undefined:
// Close may end up closing either the old or the new session depending on
// scheduling, and the caller of SwapSession still owns the session it
// received. Synchronize externally if you need a deterministic order.
func (c *CQLClient) Close() {
	if c.closed.CompareAndSwap(false, true) {
		// Stop topology watcher
		if c.topologyClose != nil {
			c.topologyClose()
		}

		// Stop the auto-refresh detector goroutine. Cancellation is
		// observed at the goroutine's select; any RefreshSession call
		// that's currently in flight will additionally see closed=true
		// at its entry and return ErrSessionClosed.
		if c.autoRefreshClose != nil {
			c.autoRefreshClose()
		}

		// Stop replay worker
		if c.config.ReplayWorker != nil {
			c.config.ReplayWorker.Stop()
		}

		c.loadSessionA().Close()
		if !c.singleCluster {
			c.loadSessionB().Close()
		}
	}
}

// SwapSession atomically replaces the live session for the given cluster
// with newSession and returns the previous session. The caller is
// responsible for closing the returned session once any in-flight work
// using it has drained.
//
// SwapSession is the lowest-level escape hatch for callers who have already
// built a fresh [cql.Session] (typically because they detected the live one
// is unrecoverable — e.g., the cluster restarted at a different endpoint).
// For most callers, [CQLClient.RefreshSession] combined with
// [WithSessionRefresher] is more ergonomic.
//
// Behavior:
//   - The swap is lock-free on the read path; concurrent Query/Batch/Iter
//     callers see either the old or the new session, never a partial state.
//   - Operations that have already resolved their session (in-flight Iter
//     or CAS, mid-execution synchronous calls, fire-and-forget writes
//     captured into a goroutine) continue against the session they
//     captured. Only operations that resolve the session after the swap
//     observe the new one.
//   - This method does NOT change cluster cardinality. Calling it with
//     ClusterB on a single-cluster client returns [types.ErrInvalidCluster].
//   - The returned old session is NOT closed by this method. The caller
//     decides when in-flights are quiet and calls [cql.Session.Close]
//     accordingly. Calling Close on the returned session before in-flights
//     drain may abort them.
//
// Parameters:
//   - cluster: The cluster whose session to replace.
//   - newSession: The replacement session. Must not be nil.
//
// Returns:
//   - cql.Session: The previous session for the given cluster.
//   - error: [types.ErrSessionClosed] if the client has been closed,
//     [types.ErrNilSession] if newSession is nil,
//     [types.ErrInvalidCluster] for ClusterB on a single-cluster client
//     or any unrecognized ClusterID.
func (c *CQLClient) SwapSession(cluster ClusterID, newSession cql.Session) (cql.Session, error) {
	if c.closed.Load() {
		return nil, types.ErrSessionClosed
	}
	if newSession == nil {
		return nil, types.ErrNilSession
	}

	var slot *atomic.Pointer[sessionHolder]
	switch cluster {
	case ClusterA:
		slot = &c.sessionA
	case ClusterB:
		if c.singleCluster {
			return nil, types.ErrInvalidCluster
		}
		slot = &c.sessionB
	default:
		return nil, types.ErrInvalidCluster
	}

	old := slot.Swap(&sessionHolder{s: newSession})

	return old.s, nil
}

// RefreshSession invokes the [SessionRefresher] registered via
// [WithSessionRefresher], atomically installs the returned session in place
// of the existing one for the given cluster, and closes the old session.
//
// This is the high-level recovery entry point for the case where the live
// session is permanently unrecoverable (e.g., the cluster restarted at a
// new endpoint and gocql cannot reconnect). The decoupling principle —
// Helix does not know how to construct a [cql.Session] for any specific
// driver version — is preserved by delegating session construction to the
// caller-supplied refresher.
//
// Unlike [CQLClient.SwapSession], RefreshSession DOES close the old session
// because the refresh contract implies the old one is dead. If the
// refresher returns an error or a nil session, no swap occurs and the
// existing session remains live.
//
// Behavior:
//   - The refresher is invoked synchronously on the calling goroutine.
//     A long-running rebuild (e.g., re-handshake against a slow cluster)
//     blocks the caller; respect the passed context.
//   - If the refresher succeeds but the swap subsequently fails (e.g., the
//     client was closed between the refresher call and the swap), the
//     newly-built session is closed before returning the error so no
//     connection is leaked.
//   - The "lastErr" passed to the refresher is currently nil. v2 will
//     thread the most-recently-observed error per cluster through this
//     argument so refreshers can tailor reconnection strategy to the
//     observed failure mode.
//
// Parameters:
//   - ctx: Context for the refresher and any timeouts the refresher honors.
//   - cluster: The cluster to refresh.
//
// Returns:
//   - error: [types.ErrSessionClosed] if the client has been closed,
//     [types.ErrNoSessionRefresher] if no refresher was configured,
//     [types.ErrInvalidCluster] for an unsupported cluster on this client,
//     [types.ErrNilSession] if the refresher returned a nil session, or
//     a wrapped error from the refresher.
func (c *CQLClient) RefreshSession(ctx context.Context, cluster ClusterID) error {
	if c.closed.Load() {
		return types.ErrSessionClosed
	}
	if c.config.SessionRefresher == nil {
		return types.ErrNoSessionRefresher
	}

	// Validate cluster before calling the (potentially expensive) refresher.
	if cluster != ClusterA && cluster != ClusterB {
		return types.ErrInvalidCluster
	}
	if cluster == ClusterB && c.singleCluster {
		return types.ErrInvalidCluster
	}

	// Thread the most recently observed failure for this cluster through
	// to the refresher so it can tailor reconnection strategy to the
	// observed failure mode (e.g. "no hosts available" suggests a hard
	// reachability issue while "timeout" suggests a slow but reachable
	// cluster). Nil if the cluster has had no recorded failures.
	var lastErr error
	if e := c.statsForCluster(cluster).lastErr.Load(); e != nil {
		lastErr = *e
	}

	newSession, err := c.config.SessionRefresher(ctx, cluster, lastErr)
	if err != nil {
		return fmt.Errorf("session refresher: %w", err)
	}
	if newSession == nil {
		return types.ErrNilSession
	}

	old, err := c.SwapSession(cluster, newSession)
	if err != nil {
		// Swap can only fail here if the client was closed between the
		// refresher call and the swap. Don't leak the just-built session.
		newSession.Close()

		return err
	}

	// Refresh contract: the old session is dead, so close it on the
	// caller's behalf. SwapSession's contract differs ("caller closes")
	// because the caller may have other references they want to drain;
	// RefreshSession owns the swap end-to-end so it owns the close too.
	if old != nil {
		old.Close()
	}

	return nil
}

// Session returns the CQLClient as a CQLSession interface.
//
// This allows the CQLClient to be used as a drop-in replacement for gocql.Session
// in code that expects a session-like interface.
//
// Example:
//
//	client, _ := helix.NewCQLClient(sessionA, sessionB)
//	session := client.Session()
//
//	// Use session like a regular gocql.Session
//	err := session.Query("INSERT INTO ...").Exec()
//
// Returns:
//   - CQLSession: The client as a CQLSession interface
func (c *CQLClient) Session() CQLSession {
	return c
}

// DefaultExecuteFunc returns an ExecuteFunc for use with replay workers.
//
// This is a convenience method that creates an executor which routes replay
// payloads to the appropriate cluster session. It handles both single queries
// and batch operations, preserving the original timestamp for idempotency.
//
// The returned function:
//   - Routes to sessionA or sessionB based on payload.TargetCluster
//   - Handles batch operations (IsBatch=true) with proper BatchType
//   - Preserves the original write timestamp for idempotent replays
//   - Respects context cancellation and timeouts via ExecContext
//
// Example:
//
//	client, _ := helix.NewCQLClient(sessionA, sessionB, helix.WithReplayer(replayer))
//
//	// Create worker with the default executor
//	worker := replay.NewMemoryWorker(replayer, client.DefaultExecuteFunc(),
//	    replay.WithOnSuccess(func(p types.ReplayPayload) {
//	        log.Printf("Replay succeeded for cluster %s", p.TargetCluster)
//	    }),
//	)
//
// Returns:
//   - replay.ExecuteFunc: A function that executes replay payloads
func (c *CQLClient) DefaultExecuteFunc() replay.ExecuteFunc {
	return func(ctx context.Context, payload types.ReplayPayload) error {
		session := c.getSession(payload.TargetCluster)

		if payload.IsBatch {
			batch := session.Batch(payload.BatchType)
			for _, stmt := range payload.BatchStatements {
				batch = batch.Query(stmt.Query, stmt.Args...)
			}

			return batch.WithTimestamp(payload.Timestamp).ExecContext(ctx)
		}

		return session.Query(payload.Query, payload.Args...).
			WithTimestamp(payload.Timestamp).
			ExecContext(ctx)
	}
}

// IsSingleCluster returns true if the client is operating in single-cluster mode.
//
// In single-cluster mode, all operations are executed directly on the primary
// session without dual-write or failover logic. The mode is fixed at
// construction time; SwapSession and RefreshSession cannot promote a
// single-cluster client to dual-cluster.
func (c *CQLClient) IsSingleCluster() bool {
	return c.singleCluster
}

// getSession returns the session for the given cluster.
// In single-cluster mode, always returns sessionA.
func (c *CQLClient) getSession(cluster ClusterID) cql.Session {
	if c.singleCluster || cluster == ClusterA {
		return c.loadSessionA()
	}

	return c.loadSessionB()
}

// getDrainStates returns the current drain state for both clusters.
func (c *CQLClient) getDrainStates() (drainA, drainB bool) {
	return c.drainA.Load(), c.drainB.Load()
}

// clusterIsDraining checks if the given cluster is draining based on cached states.
func (c *CQLClient) clusterIsDraining(cluster ClusterID, drainA, drainB bool) bool {
	if cluster == ClusterA {
		return drainA
	}

	return drainB
}

// alternativeCluster returns the other cluster.
func (c *CQLClient) alternativeCluster(cluster ClusterID) ClusterID {
	if cluster == ClusterA {
		return ClusterB
	}

	return ClusterA
}

// selectClusterForCAS returns the cluster to use for CAS (lightweight transaction)
// operations. CAS operations are single-cluster, non-replicated conditional writes
// and are NOT affected by the AllowedClusters override.
func (c *CQLClient) selectClusterForCAS(ctx context.Context) ClusterID {
	if c.IsSingleCluster() || c.config.ReadStrategy == nil {
		return ClusterA
	}
	return c.config.ReadStrategy.Select(ctx)
}

// overrideSnapshot is the resolved override state for a single operation.
// Returned by value — zero heap allocation.
type overrideSnapshot struct {
	active   bool      // true if override is in effect
	primary  ClusterID // first valid cluster (for routing)
	fallback ClusterID // second valid cluster (for failover), empty if none
}

// readTarget is the resolved cluster selection for a single read operation.
// Combines the selected cluster with the override snapshot so both come from
// a single atomic resolution — no divergence possible.
type readTarget struct {
	cluster ClusterID        // the cluster to read from
	snap    overrideSnapshot // override state for this operation
	err     error            // non-nil on fail-closed conditions
}

// callAllowedClusters invokes the AllowedClustersFunc with panic recovery.
// On panic, it returns ErrClusterOverridePanic.
func callAllowedClusters(fn AllowedClustersFunc) (raw []ClusterID, err error) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			err = fmt.Errorf("%w: %v\n%s", types.ErrClusterOverridePanic, r, buf[:n])
		}
	}()

	return fn(), nil
}

// resolveReadTarget is the single entry point for all read paths.
// It returns the selected cluster and override snapshot as one unit.
// Called exactly once per operation — no downstream function re-evaluates.
func (c *CQLClient) resolveReadTarget(ctx context.Context) readTarget {
	fn := c.config.AllowedClusters
	if fn == nil {
		return readTarget{cluster: c.normalSelect(ctx)}
	}

	raw, err := callAllowedClusters(fn)
	if err != nil {
		if c.shouldLogOverrideErr() {
			c.config.Logger.Error("cluster override function panicked",
				"error", err.Error(),
			)
		}

		return readTarget{err: err}
	}

	// nil or empty = explicit opt-out, normal behavior
	if len(raw) == 0 {
		return readTarget{cluster: c.normalSelect(ctx)}
	}

	// Single-cluster guard
	if c.IsSingleCluster() {
		for _, entry := range raw {
			if entry != ClusterA {
				if c.shouldLogOverrideErr() {
					c.config.Logger.Error("cluster override targets unconfigured cluster in single-cluster mode",
						"cluster", string(entry),
					)
				}

				return readTarget{err: types.ErrInvalidClusterOverride}
			}
		}

		c.overrideErrSeq.Store(0)

		return readTarget{
			cluster: ClusterA,
			snap:    overrideSnapshot{active: true, primary: ClusterA},
		}
	}

	// Dual-cluster: iterate once, dedup + filter known IDs + apply drain
	drainA, drainB := c.getDrainStates()
	var primary, fallback ClusterID
	hadKnown := false

	for _, entry := range raw {
		if entry != ClusterA && entry != ClusterB {
			continue // skip unknown
		}
		hadKnown = true
		if c.clusterIsDraining(entry, drainA, drainB) {
			continue // drain filters
		}
		if primary == "" {
			primary = entry
		} else if fallback == "" && entry != primary {
			fallback = entry
		}
	}

	if primary == "" {
		if hadKnown {
			if c.shouldLogOverrideErr() {
				c.config.Logger.Error("cluster override conflicts with drain state — no valid clusters for read",
					"overrideClusters", raw,
					"drainA", drainA,
					"drainB", drainB,
				)
			}

			return readTarget{err: types.ErrNoValidClusters} // drain conflict
		}

		if c.shouldLogOverrideErr() {
			c.config.Logger.Error("cluster override returned only unknown cluster IDs",
				"overrideClusters", raw,
			)
		}

		return readTarget{err: types.ErrInvalidClusterOverride} // all unknown
	}

	c.overrideErrSeq.Store(0)

	return readTarget{
		cluster: primary,
		snap:    overrideSnapshot{active: true, primary: primary, fallback: fallback},
	}
}

// normalSelect delegates to the ReadStrategy or defaults to ClusterA.
func (c *CQLClient) normalSelect(ctx context.Context) ClusterID {
	if c.IsSingleCluster() || c.config.ReadStrategy == nil {
		return ClusterA
	}
	return c.config.ReadStrategy.Select(ctx)
}

// clusterName returns the display name for the given cluster.
func (c *CQLClient) clusterName(cluster ClusterID) string {
	return c.config.ClusterNames.Name(cluster)
}

// writeContext holds information about a write operation for replay purposes.
type writeContext struct {
	statement    string
	args         []any
	timestamp    int64
	priority     PriorityLevel
	isBatch      bool
	batchType    BatchType
	batchEntries []batchEntry // Internal format, converted lazily for replay
}

// toBatchStatements converts internal batchEntries to types.BatchStatement for replay.
// This conversion is deferred to the failure path to avoid allocations on success.
func (wc *writeContext) toBatchStatements() []types.BatchStatement {
	if len(wc.batchEntries) == 0 {
		return nil
	}

	stmts := make([]types.BatchStatement, len(wc.batchEntries))
	for i, entry := range wc.batchEntries {
		stmts[i] = types.BatchStatement{
			Query: entry.statement,
			Args:  entry.args,
		}
	}

	return stmts
}

// executeWriteWithReplay performs a write operation with optional dual-write and replay support.
//
// In single-cluster mode, the write is executed directly on sessionA.
// In dual-cluster mode, writes are executed concurrently on both clusters with replay
// for partial failures.
//
// If a cluster is in drain mode, writes to that cluster are skipped and enqueued
// for replay instead. If both clusters are draining, the write fails with ErrBothClustersDraining.
func (c *CQLClient) executeWriteWithReplay(
	ctx context.Context,
	wc writeContext,
	writeFunc func(context.Context, cql.Session) error,
) error {
	if c.closed.Load() {
		return types.ErrSessionClosed
	}

	// Single-cluster mode: direct execution, no dual-write logic.
	// The auto-refresh detector tracks cluster-A outcomes only here —
	// without this site single-cluster auto-refresh silently never fires
	// because no other code path observes the err for stats purposes.
	if c.IsSingleCluster() {
		err := writeFunc(ctx, c.loadSessionA())
		c.recordOpOutcome(ClusterA, err)

		return err
	}

	// Check drain mode
	drainA := c.drainA.Load()
	drainB := c.drainB.Load()

	// If both clusters are draining, fail immediately
	if drainA && drainB {
		return types.ErrBothClustersDraining
	}

	// If only one cluster is draining, write to the healthy one and enqueue replay
	if drainA || drainB {
		return c.executeWriteWithDrain(ctx, wc, writeFunc, drainA, drainB)
	}

	// Normal dual-cluster mode: concurrent writes with replay support
	return c.executeDualWrite(ctx, wc, writeFunc)
}

// executeWriteWithDrain handles writes when one cluster is draining.
// Invariant: Exactly one of drainA or drainB is true when this function is called.
func (c *CQLClient) executeWriteWithDrain(
	ctx context.Context,
	wc writeContext,
	writeFunc func(context.Context, cql.Session) error,
	drainA, _ bool,
) error {
	var session cql.Session
	var healthyCluster, drainingCluster ClusterID

	if drainA {
		session = c.loadSessionB()
		healthyCluster = ClusterB
		drainingCluster = ClusterA
	} else {
		session = c.loadSessionA()
		healthyCluster = ClusterA
		drainingCluster = ClusterB
	}

	// Execute write on the healthy cluster
	start := time.Now()
	err := writeFunc(ctx, session)
	elapsed := time.Since(start).Seconds()

	if err != nil {
		c.config.Metrics.IncWriteTotal(healthyCluster)
		c.config.Metrics.IncWriteError(healthyCluster)
		c.config.Metrics.ObserveWriteDuration(healthyCluster, elapsed)
		c.recordOpOutcome(healthyCluster, err)

		return err
	}

	c.config.Metrics.IncWriteTotal(healthyCluster)
	c.config.Metrics.ObserveWriteDuration(healthyCluster, elapsed)
	c.recordOpOutcome(healthyCluster, nil)

	// Enqueue for replay to the draining cluster
	if c.config.Replayer != nil {
		payload := types.ReplayPayload{
			TargetCluster:   drainingCluster,
			Query:           wc.statement,
			Args:            wc.args,
			IsBatch:         wc.isBatch,
			BatchType:       wc.batchType,
			BatchStatements: wc.toBatchStatements(), // Lazy conversion
			Timestamp:       wc.timestamp,
			Priority:        wc.priority,
		}
		// Use context.WithoutCancel to ensure replay is enqueued even if the request context is cancelled
		if enqueueErr := c.config.Replayer.Enqueue(context.WithoutCancel(ctx), payload); enqueueErr == nil {
			c.config.Metrics.IncReplayEnqueued(drainingCluster)
		} else {
			c.config.Metrics.IncReplayDropped(drainingCluster)
			c.config.Logger.Error("failed to enqueue write for replay during drain",
				"cluster", c.clusterName(drainingCluster),
				"enqueueError", enqueueErr.Error(),
			)
			if c.config.OnReplayDropped != nil {
				c.config.OnReplayDropped(payload, enqueueErr)
			}
		}
	}

	return nil
}

// executeDualWrite performs the normal dual-cluster write.
func (c *CQLClient) executeDualWrite(
	ctx context.Context,
	wc writeContext,
	writeFunc func(context.Context, cql.Session) error,
) error {
	// Dual-cluster mode: concurrent writes with replay support
	// Note: We capture start times outside the write functions to avoid data races
	// when WriteStrategy uses fire-and-forget (background goroutines).
	var startA, startB atomic.Int64

	// Session refs are resolved at call time inside the closure body so a
	// concurrent SwapSession or RefreshSession is observed by the next
	// dispatch. In-flight closures that have already loaded their session
	// continue against that captured ref; this preserves "the write was
	// dispatched to cluster X" semantics for fire-and-forget strategies.
	writeA := func(ctx context.Context) error {
		startA.Store(time.Now().UnixNano())
		return writeFunc(ctx, c.loadSessionA())
	}
	writeB := func(ctx context.Context) error {
		startB.Store(time.Now().UnixNano())
		return writeFunc(ctx, c.loadSessionB())
	}

	var errA, errB error

	if c.config.WriteStrategy != nil {
		errA, errB = c.config.WriteStrategy.Execute(ctx, writeA, writeB)
	} else {
		// Default: concurrent dual write
		var wg sync.WaitGroup

		wg.Go(func() {
			errA = writeA(ctx)
		})

		wg.Go(func() {
			errB = writeB(ctx)
		})

		wg.Wait()
	}

	// Classify results: distinguish operational sentinel states from real errors.
	// ErrWriteAsync  — write is in flight via fire-and-forget (not a cluster error).
	// ErrWriteDropped — write was not attempted due to concurrency limit (not a cluster error).
	isAsyncA := errors.Is(errA, types.ErrWriteAsync)
	isDroppedA := errors.Is(errA, types.ErrWriteDropped)
	isAsyncB := errors.Is(errB, types.ErrWriteAsync)
	isDroppedB := errors.Is(errB, types.ErrWriteDropped)

	// Record metrics for both clusters.
	// Use atomic loads to safely read start times that may have been set by fire-and-forget goroutines.
	now := time.Now()
	nowNano := now.UnixNano()

	c.config.Metrics.IncWriteTotal(ClusterA)
	if startANano := startA.Load(); startANano > 0 {
		c.config.Metrics.ObserveWriteDuration(ClusterA, float64(nowNano-startANano)/float64(time.Second))
	}
	switch {
	case isAsyncA:
		c.config.Metrics.IncWriteAsync(ClusterA)
	case isDroppedA:
		c.config.Metrics.IncWriteDropped(ClusterA)
	case errA != nil:
		c.config.Metrics.IncWriteError(ClusterA)
	}

	c.config.Metrics.IncWriteTotal(ClusterB)
	if startBNano := startB.Load(); startBNano > 0 {
		c.config.Metrics.ObserveWriteDuration(ClusterB, float64(nowNano-startBNano)/float64(time.Second))
	}
	switch {
	case isAsyncB:
		c.config.Metrics.IncWriteAsync(ClusterB)
	case isDroppedB:
		c.config.Metrics.IncWriteDropped(ClusterB)
	case errB != nil:
		c.config.Metrics.IncWriteError(ClusterB)
	}

	// Auto-refresh stat tracking — invoked PER cluster so partial-success
	// (A=ok, B=err) correctly advances A's lastSuccess while accumulating
	// failures on B. recordOpOutcome internally skips ErrWriteAsync /
	// ErrWriteDropped / ErrNotFound so operational states don't poison
	// the failure counters.
	c.recordOpOutcome(ClusterA, errA)
	c.recordOpOutcome(ClusterB, errB)

	// Both succeeded definitively.
	if errA == nil && errB == nil {
		return nil
	}

	// Both clusters had real (non-operational) failures — hard error, no replay.
	realErrA := errA != nil && !isAsyncA && !isDroppedA
	realErrB := errB != nil && !isAsyncB && !isDroppedB
	if realErrA && realErrB {
		return &types.DualClusterError{ErrorA: errA, ErrorB: errB}
	}

	// At least one cluster had a non-nil result (error, async, or dropped).
	// Enqueue replay for each affected cluster to ensure eventual consistency.
	//
	// ErrWriteAsync:   write is in flight; replay is a safety net (idempotent for Cassandra
	//                  because both attempts use the same client-generated timestamp).
	// ErrWriteDropped: write was never attempted; replay is required for reconciliation.
	// Real error:      write definitively failed; replay is required.
	if c.config.Replayer != nil {
		c.enqueueReplayIfNeeded(ctx, wc, ClusterA, errA, isAsyncA, isDroppedA)
		c.enqueueReplayIfNeeded(ctx, wc, ClusterB, errB, isAsyncB, isDroppedB)
	}

	// Partial success (or all-async) is still success from the caller's perspective.
	return nil
}

// enqueueReplayIfNeeded enqueues a replay payload when a cluster write had a non-nil result.
// isAsync and isDropped distinguish the two operational sentinel states so the log message
// accurately reflects what happened: async means the write is in flight; dropped means the
// write was never attempted because the concurrency limit was full.
func (c *CQLClient) enqueueReplayIfNeeded(
	ctx context.Context,
	wc writeContext,
	cluster ClusterID,
	err error,
	isAsync, isDropped bool,
) {
	if err == nil {
		return
	}

	payload := types.ReplayPayload{
		TargetCluster:   cluster,
		Query:           wc.statement,
		Args:            wc.args,
		IsBatch:         wc.isBatch,
		BatchType:       wc.batchType,
		BatchStatements: wc.toBatchStatements(),
		Timestamp:       wc.timestamp,
		Priority:        wc.priority,
	}

	// Use context.WithoutCancel so the enqueue succeeds even if the request context is cancelled.
	if enqueueErr := c.config.Replayer.Enqueue(context.WithoutCancel(ctx), payload); enqueueErr == nil {
		c.config.Metrics.IncReplayEnqueued(cluster)
		switch {
		case isDropped:
			c.config.Logger.Info("write dropped (concurrency limit reached) on degraded cluster, enqueued for replay",
				"cluster", c.clusterName(cluster),
			)
		case isAsync:
			c.config.Logger.Info("write dispatched asynchronously to degraded cluster, enqueued for replay",
				"cluster", c.clusterName(cluster),
			)
		default:
			c.config.Logger.Warn("write failed on cluster, enqueued for replay",
				"cluster", c.clusterName(cluster),
				"error", err.Error(),
			)
		}
	} else {
		c.config.Metrics.IncReplayDropped(cluster)
		c.config.Logger.Error("failed to enqueue write for replay",
			"cluster", c.clusterName(cluster),
			"writeError", err.Error(),
			"enqueueError", enqueueErr.Error(),
		)
		if c.config.OnReplayDropped != nil {
			c.config.OnReplayDropped(payload, enqueueErr)
		}
	}
}

// readOptions holds per-read options resolved from the three-level hierarchy:
// per-query FallbackRead() > context WithFallbackRead(ctx) > client DefaultFallbackRead.
type readOptions struct {
	fallbackRead bool
}

func (c *CQLClient) resolveReadOptions(ctx context.Context, q *cqlQuery) readOptions {
	if q.fallbackRead {
		return readOptions{fallbackRead: true}
	}
	if hasFallbackRead(ctx) {
		return readOptions{fallbackRead: true}
	}
	return readOptions{fallbackRead: c.config.DefaultFallbackRead}
}

// recordReadSuccess records a successful read, using latency-aware recording if supported.
// When overrideActive is true, the ReadStrategy is frozen (no OnSuccess call).
// FailoverPolicy always receives health signals regardless of override state.
func (c *CQLClient) recordReadSuccess(cluster ClusterID, elapsed float64, overrideActive bool) {
	if !overrideActive && c.config.ReadStrategy != nil {
		c.config.ReadStrategy.OnSuccess(cluster)
	}
	if c.config.FailoverPolicy != nil {
		// Use latency-aware recording if supported, otherwise just record success
		if recorder, ok := c.config.FailoverPolicy.(LatencyRecorder); ok {
			recorder.RecordLatency(cluster, time.Duration(elapsed*float64(time.Second)))
		} else {
			c.config.FailoverPolicy.RecordSuccess(cluster)
		}
	}
}

// executeRead performs a read operation with optional sticky routing and failover.
//
// In single-cluster mode, the read is executed directly on sessionA.
// In dual-cluster mode, reads use sticky routing with failover to the alternative cluster.
// Clusters in drain mode are skipped unless both clusters are draining.
//
// When an AllowedClusters override is active, the ReadStrategy is bypassed and
// the override list directly controls routing. The strategy's internal state is
// frozen (no OnSuccess/OnFailure calls) until the override is removed.
//
// Not-found results (types.ErrNotFound) are never recorded as cluster failures.
// If opts.fallbackRead is true and the selected cluster returns not-found,
// executeFallbackRead silently tries the other cluster before returning not-found.
func (c *CQLClient) executeRead(
	ctx context.Context,
	opts readOptions,
	readFunc func(context.Context, cql.Session) error,
) error {
	if c.closed.Load() {
		return types.ErrSessionClosed
	}

	// Resolve cluster selection (override-aware).
	// This must run before the single-cluster fast path so that fail-closed
	// conditions (e.g., targeting ClusterB in single-cluster mode) are caught.
	rt := c.resolveReadTarget(ctx)
	if rt.err != nil {
		return rt.err
	}

	// Single-cluster mode: direct execution, no failover or drain logic.
	// This applies whether AllowedClusters is nil, returns nil/empty, or
	// returns [ClusterA]. In all cases resolveReadTarget returns ClusterA;
	// there is no second session to failover or FallbackRead to.
	if c.IsSingleCluster() {
		start := time.Now()
		err := readFunc(ctx, c.loadSessionA())
		elapsed := time.Since(start).Seconds()

		c.config.Metrics.IncReadTotal(ClusterA)
		c.config.Metrics.ObserveReadDuration(ClusterA, elapsed)
		// Not-found is not a health signal; skip IncReadError for it.
		// In single-cluster mode there is no alternative for FallbackRead.
		if err != nil && !types.IsNotFound(err) {
			c.config.Metrics.IncReadError(ClusterA)
		}
		// Auto-refresh outcome — recordOpOutcome internally excludes
		// ErrNotFound so the not-found check is doubly safe here.
		c.recordOpOutcome(ClusterA, err)

		return err
	}

	// Dual-cluster mode below.

	selectedCluster := rt.cluster

	// When override is NOT active, apply drain-aware selection (existing behavior)
	if !rt.snap.active {
		drainA, drainB := c.getDrainStates()
		if c.clusterIsDraining(selectedCluster, drainA, drainB) {
			alt := c.alternativeCluster(selectedCluster)
			if !c.clusterIsDraining(alt, drainA, drainB) {
				selectedCluster = alt
			}
			// If both are draining, proceed with original selection (best effort)
		}
	}

	// Try primary selection.
	session := c.getSession(selectedCluster)
	start := time.Now()
	err := readFunc(ctx, session)
	elapsed := time.Since(start).Seconds()

	c.config.Metrics.IncReadTotal(selectedCluster)
	c.config.Metrics.ObserveReadDuration(selectedCluster, elapsed)

	if err == nil {
		c.recordReadSuccess(selectedCluster, elapsed, rt.snap.active)
		c.recordOpOutcome(selectedCluster, nil)
		return nil
	}

	// Not-found is NOT a health signal — the cluster responded correctly.
	if types.IsNotFound(err) {
		// recordOpOutcome handles the ErrNotFound exclusion internally,
		// but we don't actually need to call it here — not-found is
		// neither success nor failure for auto-refresh purposes.
		if opts.fallbackRead {
			return c.executeFallbackRead(ctx, rt.snap, selectedCluster, readFunc)
		}
		return err
	}

	// Real error path: record failure and potentially failover.
	c.config.Metrics.IncReadError(selectedCluster)
	c.recordOpOutcome(selectedCluster, err)

	if rt.snap.active {
		return c.executeOverrideFailover(ctx, rt, err, readFunc)
	}

	return c.executeNormalFailover(ctx, selectedCluster, err, readFunc)
}

// executeOverrideFailover handles failover when an AllowedClusters override is active.
// The ReadStrategy is NOT consulted — failover target comes from the override snapshot.
func (c *CQLClient) executeOverrideFailover(
	ctx context.Context,
	rt readTarget,
	primaryErr error,
	readFunc func(context.Context, cql.Session) error,
) error {
	selected := rt.cluster

	if c.config.FailoverPolicy != nil {
		c.config.FailoverPolicy.RecordFailure(selected)
	}

	// No failover target in the override list
	if rt.snap.fallback == "" || rt.snap.fallback == selected {
		return primaryErr
	}

	// FailoverPolicy still gates failover
	if c.config.FailoverPolicy != nil &&
		!c.config.FailoverPolicy.ShouldFailover(selected, primaryErr) {
		return primaryErr
	}

	return c.tryFallbackCluster(ctx, selected, rt.snap.fallback, primaryErr, true, readFunc)
}

// executeNormalFailover handles failover using the ReadStrategy (no override active).
func (c *CQLClient) executeNormalFailover(
	ctx context.Context,
	selectedCluster ClusterID,
	primaryErr error,
	readFunc func(context.Context, cql.Session) error,
) error {
	if c.config.FailoverPolicy != nil {
		c.config.FailoverPolicy.RecordFailure(selectedCluster)

		if !c.config.FailoverPolicy.ShouldFailover(selectedCluster, primaryErr) {
			return primaryErr
		}
	}

	// Ask strategy for alternative.
	var alternativeCluster ClusterID
	var shouldFailover bool

	if c.config.ReadStrategy != nil {
		alternativeCluster, shouldFailover = c.config.ReadStrategy.OnFailure(selectedCluster, primaryErr)
	} else {
		alternativeCluster = c.alternativeCluster(selectedCluster)
		shouldFailover = true
	}

	// Don't failover to a draining cluster unless we came from a draining cluster too.
	drainA, drainB := c.getDrainStates()
	if shouldFailover && c.clusterIsDraining(alternativeCluster, drainA, drainB) {
		if !c.clusterIsDraining(selectedCluster, drainA, drainB) {
			shouldFailover = false
		}
	}

	if !shouldFailover {
		return primaryErr
	}

	return c.tryFallbackCluster(ctx, selectedCluster, alternativeCluster, primaryErr, false, readFunc)
}

// tryFallbackCluster executes a read on the fallback cluster after the primary
// cluster failed. It records metrics, handles not-found, and returns a
// DualClusterError when both clusters fail. Used by both override and normal
// failover paths.
func (c *CQLClient) tryFallbackCluster(
	ctx context.Context,
	selected, fallback ClusterID,
	primaryErr error,
	overrideActive bool,
	readFunc func(context.Context, cql.Session) error,
) error {
	c.config.Metrics.IncFailoverTotal(selected, fallback)
	c.config.Logger.Warn("read failed, failing over to alternative cluster",
		"fromCluster", c.clusterName(selected),
		"toCluster", c.clusterName(fallback),
		"error", primaryErr.Error(),
	)

	session := c.getSession(fallback)
	start := time.Now()
	err := readFunc(ctx, session)
	elapsed := time.Since(start).Seconds()

	c.config.Metrics.IncReadTotal(fallback)
	c.config.Metrics.ObserveReadDuration(fallback, elapsed)

	if err == nil {
		c.recordReadSuccess(fallback, elapsed, overrideActive)
		c.recordOpOutcome(fallback, nil)
		return nil
	}

	if types.IsNotFound(err) {
		return err
	}

	c.config.Metrics.IncReadError(fallback)
	c.recordOpOutcome(fallback, err)
	if c.config.FailoverPolicy != nil {
		c.config.FailoverPolicy.RecordFailure(fallback)
	}

	if selected == ClusterA {
		return &types.DualClusterError{ErrorA: primaryErr, ErrorB: err}
	}

	return &types.DualClusterError{ErrorA: err, ErrorB: primaryErr}
}

// executeFallbackRead attempts a single silent read on the alternative cluster
// after the selected cluster returned not-found.
//
// This is a one-shot check — it does NOT re-enter the main failover sequence.
// When override is not active, drain state is bypassed: the caller explicitly
// asked to check both clusters, and a draining cluster may still hold the data.
//
// When override IS active, the alternative must be in the allowed set.
// If the alternative is fenced off, ErrNotFound is returned immediately.
//
// Returns:
//   - nil when the alternative cluster has the data (divergence metric emitted)
//   - types.ErrNotFound when both clusters confirm the row is absent, OR when
//     the alternative cluster is unreachable (health metrics are still recorded
//     on the unreachable cluster, but the caller receives the primary's healthy
//     not-found to preserve availability)
func (c *CQLClient) executeFallbackRead(
	ctx context.Context,
	snap overrideSnapshot,
	selectedCluster ClusterID,
	readFunc func(context.Context, cql.Session) error,
) error {
	alternativeCluster := c.alternativeCluster(selectedCluster)

	// Override fence: don't probe a cluster excluded by the override.
	if snap.active && alternativeCluster != snap.primary && alternativeCluster != snap.fallback {
		return types.ErrNotFound
	}

	c.config.Logger.Debug("fallback read: selected cluster returned not-found, trying alternative",
		"fromCluster", c.clusterName(selectedCluster),
		"toCluster", c.clusterName(alternativeCluster),
	)

	// Bypass drain state: the caller opted in with FallbackRead.
	alternativeSession := c.getSession(alternativeCluster)
	start := time.Now()
	err := readFunc(ctx, alternativeSession)
	elapsed := time.Since(start).Seconds()

	c.config.Metrics.IncReadTotal(alternativeCluster)
	c.config.Metrics.ObserveReadDuration(alternativeCluster, elapsed)

	if err == nil {
		// Found the data on the alternative cluster — divergence (replay lag).
		c.recordReadSuccess(alternativeCluster, elapsed, snap.active)
		c.recordOpOutcome(alternativeCluster, nil)
		c.config.Metrics.IncReadDivergence(selectedCluster)
		c.config.Logger.Debug("fallback read: found data on alternative cluster",
			"staleCluster", c.clusterName(selectedCluster),
		)
		return nil
	}

	if types.IsNotFound(err) {
		// Both clusters confirmed the row is absent — definitively not found.
		c.config.Logger.Debug("fallback read: alternative cluster also returned not-found",
			"cluster", c.clusterName(alternativeCluster),
		)
		return err
	}

	// Alternative returned a real error — record the health impact but return
	// not-found to the caller. The primary (healthy) cluster already confirmed
	// the row is absent. Returning the alternative's error would make FallbackRead
	// decrease availability versus not using it: without FallbackRead, the
	// primary's not-found would have been returned cleanly. A "try harder"
	// feature must not make things worse.
	c.config.Metrics.IncReadError(alternativeCluster)
	c.recordOpOutcome(alternativeCluster, err)
	if c.config.FailoverPolicy != nil {
		c.config.FailoverPolicy.RecordFailure(alternativeCluster)
	}
	c.config.Logger.Warn("fallback read: alternative cluster returned error, returning primary not-found",
		"cluster", c.clusterName(alternativeCluster),
		"error", err.Error(),
	)

	return types.ErrNotFound
}

// cqlQuery implements the Query interface for CQLClient.
type cqlQuery struct {
	client            *CQLClient
	statement         string
	values            []any
	ctx               context.Context
	consistency       *Consistency
	serialConsistency *Consistency
	pageSize          *int
	pageState         []byte
	timestamp         *int64
	priority          *PriorityLevel
	fallbackRead      bool
}

func (q *cqlQuery) WithContext(ctx context.Context) Query {
	q.ctx = ctx
	return q
}

func (q *cqlQuery) Consistency(c Consistency) Query {
	q.consistency = &c
	return q
}

// SetConsistency sets the consistency level for this query.
//
// Deprecated: Use Consistency() instead for the modern fluent API.
// This method is provided for compatibility with gocql v1 users.
func (q *cqlQuery) SetConsistency(c Consistency) {
	q.Consistency(c)
}

func (q *cqlQuery) SerialConsistency(c Consistency) Query {
	q.serialConsistency = &c
	return q
}

func (q *cqlQuery) PageSize(n int) Query {
	q.pageSize = &n
	return q
}

func (q *cqlQuery) PageState(state []byte) Query {
	q.pageState = state
	return q
}

func (q *cqlQuery) WithTimestamp(ts int64) Query {
	q.timestamp = &ts
	return q
}

func (q *cqlQuery) WithPriority(p PriorityLevel) Query {
	q.priority = &p
	return q
}

func (q *cqlQuery) FallbackRead() Query {
	q.fallbackRead = true
	return q
}

func (q *cqlQuery) getContext() context.Context {
	if q.ctx != nil {
		return q.ctx
	}
	return context.Background()
}

func (q *cqlQuery) getTimestamp() int64 {
	if q.timestamp != nil {
		return *q.timestamp
	}
	return q.client.config.TimestampProvider()
}

func (q *cqlQuery) getPriority() PriorityLevel {
	if q.priority != nil {
		return *q.priority
	}
	return PriorityHigh
}

func (q *cqlQuery) applyConfig(query cql.Query) cql.Query {
	if q.consistency != nil {
		query = query.Consistency(*q.consistency)
	}
	if q.serialConsistency != nil {
		query = query.SerialConsistency(*q.serialConsistency)
	}
	if q.pageSize != nil {
		query = query.PageSize(*q.pageSize)
	}
	if q.pageState != nil {
		query = query.PageState(q.pageState)
	}

	return query
}

func (q *cqlQuery) Exec() error {
	return q.ExecContext(q.getContext())
}

func (q *cqlQuery) ExecContext(ctx context.Context) error {
	ts := q.getTimestamp()

	// Fast path for single-cluster mode to avoid allocations
	if q.client.IsSingleCluster() {
		if q.client.closed.Load() {
			return types.ErrSessionClosed
		}

		query := q.client.loadSessionA().Query(q.statement, q.values...)
		query = q.applyConfig(query)
		// Important for writes to generate the timestamp on the client side
		// to ensure consistency across clusters
		query = query.WithTimestamp(ts)

		err := query.ExecContext(ctx)
		q.client.recordOpOutcome(ClusterA, err)

		return err
	}

	wc := writeContext{
		statement: q.statement,
		args:      q.values,
		timestamp: ts,
		priority:  q.getPriority(),
	}

	return q.client.executeWriteWithReplay(ctx, wc, func(ctx context.Context, session cql.Session) error {
		query := session.Query(q.statement, q.values...)
		query = q.applyConfig(query)
		// Important for writes to generate the timestamp on the client side
		// to ensure consistency across clusters
		query = query.WithTimestamp(ts)

		return query.ExecContext(ctx)
	})
}

func (q *cqlQuery) Scan(dest ...any) error {
	return q.ScanContext(q.getContext(), dest...)
}

func (q *cqlQuery) ScanContext(ctx context.Context, dest ...any) error {
	opts := q.client.resolveReadOptions(ctx, q)
	return q.client.executeRead(ctx, opts, func(ctx context.Context, session cql.Session) error {
		query := session.Query(q.statement, q.values...)
		query = q.applyConfig(query)

		return query.ScanContext(ctx, dest...)
	})
}

func (q *cqlQuery) Iter() Iter {
	return q.IterContext(q.getContext())
}

// IterContext executes the query and returns an iterator.
//
// NOTE: Iterators do NOT support automatic failover. If the selected cluster
// fails during iteration, the error is returned to the caller.
// ReadStrategy.OnSuccess is only called if the iterator is closed successfully
// and no AllowedClusters override is active.
//
// If resolveReadTarget returns an error (fail-closed), an errorIter is returned
// that defers the error to Close(). Always call Close() and check its error.
func (q *cqlQuery) IterContext(ctx context.Context) Iter {
	if q.client.closed.Load() {
		return &errorIter{err: types.ErrSessionClosed}
	}

	rt := q.client.resolveReadTarget(ctx)
	if rt.err != nil {
		return &errorIter{err: rt.err}
	}

	session := q.client.getSession(rt.cluster)
	query := session.Query(q.statement, q.values...)
	query = q.applyConfig(query)

	return &cqlIter{
		iter:           query.IterContext(ctx),
		client:         q.client,
		cluster:        rt.cluster,
		overrideActive: rt.snap.active,
	}
}

func (q *cqlQuery) MapScan(m map[string]any) error {
	return q.MapScanContext(q.getContext(), m)
}

func (q *cqlQuery) MapScanContext(ctx context.Context, m map[string]any) error {
	opts := q.client.resolveReadOptions(ctx, q)
	return q.client.executeRead(ctx, opts, func(ctx context.Context, session cql.Session) error {
		query := session.Query(q.statement, q.values...)
		query = q.applyConfig(query)

		return query.MapScanContext(ctx, m)
	})
}

// ScanCAS executes a lightweight transaction (IF clause) and scans the result.
// CAS operations are executed on a single cluster and are NOT replicated.
func (q *cqlQuery) ScanCAS(dest ...any) (applied bool, err error) {
	return q.ScanCASContext(q.getContext(), dest...)
}

// ScanCASContext executes a lightweight transaction with context and scans the result.
// CAS operations are executed on a single cluster and are NOT replicated.
func (q *cqlQuery) ScanCASContext(ctx context.Context, dest ...any) (applied bool, err error) {
	selectedCluster := q.client.selectClusterForCAS(ctx)

	session := q.client.getSession(selectedCluster)
	query := session.Query(q.statement, q.values...)
	query = q.applyConfig(query)

	return query.ScanCASContext(ctx, dest...)
}

// MapScanCAS executes a lightweight transaction and scans the result into a map.
// CAS operations are executed on a single cluster and are NOT replicated.
func (q *cqlQuery) MapScanCAS(dest map[string]any) (applied bool, err error) {
	return q.MapScanCASContext(q.getContext(), dest)
}

// MapScanCASContext executes a lightweight transaction with context and scans into a map.
// CAS operations are executed on a single cluster and are NOT replicated.
func (q *cqlQuery) MapScanCASContext(ctx context.Context, dest map[string]any) (applied bool, err error) {
	selectedCluster := q.client.selectClusterForCAS(ctx)

	session := q.client.getSession(selectedCluster)
	query := session.Query(q.statement, q.values...)
	query = q.applyConfig(query)

	return query.MapScanCASContext(ctx, dest)
}

// batchEntry holds a single statement in a batch.
type batchEntry struct {
	statement string
	args      []any
}

// cqlBatch implements the Batch interface for CQLClient.
type cqlBatch struct {
	client            *CQLClient
	kind              BatchType
	entries           []batchEntry
	ctx               context.Context
	consistency       *Consistency
	serialConsistency *Consistency
	timestamp         *int64
	priority          *PriorityLevel
}

func (b *cqlBatch) Query(stmt string, args ...any) Batch {
	b.entries = append(b.entries, batchEntry{
		statement: stmt,
		args:      args,
	})
	return b
}

func (b *cqlBatch) Consistency(c Consistency) Batch {
	b.consistency = &c
	return b
}

// SetConsistency sets the consistency level for this batch.
//
// Deprecated: Use Consistency() instead for the modern fluent API.
// This method is provided for compatibility with gocql v1 users.
func (b *cqlBatch) SetConsistency(c Consistency) {
	b.Consistency(c)
}

func (b *cqlBatch) SerialConsistency(c Consistency) Batch {
	b.serialConsistency = &c
	return b
}

func (b *cqlBatch) Size() int {
	return len(b.entries)
}

func (b *cqlBatch) WithContext(ctx context.Context) Batch {
	b.ctx = ctx
	return b
}

func (b *cqlBatch) WithTimestamp(ts int64) Batch {
	b.timestamp = &ts
	return b
}

func (b *cqlBatch) WithPriority(p PriorityLevel) Batch {
	b.priority = &p
	return b
}

func (b *cqlBatch) getContext() context.Context {
	if b.ctx != nil {
		return b.ctx
	}
	return context.Background()
}

func (b *cqlBatch) getTimestamp() int64 {
	if b.timestamp != nil {
		return *b.timestamp
	}
	return b.client.config.TimestampProvider()
}

func (b *cqlBatch) getPriority() PriorityLevel {
	if b.priority != nil {
		return *b.priority
	}
	return PriorityHigh
}

func (b *cqlBatch) Exec() error {
	return b.ExecContext(b.getContext())
}

func (b *cqlBatch) ExecContext(ctx context.Context) error {
	ts := b.getTimestamp()

	// Fast path for single-cluster mode to avoid allocations
	if b.client.IsSingleCluster() {
		if b.client.closed.Load() {
			return types.ErrSessionClosed
		}

		batch := b.client.loadSessionA().Batch(b.kind)
		for _, entry := range b.entries {
			batch = batch.Query(entry.statement, entry.args...)
		}
		if b.consistency != nil {
			batch = batch.Consistency(*b.consistency)
		}
		if b.serialConsistency != nil {
			batch = batch.SerialConsistency(*b.serialConsistency)
		}
		batch = batch.WithTimestamp(ts)

		err := batch.ExecContext(ctx)
		b.client.recordOpOutcome(ClusterA, err)

		return err
	}

	wc := writeContext{
		statement:    "", // Empty for batch
		args:         nil,
		timestamp:    ts,
		priority:     b.getPriority(),
		isBatch:      true,
		batchType:    b.kind,
		batchEntries: b.entries, // Pass directly, convert lazily if needed for replay
	}

	return b.client.executeWriteWithReplay(ctx, wc, func(ctx context.Context, session cql.Session) error {
		batch := session.Batch(b.kind)
		for _, entry := range b.entries {
			batch = batch.Query(entry.statement, entry.args...)
		}
		if b.consistency != nil {
			batch = batch.Consistency(*b.consistency)
		}
		if b.serialConsistency != nil {
			batch = batch.SerialConsistency(*b.serialConsistency)
		}
		batch = batch.WithTimestamp(ts)

		return batch.ExecContext(ctx)
	})
}

// IterContext executes the batch and returns an iterator for the results.
//
// NOTE: Iterators do NOT support automatic failover. If the selected cluster
// fails during iteration, the error is returned to the caller.
// If resolveReadTarget returns an error, an errorIter is returned.
func (b *cqlBatch) IterContext(ctx context.Context) Iter {
	if b.client.closed.Load() {
		return &errorIter{err: types.ErrSessionClosed}
	}

	ts := b.getTimestamp()

	rt := b.client.resolveReadTarget(ctx)
	if rt.err != nil {
		return &errorIter{err: rt.err}
	}

	session := b.client.getSession(rt.cluster)
	batch := session.Batch(b.kind)
	for _, entry := range b.entries {
		batch = batch.Query(entry.statement, entry.args...)
	}
	if b.consistency != nil {
		batch = batch.Consistency(*b.consistency)
	}
	if b.serialConsistency != nil {
		batch = batch.SerialConsistency(*b.serialConsistency)
	}
	batch = batch.WithTimestamp(ts)

	return &cqlIter{
		iter:           batch.IterContext(ctx),
		client:         b.client,
		cluster:        rt.cluster,
		overrideActive: rt.snap.active,
	}
}

// ExecCAS executes a batch lightweight transaction.
// CAS operations are executed on a single cluster and are NOT replicated.
func (b *cqlBatch) ExecCAS(dest ...any) (applied bool, iter Iter, err error) {
	return b.ExecCASContext(b.getContext(), dest...)
}

// ExecCASContext executes a batch lightweight transaction with context.
// CAS operations are executed on a single cluster and are NOT replicated.
func (b *cqlBatch) ExecCASContext(ctx context.Context, dest ...any) (applied bool, iter Iter, err error) {
	ts := b.getTimestamp()

	selectedCluster := b.client.selectClusterForCAS(ctx)

	session := b.client.getSession(selectedCluster)
	batch := session.Batch(b.kind)
	for _, entry := range b.entries {
		batch = batch.Query(entry.statement, entry.args...)
	}
	if b.consistency != nil {
		batch = batch.Consistency(*b.consistency)
	}
	if b.serialConsistency != nil {
		batch = batch.SerialConsistency(*b.serialConsistency)
	}
	batch = batch.WithTimestamp(ts)

	applied, cqlItr, err := batch.ExecCASContext(ctx, dest...)
	if cqlItr == nil {
		return applied, nil, err
	}

	return applied, &cqlIter{
		iter:    cqlItr,
		client:  b.client,
		cluster: selectedCluster,
	}, err
}

// MapExecCAS executes a batch lightweight transaction and scans into a map.
// CAS operations are executed on a single cluster and are NOT replicated.
func (b *cqlBatch) MapExecCAS(dest map[string]any) (applied bool, iter Iter, err error) {
	return b.MapExecCASContext(b.getContext(), dest)
}

// MapExecCASContext executes a batch lightweight transaction with context and scans into a map.
// CAS operations are executed on a single cluster and are NOT replicated.
func (b *cqlBatch) MapExecCASContext(ctx context.Context, dest map[string]any) (applied bool, iter Iter, err error) {
	ts := b.getTimestamp()

	selectedCluster := b.client.selectClusterForCAS(ctx)

	session := b.client.getSession(selectedCluster)
	batch := session.Batch(b.kind)
	for _, entry := range b.entries {
		batch = batch.Query(entry.statement, entry.args...)
	}
	if b.consistency != nil {
		batch = batch.Consistency(*b.consistency)
	}
	if b.serialConsistency != nil {
		batch = batch.SerialConsistency(*b.serialConsistency)
	}
	batch = batch.WithTimestamp(ts)

	applied, cqlItr, err := batch.MapExecCASContext(ctx, dest)
	if cqlItr == nil {
		return applied, nil, err
	}

	return applied, &cqlIter{
		iter:    cqlItr,
		client:  b.client,
		cluster: selectedCluster,
	}, err
}

// cqlIter implements the Iter interface for CQLClient.
type cqlIter struct {
	iter           cql.Iter
	client         *CQLClient
	cluster        ClusterID
	overrideActive bool // captured from readTarget at creation, not re-evaluated
}

func (i *cqlIter) Scan(dest ...any) bool {
	return i.iter.Scan(dest...)
}

func (i *cqlIter) Close() error {
	err := i.iter.Close()
	if err == nil && !i.overrideActive && i.client.config.ReadStrategy != nil {
		i.client.config.ReadStrategy.OnSuccess(i.cluster)
	}
	return err
}

// errorIter is returned when resolveReadTarget fails. It defers the error
// to Close() since IterContext's signature cannot return an error directly.
// Scan()/MapScan() always return false. Close()/SliceMap() return the error.
// Metadata methods return zero values.
type errorIter struct {
	err error
}

func (e *errorIter) Scan(...any) bool                    { return false }
func (e *errorIter) Close() error                        { return e.err }
func (e *errorIter) MapScan(map[string]any) bool         { return false }
func (e *errorIter) SliceMap() ([]map[string]any, error) { return nil, e.err }
func (e *errorIter) PageState() []byte                   { return nil }
func (e *errorIter) NumRows() int                        { return 0 }
func (e *errorIter) Columns() []ColumnInfo               { return nil }
func (e *errorIter) Scanner() Scanner                    { return &errorScanner{err: e.err} }
func (e *errorIter) Warnings() []string                  { return nil }

// errorScanner is returned by errorIter.Scanner() so callers using the
// Scanner API get a graceful error instead of a nil-pointer panic.
type errorScanner struct{ err error }

func (s *errorScanner) Next() bool        { return false }
func (s *errorScanner) Scan(...any) error { return s.err }
func (s *errorScanner) Err() error        { return s.err }

func (i *cqlIter) MapScan(m map[string]any) bool {
	return i.iter.MapScan(m)
}

func (i *cqlIter) SliceMap() ([]map[string]any, error) {
	return i.iter.SliceMap()
}

func (i *cqlIter) PageState() []byte {
	return i.iter.PageState()
}

func (i *cqlIter) NumRows() int {
	return i.iter.NumRows()
}

func (i *cqlIter) Columns() []ColumnInfo {
	cqlCols := i.iter.Columns()
	result := make([]ColumnInfo, len(cqlCols))
	for idx, col := range cqlCols {
		result[idx] = ColumnInfo{
			Keyspace: col.Keyspace,
			Table:    col.Table,
			Name:     col.Name,
			TypeInfo: col.TypeInfo,
		}
	}

	return result
}

func (i *cqlIter) Scanner() Scanner {
	return &cqlScanner{scanner: i.iter.Scanner()}
}

func (i *cqlIter) Warnings() []string {
	return i.iter.Warnings()
}

// cqlScanner wraps cql.Scanner to implement helix.Scanner.
type cqlScanner struct {
	scanner cql.Scanner
}

func (s *cqlScanner) Next() bool {
	return s.scanner.Next()
}

func (s *cqlScanner) Scan(dest ...any) error {
	return s.scanner.Scan(dest...)
}

func (s *cqlScanner) Err() error {
	return s.scanner.Err()
}

// SessionA returns the underlying session for cluster A.
//
// Use with caution - direct access bypasses Helix's dual-cluster logic.
// Each call performs an atomic load and returns the live session, so the
// reference is current as of the call. Callers that store the returned
// reference will hold a stale pointer if SwapSession or RefreshSession is
// invoked subsequently — prefer calling SessionA() at point of use.
//
// Returns:
//   - cql.Session: The raw session for cluster A
func (c *CQLClient) SessionA() cql.Session {
	return c.loadSessionA()
}

// SessionB returns the underlying session for cluster B, or nil in
// single-cluster mode.
//
// Use with caution - direct access bypasses Helix's dual-cluster logic.
// See SessionA for the swap-vs-stored-reference caveat.
//
// Returns:
//   - cql.Session: The raw session for cluster B (nil in single-cluster mode)
func (c *CQLClient) SessionB() cql.Session {
	return c.loadSessionB()
}

// Config returns the current client configuration.
//
// Returns:
//   - *ClientConfig: The client's configuration
func (c *CQLClient) Config() *ClientConfig {
	return c.config
}
