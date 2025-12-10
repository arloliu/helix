package helix

import (
	"context"
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
	sessionA cql.Session
	sessionB cql.Session // nil for single-cluster mode
	config   *ClientConfig
	closed   atomic.Bool

	// Drain mode state
	drainA        atomic.Bool
	drainB        atomic.Bool
	topologyCtx   context.Context
	topologyClose context.CancelFunc
}

// Compile-time assertion that CQLClient implements CQLSession.
var _ CQLSession = (*CQLClient)(nil)

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

	// Propagate cluster names to components that support it
	propagateClusterNames(config)

	// Warn about missing Replayer in dual-cluster mode
	if sessionB != nil && config.Replayer == nil && !config.AutoMemoryWorker {
		config.Logger.Warn("dual-cluster mode with no Replayer configured - partial write failures will be lost and cannot be reconciled")
	}

	ctx, cancel := context.WithCancel(context.Background())

	client := &CQLClient{
		sessionA:      sessionA,
		sessionB:      sessionB,
		config:        config,
		topologyCtx:   ctx,
		topologyClose: cancel,
	}

	// Create auto memory worker if configured
	if config.AutoMemoryWorker {
		memReplayer := replay.NewMemoryReplayer(
			replay.WithQueueCapacity(config.AutoMemoryCapacity),
		)
		config.Replayer = memReplayer
		config.ReplayWorker = replay.NewMemoryWorker(
			memReplayer,
			client.DefaultExecuteFunc(),
			config.AutoMemoryWorkerOpts...,
		)
	}

	// Start replay worker if configured
	if config.ReplayWorker != nil {
		if err := config.ReplayWorker.Start(); err != nil {
			cancel()

			return nil, err
		}
	}

	// Start topology watcher if configured
	if config.TopologyWatcher != nil {
		go client.watchTopology()
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
// partially fails (succeeds on one cluster, fails on another), the Replay
// System will re-apply the operation, causing double-counting. Avoid using
// CounterBatch with dual-cluster mode if you require exactly-once semantics.
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
// The topology watcher and replay worker are stopped first.
// After Close is called, the client cannot be reused.
func (c *CQLClient) Close() {
	if c.closed.CompareAndSwap(false, true) {
		// Stop topology watcher
		if c.topologyClose != nil {
			c.topologyClose()
		}

		// Stop replay worker
		if c.config.ReplayWorker != nil {
			c.config.ReplayWorker.Stop()
		}

		c.sessionA.Close()
		if c.sessionB != nil {
			c.sessionB.Close()
		}
	}
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
// session without dual-write or failover logic.
func (c *CQLClient) IsSingleCluster() bool {
	return c.sessionB == nil
}

// getSession returns the session for the given cluster.
// In single-cluster mode, always returns sessionA.
func (c *CQLClient) getSession(cluster ClusterID) cql.Session {
	if c.IsSingleCluster() || cluster == ClusterA {
		return c.sessionA
	}

	return c.sessionB
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
	writeFunc func(cql.Session) error,
) error {
	if c.closed.Load() {
		return types.ErrSessionClosed
	}

	// Single-cluster mode: direct execution, no dual-write logic
	if c.IsSingleCluster() {
		return writeFunc(c.sessionA)
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
	writeFunc func(cql.Session) error,
	drainA, _ bool,
) error {
	var session cql.Session
	var healthyCluster, drainingCluster ClusterID

	if drainA {
		session = c.sessionB
		healthyCluster = ClusterB
		drainingCluster = ClusterA
	} else {
		session = c.sessionA
		healthyCluster = ClusterA
		drainingCluster = ClusterB
	}

	// Execute write on the healthy cluster
	start := time.Now()
	err := writeFunc(session)
	elapsed := time.Since(start).Seconds()

	if err != nil {
		c.config.Metrics.IncWriteTotal(healthyCluster)
		c.config.Metrics.IncWriteError(healthyCluster)
		c.config.Metrics.ObserveWriteDuration(healthyCluster, elapsed)
		return err
	}

	c.config.Metrics.IncWriteTotal(healthyCluster)
	c.config.Metrics.ObserveWriteDuration(healthyCluster, elapsed)

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
		if enqueueErr := c.config.Replayer.Enqueue(ctx, payload); enqueueErr == nil {
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
	writeFunc func(cql.Session) error,
) error {
	// Dual-cluster mode: concurrent writes with replay support
	var startA, startB time.Time

	writeA := func(_ context.Context) error {
		startA = time.Now()
		return writeFunc(c.sessionA)
	}
	writeB := func(_ context.Context) error {
		startB = time.Now()
		return writeFunc(c.sessionB)
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

	// Record metrics for both clusters
	now := time.Now()
	c.config.Metrics.IncWriteTotal(ClusterA)
	c.config.Metrics.ObserveWriteDuration(ClusterA, now.Sub(startA).Seconds())
	if errA != nil {
		c.config.Metrics.IncWriteError(ClusterA)
	}

	c.config.Metrics.IncWriteTotal(ClusterB)
	c.config.Metrics.ObserveWriteDuration(ClusterB, now.Sub(startB).Seconds())
	if errB != nil {
		c.config.Metrics.IncWriteError(ClusterB)
	}

	// Handle results
	if errA == nil && errB == nil {
		// Both succeeded
		return nil
	}

	if errA != nil && errB != nil {
		// Both failed
		return &types.DualClusterError{
			ErrorA: errA,
			ErrorB: errB,
		}
	}

	// Partial failure - enqueue for replay
	if c.config.Replayer != nil {
		var targetCluster ClusterID
		var failedErr error
		if errA != nil {
			targetCluster = ClusterA
			failedErr = errA
		} else {
			targetCluster = ClusterB
			failedErr = errB
		}

		payload := types.ReplayPayload{
			TargetCluster:   targetCluster,
			Query:           wc.statement,
			Args:            wc.args,
			IsBatch:         wc.isBatch,
			BatchType:       wc.batchType,
			BatchStatements: wc.toBatchStatements(), // Lazy conversion
			Timestamp:       wc.timestamp,
			Priority:        wc.priority,
		}

		// Best effort enqueue - don't fail the operation if replay fails
		if enqueueErr := c.config.Replayer.Enqueue(ctx, payload); enqueueErr == nil {
			c.config.Metrics.IncReplayEnqueued(targetCluster)
			c.config.Logger.Warn("write failed on cluster, enqueued for replay",
				"cluster", c.clusterName(targetCluster),
				"error", failedErr.Error(),
			)
		} else {
			c.config.Metrics.IncReplayDropped(targetCluster)
			c.config.Logger.Error("failed to enqueue write for replay",
				"cluster", c.clusterName(targetCluster),
				"writeError", failedErr.Error(),
				"enqueueError", enqueueErr.Error(),
			)
			if c.config.OnReplayDropped != nil {
				c.config.OnReplayDropped(payload, enqueueErr)
			}
		}
	}

	// Partial success is still success from caller's perspective
	return nil
}

// recordReadSuccess records a successful read, using latency-aware recording if supported.
func (c *CQLClient) recordReadSuccess(cluster ClusterID, elapsed float64) {
	if c.config.ReadStrategy != nil {
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
func (c *CQLClient) executeRead(ctx context.Context, readFunc func(cql.Session) error) error {
	if c.closed.Load() {
		return types.ErrSessionClosed
	}

	// Single-cluster mode: direct execution, no failover
	if c.IsSingleCluster() {
		start := time.Now()
		err := readFunc(c.sessionA)
		elapsed := time.Since(start).Seconds()

		c.config.Metrics.IncReadTotal(ClusterA)
		c.config.Metrics.ObserveReadDuration(ClusterA, elapsed)
		if err != nil {
			c.config.Metrics.IncReadError(ClusterA)
		}

		return err
	}

	// Dual-cluster mode: sticky routing with failover

	// Check drain states
	drainA, drainB := c.getDrainStates()

	// Select cluster using read strategy
	var selectedCluster ClusterID
	if c.config.ReadStrategy != nil {
		selectedCluster = c.config.ReadStrategy.Select(ctx)
	} else {
		selectedCluster = ClusterA // Default to A
	}

	// Override selection if the selected cluster is draining
	if c.clusterIsDraining(selectedCluster, drainA, drainB) {
		alternativeCluster := c.alternativeCluster(selectedCluster)
		if !c.clusterIsDraining(alternativeCluster, drainA, drainB) {
			// Failover to non-draining cluster
			selectedCluster = alternativeCluster
		}
		// If both are draining, proceed with original selection (best effort)
	}

	// Try primary selection
	// Note: Timeouts should be configured on the underlying gocql session.
	session := c.getSession(selectedCluster)
	start := time.Now()
	err := readFunc(session)
	elapsed := time.Since(start).Seconds()

	c.config.Metrics.IncReadTotal(selectedCluster)
	c.config.Metrics.ObserveReadDuration(selectedCluster, elapsed)

	if err == nil {
		c.recordReadSuccess(selectedCluster, elapsed)

		return nil
	}

	// Primary failed - record error and failure
	c.config.Metrics.IncReadError(selectedCluster)

	if c.config.FailoverPolicy != nil {
		c.config.FailoverPolicy.RecordFailure(selectedCluster)

		// FailoverPolicy acts as a hard guardrail - if it says no, don't even
		// consult ReadStrategy for alternatives
		if !c.config.FailoverPolicy.ShouldFailover(selectedCluster, err) {
			return err
		}
	}

	// Policy allows failover (or no policy configured) - ask strategy for alternative
	var alternativeCluster ClusterID
	var shouldFailover bool

	if c.config.ReadStrategy != nil {
		alternativeCluster, shouldFailover = c.config.ReadStrategy.OnFailure(selectedCluster, err)
	} else {
		// No read strategy - use simple failover logic
		alternativeCluster = c.alternativeCluster(selectedCluster)
		shouldFailover = true
	}

	// Don't failover to a draining cluster unless we came from a draining cluster too
	// (both clusters draining = best effort)
	if shouldFailover && c.clusterIsDraining(alternativeCluster, drainA, drainB) {
		if !c.clusterIsDraining(selectedCluster, drainA, drainB) {
			// Original wasn't draining but alternative is - don't failover
			shouldFailover = false
		}
	}

	if !shouldFailover {
		return err
	}

	// Record failover event
	c.config.Metrics.IncFailoverTotal(selectedCluster, alternativeCluster)
	c.config.Logger.Warn("read failed, failing over to alternative cluster",
		"fromCluster", c.clusterName(selectedCluster),
		"toCluster", c.clusterName(alternativeCluster),
		"error", err.Error(),
	)

	// Try alternative cluster
	alternativeSession := c.getSession(alternativeCluster)
	startSecondary := time.Now()
	errSecondary := readFunc(alternativeSession)
	elapsedSecondary := time.Since(startSecondary).Seconds()

	c.config.Metrics.IncReadTotal(alternativeCluster)
	c.config.Metrics.ObserveReadDuration(alternativeCluster, elapsedSecondary)

	if errSecondary == nil {
		c.recordReadSuccess(alternativeCluster, elapsedSecondary)

		return nil
	}

	// Record failure on alternative too
	c.config.Metrics.IncReadError(alternativeCluster)

	if c.config.FailoverPolicy != nil {
		c.config.FailoverPolicy.RecordFailure(alternativeCluster)
	}

	// Both clusters failed - return combined error with both errors for diagnosis
	if selectedCluster == ClusterA {
		return &types.DualClusterError{
			ErrorA: err,
			ErrorB: errSecondary,
		}
	}

	return &types.DualClusterError{
		ErrorA: errSecondary,
		ErrorB: err,
	}
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

	wc := writeContext{
		statement: q.statement,
		args:      q.values,
		timestamp: ts,
		priority:  PriorityHigh, // Default to high priority for individual queries
	}

	return q.client.executeWriteWithReplay(ctx, wc, func(session cql.Session) error {
		query := session.Query(q.statement, q.values...)
		query = q.applyConfig(query)
		// Important for writes to generate the timestamp on the client side
		// to ensure consistency across clusters
		query = query.WithTimestamp(ts)

		return query.Exec()
	})
}

func (q *cqlQuery) Scan(dest ...any) error {
	return q.ScanContext(q.getContext(), dest...)
}

func (q *cqlQuery) ScanContext(ctx context.Context, dest ...any) error {
	return q.client.executeRead(ctx, func(session cql.Session) error {
		query := session.Query(q.statement, q.values...)
		query = q.applyConfig(query)
		query = query.WithContext(ctx)

		return query.Scan(dest...)
	})
}

func (q *cqlQuery) Iter() Iter {
	return q.IterContext(q.getContext())
}

func (q *cqlQuery) IterContext(ctx context.Context) Iter {
	// For Iter, we need to handle failover differently since we return an iterator
	// We'll attempt to get an iterator from the preferred cluster
	var selectedCluster ClusterID
	if q.client.config.ReadStrategy != nil {
		selectedCluster = q.client.config.ReadStrategy.Select(ctx)
	} else {
		selectedCluster = ClusterA
	}

	session := q.client.getSession(selectedCluster)
	query := session.Query(q.statement, q.values...)
	query = q.applyConfig(query)
	query = query.WithContext(ctx)

	return &cqlIter{
		iter:    query.Iter(),
		client:  q.client,
		cluster: selectedCluster,
	}
}

func (q *cqlQuery) MapScan(m map[string]any) error {
	return q.MapScanContext(q.getContext(), m)
}

func (q *cqlQuery) MapScanContext(ctx context.Context, m map[string]any) error {
	return q.client.executeRead(ctx, func(session cql.Session) error {
		query := session.Query(q.statement, q.values...)
		query = q.applyConfig(query)
		query = query.WithContext(ctx)

		return query.MapScan(m)
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
	var selectedCluster ClusterID
	if q.client.config.ReadStrategy != nil {
		selectedCluster = q.client.config.ReadStrategy.Select(ctx)
	} else {
		selectedCluster = ClusterA
	}

	session := q.client.getSession(selectedCluster)
	query := session.Query(q.statement, q.values...)
	query = q.applyConfig(query)
	query = query.WithContext(ctx)

	return query.ScanCAS(dest...)
}

// MapScanCAS executes a lightweight transaction and scans the result into a map.
// CAS operations are executed on a single cluster and are NOT replicated.
func (q *cqlQuery) MapScanCAS(dest map[string]any) (applied bool, err error) {
	return q.MapScanCASContext(q.getContext(), dest)
}

// MapScanCASContext executes a lightweight transaction with context and scans into a map.
// CAS operations are executed on a single cluster and are NOT replicated.
func (q *cqlQuery) MapScanCASContext(ctx context.Context, dest map[string]any) (applied bool, err error) {
	var selectedCluster ClusterID
	if q.client.config.ReadStrategy != nil {
		selectedCluster = q.client.config.ReadStrategy.Select(ctx)
	} else {
		selectedCluster = ClusterA
	}

	session := q.client.getSession(selectedCluster)
	query := session.Query(q.statement, q.values...)
	query = q.applyConfig(query)
	query = query.WithContext(ctx)

	return query.MapScanCAS(dest)
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

func (b *cqlBatch) Exec() error {
	return b.ExecContext(b.getContext())
}

func (b *cqlBatch) ExecContext(ctx context.Context) error {
	ts := b.getTimestamp()

	wc := writeContext{
		statement:    "", // Empty for batch
		args:         nil,
		timestamp:    ts,
		priority:     PriorityHigh,
		isBatch:      true,
		batchType:    b.kind,
		batchEntries: b.entries, // Pass directly, convert lazily if needed for replay
	}

	return b.client.executeWriteWithReplay(ctx, wc, func(session cql.Session) error {
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
		batch = batch.WithContext(ctx)

		return batch.Exec()
	})
}

// IterContext executes the batch and returns an iterator for the results.
func (b *cqlBatch) IterContext(ctx context.Context) Iter {
	ts := b.getTimestamp()

	var selectedCluster ClusterID
	if b.client.config.ReadStrategy != nil {
		selectedCluster = b.client.config.ReadStrategy.Select(ctx)
	} else {
		selectedCluster = ClusterA
	}

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
	batch = batch.WithContext(ctx)

	return &cqlIter{
		iter:    batch.IterContext(ctx),
		client:  b.client,
		cluster: selectedCluster,
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

	var selectedCluster ClusterID
	if b.client.config.ReadStrategy != nil {
		selectedCluster = b.client.config.ReadStrategy.Select(ctx)
	} else {
		selectedCluster = ClusterA
	}

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
	batch = batch.WithContext(ctx)

	applied, cqlItr, err := batch.ExecCAS(dest...)
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

	var selectedCluster ClusterID
	if b.client.config.ReadStrategy != nil {
		selectedCluster = b.client.config.ReadStrategy.Select(ctx)
	} else {
		selectedCluster = ClusterA
	}

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
	batch = batch.WithContext(ctx)

	applied, cqlItr, err := batch.MapExecCAS(dest)
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
	iter    cql.Iter
	client  *CQLClient
	cluster ClusterID
}

func (i *cqlIter) Scan(dest ...any) bool {
	return i.iter.Scan(dest...)
}

func (i *cqlIter) Close() error {
	err := i.iter.Close()
	if err == nil && i.client.config.ReadStrategy != nil {
		i.client.config.ReadStrategy.OnSuccess(i.cluster)
	}
	return err
}

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
//
// Returns:
//   - cql.Session: The raw session for cluster A
func (c *CQLClient) SessionA() cql.Session {
	return c.sessionA
}

// SessionB returns the underlying session for cluster B.
//
// Use with caution - direct access bypasses Helix's dual-cluster logic.
//
// Returns:
//   - cql.Session: The raw session for cluster B
func (c *CQLClient) SessionB() cql.Session {
	return c.sessionB
}

// Config returns the current client configuration.
//
// Returns:
//   - *ClientConfig: The client's configuration
func (c *CQLClient) Config() *ClientConfig {
	return c.config
}
