package helix

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"time"

	sqladapter "github.com/arloliu/helix/adapter/sql"
	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/types"
)

// SQLClient is the Helix SQL client for single or dual-database operations.
//
// It wraps one or two SQL databases and orchestrates writes according to
// configured strategies. When only one database is provided (secondary is nil),
// the client operates in single-database mode with pass-through behavior.
//
// Single-database mode is ideal for:
//   - Migrating existing applications to Helix incrementally
//   - Development and testing environments
//   - Applications that don't need dual-database redundancy yet
//
// Limitations (dual-database mode):
//   - Single-statement non-transactional operations only
//   - No transaction support across databases
//   - Best-effort semantics (no strong consistency guarantees)
//   - Write order determined by arrival time (no USING TIMESTAMP like Cassandra)
type SQLClient struct {
	primary   sqladapter.DB
	secondary sqladapter.DB // nil for single-database mode
	config    *ClientConfig
	closed    atomic.Bool
}

// NewSQLClient creates a new Helix SQL client.
//
// The client supports two modes:
//   - Single-database mode: Pass secondary as nil. Operations are executed directly
//     on primary without dual-write or failover logic. This provides a drop-in
//     replacement for existing single-database applications.
//   - Dual-database mode: Pass both databases. Operations use configured strategies
//     for dual writes, sticky reads, and failover.
//
// If a ReplayWorker is configured, it will be started automatically.
// The worker will be stopped when Close() is called.
//
// Parameters:
//   - primary: Primary database connection (required)
//   - secondary: Secondary database connection (optional, nil for single-database mode)
//   - opts: Optional configuration options
//
// Returns:
//   - *SQLClient: A new SQL client
//   - error: ErrNilSession if primary is nil, or error from worker start
func NewSQLClient(primary, secondary sqladapter.DB, opts ...Option) (*SQLClient, error) {
	if primary == nil {
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

	client := &SQLClient{
		primary:   primary,
		secondary: secondary,
		config:    config,
	}

	// Start replay worker if configured
	if config.ReplayWorker != nil {
		if err := config.ReplayWorker.Start(); err != nil {
			return nil, err
		}
	}

	return client, nil
}

// NewSQLClientFromDB creates a new Helix SQL client from standard *sql.DB connections.
//
// This is a convenience constructor that wraps the databases in adapters.
// Pass secondary as nil for single-database mode.
//
// Parameters:
//   - primary: Primary *sql.DB connection (required)
//   - secondary: Secondary *sql.DB connection (optional, nil for single-database mode)
//   - opts: Optional configuration options
//
// Returns:
//   - *SQLClient: A new SQL client
//   - error: ErrNilSession if primary is nil
func NewSQLClientFromDB(primary, secondary *sql.DB, opts ...Option) (*SQLClient, error) {
	if primary == nil {
		return nil, types.ErrNilSession
	}

	var secondaryAdapter sqladapter.DB
	if secondary != nil {
		secondaryAdapter = sqladapter.NewDBAdapter(secondary)
	}

	return NewSQLClient(
		sqladapter.NewDBAdapter(primary),
		secondaryAdapter,
		opts...,
	)
}

// ExecContext executes a write query.
//
// In single-database mode, the query is executed directly on the primary database.
// In dual-database mode, the query is executed on both databases concurrently.
// Success is defined as at least one database succeeding.
// Failed writes are enqueued for replay if a Replayer is configured.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - query: SQL statement to execute
//   - args: Arguments for the query
//
// Returns:
//   - sql.Result: Result from the successful write (primary preferred)
//   - error: nil if at least one succeeds, error if both fail
func (c *SQLClient) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if c.closed.Load() {
		return nil, types.ErrSessionClosed
	}

	// Single-database mode: direct execution
	if c.IsSingleCluster() {
		start := time.Now()
		result, err := c.primary.ExecContext(ctx, query, args...)
		elapsed := time.Since(start).Seconds()

		c.config.Metrics.IncWriteTotal(ClusterA)
		c.config.Metrics.ObserveWriteDuration(ClusterA, elapsed)
		if err != nil {
			c.config.Metrics.IncWriteError(ClusterA)
		}

		return result, err
	}

	// Dual-database mode: concurrent writes with replay support
	var resultA, resultB sql.Result
	var errA, errB error
	var startA, startB time.Time
	var wg sync.WaitGroup

	wg.Go(func() {
		startA = time.Now()
		resultA, errA = c.primary.ExecContext(ctx, query, args...)
	})

	wg.Go(func() {
		startB = time.Now()
		resultB, errB = c.secondary.ExecContext(ctx, query, args...)
	})

	wg.Wait()

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
		// Both succeeded - return primary result
		return resultA, nil
	}

	if errA != nil && errB != nil {
		// Both failed
		return nil, &types.DualClusterError{
			ErrorA: errA,
			ErrorB: errB,
		}
	}

	// Partial failure - enqueue for replay if configured
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
			TargetCluster: targetCluster,
			Query:         query,
			Args:          args,
			Timestamp:     c.config.TimestampProvider(),
			Priority:      types.PriorityHigh,
		}

		// Best effort enqueue - don't fail the operation if replay fails
		if enqueueErr := c.config.Replayer.Enqueue(ctx, payload); enqueueErr == nil {
			c.config.Metrics.IncReplayEnqueued(targetCluster)
			c.config.Logger.Warn("write failed on database, enqueued for replay",
				"cluster", c.clusterName(targetCluster),
				"error", failedErr.Error(),
			)
		} else {
			c.config.Logger.Error("failed to enqueue write for replay",
				"cluster", c.clusterName(targetCluster),
				"writeError", failedErr.Error(),
				"enqueueError", enqueueErr.Error(),
			)
		}
	}

	// Partial success - return the successful result
	if errA == nil {
		return resultA, nil
	}

	return resultB, nil
}

// Exec executes a write query using a background context.
//
// Parameters:
//   - query: SQL statement to execute
//   - args: Arguments for the query
//
// Returns:
//   - sql.Result: Result from the successful write
//   - error: nil if at least one succeeds, error if both fail
func (c *SQLClient) Exec(query string, args ...any) (sql.Result, error) {
	return c.ExecContext(context.Background(), query, args...)
}

// QueryContext executes a read query.
//
// In single-database mode, the query is executed directly on the primary database.
// In dual-database mode, the query is routed to the preferred database based on
// the ReadStrategy. On failure, it may failover to the secondary.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - query: SQL statement to execute
//   - args: Arguments for the query
//
// Returns:
//   - *sql.Rows: Result rows from the query (caller must close)
//   - error: Error if the query fails on all databases
func (c *SQLClient) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if c.closed.Load() {
		return nil, types.ErrSessionClosed
	}

	// Single-database mode: direct execution
	if c.IsSingleCluster() {
		start := time.Now()
		rows, err := c.primary.QueryContext(ctx, query, args...)
		elapsed := time.Since(start).Seconds()

		c.config.Metrics.IncReadTotal(ClusterA)
		c.config.Metrics.ObserveReadDuration(ClusterA, elapsed)
		if err != nil {
			c.config.Metrics.IncReadError(ClusterA)
		}

		return rows, err
	}

	// Dual-database mode: sticky routing with failover

	// Select database using read strategy
	var selectedCluster ClusterID
	if c.config.ReadStrategy != nil {
		selectedCluster = c.config.ReadStrategy.Select(ctx)
	} else {
		selectedCluster = ClusterA
	}

	// Try primary selection
	db := c.getDB(selectedCluster)
	start := time.Now()
	rows, err := db.QueryContext(ctx, query, args...)
	elapsed := time.Since(start).Seconds()

	c.config.Metrics.IncReadTotal(selectedCluster)
	c.config.Metrics.ObserveReadDuration(selectedCluster, elapsed)

	if err == nil {
		if c.config.ReadStrategy != nil {
			c.config.ReadStrategy.OnSuccess(selectedCluster)
		}

		return rows, nil
	}

	// Primary failed - record error
	c.config.Metrics.IncReadError(selectedCluster)

	// Check if we should failover
	if c.config.ReadStrategy == nil {
		return nil, err
	}

	alternative, shouldFailover := c.config.ReadStrategy.OnFailure(selectedCluster, err)
	if !shouldFailover {
		return nil, err
	}

	// Record failover event
	c.config.Metrics.IncFailoverTotal(selectedCluster, alternative)
	c.config.Logger.Warn("read failed, failing over to alternative database",
		"fromCluster", c.clusterName(selectedCluster),
		"toCluster", c.clusterName(alternative),
		"error", err.Error(),
	)

	// Try alternative
	altDB := c.getDB(alternative)
	startAlt := time.Now()
	altRows, altErr := altDB.QueryContext(ctx, query, args...)
	elapsedAlt := time.Since(startAlt).Seconds()

	c.config.Metrics.IncReadTotal(alternative)
	c.config.Metrics.ObserveReadDuration(alternative, elapsedAlt)

	if altErr == nil {
		c.config.ReadStrategy.OnSuccess(alternative)
	} else {
		c.config.Metrics.IncReadError(alternative)
	}

	return altRows, altErr
}

// Query executes a read query using a background context.
//
// Parameters:
//   - query: SQL statement to execute
//   - args: Arguments for the query
//
// Returns:
//   - *sql.Rows: Result rows from the query
//   - error: Error if the query fails on all databases
func (c *SQLClient) Query(query string, args ...any) (*sql.Rows, error) {
	return c.QueryContext(context.Background(), query, args...)
}

// QueryRowContext executes a read query expecting at most one row.
//
// In single-database mode, the query is executed directly on the primary database.
// In dual-database mode, the query is routed based on the ReadStrategy.
//
// If the client is closed, returns a Row that will return sql.ErrNoRows on Scan.
// Callers should check IsClosed() before calling if they need to distinguish
// between "no rows" and "client closed" scenarios.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - query: SQL statement to execute
//   - args: Arguments for the query
//
// Returns:
//   - *sql.Row: Single row result (never nil)
func (c *SQLClient) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if c.closed.Load() {
		// Return a Row that will error on Scan with context.Canceled
		// This prevents nil pointer panics while signaling an error condition
		canceledCtx, cancel := context.WithCancel(ctx)
		cancel() // Immediately cancel

		return c.primary.QueryRowContext(canceledCtx, query, args...)
	}

	// Single-database mode: direct execution
	if c.IsSingleCluster() {
		return c.primary.QueryRowContext(ctx, query, args...)
	}

	// Dual-database mode: route based on strategy
	var selectedCluster ClusterID
	if c.config.ReadStrategy != nil {
		selectedCluster = c.config.ReadStrategy.Select(ctx)
	} else {
		selectedCluster = ClusterA
	}

	db := c.getDB(selectedCluster)

	return db.QueryRowContext(ctx, query, args...)
}

// QueryRow executes a read query expecting at most one row.
//
// Parameters:
//   - query: SQL statement to execute
//   - args: Arguments for the query
//
// Returns:
//   - *sql.Row: Single row result
func (c *SQLClient) QueryRow(query string, args ...any) *sql.Row {
	return c.QueryRowContext(context.Background(), query, args...)
}

// PingContext checks if the database(s) are reachable.
//
// In single-database mode, only the primary database is pinged.
// In dual-database mode, both databases are pinged concurrently.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//
// Returns:
//   - error: nil if at least one database responds, error if all fail
func (c *SQLClient) PingContext(ctx context.Context) error {
	if c.closed.Load() {
		return types.ErrSessionClosed
	}

	// Single-database mode: ping primary only
	if c.IsSingleCluster() {
		return c.primary.PingContext(ctx)
	}

	// Dual-database mode: ping both
	var errA, errB error
	var wg sync.WaitGroup

	wg.Go(func() {
		errA = c.primary.PingContext(ctx)
	})

	wg.Go(func() {
		errB = c.secondary.PingContext(ctx)
	})

	wg.Wait()

	if errA == nil || errB == nil {
		return nil
	}

	return &types.DualClusterError{
		ErrorA: errA,
		ErrorB: errB,
	}
}

// Ping checks if the database(s) are reachable.
//
// Returns:
//   - error: nil if at least one database responds, error if all fail
func (c *SQLClient) Ping() error {
	return c.PingContext(context.Background())
}

// Close closes database connection(s) and stops the replay worker.
//
// The replay worker is stopped first to allow any pending replays to complete.
// After Close is called, the client cannot be reused.
func (c *SQLClient) Close() error {
	if c.closed.Swap(true) {
		return nil // Already closed
	}

	// Stop replay worker first
	if c.config.ReplayWorker != nil {
		c.config.ReplayWorker.Stop()
	}

	var errA, errB error

	if c.primary != nil {
		errA = c.primary.Close()
	}

	if c.secondary != nil {
		errB = c.secondary.Close()
	}

	if errA != nil && errB != nil {
		return &types.DualClusterError{
			ErrorA: errA,
			ErrorB: errB,
		}
	}

	if errA != nil {
		return errA
	}

	return errB
}

// IsSingleCluster returns true if the client is operating in single-database mode.
//
// In single-database mode, all operations are executed directly on the primary
// database without dual-write or failover logic.
func (c *SQLClient) IsSingleCluster() bool {
	return c.secondary == nil
}

// getDB returns the database for the given cluster.
// In single-database mode, always returns primary.
func (c *SQLClient) getDB(cluster ClusterID) sqladapter.DB {
	if c.IsSingleCluster() || cluster == ClusterA {
		return c.primary
	}

	return c.secondary
}

// clusterName returns the display name for the given cluster.
func (c *SQLClient) clusterName(cluster ClusterID) string {
	return c.config.ClusterNames.Name(cluster)
}
