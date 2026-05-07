// Package helix provides a high-availability dual-database client library
// designed to support "Shared Nothing" architecture.
//
// Helix provides robustness through active-active dual writes, sticky reads,
// and asynchronous reconciliation for independent Cassandra clusters.
//
// # Key Features
//
//   - Dual Active-Active Writes: Concurrent writes to two independent clusters
//   - Sticky Read Routing: Per-client sticky reads to maximize cache hits
//   - Active Failover: Immediate failover to secondary cluster on read failures
//   - Replay System: Asynchronous reconciliation via in-memory queue or NATS JetStream
//   - Session Refresh: Manual or automatic recovery from permanently-dead sessions
//     (cluster restart with port reassignment, DNS rotation) without rebuilding
//     the client; see docs/session-refresh.md
//   - Drop-in Replacement: Interface-based design mirrors gocql API
//
// # Basic Usage
//
//	// Create sessions for both clusters
//	sessionA := v1.NewSession(clusterA.CreateSession())
//	sessionB := v1.NewSession(clusterB.CreateSession())
//
//	// Create Helix client
//	client, err := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithReadStrategy(policy.NewStickyRead()),
//	    helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
//	// Use like a normal gocql session
//	err = client.Query("INSERT INTO users (id, name) VALUES (?, ?)", id, name).Exec()
//
// # Error Handling
//
// Helix uses standard Go errors with clear semantics for dual-cluster operations.
//
// # Write Operation Errors
//
// Write operations (Exec, ExecContext, batch.Exec) follow this error model:
//   - nil: At least one cluster succeeded (partial writes are queued for replay)
//   - error: Both clusters failed (operation completely failed)
//
// When both clusters fail, a types.DualClusterError is returned:
//
//	err := client.Query("INSERT INTO ...").Exec()
//	if err != nil {
//	    var dualErr *types.DualClusterError
//	    if errors.As(err, &dualErr) {
//	        // Both clusters failed, inspect individual errors
//	        log.Printf("Cluster A: %v", dualErr.ErrorA)
//	        log.Printf("Cluster B: %v", dualErr.ErrorB)
//	    }
//	}
//
// # Sentinel Errors
//
// Helix defines several sentinel errors for specific scenarios:
//
//   - types.ErrSessionClosed: Operation attempted on closed client
//   - types.ErrReplayQueueFull: Replay queue at capacity, cannot enqueue failed write
//   - types.ErrBothClustersDraining: Both clusters in drain mode, writes rejected
//   - types.ErrWriteAsync: Write sent async to degraded cluster (AdaptiveDualWrite only)
//   - types.ErrWriteDropped: Write dropped due to concurrency limit (AdaptiveDualWrite only)
//
// Check for sentinel errors using errors.Is:
//
//	if errors.Is(err, types.ErrSessionClosed) {
//	    // Handle closed session
//	}
//
// # Wrapped Errors
//
// Cluster-specific errors are wrapped in types.ClusterError:
//
//	var clusterErr *types.ClusterError
//	if errors.As(err, &clusterErr) {
//	    log.Printf("Cluster %s failed during %s: %v",
//	        clusterErr.Cluster, clusterErr.Operation, clusterErr.Cause)
//	}
//
// # Context and Timeouts
//
// Context usage follows Go idioms. Two equivalent patterns are supported:
//
//	// Pattern 1: Direct context method (preferred for simple cases)
//	err := client.Query("SELECT ...").ExecContext(ctx)
//
//	// Pattern 2: Method chaining (useful with multiple options)
//	err := client.Query("SELECT ...").
//	    Consistency(helix.Quorum).
//	    WithContext(ctx).
//	    Exec()
//
// Both patterns respect context cancellation and deadlines. For dual-writes,
// each cluster operation gets an independent timeout context to prevent one
// slow cluster from blocking the other.
//
// # Idempotency and Timestamps
//
// Helix uses client-generated timestamps to ensure replay operations are idempotent.
// Without proper timestamps:
//   - Replayed writes may overwrite newer data
//   - Last-write-wins semantics may be violated
//
// Always configure a timestamp provider or use WithTimestamp():
//
//	helix.WithTimestampProvider(func() int64 {
//	    return time.Now().UnixMicro()
//	})
//
// # Session Lifecycle and Refresh
//
// helix.NewCQLClient takes two cql.Session references and uses them for the
// lifetime of the client by default. When a real cluster's endpoint changes
// (restart with port reassignment, DNS rotation, host migration), the
// driver's internal reconnect cannot recover and every subsequent op fails.
//
// Helix provides three layers for handling this:
//
//   - SwapSession (manual, full caller control over old-session lifecycle)
//   - RefreshSession (manual, uses a registered SessionRefresher and closes
//     the old session)
//   - WithAutoRefresh (background detector that invokes the SessionRefresher
//     when Helix observes a cluster's session is permanently dead)
//
// All three preserve the decoupling principle: Helix never imports a specific
// gocql driver, so SessionRefresher is a caller-supplied factory that builds
// a fresh session and wraps it with the v1 or v2 adapter.
//
// Quick start with auto-refresh:
//
//	client, err := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithSessionRefresher(func(ctx context.Context, cluster helix.ClusterID, lastErr error) (cql.Session, error) {
//	        return v1.NewSession(rebuildGocqlSession(cluster)), nil
//	    }),
//	    helix.WithAutoRefresh(),
//	)
//
// See docs/session-refresh.md for the full guide.
//
// # CAS/LWT Warning
//
// Lightweight Transactions (INSERT ... IF NOT EXISTS, ScanCAS, etc.) are NOT safe
// in a shared-nothing dual-cluster architecture. Helix does not guarantee correctness
// for CAS/LWT operations.
package helix
