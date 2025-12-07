// Package policy provides read strategies, write strategies, and failover policies
// for the helix dual-database client.
//
// # Read Strategies
//
// Read strategies determine how read operations are routed between clusters.
// All strategies implement the ReadStrategy interface:
//
//	type ReadStrategy interface {
//	    Select(ctx context.Context) types.ClusterID
//	}
//
// Available strategies:
//
//   - [StickyRead]: Routes all reads to a randomly selected cluster for cache affinity
//   - [PrimaryOnlyRead]: Always routes reads to ClusterA
//   - [RoundRobinRead]: Alternates between clusters for load distribution
//
// Example:
//
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithReadStrategy(policy.NewStickyRead()),
//	)
//
// # Write Strategies
//
// Write strategies determine how write operations are executed across clusters.
// All strategies implement the WriteStrategy interface:
//
//	type WriteStrategy interface {
//	    Execute(ctx context.Context, writeFunc WriteFunc) (errA, errB error)
//	}
//
// Available strategies:
//
//   - [ConcurrentDualWrite]: Writes to both clusters concurrently (default)
//   - [SyncDualWrite]: Writes to ClusterA first, then ClusterB
//   - [AdaptiveDualWrite]: Latency-aware writes with fire-and-forget for degraded clusters
//
// Example:
//
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
//	)
//
// For latency-aware dual writes that detect slow clusters:
//
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithWriteStrategy(policy.NewAdaptiveDualWrite(
//	        policy.WithAdaptiveDeltaThreshold(300 * time.Millisecond),
//	        policy.WithAdaptiveAbsoluteMax(2 * time.Second),
//	    )),
//	)
//
// # Failover Policies
//
// Failover policies control how the client responds to cluster failures.
// All policies implement the FailoverPolicy interface:
//
//	type FailoverPolicy interface {
//	    ShouldFailover(ctx context.Context, cluster types.ClusterID, err error) bool
//	    RecordSuccess(ctx context.Context, cluster types.ClusterID)
//	    RecordFailure(ctx context.Context, cluster types.ClusterID, err error)
//	}
//
// Available policies:
//
//   - [ActiveFailover]: Simple policy that fails over on any error
//   - [CircuitBreaker]: Prevents cascading failures with open/closed states
//   - [LatencyCircuitBreaker]: Circuit breaker that also tracks slow responses as failures
//
// Example:
//
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithFailoverPolicy(policy.NewActiveFailover()),
//	)
//
// For latency-aware failover that treats slow responses (>2s) as failures:
//
//	lcb := policy.NewLatencyCircuitBreaker(
//	    policy.WithLatencyAbsoluteMax(2 * time.Second),
//	    policy.WithLatencyThreshold(3), // 3 failures before circuit opens
//	)
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithFailoverPolicy(lcb),
//	)
//	// After each read, record latency:
//	// lcb.RecordLatency(cluster, latency)
package policy
