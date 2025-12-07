// Package main demonstrates the Helix replay system for handling partial write failures.
//
// This example shows how to set up:
// 1. In-memory replayer with embedded worker (development pattern)
// 2. NATS JetStream replayer for durable replay (production pattern)
//
// # Prerequisites
//
// For NATS example:
// - NATS server running with JetStream enabled (nats-server -js)
// - Two Cassandra clusters running
//
// # Running
//
//	go run main.go
package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/arloliu/helix"
	v1 "github.com/arloliu/helix/adapter/cql/v1"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
	"github.com/gocql/gocql"
)

func main() {
	fmt.Println("=== Helix Replay System Demo ===")
	fmt.Println()

	// Run the in-memory replay demo (simulated)
	demoInMemoryReplay()

	fmt.Println()
	fmt.Println("For NATS JetStream example, see demoNATSReplay() function")
	fmt.Println("Requires: nats-server -js running on localhost:4222")
}

// demoInMemoryReplay demonstrates the in-memory replay pattern.
// This is suitable for development and testing.
func demoInMemoryReplay() {
	fmt.Println("--- In-Memory Replay Demo ---")

	// For this demo, we'll simulate without real Cassandra
	// In production, you would create real gocql sessions

	// Track replay statistics
	var replayAttempts atomic.Int64
	var replaySuccesses atomic.Int64

	// Create in-memory replayer
	replayer := replay.NewMemoryReplayer(
		replay.WithQueueCapacity(10000),
	)

	// Create execute function for replay worker
	executeFunc := func(ctx context.Context, payload types.ReplayPayload) error {
		replayAttempts.Add(1)

		// Simulate replay execution
		fmt.Printf("  Replaying to cluster %s: %s\n", payload.TargetCluster, payload.Query)

		// In production, you would execute against the actual cluster:
		// session := getSession(payload.TargetCluster)
		// if payload.IsBatch {
		//     batch := session.Batch(payload.BatchType)
		//     for _, stmt := range payload.BatchStatements {
		//         batch = batch.Query(stmt.Query, stmt.Args...)
		//     }
		//     batch = batch.WithTimestamp(payload.Timestamp)
		//     return batch.Exec()
		// }
		// return session.Query(payload.Query, payload.Args...).
		//     WithTimestamp(payload.Timestamp).
		//     Exec()

		replaySuccesses.Add(1)
		return nil
	}

	// Create worker with callbacks for observability
	worker := replay.NewMemoryWorker(replayer, executeFunc,
		replay.WithPollInterval(100*time.Millisecond),
		replay.WithExecuteTimeout(5*time.Second),
		replay.WithOnSuccess(func(p types.ReplayPayload) {
			fmt.Printf("  ✓ Replay succeeded: cluster=%s\n", p.TargetCluster)
		}),
		replay.WithOnError(func(p types.ReplayPayload, err error, attempt int) {
			fmt.Printf("  ✗ Replay failed: cluster=%s attempt=%d err=%v\n",
				p.TargetCluster, attempt, err)
		}),
	)

	// Start the worker
	if err := worker.Start(); err != nil {
		log.Fatalf("Failed to start worker: %v", err)
	}
	defer worker.Stop()

	fmt.Println("Worker started, simulating partial failures...")

	// Simulate some partial write failures by enqueuing directly
	ctx := context.Background()

	// Simulate: write succeeded on A, failed on B
	payload1 := types.ReplayPayload{
		TargetCluster: types.ClusterB,
		Query:         "INSERT INTO users (id, name) VALUES (?, ?)",
		Args:          []any{"user-1", "Alice"},
		Timestamp:     time.Now().UnixMicro(),
		Priority:      types.PriorityHigh,
	}
	if err := replayer.Enqueue(ctx, payload1); err != nil {
		log.Printf("Failed to enqueue: %v", err)
	}

	// Simulate: write succeeded on B, failed on A
	payload2 := types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT INTO users (id, name) VALUES (?, ?)",
		Args:          []any{"user-2", "Bob"},
		Timestamp:     time.Now().UnixMicro(),
		Priority:      types.PriorityHigh,
	}
	if err := replayer.Enqueue(ctx, payload2); err != nil {
		log.Printf("Failed to enqueue: %v", err)
	}

	// Simulate: batch write failed on B
	payload3 := types.ReplayPayload{
		TargetCluster: types.ClusterB,
		IsBatch:       true,
		BatchType:     types.UnloggedBatch,
		BatchStatements: []types.BatchStatement{
			{Query: "INSERT INTO users (id, name) VALUES (?, ?)", Args: []any{"user-3", "Charlie"}},
			{Query: "INSERT INTO users (id, name) VALUES (?, ?)", Args: []any{"user-4", "Diana"}},
		},
		Timestamp: time.Now().UnixMicro(),
		Priority:  types.PriorityHigh,
	}
	if err := replayer.Enqueue(ctx, payload3); err != nil {
		log.Printf("Failed to enqueue: %v", err)
	}

	// Wait for worker to process
	time.Sleep(500 * time.Millisecond)

	fmt.Printf("\nReplay Statistics:\n")
	fmt.Printf("  Attempts:  %d\n", replayAttempts.Load())
	fmt.Printf("  Successes: %d\n", replaySuccesses.Load())
	fmt.Printf("  Queue Len: %d\n", replayer.Len())
}

// demoNATSReplay demonstrates the NATS JetStream replay pattern.
// This is the recommended pattern for production deployments.
//
//nolint:unused // This is example code for documentation
func demoNATSReplay() {
	fmt.Println("--- NATS JetStream Replay Demo ---")

	// Connect to NATS with JetStream
	// nc, err := nats.Connect("nats://localhost:4222")
	// if err != nil {
	//     log.Fatalf("Failed to connect to NATS: %v", err)
	// }
	// defer nc.Close()
	//
	// js, err := jetstream.New(nc)
	// if err != nil {
	//     log.Fatalf("Failed to create JetStream context: %v", err)
	// }

	// Create gocql sessions
	clusterA := gocql.NewCluster("127.0.0.1:9042")
	clusterA.Keyspace = "helix_example"
	gocqlSessionA, err := clusterA.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to cluster A: %v", err)
	}
	defer gocqlSessionA.Close()

	clusterB := gocql.NewCluster("127.0.0.1:9043")
	clusterB.Keyspace = "helix_example"
	gocqlSessionB, err := clusterB.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to cluster B: %v", err)
	}
	defer gocqlSessionB.Close()

	// Wrap with Helix adapters
	sessionA := v1.NewSession(gocqlSessionA)
	sessionB := v1.NewSession(gocqlSessionB)

	// Create NATS replayer with durable storage
	// replayer, err := replay.NewNATSReplayer(js,
	//     replay.WithNATSStreamName("helix-replay"),
	//     replay.WithNATSSubjectPrefix("helix.replay"),
	//     replay.WithNATSMaxAge(24*time.Hour),
	//     replay.WithNATSMaxMsgs(1_000_000),
	//     replay.WithNATSReplicas(3),
	// )
	// if err != nil {
	//     log.Fatalf("Failed to create NATS replayer: %v", err)
	// }
	// defer replayer.Close()

	// Create execute function
	executeFunc := func(ctx context.Context, payload types.ReplayPayload) error {
		// Determine target session
		var session interface {
			Query(string, ...any) interface {
				WithTimestamp(int64) interface{ Exec() error }
			}
			Batch(types.BatchType) interface {
				Query(string, ...any) interface{ Query(string, ...any) any }
				WithTimestamp(int64) interface{ Exec() error }
			}
		}
		if payload.TargetCluster == types.ClusterA {
			_ = sessionA // session = sessionA
		} else {
			_ = sessionB // session = sessionB
		}
		_ = session

		// Execute the replay
		// if payload.IsBatch {
		//     batch := session.Batch(payload.BatchType)
		//     for _, stmt := range payload.BatchStatements {
		//         batch = batch.Query(stmt.Query, stmt.Args...)
		//     }
		//     batch = batch.WithTimestamp(payload.Timestamp)
		//     return batch.Exec()
		// }
		//
		// return session.Query(payload.Query, payload.Args...).
		//     WithTimestamp(payload.Timestamp).
		//     Exec()

		return nil
	}

	// For embedded worker pattern:
	// worker := replay.NewNATSWorker(replayer, executeFunc,
	//     replay.WithBatchSize(100),
	//     replay.WithPollInterval(500*time.Millisecond),
	//     replay.WithOnSuccess(func(p types.ReplayPayload) {
	//         log.Printf("Replay OK: cluster=%s", p.TargetCluster)
	//     }),
	//     replay.WithOnError(func(p types.ReplayPayload, err error, attempt int) {
	//         log.Printf("Replay FAIL: cluster=%s err=%v", p.TargetCluster, err)
	//     }),
	// )

	// Create Helix client with replayer and worker
	// client, err := helix.NewCQLClient(sessionA, sessionB,
	//     helix.WithReplayer(replayer),
	//     helix.WithReplayWorker(worker), // Auto-starts worker
	//     helix.WithReadStrategy(policy.NewStickyRead()),
	//     helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
	// )
	// if err != nil {
	//     log.Fatalf("Failed to create client: %v", err)
	// }
	// defer client.Close() // Auto-stops worker

	// For dedicated replay service pattern:
	// In application: only use WithReplayer (no worker)
	// In separate service: create worker and run it standalone

	_ = executeFunc
	fmt.Println("See docs/replay-system.md for complete examples")
}

// demoWithRealClusters shows how to use replay with real Cassandra clusters.
//
//nolint:unused // This is example code for documentation
func demoWithRealClusters() {
	// Create gocql sessions
	clusterA := gocql.NewCluster("127.0.0.1:9042")
	clusterA.Keyspace = "helix_example"
	gocqlSessionA, err := clusterA.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to cluster A: %v", err)
	}
	defer gocqlSessionA.Close()

	clusterB := gocql.NewCluster("127.0.0.1:9043")
	clusterB.Keyspace = "helix_example"
	gocqlSessionB, err := clusterB.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to cluster B: %v", err)
	}
	defer gocqlSessionB.Close()

	// Wrap with Helix adapters
	sessionA := v1.NewSession(gocqlSessionA)
	sessionB := v1.NewSession(gocqlSessionB)

	// Create replayer
	replayer := replay.NewMemoryReplayer()

	// Create execute function for worker
	executeFunc := func(ctx context.Context, payload types.ReplayPayload) error {
		var session interface {
			Query(string, ...any) interface {
				WithTimestamp(int64) interface{ Exec() error }
			}
			Batch(types.BatchType) interface {
				Query(string, ...any) interface{ Query(string, ...any) any }
				WithTimestamp(int64) interface{ Exec() error }
			}
		}
		if payload.TargetCluster == types.ClusterA {
			_ = sessionA // In real code: session = sessionA
		} else {
			_ = sessionB // In real code: session = sessionB
		}
		_ = session
		return nil
	}

	// Create worker
	worker := replay.NewMemoryWorker(replayer, executeFunc)

	// Create Helix client with both replayer and worker
	client, err := helix.NewCQLClient(sessionA, sessionB,
		helix.WithReplayer(replayer),
		helix.WithReplayWorker(worker), // Worker auto-starts
		helix.WithReadStrategy(policy.NewStickyRead()),
		helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
	)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close() // Worker auto-stops

	// Now use the client - partial failures are automatically replayed
	ctx := context.Background()
	userID := gocql.TimeUUID()

	err = client.Query(
		"INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
		userID, "Alice", "alice@example.com",
	).WithContext(ctx).Exec()

	if err != nil {
		// Both clusters failed
		log.Printf("Write failed on both clusters: %v", err)
	} else {
		// At least one cluster succeeded
		// If only one succeeded, the other is being replayed in the background
		fmt.Printf("Write succeeded, user ID: %s\n", userID)
	}
}
