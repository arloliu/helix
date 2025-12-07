// Package main demonstrates basic usage of the Helix dual-database client library.
//
// This example shows how to set up a Helix CQL client with two Cassandra clusters
// and perform dual-write operations.
//
// # Prerequisites
//
// - Two Cassandra clusters running (e.g., on localhost:9042 and localhost:9043)
// - A keyspace and table created in both clusters
//
// # Running
//
//	go run main.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/arloliu/helix"
	v1 "github.com/arloliu/helix/adapter/cql/v1"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
	"github.com/gocql/gocql"
)

func main() {
	ctx := context.Background()

	// Configure cluster A
	clusterA := gocql.NewCluster("127.0.0.1:9042")
	clusterA.Keyspace = "helix_example"
	clusterA.Consistency = gocql.Quorum
	clusterA.Timeout = 10 * time.Second

	// Configure cluster B
	clusterB := gocql.NewCluster("127.0.0.1:9043")
	clusterB.Keyspace = "helix_example"
	clusterB.Consistency = gocql.Quorum
	clusterB.Timeout = 10 * time.Second

	// Create gocql sessions
	gocqlSessionA, err := clusterA.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to cluster A: %v", err)
	}
	defer gocqlSessionA.Close()

	gocqlSessionB, err := clusterB.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to cluster B: %v", err)
	}
	defer gocqlSessionB.Close()

	// Wrap with Helix adapters
	sessionA := v1.NewSession(gocqlSessionA)
	sessionB := v1.NewSession(gocqlSessionB)

	// Create a replayer for handling failed writes
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(1000))

	// Create the Helix client with configuration
	client, err := helix.NewCQLClient(sessionA, sessionB,
		helix.WithReadStrategy(policy.NewStickyRead()),
		// Use sticky reads for cache efficiency
		helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
		// Use concurrent dual-writes for low latency
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
		// Use active failover for reads
		helix.WithReplayer(replayer),
		// Configure replayer for failed writes
	)
	if err != nil {
		log.Fatalf("Failed to create Helix client: %v", err)
	}
	defer client.Close()

	// Example: Insert data (dual-write to both clusters)
	userID := gocql.TimeUUID()
	err = client.Query(
		"INSERT INTO users (id, name, email, created_at) VALUES (?, ?, ?, ?)",
		userID, "Alice", "alice@example.com", time.Now(),
	).Exec()
	if err != nil {
		log.Fatalf("Failed to insert user: %v", err)
	}
	fmt.Printf("Inserted user with ID: %s\n", userID)

	// Example: Read data (routed via sticky read strategy)
	var name, email string
	var createdAt time.Time
	err = client.Query(
		"SELECT name, email, created_at FROM users WHERE id = ?",
		userID,
	).Scan(&name, &email, &createdAt)
	if err != nil {
		log.Fatalf("Failed to read user: %v", err)
	}
	fmt.Printf("Read user: name=%s, email=%s, created=%s\n", name, email, createdAt)

	// Example: Update data (dual-write)
	err = client.Query(
		"UPDATE users SET email = ? WHERE id = ?",
		"alice.new@example.com", userID,
	).Exec()
	if err != nil {
		log.Fatalf("Failed to update user: %v", err)
	}
	fmt.Println("Updated user email")

	// Example: Batch operations
	batch := client.Batch(helix.LoggedBatch)
	batch.Query("INSERT INTO audit_log (id, action, user_id) VALUES (?, ?, ?)",
		gocql.TimeUUID(), "CREATE", userID)
	batch.Query("INSERT INTO audit_log (id, action, user_id) VALUES (?, ?, ?)",
		gocql.TimeUUID(), "UPDATE", userID)
	err = batch.Exec()
	if err != nil {
		log.Fatalf("Failed to execute batch: %v", err)
	}
	fmt.Println("Executed audit log batch")

	// Example: Check for any pending replays
	pending := replayer.Len()
	if pending > 0 {
		fmt.Printf("Warning: %d operations pending replay\n", pending)

		// Drain and process replays
		payloads := replayer.DrainAll()
		for _, payload := range payloads {
			fmt.Printf("Replay needed for cluster %s: %s\n",
				payload.TargetCluster, payload.Query)
			// In production, you would re-execute these against the target cluster
		}
	}

	// Demonstrate iterator usage
	fmt.Println("\n--- Iterator Example ---")
	iter := client.Query("SELECT id, name FROM users LIMIT 10").Iter()
	var id gocql.UUID
	for iter.Scan(&id, &name) {
		fmt.Printf("User: id=%s, name=%s\n", id, name)
	}
	if err := iter.Close(); err != nil {
		log.Printf("Iterator error: %v", err)
	}

	_ = ctx // ctx available for more advanced usage
	fmt.Println("Basic example completed successfully!")
}
