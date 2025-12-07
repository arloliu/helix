// Package main demonstrates failover behavior in the Helix dual-database client.
//
// This example shows how Helix handles cluster failures and automatically
// fails over to the secondary cluster.
//
// # Concepts Demonstrated
//
// - Active failover: Immediate switch to secondary on read failure
// - Circuit breaker: Prevents cascading failures
// - Write resilience: Dual-writes with replay for failed writes
//
// # Running
//
//	go run main.go
package main

import (
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
	// Configure cluster A
	clusterA := gocql.NewCluster("127.0.0.1:9042")
	clusterA.Keyspace = "helix_example"
	clusterA.Consistency = gocql.Quorum
	clusterA.Timeout = 5 * time.Second

	// Configure cluster B
	clusterB := gocql.NewCluster("127.0.0.1:9043")
	clusterB.Keyspace = "helix_example"
	clusterB.Consistency = gocql.Quorum
	clusterB.Timeout = 5 * time.Second

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

	sessionA := v1.NewSession(gocqlSessionA)
	sessionB := v1.NewSession(gocqlSessionB)

	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(1000))

	// Create client with Active Failover policy
	// This will immediately try the secondary cluster on any read failure
	client, err := helix.NewCQLClient(sessionA, sessionB,
		// StickyRead with 30s cooldown prevents rapid switching between clusters
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithStickyReadCooldown(30*time.Second),
		)),
		helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
		helix.WithReplayer(replayer),
	)
	if err != nil {
		log.Fatalf("Failed to create Helix client: %v", err)
	}
	defer client.Close()

	fmt.Println("=== Helix Failover Demo ===")

	// Setup: Insert test data to both clusters
	testID := gocql.TimeUUID()
	err = client.Query(
		"INSERT INTO users (id, name, email, created_at) VALUES (?, ?, ?, ?)",
		testID, "FailoverTest", "failover@example.com", time.Now(),
	).Exec()
	if err != nil {
		log.Fatalf("Failed to insert test data: %v", err)
	}
	fmt.Printf("Inserted test user: %s\n", testID)

	// Normal operation: Read succeeds from preferred cluster
	fmt.Println("\n--- Normal Operation ---")
	for i := 1; i <= 3; i++ {
		var name string
		err = client.Query("SELECT name FROM users WHERE id = ?", testID).Scan(&name)
		if err != nil {
			fmt.Printf("Read %d: FAILED - %v\n", i, err)
		} else {
			fmt.Printf("Read %d: SUCCESS - name=%s\n", i, name)
		}
	}

	// Simulate failure scenario (in production, this would be a real cluster failure)
	fmt.Println("\n--- Simulating Cluster Failure ---")
	fmt.Println("In production, when the preferred cluster fails:")
	fmt.Println("1. First read attempt to preferred cluster times out")
	fmt.Println("2. ActiveFailover policy triggers failover to secondary")
	fmt.Println("3. Read succeeds from secondary cluster")
	fmt.Println("4. Client switches preference to the healthy cluster")

	// Demonstrate write resilience
	fmt.Println("\n--- Write Resilience ---")
	fmt.Println("During partial failures:")
	fmt.Println("1. Concurrent dual-write attempts both clusters")
	fmt.Println("2. If one fails, the other may still succeed")
	fmt.Println("3. Failed write is enqueued in replayer for later retry")

	// Show replayer status
	pending := replayer.Len()
	fmt.Printf("\nPending replay operations: %d\n", pending)

	if pending > 0 {
		payloads := replayer.DrainAll()
		for _, p := range payloads {
			fmt.Printf("  - Cluster %s: %s\n", p.TargetCluster, p.Query)
		}
	}

	fmt.Println("\n=== Failover Demo Complete ===")
}
