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
// # CAS/LWT Warning
//
// Lightweight Transactions (INSERT ... IF NOT EXISTS, ScanCAS, etc.) are NOT safe
// in a shared-nothing dual-cluster architecture. Helix does not guarantee correctness
// for CAS/LWT operations.
package helix
