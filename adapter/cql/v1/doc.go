// Package v1 provides an adapter for gocql v1.x to work with the helix library.
//
// This adapter wraps gocql sessions, queries, batches, and iterators to implement
// the helix CQL interfaces.
//
// # Installation
//
// Import this package along with gocql v1.x:
//
//	import (
//	    "github.com/gocql/gocql"
//	    "github.com/arloliu/helix/adapter/cql/v1"
//	)
//
// # Usage
//
// Create a gocql session and wrap it with the v1 adapter:
//
//	// Configure gocql cluster
//	cluster := gocql.NewCluster("127.0.0.1", "127.0.0.2")
//	cluster.Keyspace = "my_keyspace"
//	cluster.Consistency = gocql.Quorum
//
//	// Create session
//	gocqlSession, err := cluster.CreateSession()
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Wrap with helix adapter
//	session := v1.NewSession(gocqlSession)
//
//	// Use with helix client
//	client, err := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithReadStrategy(policy.NewStickyRead()),
//	)
//
// # Type Conversions
//
// The adapter provides helper functions for converting between helix and gocql types:
//
//   - [ToGocqlConsistency]: Converts helix Consistency to gocql.Consistency
//   - [FromGocqlConsistency]: Converts gocql.Consistency to helix Consistency
//   - [ToGocqlBatchType]: Converts helix BatchType to gocql.BatchType
//   - [FromGocqlBatchType]: Converts gocql.BatchType to helix BatchType
//   - [ToGocqlSerialConsistency]: Converts helix Consistency to gocql.SerialConsistency
//   - [FromGocqlSerialConsistency]: Converts gocql.SerialConsistency to helix Consistency
//   - [UnwrapSession]: Returns the underlying gocql.Session
//
// # Thread Safety
//
// All adapter types are safe for concurrent use, matching gocql's thread safety guarantees.
package v1
