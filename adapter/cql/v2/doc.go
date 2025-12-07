// Package v2 provides an adapter for gocql v2 (github.com/apache/cassandra-gocql-driver).
//
// This adapter wraps the Apache Cassandra gocql driver v2 to implement
// the helix CQL interfaces, enabling dual-cluster operations.
//
// # Installation
//
// Import this package along with the Apache gocql driver:
//
//	import (
//	    "github.com/apache/cassandra-gocql-driver/gocql"
//	    v2 "github.com/arloliu/helix/adapter/cql/v2"
//	)
//
// # Usage
//
// Create a gocql session and wrap it with the v2 adapter:
//
//	cluster := gocql.NewCluster("127.0.0.1", "127.0.0.2")
//	cluster.Keyspace = "my_keyspace"
//
//	gocqlSession, err := cluster.CreateSession()
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	session := v2.NewSession(gocqlSession)
//
//	client, err := helix.NewCQLClient(sessionA, sessionB)
//
// # Type Conversions
//
// The adapter provides helper functions for converting between helix and gocql types:
//
//   - [ToGocqlConsistency]: Converts helix Consistency to gocql.Consistency
//   - [FromGocqlConsistency]: Converts gocql.Consistency to helix Consistency
//   - [ToGocqlBatchType]: Converts helix BatchType to gocql.BatchType
//   - [FromGocqlBatchType]: Converts gocql.BatchType to helix BatchType
//   - [ToGocqlSerialConsistency]: Converts helix Consistency to gocql serial consistency
//   - [UnwrapSession]: Returns the underlying gocql.Session
//
// # Differences from v1
//
// The v2 driver from Apache has some API differences:
//   - More structured error types
//   - Enhanced metadata support
//   - Different internal implementation
//
// However, the helix adapter normalizes these differences, so users
// can switch between v1 and v2 with minimal code changes.
//
// # Thread Safety
//
// All adapter types are safe for concurrent use, matching gocql's thread safety guarantees.
package v2
