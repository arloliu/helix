// Package cql provides adapter interfaces and implementations for CQL (Cassandra Query Language)
// database drivers.
//
// This package defines the common interfaces that CQL driver adapters must implement,
// allowing helix to work with different versions of gocql or other CQL drivers.
//
// # Interfaces
//
// The package defines interfaces that mirror the gocql API:
//
//   - Session: Wraps a database session for executing queries
//   - Query: Represents a CQL query with bind parameters
//   - Batch: Groups multiple queries for atomic execution
//   - Iter: Iterates over query results
//
// # Adapters
//
// Driver-specific adapters are provided in subpackages:
//
//   - [github.com/arloliu/helix/adapter/cql/v1]: Adapter for gocql v1.x
//   - [github.com/arloliu/helix/adapter/cql/v2]: Adapter for apache/cassandra-gocql-driver v2.x
//
// # Usage
//
// Import the appropriate adapter for your gocql version:
//
//	import (
//	    "github.com/arloliu/helix"
//	    "github.com/arloliu/helix/adapter/cql/v1"
//	    "github.com/gocql/gocql"
//	)
//
//	// Create gocql cluster and session
//	cluster := gocql.NewCluster("127.0.0.1")
//	gocqlSession, _ := cluster.CreateSession()
//
//	// Wrap with helix adapter
//	session := v1.NewSession(gocqlSession)
//
//	// Use with helix client
//	client, _ := helix.NewCQLClient(session, sessionB)
package cql
