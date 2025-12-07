package v2

import (
	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/arloliu/helix/adapter/cql"
)

// ToGocqlConsistency converts a helix Consistency to gocql.Consistency.
//
// This is useful when you need to interact with the underlying gocql driver
// directly while using helix consistency constants.
//
// Parameters:
//   - c: Helix consistency level
//
// Returns:
//   - gocql.Consistency: The equivalent gocql consistency level
//
// Example:
//
//	cluster := gocql.NewCluster("127.0.0.1")
//	cluster.Consistency = v2.ToGocqlConsistency(cql.Quorum)
func ToGocqlConsistency(c cql.Consistency) gocql.Consistency {
	return gocql.Consistency(c)
}

// FromGocqlConsistency converts a gocql.Consistency to helix Consistency.
//
// This is useful when you need to read consistency levels from gocql
// and use them with helix APIs.
//
// Parameters:
//   - c: gocql consistency level
//
// Returns:
//   - cql.Consistency: The equivalent helix consistency level
//
// Example:
//
//	helixConsistency := v2.FromGocqlConsistency(cluster.Consistency)
func FromGocqlConsistency(c gocql.Consistency) cql.Consistency {
	return cql.Consistency(c)
}

// ToGocqlBatchType converts a helix BatchType to gocql.BatchType.
//
// This is useful when you need to interact with the underlying gocql driver
// directly while using helix batch type constants.
//
// Parameters:
//   - bt: Helix batch type
//
// Returns:
//   - gocql.BatchType: The equivalent gocql batch type
//
// Example:
//
//	batch := session.Batch(v2.ToGocqlBatchType(cql.LoggedBatch))
func ToGocqlBatchType(bt cql.BatchType) gocql.BatchType {
	return gocql.BatchType(bt)
}

// FromGocqlBatchType converts a gocql.BatchType to helix BatchType.
//
// This is useful when you need to read batch types from gocql
// and use them with helix APIs.
//
// Parameters:
//   - bt: gocql batch type
//
// Returns:
//   - cql.BatchType: The equivalent helix batch type
func FromGocqlBatchType(bt gocql.BatchType) cql.BatchType {
	return cql.BatchType(bt)
}

// ToGocqlSerialConsistency converts a helix Consistency to gocql.Consistency.
//
// In gocql v2, serial consistency is represented as gocql.Consistency,
// not a separate type like in v1.
//
// This is useful for CAS (lightweight transaction) operations that require
// serial consistency levels.
//
// Parameters:
//   - c: Helix consistency level (should be Serial or LocalSerial)
//
// Returns:
//   - gocql.Consistency: The equivalent gocql consistency level
//
// Example:
//
//	query.SerialConsistency(v2.ToGocqlSerialConsistency(cql.LocalSerial))
func ToGocqlSerialConsistency(c cql.Consistency) gocql.Consistency {
	return gocql.Consistency(c)
}

// FromGocqlSerialConsistency converts a gocql.Consistency (used for serial) to helix Consistency.
//
// Parameters:
//   - c: gocql consistency level used for serial consistency
//
// Returns:
//   - cql.Consistency: The equivalent helix consistency level
func FromGocqlSerialConsistency(c gocql.Consistency) cql.Consistency {
	return cql.Consistency(c)
}

// UnwrapSession returns the underlying gocql.Session from a helix Session adapter.
//
// This is useful when you need direct access to the underlying gocql session
// for operations not exposed by the helix interface.
//
// Parameters:
//   - s: Helix v2 Session adapter
//
// Returns:
//   - *gocql.Session: The underlying gocql session
//
// Example:
//
//	gocqlSession := v2.UnwrapSession(session)
//	keyspaceMeta, _ := gocqlSession.KeyspaceMetadata("my_keyspace")
func UnwrapSession(s *Session) *gocql.Session {
	return s.session
}
