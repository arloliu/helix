// Package types provides shared types and error definitions for the helix library.
//
// This is a leaf package with zero helix imports to prevent import cycles.
// All packages in helix can safely import this package.
//
// # Types
//
// ClusterID identifies which cluster is being referenced:
//
//	const (
//	    ClusterA ClusterID = 0
//	    ClusterB ClusterID = 1
//	)
//
// Consistency levels mirror gocql consistency levels for database operations:
//
//	const (
//	    Any         Consistency = 0x00
//	    One         Consistency = 0x01
//	    Two         Consistency = 0x02
//	    Three       Consistency = 0x03
//	    Quorum      Consistency = 0x04
//	    All         Consistency = 0x05
//	    LocalQuorum Consistency = 0x06
//	    EachQuorum  Consistency = 0x07
//	    LocalOne    Consistency = 0x0A
//	)
//
// # Errors
//
// Sentinel errors are provided for common failure scenarios:
//
//   - ErrBothClustersFailed: Both clusters failed during a dual-write operation
//   - ErrNilSession: A nil session was provided
//   - ErrNotConnected: Client is not connected to any cluster
//   - ErrReplayQueueFull: Replay queue has reached capacity
//   - ErrNoHealthyCluster: No healthy cluster available for the operation
//
// # ReplayPayload
//
// ReplayPayload carries failed write operations for asynchronous reconciliation:
//
//	type ReplayPayload struct {
//	    TargetCluster ClusterID
//	    Query         string
//	    Args          []any
//	    Priority      PriorityLevel
//	    Timestamp     time.Time
//	}
package types
