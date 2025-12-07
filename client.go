package helix

import "github.com/arloliu/helix/types"

// Type aliases for convenience - re-export from types package.
type (
	ClusterID        = types.ClusterID
	ClusterNames     = types.ClusterNames
	Consistency      = types.Consistency
	BatchType        = types.BatchType
	PriorityLevel    = types.PriorityLevel
	ReplayPayload    = types.ReplayPayload
	BatchStatement   = types.BatchStatement
	Logger           = types.Logger
	MetricsCollector = types.MetricsCollector
)

// Re-export cluster ID constants for convenience.
const (
	ClusterA = types.ClusterA
	ClusterB = types.ClusterB
)

// Re-export consistency level constants for convenience.
const (
	Any         = types.Any
	One         = types.One
	Two         = types.Two
	Three       = types.Three
	Quorum      = types.Quorum
	All         = types.All
	LocalQuorum = types.LocalQuorum
	EachQuorum  = types.EachQuorum
	Serial      = types.Serial
	LocalSerial = types.LocalSerial
	LocalOne    = types.LocalOne
)

// Re-export batch type constants for convenience.
const (
	LoggedBatch   = types.LoggedBatch
	UnloggedBatch = types.UnloggedBatch
	CounterBatch  = types.CounterBatch
)

// Re-export priority level constants for convenience.
const (
	PriorityHigh = types.PriorityHigh
	PriorityLow  = types.PriorityLow
)
