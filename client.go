package helix

import (
	"context"

	"github.com/arloliu/helix/types"
)

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

var ErrNotFound = types.ErrNotFound

// IsNotFound reports whether err represents a "not found" result.
// See [types.IsNotFound] for details.
func IsNotFound(err error) bool {
	return types.IsNotFound(err)
}

type fallbackReadKey struct{}

// WithFallbackRead returns a context that enables FallbackRead for all Scan
// and MapScan queries executed with this context.
//
// Per-query FallbackRead() takes precedence over context-level enabling.
// Client-level WithDefaultFallbackRead(true) is overridden by either.
//
// Parameters:
//   - ctx: The parent context
//
// Returns:
//   - context.Context: A new context with FallbackRead enabled
func WithFallbackRead(ctx context.Context) context.Context {
	return context.WithValue(ctx, fallbackReadKey{}, true)
}

func hasFallbackRead(ctx context.Context) bool {
	v, _ := ctx.Value(fallbackReadKey{}).(bool)
	return v
}
