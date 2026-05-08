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

// Sentinel errors for strict write paths.
var (
	// ErrClusterDegraded is returned by a Strict() write when a cluster is
	// degraded (AdaptiveDualWrite only). The write was not attempted on that
	// cluster; the error surfaces as [PartialWriteError.Cause].
	ErrClusterDegraded = types.ErrClusterDegraded

	// ErrClusterDraining is returned by a Strict() write when a cluster is
	// in drain mode. The write was not attempted on that cluster; the error
	// surfaces as [PartialWriteError.Cause].
	ErrClusterDraining = types.ErrClusterDraining

	// ErrStrictUnsupported is returned when Strict() is called but the
	// configured WriteStrategy does not implement [StrictWriter].
	ErrStrictUnsupported = types.ErrStrictUnsupported

	// ErrStrictMirrorUnsupported is returned when both Strict() and Mirror()
	// are called on the same query or batch. The two modes are incompatible.
	ErrStrictMirrorUnsupported = types.ErrStrictMirrorUnsupported
)

// Re-export strict write result types.
type (
	// PartialWriteError is returned by a Strict() write when exactly one
	// cluster acknowledged the write. See [types.PartialWriteError] for details.
	PartialWriteError = types.PartialWriteError

	// DualClusterError is returned when both clusters return errors for the
	// same operation. See [types.DualClusterError] for details.
	DualClusterError = types.DualClusterError
)

// AsPartialWriteError unwraps err as a [PartialWriteError]. See
// [types.AsPartialWriteError] for details.
func AsPartialWriteError(err error) (*PartialWriteError, bool) {
	return types.AsPartialWriteError(err)
}

// IsPartialWrite reports whether err is a [PartialWriteError]. See
// [types.IsPartialWrite] for details.
func IsPartialWrite(err error) bool {
	return types.IsPartialWrite(err)
}

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
