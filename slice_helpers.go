package helix

import (
	"context"
	"errors"
)

// errNilSliceScanAsDecode is returned by [SliceScanAs] when the caller passes
// a nil decode callback. Unexported and symmetric with errNilSliceScanFn: a
// nil callback is programmer error, not a recoverable runtime condition, so
// callers should not branch on it via errors.Is.
var errNilSliceScanAsDecode = errors.New("helix: SliceScanAs decode must not be nil")

// errNilSliceScanAsQuery is returned by [SliceScanAs] when the caller passes
// a nil Query. Without this guard, q.SliceScanContext would panic on a
// nil-interface method call instead of returning a diagnosable error —
// symmetric with the decode-nil guard above.
var errNilSliceScanAsQuery = errors.New("helix: SliceScanAs query must not be nil")

// SliceScanAs runs q.SliceScanContext and collects each decoded row into a
// []T. The decode callback receives a [RowScanner] positioned at the current
// row and a *T to populate.
//
// SliceScanAs is a generic free function (Go does not allow type parameters
// on interface methods) layered on top of [Query.SliceScanContext]. All
// semantics of the underlying SliceScan apply unchanged: [Query.FallbackRead]
// empty-retry, [Query.MaxRows] / [Config.DefaultMaxRows] bounding, page-size
// clamping, no standard failover for SliceScan, and the [Query.PageState]
// cross-cluster short-circuit.
//
// Return shape:
//   - On a successful drain that yielded N rows, returns ([]T of length N, nil).
//   - On a successful empty drain (or empty-retry on both clusters), returns
//     (nil, nil) — same shape as SliceScan's (rowCount=0, err=nil).
//   - On ANY error (decode-callback error, [ErrRowLimitExceeded], context
//     error, primary cluster error, or alt error propagated through
//     FallbackRead), returns (nil, err). The partial accumulator is NOT
//     exposed to the caller — callers who need partial state on error
//     should use [Query.SliceScan] directly and manage the accumulator
//     themselves.
//
// If decode or q is nil, SliceScanAs returns an error before any cluster
// contact — neither the primary nor the alternative cluster is queried. The
// decode-nil case is symmetric with [Query.SliceScan]'s nil-scanFn contract.
//
// Example:
//
//	type Person struct {
//	    ID   string
//	    Name string
//	}
//
//	rows, err := helix.SliceScanAs(ctx,
//	    client.Query("SELECT id, name FROM people WHERE org = ?", orgID).
//	        FallbackRead().
//	        MaxRows(10_000),
//	    func(r helix.RowScanner, dst *Person) error {
//	        return r.Scan(&dst.ID, &dst.Name)
//	    },
//	)
//
// Parameters:
//   - ctx: Context for cancellation and timeout, forwarded to SliceScanContext
//   - q: A Query (typically chained with options like FallbackRead, MaxRows)
//   - decode: Callback that populates one *T per row using r.Scan(dest...)
//
// Returns:
//   - []T: Decoded rows on success; nil on empty result or any error
//   - error: nil on success, propagated from the underlying SliceScanContext
//     otherwise (decode error, [ErrRowLimitExceeded], ctx error, cluster error)
func SliceScanAs[T any](
	ctx context.Context,
	q Query,
	decode func(r RowScanner, dst *T) error,
) ([]T, error) {
	if decode == nil {
		return nil, errNilSliceScanAsDecode
	}
	if q == nil {
		return nil, errNilSliceScanAsQuery
	}

	var out []T
	_, err := q.SliceScanContext(ctx, func(r RowScanner) error {
		var v T
		if derr := decode(r, &v); derr != nil {
			return derr
		}
		out = append(out, v)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}
