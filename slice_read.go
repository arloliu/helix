package helix

import (
	"context"
	"errors"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/types"
)

// errNilSliceScanFn is returned by [Query.SliceScan] / [Query.SliceScanContext]
// when the caller passes a nil scan callback. Unexported: callers should not
// branch on it via errors.Is; a nil callback is programmer error, not a
// recoverable runtime condition.
var errNilSliceScanFn = errors.New("helix: scanFn must not be nil")

// scanFnNotFoundShieldError wraps a user-returned types.ErrNotFound (or any error
// wrapping it) so the read pipeline cannot mistake it for the synthetic
// "drained 0 rows" signal. Without this shield, classifyReadErr would report
// readNotFound, executeRead / executeReadNoFailover would trigger an
// empty-retry against the alt session, and the SliceScanContext boundary
// translation would silently translate the caller's error to (0, nil).
//
// Deliberately omits Unwrap/Is — that breaks errors.Is(shield, ErrNotFound)
// throughout the internal pipeline. SliceScanContext unwraps the shield
// before returning, so callers' errors.Is(err, types.ErrNotFound) checks
// still match.
//
// classifyReadErr recognises the shield as readCallerNotFound, its own kind:
// the read terminates, no cluster-health signal is recorded, and no
// FallbackRead probe is attempted.
type scanFnNotFoundShieldError struct {
	err error
}

func (e *scanFnNotFoundShieldError) Error() string { return e.err.Error() }

// isShieldedScanFnNotFound reports whether err is (or wraps, via errors.As)
// a *scanFnNotFoundShieldError. Deliberately implemented with errors.As
// rather than adding Is to the type itself: an Is method would also make
// errors.Is(err, types.ErrNotFound) succeed everywhere the shield flows,
// reopening the exact empty-retry / silent-translation bug the shield
// exists to prevent.
func isShieldedScanFnNotFound(err error) bool {
	var shielded *scanFnNotFoundShieldError

	return errors.As(err, &shielded)
}

// rowScannerAdapter narrows a driver [cql.Scanner] down to the public
// [RowScanner] surface (Scan only). Helix's counted-drain loop owns Next()
// and Err()/Close(); exposing them through the user callback would let a
// caller advance or close the iterator out from under the loop.
//
// The pointer receiver matters: drainIterScanWithLimit allocates one adapter
// before the drain loop and pre-boxes it into a RowScanner interface so
// per-row scanFn calls do not re-allocate. A value receiver would force the
// compiler to re-box the value into the interface on every call site, adding
// one allocation per row.
type rowScannerAdapter struct {
	scanner cql.Scanner
}

// Scan delegates to the underlying driver scanner. The destination types
// follow the driver's unmarshalling conventions (custom UnmarshalCQL etc.).
func (r *rowScannerAdapter) Scan(dest ...any) error {
	return r.scanner.Scan(dest...)
}

// drainIterScanWithLimit drains iter row-by-row through scanFn, returning the
// count of completed (nil-returning) scanFn invocations and the first error
// observed.
//
// Ownership: the helper does NOT call iter.Close() directly — gocql's scanner
// reassigns its internal iter pointer across page boundaries, so a deferred
// close on the original iter would close an already-drained page-1 iter while
// leaking the active later-page iter that the scanner holds. scanner.Err()
// closes the scanner's currently-held iter, which is the correct close-owner
// across page boundaries (verified against gocql v1 and cassandra-gocql-driver
// v2). Error precedence: ErrRowLimitExceeded and scanFn errors win over the
// deferred close error (the err == nil guard in the defer prevents overwrite).
//
// The wider PageState-aware / FallbackRead-aware decisions live in the public
// methods and the read-pipeline wrappers; this helper is purely the counted
// drain.
func drainIterScanWithLimit(
	iter cql.Iter, limit int, scanFn func(RowScanner) error,
) (rowCount int, err error) {
	if scanFn == nil { // defense-in-depth; public method has already screened
		return 0, errNilSliceScanFn
	}
	scanner := iter.Scanner()
	defer func() {
		if cerr := scanner.Err(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	// Hoist + pre-box: one adapter allocation and one interface envelope shared
	// across every row. A per-iteration scanFn(rowScannerAdapter{...}) would
	// allocate once per row (escape analysis boxes the value into the
	// RowScanner interface inside the loop). gocql's iterScanner.Next reassigns
	// its internal iter pointer across page boundaries while the scanner
	// value itself stays stable, so reuse is safe.
	adapter := &rowScannerAdapter{scanner: scanner}
	var rs RowScanner = adapter
	for scanner.Next() {
		if limit > 0 && rowCount >= limit {
			return rowCount, types.ErrRowLimitExceeded
		}
		if cberr := scanFn(rs); cberr != nil {
			return rowCount, cberr
		}
		rowCount++
	}

	return rowCount, nil
}

// sliceMapPreallocCap bounds the preallocation drainIterToSliceMapWithLimit
// performs when a positive limit is known. See the comment inside that
// function for the rationale (limit is often a generous safety cap, not the
// expected row count).
const sliceMapPreallocCap = 1024

// drainIterToSliceMapWithLimit drains iter into a slice of column-name maps,
// stopping when max > 0 is reached.
//
// Discard-on-any-error contract: on any non-nil err (overflow, mid-drain
// MapScan error captured by iter.Close, anything), the returned rows slice
// is forced to nil. The public method may capture rows through a readFunc
// closure that runs twice (primary then alt via executeFallbackRead); the
// discard contract prevents a partial primary buffer from leaking into the
// caller's return value when the wrapper translates the err to ErrNotFound
// or DualClusterError.
//
// iter.Close() is idempotent and is the correct close-owner for SliceMap's
// page semantics: iter.MapScan does an in-place page replacement, so a
// deferred close on the original iter always closes the active page
// (verified against gocql v1 and cassandra-gocql-driver v2).
//
// rows must stay nil on a zero-row drain: SliceMapContext's readFunc uses
// `drained == nil` (not len == 0) to detect "primary drained empty" and
// synthesize ErrNotFound for the FallbackRead empty-retry. Preallocating
// unconditionally before the loop breaks that signal (a preallocated-but-
// unused slice is non-nil), so the capacity hint below is applied lazily on
// the first row instead of up front.
func drainIterToSliceMapWithLimit(
	iter cql.Iter, limit int,
) (rows []map[string]any, err error) {
	defer func() {
		if cerr := iter.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err != nil {
			rows = nil
		}
	}()
	for {
		m := map[string]any{}
		if !iter.MapScan(m) {
			break
		}
		if limit > 0 && len(rows) >= limit {
			return nil, types.ErrRowLimitExceeded
		}
		if rows == nil && limit > 0 {
			// limit is frequently set as a generous safety cap (e.g.
			// MaxRows(100_000)) rather than the exact expected row count, so
			// preallocating to limit verbatim could itself become a large
			// wasted allocation when the actual result is much smaller.
			// Capping keeps the common case (result sets up to a few
			// thousand rows) reallocation-free while bounding the worst
			// case for large caps.
			prealloc := min(limit, sliceMapPreallocCap)
			rows = make([]map[string]any, 0, prealloc)
		}
		rows = append(rows, m)
	}

	return rows, nil
}

// effectiveMaxRows returns the cap that the slice drain helpers enforce.
// Precedence: per-query MaxRows > Config.DefaultMaxRows > 0 (unbounded).
func (q *cqlQuery) effectiveMaxRows() int {
	if q.maxRows != nil {
		return *q.maxRows
	}
	if q.client.config.DefaultMaxRows > 0 {
		return q.client.config.DefaultMaxRows
	}
	return 0
}

// applyMaxRowsClamp bounds the driver's per-page network fetch to limit+1 rows.
// The +1 lets Helix detect the (limit+1)th row without fetching a second page.
// When the user supplied a smaller page size, that constraint wins.
func (q *cqlQuery) applyMaxRowsClamp(query cql.Query, limit int) cql.Query {
	if limit <= 0 {
		return query
	}
	clampedPageSize := limit + 1
	if q.pageSize != nil {
		clampedPageSize = min(*q.pageSize, clampedPageSize)
	}

	return query.PageSize(clampedPageSize)
}

// sliceReadOpts derives effective readOptions for the four slice methods and
// is the single source of truth for the PageState short-circuit.
//
// When q.pageState != nil, all four cluster-switching mechanisms must be
// disabled together because an opaque cursor is unsound on the wrong cluster,
// and a token that names its issuing cluster pins the read to it:
//
//   - opts.fallbackRead = false → wrapper's executeFallbackRead empty-retry
//     gate stays closed.
//   - opts.preserveSelectedCluster = true → runPrimaryRead skips drain-aware
//     re-selection and resolveReadTarget skips the AllowedClusters override
//     drain-filter fallback.
//   - useNoFailover = true → caller routes through executeReadNoFailover,
//     so standard executeRead failover is bypassed too.
//
// SliceScan ignores useNoFailover (its no-failover contract is unconditional),
// but it still needs the fallbackRead / preserveSelectedCluster flags applied
// here so the wrapper's behavior matches under PageState.
func (q *cqlQuery) sliceReadOpts(ctx context.Context) (opts readOptions, useNoFailover bool) {
	opts = q.client.resolveReadOptions(ctx, q)
	if q.pageState != nil {
		opts.fallbackRead = false
		opts.preserveSelectedCluster = true
		opts.pinnedCluster, _ = decodePageState(q.pageState)

		return opts, true
	}

	return opts, false
}

func (q *cqlQuery) SliceMap() ([]map[string]any, error) {
	return q.SliceMapContext(q.getContext())
}

// SliceMapContext drains the query into a slice of column maps.
// See [Query.SliceMap] for the caller-facing contract.
func (q *cqlQuery) SliceMapContext(ctx context.Context) ([]map[string]any, error) {
	opts, useNoFailover := q.sliceReadOpts(ctx)
	opts.fallbackOpts = sliceMapFallbackOpts
	limit := q.effectiveMaxRows()

	var rows []map[string]any
	readFunc := func(ctx context.Context, session cql.Session) error {
		query := session.Query(q.statement, q.values...)
		query = q.applyConfig(query)
		query = q.applyMaxRowsClamp(query, limit)
		iter := query.IterContext(ctx)

		drained, derr := drainIterToSliceMapWithLimit(iter, limit)
		rows = drained

		// Synthesize ErrNotFound on a successful empty drain so executeRead /
		// executeReadNoFailover route through executeFallbackRead exactly the
		// same way a single-row Scan returning ErrNotFound does. The public
		// method translates this back to (nil, nil) before returning.
		if derr == nil && drained == nil {
			return types.ErrNotFound
		}

		return derr
	}

	var err error
	if useNoFailover {
		err = q.client.executeReadNoFailover(ctx, opts, readFunc)
	} else {
		err = q.client.executeRead(ctx, opts, readFunc)
	}
	if err != nil {
		// ErrNotFound here means primary drained empty AND (FallbackRead was
		// off OR alt also drained empty OR alt was draining-and-skipped OR
		// PageState suppressed the empty-retry). All of those collapse to
		// the "empty but successful" return shape for callers.
		if classifyReadErr(ctx, err) == readNotFound {
			return nil, nil
		}
		// drainIterToSliceMapWithLimit's discard-on-error contract already nils
		// rows whenever readFunc returned an error; this also covers
		// pre-attempt fail-closed paths where readFunc never ran at all.
		return nil, err
	}

	return rows, nil
}

// sliceMapFallbackOpts is the executeFallbackRead policy for SliceMap. It
// is stateless (no closure capture), so a package-level value avoids the
// per-call allocation that building it inline would incur.
//
// ErrRowLimitExceeded and caller-context errors need no propagation
// predicate: executeFallbackRead returns both before the predicate is
// consulted.
var sliceMapFallbackOpts = fallbackReadOptions{}

func (q *cqlQuery) SliceScan(scanFn func(r RowScanner) error) (int, error) {
	return q.SliceScanContext(q.getContext(), scanFn)
}

// SliceScanContext drains the query and invokes scanFn once per row.
// See [Query.SliceScan] for the caller-facing contract.
//
// The nil-scanFn guard sits at this public boundary so the "no cluster
// contact when scanFn is nil" promise holds mechanically — by the time the
// drain helper has an iter, a session has already been selected.
func (q *cqlQuery) SliceScanContext(
	ctx context.Context, scanFn func(r RowScanner) error,
) (int, error) {
	if scanFn == nil {
		return 0, errNilSliceScanFn
	}
	opts, _ := q.sliceReadOpts(ctx)
	limit := q.effectiveMaxRows()

	// scanFnInvokedOnAlt: shared via closure capture between readFunc (which
	// runs against the primary, then again against the alt inside
	// executeFallbackRead) and propagateAltErr (evaluated only on the alt
	// leg). The primary leg only reaches executeFallbackRead when its drain
	// returned 0 rows; in that case the for-loop never iterates and the
	// flag stays false through the primary's run. Alt's drain then sets it
	// on each row, so the flag at predicate-evaluation time reflects the
	// alt exclusively.
	var rowCount int
	var scanFnInvokedOnAlt bool
	wrapped := func(r RowScanner) error {
		scanFnInvokedOnAlt = true
		cberr := scanFn(r)
		// Shield a user-returned types.ErrNotFound (and any error wrapping it)
		// from the read-pipeline's empty-retry classification — otherwise the
		// caller's intentional ErrNotFound would be mistaken for the synthetic
		// "drained 0 rows" signal and silently translated to (0, nil).
		if cberr != nil && types.IsNotFound(cberr) {
			return &scanFnNotFoundShieldError{err: cberr}
		}

		return cberr
	}
	readFunc := func(ctx context.Context, session cql.Session) error {
		query := session.Query(q.statement, q.values...)
		query = q.applyConfig(query)
		query = q.applyMaxRowsClamp(query, limit)
		iter := query.IterContext(ctx)

		n, derr := drainIterScanWithLimit(iter, limit, wrapped)
		rowCount = n

		// Synthesize ErrNotFound on a successful empty drain so the wrapper
		// routes through executeFallbackRead the same way the single-row
		// path does. Translated back to (0, nil) at the public boundary.
		if derr == nil && n == 0 {
			return types.ErrNotFound
		}

		return derr
	}

	opts.fallbackOpts = fallbackReadOptions{
		propagateAltErr: func(error) bool {
			// Any invocation of scanFn on the alt — successful or not —
			// either mutated the caller's accumulator or surfaced a scanFn
			// error that the public API contract requires to propagate.
			// Suppression to ErrNotFound would silently hide either.
			return scanFnInvokedOnAlt
		},
	}

	err := q.client.executeReadNoFailover(ctx, opts, readFunc)
	var shielded *scanFnNotFoundShieldError
	if errors.As(err, &shielded) {
		// Unwrap the user-callback ErrNotFound the shield carried through
		// the pipeline. Caller's errors.Is(err, types.ErrNotFound) matches
		// the unwrapped value.
		return rowCount, shielded.err
	}
	if classifyReadErr(ctx, err) == readNotFound {
		// rowCount is zero by construction: synthetic ErrNotFound only
		// fires when the drain produced no rows, and executeFallbackRead
		// returns ErrNotFound only when alt also drained empty.
		return 0, nil
	}

	return rowCount, err
}
