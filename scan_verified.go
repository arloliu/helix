package helix

import (
	"context"
	"errors"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/types"
)

// errNilScanVerifiedFn is returned by ScanVerified when accept is nil.
var errNilScanVerifiedFn = errors.New("helix: accept must not be nil")

// verifiedRow is the [RowScanner] a ScanVerified callback receives: the one
// row its leg returned.
// It remembers a failed Scan, so the leg ends with the driver's error
// whatever verdict the callback went on to return.
type verifiedRow struct {
	scanner cql.Scanner
	iter    cql.Iter
	columns []ColumnInfo
	err     error
}

func (r *verifiedRow) Scan(dest ...any) error {
	err := r.scanner.Scan(dest...)
	if err != nil && r.err == nil {
		r.err = err
	}

	return err
}

// Columns converts the metadata on first use: most callbacks scan by position.
func (r *verifiedRow) Columns() []ColumnInfo {
	if r.columns == nil {
		r.columns = toColumnInfo(r.iter.Columns())
	}

	return r.columns
}

func (q *cqlQuery) ScanVerified(accept func(r RowScanner) bool) error {
	return q.ScanVerifiedContext(q.getContext(), accept)
}

func (q *cqlQuery) ScanVerifiedContext(ctx context.Context, accept func(r RowScanner) bool) error {
	if accept == nil {
		return errNilScanVerifiedFn
	}

	opts := q.client.resolveReadOptions(ctx, q)
	// A paging token is meaningless on the other cluster, so a paged read
	// neither probes it on not-found nor fails over to it.
	paged := q.pageState != nil
	opts.fallbackRead = !paged

	// Each leg overwrites the verdict; it is read only after a leg that
	// returned nil, which is a leg whose row the callback has judged.
	accepted := false
	opts.fallbackOpts.verify = func() bool { return accepted }

	readFunc := func(ctx context.Context, session cql.Session) (err error) {
		query := session.Query(q.statement, q.values...)
		query = q.applyConfig(query)
		iter := query.IterContext(ctx)
		scanner := iter.Scanner()
		row := &verifiedRow{scanner: scanner, iter: iter}
		found := false

		// scanner.Err closes the iterator the scanner holds; deferred so a
		// panicking callback cannot leak it.
		// A failed Scan outranks the close error, and both outrank not-found.
		defer func() {
			closeErr := scanner.Err()
			switch {
			case row.err != nil:
				err = row.err
			case closeErr != nil:
				err = closeErr
			case !found:
				err = types.ErrNotFound
			}
		}()

		if found = scanner.Next(); found {
			accepted = accept(row)
		}

		return nil
	}

	if paged {
		return q.client.executeReadNoFailover(ctx, opts, readFunc)
	}

	return q.client.executeRead(ctx, opts, readFunc)
}
