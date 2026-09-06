package helix

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"sync"

	"github.com/arloliu/helix/adapter/cql"
)

// cqlIter implements the Iter interface for CQLClient.
type cqlIter struct {
	iter           cql.Iter
	client         *CQLClient
	cluster        ClusterID
	holder         *sessionHolder  // the session the iterator was opened on
	ctx            context.Context // the caller's context, for error provenance on Close
	overrideActive bool            // captured from readTarget at creation, not re-evaluated
	firstPage      *firstPageCtx   // the leg context the driver keeps; nil when the first page was not bounded

	closeOnce sync.Once
	closeErr  error
}

// toColumnInfo restates the adapter's column metadata in the public
// [ColumnInfo] shape.
// It is the single conversion both column surfaces share: [Iter.Columns] and
// the [RowScanner] a [Query.SliceScan] callback receives.
// The result is freshly allocated for the caller, and empty rather than nil
// when the adapter reports no columns.
func toColumnInfo(cqlCols []cql.ColumnInfo) []ColumnInfo {
	cols := make([]ColumnInfo, len(cqlCols))
	for idx, col := range cqlCols {
		cols[idx] = ColumnInfo{
			Keyspace: col.Keyspace,
			Table:    col.Table,
			Name:     col.Name,
			TypeInfo: col.TypeInfo,
		}
	}

	return cols
}

func (i *cqlIter) Scan(dest ...any) bool {
	return i.iter.Scan(dest...)
}

// Close closes the iterator and reports its outcome like any other read:
// a clean close is a success for the read strategy and the failover
// policy, a cluster error is a failure for both (the strategy's suggested
// alternative is ignored because an iterator cannot be retried), and data
// sentinels or a caller-context error are neither. Auto-refresh accounting
// sees every outcome except a caller-context error.
// Only the first Close reports; later calls return the same error.
func (i *cqlIter) Close() error {
	i.closeOnce.Do(func() { i.closeErr = i.closeAndReport() })

	return i.closeErr
}

func (i *cqlIter) closeAndReport() error {
	err := i.iter.Close()
	i.report(err)

	return err
}

// endFromScanner ends the iterator on behalf of a Scanner consumer, whose
// Err has already closed the driver iterator.
//
// It shares closeOnce with Close, so the everyday idiom — a Scanner loop
// under a deferred Close — reports the read exactly once, and a Close that
// follows returns the error the Scanner already saw rather than closing the
// driver iterator a second time.
func (i *cqlIter) endFromScanner(err error) {
	i.closeOnce.Do(func() {
		i.closeErr = err
		i.report(err)
	})
}

// report releases the leg context and records the read's outcome. It never
// touches the driver iterator, so both the Close path and the Scanner path
// reach it once the driver has let its own resources go.
func (i *cqlIter) report(err error) {
	i.finishFirstPage()
	i.client.health.iterClosed(i.holder, i.cluster, classifyReadErr(i.ctx, err), err, i.overrideActive)
}

// finishFirstPage releases the leg context the first page ran under,
// however the caller is done with the iterator.
//
// It runs after the driver has closed its iterator — from Close, or from a
// Scanner consumer's Err — so a prefetch the driver was still reading from
// is cancelled only once the driver has let it go, and cleanup can never
// turn a clean close into an error.
func (i *cqlIter) finishFirstPage() {
	if i.firstPage == nil {
		return
	}
	i.firstPage.finish()
}

// pageStateHeader identifies a paging token issued by Helix: a magic and a
// format version, followed by the issuing cluster's letter and a CRC-32 of
// the driver token. The checksum keeps a driver token that happens to
// start with the magic from being misread as a Helix token.
const pageStateHeader = "hx1"

const pageStatePrefixLen = len(pageStateHeader) + 1 + crc32.Size

// encodePageState prepends the issuing cluster to a driver paging token.
// An empty token (no more pages) is returned unchanged so callers keep the
// driver's "nil means done" convention.
func encodePageState(cluster ClusterID, raw []byte) []byte {
	if len(raw) == 0 {
		return raw
	}
	out := make([]byte, 0, pageStatePrefixLen+len(raw))
	out = append(out, pageStateHeader...)
	out = append(out, cluster...)
	out = binary.BigEndian.AppendUint32(out, crc32.ChecksumIEEE(raw))

	return append(out, raw...)
}

// decodePageState splits a token produced by encodePageState into the
// issuing cluster and the driver token. A token without a valid Helix
// header (one produced by a driver directly, or by an older Helix) is
// returned as-is with an empty cluster.
func decodePageState(state []byte) (cluster ClusterID, raw []byte) {
	if len(state) <= pageStatePrefixLen || !bytes.HasPrefix(state, []byte(pageStateHeader)) {
		return "", state
	}
	cluster = ClusterID(state[len(pageStateHeader) : len(pageStateHeader)+1])
	if cluster != ClusterA && cluster != ClusterB {
		return "", state
	}
	raw = state[pageStatePrefixLen:]
	if binary.BigEndian.Uint32(state[len(pageStateHeader)+1:]) != crc32.ChecksumIEEE(raw) {
		return "", state
	}

	return cluster, raw
}

// errorIter is returned when resolveReadTarget fails. It defers the error
// to Close() since IterContext's signature cannot return an error directly.
// Scan()/MapScan() always return false. Close()/SliceMap() return the error.
// Metadata methods return zero values.
type errorIter struct {
	err error
}

func (e *errorIter) Scan(...any) bool                    { return false }
func (e *errorIter) Close() error                        { return e.err }
func (e *errorIter) MapScan(map[string]any) bool         { return false }
func (e *errorIter) SliceMap() ([]map[string]any, error) { return nil, e.err }
func (e *errorIter) PageState() []byte                   { return nil }
func (e *errorIter) NumRows() int                        { return 0 }
func (e *errorIter) Columns() []ColumnInfo               { return nil }
func (e *errorIter) Scanner() Scanner                    { return &errorScanner{err: e.err} }
func (e *errorIter) Warnings() []string                  { return nil }

// errorScanner is returned by errorIter.Scanner() so callers using the
// Scanner API get a graceful error instead of a nil-pointer panic.
type errorScanner struct{ err error }

func (s *errorScanner) Next() bool        { return false }
func (s *errorScanner) Scan(...any) error { return s.err }
func (s *errorScanner) Err() error        { return s.err }

func (i *cqlIter) MapScan(m map[string]any) bool {
	return i.iter.MapScan(m)
}

func (i *cqlIter) SliceMap() ([]map[string]any, error) {
	return i.iter.SliceMap()
}

// PageState returns the token for resuming this iteration. When more pages
// remain, the token carries the cluster that issued it so the next page is
// read from the same cluster whatever the read routing decides meanwhile.
func (i *cqlIter) PageState() []byte {
	return encodePageState(i.cluster, i.iter.PageState())
}

func (i *cqlIter) NumRows() int {
	return i.iter.NumRows()
}

func (i *cqlIter) Columns() []ColumnInfo {
	return toColumnInfo(i.iter.Columns())
}

func (i *cqlIter) Scanner() Scanner {
	return &cqlScanner{scanner: i.iter.Scanner(), owner: i}
}

func (i *cqlIter) Warnings() []string {
	return i.iter.Warnings()
}

// cqlScanner wraps cql.Scanner to implement helix.Scanner.
// It keeps the iterator it came from, because a consumer that switches to
// the Scanner API stops using that iterator directly and ends the read
// through Err instead of Close.
type cqlScanner struct {
	scanner cql.Scanner
	owner   *cqlIter
}

func (s *cqlScanner) Next() bool {
	return s.scanner.Next()
}

func (s *cqlScanner) Scan(dest ...any) error {
	return s.scanner.Scan(dest...)
}

// Err returns the iterator's error and ends the read with it: the outcome
// reaches the read strategy, the failover policy and auto-refresh exactly
// as it does from Close, and the leg context the first page ran under is
// released.
//
// Reporting from here is what keeps a Scanner consumer's cluster failures
// visible. Without it a caller that drains with Scanner and never calls
// Close would leave a failing cluster with a clean record: no breaker
// input, no degraded marking, and reads still routed to it.
//
// The driver's own Err has already closed the iterator, so this adds no
// second close.
func (s *cqlScanner) Err() error {
	err := s.scanner.Err()
	s.owner.endFromScanner(err)

	return err
}
