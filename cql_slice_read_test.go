package helix

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─────────────────────────────────────────────
// Phase 2 test fixtures: cql.Session/Query/Iter/Scanner producing rows
// ─────────────────────────────────────────────

// sliceSpec describes the rows a sliceTestIter plays back along with optional
// mid-drain or close errors to exercise the error paths the slice methods
// must handle.
type sliceSpec struct {
	cols       []string
	rows       [][]any
	closeErr   error // returned from iter.Close (drain-end / mid-drain captured)
	midDrainAt int   // if > 0, MapScan / Scanner.Next return false at row index midDrainAt-1 and Err/Close surface midDrainErr
	midDrain   error // mid-drain error injected after pos == midDrainAt-1
}

// sliceTestSession is a single-cluster fake cql.Session whose Query factory
// returns iters built from spec. Tests that need different rows per cluster
// build two sliceTestSession values via newSliceClient.
type sliceTestSession struct {
	spec         *sliceSpec
	iterCalls    atomic.Int32    // increments per Iter() call (must stay zero for slice methods)
	iterCtxCalls atomic.Int32    // increments per IterContext() call
	lastQuery    *sliceTestQuery // last query built, for page-size clamp assertions
}

func (s *sliceTestSession) Query(stmt string, values ...any) cql.Query {
	q := &sliceTestQuery{
		session:   s,
		statement: stmt,
		values:    values,
	}
	s.lastQuery = q
	return q
}

func (s *sliceTestSession) Batch(_ cql.BatchType) cql.Batch { return nil }
func (s *sliceTestSession) Close()                          {}

type sliceTestQuery struct {
	session      *sliceTestSession
	statement    string
	values       []any
	lastPageSize *int // records final applied page size for clamp assertions
}

func (q *sliceTestQuery) Consistency(_ cql.Consistency) cql.Query       { return q }
func (q *sliceTestQuery) SerialConsistency(_ cql.Consistency) cql.Query { return q }
func (q *sliceTestQuery) PageSize(n int) cql.Query {
	v := n
	q.lastPageSize = &v

	return q
}

func (q *sliceTestQuery) PageState(_ []byte) cql.Query                  { return q }
func (q *sliceTestQuery) WithTimestamp(_ int64) cql.Query               { return q }
func (q *sliceTestQuery) Exec() error                                   { return nil }
func (q *sliceTestQuery) ExecContext(_ context.Context) error           { return nil }
func (q *sliceTestQuery) Scan(_ ...any) error                           { return nil }
func (q *sliceTestQuery) ScanContext(_ context.Context, _ ...any) error { return nil }
func (q *sliceTestQuery) MapScan(_ map[string]any) error                { return nil }
func (q *sliceTestQuery) MapScanContext(_ context.Context, _ map[string]any) error {
	return nil
}

func (q *sliceTestQuery) Iter() cql.Iter {
	q.session.iterCalls.Add(1)
	return &sliceTestIter{spec: q.session.spec}
}

func (q *sliceTestQuery) IterContext(_ context.Context) cql.Iter {
	q.session.iterCtxCalls.Add(1)
	return &sliceTestIter{spec: q.session.spec}
}

func (q *sliceTestQuery) ScanCAS(_ ...any) (bool, error) { return false, nil }
func (q *sliceTestQuery) ScanCASContext(_ context.Context, _ ...any) (bool, error) {
	return false, nil
}

func (q *sliceTestQuery) MapScanCAS(_ map[string]any) (bool, error) { return false, nil }
func (q *sliceTestQuery) MapScanCASContext(_ context.Context, _ map[string]any) (bool, error) {
	return false, nil
}

func (q *sliceTestQuery) Statement() string { return q.statement }
func (q *sliceTestQuery) Values() []any     { return q.values }
func (q *sliceTestQuery) Release()          {}

type sliceTestIter struct {
	spec *sliceSpec
	pos  int
	err  error // set when mid-drain triggers
}

func (i *sliceTestIter) Scan(_ ...any) bool { return false }

func (i *sliceTestIter) Close() error {
	if i.err != nil {
		return i.err
	}
	if i.spec.closeErr != nil {
		return i.spec.closeErr
	}
	return nil
}

func (i *sliceTestIter) MapScan(m map[string]any) bool {
	if i.spec.midDrainAt > 0 && i.pos == i.spec.midDrainAt {
		i.err = i.spec.midDrain
		return false
	}
	if i.pos >= len(i.spec.rows) {
		return false
	}
	for k, c := range i.spec.cols {
		if k < len(i.spec.rows[i.pos]) {
			m[c] = i.spec.rows[i.pos][k]
		}
	}
	i.pos++

	return true
}

func (i *sliceTestIter) SliceMap() ([]map[string]any, error) { return nil, nil }
func (i *sliceTestIter) PageState() []byte                   { return nil }
func (i *sliceTestIter) NumRows() int                        { return len(i.spec.rows) }
func (i *sliceTestIter) Columns() []cql.ColumnInfo           { return nil }
func (i *sliceTestIter) Warnings() []string                  { return nil }

func (i *sliceTestIter) Scanner() cql.Scanner {
	return &sliceTestScanner{iter: i}
}

type sliceTestScanner struct {
	iter *sliceTestIter
}

func (s *sliceTestScanner) Next() bool {
	if s.iter.spec.midDrainAt > 0 && s.iter.pos == s.iter.spec.midDrainAt {
		s.iter.err = s.iter.spec.midDrain
		return false
	}
	if s.iter.pos >= len(s.iter.spec.rows) {
		return false
	}
	s.iter.pos++

	return true
}

func (s *sliceTestScanner) Scan(dest ...any) error {
	if s.iter.pos == 0 || s.iter.pos > len(s.iter.spec.rows) {
		return errors.New("sliceTestScanner.Scan called outside the active row")
	}
	row := s.iter.spec.rows[s.iter.pos-1]
	// Mirror the type-switch precedent in cql_client_test.go's mockQuery.Scan:
	// every row value supplied by phase-2 tests is int or string. Extend here
	// when a new column kind is needed by a test.
	for k := range dest {
		if k >= len(row) {
			break
		}
		switch d := dest[k].(type) {
		case *string:
			if v, ok := row[k].(string); ok {
				*d = v
			}
		case *int:
			if v, ok := row[k].(int); ok {
				*d = v
			}
		}
	}

	return nil
}

// Err mirrors gocql's Scanner.Err semantics: it closes the iter that the
// scanner currently holds and returns the first error encountered — mid-drain
// or surfaced by Close. Tests rely on this propagation matching the real
// driver contract that drainIterScanWithLimit's deferred scanner.Err() check
// depends on.
func (s *sliceTestScanner) Err() error {
	if s.iter.err != nil {
		return s.iter.err
	}
	if s.iter.spec.closeErr != nil {
		return s.iter.spec.closeErr
	}
	return nil
}

// newSliceClient builds a dual-cluster CQLClient where cluster A's session
// plays specA and cluster B's session plays specB. opts are forwarded to
// NewCQLClient.
func newSliceClient(
	t *testing.T, specA, specB *sliceSpec, opts ...Option,
) (client *CQLClient, sessionA, sessionB *sliceTestSession) {
	t.Helper()
	sessionA = &sliceTestSession{spec: specA}
	sessionB = &sliceTestSession{spec: specB}
	client, err := NewCQLClient(sessionA, sessionB, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	return client, sessionA, sessionB
}

// newSingleSliceClient builds a single-cluster CQLClient.
func newSingleSliceClient(t *testing.T, spec *sliceSpec) (*CQLClient, *sliceTestSession) {
	t.Helper()
	sa := &sliceTestSession{spec: spec}
	client, err := NewCQLClient(sa, nil)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	return client, sa
}

func rowsOf(values ...[]any) [][]any { return values }

// ─────────────────────────────────────────────
// Happy path
// ─────────────────────────────────────────────

func TestSliceMap_HappyPath_ReturnsRows(t *testing.T) {
	spec := &sliceSpec{
		cols: []string{"id", "name"},
		rows: rowsOf(
			[]any{1, "alice"},
			[]any{2, "bob"},
			[]any{3, "carol"},
		),
	}
	client, _ := newSingleSliceClient(t, spec)

	rows, err := client.Query("SELECT id, name FROM t").SliceMapContext(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 3)
	assert.Equal(t, "alice", rows[0]["name"])
	assert.Equal(t, "carol", rows[2]["name"])
}

func TestSliceScan_HappyPath_InvokesCallbackPerRow(t *testing.T) {
	spec := &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{1}, []any{2}, []any{3}),
	}
	client, _ := newSingleSliceClient(t, spec)

	var got []int
	rowCount, err := client.Query("SELECT id FROM t").SliceScanContext(context.Background(), func(r RowScanner) error {
		var v int
		if err := r.Scan(&v); err != nil {
			return err
		}
		got = append(got, v)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, rowCount)
	assert.Equal(t, []int{1, 2, 3}, got)
}

// ─────────────────────────────────────────────
// Empty result (no fallback wired in phase 2)
// ─────────────────────────────────────────────

func TestSliceMap_Empty_ReturnsNilNil(t *testing.T) {
	spec := &sliceSpec{cols: []string{"id"}}
	client, _ := newSingleSliceClient(t, spec)

	rows, err := client.Query("SELECT id FROM t").SliceMapContext(context.Background())
	require.NoError(t, err)
	assert.Nil(t, rows)
}

func TestSliceScan_Empty_ReturnsZeroNil(t *testing.T) {
	spec := &sliceSpec{cols: []string{"id"}}
	client, _ := newSingleSliceClient(t, spec)

	rowCount, err := client.Query("SELECT id FROM t").SliceScanContext(context.Background(), func(_ RowScanner) error {
		t.Fatal("scanFn must not be invoked on an empty result")
		return nil
	})
	require.NoError(t, err)
	assert.Zero(t, rowCount)
}

// ─────────────────────────────────────────────
// SliceMap: standard failover via executeRead
// ─────────────────────────────────────────────

func TestSliceMap_PrimaryRealError_StandardFailoverInvokesAlt(t *testing.T) {
	primary := errors.New("primary boom")
	specA := &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{1}),
		// Mid-drain: read one row, then surface an error from Close.
		midDrainAt: 1,
		midDrain:   primary,
	}
	specB := &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{9}, []any{10}),
	}
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}
	client, sa, sb := newSliceClient(t, specA, specB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)

	rows, err := client.Query("SELECT id FROM t").SliceMapContext(context.Background())
	require.NoError(t, err, "alt succeeded → public error is nil after standard failover")
	require.Len(t, rows, 2, "rows must be the alt's drain, not a mix")
	assert.Equal(t, 9, rows[0]["id"])
	assert.Equal(t, 10, rows[1]["id"])

	assert.Equal(t, int32(1), sa.iterCtxCalls.Load(), "primary cluster A was attempted via IterContext")
	assert.Equal(t, int32(1), sb.iterCtxCalls.Load(), "alt cluster B was re-attempted via IterContext")
	assert.Equal(t, int64(1), met.get(met.ReadErrors, ClusterA), "primary error increments IncReadError on A")
	assert.Equal(t, int64(1), met.get(met.ReadTotal, ClusterA))
	assert.Equal(t, int64(1), met.get(met.ReadTotal, ClusterB))
	assert.Equal(t, []ClusterID{ClusterA}, policy.RecordFailureCalls,
		"RecordFailure fires on the failed primary (failover branch owns it)")
}

func TestSliceMap_PrimaryRealError_BothClustersFail_ReturnsDualClusterError(t *testing.T) {
	primaryErr := errors.New("primary boom")
	altErr := errors.New("alt boom")
	specA := &sliceSpec{cols: []string{"id"}, rows: rowsOf([]any{1}), midDrainAt: 1, midDrain: primaryErr}
	specB := &sliceSpec{cols: []string{"id"}, rows: rowsOf([]any{9}), midDrainAt: 1, midDrain: altErr}
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}
	client, _, _ := newSliceClient(t, specA, specB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)

	rows, err := client.Query("SELECT id FROM t").SliceMapContext(context.Background())
	assert.Nil(t, rows, "discard contract: no partial rows leak when both clusters fail")
	var dce *types.DualClusterError
	require.ErrorAs(t, err, &dce)
	assert.ErrorIs(t, dce.ErrorA, primaryErr)
	assert.ErrorIs(t, dce.ErrorB, altErr)
}

// ─────────────────────────────────────────────
// SliceScan: no failover, error propagates with metrics
// ─────────────────────────────────────────────

func TestSliceScan_PrimaryRealError_NoFailover_ErrorPropagatesWithMetricsIntact(t *testing.T) {
	primaryErr := errors.New("primary boom")
	specA := &sliceSpec{cols: []string{"id"}, rows: rowsOf([]any{1}), midDrainAt: 1, midDrain: primaryErr}
	specB := &sliceSpec{cols: []string{"id"}, rows: rowsOf([]any{9})}
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}
	client, _, _ := newSliceClient(t, specA, specB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)

	rowCount, err := client.Query("SELECT id FROM t").SliceScanContext(context.Background(), func(r RowScanner) error {
		var v int
		return r.Scan(&v)
	})
	require.ErrorIs(t, err, primaryErr, "no failover: caller sees the primary's error")
	assert.Equal(t, 1, rowCount, "rowCount reflects successful scanFn invocations before the iter errored")

	assert.Equal(t, int64(1), met.get(met.ReadTotal, ClusterA), "IncReadTotal still recorded on no-failover path")
	assert.Equal(t, int64(1), met.get(met.ReadErrors, ClusterA), "IncReadError recorded by executeReadNoFailover")
	assert.Equal(t, int64(0), met.get(met.ReadTotal, ClusterB), "alt is not contacted")

	assert.Equal(t, []ClusterID{ClusterA}, policy.RecordFailureCalls,
		"RecordFailure fires on no-failover path (telemetry parity with executeRead's failover branch)")
}

// ─────────────────────────────────────────────
// scanFn error
// ─────────────────────────────────────────────

func TestSliceScan_ScanFnError_AbortsAndPropagates(t *testing.T) {
	spec := &sliceSpec{cols: []string{"id"}, rows: rowsOf([]any{1}, []any{2}, []any{3})}
	client, _ := newSingleSliceClient(t, spec)

	cbErr := errors.New("callback rejected")
	var seen int
	rowCount, err := client.Query("SELECT id FROM t").SliceScanContext(context.Background(), func(r RowScanner) error {
		var v int
		if err := r.Scan(&v); err != nil {
			return err
		}
		seen++
		if v == 2 {
			return cbErr
		}

		return nil
	})
	require.ErrorIs(t, err, cbErr)
	assert.Equal(t, 1, rowCount, "rowCount counts only successful (nil-returning) invocations")
	assert.Equal(t, 2, seen, "callback was invoked on row 1 (success) and row 2 (returned cbErr)")
}

// ─────────────────────────────────────────────
// Nil callback: no cluster contact
// ─────────────────────────────────────────────

func TestSliceScan_NilScanFn_ReturnsErrorBeforeAnyClusterContact(t *testing.T) {
	spec := &sliceSpec{cols: []string{"id"}, rows: rowsOf([]any{1})}
	client, sa := newSingleSliceClient(t, spec)

	rowCount, err := client.Query("SELECT id FROM t").SliceScanContext(context.Background(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scanFn")
	assert.Zero(t, rowCount)

	assert.Equal(t, int32(0), sa.iterCalls.Load(), "no Iter() call")
	assert.Equal(t, int32(0), sa.iterCtxCalls.Load(), "no IterContext() call either — guard fires before runPrimaryRead")
}

// ─────────────────────────────────────────────
// IterContext binding (mechanical)
// ─────────────────────────────────────────────

func TestSliceMapContext_BindsIterContextNotIter(t *testing.T) {
	spec := &sliceSpec{cols: []string{"id"}, rows: rowsOf([]any{1})}
	client, sa := newSingleSliceClient(t, spec)

	_, err := client.Query("SELECT id FROM t").SliceMapContext(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int32(0), sa.iterCalls.Load(), "SliceMap must NOT call Iter()")
	assert.Equal(t, int32(1), sa.iterCtxCalls.Load(), "SliceMap must call IterContext() exactly once")
}

func TestSliceScanContext_BindsIterContextNotIter(t *testing.T) {
	spec := &sliceSpec{cols: []string{"id"}, rows: rowsOf([]any{1})}
	client, sa := newSingleSliceClient(t, spec)

	_, err := client.Query("SELECT id FROM t").SliceScanContext(context.Background(), func(_ RowScanner) error { return nil })
	require.NoError(t, err)
	assert.Equal(t, int32(0), sa.iterCalls.Load(), "SliceScan must NOT call Iter()")
	assert.Equal(t, int32(1), sa.iterCtxCalls.Load(), "SliceScan must call IterContext() exactly once")
}

// ─────────────────────────────────────────────
// PageState short-circuit (mechanical via sliceReadOpts)
// ─────────────────────────────────────────────

func TestSliceMap_PageState_DerivesNoFailoverAndFallbackReadFalse(t *testing.T) {
	q := &cqlQuery{
		client:       &CQLClient{config: &ClientConfig{DefaultFallbackRead: true}},
		pageState:    []byte("cursor"),
		fallbackRead: true,
	}
	opts, useNoFailover := q.sliceReadOpts(context.Background())
	assert.True(t, useNoFailover, "pageState != nil routes through executeReadNoFailover")
	assert.False(t, opts.fallbackRead, "pageState != nil suppresses the wrapper's empty-retry gate")
	assert.True(t, opts.preserveSelectedCluster,
		"pageState != nil suppresses drain-aware re-selection and override drain-filter fallback")
}

func TestSliceScan_PageState_DerivesFallbackReadFalseEvenWhenChained(t *testing.T) {
	// Without PageState, FallbackRead chained on the query flows through to opts.
	qNoPaging := &cqlQuery{client: &CQLClient{config: &ClientConfig{}}, fallbackRead: true}
	opts, useNoFailover := qNoPaging.sliceReadOpts(context.Background())
	assert.False(t, useNoFailover)
	assert.True(t, opts.fallbackRead, "FallbackRead chained without PageState reaches the wrapper")

	// With PageState set, FallbackRead is mechanically suppressed.
	qPaging := &cqlQuery{
		client:       &CQLClient{config: &ClientConfig{}},
		pageState:    []byte("cursor"),
		fallbackRead: true,
	}
	opts, useNoFailover = qPaging.sliceReadOpts(context.Background())
	assert.True(t, useNoFailover)
	assert.False(t, opts.fallbackRead,
		"q.pageState != nil + chained FallbackRead → opts.fallbackRead must be false (load-bearing for the PageState short-circuit)")
}

// ─────────────────────────────────────────────
// SliceMap-via-PageState routes through no-failover (no alt contact on primary error)
// ─────────────────────────────────────────────

func TestSliceMap_PageState_PrimaryError_DoesNotInvokeAlt(t *testing.T) {
	primaryErr := errors.New("primary boom")
	specA := &sliceSpec{cols: []string{"id"}, rows: rowsOf([]any{1}), midDrainAt: 1, midDrain: primaryErr}
	specB := &sliceSpec{cols: []string{"id"}, rows: rowsOf([]any{9})}
	client, _, sb := newSliceClient(t, specA, specB)

	rows, err := client.Query("SELECT id FROM t").PageState([]byte("cursor")).SliceMapContext(context.Background())
	require.ErrorIs(t, err, primaryErr, "no failover when pageState is set")
	assert.Nil(t, rows)
	assert.Equal(t, int32(0), sb.iterCtxCalls.Load(), "alt session must never be queried in PageState short-circuit")
}

// ─────────────────────────────────────────────
// MaxRows setter shape
// ─────────────────────────────────────────────

func TestMaxRows_StoresPerQueryValue_ZeroClears(t *testing.T) {
	q := &cqlQuery{}
	q.MaxRows(10)
	require.NotNil(t, q.maxRows)
	assert.Equal(t, 10, *q.maxRows)
	q.MaxRows(0)
	assert.Nil(t, q.maxRows, "MaxRows(0) clears the per-query override")
}

// ─────────────────────────────────────────────
// Helper-level tests: discard contract, scanner err precedence
// ─────────────────────────────────────────────

func TestDrainIterScanWithLimit_NilScanFn_ReturnsSentinelWithoutDraining(t *testing.T) {
	iter := &sliceTestIter{spec: &sliceSpec{cols: []string{"id"}, rows: rowsOf([]any{1})}}
	rowCount, err := drainIterScanWithLimit(iter, 0, nil)
	require.ErrorIs(t, err, errNilSliceScanFn)
	assert.Zero(t, rowCount)
	assert.Zero(t, iter.pos, "no row should have been advanced")
}

func TestDrainIterToSliceMapWithLimit_MidDrainErrorDiscardsPartialRows(t *testing.T) {
	mid := errors.New("oops mid-drain")
	iter := &sliceTestIter{spec: &sliceSpec{
		cols:       []string{"id"},
		rows:       rowsOf([]any{1}, []any{2}, []any{3}),
		midDrainAt: 2, // mid-drain error after row 2
		midDrain:   mid,
	}}
	rows, err := drainIterToSliceMapWithLimit(iter, 0)
	require.ErrorIs(t, err, mid)
	assert.Nil(t, rows, "partial accumulator MUST be discarded on any error")
}

func TestDrainIterScanWithLimit_ScannerErrPropagatesAfterDrain(t *testing.T) {
	endErr := errors.New("end of drain error")
	iter := &sliceTestIter{spec: &sliceSpec{
		cols:     []string{"id"},
		rows:     rowsOf([]any{1}, []any{2}),
		closeErr: endErr,
	}}
	rowCount, err := drainIterScanWithLimit(iter, 0, func(r RowScanner) error {
		var v int

		return r.Scan(&v)
	})
	require.ErrorIs(t, err, endErr,
		"scanner.Err() captured at the page-iter close site is the right close-owner across page boundaries")
	assert.Equal(t, 2, rowCount, "successful callbacks still counted")
}

// Off-by-one bound enforcement: phase 2 helpers admit up to limit rows and
// abort with ErrRowLimitExceeded upon detecting the (limit+1)th. The
// validation panic on negative / >= MaxInt32 land in phase 3 alongside the
// page-size clamp; these tests lock the helper-level boundary spec early so
// the contract cannot regress before phase 3's tests cover the broader matrix.

func TestDrainIterToSliceMapWithLimit_LimitExceeded_DiscardsRowsAndReturnsSentinel(t *testing.T) {
	iter := &sliceTestIter{spec: &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{1}, []any{2}, []any{3}, []any{4}),
	}}
	rows, err := drainIterToSliceMapWithLimit(iter, 2)
	require.ErrorIs(t, err, types.ErrRowLimitExceeded)
	assert.Nil(t, rows, "materializing helper MUST discard the partial buffer on overflow")
}

func TestDrainIterScanWithLimit_LimitExceeded_ReturnsRowCountAndSentinel(t *testing.T) {
	iter := &sliceTestIter{spec: &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{1}, []any{2}, []any{3}, []any{4}),
	}}
	invoked := 0
	rowCount, err := drainIterScanWithLimit(iter, 2, func(_ RowScanner) error {
		invoked++

		return nil
	})
	require.ErrorIs(t, err, types.ErrRowLimitExceeded)
	assert.Equal(t, 2, rowCount, "rowCount reflects the limit, not the underlying row count")
	assert.Equal(t, 2, invoked, "scanFn invoked exactly limit times; abort fires before invocation N+1")
}
