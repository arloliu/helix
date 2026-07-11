package helix

import (
	"context"
	"errors"
	"testing"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sliceScanAsPerson is the typed-row shape used across the SliceScanAs tests.
// Two fields exercise both *int and *string scanner destinations, which the
// sliceTestScanner already supports (cql_slice_read_test.go:178-188).
type sliceScanAsPerson struct {
	ID   int
	Name string
}

func decodePerson(r RowScanner, dst *sliceScanAsPerson) error {
	return r.Scan(&dst.ID, &dst.Name)
}

// ─────────────────────────────────────────────
// Happy path + empty
// ─────────────────────────────────────────────

func TestSliceScanAs_HappyPath_TypedSlicePopulated(t *testing.T) {
	spec := &sliceSpec{
		cols: []string{"id", "name"},
		rows: rowsOf(
			[]any{1, "alice"},
			[]any{2, "bob"},
			[]any{3, "carol"},
		),
	}
	client, _ := newSingleSliceClient(t, spec)

	rows, err := SliceScanAs(context.Background(),
		client.Query("SELECT id, name FROM t"),
		decodePerson)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	assert.Equal(t, sliceScanAsPerson{ID: 1, Name: "alice"}, rows[0])
	assert.Equal(t, sliceScanAsPerson{ID: 2, Name: "bob"}, rows[1])
	assert.Equal(t, sliceScanAsPerson{ID: 3, Name: "carol"}, rows[2])
}

func TestSliceScanAs_Empty_ReturnsNilNil(t *testing.T) {
	spec := &sliceSpec{cols: []string{"id", "name"}}
	client, _ := newSingleSliceClient(t, spec)

	rows, err := SliceScanAs(context.Background(),
		client.Query("SELECT id, name FROM t"),
		decodePerson)
	require.NoError(t, err, "empty result must NOT surface ErrNotFound through the helper")
	assert.Nil(t, rows)
}

// ─────────────────────────────────────────────
// Nil-on-error contract: partial accumulator is discarded
// ─────────────────────────────────────────────

func TestSliceScanAs_DecodeError_ReturnsNilErr(t *testing.T) {
	spec := &sliceSpec{
		cols: []string{"id", "name"},
		rows: rowsOf(
			[]any{1, "alice"},
			[]any{2, "bob"},
			[]any{3, "carol"},
		),
	}
	client, _ := newSingleSliceClient(t, spec)

	decodeErr := errors.New("decode rejected this row")
	rows, err := SliceScanAs(context.Background(),
		client.Query("SELECT id, name FROM t"),
		func(r RowScanner, dst *sliceScanAsPerson) error {
			if derr := r.Scan(&dst.ID, &dst.Name); derr != nil {
				return derr
			}
			if dst.ID == 3 {
				return decodeErr
			}
			return nil
		})
	require.ErrorIs(t, err, decodeErr)
	assert.Nil(t, rows,
		"helper returns nil on any error even though decode appended rows 1 and 2 internally")
}

func TestSliceScanAs_MaxRowsOverflow_ReturnsNilErrRowLimitExceeded(t *testing.T) {
	spec := &sliceSpec{
		cols: []string{"id", "name"},
		rows: rowsOf(
			[]any{1, "alice"},
			[]any{2, "bob"},
			[]any{3, "carol"},
		),
	}
	client, _ := newSingleSliceClient(t, spec)

	rows, err := SliceScanAs(context.Background(),
		client.Query("SELECT id, name FROM t").MaxRows(2),
		decodePerson)
	require.ErrorIs(t, err, types.ErrRowLimitExceeded)
	assert.Nil(t, rows,
		"helper discards the bounded-but-overflowing slice — caller never sees a partial typed result")
}

// ─────────────────────────────────────────────
// Chained options pass through (FallbackRead, MaxRows)
// ─────────────────────────────────────────────

func TestSliceScanAs_ChainedFallbackRead_AppliesThroughHelper(t *testing.T) {
	specA := &sliceSpec{cols: []string{"id", "name"}} // primary empty → triggers empty-retry
	specB := &sliceSpec{
		cols: []string{"id", "name"},
		rows: rowsOf([]any{9, "ivan"}, []any{10, "jane"}),
	}
	client, sa, sb := newSliceClient(t, specA, specB)

	rows, err := SliceScanAs(context.Background(),
		client.Query("SELECT id, name FROM t").FallbackRead(),
		decodePerson)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, sliceScanAsPerson{ID: 9, Name: "ivan"}, rows[0])
	assert.Equal(t, sliceScanAsPerson{ID: 10, Name: "jane"}, rows[1])

	assert.Equal(t, int32(1), sa.iterCtxCalls.Load(), "primary contacted via the helper's underlying SliceScan")
	assert.Equal(t, int32(1), sb.iterCtxCalls.Load(),
		"FallbackRead chained on the Query reached executeFallbackRead through the helper layer")
}

func TestSliceScanAs_ChainedMaxRowsOverflow_PropagatesErrRowLimitExceeded(t *testing.T) {
	// Beyond the basic MaxRowsOverflow test: locks that MaxRows still bounds
	// the drain when FallbackRead is chained on the SAME query. Proves the
	// two options compose without one's wiring (sliceMapFallbackOpts /
	// fallbackReadOptions threading) interfering with MaxRows propagation
	// through the helper to drainIterScanWithLimit.
	spec := &sliceSpec{
		cols: []string{"id", "name"},
		rows: rowsOf([]any{1, "a"}, []any{2, "b"}, []any{3, "c"}, []any{4, "d"}),
	}
	client, _ := newSingleSliceClient(t, spec)

	rows, err := SliceScanAs(context.Background(),
		client.Query("SELECT id, name FROM t").FallbackRead().MaxRows(3),
		decodePerson)
	require.ErrorIs(t, err, types.ErrRowLimitExceeded)
	assert.Nil(t, rows)
}

// ─────────────────────────────────────────────
// Nil-decode guard: no cluster contact
// ─────────────────────────────────────────────

func TestSliceScanAs_NilDecode_ReturnsError_NoClusterContact(t *testing.T) {
	spec := &sliceSpec{cols: []string{"id", "name"}, rows: rowsOf([]any{1, "a"})}
	client, sa := newSingleSliceClient(t, spec)

	rows, err := SliceScanAs[sliceScanAsPerson](context.Background(),
		client.Query("SELECT id, name FROM t"),
		nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode")
	assert.Nil(t, rows)

	assert.Equal(t, int32(0), sa.iterCalls.Load(), "no Iter() call")
	assert.Equal(t, int32(0), sa.iterCtxCalls.Load(),
		"no IterContext() call either — guard fires before SliceScanContext")
}

// TestSliceScanAs_NilQuery_ReturnsError_NoPanic is a regression test for
// slice_helpers.go:75 (nil-safety finding): SliceScanAs guarded a nil decode
// callback but not a nil Query, so q.SliceScanContext would panic on a
// nil-interface method call. Symmetric with the nil-decode guard above.
func TestSliceScanAs_NilQuery_ReturnsError_NoPanic(t *testing.T) {
	rows, err := SliceScanAs(context.Background(), nil, decodePerson)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query")
	assert.Nil(t, rows)
}

// ─────────────────────────────────────────────
// FallbackRead alt-leg composition: helper's "nil on err" must compose
// with the underlying SliceScan's scanFnInvokedOnAlt-driven propagation.
// Per plan §1901, §1927.
// ─────────────────────────────────────────────

func TestSliceScanAs_FallbackRead_AltInvokedDecodeThenErrored_ReturnsNilAndAltErr(t *testing.T) {
	iterErr := errors.New("alt iter boom after first row")
	specA := &sliceSpec{cols: []string{"id", "name"}} // primary empty → fallback fires
	specB := &sliceSpec{
		cols:       []string{"id", "name"},
		rows:       rowsOf([]any{9, "ivan"}),
		midDrainAt: 1, // row 1 succeeds, iter errors after
		midDrain:   iterErr,
	}
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}
	client, _, _ := newSliceClient(t, specA, specB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)

	rows, err := SliceScanAs(context.Background(),
		client.Query("SELECT id, name FROM t").FallbackRead(),
		decodePerson)
	require.ErrorIs(t, err, iterErr,
		"alt iter error propagates through SliceScan (scanFnInvokedOnAlt → propagateAltErr) and through the helper")
	assert.Nil(t, rows,
		"helper's nil-on-error contract discards the typed row that decode already appended internally")
}

func TestSliceScanAs_FallbackRead_AltDecodeErrorOnFirstRow_ReturnsNilErr(t *testing.T) {
	decodeErr := errors.New("decode rejected alt's first row")
	specA := &sliceSpec{cols: []string{"id", "name"}} // primary empty → fallback fires
	specB := &sliceSpec{
		cols: []string{"id", "name"},
		rows: rowsOf([]any{9, "ivan"}),
	}
	client, _, _ := newSliceClient(t, specA, specB)

	rows, err := SliceScanAs(context.Background(),
		client.Query("SELECT id, name FROM t").FallbackRead(),
		func(_ RowScanner, _ *sliceScanAsPerson) error { return decodeErr })
	require.ErrorIs(t, err, decodeErr,
		"first-row decode error on the alt path must propagate — NOT silently swallowed to (nil, nil)")
	assert.Nil(t, rows)
}
