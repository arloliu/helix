package helix

import (
	"context"
	"math"
	"testing"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─────────────────────────────────────────────
// MaxRows validation panics
// ─────────────────────────────────────────────

func TestMaxRows_NegativeValue_Panics(t *testing.T) {
	q := &cqlQuery{}
	assert.Panics(t, func() { q.MaxRows(-1) })
}

func TestMaxRows_MaxInt32_Panics(t *testing.T) {
	q := &cqlQuery{}
	assert.Panics(t, func() { q.MaxRows(math.MaxInt32) })
}

func TestMaxRows_MaxInt32MinusOne_DoesNotPanic(t *testing.T) {
	q := &cqlQuery{}
	assert.NotPanics(t, func() { q.MaxRows(math.MaxInt32 - 1) })
	require.NotNil(t, q.maxRows)
	assert.Equal(t, math.MaxInt32-1, *q.maxRows)
}

// ─────────────────────────────────────────────
// WithDefaultMaxRows option validation
// ─────────────────────────────────────────────

func TestWithDefaultMaxRows_NegativeValue_ReturnsOptionError(t *testing.T) {
	_, err := NewCQLClient(newMockSession(), nil, WithDefaultMaxRows(-1))
	require.Error(t, err)
	assert.True(t, types.IsOptionError(err))
	assert.ErrorContains(t, err, "WithDefaultMaxRows")
}

func TestWithDefaultMaxRows_MaxInt32_ReturnsOptionError(t *testing.T) {
	_, err := NewCQLClient(newMockSession(), nil, WithDefaultMaxRows(math.MaxInt32))
	require.Error(t, err)
	assert.True(t, types.IsOptionError(err))
	assert.ErrorContains(t, err, "WithDefaultMaxRows")
}

func TestWithDefaultMaxRows_Zero_Accepted(t *testing.T) {
	client, err := NewCQLClient(newMockSession(), nil, WithDefaultMaxRows(0))
	require.NoError(t, err)
	client.Close()
}

func TestWithDefaultMaxRows_ValidPositive_Accepted(t *testing.T) {
	client, err := NewCQLClient(newMockSession(), nil, WithDefaultMaxRows(100))
	require.NoError(t, err)
	client.Close()
}

func TestWithDefaultMaxRows_MaxInt32MinusOne_Accepted(t *testing.T) {
	client, err := NewCQLClient(newMockSession(), nil, WithDefaultMaxRows(math.MaxInt32-1))
	require.NoError(t, err)
	client.Close()
}

// ─────────────────────────────────────────────
// DefaultMaxRows precedence
// ─────────────────────────────────────────────

// TestSliceMap_DefaultMaxRows_EnforcedWhenPerQueryZero verifies that
// Config.DefaultMaxRows is applied when the per-query override is 0 (cleared).
func TestSliceMap_DefaultMaxRows_EnforcedWhenPerQueryZero(t *testing.T) {
	spec := &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{1}, []any{2}, []any{3}),
	}
	sa := &sliceTestSession{spec: spec}
	client, err := NewCQLClient(sa, nil, WithDefaultMaxRows(2))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	// MaxRows(0) clears the per-query override; DefaultMaxRows(2) applies.
	rows, rerr := client.Query("SELECT id FROM t").MaxRows(0).SliceMapContext(context.Background())
	require.ErrorIs(t, rerr, types.ErrRowLimitExceeded)
	assert.Nil(t, rows)
}

// TestSliceMap_PerQueryMaxRows_WinsOverDefault verifies that a per-query
// MaxRows(N) takes precedence over Config.DefaultMaxRows.
// DefaultMaxRows(10) admits all 3 rows on its own; only MaxRows(2) overflows.
func TestSliceMap_PerQueryMaxRows_WinsOverDefault(t *testing.T) {
	spec := &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{1}, []any{2}, []any{3}),
	}
	sa := &sliceTestSession{spec: spec}
	client, err := NewCQLClient(sa, nil, WithDefaultMaxRows(10))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	rows, rerr := client.Query("SELECT id FROM t").MaxRows(2).SliceMapContext(context.Background())
	require.ErrorIs(t, rerr, types.ErrRowLimitExceeded,
		"per-query MaxRows(2) must win; DefaultMaxRows(10) would admit all 3 rows")
	assert.Nil(t, rows)
}

// TestSliceMap_DefaultMaxRows_NotAppliedWhenPerQuerySet verifies that when
// a per-query MaxRows(N) is set, DefaultMaxRows is NOT consulted.
func TestSliceMap_DefaultMaxRows_NotAppliedWhenPerQuerySet(t *testing.T) {
	spec := &sliceSpec{
		cols: []string{"id"},
		// 4 rows: DefaultMaxRows(2) would overflow; per-query MaxRows(5) admits all.
		rows: rowsOf([]any{1}, []any{2}, []any{3}, []any{4}),
	}
	sa := &sliceTestSession{spec: spec}
	client, err := NewCQLClient(sa, nil, WithDefaultMaxRows(2))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	rows, rerr := client.Query("SELECT id FROM t").MaxRows(5).SliceMapContext(context.Background())
	require.NoError(t, rerr, "per-query MaxRows(5) > 4 rows: no overflow")
	require.Len(t, rows, 4)
}

// ─────────────────────────────────────────────
// SliceMap overflow — standard-failover and no-failover paths
// ─────────────────────────────────────────────

// TestSliceMap_MaxRows_Overflow_StandardFailoverPath verifies that
// ErrRowLimitExceeded from primary does NOT trigger failover and propagates
// as-is through the executeRead path.
func TestSliceMap_MaxRows_Overflow_StandardFailoverPath(t *testing.T) {
	specA := &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{1}, []any{2}, []any{3}),
	}
	specB := &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{9}),
	}
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}
	client, _, sb := newSliceClient(t, specA, specB, WithFailoverPolicy(policy))

	rows, err := client.Query("SELECT id FROM t").MaxRows(2).SliceMapContext(context.Background())
	require.ErrorIs(t, err, types.ErrRowLimitExceeded)
	assert.Nil(t, rows, "discard contract holds on overflow")

	assert.Equal(t, int32(0), sb.iterCtxCalls.Load(),
		"ErrRowLimitExceeded must not trigger failover to alt cluster")
	assert.Empty(t, policy.RecordFailureCalls,
		"row-limit overflow is not a health failure; RecordFailure must not fire")
}

// TestSliceMap_MaxRows_Overflow_NoFailoverPath verifies that ErrRowLimitExceeded
// propagates correctly through the executeReadNoFailover path (PageState set).
func TestSliceMap_MaxRows_Overflow_NoFailoverPath(t *testing.T) {
	specA := &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{1}, []any{2}, []any{3}),
	}
	specB := &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{9}),
	}
	client, _, sb := newSliceClient(t, specA, specB)

	rows, err := client.Query("SELECT id FROM t").
		PageState([]byte("cursor")).
		MaxRows(2).
		SliceMapContext(context.Background())
	require.ErrorIs(t, err, types.ErrRowLimitExceeded)
	assert.Nil(t, rows)

	assert.Equal(t, int32(0), sb.iterCtxCalls.Load(),
		"no-failover path must not contact alt")
}

// ─────────────────────────────────────────────
// SliceScan overflow
// ─────────────────────────────────────────────

// TestSliceScan_MaxRows_Overflow_ReturnsRowCountAndSentinel verifies that
// SliceScan returns (N, ErrRowLimitExceeded) where N is the number of
// successful scanFn invocations before the bound was hit.
func TestSliceScan_MaxRows_Overflow_ReturnsRowCountAndSentinel(t *testing.T) {
	spec := &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{1}, []any{2}, []any{3}, []any{4}),
	}
	client, _ := newSingleSliceClient(t, spec)

	var got []int
	rowCount, err := client.Query("SELECT id FROM t").MaxRows(2).SliceScanContext(
		context.Background(),
		func(r RowScanner) error {
			var v int
			if e := r.Scan(&v); e != nil {
				return e
			}
			got = append(got, v)
			return nil
		},
	)
	require.ErrorIs(t, err, types.ErrRowLimitExceeded)
	assert.Equal(t, 2, rowCount, "rowCount = number of successful callbacks before abort")
	assert.Equal(t, []int{1, 2}, got)
}

// TestSliceScan_DefaultMaxRows_EnforcedWhenPerQueryZero verifies that
// Config.DefaultMaxRows bounds SliceScan when no per-query MaxRows is set.
// Returns (N, ErrRowLimitExceeded) where N is the DefaultMaxRows bound.
func TestSliceScan_DefaultMaxRows_EnforcedWhenPerQueryZero(t *testing.T) {
	spec := &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{1}, []any{2}, []any{3}, []any{4}),
	}
	sa := &sliceTestSession{spec: spec}
	client, err := NewCQLClient(sa, nil, WithDefaultMaxRows(2))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	var got []int
	rowCount, rerr := client.Query("SELECT id FROM t").SliceScanContext(
		context.Background(),
		func(r RowScanner) error {
			var v int
			if e := r.Scan(&v); e != nil {
				return e
			}
			got = append(got, v)
			return nil
		},
	)
	require.ErrorIs(t, rerr, types.ErrRowLimitExceeded)
	assert.Equal(t, 2, rowCount, "rowCount = DefaultMaxRows (number of successful callbacks before abort)")
	assert.Equal(t, []int{1, 2}, got)
}

// ─────────────────────────────────────────────
// Page-size clamp
// ─────────────────────────────────────────────

// TestSliceMap_PageSizeClamp_UserPageSizeClamped verifies that when both
// PageSize and MaxRows are set, the underlying cql.Query receives
// min(userPageSize, MaxRows+1), not the user's raw page size.
func TestSliceMap_PageSizeClamp_UserPageSizeClamped(t *testing.T) {
	spec := &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{1}),
	}
	sa := &sliceTestSession{spec: spec}
	client, err := NewCQLClient(sa, nil)
	require.NoError(t, err)
	defer client.Close()

	_, _ = client.Query("SELECT id FROM t").PageSize(100_000).MaxRows(1000).SliceMapContext(context.Background())

	require.NotNil(t, sa.lastQuery, "session.Query must have been called")
	require.NotNil(t, sa.lastQuery.lastPageSize,
		"page size must have been set by the clamp")
	assert.Equal(t, 1001, *sa.lastQuery.lastPageSize,
		"clamp = min(100_000, 1000+1) = 1001")
}

// TestSliceMap_PageSizeClamp_NoUserPageSize verifies that when the user did
// not set PageSize but MaxRows is active, the clamp sets PageSize = MaxRows+1.
func TestSliceMap_PageSizeClamp_NoUserPageSize(t *testing.T) {
	spec := &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{1}),
	}
	sa := &sliceTestSession{spec: spec}
	client, err := NewCQLClient(sa, nil)
	require.NoError(t, err)
	defer client.Close()

	_, _ = client.Query("SELECT id FROM t").MaxRows(1000).SliceMapContext(context.Background())

	require.NotNil(t, sa.lastQuery, "session.Query must have been called")
	require.NotNil(t, sa.lastQuery.lastPageSize,
		"page size must have been set by the clamp")
	assert.Equal(t, 1001, *sa.lastQuery.lastPageSize,
		"clamp sets PageSize = MaxRows+1 = 1001 when user did not set PageSize")
}

// TestSliceMap_PageSizeClamp_MaxRowsUnset_PageSizeUntouched verifies that
// when MaxRows is 0 (unset), the page-size clamp is not applied and the
// underlying query's page size is left at whatever applyConfig set.
func TestSliceMap_PageSizeClamp_MaxRowsUnset_PageSizeUntouched(t *testing.T) {
	spec := &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{1}),
	}
	sa := &sliceTestSession{spec: spec}
	client, err := NewCQLClient(sa, nil)
	require.NoError(t, err)
	defer client.Close()

	_, _ = client.Query("SELECT id FROM t").SliceMapContext(context.Background())

	require.NotNil(t, sa.lastQuery, "session.Query must have been called")
	assert.Nil(t, sa.lastQuery.lastPageSize,
		"when MaxRows is unset, page size must be left untouched (nil = driver default)")
}

// TestSliceScan_PageSizeClamp_NoUserPageSize verifies the clamp also applies
// to SliceScan, not just SliceMap.
func TestSliceScan_PageSizeClamp_NoUserPageSize(t *testing.T) {
	spec := &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{1}),
	}
	sa := &sliceTestSession{spec: spec}
	client, err := NewCQLClient(sa, nil)
	require.NoError(t, err)
	defer client.Close()

	_, _ = client.Query("SELECT id FROM t").MaxRows(500).SliceScanContext(
		context.Background(),
		func(_ RowScanner) error { return nil },
	)

	require.NotNil(t, sa.lastQuery, "session.Query must have been called")
	require.NotNil(t, sa.lastQuery.lastPageSize,
		"page size must have been set by the clamp")
	assert.Equal(t, 501, *sa.lastQuery.lastPageSize)
}

// TestSliceMap_PageSizeClamp_UserPageSizeSmallerThanMaxRows verifies that
// when the user's PageSize is already smaller than MaxRows+1, the clamp does
// not increase it — min semantics hold.
func TestSliceMap_PageSizeClamp_UserPageSizeSmallerThanMaxRows(t *testing.T) {
	spec := &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{1}),
	}
	sa := &sliceTestSession{spec: spec}
	client, err := NewCQLClient(sa, nil)
	require.NoError(t, err)
	defer client.Close()

	// PageSize(10) < MaxRows(1000)+1=1001 → clamp keeps 10.
	_, _ = client.Query("SELECT id FROM t").PageSize(10).MaxRows(1000).SliceMapContext(context.Background())

	require.NotNil(t, sa.lastQuery)
	require.NotNil(t, sa.lastQuery.lastPageSize)
	assert.Equal(t, 10, *sa.lastQuery.lastPageSize,
		"clamp = min(10, 1001) = 10; user's page size is the binding constraint")
}
