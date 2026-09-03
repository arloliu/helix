package helix

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─────────────────────────────────────────────
// Phase 4: empty-result FallbackRead — the core feature
// ─────────────────────────────────────────────

func TestSliceMap_PrimaryEmpty_AltHasData_FallbackReadReturnsRows(t *testing.T) {
	specA := &sliceSpec{cols: []string{"id"}}
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

	rows, err := client.Query("SELECT id FROM t").FallbackRead().SliceMapContext(context.Background())
	require.NoError(t, err, "alt had data; public error must be nil")
	require.Len(t, rows, 2)
	assert.Equal(t, 9, rows[0]["id"])
	assert.Equal(t, 10, rows[1]["id"])

	assert.Equal(t, int32(1), sa.iterCtxCalls.Load(), "primary attempted once")
	assert.Equal(t, int32(1), sb.iterCtxCalls.Load(), "alt attempted exactly once via FallbackRead empty-retry")
	assert.Equal(t, int64(1), met.get(met.ReadTotal, ClusterB), "alt IncReadTotal recorded")
	assert.Equal(t, int64(1), met.get(met.ReadDivergence, ClusterA),
		"divergence fires on the primary (stale) cluster")
	assert.Empty(t, policy.RecordFailureCalls, "no failures — both clusters responded successfully")
}

func TestSliceMap_BothEmpty_ReturnsNilNil(t *testing.T) {
	specA := &sliceSpec{cols: []string{"id"}}
	specB := &sliceSpec{cols: []string{"id"}}
	met := newReadTestMetrics()
	client, sa, sb := newSliceClient(t, specA, specB, WithMetrics(met))

	rows, err := client.Query("SELECT id FROM t").FallbackRead().SliceMapContext(context.Background())
	require.NoError(t, err, "both-empty must NOT surface ErrNotFound to slice callers")
	assert.Nil(t, rows)

	assert.Equal(t, int32(1), sa.iterCtxCalls.Load(), "primary contacted")
	assert.Equal(t, int32(1), sb.iterCtxCalls.Load(), "alt also contacted (empty-retry fired)")
}

func TestSliceScan_PrimaryEmpty_AltHasData_FallbackReadInvokesAllCallbacks(t *testing.T) {
	specA := &sliceSpec{cols: []string{"id"}}
	specB := &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{9}, []any{10}, []any{11}),
	}
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}
	client, sa, sb := newSliceClient(t, specA, specB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)

	var seen []int
	rowCount, err := client.Query("SELECT id FROM t").FallbackRead().SliceScanContext(
		context.Background(), func(r RowScanner) error {
			var v int
			if serr := r.Scan(&v); serr != nil {
				return serr
			}
			seen = append(seen, v)
			return nil
		})
	require.NoError(t, err)
	assert.Equal(t, 3, rowCount, "callbacks invoked once per alt row")
	assert.Equal(t, []int{9, 10, 11}, seen)

	assert.Equal(t, int32(1), sa.iterCtxCalls.Load(), "primary attempted")
	assert.Equal(t, int32(1), sb.iterCtxCalls.Load(), "alt attempted via empty-retry")
	assert.Equal(t, int64(1), met.get(met.ReadDivergence, ClusterA),
		"divergence fires on primary when alt finds data")
}

func TestSliceScan_BothEmpty_ReturnsZeroNil(t *testing.T) {
	specA := &sliceSpec{cols: []string{"id"}}
	specB := &sliceSpec{cols: []string{"id"}}
	client, _, sb := newSliceClient(t, specA, specB)

	rowCount, err := client.Query("SELECT id FROM t").FallbackRead().SliceScanContext(
		context.Background(), func(_ RowScanner) error {
			t.Fatal("scanFn must not be invoked on a both-empty result")
			return nil
		})
	require.NoError(t, err, "both-empty must NOT surface ErrNotFound to slice callers")
	assert.Zero(t, rowCount)
	assert.Equal(t, int32(1), sb.iterCtxCalls.Load(), "alt contacted to confirm not-found")
}

// ─────────────────────────────────────────────
// Phase 4: propagateAltErr — SliceMap
// ─────────────────────────────────────────────

// canceledContext returns a context that is already done, standing in for
// a caller that gave up. The slice fixtures ignore the context, so the
// scripted rows still play back.
func canceledContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	return ctx
}

func TestSliceMap_AltCtxCanceled_PropagatesWithoutHealthImpact(t *testing.T) {
	specA := &sliceSpec{cols: []string{"id"}}
	specB := &sliceSpec{cols: []string{"id"}, closeErr: context.Canceled}
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}
	client, _, _ := newSliceClient(t, specA, specB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)

	rows, err := client.Query("SELECT id FROM t").FallbackRead().SliceMapContext(canceledContext(t))
	require.ErrorIs(t, err, context.Canceled, "ctx errors on the alt leg propagate to caller")
	assert.Nil(t, rows, "discard-on-error contract: no partial rows leak")

	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterB),
		"ctx errors are caller-driven → no IncReadError on alt")
	assert.Empty(t, policy.RecordFailureCalls,
		"ctx errors on alt do not advance per-cluster health")
}

func TestSliceMap_AltRealError_SuppressedAndHealthRecorded(t *testing.T) {
	realErr := errors.New("alt boom")
	specA := &sliceSpec{cols: []string{"id"}}
	specB := &sliceSpec{cols: []string{"id"}, closeErr: realErr}
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}
	client, _, _ := newSliceClient(t, specA, specB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)

	rows, err := client.Query("SELECT id FROM t").FallbackRead().SliceMapContext(context.Background())
	require.NoError(t, err, "real alt error must NOT decrease availability: suppressed to (nil, nil)")
	assert.Nil(t, rows)

	assert.Equal(t, int64(1), met.get(met.ReadErrors, ClusterB),
		"real alt errors record health (IncReadError) on alt")
	assert.Equal(t, []ClusterID{ClusterB}, policy.RecordFailureCalls,
		"RecordFailure recorded on alt for real error")
}

// TestSliceMap_AltRowLimitExceeded_PropagatesAsIs is a brief assertion
// that ErrRowLimitExceeded continues to propagate from the alt leg, locking
// the behavior already exercised end-to-end by cql_row_limit_test.go. It
// also doubles as a regression that the new sliceMapFallbackOpts predicates
// do not interfere with executeFallbackRead's pre-existing ErrRowLimitExceeded
// branch (which fires before nonHealthAltErr / propagateAltErr are consulted).
func TestSliceMap_AltRowLimitExceeded_PropagatesAsIs(t *testing.T) {
	specA := &sliceSpec{cols: []string{"id"}}
	specB := &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{1}, []any{2}, []any{3}),
	}
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}
	client, _, _ := newSliceClient(t, specA, specB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)

	rows, err := client.Query("SELECT id FROM t").
		FallbackRead().
		MaxRows(2).
		SliceMapContext(context.Background())
	require.ErrorIs(t, err, types.ErrRowLimitExceeded,
		"ErrRowLimitExceeded from alt must propagate unchanged to caller")
	assert.Nil(t, rows, "discard contract on overflow")

	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterB),
		"row-limit is a data sentinel, not a fault — no IncReadError")
	assert.Empty(t, policy.RecordFailureCalls,
		"row-limit must NOT advance failover policy state")
}

// ─────────────────────────────────────────────
// Phase 4: propagateAltErr — SliceScan (scanFnInvokedOnAlt closure)
// ─────────────────────────────────────────────

func TestSliceScan_AltScanFnSucceededThenIterErrors_PropagatesWithRowCount(t *testing.T) {
	iterErr := errors.New("alt iter boom")
	specA := &sliceSpec{cols: []string{"id"}}
	specB := &sliceSpec{
		cols:       []string{"id"},
		rows:       rowsOf([]any{9}),
		midDrainAt: 1, // first row succeeds, then iter surfaces midDrain
		midDrain:   iterErr,
	}
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}
	client, _, _ := newSliceClient(t, specA, specB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)

	rowCount, err := client.Query("SELECT id FROM t").FallbackRead().SliceScanContext(
		context.Background(), func(r RowScanner) error {
			var v int
			return r.Scan(&v)
		})
	require.ErrorIs(t, err, iterErr,
		"scanFnInvokedOnAlt → propagateAltErr returns true → alt error reaches caller")
	assert.Equal(t, 1, rowCount, "rowCount reflects the one successful callback before alt iter errored")

	assert.Equal(t, int64(1), met.get(met.ReadErrors, ClusterB),
		"real (non-ctx) iter error records health on alt")
	assert.Equal(t, []ClusterID{ClusterB}, policy.RecordFailureCalls)
}

func TestSliceScan_AltScanFnReturnsError_PropagatesEvenIfFirstRow(t *testing.T) {
	cbErr := errors.New("alt callback rejected row")
	specA := &sliceSpec{cols: []string{"id"}}
	specB := &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{9}),
	}
	client, _, _ := newSliceClient(t, specA, specB)

	rowCount, err := client.Query("SELECT id FROM t").FallbackRead().SliceScanContext(
		context.Background(), func(_ RowScanner) error { return cbErr })
	require.ErrorIs(t, err, cbErr,
		"scanFnInvokedOnAlt fires before the user callback runs, so a scanFn error counts as an alt invocation")
	assert.Zero(t, rowCount, "rowCount counts SUCCESSFUL callbacks only")
}

func TestSliceScan_AltRealErrorBeforeScanFn_SuppressedToZeroNil(t *testing.T) {
	realErr := errors.New("alt boom before any row")
	specA := &sliceSpec{cols: []string{"id"}}
	specB := &sliceSpec{cols: []string{"id"}, closeErr: realErr}
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}
	client, _, _ := newSliceClient(t, specA, specB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)

	rowCount, err := client.Query("SELECT id FROM t").FallbackRead().SliceScanContext(
		context.Background(), func(_ RowScanner) error {
			t.Fatal("scanFn must not be invoked when alt errors before any row")
			return nil
		})
	require.NoError(t, err,
		"alt failed before scanFn → propagateAltErr false → suppressed to ErrNotFound → public (0, nil)")
	assert.Zero(t, rowCount)

	assert.Equal(t, int64(1), met.get(met.ReadErrors, ClusterB),
		"real alt errors record health even when propagation is suppressed")
	assert.Equal(t, []ClusterID{ClusterB}, policy.RecordFailureCalls)
}

func TestSliceScan_AltCtxCanceledBeforeScanFn_PropagatesWithoutHealthImpact(t *testing.T) {
	specA := &sliceSpec{cols: []string{"id"}}
	specB := &sliceSpec{cols: []string{"id"}, closeErr: context.Canceled}
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}
	client, _, _ := newSliceClient(t, specA, specB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)

	rowCount, err := client.Query("SELECT id FROM t").FallbackRead().SliceScanContext(
		canceledContext(t), func(_ RowScanner) error {
			t.Fatal("scanFn must not be invoked when alt errors before any row")
			return nil
		})
	require.ErrorIs(t, err, context.Canceled, "ctx errors propagate even without scanFn invocation")
	assert.Zero(t, rowCount)

	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterB),
		"ctx errors are caller-driven — no IncReadError on alt")
	assert.Empty(t, policy.RecordFailureCalls)
}

func TestSliceScan_AltCtxCanceledAfterScanFn_PropagatesViaScanFnInvokedFlag(t *testing.T) {
	specA := &sliceSpec{cols: []string{"id"}}
	specB := &sliceSpec{
		cols:       []string{"id"},
		rows:       rowsOf([]any{9}),
		midDrainAt: 1,
		midDrain:   context.Canceled,
	}
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}
	client, _, _ := newSliceClient(t, specA, specB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)

	// The caller gives up while the alt probe is streaming: the context is
	// cancelled from inside the first callback, and the fixture then fails
	// the drain with the context error.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	rowCount, err := client.Query("SELECT id FROM t").FallbackRead().SliceScanContext(
		ctx, func(r RowScanner) error {
			var v int
			cancel()
			return r.Scan(&v)
		})
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, rowCount, "rowCount reflects the one successful callback before ctx fired")

	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterB),
		"ctx still classified as nonHealth even though scanFnInvokedOnAlt drove propagation")
	assert.Empty(t, policy.RecordFailureCalls)
}

// ─────────────────────────────────────────────
// Phase 4: skipDrainingAlt (slice-only; Scan regression is locked by
// TestFallback_DrainStateBypass in cql_fallback_test.go)
// ─────────────────────────────────────────────

func TestSliceMap_PrimaryEmpty_AltDraining_SkipsAltAndReturnsNilNil(t *testing.T) {
	specA := &sliceSpec{cols: []string{"id"}}
	specB := &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{9}), // alt has data but is draining — must NOT be contacted
	}
	watcher := newMockTopologyWatcher()
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}
	client, _, sb := newSliceClient(t, specA, specB,
		WithTopologyWatcher(watcher),
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)
	watcher.SetDrain(ClusterB, true)
	waitDraining(t, client, ClusterB)

	rows, err := client.Query("SELECT id FROM t").FallbackRead().SliceMapContext(context.Background())
	require.NoError(t, err, "drain-skip returns ErrNotFound internally → public (nil, nil)")
	assert.Nil(t, rows)

	assert.Equal(t, int32(0), sb.iterCtxCalls.Load(), "alt MUST NOT be contacted when draining")
	assert.Equal(t, int64(0), met.get(met.ReadTotal, ClusterB), "no alt IncReadTotal — drain skip is silent")
	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterB))
	assert.Equal(t, int64(0), met.get(met.ReadDivergence, ClusterA),
		"divergence must NOT fire — alt was never probed, so we don't know if it diverged")
	assert.Empty(t, policy.RecordFailureCalls, "drain skip is a routing decision, not a fault")
}

func TestSliceScan_PrimaryEmpty_AltDraining_SkipsAltAndReturnsZeroNil(t *testing.T) {
	specA := &sliceSpec{cols: []string{"id"}}
	specB := &sliceSpec{cols: []string{"id"}, rows: rowsOf([]any{9})}
	watcher := newMockTopologyWatcher()
	met := newReadTestMetrics()
	client, _, sb := newSliceClient(t, specA, specB,
		WithTopologyWatcher(watcher),
		WithMetrics(met),
	)
	watcher.SetDrain(ClusterB, true)
	waitDraining(t, client, ClusterB)

	rowCount, err := client.Query("SELECT id FROM t").FallbackRead().SliceScanContext(
		context.Background(), func(_ RowScanner) error {
			t.Fatal("scanFn must not run when alt was drain-skipped")
			return nil
		})
	require.NoError(t, err)
	assert.Zero(t, rowCount)

	assert.Equal(t, int32(0), sb.iterCtxCalls.Load(), "alt MUST NOT be contacted when draining")
	assert.Equal(t, int64(0), met.get(met.ReadTotal, ClusterB))
}

// ─────────────────────────────────────────────
// Phase 4 fix: user-callback ErrNotFound must not be misclassified as the
// synthetic "drained 0 rows" signal. Without the scanFnNotFoundShieldError, the
// pipeline would silently translate the caller's intentional ErrNotFound
// to (0, nil) (single-cluster) or invoke fallback (dual-cluster).
// ─────────────────────────────────────────────

func TestSliceScan_ScanFnReturnsErrNotFound_PropagatesToCallerSingleCluster(t *testing.T) {
	spec := &sliceSpec{cols: []string{"id"}, rows: rowsOf([]any{1})}
	met := newReadTestMetrics()
	client, _ := newSingleSliceClient(t, spec)
	_ = met // single-cluster has no failover policy

	rowCount, err := client.Query("SELECT id FROM t").SliceScanContext(
		context.Background(), func(_ RowScanner) error { return types.ErrNotFound })
	require.ErrorIs(t, err, types.ErrNotFound,
		"user scanFn returning ErrNotFound must reach the caller, NOT be silently swallowed to (0, nil)")
	assert.Zero(t, rowCount)
}

func TestSliceScan_ScanFnReturnsWrappedErrNotFound_PropagatesToCaller(t *testing.T) {
	spec := &sliceSpec{cols: []string{"id"}, rows: rowsOf([]any{1})}
	client, _ := newSingleSliceClient(t, spec)

	wrapped := fmt.Errorf("custom message: %w", types.ErrNotFound)
	rowCount, err := client.Query("SELECT id FROM t").SliceScanContext(
		context.Background(), func(_ RowScanner) error { return wrapped })
	require.ErrorIs(t, err, types.ErrNotFound,
		"wrapped ErrNotFound (errors.Is match in chain) must also be shielded and propagated")
	assert.Contains(t, err.Error(), "custom message",
		"the caller's wrapped error including its custom message must survive the shield round-trip")
	assert.Zero(t, rowCount)
}

func TestSliceScan_ScanFnReturnsErrNotFound_DualCluster_DoesNotInvokeAlt(t *testing.T) {
	specA := &sliceSpec{cols: []string{"id"}, rows: rowsOf([]any{1})}
	specB := &sliceSpec{cols: []string{"id"}, rows: rowsOf([]any{9})}
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}
	client, _, sb := newSliceClient(t, specA, specB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)

	rowCount, err := client.Query("SELECT id FROM t").FallbackRead().SliceScanContext(
		context.Background(), func(_ RowScanner) error { return types.ErrNotFound })
	require.ErrorIs(t, err, types.ErrNotFound,
		"user-callback ErrNotFound is a real scanFn error, not the empty signal — must propagate")
	assert.Zero(t, rowCount)

	assert.Equal(t, int32(0), sb.iterCtxCalls.Load(),
		"shield prevents the wrapper from misclassifying the user error as the empty signal; alt must NOT be contacted")
	// Regression for the codebase-review nil/error-handling audit
	// (finding cql_client.go:2941): a business-logic ErrNotFound from the
	// caller's own scanFn is a data sentinel, not a primary-cluster fault —
	// it must NOT record health, matching every other ErrNotFound path in
	// recordOpOutcomeAt. Superseded the older "record health on the primary"
	// expectation from plan §1547-1549, which did not anticipate this
	// interaction with the shield.
	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterA),
		"scanFn-returned ErrNotFound must NOT record health on the primary")
	assert.Empty(t, policy.RecordFailureCalls,
		"scanFn-returned ErrNotFound must NOT advance FailoverPolicy state")
}

func TestSliceScan_AltScanFnReturnsErrNotFound_PropagatesViaShield(t *testing.T) {
	specA := &sliceSpec{cols: []string{"id"}} // primary empty → triggers empty-retry
	specB := &sliceSpec{cols: []string{"id"}, rows: rowsOf([]any{9})}
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}
	client, _, _ := newSliceClient(t, specA, specB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)

	rowCount, err := client.Query("SELECT id FROM t").FallbackRead().SliceScanContext(
		context.Background(), func(_ RowScanner) error { return types.ErrNotFound })
	require.ErrorIs(t, err, types.ErrNotFound,
		"alt-leg user-callback ErrNotFound must propagate even when scanFnInvokedOnAlt is the trigger")
	assert.Zero(t, rowCount, "rowCount counts only successful callbacks; the ErrNotFound was returned on the first row")

	// Regression for the codebase-review nil/error-handling audit
	// (finding cql_client.go:2941): same rationale as the primary-leg case
	// above, applied to the alt leg of executeFallbackRead.
	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterB),
		"scanFn-returned ErrNotFound on alt must NOT record health")
	assert.Empty(t, policy.RecordFailureCalls,
		"scanFn-returned ErrNotFound on alt must NOT advance FailoverPolicy state")
}

// ─────────────────────────────────────────────
// Phase 4 LOWs from GPT review: single-cluster bypass + SliceScan row-limit
// ─────────────────────────────────────────────

func TestSliceMap_SingleCluster_FallbackRead_NoOp(t *testing.T) {
	spec := &sliceSpec{cols: []string{"id"}}
	client, sa := newSingleSliceClient(t, spec)

	rows, err := client.Query("SELECT id FROM t").FallbackRead().SliceMapContext(context.Background())
	require.NoError(t, err, "single-cluster empty must return (nil, nil) without trying to fall back")
	assert.Nil(t, rows)
	assert.Equal(t, int32(1), sa.iterCtxCalls.Load(), "primary attempted exactly once")
}

func TestSliceScan_SingleCluster_FallbackRead_NoOp(t *testing.T) {
	spec := &sliceSpec{cols: []string{"id"}}
	client, sa := newSingleSliceClient(t, spec)

	rowCount, err := client.Query("SELECT id FROM t").FallbackRead().SliceScanContext(
		context.Background(), func(_ RowScanner) error {
			t.Fatal("scanFn must not be invoked")
			return nil
		})
	require.NoError(t, err)
	assert.Zero(t, rowCount)
	assert.Equal(t, int32(1), sa.iterCtxCalls.Load())
}

func TestSliceScan_AltRowLimitExceeded_PropagatesAsIs(t *testing.T) {
	specA := &sliceSpec{cols: []string{"id"}} // primary empty → triggers empty-retry
	specB := &sliceSpec{cols: []string{"id"}, rows: rowsOf([]any{1}, []any{2}, []any{3})}
	met := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}
	client, _, _ := newSliceClient(t, specA, specB,
		WithMetrics(met),
		WithFailoverPolicy(policy),
	)

	rowCount, err := client.Query("SELECT id FROM t").
		FallbackRead().
		MaxRows(2).
		SliceScanContext(context.Background(), func(r RowScanner) error {
			var v int
			return r.Scan(&v)
		})
	require.ErrorIs(t, err, types.ErrRowLimitExceeded,
		"alt's MaxRows overflow propagates unchanged — pre-real-error branch wins over propagateAltErr predicate")
	assert.Equal(t, 2, rowCount, "rowCount holds the bounded count from alt before the (N+1)th detection")

	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterB),
		"row-limit is a data sentinel — no IncReadError on alt")
	assert.Empty(t, policy.RecordFailureCalls)
}

// ─────────────────────────────────────────────
// Phase 4: PageState + FallbackRead disabled (regression — locks the
// sliceReadOpts short-circuit now that FallbackRead is fully wired)
// ─────────────────────────────────────────────

func TestSliceMap_PageState_FallbackReadChained_EmptyResultDoesNotContactAlt(t *testing.T) {
	specA := &sliceSpec{cols: []string{"id"}}
	specB := &sliceSpec{
		cols: []string{"id"},
		rows: rowsOf([]any{9}, []any{10}),
	}
	met := newReadTestMetrics()
	client, _, sb := newSliceClient(t, specA, specB, WithMetrics(met))

	rows, err := client.Query("SELECT id FROM t").
		FallbackRead().
		PageState([]byte("cursor")).
		SliceMapContext(context.Background())
	require.NoError(t, err,
		"PageState short-circuit suppresses opts.fallbackRead → primary's empty result returns as-is")
	assert.Nil(t, rows)

	assert.Equal(t, int32(0), sb.iterCtxCalls.Load(),
		"alt MUST NOT be contacted: shipping an opaque PageState cursor cross-cluster is unsound")
	assert.Equal(t, int64(0), met.get(met.ReadTotal, ClusterB))
}
