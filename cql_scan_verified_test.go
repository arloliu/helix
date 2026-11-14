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

// errVerifyCluster stands for a cluster fault on one read leg.
var errVerifyCluster = errors.New("cluster unreachable")

// verifyLeg is what one cluster answers: a row holding val, no row, or err.
type verifyLeg struct {
	val string
	err error
}

func (l verifyLeg) spec() *sliceSpec {
	spec := &sliceSpec{cols: []string{"v"}, closeErr: l.err}
	if l.err == nil && l.val != "" {
		spec.rows = [][]any{{l.val}}
	}

	return spec
}

// acceptNew scans the row into a local, records it in *seen, and accepts
// only the value "new", storing it in *got.
func acceptNew(got *string, seen *[]string) func(RowScanner) bool {
	return func(r RowScanner) bool {
		var v string
		if r.Scan(&v) != nil {
			return false
		}
		*seen = append(*seen, v)
		if v != "new" {
			return false
		}
		*got = v

		return true
	}
}

// TestScanVerified_Outcomes pins the result, the row the callback kept,
// the rows it was shown, and the reads each cluster received,
// for every combination of what the selected cluster (A) and the alternative (B) answer.
func TestScanVerified_Outcomes(t *testing.T) {
	notFound := verifyLeg{}
	down := verifyLeg{err: errVerifyCluster}

	tests := []struct {
		name         string
		a, b         verifyLeg
		wantErr      error
		wantGot      string
		wantSeen     []string
		wantReadsB   int32
		wantDiverged int64
	}{
		{name: "selected accepted", a: verifyLeg{val: "new"}, b: verifyLeg{val: "other"}, wantGot: "new", wantSeen: []string{"new"}},
		{
			name: "selected rejected, alternative accepted", a: verifyLeg{val: "old"}, b: verifyLeg{val: "new"},
			wantGot: "new", wantSeen: []string{"old", "new"}, wantReadsB: 1, wantDiverged: 1,
		},
		{
			name: "both rejected", a: verifyLeg{val: "old"}, b: verifyLeg{val: "older"},
			wantErr: types.ErrReadRejected, wantSeen: []string{"old", "older"}, wantReadsB: 1,
		},
		{
			name: "selected rejected, alternative not found", a: verifyLeg{val: "old"}, b: notFound,
			wantErr: types.ErrReadRejected, wantSeen: []string{"old"}, wantReadsB: 1,
		},
		{
			name: "selected rejected, alternative down", a: verifyLeg{val: "old"}, b: down,
			wantErr: types.ErrReadRejected, wantSeen: []string{"old"}, wantReadsB: 1,
		},
		{
			name: "selected not found, alternative accepted", a: notFound, b: verifyLeg{val: "new"},
			wantGot: "new", wantSeen: []string{"new"}, wantReadsB: 1, wantDiverged: 1,
		},
		{
			name: "selected not found, alternative rejected", a: notFound, b: verifyLeg{val: "old"},
			wantErr: types.ErrReadRejected, wantSeen: []string{"old"}, wantReadsB: 1,
		},
		{name: "both not found", a: notFound, b: notFound, wantErr: types.ErrNotFound, wantReadsB: 1},
		{name: "selected not found, alternative down", a: notFound, b: down, wantErr: types.ErrNotFound, wantReadsB: 1},
		{
			name: "selected down, failover accepted", a: down, b: verifyLeg{val: "new"},
			wantGot: "new", wantSeen: []string{"new"}, wantReadsB: 1,
		},
		{
			name: "selected down, failover rejected", a: down, b: verifyLeg{val: "old"},
			wantErr: types.ErrReadRejected, wantSeen: []string{"old"}, wantReadsB: 1,
		},
		{name: "selected down, failover not found", a: down, b: notFound, wantErr: types.ErrNotFound, wantReadsB: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			met := newReadTestMetrics()
			client, sessionA, sessionB := newSliceClient(t, tt.a.spec(), tt.b.spec(), WithMetrics(met))

			var got string
			var seen []string
			scanErr := client.Query("SELECT v").ScanVerified(acceptNew(&got, &seen))

			if tt.wantErr == nil {
				require.NoError(t, scanErr)
			} else {
				require.ErrorIs(t, scanErr, tt.wantErr)
			}
			assert.Equal(t, tt.wantGot, got, "row the callback kept")
			assert.Equal(t, tt.wantSeen, seen, "rows the callback was shown")
			assert.Equal(t, int32(1), sessionA.iterCtxCalls.Load(), "the selected cluster is read exactly once")
			assert.Equal(t, tt.wantReadsB, sessionB.iterCtxCalls.Load(), "reads on the alternative")
			assert.Equal(t, tt.wantDiverged, met.get(met.ReadDivergence, ClusterA), "divergence on the selected cluster")
			assert.Equal(t, int64(0), met.get(met.ReadDivergence, ClusterB))
		})
	}
}

// TestScanVerified_RejectionIsNotAHealthSignal verifies that a rejected row
// counts as a successful read of the cluster that returned it:
// no failure reaches the failover policy, the read strategy, or the error counter.
func TestScanVerified_RejectionIsNotAHealthSignal(t *testing.T) {
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}
	strategy := &trackingReadStrategy{}
	met := newReadTestMetrics()
	client, _, _ := newSliceClient(t, verifyLeg{val: "old"}.spec(), verifyLeg{val: "older"}.spec(),
		WithFailoverPolicy(policy), WithReadStrategy(strategy), WithMetrics(met))

	scanErr := client.Query("SELECT v").ScanVerified(func(RowScanner) bool { return false })
	require.ErrorIs(t, scanErr, types.ErrReadRejected)

	assert.Empty(t, policy.RecordFailureCalls)
	assert.Empty(t, strategy.OnFailureCalls)
	assert.Equal(t, []ClusterID{ClusterA, ClusterB}, strategy.OnSuccessCalls, "both clusters answered")
	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterA))
	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterB))
}

// TestScanVerified_AlternativeFaultIsRecorded verifies that a cluster fault
// on the second read is still charged to that cluster.
func TestScanVerified_AlternativeFaultIsRecorded(t *testing.T) {
	policy := &trackingFailoverPolicy{}
	met := newReadTestMetrics()
	client, _, _ := newSliceClient(t, verifyLeg{val: "old"}.spec(), verifyLeg{err: errVerifyCluster}.spec(),
		WithFailoverPolicy(policy), WithMetrics(met))

	scanErr := client.Query("SELECT v").ScanVerified(func(RowScanner) bool { return false })
	require.ErrorIs(t, scanErr, types.ErrReadRejected)

	assert.Equal(t, []ClusterID{ClusterB}, policy.RecordFailureCalls)
	assert.Equal(t, int64(1), met.get(met.ReadErrors, ClusterB))
	assert.Equal(t, int64(0), met.get(met.ReadErrors, ClusterA))
}

// TestScanVerified_ScanErrorIsTheReadsError verifies that a failed Scan
// inside the callback ends the read with that error, whatever the callback returned.
func TestScanVerified_ScanErrorIsTheReadsError(t *testing.T) {
	scanFailure := errors.New("cannot unmarshal")
	spec := verifyLeg{val: "new"}.spec()
	spec.scanErr = scanFailure
	client, _ := newSingleSliceClient(t, spec)

	scanErr := client.Query("SELECT v").ScanVerified(func(r RowScanner) bool {
		_ = r.Scan(new(string))

		return true
	})

	require.ErrorIs(t, scanErr, scanFailure)
}

// TestScanVerified_DrainingAlternativeIsSkipped verifies that a rejected row
// does not send the read to a draining cluster.
func TestScanVerified_DrainingAlternativeIsSkipped(t *testing.T) {
	client, _, sessionB := newSliceClient(t, verifyLeg{val: "old"}.spec(), verifyLeg{val: "new"}.spec())
	client.drainB.Store(true)

	var got string
	var seen []string
	scanErr := client.Query("SELECT v").ScanVerified(acceptNew(&got, &seen))

	require.ErrorIs(t, scanErr, types.ErrReadRejected)
	assert.Equal(t, []string{"old"}, seen)
	assert.Zero(t, sessionB.iterCtxCalls.Load(), "a draining alternative must not be asked")
}

// TestScanVerified_CallerContextEndedBeforeSecondRead verifies that a caller
// whose context ended by the time its row was rejected gets the context's error,
// and the alternative is not read with a dead context.
func TestScanVerified_CallerContextEndedBeforeSecondRead(t *testing.T) {
	client, _, sessionB := newSliceClient(t, verifyLeg{val: "old"}.spec(), verifyLeg{val: "new"}.spec())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	scanErr := client.Query("SELECT v").ScanVerifiedContext(ctx, func(RowScanner) bool {
		cancel()
		return false
	})

	require.ErrorIs(t, scanErr, context.Canceled)
	assert.Zero(t, sessionB.iterCtxCalls.Load())
}

// TestScanVerified_DivergenceEventNamesTheRejection verifies that the event
// for a row accepted on the alternative says the caller rejected the first.
func TestScanVerified_DivergenceEventNamesTheRejection(t *testing.T) {
	rec := newInternalEventRecorder()
	client, _, _ := newSliceClient(t, verifyLeg{val: "old"}.spec(), verifyLeg{val: "new"}.spec(),
		WithOnClusterEvent(rec.handler))

	var got string
	var seen []string
	require.NoError(t, client.Query("SELECT v").ScanVerified(acceptNew(&got, &seen)))

	ev := rec.waitFor(t, func(ev types.ClusterEvent) bool {
		return ev.Kind == types.EventReadDivergence
	})
	assert.Equal(t, ClusterA, ev.Cluster)
	assert.Contains(t, ev.Reason, "rejected")
}

// TestScanVerified_NoOtherClusterToRead covers the reads that have no second
// cluster to turn to: the callback still judges the one row,
// and a rejection is final.
func TestScanVerified_NoOtherClusterToRead(t *testing.T) {
	t.Run("single-cluster client", func(t *testing.T) {
		client, _ := newSingleSliceClient(t, verifyLeg{val: "old"}.spec())

		var got string
		var seen []string
		require.ErrorIs(t, client.Query("SELECT v").ScanVerified(acceptNew(&got, &seen)), types.ErrReadRejected)
		assert.Equal(t, []string{"old"}, seen)
	})

	t.Run("single-cluster client, accepted", func(t *testing.T) {
		client, _ := newSingleSliceClient(t, verifyLeg{val: "new"}.spec())

		var got string
		var seen []string
		require.NoError(t, client.Query("SELECT v").ScanVerified(acceptNew(&got, &seen)))
		assert.Equal(t, "new", got)
	})

	t.Run("single-cluster client, not found", func(t *testing.T) {
		client, _ := newSingleSliceClient(t, verifyLeg{}.spec())

		var got string
		var seen []string
		require.ErrorIs(t, client.Query("SELECT v").ScanVerified(acceptNew(&got, &seen)), types.ErrNotFound)
		assert.Empty(t, seen)
	})

	// A paging token is meaningless on the other cluster,
	// so whatever the selected cluster answers is the result.
	paged := []struct {
		name    string
		leg     verifyLeg
		wantErr error
	}{
		{name: "paged read, accepted", leg: verifyLeg{val: "new"}},
		{name: "paged read, rejected", leg: verifyLeg{val: "old"}, wantErr: types.ErrReadRejected},
		{name: "paged read, not found", leg: verifyLeg{}, wantErr: types.ErrNotFound},
		{name: "paged read, cluster error", leg: verifyLeg{err: errVerifyCluster}, wantErr: errVerifyCluster},
	}
	for _, tt := range paged {
		t.Run(tt.name, func(t *testing.T) {
			client, _, sessionB := newSliceClient(t, tt.leg.spec(), verifyLeg{val: "new"}.spec())

			var got string
			var seen []string
			scanErr := client.Query("SELECT v").PageState([]byte("cursor")).ScanVerified(acceptNew(&got, &seen))

			if tt.wantErr == nil {
				require.NoError(t, scanErr)
				assert.Equal(t, "new", got)
			} else {
				require.ErrorIs(t, scanErr, tt.wantErr)
			}
			assert.Zero(t, sessionB.iterCtxCalls.Load())
		})
	}
}

// TestScanVerified_NilCallback verifies that a nil callback is refused
// before any cluster is contacted.
func TestScanVerified_NilCallback(t *testing.T) {
	client, sessionA, sessionB := newSliceClient(t, verifyLeg{val: "new"}.spec(), verifyLeg{val: "new"}.spec())

	require.Error(t, client.Query("SELECT v").ScanVerified(nil))
	assert.Zero(t, sessionA.iterCtxCalls.Load())
	assert.Zero(t, sessionB.iterCtxCalls.Load())
}

// TestScanVerified_ColumnsDescribeTheRow verifies that the RowScanner the
// callback receives carries the result's column metadata.
func TestScanVerified_ColumnsDescribeTheRow(t *testing.T) {
	client, _ := newSingleSliceClient(t, verifyLeg{val: "new"}.spec())

	var names []string
	require.NoError(t, client.Query("SELECT v").ScanVerified(func(r RowScanner) bool {
		for _, col := range r.Columns() {
			names = append(names, col.Name)
		}

		return true
	}))
	assert.Equal(t, []string{"v"}, names)
}

// ExampleQuery_ScanVerified reads a row the caller can tell is out of date
// and lets Helix fetch it from the other cluster.
func ExampleQuery_ScanVerified() {
	// In real code, create gocql sessions wrapped with v1.NewSession() or v2.NewSession().
	// Here cluster A still holds the row from before an update that reached only cluster B.
	sessionA := &sliceTestSession{spec: &sliceSpec{cols: []string{"status"}, rows: rowsOf([]any{"pending"})}}
	sessionB := &sliceTestSession{spec: &sliceSpec{cols: []string{"status"}, rows: rowsOf([]any{"completed"})}}

	client, err := NewCQLClient(sessionA, sessionB)
	if err != nil {
		return
	}
	defer client.Close()

	// The callback scans the row into its own variables.
	// It keeps the row only when it accepts it, because it may run twice.
	var status string
	err = client.Query("SELECT status FROM orders WHERE id = ?", "order-1").
		ScanVerified(func(row RowScanner) bool {
			var s string
			if row.Scan(&s) != nil {
				return false
			}
			fmt.Println("checking:", s)
			if s != "completed" {
				return false // out of date: Helix reads the other cluster
			}
			status = s

			return true
		})

	switch {
	case err == nil:
		fmt.Println("accepted:", status)
	case errors.Is(err, ErrReadRejected):
		fmt.Println("a row exists, but no cluster has an acceptable one yet")
	case errors.Is(err, ErrNotFound):
		fmt.Println("no cluster has the row")
	default:
		fmt.Println("read failed:", err)
	}

	// Output:
	// checking: pending
	// checking: completed
	// accepted: completed
}
