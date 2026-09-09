package helix

import (
	"context"
	"testing"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/policy"
	"github.com/stretchr/testify/require"
)

// TestIter_CloseReportsOnce asserts that closing an iterator twice records
// its outcome once and returns the same error both times.
func TestIter_CloseReportsOnce(t *testing.T) {
	sa, sb := newReadProbeSession(), newReadProbeSession()
	cb := policy.NewCircuitBreaker(policy.WithThreshold(2))
	client := newReadProbeClient(t, sa, sb, WithFailoverPolicy(cb))

	sa.setIterCloseErr(errReadProbeCluster)
	it := client.Query("SELECT v FROM t").IterContext(t.Context())
	require.ErrorIs(t, it.Close(), errReadProbeCluster)
	require.ErrorIs(t, it.Close(), errReadProbeCluster, "a repeated Close returns the same error")
	require.EqualValues(t, 1, cb.Failures(ClusterA), "one iterator error is recorded once")
}

// TestIter_CountsTheReadItMakes asserts an iterator is as visible to an
// operator as a Scan: opening it counts one read_total for the cluster it
// reads from, closing it observes one duration covering the caller's use of
// it, and a cluster error at Close counts one read_errors_total.
func TestIter_CountsTheReadItMakes(t *testing.T) {
	sa, sb := newReadProbeSession(), newReadProbeSession()
	met := newReadTestMetrics()
	client := newReadProbeClient(t, sa, sb,
		WithMetrics(met),
		WithFailoverPolicy(policy.NewCircuitBreaker(policy.WithThreshold(2))),
	)

	sa.setIterCloseErr(errReadProbeCluster)
	it := client.Query("SELECT v FROM t").IterContext(t.Context())
	require.EqualValues(t, 1, met.get(met.ReadTotal, ClusterA), "opening the iterator counts the attempt")
	require.Empty(t, met.durations(ClusterA), "the duration is not known until the iterator closes")

	// A round trip through another goroutine puts real elapsed time between
	// opening the iterator and closing it, with no sleep.
	scanned := make(chan struct{})
	go func() { close(scanned) }()
	<-scanned

	require.ErrorIs(t, it.Close(), errReadProbeCluster)
	require.ErrorIs(t, it.Close(), errReadProbeCluster)

	require.EqualValues(t, 1, met.get(met.ReadTotal, ClusterA), "a repeated Close counts the attempt once")
	require.EqualValues(t, 1, met.get(met.ReadErrors, ClusterA),
		"a cluster error at Close is counted like the same error from a Scan")
	require.Len(t, met.durations(ClusterA), 1, "a repeated Close observes the read once")
	require.Positive(t, met.durations(ClusterA)[0], "the duration spans the caller's use of the iterator")
	require.Zero(t, met.get(met.ReadTotal, ClusterB), "an iterator never contacts the alternative")
	require.Empty(t, met.durations(ClusterB), "a cluster no read reached observes nothing")
}

// TestBatchCASIter_CountsTheReadItMakes covers the two batch CAS entry
// points, whose iterator reports through the read path like any other.
// They are not part of the read classification matrix: a CAS never fails
// over and never re-routes, so the matrix has no entry for it.
func TestBatchCASIter_CountsTheReadItMakes(t *testing.T) {
	tests := []struct {
		name string
		run  func(b Batch, ctx context.Context) (bool, Iter, error)
	}{
		{
			name: "ExecCASContext",
			run: func(b Batch, ctx context.Context) (bool, Iter, error) {
				return b.ExecCASContext(ctx)
			},
		},
		{
			name: "MapExecCASContext",
			run: func(b Batch, ctx context.Context) (bool, Iter, error) {
				return b.MapExecCASContext(ctx, map[string]any{})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sa := &casProbeSession{readProbeSession: newReadProbeSession(), closeErr: errReadProbeCluster}
			sb := &casProbeSession{readProbeSession: newReadProbeSession()}
			met := newReadTestMetrics()
			client, err := NewCQLClient(sa, sb,
				WithMetrics(met),
				WithFailoverPolicy(policy.NewCircuitBreaker(policy.WithThreshold(2))),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			batch := client.Batch(LoggedBatch).Query("UPDATE t SET v = ? WHERE k = ? IF v = ?", 1, 1, 0)
			applied, it, err := tt.run(batch, t.Context())
			require.NoError(t, err)
			require.True(t, applied)
			require.EqualValues(t, 1, met.get(met.ReadTotal, ClusterA), "the CAS iterator counts its attempt")

			require.ErrorIs(t, it.Close(), errReadProbeCluster)
			require.EqualValues(t, 1, met.get(met.ReadErrors, ClusterA))
			require.Len(t, met.durations(ClusterA), 1)
			require.Zero(t, met.get(met.ReadTotal, ClusterB), "a CAS is never replicated")
		})
	}
}

// casProbeSession is a readProbeSession whose batches hand back a CAS
// iterator that reports closeErr, which mockBatch's cannot.
type casProbeSession struct {
	*readProbeSession
	closeErr error
}

func (s *casProbeSession) Batch(_ cql.BatchType) cql.Batch { return &casProbeBatch{session: s} }

// casProbeBatch is the smallest cql.Batch that keeps its own identity
// through the builder calls cqlBatch makes before it executes the CAS.
type casProbeBatch struct {
	session *casProbeSession
}

func (b *casProbeBatch) Query(_ string, _ ...any) cql.Batch            { return b }
func (b *casProbeBatch) Consistency(_ cql.Consistency) cql.Batch       { return b }
func (b *casProbeBatch) SerialConsistency(_ cql.Consistency) cql.Batch { return b }
func (b *casProbeBatch) WithTimestamp(_ int64) cql.Batch               { return b }
func (b *casProbeBatch) Size() int                                     { return 1 }
func (b *casProbeBatch) Statements() []cql.BatchEntry                  { return nil }
func (b *casProbeBatch) Exec() error                                   { return nil }
func (b *casProbeBatch) ExecContext(_ context.Context) error           { return nil }
func (b *casProbeBatch) IterContext(_ context.Context) cql.Iter        { return b.casIter() }

func (b *casProbeBatch) ExecCAS(_ ...any) (bool, cql.Iter, error) { return true, b.casIter(), nil }
func (b *casProbeBatch) ExecCASContext(_ context.Context, _ ...any) (bool, cql.Iter, error) {
	return true, b.casIter(), nil
}

func (b *casProbeBatch) MapExecCAS(_ map[string]any) (bool, cql.Iter, error) {
	return true, b.casIter(), nil
}

func (b *casProbeBatch) MapExecCASContext(_ context.Context, _ map[string]any) (bool, cql.Iter, error) {
	return true, b.casIter(), nil
}

func (b *casProbeBatch) casIter() cql.Iter {
	return &readProbeIter{closeErr: b.session.closeErr}
}
