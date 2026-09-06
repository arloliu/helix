package helix

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// callerExpiredCollector wraps NopMetrics and records the caller-expired
// counters together with the error counters they must never fire alongside.
type callerExpiredCollector struct {
	metrics.NopMetrics

	readExpiredA  atomic.Int32
	readExpiredB  atomic.Int32
	writeExpiredA atomic.Int32
	writeExpiredB atomic.Int32
	readErrors    atomic.Int32
	writeErrors   atomic.Int32
}

func (c *callerExpiredCollector) IncReadCallerExpired(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		c.readExpiredA.Add(1)
	} else {
		c.readExpiredB.Add(1)
	}
}

func (c *callerExpiredCollector) IncWriteCallerExpired(cluster types.ClusterID) {
	if cluster == types.ClusterA {
		c.writeExpiredA.Add(1)
	} else {
		c.writeExpiredB.Add(1)
	}
}

func (c *callerExpiredCollector) IncReadError(_ types.ClusterID)  { c.readErrors.Add(1) }
func (c *callerExpiredCollector) IncWriteError(_ types.ClusterID) { c.writeErrors.Add(1) }

var (
	_ types.MetricsCollector     = (*callerExpiredCollector)(nil)
	_ types.CallerContextMetrics = (*callerExpiredCollector)(nil)
)

// newCallerExpiredClient builds a client over the given sessions with a
// callerExpiredCollector installed, closing the client when the test ends.
// A nil sessionB builds a single-cluster client.
func newCallerExpiredClient(t *testing.T, sessionA, sessionB cql.Session, opts ...Option) (*CQLClient, *callerExpiredCollector) {
	t.Helper()
	mc := &callerExpiredCollector{}
	client, err := NewCQLClient(sessionA, sessionB, append(opts, WithMetrics(mc))...)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	return client, mc
}

// TestCallerContextMetrics_ReadLegEndsAfterTheCallerContext covers the
// failure the counter exists for: a cluster that accepts the read and never
// answers, with no leg deadline to attribute the stall to it.
// The read is attributed to the caller, so nothing else records it.
func TestCallerContextMetrics_ReadLegEndsAfterTheCallerContext(t *testing.T) {
	tests := []struct {
		name string
		dual bool
	}{
		{name: "dual-cluster", dual: true},
		{name: "single-cluster", dual: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sa := newStalledReadSession()
			var sb cql.Session
			if tt.dual {
				sb = newStalledReadSession()
			}
			breaker := policy.NewCircuitBreaker()
			client, mc := newCallerExpiredClient(t, sa, sb, WithFailoverPolicy(breaker))

			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			t.Cleanup(cancel)

			var got int
			err := client.Query("SELECT v FROM t WHERE k = ?", 1).ScanContext(ctx, &got)
			require.ErrorIs(t, err, context.DeadlineExceeded)

			require.Equal(t, int32(1), mc.readExpiredA.Load(),
				"the stalled leg is counted once against the cluster it targeted")
			require.Equal(t, int32(0), mc.readExpiredB.Load(),
				"no read reached B: it is either absent or the caller's whole budget went to A")
			require.Equal(t, int32(0), mc.readErrors.Load(),
				"a caller-expired read is not a cluster error")
			require.EqualValues(t, 0, breaker.Failures(ClusterA),
				"a caller-expired read is not a health signal")
			require.Equal(t, int32(0), client.statsForCluster(ClusterA).consecutiveFailures.Load())
		})
	}
}

// TestCallerContextMetrics_LegDeadlineExpiryIsNotCounted is the other half
// of the pair: with a leg deadline the stall expires on Helix's own clock
// while the caller is still waiting, so it is attributed to the cluster and
// the caller-expired counter stays flat.
func TestCallerContextMetrics_LegDeadlineExpiryIsNotCounted(t *testing.T) {
	sa, sb := newStalledReadSession(), newStalledReadSession()
	client, mc := newCallerExpiredClient(t, sa, sb, WithClusterReadTimeout(20*time.Millisecond))

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	var got int
	err := client.Query("SELECT v FROM t WHERE k = ?", 1).ScanContext(ctx, &got)
	require.ErrorIs(t, err, types.ErrClusterTimeout)

	require.Equal(t, int32(0), mc.readExpiredA.Load(),
		"Helix's own deadline expiring is the cluster's failure, not the caller's")
	require.Equal(t, int32(0), mc.readExpiredB.Load())
	require.Equal(t, int32(2), mc.readErrors.Load(), "both legs failed against their cluster")
	require.Equal(t, int32(1), client.statsForCluster(ClusterA).consecutiveFailures.Load())
}

// TestCallerContextMetrics_IterCloseAfterTheCallerContextEnded covers the
// second read entry point: an iterator whose Close reports an error once the
// caller has already given up.
func TestCallerContextMetrics_IterCloseAfterTheCallerContextEnded(t *testing.T) {
	sa, sb := newReadProbeSession(), newReadProbeSession()
	breaker := policy.NewCircuitBreaker()
	client, mc := newCallerExpiredClient(t, sa, sb, WithFailoverPolicy(breaker))

	sa.setIterCloseErr(errReadProbeCluster)
	ctx, cancel := context.WithCancel(t.Context())
	it := client.Query("SELECT v FROM t").IterContext(ctx)
	cancel()

	require.ErrorIs(t, it.Close(), errReadProbeCluster)
	require.ErrorIs(t, it.Close(), errReadProbeCluster)

	require.Equal(t, int32(1), mc.readExpiredA.Load(),
		"a repeated Close reports the outcome once, so the counter moves once")
	require.Equal(t, int32(0), mc.readErrors.Load())
	require.EqualValues(t, 0, breaker.Failures(ClusterA))
}

// TestCallerContextMetrics_WriteLegEndsAfterTheCallerContext asserts that
// every affected cluster's leg is counted once, in dual- and single-cluster
// mode alike.
func TestCallerContextMetrics_WriteLegEndsAfterTheCallerContext(t *testing.T) {
	tests := []struct {
		name         string
		dual         bool
		wantExpiredB int32
		assertErr    func(t *testing.T, err error)
	}{
		{
			name:         "dual-cluster counts both legs",
			dual:         true,
			wantExpiredB: 1,
			assertErr: func(t *testing.T, err error) {
				t.Helper()
				// Both legs are caller-cancelled, and writeLegErrKind.failed()
				// counts that as a failure, so the aggregation returns before
				// the replay branch with both legs' errors.
				var dual *types.DualClusterError
				require.ErrorAs(t, err, &dual)
				require.ErrorIs(t, dual.ErrorA, context.Canceled)
				require.ErrorIs(t, dual.ErrorB, context.Canceled)
			},
		},
		{
			name: "single-cluster counts its only leg",
			dual: false,
			assertErr: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, context.Canceled, "the only leg's error is returned verbatim")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sa := newMockSession()
			sa.execErr = context.Canceled
			var sb cql.Session
			if tt.dual {
				other := newMockSession()
				other.execErr = context.Canceled
				sb = other
			}
			client, mc := newCallerExpiredClient(t, sa, sb, WithReplayer(&mockReplayer{}))

			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			err := client.Query("INSERT INTO t (k) VALUES (?)", "x").ExecContext(ctx)
			tt.assertErr(t, err)

			require.Equal(t, int32(1), mc.writeExpiredA.Load())
			require.Equal(t, tt.wantExpiredB, mc.writeExpiredB.Load())
			require.Equal(t, int32(0), mc.writeErrors.Load(),
				"a caller-cancelled leg is not a cluster write error")
			require.Equal(t, int32(0), client.statsForCluster(ClusterA).consecutiveFailures.Load())
		})
	}
}

// TestCallerContextMetrics_UndispatchedLegIsNotCounted pins the other
// negative case: SyncDualWrite returns the caller's context error for the
// second leg once the first has spent the budget, without ever contacting
// it. That leg says nothing about whether its cluster is answering, so it
// must not be counted — otherwise every caller timeout on A would produce a
// phantom count on B.
func TestCallerContextMetrics_UndispatchedLegIsNotCounted(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	client, mc := newCallerExpiredClient(t, sa, sb,
		WithWriteStrategy(policy.NewSyncDualWrite()),
		WithReplayer(&mockReplayer{}),
	)

	// A is written first and succeeds; the context is dead by the time the
	// strategy looks at B, so B is skipped with the caller's error.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := client.Query("INSERT INTO t (k) VALUES (?)", "x").ExecContext(ctx)
	require.NoError(t, err, "A acknowledged the write")

	require.Equal(t, int32(0), mc.writeExpiredA.Load(), "A answered")
	require.Equal(t, int32(0), mc.writeExpiredB.Load(),
		"B was never dispatched, so it must not be counted as caller-expired")
	require.Empty(t, sb.queries, "the second leg was skipped, not sent")
}

// backgroundDeferredStrategy acknowledges cluster A synchronously and runs
// cluster B's leg on a background goroutine whose result arrives later
// through a DeferredWriteResult, the way AdaptiveDualWrite reports a
// degraded cluster. release lets the test decide when the background leg
// finishes, so it can be made to finish after the caller has given up.
type backgroundDeferredStrategy struct {
	result  *manualDeferredError
	release <-chan struct{}
}

func (s *backgroundDeferredStrategy) Execute(
	ctx context.Context,
	writeA, writeB func(context.Context) error,
) (errA, errB error) {
	go func() {
		<-s.release
		s.result.complete(writeB(ctx))
	}()

	return writeA(ctx), s.result
}

// TestCallerContextMetrics_DeferredLegIsNeverCounted pins the decision the
// interface documents: the late result of a background leg is classified
// against a context that carries the caller's values but none of its
// cancellation, so a leg that finishes long after the caller gave up is
// still the cluster's own outcome — counted as a cluster failure, never as
// caller-expired.
func TestCallerContextMetrics_DeferredLegIsNeverCounted(t *testing.T) {
	release := make(chan struct{})
	deferred := &manualDeferredError{}
	sa, sb := newMockSession(), newMockSession()
	sb.execErr = errUnreachableForTest

	client, mc := newCallerExpiredClient(t, sa, sb,
		WithWriteStrategy(&backgroundDeferredStrategy{result: deferred, release: release}),
		WithReplayer(&mockReplayer{}),
	)

	ctx, cancel := context.WithCancel(t.Context())
	require.NoError(t, client.Query("INSERT INTO t (k) VALUES (?)", "x").ExecContext(ctx),
		"A acknowledged the write while B is still in flight")

	// The caller gives up first; only then does the background leg finish.
	cancel()
	close(release)

	// Close returns once the deferred leg has completed and been accounted
	// for, so no sleep is needed to observe its result.
	client.Close()

	require.Equal(t, int32(0), mc.writeExpiredB.Load(),
		"a background leg is classified without the caller's cancellation, so it is never caller-expired")
	require.Equal(t, int32(0), mc.writeExpiredA.Load())
	require.Equal(t, int32(1), client.statsForCluster(ClusterB).consecutiveFailures.Load(),
		"the late failure still reaches B's health as the cluster's own")
}

// TestCallerContextMetrics_ClusterErrorsAreNotCounted pins the negative
// case: while the caller's context is live, an error belongs to the cluster
// and only the cluster's counters move.
func TestCallerContextMetrics_ClusterErrorsAreNotCounted(t *testing.T) {
	errCluster := errors.New("cluster is unreachable")

	t.Run("read", func(t *testing.T) {
		sa, sb := newMockSession(), newMockSession()
		sa.scanErr = errCluster
		sb.scanValues = []any{42}
		client, mc := newCallerExpiredClient(t, sa, sb)

		var got int
		require.NoError(t, client.Query("SELECT v FROM t WHERE k = ?", 1).ScanContext(t.Context(), &got))

		require.Equal(t, int32(0), mc.readExpiredA.Load())
		require.Equal(t, int32(0), mc.readExpiredB.Load())
		require.Equal(t, int32(1), mc.readErrors.Load(), "A's failure is A's own")
	})

	t.Run("write", func(t *testing.T) {
		sa, sb := newMockSession(), newMockSession()
		sa.execErr = errCluster
		client, mc := newCallerExpiredClient(t, sa, sb, WithReplayer(&mockReplayer{}))

		require.NoError(t, client.Query("INSERT INTO t (k) VALUES (?)", "x").ExecContext(t.Context()))

		require.Equal(t, int32(0), mc.writeExpiredA.Load())
		require.Equal(t, int32(0), mc.writeExpiredB.Load())
		require.Equal(t, int32(1), mc.writeErrors.Load(), "A's failed leg is A's own")
	})
}
