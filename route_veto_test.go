package helix

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// vetoPolicy is a failover policy that always allows failover and vetoes
// the clusters a test names.
type vetoPolicy struct {
	*mockFailoverPolicy
	mu     sync.Mutex
	vetoed map[ClusterID]bool
}

func newVetoPolicy(clusters ...ClusterID) *vetoPolicy {
	p := &vetoPolicy{mockFailoverPolicy: newMockFailoverPolicy(true), vetoed: map[ClusterID]bool{}}
	for _, c := range clusters {
		p.vetoed[c] = true
	}

	return p
}

func (p *vetoPolicy) VetoRoute(cluster ClusterID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.vetoed[cluster]
}

func TestRouteVeto_OrdinaryReads(t *testing.T) {
	cases := []struct {
		name   string
		vetoed []ClusterID
		drainA bool
		drainB bool
		want   ClusterID
	}{
		{name: "selected vetoed, alternative eligible", vetoed: []ClusterID{ClusterA}, want: ClusterB},
		{name: "nothing vetoed", want: ClusterA},
		{name: "both vetoed: the selection stands", vetoed: []ClusterID{ClusterA, ClusterB}, want: ClusterA},
		{name: "selected vetoed, alternative draining: the selection stands", vetoed: []ClusterID{ClusterA}, drainB: true, want: ClusterA},
		{name: "selected draining, alternative vetoed: the selection stands", vetoed: []ClusterID{ClusterB}, drainA: true, want: ClusterA},
		{name: "selected draining, alternative eligible", drainA: true, want: ClusterB},
		{name: "both draining, nothing vetoed: the selection stands", drainA: true, drainB: true, want: ClusterA},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sa, sb := newReadProbeSession(), newReadProbeSession()
			sticky := policy.NewStickyRead(policy.WithPreferredCluster(ClusterA))
			client := newReadProbeClient(t, sa, sb,
				WithReadStrategy(sticky),
				WithFailoverPolicy(newVetoPolicy(tc.vetoed...)),
				WithRouteVeto(true),
			)
			client.drainA.Store(tc.drainA)
			client.drainB.Store(tc.drainB)

			var v string
			require.NoError(t, client.Query("SELECT v FROM t").ScanContext(t.Context(), &v))
			served := ClusterA
			if sb.scans.Load() == 1 {
				served = ClusterB
			}
			require.Equal(t, tc.want, served)
			require.Equal(t, int64(1), sa.scans.Load()+sb.scans.Load(), "exactly one cluster is asked")
			require.Equal(t, ClusterA, sticky.Preferred(), "a veto never moves the sticky preference")
		})
	}
}

func TestRouteVeto_DoesNotTouchPinnedOverrideOrCAS(t *testing.T) {
	newClient := func(t *testing.T, opts ...Option) (*CQLClient, *readProbeSession, *readProbeSession) {
		t.Helper()
		sa, sb := newReadProbeSession(), newReadProbeSession()
		client := newReadProbeClient(t, sa, sb, append([]Option{
			WithReadStrategy(policy.NewStickyRead(policy.WithPreferredCluster(ClusterA))),
			WithFailoverPolicy(newVetoPolicy(ClusterA)),
			WithRouteVeto(true),
		}, opts...)...)

		return client, sa, sb
	}

	t.Run("pinned paging cursor stays on its cluster", func(t *testing.T) {
		client, sa, sb := newClient(t)
		cursor := encodePageState(ClusterA, []byte("cursor-from-a"))
		iter := client.Query("SELECT v FROM t").PageSize(10).PageState(cursor).IterContext(t.Context())
		require.NoError(t, iter.Close())
		require.Equal(t, int64(1), sa.iters.Load())
		require.Zero(t, sb.iters.Load())
	})
	t.Run("legacy paging token preserves the selection", func(t *testing.T) {
		client, sa, sb := newClient(t)
		iter := client.Query("SELECT v FROM t").PageSize(10).PageState([]byte("legacy-driver-token")).IterContext(t.Context())
		require.NoError(t, iter.Close())
		require.Equal(t, int64(1), sa.iters.Load())
		require.Zero(t, sb.iters.Load())
	})
	t.Run("AllowedClusters override is authoritative", func(t *testing.T) {
		client, sa, sb := newClient(t, WithAllowedClusters(func() []ClusterID { return []ClusterID{ClusterA} }))
		var v string
		require.NoError(t, client.Query("SELECT v FROM t").ScanContext(t.Context(), &v))
		require.Equal(t, int64(1), sa.scans.Load())
		require.Zero(t, sb.scans.Load())
	})
	t.Run("CAS is not rerouted", func(t *testing.T) {
		client, sa, sb := newClient(t)
		var v string
		_, err := client.Query("UPDATE t SET v = ? WHERE k = ? IF v = ?", "n", "k", "o").ScanCASContext(t.Context(), &v)
		require.NoError(t, err)
		require.Equal(t, int64(1), sa.cas.Load())
		require.Zero(t, sb.cas.Load())
	})
	t.Run("single-cluster client is untouched", func(t *testing.T) {
		sa := newReadProbeSession()
		client, err := NewCQLClient(sa, nil,
			WithFailoverPolicy(newVetoPolicy(ClusterA)),
			WithRouteVeto(true),
		)
		require.NoError(t, err)
		t.Cleanup(client.Close)
		var v string
		require.NoError(t, client.Query("SELECT v FROM t").ScanContext(t.Context(), &v))
		require.Equal(t, int64(1), sa.scans.Load())
	})
}

func TestRouteVeto_FallbackReadSkipsVetoedAlternative(t *testing.T) {
	sa, sb := newReadProbeSession(), newReadProbeSession()
	sa.setScan(func(context.Context) error { return types.ErrNotFound })
	client := newReadProbeClient(t, sa, sb,
		WithReadStrategy(policy.NewStickyRead(policy.WithPreferredCluster(ClusterA))),
		WithFailoverPolicy(newVetoPolicy(ClusterB)),
		WithRouteVeto(true),
	)

	var v string
	err := client.Query("SELECT v FROM t WHERE k = ?", "k").FallbackRead().ScanContext(t.Context(), &v)
	require.True(t, errors.Is(err, types.ErrNotFound))
	require.Equal(t, int64(1), sa.scans.Load())
	require.Zero(t, sb.scans.Load(), "a vetoed alternative is not asked")
}

func TestRouteVeto_OrdinarySuccessCallbacksUnchanged(t *testing.T) {
	t.Run("PrimaryOnlyRead clears its failed-over state on a rerouted success", func(t *testing.T) {
		sa, sb := newReadProbeSession(), newReadProbeSession()
		primary := policy.NewPrimaryOnlyRead()
		veto := newVetoPolicy()
		client := newReadProbeClient(t, sa, sb,
			WithReadStrategy(primary),
			WithFailoverPolicy(veto),
			WithRouteVeto(true),
		)
		// Fail over to B through a real failure on A.
		sa.setScan(func(context.Context) error { return errReadProbeCluster })
		var v string
		require.NoError(t, client.Query("SELECT v FROM t").ScanContext(t.Context(), &v))
		require.Equal(t, ClusterB, primary.Select(t.Context()))

		// Veto B: the next read is rerouted to A, succeeds there, and the
		// strategy observes that success as it would any other.
		sa.setScan(nil)
		veto.mu.Lock()
		veto.vetoed[ClusterB] = true
		veto.mu.Unlock()
		require.NoError(t, client.Query("SELECT v FROM t").ScanContext(t.Context(), &v))
		require.Equal(t, ClusterA, primary.Select(t.Context()), "OnSuccess(A) ends the failed-over state")
	})
}

func TestRouteVeto_SnapshotAgreesWithOrdinaryClose(t *testing.T) {
	sa, sb := newReadProbeSession(), newReadProbeSession()
	lcb := policy.NewLatencyCircuitBreaker(
		policy.WithLatencyAbsoluteMax(time.Second),
		policy.WithLatencyThreshold(1),
		policy.WithLatencyResetTimeout(time.Hour),
	)
	client := newReadProbeClient(t, sa, sb,
		WithReadStrategy(policy.NewStickyRead(policy.WithPreferredCluster(ClusterA))),
		WithFailoverPolicy(lcb),
		WithRouteVeto(true),
		WithAllowedClusters(func() []ClusterID { return []ClusterID{ClusterA} }),
	)

	lcb.RecordFailure(ClusterA)
	require.True(t, lcb.VetoRoute(ClusterA), "an open breaker vetoes")

	// An override read bypasses the veto; its fast success closes the
	// breaker through the ordinary path and the snapshot follows.
	var v string
	require.NoError(t, client.Query("SELECT v FROM t").ScanContext(t.Context(), &v))
	require.Equal(t, int64(1), sa.scans.Load())
	require.False(t, lcb.ShouldFailover(ClusterA, nil))
	require.False(t, lcb.VetoRoute(ClusterA), "the snapshot clears on an ordinary close")
}
