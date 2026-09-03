package helix

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// authorityLog records every authority call the hub makes, in order.
type authorityLog struct {
	mu    sync.Mutex
	calls []string
}

func (l *authorityLog) add(call string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, call)
}

func (l *authorityLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.calls...)
}

// spyStrategy always selects cluster A and fails over to the other cluster.
type spyStrategy struct{ log *authorityLog }

func (s *spyStrategy) Select(context.Context) ClusterID { return ClusterA }
func (s *spyStrategy) OnSuccess(cluster ClusterID) {
	s.log.add("strategy.OnSuccess(" + string(cluster) + ")")
}
func (s *spyStrategy) OnFailure(cluster ClusterID, _ error) (ClusterID, bool) {
	s.log.add("strategy.OnFailure(" + string(cluster) + ")")
	if cluster == ClusterA {
		return ClusterB, true
	}

	return ClusterA, true
}

// spyPolicy always allows failover and records its health calls.
type spyPolicy struct{ log *authorityLog }

func (p *spyPolicy) ShouldFailover(ClusterID, error) bool { return true }
func (p *spyPolicy) RecordFailure(cluster ClusterID) {
	p.log.add("policy.RecordFailure(" + string(cluster) + ")")
}

func (p *spyPolicy) RecordSuccess(cluster ClusterID) {
	p.log.add("policy.RecordSuccess(" + string(cluster) + ")")
}

// spyLatencyPolicy is a spyPolicy that also records latency.
type spyLatencyPolicy struct{ spyPolicy }

func (p *spyLatencyPolicy) RecordLatency(cluster ClusterID, _ time.Duration) {
	p.log.add("policy.RecordLatency(" + string(cluster) + ")")
}

type hubFixture struct {
	log    *authorityLog
	sa, sb *readProbeSession
	client *CQLClient
}

func newHubFixture(t *testing.T, latency bool, opts ...Option) *hubFixture {
	t.Helper()
	log := &authorityLog{}
	var policy FailoverPolicy = &spyPolicy{log: log}
	if latency {
		policy = &spyLatencyPolicy{spyPolicy{log: log}}
	}
	sa, sb := newReadProbeSession(), newReadProbeSession()
	client := newReadProbeClient(t, sa, sb, append([]Option{
		WithReadStrategy(&spyStrategy{log: log}),
		WithFailoverPolicy(policy),
	}, opts...)...)

	return &hubFixture{log: log, sa: sa, sb: sb, client: client}
}

func (f *hubFixture) stats(cluster ClusterID) (failures int32, lastErr error) {
	s := f.client.statsForCluster(cluster)
	if e := s.lastErr.Load(); e != nil {
		lastErr = *e
	}

	return s.consecutiveFailures.Load(), lastErr
}

func TestHub_ReadAuthorityOrder(t *testing.T) {
	var v string
	fail := func(context.Context) error { return errReadProbeCluster }

	t.Run("initial success with a plain policy", func(t *testing.T) {
		f := newHubFixture(t, false)
		require.NoError(t, f.client.Query("SELECT v FROM t").ScanContext(t.Context(), &v))
		require.Equal(t, []string{"strategy.OnSuccess(A)", "policy.RecordSuccess(A)"}, f.log.snapshot())
	})
	t.Run("initial success with a latency recorder", func(t *testing.T) {
		f := newHubFixture(t, true)
		require.NoError(t, f.client.Query("SELECT v FROM t").ScanContext(t.Context(), &v))
		require.Equal(t, []string{"strategy.OnSuccess(A)", "policy.RecordLatency(A)"}, f.log.snapshot(),
			"the latency sample is the success signal; RecordSuccess must not follow it")
	})
	t.Run("override success freezes the strategy", func(t *testing.T) {
		f := newHubFixture(t, false, WithAllowedClusters(func() []ClusterID { return []ClusterID{ClusterA} }))
		require.NoError(t, f.client.Query("SELECT v FROM t").ScanContext(t.Context(), &v))
		require.Equal(t, []string{"policy.RecordSuccess(A)"}, f.log.snapshot())
	})
	t.Run("initial failure then failover success", func(t *testing.T) {
		f := newHubFixture(t, false)
		f.sa.setScan(fail)
		require.NoError(t, f.client.Query("SELECT v FROM t").ScanContext(t.Context(), &v))
		require.Equal(t, []string{
			"policy.RecordFailure(A)",
			"strategy.OnFailure(A)",
			"strategy.OnSuccess(B)",
			"policy.RecordSuccess(B)",
		}, f.log.snapshot(), "the failure is recorded once, before the routing decision")
		failures, lastErr := f.stats(ClusterA)
		require.Equal(t, int32(1), failures)
		require.ErrorIs(t, lastErr, errReadProbeCluster, "the original error is kept for the refresher")
	})
	t.Run("both attempts fail", func(t *testing.T) {
		f := newHubFixture(t, false)
		f.sa.setScan(fail)
		f.sb.setScan(fail)
		require.Error(t, f.client.Query("SELECT v FROM t").ScanContext(t.Context(), &v))
		require.Equal(t, []string{
			"policy.RecordFailure(A)",
			"strategy.OnFailure(A)",
			"policy.RecordFailure(B)",
		}, f.log.snapshot(), "a failed failover attempt never asks the strategy again")
	})
	t.Run("fallback probe success and failure", func(t *testing.T) {
		f := newHubFixture(t, true)
		f.sa.setScan(func(context.Context) error { return types.ErrNotFound })
		require.NoError(t, f.client.Query("SELECT v FROM t").FallbackRead().ScanContext(t.Context(), &v))
		require.Equal(t, []string{"strategy.OnSuccess(B)", "policy.RecordLatency(B)"}, f.log.snapshot(),
			"a not-found primary is not a health signal; the probe's success is")

		f.log.calls = nil
		f.sb.setScan(fail)
		err := f.client.Query("SELECT v FROM t").FallbackRead().ScanContext(t.Context(), &v)
		require.ErrorIs(t, err, types.ErrNotFound)
		require.Equal(t, []string{"policy.RecordFailure(B)"}, f.log.snapshot())
		failures, _ := f.stats(ClusterB)
		require.Equal(t, int32(1), failures)
	})
	t.Run("caller-cancelled read reaches no authority", func(t *testing.T) {
		f := newHubFixture(t, false)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		f.sa.setScan(func(ctx context.Context) error { return ctx.Err() })
		require.ErrorIs(t, f.client.Query("SELECT v FROM t").ScanContext(ctx, &v), context.Canceled)
		require.Empty(t, f.log.snapshot())
		failures, _ := f.stats(ClusterA)
		require.Zero(t, failures)
	})
}

func TestHub_IteratorCloseAuthorityOrder(t *testing.T) {
	t.Run("clean close", func(t *testing.T) {
		f := newHubFixture(t, true)
		require.NoError(t, f.client.Query("SELECT v FROM t").IterContext(t.Context()).Close())
		require.Equal(t, []string{"strategy.OnSuccess(A)", "policy.RecordSuccess(A)"}, f.log.snapshot(),
			"an iterator has no latency sample, so the policy gets RecordSuccess")
	})
	t.Run("cluster error", func(t *testing.T) {
		f := newHubFixture(t, false)
		f.sa.setIterCloseErr(errReadProbeCluster)
		require.ErrorIs(t, f.client.Query("SELECT v FROM t").IterContext(t.Context()).Close(), errReadProbeCluster)
		require.Equal(t, []string{"policy.RecordFailure(A)", "strategy.OnFailure(A)"}, f.log.snapshot())
		failures, lastErr := f.stats(ClusterA)
		require.Equal(t, int32(1), failures)
		require.ErrorIs(t, lastErr, errReadProbeCluster)
	})
}

func TestHub_SingleClusterTouchesOnlyStats(t *testing.T) {
	log := &authorityLog{}
	sa := newReadProbeSession()
	client, err := NewCQLClient(sa, nil,
		WithReadStrategy(&spyStrategy{log: log}),
		WithFailoverPolicy(&spyPolicy{log: log}),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	var v string
	require.NoError(t, client.Query("SELECT v FROM t").ScanContext(t.Context(), &v))
	sa.setScan(func(context.Context) error { return errReadProbeCluster })
	require.Error(t, client.Query("SELECT v FROM t").ScanContext(t.Context(), &v))
	require.Empty(t, log.snapshot(), "a single-cluster read has no strategy or policy to inform")
	stats := client.statsForCluster(ClusterA)
	require.Equal(t, int32(1), stats.consecutiveFailures.Load())
}

func TestHub_WriteLegsReachOnlyStats(t *testing.T) {
	f := newHubFixture(t, false)
	require.NoError(t, f.client.Query("INSERT INTO t (k) VALUES (?)", 1).ExecContext(t.Context()))
	require.Empty(t, f.log.snapshot(), "write outcomes never reach the read authorities")
	failures, _ := f.stats(ClusterA)
	require.Zero(t, failures)
	require.NotZero(t, f.client.statsForCluster(ClusterA).lastSuccessNanos.Load())
}

// TestHub_LateReportLandsOnReplacedHolder pauses a write leg on the old
// session, swaps the session while it is paused, and proves the report
// lands on the replaced holder: the installed holder's stats stay clean in
// both the old-failure and the old-success ordering.
func TestHub_LateReportLandsOnReplacedHolder(t *testing.T) {
	cases := []struct {
		name       string
		oldErr     error
		newErr     error
		wantNewErr bool
	}{
		{name: "old failure after new success", oldErr: fmt.Errorf("old session died: %w", types.ErrClusterUnreachable), newErr: nil},
		{name: "old success after new failure", oldErr: nil, newErr: fmt.Errorf("new session failed: %w", types.ErrClusterUnreachable), wantNewErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			old := newBlockingSession()
			old.releaseErr = tc.oldErr
			client, err := NewCQLClient(old, newReadProbeSession())
			require.NoError(t, err)
			t.Cleanup(client.Close)

			done := make(chan error, 1)
			go func() {
				done <- client.Query("INSERT INTO t (k) VALUES (?)", 1).ExecContext(context.Background())
			}()
			<-old.entered // the leg has loaded the old holder and is inside the session

			replacement := newBlockingSession()
			replacement.releaseErr = tc.newErr
			close(replacement.release) // never blocks: the new session answers at once
			_, err = client.SwapSession(ClusterA, replacement)
			require.NoError(t, err)
			// The new session's own outcome lands on the installed holder.
			require.NoError(t, client.Query("INSERT INTO t (k) VALUES (?)", 2).ExecContext(t.Context()))

			close(old.release)
			require.NoError(t, <-done, "cluster B acknowledged the paused write")

			installed := client.statsForCluster(ClusterA)
			if tc.wantNewErr {
				require.Equal(t, int32(1), installed.consecutiveFailures.Load(), "the installed holder keeps its own failure")
				require.ErrorIs(t, *installed.lastErr.Load(), tc.newErr)
			} else {
				require.Zero(t, installed.consecutiveFailures.Load(), "the old session's failure must not reach the installed holder")
				require.Nil(t, installed.lastErr.Load())
			}
		})
	}
}

// TestHub_SwappedSessionStartsFresh pins the observable side of holder-owned
// stats: a swap installs fresh counters.
func TestHub_SwappedSessionStartsFresh(t *testing.T) {
	sa := newMockSession()
	sa.execErr = fmt.Errorf("dead: %w", types.ErrClusterUnreachable)
	client, err := NewCQLClient(sa, newMockSession())
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.NoError(t, client.Query("INSERT INTO t (k) VALUES (?)", 1).ExecContext(t.Context()))
	require.Equal(t, int32(1), client.statsForCluster(ClusterA).consecutiveFailures.Load())

	_, err = client.SwapSession(ClusterA, newMockSession())
	require.NoError(t, err)
	require.Zero(t, client.statsForCluster(ClusterA).consecutiveFailures.Load())
}

var _ cql.Session = (*blockingSession)(nil)
