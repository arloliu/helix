package helix

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// errUnreachableForTest is the one connectivity error the package's tests
// share: it satisfies the default auto-refresh classifier.
var errUnreachableForTest = fmt.Errorf("simulated cluster failure: %w", types.ErrClusterUnreachable)

func TestAutoRefresh_ClassifierRestoresEveryErrorCounting(t *testing.T) {
	sa := newMockSession()
	sa.execErr = errors.New("Unconfigured table")
	client, err := NewCQLClient(sa, newMockSession(),
		WithAutoRefresh(WithAutoRefreshFailureClassifier(func(error) bool { return true })),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.NoError(t, client.Query("INSERT INTO t (k) VALUES (?)", 1).ExecContext(t.Context()))
	require.Equal(t, int32(1), client.statsForCluster(ClusterA).consecutiveFailures.Load(),
		"the restore classifier counts every error, as before")
}

func TestAutoRefresh_LegTimeoutCountsAsConnectivityFailure(t *testing.T) {
	sa := newBlockingSession() // never released: only the leg deadline ends the write
	client, err := NewCQLClient(sa, newMockSession(),
		WithClusterWriteTimeout(10*time.Millisecond),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.NoError(t, client.Query("INSERT INTO t (k) VALUES (?)", 1).ExecContext(t.Context()))
	stats := client.statsForCluster(ClusterA)
	require.Equal(t, int32(1), stats.consecutiveFailures.Load(), "a Helix-owned leg deadline is a connectivity failure")
	require.ErrorIs(t, *stats.lastErr.Load(), types.ErrClusterTimeout)
	require.ErrorIs(t, *stats.lastErr.Load(), context.DeadlineExceeded, "the driver's error stays reachable")
}

func TestRecoveryProbe_OutcomesReachAutoRefreshStats(t *testing.T) {
	sa := newMockSession()
	adaptive := policy.NewAdaptiveDualWrite()
	var probeErr atomic.Pointer[error]
	failing := errUnreachableForTest
	probeErr.Store(&failing)
	probed := make(chan struct{}, 64)
	probe := RecoveryProbe{
		Probe: func(_ context.Context, _ cql.Session) error {
			select {
			case probed <- struct{}{}:
			default:
			}
			if e := probeErr.Load(); e != nil {
				return *e
			}

			return nil
		},
		Interval: 5 * time.Millisecond,
		Timeout:  time.Second,
	}
	client, err := NewCQLClient(sa, newMockSession(),
		WithWriteStrategy(adaptive),
		WithRecoveryProbe(probe),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	degradeClusterA(t, adaptive)

	stats := client.statsForCluster(ClusterA)
	require.Eventually(t, func() bool { return stats.consecutiveFailures.Load() >= 3 },
		time.Second, 5*time.Millisecond, "failed probes must count as connectivity failures")
	require.ErrorIs(t, *stats.lastErr.Load(), types.ErrClusterUnreachable)

	probeErr.Store(nil)
	require.Eventually(t, func() bool { return stats.consecutiveFailures.Load() == 0 },
		time.Second, 5*time.Millisecond, "a successful probe resets the counter")
}

func TestRecoveryProbe_TimeoutIsAConnectivityFailure(t *testing.T) {
	adaptive := policy.NewAdaptiveDualWrite()
	probe := RecoveryProbe{
		Probe: func(ctx context.Context, _ cql.Session) error {
			<-ctx.Done()

			return ctx.Err()
		},
		Interval: 5 * time.Millisecond,
		Timeout:  5 * time.Millisecond,
	}
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithWriteStrategy(adaptive),
		WithRecoveryProbe(probe),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	degradeClusterA(t, adaptive)

	stats := client.statsForCluster(ClusterA)
	require.Eventually(t, func() bool { return stats.consecutiveFailures.Load() >= 1 },
		time.Second, 5*time.Millisecond)
	require.ErrorIs(t, *stats.lastErr.Load(), types.ErrClusterTimeout, "the probe's own deadline is Helix's, so it counts")
}

// A probe that fails at once with an error that proves the session
// reachable (a schema or query error) must not be relabelled as a timeout
// and must not count toward auto-refresh.
func TestRecoveryProbe_ImmediateNonConnectivityErrorStaysUnwrapped(t *testing.T) {
	adaptive := policy.NewAdaptiveDualWrite()
	schemaErr := errors.New("unconfigured table")
	probed := make(chan struct{}, 64)
	probe := RecoveryProbe{
		Probe: func(context.Context, cql.Session) error {
			select {
			case probed <- struct{}{}:
			default:
			}

			return schemaErr
		},
		Interval: 5 * time.Millisecond,
		Timeout:  time.Second,
	}
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithWriteStrategy(adaptive),
		WithRecoveryProbe(probe),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	degradeClusterA(t, adaptive)

	for range 3 {
		select {
		case <-probed:
		case <-time.After(time.Second):
			t.Fatal("the probe did not run")
		}
	}
	stats := client.statsForCluster(ClusterA)
	require.Zero(t, stats.consecutiveFailures.Load(), "a reachable session's error is not a connectivity failure")
	require.Nil(t, stats.lastErr.Load(), "nothing was recorded, so nothing was relabelled as a timeout")
}

func TestRecoveryProbe_CancelledByCloseRecordsNothing(t *testing.T) {
	adaptive := policy.NewAdaptiveDualWrite()
	entered := make(chan struct{}, 1)
	probe := RecoveryProbe{
		Probe: func(ctx context.Context, _ cql.Session) error {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-ctx.Done()

			return ctx.Err()
		},
		Interval: 5 * time.Millisecond,
		Timeout:  time.Hour,
	}
	probes := &probeCounters{}
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithWriteStrategy(adaptive),
		WithRecoveryProbe(probe),
		WithMetrics(probes),
	)
	require.NoError(t, err)
	degradeClusterA(t, adaptive)
	<-entered
	holder := client.holderFor(ClusterA)

	client.Close()
	require.Zero(t, holder.stats.consecutiveFailures.Load(), "a probe the client cancelled is not a health observation")
	require.Zero(t, probes.failureA.Load(), "nor a probe failure")
}

func TestRefreshSession_ClosesOldSessionAfterGrace(t *testing.T) {
	old := newMockSession()
	refresher := func(context.Context, ClusterID, error) (cql.Session, error) { return newMockSession(), nil }
	const grace = 30 * time.Millisecond
	client, err := NewCQLClient(old, newMockSession(),
		WithSessionRefresher(refresher),
		WithAutoRefresh(WithAutoRefreshRefreshTimeout(grace)),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	before := time.Now()
	require.NoError(t, client.RefreshSession(t.Context(), ClusterA))
	require.False(t, old.closed.Load(), "the replaced session stays open for the grace period")
	require.Eventually(t, func() bool { return old.closed.Load() }, time.Second, time.Millisecond)
	require.GreaterOrEqual(t, time.Duration(old.closedAt.Load()-before.UnixNano()), grace)
}

// The auto-refresh detector qualifies one holder; a session installed
// after that (an operator's SwapSession) must not be replaced on the
// strength of its predecessor's failures.
func TestAutoRefresh_DoesNotReplaceASessionInstalledMeanwhile(t *testing.T) {
	old := newMockSession()
	built := newMockSession()
	refresher := func(context.Context, ClusterID, error) (cql.Session, error) { return built, nil }
	client, err := NewCQLClient(old, newMockSession(), WithSessionRefresher(refresher))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	qualified := client.holderFor(ClusterA)
	swappedIn := newMockSession()
	_, err = client.SwapSession(ClusterA, swappedIn)
	require.NoError(t, err)

	err = client.replaceHolder(t.Context(), ClusterA, &client.sessionA, qualified)
	require.ErrorIs(t, err, types.ErrSessionReplaced)
	require.Same(t, swappedIn, client.holderFor(ClusterA).s, "the newer session stays installed")
	require.False(t, swappedIn.closed.Load())
	require.True(t, built.closed.Load(), "the refresher's session is not leaked")
}

// A retired session is forgotten once its grace timer has closed it, so a
// long-lived client does not accumulate every session it ever replaced.
func TestRefreshSession_ForgetsClosedRetiredSessions(t *testing.T) {
	refresher := func(context.Context, ClusterID, error) (cql.Session, error) { return newMockSession(), nil }
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithSessionRefresher(refresher),
		WithAutoRefresh(WithAutoRefreshRefreshTimeout(time.Millisecond)),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	for range 3 {
		require.NoError(t, client.RefreshSession(t.Context(), ClusterA))
	}
	tracked := func() int {
		client.retired.mu.Lock()
		defer client.retired.mu.Unlock()

		return len(client.retired.entries)
	}
	require.Eventually(t, func() bool { return tracked() == 0 }, time.Second, time.Millisecond)
}

func TestRefreshSession_CloseClosesRetiredSessionsAtOnce(t *testing.T) {
	old := newMockSession()
	refresher := func(context.Context, ClusterID, error) (cql.Session, error) { return newMockSession(), nil }
	client, err := NewCQLClient(old, newMockSession(),
		WithSessionRefresher(refresher),
		WithAutoRefresh(WithAutoRefreshRefreshTimeout(time.Hour)),
	)
	require.NoError(t, err)

	require.NoError(t, client.RefreshSession(t.Context(), ClusterA))
	require.False(t, old.closed.Load())
	client.Close()
	require.True(t, old.closed.Load(), "Close does not wait for the grace period")
}

func TestRefreshSession_NoGraceWithoutAutoRefresh(t *testing.T) {
	old := newMockSession()
	refresher := func(context.Context, ClusterID, error) (cql.Session, error) { return newMockSession(), nil }
	client, err := NewCQLClient(old, newMockSession(), WithSessionRefresher(refresher))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.NoError(t, client.RefreshSession(t.Context(), ClusterA))
	require.True(t, old.closed.Load(), "without a refresh timeout the old session closes at once, as before")
}

// TestDeferredLeg_NeverCallsNowProvider proves the deferred completion path
// and Close never enter the configurable clock: a NowProvider that blocks
// forever would otherwise hang shutdown.
func TestDeferredLeg_NeverCallsNowProvider(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{name: "deferred success", err: nil},
		{name: "deferred failure", err: errUnreachableForTest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sb := newBlockingSession()
			sb.releaseErr = tc.err
			adaptive := policy.NewAdaptiveDualWrite()
			adaptive.ForceDegrade(ClusterB)

			var armed atomic.Bool
			block := make(chan struct{})
			blockingNow := func() int64 {
				if armed.Load() {
					<-block // never released
				}

				return time.Now().UnixNano()
			}
			client, err := NewCQLClient(newMockSession(), sb,
				WithWriteStrategy(adaptive),
				WithRecoveryProbeDisabled(),
				WithReplayer(&mockReplayer{}),
				func(c *ClientConfig) { c.NowProvider = blockingNow },
			)
			require.NoError(t, err)
			holder := client.holderFor(ClusterB)

			require.NoError(t, client.Query("INSERT INTO t (k) VALUES (?)", 1).ExecContext(t.Context()))
			<-sb.entered
			armed.Store(true) // from here on, any NowProvider call hangs
			close(sb.release)

			done := make(chan struct{})
			go func() { client.Close(); close(done) }()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("Close hung: the deferred completion or Close called the blocking NowProvider")
			}
			if tc.err != nil {
				require.Equal(t, int32(1), holder.stats.consecutiveFailures.Load(), "the deferred failure reached the holder it used")
			} else {
				require.Zero(t, holder.stats.consecutiveFailures.Load())
			}
		})
	}
}
