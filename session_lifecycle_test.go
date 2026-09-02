package helix

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// TestRefreshSession_KeepsSessionInstalledMeanwhile asserts that a session
// installed while the refresher runs is kept, and the refresher's own
// session is closed rather than the newer one.
func TestRefreshSession_KeepsSessionInstalledMeanwhile(t *testing.T) {
	original := newMockSession()
	operatorSession := newMockSession()
	refresherSession := newMockSession()

	var client *CQLClient
	refresher := func(_ context.Context, cluster ClusterID, _ error) (cql.Session, error) {
		// An operator swap lands while the refresher is building its session.
		old, err := client.SwapSession(cluster, operatorSession)
		require.NoError(t, err)
		require.Same(t, original, old)

		return refresherSession, nil
	}

	client, err := NewCQLClient(original, newMockSession(), WithSessionRefresher(refresher))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	err = client.RefreshSession(t.Context(), ClusterA)
	require.ErrorIs(t, err, types.ErrSessionReplaced)
	require.Same(t, operatorSession, client.loadSessionA(), "the operator's session must stay installed")
	require.True(t, refresherSession.closed.Load(), "the refresher's session must be closed, not leaked")
	require.False(t, operatorSession.closed.Load(), "the operator's session must not be closed")
	require.False(t, original.closed.Load(), "the swapped-out session belongs to the operator")
}

// TestClose_WaitsForRefreshInFlight asserts that Close does not return
// while the auto-refresh detector is still inside the refresher.
func TestClose_WaitsForRefreshInFlight(t *testing.T) {
	entered := make(chan struct{})
	var returned atomic.Bool
	refresher := func(ctx context.Context, _ ClusterID, _ error) (cql.Session, error) {
		close(entered)
		<-ctx.Done() // released by Close cancelling the detector
		returned.Store(true)

		return nil, ctx.Err()
	}

	failing := newMockSession()
	failing.execErr = errMatrixCluster
	client, err := NewCQLClient(failing, newMockSession(),
		WithSessionRefresher(refresher),
		WithAutoRefresh(
			WithAutoRefreshFailureThreshold(1),
			WithAutoRefreshSustainedFailureWindow(1),
			WithAutoRefreshMinRetryInterval(1),
			WithAutoRefreshCheckInterval(1),
			WithAutoRefreshRefreshTimeout(1<<30),
		),
	)
	require.NoError(t, err)

	// One failed write on A arms the detector; the next tick calls the refresher.
	_ = client.Query("INSERT INTO t (id) VALUES (1)").Exec()
	<-entered

	client.Close()
	require.True(t, returned.Load(), "Close must return only after the in-flight refresher has returned")
}
