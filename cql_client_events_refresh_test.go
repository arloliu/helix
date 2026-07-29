package helix_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
)

func TestClusterEvents_SessionRefreshError(t *testing.T) {
	rec := newEventRecorder()
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	mockA := newFailingMock()
	mockB := newFailingMock() // healthy throughout
	refreshErr := errors.New("refresh failed")

	refresher := func(_ context.Context, _ helix.ClusterID, _ error) (cql.Session, error) {
		return nil, refreshErr
	}

	client, err := helix.NewCQLClient(mockA, mockB,
		helix.WithSessionRefresher(refresher),
		helix.WithAutoRefresh(fastAutoRefreshOpts()...),
		helix.WithOnClusterEvent(rec.handler),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	helix.SetClientNowFuncForTest(client, clock.NowFunc())

	mockA.fail.Store(true)
	driveOps(t, client, 5)
	clock.Advance(50 * time.Millisecond)

	helix.MaybeAutoRefreshForTest(client, helix.ClusterA)

	attempt := rec.waitFor(t, func(ev types.ClusterEvent) bool {
		return ev.Kind == types.EventSessionRefreshAttempt
	})
	require.Equal(t, types.ClusterA, attempt.Cluster)
	// Each dual-write op that fails on cluster A calls recordOpOutcomeAt
	// once for that cluster, incrementing consecutiveFailures by 1; 5
	// driven failing ops before the refresh call yield exactly 5.
	require.Equal(t, 5, attempt.Count,
		"Count must carry the exact qualifying consecutive-failure snapshot")

	failed := rec.waitFor(t, func(ev types.ClusterEvent) bool {
		return ev.Kind == types.EventSessionRefreshError
	})
	require.Equal(t, types.ClusterA, failed.Cluster)
	require.ErrorIs(t, failed.Err, refreshErr)
}

func TestClusterEvents_SessionRefreshSuccess(t *testing.T) {
	rec := newEventRecorder()
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	mockA := newFailingMock()
	mockB := newFailingMock()
	newMockA := newFailingMock() // the replacement session

	refresher := func(_ context.Context, _ helix.ClusterID, _ error) (cql.Session, error) {
		return newMockA, nil
	}

	client, err := helix.NewCQLClient(mockA, mockB,
		helix.WithSessionRefresher(refresher),
		helix.WithAutoRefresh(fastAutoRefreshOpts()...),
		helix.WithOnClusterEvent(rec.handler),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	helix.SetClientNowFuncForTest(client, clock.NowFunc())

	mockA.fail.Store(true)
	driveOps(t, client, 5)
	clock.Advance(50 * time.Millisecond)

	helix.MaybeAutoRefreshForTest(client, helix.ClusterA)

	ok := rec.waitFor(t, func(ev types.ClusterEvent) bool {
		return ev.Kind == types.EventSessionRefreshSuccess
	})
	require.Equal(t, types.ClusterA, ok.Cluster)
}

// gateAttemptMetrics blocks inside IncSessionRefreshAttempt, creating a
// deterministic window in which concurrent successes reset the failure
// counter — proving the event carries the qualifying snapshot, not a
// re-read of the live counter.
type gateAttemptMetrics struct {
	types.MetricsCollector
	entered chan struct{}
	release chan struct{}
}

func (g *gateAttemptMetrics) IncSessionRefreshAttempt(_ types.ClusterID) {
	close(g.entered)
	<-g.release
}
func (g *gateAttemptMetrics) IncSessionRefreshSuccess(_ types.ClusterID) {}
func (g *gateAttemptMetrics) IncSessionRefreshError(_ types.ClusterID)   {}

func TestClusterEvents_SessionRefreshCountIsQualifyingSnapshot(t *testing.T) {
	rec := newEventRecorder()
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	mockA := newFailingMock()
	mockB := newFailingMock()
	gate := &gateAttemptMetrics{
		MetricsCollector: testutil.NewTestMetricsCollector(),
		entered:          make(chan struct{}),
		release:          make(chan struct{}),
	}

	refresher := func(_ context.Context, _ helix.ClusterID, _ error) (cql.Session, error) {
		return newFailingMock(), nil
	}

	client, err := helix.NewCQLClient(mockA, mockB,
		helix.WithSessionRefresher(refresher),
		helix.WithAutoRefresh(fastAutoRefreshOpts()...),
		helix.WithMetrics(gate),
		helix.WithOnClusterEvent(rec.handler),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	helix.SetClientNowFuncForTest(client, clock.NowFunc())

	mockA.fail.Store(true)
	driveOps(t, client, 5)
	clock.Advance(50 * time.Millisecond)

	refreshDone := make(chan struct{})
	go func() {
		helix.MaybeAutoRefreshForTest(client, helix.ClusterA)
		close(refreshDone)
	}()

	// The detector is parked inside IncSessionRefreshAttempt — AFTER the
	// qualification snapshot. Reset the failure counter via successes.
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh attempt never reached the metrics gate")
	}
	mockA.fail.Store(false)
	driveOps(t, client, 3) // successes reset consecutiveFailures to 0
	close(gate.release)

	select {
	case <-refreshDone:
	case <-time.After(5 * time.Second):
		t.Fatal("maybeAutoRefresh did not complete")
	}

	attempt := rec.waitFor(t, func(ev types.ClusterEvent) bool {
		return ev.Kind == types.EventSessionRefreshAttempt
	})
	// The attempt event must carry the 5 failures that qualified this
	// refresh, not the live counter — the 3 concurrent successes above
	// reset consecutiveFailures to 0 before the event was emitted.
	require.Equal(t, 5, attempt.Count,
		"event must report the snapshot that qualified the attempt, not the reset counter")
}
