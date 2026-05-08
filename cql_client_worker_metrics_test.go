package helix_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
)

// Tests for the auto-injection of metrics into a caller-supplied
// replay.Worker. SPIKE_FINDINGS §6 surfaced the gotcha that a worker
// constructed without WithWorkerMetrics(...) silently uses NopMetrics
// internally — making worker-side IncReplaySuccess/Dropped/Error
// invisible to the client's metric collector. NewCQLClient now
// detects this and injects the client's metrics into the worker
// (without overwriting an explicit caller choice).

// TestNewCQLClient_AutoInjectsClientMetricsIntoCircuitBreaker verifies
// that FailoverPolicy auto-injection works for CircuitBreaker — a CB
// constructed without WithCircuitBreakerMetrics receives the client's
// metrics collector during NewCQLClient. The same mechanism extends to
// LatencyCircuitBreaker via its embedded *CircuitBreaker.
//
// This closes the gap surfaced when the e2e plain-CB test had to wire
// WithCircuitBreakerMetrics explicitly to observe IncCircuitBreakerTrip
// in the test collector.
func TestNewCQLClient_AutoInjectsClientMetricsIntoCircuitBreaker(t *testing.T) {
	mc := testutil.NewTestMetricsCollector()

	cb := policy.NewCircuitBreaker(
		policy.WithThreshold(2),
		policy.WithResetTimeout(50*time.Millisecond),
	)
	require.False(t, cb.MetricsConfigured(),
		"baseline: CB built without WithCircuitBreakerMetrics reports not-configured")

	client, err := helix.NewCQLClient(
		newAlwaysOKMock(), newAlwaysOKMock(),
		helix.WithMetrics(mc),
		helix.WithFailoverPolicy(cb),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	assert.True(t, cb.MetricsConfigured(),
		"NewCQLClient must auto-inject metrics into a CircuitBreaker that didn't have one")

	// Trip the breaker (call ShouldFailover after enough RecordFailures)
	// and verify the trip metric reaches the client's collector.
	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA)
	require.True(t, cb.ShouldFailover(types.ClusterA, nil),
		"CB should be open after threshold failures")
	assert.GreaterOrEqual(t, mc.CircuitBreakerTrips[types.ClusterA], int64(1),
		"trip metric must reach the client-level collector after auto-inject")
}

// TestNewCQLClient_DoesNotOverwriteExplicitCircuitBreakerMetrics
// verifies that a caller who explicitly passes
// WithCircuitBreakerMetrics(otherMc) keeps that choice — auto-inject
// must NOT overwrite.
func TestNewCQLClient_DoesNotOverwriteExplicitCircuitBreakerMetrics(t *testing.T) {
	clientMc := testutil.NewTestMetricsCollector()
	cbMc := testutil.NewTestMetricsCollector()

	cb := policy.NewCircuitBreaker(
		policy.WithThreshold(2),
		policy.WithResetTimeout(50*time.Millisecond),
		policy.WithCircuitBreakerMetrics(cbMc),
	)
	require.True(t, cb.MetricsConfigured())

	client, err := helix.NewCQLClient(
		newAlwaysOKMock(), newAlwaysOKMock(),
		helix.WithMetrics(clientMc),
		helix.WithFailoverPolicy(cb),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	cb.RecordFailure(types.ClusterA)
	cb.RecordFailure(types.ClusterA)
	require.True(t, cb.ShouldFailover(types.ClusterA, nil))

	assert.GreaterOrEqual(t, cbMc.CircuitBreakerTrips[types.ClusterA], int64(1),
		"explicit CB metrics collector must receive the trip")
	assert.Equal(t, int64(0), clientMc.CircuitBreakerTrips[types.ClusterA],
		"client-level collector must NOT receive the trip when CB had its own")
}

// TestNewCQLClient_AutoInjectsClientMetricsIntoWorker verifies that a
// worker constructed WITHOUT WithWorkerMetrics has the client's metrics
// collector injected during NewCQLClient.
func TestNewCQLClient_AutoInjectsClientMetricsIntoWorker(t *testing.T) {
	mc := testutil.NewTestMetricsCollector()

	memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(10))
	worker := replay.NewMemoryWorker(memReplayer, func(_ context.Context, _ types.ReplayPayload) error {
		return nil
	})
	require.False(t, worker.MetricsConfigured(),
		"baseline: worker built without WithWorkerMetrics reports not-configured")
	require.NotSame(t, mc, worker.Metrics(),
		"baseline: worker has its own (default NopMetrics), not the client's mc")

	client, err := helix.NewCQLClient(
		newAlwaysOKMock(), newAlwaysOKMock(),
		helix.WithMetrics(mc),
		helix.WithReplayer(memReplayer),
		helix.WithReplayWorker(worker),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	assert.Same(t, mc, worker.Metrics(),
		"NewCQLClient must inject the client's mc into the worker that didn't have one")
	assert.True(t, worker.MetricsConfigured(),
		"after auto-inject, MetricsConfigured reports true (subsequent SetMetrics is a no-op)")
}

// TestNewCQLClient_DoesNotOverwriteExplicitWorkerMetrics verifies that
// a caller who explicitly passes WithWorkerMetrics(otherMc) keeps that
// choice — auto-inject must NOT overwrite.
func TestNewCQLClient_DoesNotOverwriteExplicitWorkerMetrics(t *testing.T) {
	clientMc := testutil.NewTestMetricsCollector()
	workerMc := testutil.NewTestMetricsCollector()

	memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(10))
	worker := replay.NewMemoryWorker(memReplayer,
		func(_ context.Context, _ types.ReplayPayload) error { return nil },
		replay.WithWorkerMetrics(workerMc),
	)
	require.True(t, worker.MetricsConfigured(),
		"explicit WithWorkerMetrics marks the config as explicitly set")

	client, err := helix.NewCQLClient(
		newAlwaysOKMock(), newAlwaysOKMock(),
		helix.WithMetrics(clientMc),
		helix.WithReplayer(memReplayer),
		helix.WithReplayWorker(worker),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	assert.Same(t, workerMc, worker.Metrics(),
		"caller's explicit WithWorkerMetrics must not be overwritten by auto-inject")
}

// TestNewCQLClient_AutoMemoryWorkerInheritsMetrics verifies that the
// auto-memory worker (created internally when WithAutoMemoryWorker is
// set) also picks up the client's metrics collector by default.
func TestNewCQLClient_AutoMemoryWorkerInheritsMetrics(t *testing.T) {
	mc := testutil.NewTestMetricsCollector()

	client, err := helix.NewCQLClient(
		newAlwaysOKMock(), newAlwaysOKMock(),
		helix.WithMetrics(mc),
		helix.WithAutoMemoryWorker(10),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	worker, ok := client.Config().ReplayWorker.(*replay.Worker)
	require.True(t, ok, "auto-memory worker should be a *replay.Worker")
	assert.Same(t, mc, worker.Metrics(),
		"auto-memory worker should be using the client's mc (passed at construction)")
	assert.True(t, worker.MetricsConfigured(),
		"auto-memory worker should have metrics flagged as explicitly configured")
}
