package helix

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// strictMetricsCollector wraps NopMetrics and tracks IncWriteSkipped calls.
type strictMetricsCollector struct {
	metrics.NopMetrics
	skippedA, skippedB atomic.Int32
}

func (s *strictMetricsCollector) IncWriteSkipped(cluster types.ClusterID) {
	if cluster == ClusterA {
		s.skippedA.Add(1)
	} else {
		s.skippedB.Add(1)
	}
}

var _ types.MetricsCollector = (*strictMetricsCollector)(nil)
var _ types.StrictMetrics = (*strictMetricsCollector)(nil)

// newDualClient creates a dual-cluster client with the given sessions and options.
func newDualClient(t *testing.T, sessionA, sessionB *mockSession, opts ...Option) *CQLClient {
	t.Helper()
	client, err := NewCQLClient(sessionA, sessionB, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	return client
}

// TestStrict_MirrorConflict_Query verifies that Strict().Mirror() on a query
// returns ErrStrictMirrorUnsupported before any write is attempted.
func TestStrict_MirrorConflict_Query(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	client := newDualClient(t, sa, sb)

	err := client.Query("INSERT INTO t (k) VALUES (?)", "x").Strict().Mirror().ExecContext(t.Context())

	require.ErrorIs(t, err, types.ErrStrictMirrorUnsupported)
	require.Empty(t, sa.queries, "no write must reach cluster A")
	require.Empty(t, sb.queries, "no write must reach cluster B")
}

// TestStrict_MirrorConflict_Batch verifies the same for batches.
func TestStrict_MirrorConflict_Batch(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	client := newDualClient(t, sa, sb)

	err := client.Batch(LoggedBatch).Query("INSERT INTO t (k) VALUES (?)", "x").Strict().Mirror().ExecContext(t.Context())

	require.ErrorIs(t, err, types.ErrStrictMirrorUnsupported)
	require.Empty(t, sa.queries, "no write must reach cluster A")
	require.Empty(t, sb.queries, "no write must reach cluster B")
}

// noStrictWriter is a WriteStrategy that intentionally omits ExecuteStrict to
// let tests verify that ErrStrictUnsupported is returned.
type noStrictWriter struct{}

func (noStrictWriter) Execute(ctx context.Context, writeA, writeB func(context.Context) error) (errA, errB error) {
	var wg sync.WaitGroup
	wg.Go(func() { errA = writeA(ctx) })
	wg.Go(func() { errB = writeB(ctx) })
	wg.Wait()

	return errA, errB
}

// sentinelStrictWriter injects fixed sentinel errors from ExecuteStrict without
// invoking the cluster write func for the injected slot. Used to verify that
// ErrWriteAsync / ErrWriteDropped are surfaced as PartialWriteError.Cause.
type sentinelStrictWriter struct {
	errA, errB error
}

func (s sentinelStrictWriter) Execute(ctx context.Context, writeA, writeB func(context.Context) error) (errA, errB error) {
	var wg sync.WaitGroup
	wg.Go(func() { errA = writeA(ctx) })
	wg.Go(func() { errB = writeB(ctx) })
	wg.Wait()
	return errA, errB
}

func (s sentinelStrictWriter) ExecuteStrict(ctx context.Context, writeA, writeB func(context.Context) error) (errA, errB error) {
	if s.errA != nil {
		errA = s.errA
	} else {
		errA = writeA(ctx)
	}
	if s.errB != nil {
		errB = s.errB
	} else {
		errB = writeB(ctx)
	}

	return errA, errB
}

// TestStrict_NilStrategy verifies that Strict() with no configured WriteStrategy
// falls back to inline concurrent write and succeeds when both clusters are healthy.
func TestStrict_NilStrategy(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	client := newDualClient(t, sa, sb)

	err := client.Query("INSERT INTO t (k) VALUES (?)", "x").Strict().ExecContext(t.Context())

	require.NoError(t, err)
}

// TestStrict_UnsupportedStrategy verifies that Strict() on a client whose
// WriteStrategy does not implement StrictWriter returns ErrStrictUnsupported.
func TestStrict_UnsupportedStrategy(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	client := newDualClient(t, sa, sb, WithWriteStrategy(noStrictWriter{}))

	err := client.Query("INSERT INTO t (k) VALUES (?)", "x").Strict().ExecContext(t.Context())

	require.ErrorIs(t, err, types.ErrStrictUnsupported)
}

// TestStrict_BothSucceed verifies that Strict() returns nil when both clusters ack.
func TestStrict_BothSucceed(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	client := newDualClient(t, sa, sb, WithWriteStrategy(policy.NewConcurrentDualWrite()))

	err := client.Query("INSERT INTO t (k) VALUES (?)", "x").Strict().ExecContext(t.Context())

	require.NoError(t, err)
}

// TestStrict_PartialFailure_Query verifies that when one cluster fails, Strict()
// returns *PartialWriteError without enqueueing for replay.
func TestStrict_PartialFailure_Query(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	sa.execErr = errors.New("cluster A unavailable")

	client := newDualClient(t, sa, sb, WithWriteStrategy(policy.NewConcurrentDualWrite()))

	err := client.Query("INSERT INTO t (k) VALUES (?)", "x").Strict().ExecContext(t.Context())

	pwe, ok := types.AsPartialWriteError(err)
	require.True(t, ok, "expected PartialWriteError, got %T: %v", err, err)
	assert.Equal(t, ClusterB, pwe.Acknowledged)
	assert.Equal(t, ClusterA, pwe.Unacknowledged)
	assert.ErrorIs(t, pwe.Cause, sa.execErr)
}

// TestStrict_PartialFailure_Batch verifies the same partial-failure semantics for batches.
func TestStrict_PartialFailure_Batch(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	sb.execErr = errors.New("cluster B unavailable")

	client := newDualClient(t, sa, sb, WithWriteStrategy(policy.NewConcurrentDualWrite()))

	err := client.Batch(LoggedBatch).
		Query("INSERT INTO t (k) VALUES (?)", "y").
		Strict().
		ExecContext(t.Context())

	pwe, ok := types.AsPartialWriteError(err)
	require.True(t, ok, "expected PartialWriteError, got %T: %v", err, err)
	assert.Equal(t, ClusterA, pwe.Acknowledged)
	assert.Equal(t, ClusterB, pwe.Unacknowledged)
	assert.ErrorIs(t, pwe.Cause, sb.execErr)
}

// TestStrict_BothFail verifies that when both clusters fail, Strict() returns
// *DualClusterError without replay.
func TestStrict_BothFail(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	sa.execErr = errors.New("A down")
	sb.execErr = errors.New("B down")

	client := newDualClient(t, sa, sb, WithWriteStrategy(policy.NewConcurrentDualWrite()))

	err := client.Query("INSERT INTO t (k) VALUES (?)", "x").Strict().ExecContext(t.Context())

	var dce *types.DualClusterError
	require.True(t, errors.As(err, &dce), "expected DualClusterError, got %T: %v", err, err)
	assert.ErrorIs(t, dce.ErrorA, sa.execErr)
	assert.ErrorIs(t, dce.ErrorB, sb.execErr)
}

// TestStrict_DrainOneClustersReturnsPartialWriteError verifies that with one
// cluster draining, Strict() returns *PartialWriteError{Cause: ErrClusterDraining}
// without replay enqueue.
func TestStrict_DrainOneClustersReturnsPartialWriteError(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	watcher := newMockTopologyWatcher()

	client := newDualClient(t, sa, sb,
		WithTopologyWatcher(watcher),
		WithWriteStrategy(policy.NewConcurrentDualWrite()),
	)

	watcher.SetDrain(ClusterA, true)
	time.Sleep(50 * time.Millisecond)

	err := client.Query("INSERT INTO t (k) VALUES (?)", "x").Strict().ExecContext(t.Context())

	pwe, ok := types.AsPartialWriteError(err)
	require.True(t, ok, "expected PartialWriteError, got %T: %v", err, err)
	assert.Equal(t, ClusterB, pwe.Acknowledged)
	assert.Equal(t, ClusterA, pwe.Unacknowledged)
	assert.ErrorIs(t, pwe.Cause, types.ErrClusterDraining)

	// Only the healthy cluster received the write
	require.Len(t, sa.queries, 0)
	require.Len(t, sb.queries, 1)
}

// TestStrict_BothDrainingReturnsDualClusterError verifies that with both clusters
// draining, Strict() returns *DualClusterError{ErrClusterDraining, ErrClusterDraining}.
func TestStrict_BothDrainingReturnsDualClusterError(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	watcher := newMockTopologyWatcher()

	client := newDualClient(t, sa, sb,
		WithTopologyWatcher(watcher),
		WithWriteStrategy(policy.NewConcurrentDualWrite()),
	)

	watcher.SetDrain(ClusterA, true)
	watcher.SetDrain(ClusterB, true)
	time.Sleep(50 * time.Millisecond)

	err := client.Query("INSERT INTO t (k) VALUES (?)", "x").Strict().ExecContext(t.Context())

	var dce *types.DualClusterError
	require.True(t, errors.As(err, &dce), "expected DualClusterError, got %T: %v", err, err)
	assert.ErrorIs(t, dce.ErrorA, types.ErrClusterDraining)
	assert.ErrorIs(t, dce.ErrorB, types.ErrClusterDraining)

	require.Empty(t, sa.queries)
	require.Empty(t, sb.queries)
}

// TestStrict_AdaptiveDegraded verifies that when AdaptiveDualWrite has one cluster
// degraded, Strict() returns *PartialWriteError{Cause: ErrClusterDegraded} instead
// of firing a background goroutine.
func TestStrict_AdaptiveDegraded(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	adaptive := policy.NewAdaptiveDualWrite()
	adaptive.ForceDegrade(types.ClusterA)

	client := newDualClient(t, sa, sb, WithWriteStrategy(adaptive))

	err := client.Query("INSERT INTO t (k) VALUES (?)", "x").Strict().ExecContext(t.Context())

	pwe, ok := types.AsPartialWriteError(err)
	require.True(t, ok, "expected PartialWriteError, got %T: %v", err, err)
	assert.Equal(t, ClusterB, pwe.Acknowledged)
	assert.Equal(t, ClusterA, pwe.Unacknowledged)
	assert.ErrorIs(t, pwe.Cause, types.ErrClusterDegraded)

	// Degraded cluster A must not have received any write
	require.Empty(t, sa.queries)
	// Healthy cluster B must have received the write
	require.Len(t, sb.queries, 1)
}

// TestStrict_SingleClusterIsNoop verifies that in single-cluster mode, Strict()
// is a no-op: the write runs normally and returns nil on success.
func TestStrict_SingleClusterIsNoop(t *testing.T) {
	sa := newMockSession()
	client, err := NewCQLClient(sa, nil)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	err = client.Query("INSERT INTO t (k) VALUES (?)", "x").Strict().ExecContext(t.Context())

	require.NoError(t, err)
	require.Len(t, sa.queries, 1)
}

// TestStrict_NoReplayOnPartialFailure verifies that on partial failure, no replay
// payload is enqueued — strict mode suppresses replay.
func TestStrict_NoReplayOnPartialFailure(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	sa.execErr = errors.New("A fail")

	replayer := &mockReplayer{}

	client := newDualClient(t, sa, sb,
		WithWriteStrategy(policy.NewConcurrentDualWrite()),
		WithReplayer(replayer),
	)

	err := client.Query("INSERT INTO t (k) VALUES (?)", "x").Strict().ExecContext(t.Context())

	require.True(t, types.IsPartialWrite(err))

	replayer.Lock()
	enqueued := len(replayer.payloads)
	replayer.Unlock()
	assert.Equal(t, 0, enqueued, "strict partial failure must not enqueue replay")
}

// TestStrict_WriteSkippedMetric_DegradedCluster verifies that IncWriteSkipped
// is incremented for the degraded cluster and IncWriteError is not.
func TestStrict_WriteSkippedMetric_DegradedCluster(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	adaptive := policy.NewAdaptiveDualWrite()
	adaptive.ForceDegrade(types.ClusterA)

	mc := &strictMetricsCollector{}
	client := newDualClient(t, sa, sb,
		WithWriteStrategy(adaptive),
		WithMetrics(mc),
		WithRecoveryProbeDisabled(),
	)

	err := client.Query("INSERT INTO t (k) VALUES (?)", "x").Strict().ExecContext(t.Context())

	require.True(t, types.IsPartialWrite(err))
	assert.Equal(t, int32(1), mc.skippedA.Load(), "IncWriteSkipped must fire for degraded cluster A")
	assert.Equal(t, int32(0), mc.skippedB.Load(), "IncWriteSkipped must not fire for healthy cluster B")
}

// TestStrict_WriteSkippedMetric_DrainingCluster verifies that IncWriteSkipped
// is incremented for a draining cluster in the strict drain path.
func TestStrict_WriteSkippedMetric_DrainingCluster(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	watcher := newMockTopologyWatcher()

	mc := &strictMetricsCollector{}
	client := newDualClient(t, sa, sb,
		WithWriteStrategy(policy.NewConcurrentDualWrite()),
		WithTopologyWatcher(watcher),
		WithMetrics(mc),
	)

	watcher.SetDrain(ClusterA, true)
	time.Sleep(50 * time.Millisecond)

	err := client.Query("INSERT INTO t (k) VALUES (?)", "x").Strict().ExecContext(t.Context())

	pwe, ok := types.AsPartialWriteError(err)
	require.True(t, ok)
	assert.ErrorIs(t, pwe.Cause, types.ErrClusterDraining)
	assert.Equal(t, int32(1), mc.skippedA.Load(), "IncWriteSkipped must fire for draining cluster A")
	assert.Equal(t, int32(0), mc.skippedB.Load())
}

// TestStrict_WriteSkippedMetric_BothDraining verifies that IncWriteSkipped fires
// for both clusters when both are draining.
func TestStrict_WriteSkippedMetric_BothDraining(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	watcher := newMockTopologyWatcher()

	mc := &strictMetricsCollector{}
	client := newDualClient(t, sa, sb,
		WithWriteStrategy(policy.NewConcurrentDualWrite()),
		WithTopologyWatcher(watcher),
		WithMetrics(mc),
	)

	watcher.SetDrain(ClusterA, true)
	watcher.SetDrain(ClusterB, true)
	time.Sleep(50 * time.Millisecond)

	err := client.Query("INSERT INTO t (k) VALUES (?)", "x").Strict().ExecContext(t.Context())

	var dce *types.DualClusterError
	require.True(t, errors.As(err, &dce))
	assert.Equal(t, int32(1), mc.skippedA.Load(), "IncWriteSkipped must fire for draining cluster A")
	assert.Equal(t, int32(1), mc.skippedB.Load(), "IncWriteSkipped must fire for draining cluster B")
}

// TestStrict_CustomStrategy_AsyncSentinel_SurfacedAsPartialWriteError verifies that
// ErrWriteAsync returned by a custom ExecuteStrict surfaces as PartialWriteError.Cause
// with the correct Acknowledged/Unacknowledged cluster assignment.
func TestStrict_CustomStrategy_AsyncSentinel_SurfacedAsPartialWriteError(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	client := newDualClient(t, sa, sb, WithWriteStrategy(sentinelStrictWriter{errA: types.ErrWriteAsync}))

	err := client.Query("INSERT INTO t (k) VALUES (?)", "x").Strict().ExecContext(t.Context())

	pwe, ok := types.AsPartialWriteError(err)
	require.True(t, ok, "expected PartialWriteError, got %T: %v", err, err)
	assert.ErrorIs(t, pwe.Cause, types.ErrWriteAsync)
	assert.Equal(t, ClusterB, pwe.Acknowledged)
	assert.Equal(t, ClusterA, pwe.Unacknowledged)
	require.Empty(t, sa.queries, "A was not invoked by the strategy")
	require.Len(t, sb.queries, 1, "B must have received the write")
}

// TestStrict_CustomStrategy_DroppedSentinel_SurfacedAsPartialWriteError verifies that
// ErrWriteDropped returned by a custom ExecuteStrict surfaces as PartialWriteError.Cause.
func TestStrict_CustomStrategy_DroppedSentinel_SurfacedAsPartialWriteError(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	client := newDualClient(t, sa, sb, WithWriteStrategy(sentinelStrictWriter{errB: types.ErrWriteDropped}))

	err := client.Query("INSERT INTO t (k) VALUES (?)", "x").Strict().ExecContext(t.Context())

	pwe, ok := types.AsPartialWriteError(err)
	require.True(t, ok, "expected PartialWriteError, got %T: %v", err, err)
	assert.ErrorIs(t, pwe.Cause, types.ErrWriteDropped)
	assert.Equal(t, ClusterA, pwe.Acknowledged)
	assert.Equal(t, ClusterB, pwe.Unacknowledged)
	require.Len(t, sa.queries, 1, "A must have received the write")
	require.Empty(t, sb.queries, "B was not invoked by the strategy")
}

// TestStrict_SyncDualWrite_ContextCanceled_ReturnsPartialWriteError verifies that
// when the context is canceled, SyncDualWrite's between-writes ctx check skips the
// second cluster and the client surfaces a PartialWriteError naming it as Unacknowledged.
// The mock ignores ctx so cluster A still executes; B is skipped by SyncDualWrite.
func TestStrict_SyncDualWrite_ContextCanceled_ReturnsPartialWriteError(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // pre-cancel: mock ignores ctx so A executes; SyncDualWrite ctx-check skips B

	client := newDualClient(t, sa, sb, WithWriteStrategy(policy.NewSyncDualWrite()))

	err := client.Query("INSERT INTO t (k) VALUES (?)", "x").Strict().ExecContext(ctx)

	pwe, ok := types.AsPartialWriteError(err)
	require.True(t, ok, "expected PartialWriteError, got %T: %v", err, err)
	assert.Equal(t, ClusterA, pwe.Acknowledged, "A completes before the context check")
	assert.Equal(t, ClusterB, pwe.Unacknowledged, "B is skipped by SyncDualWrite context check")
	assert.ErrorIs(t, pwe.Cause, context.Canceled)
	require.Len(t, sa.queries, 1, "A must have been called")
	require.Empty(t, sb.queries, "B must be skipped")
}

// TestStrict_ConcurrentDualWrite_DeadlineError_SurfacedAsPartialWriteError verifies
// that a context deadline error from one cluster under ConcurrentDualWrite is surfaced
// as PartialWriteError.Cause with the failing cluster named as Unacknowledged.
func TestStrict_ConcurrentDualWrite_DeadlineError_SurfacedAsPartialWriteError(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	sa.execErr = context.DeadlineExceeded // simulate A timing out mid-flight

	client := newDualClient(t, sa, sb, WithWriteStrategy(policy.NewConcurrentDualWrite()))

	err := client.Query("INSERT INTO t (k) VALUES (?)", "x").Strict().ExecContext(t.Context())

	pwe, ok := types.AsPartialWriteError(err)
	require.True(t, ok, "expected PartialWriteError, got %T: %v", err, err)
	assert.ErrorIs(t, pwe.Cause, context.DeadlineExceeded)
	assert.Equal(t, ClusterB, pwe.Acknowledged)
	assert.Equal(t, ClusterA, pwe.Unacknowledged)
}
