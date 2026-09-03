package helix

import (
	"context"
	"errors"
	"testing"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// legRecordingStrategy runs both legs and keeps what each returned, so a
// test can see exactly what the write strategy observed.
type legRecordingStrategy struct {
	errA, errB error
}

func (s *legRecordingStrategy) Execute(
	ctx context.Context,
	writeA func(context.Context) error,
	writeB func(context.Context) error,
) (resultA, resultB error) {
	s.errA = writeA(ctx)
	s.errB = writeB(ctx)

	return s.errA, s.errB
}

// TestWrite_DrainingLegIsSkippedInsideTheStrategy asserts that a draining
// cluster is skipped by its own write leg: the strategy still runs, sees
// ErrClusterDraining for that leg, the session is never contacted, the
// skipped leg is counted, and the write is enqueued for replay to the
// draining cluster.
func TestWrite_DrainingLegIsSkippedInsideTheStrategy(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	strategy := &legRecordingStrategy{}
	replayer := &mockReplayer{}
	metrics := &strictMetricsCollector{}

	client, err := NewCQLClient(sessionA, sessionB,
		WithWriteStrategy(strategy),
		WithReplayer(replayer),
		WithMetrics(metrics),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	client.drainA.Store(true)

	require.NoError(t, client.Query("INSERT INTO t (id) VALUES (1)").Exec())

	require.ErrorIs(t, strategy.errA, types.ErrClusterDraining)
	require.NoError(t, strategy.errB)
	require.Empty(t, sessionA.queries, "draining cluster must not be contacted")
	require.Len(t, sessionB.queries, 1)
	require.Equal(t, int32(1), metrics.skippedA.Load())
	require.Equal(t, int32(0), metrics.skippedB.Load())

	replayer.Lock()
	defer replayer.Unlock()
	require.Len(t, replayer.payloads, 1)
	require.Equal(t, ClusterA, replayer.payloads[0].TargetCluster)
}

// TestWrite_HealthyLegFailsWhileOtherDrains asserts that when the only
// cluster that can acknowledge the write fails, no cluster acknowledged
// it: both legs are enqueued for replay and the caller sees the failure
// inside a NoSynchronousAckError.
func TestWrite_HealthyLegFailsWhileOtherDrains(t *testing.T) {
	errB := errors.New("cluster B rejected the write")
	sessionA := newMockSession()
	sessionB := newMockSession()
	sessionB.execErr = errB
	replayer := &mockReplayer{}

	client, err := NewCQLClient(sessionA, sessionB, WithReplayer(replayer))
	require.NoError(t, err)
	t.Cleanup(client.Close)
	client.drainA.Store(true)

	err = client.Query("INSERT INTO t (id) VALUES (1)").Exec()
	require.ErrorIs(t, err, errB)
	require.ErrorIs(t, err, types.ErrNoSynchronousAck)
	require.ErrorIs(t, err, types.ErrClusterDraining)
	require.Empty(t, sessionA.queries)

	replayer.Lock()
	defer replayer.Unlock()
	require.Len(t, replayer.payloads, 2, "both unacknowledged legs are replayed")
	require.Equal(t, ClusterA, replayer.payloads[0].TargetCluster)
	require.Equal(t, ClusterB, replayer.payloads[1].TargetCluster)
}

// TestCAS_AvoidsDrainingCluster asserts that a lightweight transaction is
// routed away from a draining cluster like any other read.
func TestCAS_AvoidsDrainingCluster(t *testing.T) {
	sessionA, sessionB := newMockSession(), newMockSession()
	client, err := NewCQLClient(sessionA, sessionB,
		WithReadStrategy(newMockReadStrategy(ClusterA, ClusterB, true)),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	client.drainA.Store(true)

	_, err = client.Query("UPDATE t SET v = ? WHERE k = ? IF v = ?", 2, 1, 1).ScanCAS()
	require.NoError(t, err)
	require.Empty(t, sessionA.queries, "the draining cluster must not receive the transaction")
	require.Len(t, sessionB.queries, 1)

	batch := client.Batch(LoggedBatch).Query("UPDATE t SET v = ? WHERE k = ? IF v = ?", 2, 1, 1)
	_, _, err = batch.ExecCAS()
	require.NoError(t, err)
	require.Empty(t, sessionA.queries)
}
