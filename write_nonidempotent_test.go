package helix

import (
	"errors"
	"testing"
	"time"

	"github.com/arloliu/helix/mirror"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// TestWrite_NonIdempotentIsNeverReplayed asserts that a non-idempotent
// statement takes the strict path: a partial failure is reported and
// nothing is enqueued for replay.
func TestWrite_NonIdempotentIsNeverReplayed(t *testing.T) {
	failB := errors.New("cluster B down")
	sessionA, sessionB := newMockSession(), newMockSession()
	sessionB.execErr = failB
	replayer := &mockReplayer{}
	client, err := NewCQLClient(sessionA, sessionB, WithReplayer(replayer))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	err = client.Query("UPDATE counters SET hits = hits + 1 WHERE id = ?", 1).NonIdempotent().Exec()
	var partial *types.PartialWriteError
	require.ErrorAs(t, err, &partial)
	require.Equal(t, ClusterA, partial.Acknowledged)
	require.Equal(t, ClusterB, partial.Unacknowledged)
	require.ErrorIs(t, partial.Cause, failB)
	require.Empty(t, replayer.payloads, "a non-idempotent statement must not be replayed")
}

// TestWrite_CounterBatchIsNonIdempotent asserts that a counter batch is
// treated as non-idempotent without any marker.
func TestWrite_CounterBatchIsNonIdempotent(t *testing.T) {
	failB := errors.New("cluster B down")
	sessionA, sessionB := newMockSession(), newMockSession()
	sessionB.execErr = failB
	replayer := &mockReplayer{}
	client, err := NewCQLClient(sessionA, sessionB, WithReplayer(replayer))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	err = client.Batch(CounterBatch).Query("UPDATE counters SET hits = hits + 1 WHERE id = ?", 1).Exec()
	var partial *types.PartialWriteError
	require.ErrorAs(t, err, &partial)
	require.Empty(t, replayer.payloads, "a counter batch must not be replayed")

	logged := client.Batch(LoggedBatch).Query("INSERT INTO t (id) VALUES (?)", 1)
	require.NoError(t, logged.Exec(), "a logged batch keeps replay semantics")
	require.Len(t, replayer.payloads, 1)
}

// TestWrite_NonIdempotentAllowsMirror asserts that, unlike Strict, a
// non-idempotent statement may still be mirrored.
func TestWrite_NonIdempotentAllowsMirror(t *testing.T) {
	rec := newRecordingMirror()
	client, err := NewCQLClient(newMockSession(), newMockSession())
	require.NoError(t, err)
	t.Cleanup(client.Close)
	installMirrorEngine(t, client, rec.execute(), mirror.WithWorkers(1))

	require.NoError(t, client.Query("UPDATE counters SET hits = hits + 1 WHERE id = ?", 1).NonIdempotent().Mirror().Exec())
	rec.waitForOne(t, time.Second)
	require.Len(t, rec.snapshot(), 1, "the mirror destination must receive the statement")

	require.ErrorIs(t, client.Query("UPDATE t SET v = 1").Strict().Mirror().Exec(), types.ErrStrictMirrorUnsupported)
}
