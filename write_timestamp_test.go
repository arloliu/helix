package helix

import (
	"testing"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

func TestWrite_ZeroTimestampIsRejected(t *testing.T) {
	sessionA, sessionB := newMockSession(), newMockSession()
	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	err = client.Query("INSERT INTO t (id) VALUES (?)", 1).WithTimestamp(0).Exec()
	require.ErrorIs(t, err, types.ErrInvalidTimestamp)

	batch := client.Batch(LoggedBatch).Query("INSERT INTO t (id) VALUES (?)", 1).WithTimestamp(0)
	require.ErrorIs(t, batch.Exec(), types.ErrInvalidTimestamp)
	require.ErrorIs(t, batch.IterContext(t.Context()).Close(), types.ErrInvalidTimestamp)
	_, _, err = batch.ExecCAS()
	require.ErrorIs(t, err, types.ErrInvalidTimestamp)

	require.Empty(t, sessionA.queries, "a rejected write must not reach cluster A")
	require.Empty(t, sessionB.queries, "a rejected write must not reach cluster B")
}

func TestNewCQLClient_RejectsZeroTimestampProvider(t *testing.T) {
	_, err := NewCQLClient(newMockSession(), newMockSession(),
		WithTimestampProvider(func() int64 { return 0 }),
	)
	require.Error(t, err)
	require.True(t, types.IsOptionError(err))
	require.ErrorContains(t, err, "WithTimestampProvider")
}

func TestWrite_ProviderReturningZeroLaterIsRejected(t *testing.T) {
	calls := 0
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithTimestampProvider(func() int64 {
			calls++
			if calls == 1 {
				return 1
			}

			return 0
		}),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	err = client.Query("INSERT INTO t (id) VALUES (?)", 1).Exec()
	require.ErrorIs(t, err, types.ErrInvalidTimestamp)
}
