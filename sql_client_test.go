package helix

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"

	sqladapter "github.com/arloliu/helix/adapter/sql"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// mockDB implements sqladapter.DB for testing.
type mockDB struct {
	execErr    error
	execResult sql.Result
	queryErr   error
	pingErr    error
	closed     atomic.Bool
	execCount  atomic.Int32
	queryCount atomic.Int32
}

// Compile-time assertion.
var _ sqladapter.DB = (*mockDB)(nil)

func newMockDB() *mockDB {
	return &mockDB{
		execResult: &mockResult{lastID: 1, rowsAffected: 1},
	}
}

func (m *mockDB) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	m.execCount.Add(1)

	if m.execErr != nil {
		return nil, m.execErr
	}

	return m.execResult, nil
}

func (m *mockDB) QueryContext(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	m.queryCount.Add(1)

	return nil, m.queryErr
}

func (m *mockDB) QueryRowContext(_ context.Context, _ string, _ ...any) *sql.Row {
	m.queryCount.Add(1)

	// Return nil - in real tests we'd use sqlmock
	return nil
}

func (m *mockDB) PingContext(_ context.Context) error {
	return m.pingErr
}

func (m *mockDB) Close() error {
	m.closed.Store(true)

	return nil
}

// mockResult implements sql.Result for testing.
type mockResult struct {
	lastID       int64
	rowsAffected int64
}

func (r *mockResult) LastInsertId() (int64, error) {
	return r.lastID, nil
}

func (r *mockResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

func TestNewSQLClient(t *testing.T) {
	dbA := newMockDB()
	dbB := newMockDB()

	client, err := NewSQLClient(dbA, dbB)
	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestNewSQLClientNilDB(t *testing.T) {
	dbA := newMockDB()

	// primary is required
	_, err := NewSQLClient(nil, dbA)
	require.ErrorIs(t, err, types.ErrNilSession)

	// secondary is optional (nil = single-database mode)
	client, err := NewSQLClient(dbA, nil)
	require.NoError(t, err)
	require.True(t, client.IsSingleCluster())
}

func TestSQLClientSingleDatabaseMode(t *testing.T) {
	t.Run("exec goes to single database", func(t *testing.T) {
		db := newMockDB()

		client, err := NewSQLClient(db, nil)
		require.NoError(t, err)
		require.True(t, client.IsSingleCluster())
		defer client.Close()

		result, err := client.ExecContext(t.Context(), "INSERT INTO test (id) VALUES (?)", 1)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Only one database should be called
		require.Equal(t, int32(1), db.execCount.Load())
	})

	t.Run("query goes to single database", func(t *testing.T) {
		db := newMockDB()

		client, err := NewSQLClient(db, nil)
		require.NoError(t, err)
		defer client.Close()

		// The mock returns nil rows, but we verify the database was called
		//nolint:rowserrcheck // mock returns nil rows
		rows, _ := client.QueryContext(t.Context(), "SELECT * FROM test")
		if rows != nil {
			defer rows.Close()
		}

		require.Equal(t, int32(1), db.queryCount.Load())
	})

	t.Run("query row goes to single database", func(t *testing.T) {
		db := newMockDB()

		client, err := NewSQLClient(db, nil)
		require.NoError(t, err)
		defer client.Close()

		// The mock returns nil row, but we verify the database was called
		_ = client.QueryRowContext(t.Context(), "SELECT * FROM test WHERE id = ?", 1)
		require.Equal(t, int32(1), db.queryCount.Load())
	})

	t.Run("ping goes to single database", func(t *testing.T) {
		db := newMockDB()

		client, err := NewSQLClient(db, nil)
		require.NoError(t, err)
		defer client.Close()

		err = client.PingContext(t.Context())
		require.NoError(t, err)
	})

	t.Run("close only closes single database", func(t *testing.T) {
		db := newMockDB()

		client, err := NewSQLClient(db, nil)
		require.NoError(t, err)

		err = client.Close()
		require.NoError(t, err)
	})

	t.Run("dual database mode returns false for IsSingleCluster", func(t *testing.T) {
		dbA := newMockDB()
		dbB := newMockDB()

		client, err := NewSQLClient(dbA, dbB)
		require.NoError(t, err)
		require.False(t, client.IsSingleCluster())
		client.Close()
	})
}

func TestSQLClientExecBothSucceed(t *testing.T) {
	dbA := newMockDB()
	dbB := newMockDB()

	client, err := NewSQLClient(dbA, dbB)
	require.NoError(t, err)

	result, err := client.ExecContext(t.Context(), "INSERT INTO test (id) VALUES (?)", 1)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Both databases should have been called
	require.Equal(t, int32(1), dbA.execCount.Load())
	require.Equal(t, int32(1), dbB.execCount.Load())
}

func TestSQLClientExecPrimaryFails(t *testing.T) {
	dbA := newMockDB()
	dbA.execErr = errors.New("primary failed")
	dbB := newMockDB()

	client, err := NewSQLClient(dbA, dbB)
	require.NoError(t, err)

	// Should succeed because secondary succeeds
	result, err := client.ExecContext(t.Context(), "INSERT INTO test (id) VALUES (?)", 1)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestSQLClientExecSecondaryFails(t *testing.T) {
	dbA := newMockDB()
	dbB := newMockDB()
	dbB.execErr = errors.New("secondary failed")

	client, err := NewSQLClient(dbA, dbB)
	require.NoError(t, err)

	// Should succeed because primary succeeds
	result, err := client.ExecContext(t.Context(), "INSERT INTO test (id) VALUES (?)", 1)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestSQLClientExecBothFail(t *testing.T) {
	dbA := newMockDB()
	dbA.execErr = errors.New("primary failed")
	dbB := newMockDB()
	dbB.execErr = errors.New("secondary failed")

	client, err := NewSQLClient(dbA, dbB)
	require.NoError(t, err)

	result, err := client.ExecContext(t.Context(), "INSERT INTO test (id) VALUES (?)", 1)
	require.Error(t, err)
	require.ErrorIs(t, err, types.ErrBothClustersFailed)
	require.Nil(t, result)
}

func TestSQLClientExecWithReplayer(t *testing.T) {
	dbA := newMockDB()
	dbA.execErr = errors.New("primary failed")
	dbB := newMockDB()

	replayer := &mockSQLReplayer{}

	client, err := NewSQLClient(dbA, dbB, WithReplayer(replayer))
	require.NoError(t, err)

	_, err = client.ExecContext(t.Context(), "INSERT INTO test (id) VALUES (?)", 1)
	require.NoError(t, err)

	// Failed write to primary should be enqueued
	require.Len(t, replayer.payloads, 1)
	require.Equal(t, types.ClusterA, replayer.payloads[0].TargetCluster)
}

func TestSQLClientExecAfterClose(t *testing.T) {
	dbA := newMockDB()
	dbB := newMockDB()

	client, err := NewSQLClient(dbA, dbB)
	require.NoError(t, err)

	err = client.Close()
	require.NoError(t, err)

	_, err = client.ExecContext(t.Context(), "INSERT INTO test (id) VALUES (?)", 1)
	require.ErrorIs(t, err, types.ErrSessionClosed)
}

func TestSQLClientClose(t *testing.T) {
	dbA := newMockDB()
	dbB := newMockDB()

	client, err := NewSQLClient(dbA, dbB)
	require.NoError(t, err)

	err = client.Close()
	require.NoError(t, err)

	require.True(t, dbA.closed.Load())
	require.True(t, dbB.closed.Load())
}

func TestSQLClientCloseIdempotent(t *testing.T) {
	dbA := newMockDB()
	dbB := newMockDB()

	client, err := NewSQLClient(dbA, dbB)
	require.NoError(t, err)

	err = client.Close()
	require.NoError(t, err)

	// Second close should be no-op
	err = client.Close()
	require.NoError(t, err)
}

//nolint:rowserrcheck,sqlclosecheck // Mock returns nil rows
func TestSQLClientQueryContextWithStickyRead(t *testing.T) {
	dbA := newMockDB()
	dbB := newMockDB()

	client, err := NewSQLClient(dbA, dbB,
		WithReadStrategy(policy.NewStickyRead()),
	)
	require.NoError(t, err)

	// Execute multiple queries - should all go to same DB
	for range 5 {
		_, _ = client.QueryContext(t.Context(), "SELECT * FROM test")
	}

	// One DB should have all queries, other should have none
	totalQueries := dbA.queryCount.Load() + dbB.queryCount.Load()
	require.Equal(t, int32(5), totalQueries)

	// Sticky should route all to same DB
	hasAllQueries := dbA.queryCount.Load() == 5 || dbB.queryCount.Load() == 5
	require.True(t, hasAllQueries, "Sticky read should route all queries to same DB")
}

//nolint:rowserrcheck,sqlclosecheck // Mock returns nil rows
func TestSQLClientQueryContextWithFailover(t *testing.T) {
	dbA := newMockDB()
	dbA.queryErr = errors.New("primary query failed")
	dbB := newMockDB()

	// Use PrimaryOnlyRead so we know it will try A first
	client, err := NewSQLClient(dbA, dbB,
		WithReadStrategy(policy.NewPrimaryOnlyRead()),
	)
	require.NoError(t, err)

	// Query should failover to B
	_, err = client.QueryContext(t.Context(), "SELECT * FROM test")
	require.NoError(t, err)

	// Both should have been called (A failed, B succeeded)
	require.Equal(t, int32(1), dbA.queryCount.Load())
	require.Equal(t, int32(1), dbB.queryCount.Load())
}

func TestSQLClientPingContext(t *testing.T) {
	dbA := newMockDB()
	dbB := newMockDB()

	client, err := NewSQLClient(dbA, dbB)
	require.NoError(t, err)

	err = client.PingContext(t.Context())
	require.NoError(t, err)
}

func TestSQLClientPingContextOneFails(t *testing.T) {
	dbA := newMockDB()
	dbA.pingErr = errors.New("primary ping failed")
	dbB := newMockDB()

	client, err := NewSQLClient(dbA, dbB)
	require.NoError(t, err)

	// Should succeed because secondary succeeds
	err = client.PingContext(t.Context())
	require.NoError(t, err)
}

func TestSQLClientPingContextBothFail(t *testing.T) {
	dbA := newMockDB()
	dbA.pingErr = errors.New("primary ping failed")
	dbB := newMockDB()
	dbB.pingErr = errors.New("secondary ping failed")

	client, err := NewSQLClient(dbA, dbB)
	require.NoError(t, err)

	err = client.PingContext(t.Context())
	require.Error(t, err)
	require.ErrorIs(t, err, types.ErrBothClustersFailed)
}

func TestSQLClientExec(t *testing.T) {
	dbA := newMockDB()
	dbB := newMockDB()

	client, err := NewSQLClient(dbA, dbB)
	require.NoError(t, err)

	result, err := client.Exec("INSERT INTO test (id) VALUES (?)", 1)
	require.NoError(t, err)
	require.NotNil(t, result)
}

//nolint:rowserrcheck,sqlclosecheck // Mock returns nil rows
func TestSQLClientQuery(t *testing.T) {
	dbA := newMockDB()
	dbB := newMockDB()

	client, err := NewSQLClient(dbA, dbB)
	require.NoError(t, err)

	_, err = client.Query("SELECT * FROM test")
	require.NoError(t, err)
}

// mockSQLReplayer is a simple replayer for testing.
type mockSQLReplayer struct {
	payloads []types.ReplayPayload
}

func (m *mockSQLReplayer) Enqueue(_ context.Context, payload types.ReplayPayload) error {
	m.payloads = append(m.payloads, payload)

	return nil
}
