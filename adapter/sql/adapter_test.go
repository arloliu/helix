package sql_test

import (
	"context"
	"database/sql"
	"testing"

	sqladapter "github.com/arloliu/helix/adapter/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestNewDBAdapter(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	adapter := sqladapter.NewDBAdapter(db)
	require.NotNil(t, adapter)

	// Verify it implements the DB interface
	require.Implements(t, (*sqladapter.DB)(nil), adapter)
}

func TestWrapDB(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	adapter := sqladapter.WrapDB(db)
	require.NotNil(t, adapter)

	// Verify it implements the DB interface
	require.Implements(t, (*sqladapter.DB)(nil), adapter)
}

func TestDBAdapterExecContext(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	adapter := sqladapter.WrapDB(db)
	ctx := context.Background()

	// Create a table
	result, err := adapter.ExecContext(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(t, err)
	require.NotNil(t, result)

	// Insert a row
	result, err = adapter.ExecContext(ctx, "INSERT INTO test (id, name) VALUES (?, ?)", 1, "Alice")
	require.NoError(t, err)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rowsAffected)

	lastID, err := result.LastInsertId()
	require.NoError(t, err)
	require.Equal(t, int64(1), lastID)
}

func TestDBAdapterQueryContext(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	adapter := sqladapter.WrapDB(db)
	ctx := context.Background()

	// Setup
	_, err = adapter.ExecContext(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	_, err = adapter.ExecContext(ctx, "INSERT INTO test (id, name) VALUES (?, ?)", 1, "Alice")
	require.NoError(t, err)

	_, err = adapter.ExecContext(ctx, "INSERT INTO test (id, name) VALUES (?, ?)", 2, "Bob")
	require.NoError(t, err)

	// Query
	rows, err := adapter.QueryContext(ctx, "SELECT id, name FROM test ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		id   int
		name string
	}

	for rows.Next() {
		var r struct {
			id   int
			name string
		}
		err := rows.Scan(&r.id, &r.name)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 2)
	require.Equal(t, 1, results[0].id)
	require.Equal(t, "Alice", results[0].name)
	require.Equal(t, 2, results[1].id)
	require.Equal(t, "Bob", results[1].name)
}

func TestDBAdapterQueryRowContext(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	adapter := sqladapter.WrapDB(db)
	ctx := context.Background()

	// Setup
	_, err = adapter.ExecContext(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	_, err = adapter.ExecContext(ctx, "INSERT INTO test (id, name) VALUES (?, ?)", 1, "Alice")
	require.NoError(t, err)

	// Query single row
	row := adapter.QueryRowContext(ctx, "SELECT name FROM test WHERE id = ?", 1)
	require.NotNil(t, row)

	var name string
	err = row.Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "Alice", name)
}

func TestDBAdapterQueryRowContextNotFound(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	adapter := sqladapter.WrapDB(db)
	ctx := context.Background()

	// Setup
	_, err = adapter.ExecContext(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	// Query non-existent row
	row := adapter.QueryRowContext(ctx, "SELECT name FROM test WHERE id = ?", 999)
	require.NotNil(t, row)

	var name string
	err = row.Scan(&name)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestDBAdapterPingContext(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	adapter := sqladapter.WrapDB(db)
	ctx := context.Background()

	err = adapter.PingContext(ctx)
	require.NoError(t, err)
}

func TestDBAdapterClose(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)

	adapter := sqladapter.WrapDB(db)

	err = adapter.Close()
	require.NoError(t, err)

	// Subsequent operations should fail
	err = adapter.PingContext(context.Background())
	require.Error(t, err)
}

func TestDBAdapterExecContextError(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	adapter := sqladapter.WrapDB(db)
	ctx := context.Background()

	// Invalid SQL should return error
	_, err = adapter.ExecContext(ctx, "INVALID SQL SYNTAX")
	require.Error(t, err)
}

func TestDBAdapterQueryContextError(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	adapter := sqladapter.WrapDB(db)

	// Query non-existent table should return error.
	// We don't capture rows since it's nil on error and the linter
	// incorrectly flags it as unclosed.
	//nolint:rowserrcheck,sqlclosecheck // rows is nil when error is returned
	_, queryErr := adapter.QueryContext(context.Background(), "SELECT * FROM nonexistent_table")
	require.Error(t, queryErr)
}
