package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/types"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestSQLDualWriteIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()

	// Create two in-memory SQLite databases
	dbA, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbA.Close()

	dbB, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbB.Close()

	// Create tables in both databases
	createTable := `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`
	_, err = dbA.ExecContext(ctx, createTable)
	require.NoError(t, err)
	_, err = dbB.ExecContext(ctx, createTable)
	require.NoError(t, err)

	// Create Helix SQL client
	client, err := helix.NewSQLClientFromDB(dbA, dbB)
	require.NoError(t, err)
	defer client.Close()

	// Test: Insert a row using dual-write
	result, err := client.ExecContext(ctx,
		"INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
		1, "Alice", "alice@example.com",
	)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify data exists in both databases
	var name, email string
	err = dbA.QueryRowContext(ctx, "SELECT name, email FROM users WHERE id = ?", 1).Scan(&name, &email)
	require.NoError(t, err)
	require.Equal(t, "Alice", name)
	require.Equal(t, "alice@example.com", email)

	err = dbB.QueryRowContext(ctx, "SELECT name, email FROM users WHERE id = ?", 1).Scan(&name, &email)
	require.NoError(t, err)
	require.Equal(t, "Alice", name)
	require.Equal(t, "alice@example.com", email)
}

func TestSQLDualWriteWithUpdateIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()

	dbA, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbA.Close()

	dbB, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbB.Close()

	// Setup tables
	createTable := `CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)`
	_, err = dbA.ExecContext(ctx, createTable)
	require.NoError(t, err)
	_, err = dbB.ExecContext(ctx, createTable)
	require.NoError(t, err)

	client, err := helix.NewSQLClientFromDB(dbA, dbB)
	require.NoError(t, err)
	defer client.Close()

	// Insert initial data
	_, err = client.ExecContext(ctx,
		"INSERT INTO products (id, name, price) VALUES (?, ?, ?)",
		1, "Widget", 9.99,
	)
	require.NoError(t, err)

	// Update the price
	_, err = client.ExecContext(ctx,
		"UPDATE products SET price = ? WHERE id = ?",
		19.99, 1,
	)
	require.NoError(t, err)

	// Verify update in both databases
	var price float64
	err = dbA.QueryRowContext(ctx, "SELECT price FROM products WHERE id = ?", 1).Scan(&price)
	require.NoError(t, err)
	require.Equal(t, 19.99, price)

	err = dbB.QueryRowContext(ctx, "SELECT price FROM products WHERE id = ?", 1).Scan(&price)
	require.NoError(t, err)
	require.Equal(t, 19.99, price)
}

func TestSQLReadWithStickyRoutingIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()

	dbA, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbA.Close()

	dbB, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbB.Close()

	// Setup tables with different data to verify which DB is read
	_, err = dbA.ExecContext(ctx, `CREATE TABLE config (key TEXT PRIMARY KEY, value TEXT)`)
	require.NoError(t, err)
	_, err = dbB.ExecContext(ctx, `CREATE TABLE config (key TEXT PRIMARY KEY, value TEXT)`)
	require.NoError(t, err)

	// Insert same key with different values to track which DB is read
	_, err = dbA.ExecContext(ctx, `INSERT INTO config (key, value) VALUES ('source', 'A')`)
	require.NoError(t, err)
	_, err = dbB.ExecContext(ctx, `INSERT INTO config (key, value) VALUES ('source', 'B')`)
	require.NoError(t, err)

	// Create client with sticky read strategy
	client, err := helix.NewSQLClientFromDB(dbA, dbB,
		helix.WithReadStrategy(policy.NewStickyRead()),
	)
	require.NoError(t, err)
	defer client.Close()

	// Read multiple times - should always get same source due to sticky routing
	sources := make([]string, 0, 5)
	for range 5 {
		row := client.QueryRowContext(ctx, "SELECT value FROM config WHERE key = ?", "source")
		require.NotNil(t, row)

		var value string
		err := row.Scan(&value)
		require.NoError(t, err)
		sources = append(sources, value)
	}

	// All reads should return the same source (sticky)
	for i := 1; i < len(sources); i++ {
		require.Equal(t, sources[0], sources[i], "Sticky routing should return same source")
	}
}

func TestSQLPartialFailureWithReplayIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()

	dbA, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbA.Close()

	dbB, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbB.Close()

	// Only create table in dbA - dbB will fail
	_, err = dbA.ExecContext(ctx, `CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL)`)
	require.NoError(t, err)

	// Track replayed operations
	replayer := &testReplayer{}

	client, err := helix.NewSQLClientFromDB(dbA, dbB,
		helix.WithReplayer(replayer),
	)
	require.NoError(t, err)
	defer client.Close()

	// This should succeed (A succeeds) but enqueue failure for B
	_, err = client.ExecContext(ctx,
		"INSERT INTO orders (id, amount) VALUES (?, ?)",
		1, 99.99,
	)
	require.NoError(t, err)

	// Verify data in dbA
	var amount float64
	err = dbA.QueryRowContext(ctx, "SELECT amount FROM orders WHERE id = ?", 1).Scan(&amount)
	require.NoError(t, err)
	require.Equal(t, 99.99, amount)

	// Verify replay was enqueued for dbB
	require.Len(t, replayer.payloads, 1)
	require.Equal(t, types.ClusterB, replayer.payloads[0].TargetCluster)
}

func TestSQLBothFailIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()

	dbA, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbA.Close()

	dbB, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbB.Close()

	// Don't create tables - both will fail
	client, err := helix.NewSQLClientFromDB(dbA, dbB)
	require.NoError(t, err)
	defer client.Close()

	// Both should fail
	_, err = client.ExecContext(ctx,
		"INSERT INTO nonexistent (id) VALUES (?)",
		1,
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, types.ErrBothClustersFailed))
}

func TestSQLMultipleWritesIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()

	dbA, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbA.Close()

	dbB, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbB.Close()

	// Setup
	createTable := `CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT)`
	_, err = dbA.ExecContext(ctx, createTable)
	require.NoError(t, err)
	_, err = dbB.ExecContext(ctx, createTable)
	require.NoError(t, err)

	client, err := helix.NewSQLClientFromDB(dbA, dbB)
	require.NoError(t, err)
	defer client.Close()

	// Insert multiple rows
	for i := 1; i <= 10; i++ {
		_, err = client.ExecContext(ctx,
			"INSERT INTO events (id, name) VALUES (?, ?)",
			i, "Event"+string(rune('A'+i-1)),
		)
		require.NoError(t, err)
	}

	// Verify both databases have all rows
	var countA, countB int
	err = dbA.QueryRowContext(ctx, "SELECT COUNT(*) FROM events").Scan(&countA)
	require.NoError(t, err)
	require.Equal(t, 10, countA)

	err = dbB.QueryRowContext(ctx, "SELECT COUNT(*) FROM events").Scan(&countB)
	require.NoError(t, err)
	require.Equal(t, 10, countB)
}

func TestSQLDeleteIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()

	dbA, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbA.Close()

	dbB, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer dbB.Close()

	// Setup with initial data in both DBs
	_, err = dbA.ExecContext(ctx, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)
	_, err = dbA.ExecContext(ctx, `INSERT INTO items (id, name) VALUES (1, 'Item1'), (2, 'Item2')`)
	require.NoError(t, err)

	_, err = dbB.ExecContext(ctx, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)
	_, err = dbB.ExecContext(ctx, `INSERT INTO items (id, name) VALUES (1, 'Item1'), (2, 'Item2')`)
	require.NoError(t, err)

	client, err := helix.NewSQLClientFromDB(dbA, dbB)
	require.NoError(t, err)
	defer client.Close()

	// Delete a row
	_, err = client.ExecContext(ctx, "DELETE FROM items WHERE id = ?", 1)
	require.NoError(t, err)

	// Verify deletion in both databases
	var countA, countB int
	err = dbA.QueryRowContext(ctx, "SELECT COUNT(*) FROM items").Scan(&countA)
	require.NoError(t, err)
	require.Equal(t, 1, countA)

	err = dbB.QueryRowContext(ctx, "SELECT COUNT(*) FROM items").Scan(&countB)
	require.NoError(t, err)
	require.Equal(t, 1, countB)
}

// testReplayer is a simple replayer for testing.
type testReplayer struct {
	payloads []types.ReplayPayload
}

func (r *testReplayer) Enqueue(_ context.Context, payload types.ReplayPayload) error {
	r.payloads = append(r.payloads, payload)

	return nil
}
