package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/adapter/cql"
	v1 "github.com/arloliu/helix/adapter/cql/v1" //nolint:revive // goimports requires explicit alias
	"github.com/arloliu/helix/policy"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
)

func TestCQLDualWriteIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, sessionB := getSharedSessions(t)

	// Create unique tables for this test on both clusters
	usersTable := createTestTableOnBoth(t, "users", usersTableSchema)

	// Create Helix CQL client with v1 adapters
	// Note: We don't defer client.Close() because it would close the shared gocql sessions.
	// The sessions are managed by TestMain and cleaned up in teardownSharedContainers.
	helixSessionA := v1.NewSession(sessionA)
	helixSessionB := v1.NewSession(sessionB)

	client, err := helix.NewCQLClient(helixSessionA, helixSessionB)
	require.NoError(t, err)

	// Generate a test UUID
	userID := gocql.TimeUUID()
	now := time.Now()

	// Test: Insert a row using dual-write
	err = client.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
		userID, "Alice", "alice@example.com", now,
	).ExecContext(ctx)
	require.NoError(t, err)

	// Verify data exists in both clusters
	var name, email string
	var createdAt time.Time

	err = sessionA.Query(
		"SELECT name, email, created_at FROM "+usersTable+" WHERE id = ?", userID,
	).Scan(&name, &email, &createdAt)
	require.NoError(t, err)
	require.Equal(t, "Alice", name)
	require.Equal(t, "alice@example.com", email)

	err = sessionB.Query(
		"SELECT name, email, created_at FROM "+usersTable+" WHERE id = ?", userID,
	).Scan(&name, &email, &createdAt)
	require.NoError(t, err)
	require.Equal(t, "Alice", name)
	require.Equal(t, "alice@example.com", email)
}

func TestCQLDualWriteWithUpdateIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, sessionB := getSharedSessions(t)

	productsTable := createTestTableOnBoth(t, "products", productsTableSchema)

	helixSessionA := v1.NewSession(sessionA)
	helixSessionB := v1.NewSession(sessionB)

	client, err := helix.NewCQLClient(helixSessionA, helixSessionB)
	require.NoError(t, err)

	productID := gocql.TimeUUID()

	// Insert initial data
	err = client.Query(
		"INSERT INTO "+productsTable+" (id, name, price, stock) VALUES (?, ?, ?, ?)",
		productID, "Widget", 9.99, 100,
	).ExecContext(ctx)
	require.NoError(t, err)

	// Update the stock
	err = client.Query(
		"UPDATE "+productsTable+" SET stock = ? WHERE id = ?",
		50, productID,
	).ExecContext(ctx)
	require.NoError(t, err)

	// Verify update in both clusters
	var stock int
	err = sessionA.Query(
		"SELECT stock FROM "+productsTable+" WHERE id = ?", productID,
	).Scan(&stock)
	require.NoError(t, err)
	require.Equal(t, 50, stock)

	err = sessionB.Query(
		"SELECT stock FROM "+productsTable+" WHERE id = ?", productID,
	).Scan(&stock)
	require.NoError(t, err)
	require.Equal(t, 50, stock)
}

func TestCQLReadWithStickyRoutingIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, sessionB := getSharedSessions(t)

	usersTable := createTestTableOnBoth(t, "users", usersTableSchema)

	helixSessionA := v1.NewSession(sessionA)
	helixSessionB := v1.NewSession(sessionB)

	// Create client with sticky read strategy
	client, err := helix.NewCQLClient(helixSessionA, helixSessionB,
		helix.WithReadStrategy(policy.NewStickyRead()),
	)
	require.NoError(t, err)

	userID := gocql.TimeUUID()

	// Insert data through Helix
	err = client.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
		userID, "Bob", "bob@example.com", time.Now(),
	).ExecContext(ctx)
	require.NoError(t, err)

	// Read should use sticky routing (same cluster each time)
	var name string
	err = client.Query(
		"SELECT name FROM "+usersTable+" WHERE id = ?", userID,
	).ScanContext(ctx, &name)
	require.NoError(t, err)
	require.Equal(t, "Bob", name)
}

func TestCQLBatchWriteIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, sessionB := getSharedSessions(t)

	usersTable := createTestTableOnBoth(t, "users", usersTableSchema)

	helixSessionA := v1.NewSession(sessionA)
	helixSessionB := v1.NewSession(sessionB)

	client, err := helix.NewCQLClient(helixSessionA, helixSessionB)
	require.NoError(t, err)

	user1ID := gocql.TimeUUID()
	user2ID := gocql.TimeUUID()
	now := time.Now()

	// Create batch with multiple inserts
	batch := client.Batch(helix.LoggedBatch)
	batch.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
		user1ID, "User1", "user1@example.com", now,
	)
	batch.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
		user2ID, "User2", "user2@example.com", now,
	)

	err = batch.ExecContext(ctx)
	require.NoError(t, err)

	// Verify both users exist in cluster A
	var name string
	err = sessionA.Query("SELECT name FROM "+usersTable+" WHERE id = ?", user1ID).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "User1", name)

	err = sessionA.Query("SELECT name FROM "+usersTable+" WHERE id = ?", user2ID).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "User2", name)

	// Verify both users exist in cluster B
	err = sessionB.Query("SELECT name FROM "+usersTable+" WHERE id = ?", user1ID).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "User1", name)

	err = sessionB.Query("SELECT name FROM "+usersTable+" WHERE id = ?", user2ID).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "User2", name)
}

func TestCQLDeleteIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, sessionB := getSharedSessions(t)

	usersTable := createTestTableOnBoth(t, "users", usersTableSchema)

	helixSessionA := v1.NewSession(sessionA)
	helixSessionB := v1.NewSession(sessionB)

	client, err := helix.NewCQLClient(helixSessionA, helixSessionB)
	require.NoError(t, err)

	userID := gocql.TimeUUID()

	// Insert data
	err = client.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
		userID, "ToDelete", "delete@example.com", time.Now(),
	).ExecContext(ctx)
	require.NoError(t, err)

	// Delete the row
	err = client.Query(
		"DELETE FROM "+usersTable+" WHERE id = ?", userID,
	).ExecContext(ctx)
	require.NoError(t, err)

	// Verify deletion in both clusters
	var name string
	err = sessionA.Query("SELECT name FROM "+usersTable+" WHERE id = ?", userID).Scan(&name)
	require.Error(t, err) // Should be not found

	err = sessionB.Query("SELECT name FROM "+usersTable+" WHERE id = ?", userID).Scan(&name)
	require.Error(t, err) // Should be not found
}

func TestCQLConsistencyLevelIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, sessionB := getSharedSessions(t)

	usersTable := createTestTableOnBoth(t, "users", usersTableSchema)

	helixSessionA := v1.NewSession(sessionA)
	helixSessionB := v1.NewSession(sessionB)

	client, err := helix.NewCQLClient(helixSessionA, helixSessionB)
	require.NoError(t, err)

	userID := gocql.TimeUUID()

	// Insert with specific consistency level
	err = client.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
		userID, "ConsistencyTest", "consistency@example.com", time.Now(),
	).Consistency(helix.One).ExecContext(ctx)
	require.NoError(t, err)

	// Verify data exists
	var name string
	err = sessionA.Query("SELECT name FROM "+usersTable+" WHERE id = ?", userID).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "ConsistencyTest", name)
}

func TestCQLIteratorIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, sessionB := getSharedSessions(t)

	usersTable := createTestTableOnBoth(t, "users", usersTableSchema)

	helixSessionA := v1.NewSession(sessionA)
	helixSessionB := v1.NewSession(sessionB)

	client, err := helix.NewCQLClient(helixSessionA, helixSessionB)
	require.NoError(t, err)

	// Insert multiple users
	userIDs := make([]gocql.UUID, 5)
	for i := 0; i < 5; i++ {
		userIDs[i] = gocql.TimeUUID()
		err = client.Query(
			"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
			userIDs[i], "User"+string(rune('A'+i)), "user@example.com", time.Now(),
		).ExecContext(ctx)
		require.NoError(t, err)
	}

	// Use iterator to read all users
	iter := client.Query("SELECT id, name FROM " + usersTable).IterContext(ctx)
	require.NotNil(t, iter)

	var id gocql.UUID
	var name string
	count := 0
	for iter.Scan(&id, &name) {
		count++
	}
	require.NoError(t, iter.Close())
	require.Equal(t, 5, count)
}

func TestCQLRoundRobinReadIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, sessionB := getSharedSessions(t)

	usersTable := createTestTableOnBoth(t, "users", usersTableSchema)

	helixSessionA := v1.NewSession(sessionA)
	helixSessionB := v1.NewSession(sessionB)

	// Create client with round-robin read strategy
	client, err := helix.NewCQLClient(helixSessionA, helixSessionB,
		helix.WithReadStrategy(policy.NewRoundRobinRead()),
	)
	require.NoError(t, err)

	userID := gocql.TimeUUID()

	// Insert data
	err = client.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
		userID, "RoundRobin", "roundrobin@example.com", time.Now(),
	).ExecContext(ctx)
	require.NoError(t, err)

	// Multiple reads should alternate (we can't easily verify which cluster was used,
	// but we can verify reads succeed)
	for i := 0; i < 4; i++ {
		var name string
		err = client.Query(
			"SELECT name FROM "+usersTable+" WHERE id = ?", userID,
		).ScanContext(ctx, &name)
		require.NoError(t, err)
		require.Equal(t, "RoundRobin", name)
	}
}

func TestCQLFailoverReadIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, sessionB := getSharedSessions(t)

	usersTable := createTestTableOnBoth(t, "users", usersTableSchema)

	helixSessionA := v1.NewSession(sessionA)
	helixSessionB := v1.NewSession(sessionB)

	// Create client with active failover policy
	client, err := helix.NewCQLClient(helixSessionA, helixSessionB,
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
	)
	require.NoError(t, err)

	userID := gocql.TimeUUID()

	// Insert data
	err = client.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
		userID, "FailoverTest", "failover@example.com", time.Now(),
	).ExecContext(ctx)
	require.NoError(t, err)

	// Read should succeed (failover would kick in if primary failed)
	var name string
	err = client.Query(
		"SELECT name FROM "+usersTable+" WHERE id = ?", userID,
	).ScanContext(ctx, &name)
	require.NoError(t, err)
	require.Equal(t, "FailoverTest", name)
}

func TestCQLMultipleWritesIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, sessionB := getSharedSessions(t)

	usersTable := createTestTableOnBoth(t, "users", usersTableSchema)

	helixSessionA := v1.NewSession(sessionA)
	helixSessionB := v1.NewSession(sessionB)

	client, err := helix.NewCQLClient(helixSessionA, helixSessionB)
	require.NoError(t, err)

	// Insert many rows
	numRows := 20
	userIDs := make([]gocql.UUID, numRows)
	for i := 0; i < numRows; i++ {
		userIDs[i] = gocql.TimeUUID()
		err = client.Query(
			"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
			userIDs[i], "BulkUser", "bulk@example.com", time.Now(),
		).ExecContext(ctx)
		require.NoError(t, err)
	}

	// Verify all rows exist in both clusters
	for _, id := range userIDs {
		var name string
		err = sessionA.Query("SELECT name FROM "+usersTable+" WHERE id = ?", id).Scan(&name)
		require.NoError(t, err)
		require.Equal(t, "BulkUser", name)

		err = sessionB.Query("SELECT name FROM "+usersTable+" WHERE id = ?", id).Scan(&name)
		require.NoError(t, err)
		require.Equal(t, "BulkUser", name)
	}
}

func TestCQLClientLifecycleIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)

	helixSessionA := v1.NewSession(sessionA)
	helixSessionB := v1.NewSession(sessionB)

	// Create client multiple times to verify it can be done
	// Note: We do NOT call client.Close() because that would close the shared
	// underlying gocql sessions which are reused across all integration tests.
	// Close behavior is tested separately in unit tests.
	for i := 0; i < 3; i++ {
		client, err := helix.NewCQLClient(helixSessionA, helixSessionB)
		require.NoError(t, err)
		require.NotNil(t, client)

		// Client should be usable
		require.NotNil(t, client.Query("SELECT now() FROM system.local"))
	}
}

func TestCQLPaginationIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, sessionB := getSharedSessions(t)

	usersTable := createTestTableOnBoth(t, "users", usersTableSchema)

	helixSessionA := v1.NewSession(sessionA)
	helixSessionB := v1.NewSession(sessionB)

	client, err := helix.NewCQLClient(helixSessionA, helixSessionB)
	require.NoError(t, err)

	// Insert 10 users for pagination test
	for i := 0; i < 10; i++ {
		err = client.Query(
			"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
			gocql.TimeUUID(), "PageUser", "page@example.com", time.Now(),
		).ExecContext(ctx)
		require.NoError(t, err)
	}

	// Query with page size of 3
	iter := client.Query("SELECT id, name FROM " + usersTable).PageSize(3).IterContext(ctx)
	require.NotNil(t, iter)

	// Read first page
	var id gocql.UUID
	var name string
	pageCount := 0
	for iter.Scan(&id, &name) {
		pageCount++
	}
	require.NoError(t, iter.Close())

	// Should have read all 10 rows (iterator handles pagination internally)
	require.Equal(t, 10, pageCount)
}

func TestCQLMapScanIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, sessionB := getSharedSessions(t)

	usersTable := createTestTableOnBoth(t, "users", usersTableSchema)

	helixSessionA := v1.NewSession(sessionA)
	helixSessionB := v1.NewSession(sessionB)

	client, err := helix.NewCQLClient(helixSessionA, helixSessionB)
	require.NoError(t, err)

	userID := gocql.TimeUUID()

	// Insert a user
	err = client.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
		userID, "MapScanUser", "mapscan@example.com", time.Now(),
	).ExecContext(ctx)
	require.NoError(t, err)

	// Query using MapScan
	result := make(map[string]any)
	err = client.Query(
		"SELECT name, email FROM "+usersTable+" WHERE id = ?", userID,
	).WithContext(ctx).MapScan(result)
	require.NoError(t, err)

	require.Equal(t, "MapScanUser", result["name"])
	require.Equal(t, "mapscan@example.com", result["email"])
}

func TestCQLSliceMapIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, sessionB := getSharedSessions(t)

	usersTable := createTestTableOnBoth(t, "users", usersTableSchema)

	helixSessionA := v1.NewSession(sessionA)
	helixSessionB := v1.NewSession(sessionB)

	client, err := helix.NewCQLClient(helixSessionA, helixSessionB)
	require.NoError(t, err)

	// Insert 3 users
	for i := 0; i < 3; i++ {
		err = client.Query(
			"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
			gocql.TimeUUID(), "SliceMapUser", "slicemap@example.com", time.Now(),
		).ExecContext(ctx)
		require.NoError(t, err)
	}

	// Query using SliceMap
	iter := client.Query("SELECT name, email FROM " + usersTable).IterContext(ctx)
	require.NotNil(t, iter)

	results, err := iter.SliceMap()
	require.NoError(t, err)
	require.Len(t, results, 3)

	for _, row := range results {
		require.Equal(t, "SliceMapUser", row["name"])
		require.Equal(t, "slicemap@example.com", row["email"])
	}
}

func TestCQLContextTimeoutIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)

	helixSessionA := v1.NewSession(sessionA)
	helixSessionB := v1.NewSession(sessionB)

	client, err := helix.NewCQLClient(helixSessionA, helixSessionB)
	require.NoError(t, err)

	// Create a very short timeout context
	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Nanosecond)
	defer cancel()

	// Wait for the context to expire
	time.Sleep(10 * time.Millisecond)

	// Query should fail due to context timeout
	var now time.Time
	err = client.Query("SELECT now() FROM system.local").ScanContext(ctx, &now)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestCQLIteratorMapScanIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, sessionB := getSharedSessions(t)

	usersTable := createTestTableOnBoth(t, "users", usersTableSchema)

	helixSessionA := v1.NewSession(sessionA)
	helixSessionB := v1.NewSession(sessionB)

	client, err := helix.NewCQLClient(helixSessionA, helixSessionB)
	require.NoError(t, err)

	// Insert 3 users
	for i := 0; i < 3; i++ {
		err = client.Query(
			"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
			gocql.TimeUUID(), "IterMapUser", "itermap@example.com", time.Now(),
		).ExecContext(ctx)
		require.NoError(t, err)
	}

	// Use Iterator.MapScan
	iter := client.Query("SELECT name, email FROM " + usersTable).IterContext(ctx)
	require.NotNil(t, iter)

	count := 0
	for {
		row := make(map[string]any)
		if !iter.MapScan(row) {
			break
		}
		require.Equal(t, "IterMapUser", row["name"])
		require.Equal(t, "itermap@example.com", row["email"])
		count++
	}
	require.NoError(t, iter.Close())
	require.Equal(t, 3, count)
}

// TestCQLAdapterScanCASIntegration tests the v1 adapter ScanCAS methods directly.
// CAS operations have special dual-write semantics, so we test adapter-level here.
func TestCQLAdapterScanCASIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, _ := getSharedSessions(t)

	usersTable := createTestTableOnBoth(t, "users", usersTableSchema)

	// Test adapter directly using v1.Session
	adapter := v1.NewSession(sessionA)

	userID := gocql.TimeUUID()

	// Test INSERT IF NOT EXISTS using MapScanCAS for column-order independence
	existingRow := make(map[string]any)

	applied, err := adapter.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
		userID, "CASUser", "cas@example.com", time.Now(),
	).MapScanCASContext(ctx, existingRow)
	require.NoError(t, err)
	require.True(t, applied, "INSERT IF NOT EXISTS should succeed for new row")

	// Try to insert the same row again - should fail (row already exists)
	existingRow = make(map[string]any)
	applied, err = adapter.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
		userID, "CASUser2", "cas2@example.com", time.Now(),
	).MapScanCASContext(ctx, existingRow)
	require.NoError(t, err)
	require.False(t, applied, "INSERT IF NOT EXISTS should fail for existing row")
	require.Equal(t, "CASUser", existingRow["name"], "Should return existing name")
	require.Equal(t, "cas@example.com", existingRow["email"], "Should return existing email")

	// Test non-context version
	existingRow = make(map[string]any)
	applied, err = adapter.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
		gocql.TimeUUID(), "NewCASUser", "newcas@example.com", time.Now(),
	).MapScanCAS(existingRow)
	require.NoError(t, err)
	require.True(t, applied, "INSERT IF NOT EXISTS should succeed for new row")
}

// TestCQLAdapterMapScanCASIntegration tests the v1 adapter MapScanCAS methods directly.
func TestCQLAdapterMapScanCASIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, _ := getSharedSessions(t)

	usersTable := createTestTableOnBoth(t, "users", usersTableSchema)

	adapter := v1.NewSession(sessionA)

	userID := gocql.TimeUUID()

	// Insert initial row
	err := adapter.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
		userID, "MapCASUser", "mapcas@example.com", time.Now(),
	).ExecContext(ctx)
	require.NoError(t, err)

	// Test UPDATE with IF condition - should succeed
	result := make(map[string]any)
	applied, err := adapter.Query(
		"UPDATE "+usersTable+" SET email = ? WHERE id = ? IF name = ?",
		"updated@example.com", userID, "MapCASUser",
	).MapScanCASContext(ctx, result)
	require.NoError(t, err)
	require.True(t, applied, "UPDATE IF should succeed when condition matches")

	// Verify the update
	var email string
	err = sessionA.Query("SELECT email FROM "+usersTable+" WHERE id = ?", userID).Scan(&email)
	require.NoError(t, err)
	require.Equal(t, "updated@example.com", email)

	// Test UPDATE with IF condition - should fail (wrong condition)
	result = make(map[string]any)
	applied, err = adapter.Query(
		"UPDATE "+usersTable+" SET email = ? WHERE id = ? IF name = ?",
		"shouldnotupdate@example.com", userID, "WrongName",
	).MapScanCASContext(ctx, result)
	require.NoError(t, err)
	require.False(t, applied, "UPDATE IF should fail when condition doesn't match")
	require.Equal(t, "MapCASUser", result["name"], "Should return existing name in result")

	// Test non-context version
	result = make(map[string]any)
	applied, err = adapter.Query(
		"UPDATE "+usersTable+" SET email = ? WHERE id = ? IF name = ?",
		"updated2@example.com", userID, "MapCASUser",
	).MapScanCAS(result)
	require.NoError(t, err)
	require.True(t, applied)
}

// TestCQLAdapterBatchCASIntegration tests the v1 adapter Batch CAS methods directly.
func TestCQLAdapterBatchCASIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, _ := getSharedSessions(t)

	usersTable := createTestTableOnBoth(t, "users", usersTableSchema)

	adapter := v1.NewSession(sessionA)

	userID := gocql.TimeUUID()

	// Create an unlogged batch with conditional insert
	// Note: CAS operations require unlogged batches in Cassandra/ScyllaDB
	batch := adapter.Batch(cql.UnloggedBatch)
	batch.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
		userID, "BatchCASUser", "batchcas@example.com", time.Now(),
	)

	// Execute batch with CAS using MapExecCAS for column-order independence
	existingRow := make(map[string]any)
	applied, iter, err := batch.MapExecCASContext(ctx, existingRow)
	require.NoError(t, err)
	require.True(t, applied, "Batch INSERT IF NOT EXISTS should succeed for new row")
	require.NoError(t, iter.Close())

	// Verify data
	var name string
	err = sessionA.Query("SELECT name FROM "+usersTable+" WHERE id = ?", userID).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "BatchCASUser", name)

	// Try again - should fail
	batch2 := adapter.Batch(cql.UnloggedBatch)
	batch2.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
		userID, "BatchCASUser2", "batchcas2@example.com", time.Now(),
	)

	existingRow = make(map[string]any)
	applied, iter, err = batch2.MapExecCASContext(ctx, existingRow)
	require.NoError(t, err)
	require.False(t, applied, "Batch INSERT IF NOT EXISTS should fail for existing row")
	require.NoError(t, iter.Close())

	// Test non-context version with new user
	newUserID := gocql.TimeUUID()
	batch3 := adapter.Batch(cql.UnloggedBatch)
	batch3.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
		newUserID, "BatchCASUser3", "batchcas3@example.com", time.Now(),
	)
	existingRow = make(map[string]any)
	applied, iter, err = batch3.MapExecCAS(existingRow)
	require.NoError(t, err)
	require.True(t, applied)
	require.NoError(t, iter.Close())
}

// TestCQLAdapterMapExecCASIntegration tests the v1 adapter MapExecCAS methods directly.
func TestCQLAdapterMapExecCASIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, _ := getSharedSessions(t)

	usersTable := createTestTableOnBoth(t, "users", usersTableSchema)

	adapter := v1.NewSession(sessionA)

	userID := gocql.TimeUUID()

	// Insert initial row
	err := adapter.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
		userID, "MapExecCASUser", "mapexeccas@example.com", time.Now(),
	).ExecContext(ctx)
	require.NoError(t, err)

	// Create a batch with conditional update
	batch := adapter.Batch(cql.LoggedBatch)
	batch.Query(
		"UPDATE "+usersTable+" SET email = ? WHERE id = ? IF name = ?",
		"mapexec_updated@example.com", userID, "MapExecCASUser",
	)

	// Execute batch with MapExecCAS
	result := make(map[string]any)
	applied, iter, err := batch.MapExecCASContext(ctx, result)
	require.NoError(t, err)
	require.True(t, applied, "Batch UPDATE IF should succeed when condition matches")
	require.NoError(t, iter.Close())

	// Verify the update
	var email string
	err = sessionA.Query("SELECT email FROM "+usersTable+" WHERE id = ?", userID).Scan(&email)
	require.NoError(t, err)
	require.Equal(t, "mapexec_updated@example.com", email)

	// Test non-context version
	batch2 := adapter.Batch(cql.LoggedBatch)
	batch2.Query(
		"UPDATE "+usersTable+" SET email = ? WHERE id = ? IF name = ?",
		"mapexec_updated2@example.com", userID, "MapExecCASUser",
	)
	result = make(map[string]any)
	applied, iter, err = batch2.MapExecCAS(result)
	require.NoError(t, err)
	require.True(t, applied)
	require.NoError(t, iter.Close())
}

// TestCQLAdapterContextMethodsIntegration tests the v1 adapter context methods directly.
func TestCQLAdapterContextMethodsIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, _ := getSharedSessions(t)

	usersTable := createTestTableOnBoth(t, "users", usersTableSchema)

	adapter := v1.NewSession(sessionA)

	userID := gocql.TimeUUID()

	// Test ExecContext
	err := adapter.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
		userID, "ContextUser", "context@example.com", time.Now(),
	).ExecContext(ctx)
	require.NoError(t, err)

	// Test ScanContext
	var name string
	err = adapter.Query(
		"SELECT name FROM "+usersTable+" WHERE id = ?", userID,
	).ScanContext(ctx, &name)
	require.NoError(t, err)
	require.Equal(t, "ContextUser", name)

	// Test IterContext
	iter := adapter.Query("SELECT name, email FROM "+usersTable+" WHERE id = ?", userID).IterContext(ctx)
	require.NotNil(t, iter)

	var iterName, iterEmail string
	require.True(t, iter.Scan(&iterName, &iterEmail))
	require.Equal(t, "ContextUser", iterName)
	require.Equal(t, "context@example.com", iterEmail)
	require.NoError(t, iter.Close())

	// Test MapScanContext
	result := make(map[string]any)
	err = adapter.Query(
		"SELECT name, email FROM "+usersTable+" WHERE id = ?", userID,
	).MapScanContext(ctx, result)
	require.NoError(t, err)
	require.Equal(t, "ContextUser", result["name"])
	require.Equal(t, "context@example.com", result["email"])

	// Test Batch ExecContext
	user2ID := gocql.TimeUUID()
	batch := adapter.Batch(cql.LoggedBatch)
	batch.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
		user2ID, "BatchContextUser", "batchcontext@example.com", time.Now(),
	)
	err = batch.ExecContext(ctx)
	require.NoError(t, err)

	// Verify batch insert
	err = adapter.Query(
		"SELECT name FROM "+usersTable+" WHERE id = ?", user2ID,
	).ScanContext(ctx, &name)
	require.NoError(t, err)
	require.Equal(t, "BatchContextUser", name)

	// Test Batch IterContext
	user3ID := gocql.TimeUUID()
	batch2 := adapter.Batch(cql.LoggedBatch)
	batch2.Query(
		"INSERT INTO "+usersTable+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
		user3ID, "BatchIterUser", "batchiter@example.com", time.Now(),
	)
	batchIter := batch2.IterContext(ctx)
	require.NotNil(t, batchIter)
	require.NoError(t, batchIter.Close())
}
