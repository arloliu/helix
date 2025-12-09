package integration_test

import (
	"net"
	"testing"
	"time"

	gocqlv2 "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/arloliu/helix"
	cqlv2 "github.com/arloliu/helix/adapter/cql/v2"
	"github.com/stretchr/testify/require"
)

func TestCQLV2DualWriteIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, sessionB := getSharedSessionsV2(t)

	// Create unique tables for this test on both clusters
	usersTable := createTestTableOnBoth(t, "users_v2", usersTableSchema)

	// Create Helix CQL client with v2 adapters
	helixSessionA := cqlv2.NewSession(sessionA)
	helixSessionB := cqlv2.NewSession(sessionB)

	client, err := helix.NewCQLClient(helixSessionA, helixSessionB)
	require.NoError(t, err)

	// Generate a test UUID using v2 driver
	userID := gocqlv2.TimeUUID()
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

	// Verify Cluster A
	err = helixSessionA.Query("SELECT name, email, created_at FROM "+usersTable+" WHERE id = ?", userID).
		ScanContext(ctx, &name, &email, &createdAt)
	require.NoError(t, err, "Row should exist in Cluster A")
	require.Equal(t, "Alice", name)
	require.Equal(t, "alice@example.com", email)
	// Time comparison might need tolerance due to precision differences
	require.WithinDuration(t, now, createdAt, time.Millisecond)

	// Verify Cluster B
	err = helixSessionB.Query("SELECT name, email, created_at FROM "+usersTable+" WHERE id = ?", userID).
		ScanContext(ctx, &name, &email, &createdAt)
	require.NoError(t, err, "Row should exist in Cluster B")
	require.Equal(t, "Alice", name)
	require.Equal(t, "alice@example.com", email)
	require.WithinDuration(t, now, createdAt, time.Millisecond)
}

func TestCQLV2MapScanBehavior(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, _ := getSharedSessionsV2(t)
	helixSessionA := cqlv2.NewSession(sessionA)

	// Create table with inet and collection types
	tableName := createTestTableOnBoth(t, "types_v2", `CREATE TABLE IF NOT EXISTS %s (
		id uuid PRIMARY KEY,
		ip_addr inet,
		tags list<text>,
		metadata map<text, text>
	)`)

	id := gocqlv2.TimeUUID()

	// Test 1: Insert with values
	err := helixSessionA.Query("INSERT INTO "+tableName+" (id, ip_addr, tags, metadata) VALUES (?, ?, ?, ?)",
		id, "127.0.0.1", []string{"tag1"}, map[string]string{"key": "val"}).ExecContext(ctx)
	require.NoError(t, err)

	// Verify MapScan types
	m := make(map[string]any)
	err = helixSessionA.Query("SELECT ip_addr, tags, metadata FROM "+tableName+" WHERE id = ?", id).MapScanContext(ctx, m)
	require.NoError(t, err)

	// Check IP type (gocql v2 returns net.IP, v1 returned string)
	// Note: This confirms v2 behavior. If we wanted v1 compatibility, we'd need to convert in the adapter.
	require.IsType(t, net.IP{}, m["ip_addr"], "v2 adapter should return net.IP for inet columns")

	// Test 2: Insert with nulls/empty collections
	id2 := gocqlv2.TimeUUID()
	// Note: In CQL, empty collections are often treated as null, but let's try explicit null insert
	// or just not setting them. Here we insert nulls explicitly.
	err = helixSessionA.Query("INSERT INTO "+tableName+" (id) VALUES (?)", id2).ExecContext(ctx)
	require.NoError(t, err)

	m2 := make(map[string]any)
	err = helixSessionA.Query("SELECT ip_addr, tags, metadata FROM "+tableName+" WHERE id = ?", id2).MapScanContext(ctx, m2)
	require.NoError(t, err)

	// Check nil behavior (gocql v2 returns nil for empty/null collections, v1 returned empty slice/map)
	require.Nil(t, m2["tags"], "v2 adapter should return nil for null list")
	require.Nil(t, m2["metadata"], "v2 adapter should return nil for null map")
}
