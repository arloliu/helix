package integration_test

import (
	"testing"
	"time"

	"github.com/arloliu/helix"
	cqlv2 "github.com/arloliu/helix/adapter/cql/v2"
	"github.com/gocql/gocql"
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
