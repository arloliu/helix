package integration_test

import (
	"net"
	"testing"
	"time"

	gocqlv2 "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	cqlv2 "github.com/arloliu/helix/adapter/cql/v2"
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

// TestCQLV2AdapterContextMethodsIntegration mirrors TestCQLAdapterContextMethodsIntegration
// from the v1 file, covering all context-aware query and batch methods on the v2 adapter.
func TestCQLV2AdapterContextMethodsIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, _ := getSharedSessionsV2(t)
	adapter := cqlv2.NewSession(sessionA)

	table := createTestTableOnBoth(t, "v2_ctx_methods", usersTableSchema)

	userID := gocqlv2.TimeUUID()

	// ExecContext
	err := adapter.Query(
		"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
		userID, "V2CtxUser", "v2ctx@example.com", time.Now(),
	).ExecContext(ctx)
	require.NoError(t, err)

	// ScanContext
	var name string
	err = adapter.Query(
		"SELECT name FROM "+table+" WHERE id = ?", userID,
	).ScanContext(ctx, &name)
	require.NoError(t, err)
	require.Equal(t, "V2CtxUser", name)

	// IterContext
	iter := adapter.Query("SELECT name, email FROM "+table+" WHERE id = ?", userID).IterContext(ctx)
	require.NotNil(t, iter)
	var iterName, iterEmail string
	require.True(t, iter.Scan(&iterName, &iterEmail))
	require.Equal(t, "V2CtxUser", iterName)
	require.Equal(t, "v2ctx@example.com", iterEmail)
	require.NoError(t, iter.Close())

	// MapScanContext
	result := make(map[string]any)
	err = adapter.Query(
		"SELECT name, email FROM "+table+" WHERE id = ?", userID,
	).MapScanContext(ctx, result)
	require.NoError(t, err)
	require.Equal(t, "V2CtxUser", result["name"])
	require.Equal(t, "v2ctx@example.com", result["email"])

	// Batch ExecContext
	user2ID := gocqlv2.TimeUUID()
	batch := adapter.Batch(helix.LoggedBatch)
	batch.Query(
		"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
		user2ID, "V2BatchCtxUser", "v2batchctx@example.com", time.Now(),
	)
	err = batch.ExecContext(ctx)
	require.NoError(t, err)

	err = adapter.Query(
		"SELECT name FROM "+table+" WHERE id = ?", user2ID,
	).ScanContext(ctx, &name)
	require.NoError(t, err)
	require.Equal(t, "V2BatchCtxUser", name)

	// Batch IterContext — v2 returns a real iterator (unlike v1 which always returns empty)
	user3ID := gocqlv2.TimeUUID()
	batch2 := adapter.Batch(helix.LoggedBatch)
	batch2.Query(
		"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
		user3ID, "V2BatchIterUser", "v2batchiter@example.com", time.Now(),
	)
	batchIter := batch2.IterContext(ctx)
	require.NotNil(t, batchIter)
	require.NoError(t, batchIter.Close())
}

// TestCQLV2AdapterScanCASIntegration tests all v2 adapter LWT query methods:
// ScanCAS, ScanCASContext, MapScanCAS, MapScanCASContext.
func TestCQLV2AdapterScanCASIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, _ := getSharedSessionsV2(t)
	adapter := cqlv2.NewSession(sessionA)

	table := createTestTableOnBoth(t, "v2_scancas", usersTableSchema)

	t.Run("ScanCAS applied and not-applied", func(t *testing.T) {
		userID := gocqlv2.TimeUUID()

		// Dest vars for LWT scan results. ScyllaDB returns all columns even when applied=true,
		// so dest vars must always be provided to avoid "not enough columns to scan" errors.
		// Column order (Cassandra/ScyllaDB LWT): id, created_at, email, name (PK first, non-key alphabetical).
		var existingID gocqlv2.UUID
		var existingCreatedAt time.Time
		var existingEmail, existingName string

		// applied=true: first insert.
		applied, err := adapter.Query(
			"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
			userID, "V2ScanCAS", "v2scancas@example.com", time.Now(),
		).ScanCAS(&existingID, &existingCreatedAt, &existingEmail, &existingName)
		require.NoError(t, err)
		require.True(t, applied, "first INSERT IF NOT EXISTS should be applied")

		// applied=false: result has [applied] + 4 table columns.
		applied, err = adapter.Query(
			"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
			userID, "V2ScanCAS2", "v2scancas2@example.com", time.Now(),
		).ScanCAS(&existingID, &existingCreatedAt, &existingEmail, &existingName)
		require.NoError(t, err)
		require.False(t, applied, "duplicate INSERT IF NOT EXISTS should not be applied")
		require.Equal(t, "V2ScanCAS", existingName, "existing row name returned on conflict")
	})

	t.Run("ScanCASContext applied and not-applied", func(t *testing.T) {
		userID := gocqlv2.TimeUUID()

		// Dest vars for LWT scan results. ScyllaDB returns all columns even when applied=true,
		// so dest vars must always be provided to avoid "not enough columns to scan" errors.
		var existingID gocqlv2.UUID
		var existingCreatedAt time.Time
		var existingEmail, existingName string

		// applied=true: first insert.
		applied, err := adapter.Query(
			"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
			userID, "V2ScanCASCtx", "v2scancasctx@example.com", time.Now(),
		).ScanCASContext(ctx, &existingID, &existingCreatedAt, &existingEmail, &existingName)
		require.NoError(t, err)
		require.True(t, applied)

		// applied=false: dest vars required for the 4 table columns (id, created_at, email, name).
		applied, err = adapter.Query(
			"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
			userID, "V2ScanCASCtx2", "v2scancasctx2@example.com", time.Now(),
		).ScanCASContext(ctx, &existingID, &existingCreatedAt, &existingEmail, &existingName)
		require.NoError(t, err)
		require.False(t, applied)
		require.Equal(t, "V2ScanCASCtx", existingName, "existing row name returned on conflict")
	})

	t.Run("MapScanCAS applied and not-applied", func(t *testing.T) {
		userID := gocqlv2.TimeUUID()

		dest := make(map[string]any)
		applied, err := adapter.Query(
			"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
			userID, "V2MapScanCAS", "v2mapscancas@example.com", time.Now(),
		).MapScanCAS(dest)
		require.NoError(t, err)
		require.True(t, applied)

		dest = make(map[string]any)
		applied, err = adapter.Query(
			"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
			userID, "V2MapScanCAS2", "v2mapscancas2@example.com", time.Now(),
		).MapScanCAS(dest)
		require.NoError(t, err)
		require.False(t, applied)
		require.Equal(t, "V2MapScanCAS", dest["name"], "existing row name returned on conflict")
	})

	t.Run("MapScanCASContext with UPDATE IF condition", func(t *testing.T) {
		userID := gocqlv2.TimeUUID()

		// Insert initial row
		err := adapter.Query(
			"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
			userID, "V2MapScanCASCtxUser", "v2mapscancasctx@example.com", time.Now(),
		).ExecContext(ctx)
		require.NoError(t, err)

		// UPDATE IF matches: applied
		result := make(map[string]any)
		applied, err := adapter.Query(
			"UPDATE "+table+" SET email = ? WHERE id = ? IF name = ?",
			"updated@example.com", userID, "V2MapScanCASCtxUser",
		).MapScanCASContext(ctx, result)
		require.NoError(t, err)
		require.True(t, applied, "UPDATE IF should succeed when condition matches")

		// UPDATE IF does not match: not applied, returns existing row
		result = make(map[string]any)
		applied, err = adapter.Query(
			"UPDATE "+table+" SET email = ? WHERE id = ? IF name = ?",
			"wrong@example.com", userID, "WrongName",
		).MapScanCASContext(ctx, result)
		require.NoError(t, err)
		require.False(t, applied, "UPDATE IF should fail when condition does not match")
		require.Equal(t, "V2MapScanCASCtxUser", result["name"])
	})
}

// TestCQLV2AdapterBatchCASIntegration tests all v2 adapter LWT batch methods:
// ExecCAS, ExecCASContext, MapExecCAS, MapExecCASContext.
func TestCQLV2AdapterBatchCASIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	sessionA, _ := getSharedSessionsV2(t)
	adapter := cqlv2.NewSession(sessionA)

	table := createTestTableOnBoth(t, "v2_batchcas", usersTableSchema)

	t.Run("ExecCAS applied and not-applied", func(t *testing.T) {
		userID := gocqlv2.TimeUUID()

		// Dest vars for LWT scan results. ScyllaDB returns all columns even when applied=true,
		// so dest vars must always be provided to avoid "not enough columns to scan" errors.
		// Column order (Cassandra/ScyllaDB LWT): id, created_at, email, name (PK first, non-key alphabetical).
		var existingID gocqlv2.UUID
		var existingCreatedAt time.Time
		var existingEmail, existingName string

		// applied=true: first insert.
		batch := adapter.Batch(helix.UnloggedBatch)
		batch.Query(
			"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
			userID, "V2ExecCAS", "v2execcas@example.com", time.Now(),
		)
		applied, iter, err := batch.ExecCAS(&existingID, &existingCreatedAt, &existingEmail, &existingName)
		require.NoError(t, err)
		require.True(t, applied)
		require.NoError(t, iter.Close())

		// applied=false: result has [applied] + 4 table columns.
		batch2 := adapter.Batch(helix.UnloggedBatch)
		batch2.Query(
			"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
			userID, "V2ExecCAS2", "v2execcas2@example.com", time.Now(),
		)
		applied, iter, err = batch2.ExecCAS(&existingID, &existingCreatedAt, &existingEmail, &existingName)
		require.NoError(t, err)
		require.False(t, applied)
		require.Equal(t, "V2ExecCAS", existingName, "existing row name returned on conflict")
		require.NoError(t, iter.Close())
	})

	t.Run("ExecCASContext applied and not-applied", func(t *testing.T) {
		userID := gocqlv2.TimeUUID()

		// Dest vars for LWT scan results. ScyllaDB returns all columns even when applied=true,
		// so dest vars must always be provided to avoid "not enough columns to scan" errors.
		var existingID gocqlv2.UUID
		var existingCreatedAt time.Time
		var existingEmail, existingName string

		// applied=true: first insert.
		batch := adapter.Batch(helix.UnloggedBatch)
		batch.Query(
			"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
			userID, "V2ExecCASCtx", "v2execcasctx@example.com", time.Now(),
		)
		applied, iter, err := batch.ExecCASContext(ctx, &existingID, &existingCreatedAt, &existingEmail, &existingName)
		require.NoError(t, err)
		require.True(t, applied)
		require.NoError(t, iter.Close())

		// applied=false: dest vars required for the 4 table columns (id, created_at, email, name).
		batch2 := adapter.Batch(helix.UnloggedBatch)
		batch2.Query(
			"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
			userID, "V2ExecCASCtx2", "v2execcasctx2@example.com", time.Now(),
		)
		applied, iter, err = batch2.ExecCASContext(ctx, &existingID, &existingCreatedAt, &existingEmail, &existingName)
		require.NoError(t, err)
		require.False(t, applied)
		require.Equal(t, "V2ExecCASCtx", existingName, "existing row name returned on conflict")
		require.NoError(t, iter.Close())
	})

	t.Run("MapExecCAS applied and not-applied", func(t *testing.T) {
		userID := gocqlv2.TimeUUID()

		dest := make(map[string]any)
		batch := adapter.Batch(helix.UnloggedBatch)
		batch.Query(
			"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
			userID, "V2MapExecCAS", "v2mapexeccas@example.com", time.Now(),
		)
		applied, iter, err := batch.MapExecCAS(dest)
		require.NoError(t, err)
		require.True(t, applied)
		require.NoError(t, iter.Close())

		dest = make(map[string]any)
		batch2 := adapter.Batch(helix.UnloggedBatch)
		batch2.Query(
			"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
			userID, "V2MapExecCAS2", "v2mapexeccas2@example.com", time.Now(),
		)
		applied, iter, err = batch2.MapExecCAS(dest)
		require.NoError(t, err)
		require.False(t, applied)
		require.NoError(t, iter.Close())
	})

	t.Run("MapExecCASContext with UPDATE IF condition", func(t *testing.T) {
		userID := gocqlv2.TimeUUID()

		err := adapter.Query(
			"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?)",
			userID, "V2MapExecCASCtxUser", "v2mapexeccasctx@example.com", time.Now(),
		).ExecContext(ctx)
		require.NoError(t, err)

		// UPDATE IF matches
		result := make(map[string]any)
		batch := adapter.Batch(helix.LoggedBatch)
		batch.Query(
			"UPDATE "+table+" SET email = ? WHERE id = ? IF name = ?",
			"updated@example.com", userID, "V2MapExecCASCtxUser",
		)
		applied, iter, err := batch.MapExecCASContext(ctx, result)
		require.NoError(t, err)
		require.True(t, applied)
		require.NoError(t, iter.Close())

		// UPDATE IF does not match
		result = make(map[string]any)
		batch2 := adapter.Batch(helix.LoggedBatch)
		batch2.Query(
			"UPDATE "+table+" SET email = ? WHERE id = ? IF name = ?",
			"wrong@example.com", userID, "WrongName",
		)
		applied, iter, err = batch2.MapExecCASContext(ctx, result)
		require.NoError(t, err)
		require.False(t, applied)
		require.Equal(t, "V2MapExecCASCtxUser", result["name"])
		require.NoError(t, iter.Close())
	})
}
