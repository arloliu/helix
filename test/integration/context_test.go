package integration_test

import (
	"context"
	"testing"
	"time"

	gocqlv2 "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/adapter/cql"
	cqlv1 "github.com/arloliu/helix/adapter/cql/v1"
	cqlv2 "github.com/arloliu/helix/adapter/cql/v2"
)

func TestBatchContextCancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, _ := getSharedSessions(t)
	helixSession := cqlv1.NewSession(sessionA)

	// Create a table
	table := createTestTableOnBoth(t, "batch_context_test", "CREATE TABLE %s (id uuid PRIMARY KEY, val text)")

	t.Run("ExecContext returns error on canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		batch := helixSession.Batch(cql.LoggedBatch)
		batch.Query("INSERT INTO " + table + " (id, val) VALUES (uuid(), 'test')")

		err := batch.ExecContext(ctx)
		require.Error(t, err)
		// gocql returns context.Canceled when context is canceled
		require.Equal(t, context.Canceled, err)
	})

	t.Run("IterContext handles canceled context gracefully", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		batch := helixSession.Batch(cql.LoggedBatch)
		batch.Query("INSERT INTO " + table + " (id, val) VALUES (uuid(), 'test')")

		// IterContext in v1 adapter returns an iterator and swallows execution errors (returning a nil-safe iterator).
		// We verify that it doesn't panic and returns a non-nil iterator wrapper.
		iter := batch.IterContext(ctx)
		require.NotNil(t, iter)

		// The iterator should be empty/closed effectively
		require.NoError(t, iter.Close())
	})
}

// =============================================================================
// Fix #1 Verification: v1 Batch ExecCASContext / MapExecCASContext
//
// Before fix: b.batch.WithContext(ctx) return value was discarded (gocql v1
// returns a copy), so context was never applied. A canceled context would be
// silently ignored and the operation would succeed. These tests would FAIL on
// the pre-fix code — proving the fix is behaviorally correct.
// =============================================================================

func TestV1BatchExecCASContextCanceled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, _ := getSharedSessions(t)
	helixSession := cqlv1.NewSession(sessionA)

	table := createTestTableOnBoth(t, "v1_execcas_ctx", usersTableSchema)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel before executing

	batch := helixSession.Batch(cql.UnloggedBatch)
	batch.Query(
		"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
		gocql.TimeUUID(), "CASCtxUser", "cas@example.com", time.Now(),
	)

	_, _, err := batch.ExecCASContext(ctx)
	require.Error(t, err, "ExecCASContext with canceled context must return an error")
	require.ErrorIs(t, err, context.Canceled)
}

func TestV1BatchMapExecCASContextCanceled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, _ := getSharedSessions(t)
	helixSession := cqlv1.NewSession(sessionA)

	table := createTestTableOnBoth(t, "v1_mapexeccas_ctx", usersTableSchema)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	batch := helixSession.Batch(cql.UnloggedBatch)
	batch.Query(
		"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
		gocql.TimeUUID(), "MapCASCtxUser", "mapcas@example.com", time.Now(),
	)

	dest := make(map[string]any)
	_, _, err := batch.MapExecCASContext(ctx, dest)
	require.Error(t, err, "MapExecCASContext with canceled context must return an error")
	require.ErrorIs(t, err, context.Canceled)
}

// =============================================================================
// Fix #4 Verification: v2 Query/Batch CAS methods honor stored WithContext ctx
//
// Before fix: ScanCAS, MapScanCAS, ExecCAS, MapExecCAS did not check the ctx
// stored by WithContext, so a canceled context was silently ignored and the
// operation would succeed. These tests would FAIL on pre-fix code.
// =============================================================================

func TestV2QueryScanCASWithCanceledContext(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, _ := getSharedSessionsV2(t)
	helixSession := cqlv2.NewSession(sessionA)

	table := createTestTableOnBoth(t, "v2_scancas_ctx", usersTableSchema)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := helixSession.Query(
		"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
		gocqlv2.TimeUUID(), "V2CASCtx", "v2cas@example.com", time.Now(),
	).ScanCASContext(ctx)
	require.Error(t, err, "ScanCASContext with canceled context must return an error")
	require.ErrorIs(t, err, context.Canceled)
}

func TestV2QueryMapScanCASWithCanceledContext(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, _ := getSharedSessionsV2(t)
	helixSession := cqlv2.NewSession(sessionA)

	table := createTestTableOnBoth(t, "v2_mapscancas_ctx", usersTableSchema)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	dest := make(map[string]any)
	_, err := helixSession.Query(
		"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
		gocqlv2.TimeUUID(), "V2MapCASCtx", "v2mapcas@example.com", time.Now(),
	).MapScanCASContext(ctx, dest)
	require.Error(t, err, "MapScanCASContext with canceled context must return an error")
	require.ErrorIs(t, err, context.Canceled)
}

func TestV2BatchExecCASWithCanceledContext(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, _ := getSharedSessionsV2(t)
	helixSession := cqlv2.NewSession(sessionA)

	table := createTestTableOnBoth(t, "v2_batchexeccas_ctx", usersTableSchema)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	batch := helixSession.Batch(cql.UnloggedBatch)
	batch.Query(
		"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
		gocqlv2.TimeUUID(), "V2BatchCASCtx", "v2batchcas@example.com", time.Now(),
	)

	_, iter, err := batch.ExecCASContext(ctx)
	require.Error(t, err, "ExecCASContext with canceled context must return an error")
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, iter.Close())
}

func TestV2BatchMapExecCASWithCanceledContext(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, _ := getSharedSessionsV2(t)
	helixSession := cqlv2.NewSession(sessionA)

	table := createTestTableOnBoth(t, "v2_batchmapexeccas_ctx", usersTableSchema)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	batch := helixSession.Batch(cql.UnloggedBatch)
	batch.Query(
		"INSERT INTO "+table+" (id, name, email, created_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
		gocqlv2.TimeUUID(), "V2BatchMapCASCtx", "v2batchmapcas@example.com", time.Now(),
	)

	dest := make(map[string]any)
	_, iter, err := batch.MapExecCASContext(ctx, dest)
	require.Error(t, err, "MapExecCASContext with canceled context must return an error")
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, iter.Close())
}
