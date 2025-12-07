package integration_test

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gocql/gocql"

	"github.com/arloliu/helix/test/testutil"
)

// sharedClusters holds the shared CQL clusters for all integration tests.
var sharedClusters struct {
	clusterA *testutil.CQLCluster
	clusterB *testutil.CQLCluster
}

// TestMain sets up shared test infrastructure for all CQL integration tests.
// This avoids the overhead of starting containers for each individual test.
// Prefers ScyllaDB for faster startup, falls back to Cassandra if AIO is unavailable.
func TestMain(m *testing.M) {
	flag.Parse()

	if testing.Short() {
		return
	}

	// Check if we should skip container setup (for unit tests or CI without Docker)
	if os.Getenv("SKIP_INTEGRATION_TESTS") == "1" {
		fmt.Println("Skipping integration tests (SKIP_INTEGRATION_TESTS=1)")

		return
	}

	ctx := context.Background()
	// Start clusters using testutil (prefers ScyllaDB, falls back to Cassandra)
	if err := setupSharedClusters(ctx); err != nil {
		fmt.Printf("Failed to setup shared clusters: %v\n", err)

		return
	}

	// Run tests
	_ = m.Run()

	// Cleanup clusters
	teardownSharedClusters(ctx)
}

func setupSharedClusters(ctx context.Context) error {
	fmt.Println("Starting shared CQL clusters for integration tests...")

	clusterA, clusterB, err := testutil.StartTwoCQLClusters(ctx)
	if err != nil {
		return err
	}

	sharedClusters.clusterA = clusterA
	sharedClusters.clusterB = clusterB

	fmt.Printf("Shared clusters ready! (using %s)\n", clusterA.Type)

	return nil
}

func teardownSharedClusters(ctx context.Context) {
	fmt.Println("Cleaning up shared CQL clusters...")

	if sharedClusters.clusterA != nil {
		_ = sharedClusters.clusterA.Terminate(ctx)
	}
	if sharedClusters.clusterB != nil {
		_ = sharedClusters.clusterB.Terminate(ctx)
	}

	fmt.Println("Cleanup complete!")
}

// getSharedSessions returns the shared gocql sessions for tests.
// Each test should create its own tables using unique names to avoid conflicts.
// Note: Do not call session.Close() in tests - these sessions are shared
// across all tests and will be closed by TestMain's teardown.
func getSharedSessions(t *testing.T) (sessionA *gocql.Session, sessionB *gocql.Session) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	if sharedClusters.clusterA == nil || sharedClusters.clusterB == nil {
		t.Skip("shared clusters not available (run with -short=false and Docker)")
	}

	return sharedClusters.clusterA.Session, sharedClusters.clusterB.Session
}

// createTestTableOnBoth creates a table with a unique name on both clusters.
// This ensures both clusters have the same table for dual-write testing.
func createTestTableOnBoth(t *testing.T, tableNameSuffix, schema string) string {
	t.Helper()

	sessionA, sessionB := getSharedSessions(t)

	// Create unique table name based on test name
	tableName := fmt.Sprintf("test_%s_%d", tableNameSuffix, time.Now().UnixNano())

	// Replace placeholder in schema
	query := fmt.Sprintf(schema, tableName)

	// Create table on cluster A
	if err := sessionA.Query(query).Exec(); err != nil {
		t.Fatalf("failed to create table %s on cluster A: %v", tableName, err)
	}

	// Create table on cluster B
	if err := sessionB.Query(query).Exec(); err != nil {
		t.Fatalf("failed to create table %s on cluster B: %v", tableName, err)
	}

	// Register cleanup for both clusters
	t.Cleanup(func() {
		_ = sessionA.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)).Exec()
		_ = sessionB.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)).Exec()
	})

	return tableName
}

// Table schema templates with %s placeholder for table name.
const (
	usersTableSchema = `
		CREATE TABLE IF NOT EXISTS %s (
			id UUID PRIMARY KEY,
			name TEXT,
			email TEXT,
			created_at TIMESTAMP
		)
	`
	productsTableSchema = `
		CREATE TABLE IF NOT EXISTS %s (
			id UUID PRIMARY KEY,
			name TEXT,
			price DOUBLE,
			stock INT
		)
	`
)
