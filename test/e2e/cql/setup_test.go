//go:build e2e

package cql_test

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	cqlv1 "github.com/arloliu/helix/adapter/cql/v1"
	cqlv2 "github.com/arloliu/helix/adapter/cql/v2"
	"github.com/arloliu/helix/test/testutil"
)

// e2eClusters holds the cluster pair used by all tests in this package.
// It is dedicated to e2e/cql; do NOT share with test/integration.
var e2eClusters struct {
	a *testutil.CQLCluster
	b *testutil.CQLCluster
}

// e2eOptions defines the cluster configuration used for every test in this
// package. Short timeouts surface failure modes quickly and make v1/v2
// parity assertions meaningful — divergent default timeouts would otherwise
// produce spurious wall-clock differences.
func e2eOptions(keyspace string) testutil.CQLClusterOptions {
	return testutil.CQLClusterOptions{
		Keyspace:          keyspace,
		PreferScyllaDB:    true,
		ScyllaDBImage:     "scylladb/scylla:6.2",
		CassandraImage:    "cassandra:4.1",
		ScyllaDBMemory:    "512M",
		ScyllaDBSMP:       1,
		SessionTimeout:    2 * time.Second,
		ConnectTimeout:    2 * time.Second,
		ReconnectInterval: 500 * time.Millisecond,
	}
}

func TestMain(m *testing.M) {
	flag.Parse()

	if testing.Short() {
		return
	}
	if os.Getenv("SKIP_INTEGRATION_TESTS") == "1" {
		fmt.Println("Skipping e2e/cql tests (SKIP_INTEGRATION_TESTS=1)")
		return
	}

	ctx := context.Background()
	if err := setupE2EClusters(ctx); err != nil {
		fmt.Printf("Failed to setup e2e clusters: %v\n", err)
		return
	}
	defer teardownE2EClusters(ctx)

	m.Run()
}

func setupE2EClusters(ctx context.Context) error {
	fmt.Println("Starting e2e/cql cluster pair (short timeouts)…")

	a, err := testutil.StartCQLCluster(ctx, e2eOptions("helix_e2e_a"))
	if err != nil {
		return fmt.Errorf("start cluster A: %w", err)
	}
	b, err := testutil.StartCQLCluster(ctx, e2eOptions("helix_e2e_b"))
	if err != nil {
		_ = a.Terminate(ctx)

		return fmt.Errorf("start cluster B: %w", err)
	}
	e2eClusters.a = a
	e2eClusters.b = b
	fmt.Printf("e2e/cql clusters ready (type=%s)\n", a.Type)

	return nil
}

func teardownE2EClusters(ctx context.Context) {
	fmt.Println("Tearing down e2e/cql clusters…")
	if e2eClusters.a != nil {
		_ = e2eClusters.a.Terminate(ctx)
	}
	if e2eClusters.b != nil {
		_ = e2eClusters.b.Terminate(ctx)
	}
}

// sharedClusters returns the package-scoped cluster pair, skipping the test
// if either is unavailable (e.g., Docker missing or short mode).
func sharedClusters(t *testing.T) (a, b *testutil.CQLCluster) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}
	if e2eClusters.a == nil || e2eClusters.b == nil {
		t.Skip("e2e clusters not available (Docker required)")
	}

	return e2eClusters.a, e2eClusters.b
}

// driverCase parameterizes a test over the cql adapter version. Tests use
// this with t.Run to produce v1 and v2 sub-tests sharing the same body.
type driverCase struct {
	name string
	wrap func(c *testutil.CQLCluster) cql.Session
}

// allDrivers is the canonical table for any e2e scenario that should be
// asserted on both adapters. New scenarios should iterate this list unless
// they specifically target one driver.
//
// The wrap functions return a noCloseSession around the adapter, so that
// helix.CQLClient.Close() (which is registered as t.Cleanup) does NOT
// close the underlying gocql session shared with the next test. Same
// pattern as test/simulation/chaos/session.go.
var allDrivers = []driverCase{
	{name: "v1", wrap: func(c *testutil.CQLCluster) cql.Session {
		return &noCloseSession{Session: cqlv1.NewSession(c.Session)}
	}},
	{name: "v2", wrap: func(c *testutil.CQLCluster) cql.Session {
		return &noCloseSession{Session: cqlv2.NewSession(c.SessionV2)}
	}},
}

// noCloseSession wraps a cql.Session and converts Close() into a no-op.
// The underlying session lifetime is managed by testutil.CQLCluster;
// Close on the wrapper would prematurely terminate the connection that
// later tests still need.
type noCloseSession struct {
	cql.Session
}

func (s *noCloseSession) Close() {}

// withRestoredCluster registers a restore Cleanup BEFORE returning, so a
// caller-side panic between this call and the destructive Stop/Pause does
// not leave the cluster dead for the next test.
//
// Restoration steps (best-effort; errors logged but not failing):
//  1. Unpause (no-op if not paused).
//  2. Start  (no-op if not stopped).
//  3. Reconnect — rebuild sessions in case the live ones are unrecoverable.
func withRestoredCluster(t *testing.T, cluster *testutil.CQLCluster) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		// Unpause first — Start on a paused container is a no-op error.
		_ = cluster.Unpause(ctx)
		if err := cluster.Start(ctx); err != nil {
			t.Logf("restore: Start failed: %v", err)
		}
		if err := cluster.Reconnect(ctx); err != nil {
			t.Logf("restore: Reconnect failed: %v", err)
		}
	})
}

// table-creation helpers ----------------------------------------------------

// uniqueTableName produces a per-test unique table name to avoid collisions
// across sequential tests sharing the cluster pair.
var (
	tableSeq   uint64
	tableSeqMu sync.Mutex
)

func uniqueTableName(prefix string) string {
	tableSeqMu.Lock()
	defer tableSeqMu.Unlock()
	tableSeq++
	return fmt.Sprintf("e2e_%s_%d", prefix, tableSeq)
}

// createKVTableOnBoth creates the standard (key TEXT PRIMARY KEY, value TEXT)
// schema on both clusters with a per-test unique name and registers a
// TRUNCATE cleanup (not DROP — DROP is slow on Cassandra).
func createKVTableOnBoth(t *testing.T, prefix string) string {
	t.Helper()
	a, b := sharedClusters(t)

	tableName := uniqueTableName(prefix)
	stmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	key TEXT PRIMARY KEY,
	value TEXT
)`, tableName)

	if err := a.Session.Query(stmt).Exec(); err != nil {
		t.Fatalf("create table on A: %v", err)
	}
	if err := b.Session.Query(stmt).Exec(); err != nil {
		t.Fatalf("create table on B: %v", err)
	}
	t.Cleanup(func() {
		_ = a.Session.Query("TRUNCATE " + tableName).Exec()
		_ = b.Session.Query("TRUNCATE " + tableName).Exec()
	})

	return tableName
}
