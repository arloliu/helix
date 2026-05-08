package integration_test

// End-to-end integration tests for the v1.4.0 async mirror feature.
//
// These tests exercise the mirror engine against real Cassandra/ScyllaDB
// clusters and (for publisher mode) an embedded NATS JetStream server.
// They complement the unit-level mirror tests in mirror_dispatch_test.go
// and mirror/engine_test.go which cover behaviour without infrastructure
// dependencies.
//
// The shared-cluster harness gives us only two CQL clusters
// (sharedClusters.clusterA / clusterB). The mirror feature conceptually
// involves four — two on the primary (current) pair plus two on the
// mirror (new) pair. To stay within the harness budget the tests below
// configure the primary client in single-cluster mode against clusterA
// and the mirror client in single-cluster mode against clusterB. The
// hot path semantics (Mirror() opt-in, capture, async dispatch, timestamp
// preservation, replay durability) are unaffected by single- vs dual-
// cluster.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	cqlv1 "github.com/arloliu/helix/adapter/cql/v1"
	"github.com/arloliu/helix/mirror"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/testutil"
)

const mirrorTableSchema = `
	CREATE TABLE IF NOT EXISTS %s (
		id TEXT PRIMARY KEY,
		v  TEXT
	)
`

// createMirrorTable creates an identical table on both shared clusters
// (the primary side and the mirror side use the same table name on
// different clusters).
func createMirrorTable(t *testing.T, suffix string) (table string) {
	t.Helper()

	sessionA, sessionB := getSharedSessions(t)

	table = fmt.Sprintf("mirror_%s_%d", suffix, time.Now().UnixNano())

	if err := sessionA.Query(fmt.Sprintf(mirrorTableSchema, table)).Exec(); err != nil {
		t.Fatalf("create %s on cluster A: %v", table, err)
	}
	if err := sessionB.Query(fmt.Sprintf(mirrorTableSchema, table)).Exec(); err != nil {
		t.Fatalf("create %s on cluster B: %v", table, err)
	}

	t.Cleanup(func() {
		_ = sessionA.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s", table)).Exec()
		_ = sessionB.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s", table)).Exec()
	})

	return table
}

// TestMirrorTargetModeEndToEnd verifies that a write opted into Mirror()
// against the primary lands at the mirror destination via helix.WithMirror,
// and that the mirror cluster observes the row. Non-mirrored writes do
// NOT land on the mirror destination.
func TestMirrorTargetModeEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	table := createMirrorTable(t, "tgt")
	sessionA, sessionB := getSharedSessions(t)

	// Do not defer Close — wrapped sessions are shared and cleaned up by
	// TestMain teardown.
	mirrorClient, err := helix.NewCQLClient(cqlv1.NewSession(sessionB), nil)
	require.NoError(t, err)

	primary, err := helix.NewCQLClient(cqlv1.NewSession(sessionA), nil,
		helix.WithMirror(mirrorClient,
			mirror.WithQueueSize(64),
			mirror.WithWorkers(1),
		),
	)
	require.NoError(t, err)

	stmt := fmt.Sprintf("INSERT INTO %s (id, v) VALUES (?, ?)", table)
	ctx := t.Context()

	// k1: opted into Mirror() — should land on mirror destination.
	require.NoError(t, primary.Session().Query(stmt, "k1", "v1").Mirror().ExecContext(ctx))

	// k2: NOT opted into Mirror() — must not land on mirror destination.
	require.NoError(t, primary.Session().Query(stmt, "k2", "v2").ExecContext(ctx))

	requireEventually(t, 3*time.Second, func() bool {
		var v string
		err := sessionB.Query(fmt.Sprintf("SELECT v FROM %s WHERE id = ?", table), "k1").Scan(&v)
		return err == nil && v == "v1"
	}, "mirror destination never observed k1")

	var v string
	err = sessionB.Query(fmt.Sprintf("SELECT v FROM %s WHERE id = ?", table), "k2").Scan(&v)
	require.ErrorIs(t, err, gocql.ErrNotFound, "non-mirrored write must not land on mirror")

	require.GreaterOrEqual(t, primary.Mirror().Stats().Success, uint64(1))
}

// TestMirrorReplayerDurableRetry verifies that when a mirror exec fails
// initially, helix.WithMirrorReplayer pushes the failure into the replayer
// and the auto-built worker eventually delivers the write once the
// destination recovers.
//
// We simulate "destination unreachable" by NOT pre-creating the mirror
// table; the first mirror exec fails ("table does not exist"), the
// replayer worker retries, and once the table is created the row lands.
func TestMirrorReplayerDurableRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	table := fmt.Sprintf("mirror_replay_%d", time.Now().UnixNano())

	// Only create the table on cluster A initially; cluster B (mirror
	// destination) gets it later.
	require.NoError(t, sessionA.Query(fmt.Sprintf(mirrorTableSchema, table)).Exec())
	t.Cleanup(func() {
		_ = sessionA.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s", table)).Exec()
		_ = sessionB.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s", table)).Exec()
	})

	mirrorClient, err := helix.NewCQLClient(cqlv1.NewSession(sessionB), nil)
	require.NoError(t, err)

	memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(64))

	primary, err := helix.NewCQLClient(cqlv1.NewSession(sessionA), nil,
		helix.WithMirror(mirrorClient,
			mirror.WithWorkers(1),
			mirror.WithQueueSize(8),
		),
		helix.WithMirrorReplayer(memReplayer,
			replay.WithPollInterval(20*time.Millisecond),
			replay.WithRetryDelay(20*time.Millisecond),
			replay.WithMaxAttempts(200),
		),
	)
	require.NoError(t, err)

	stmt := fmt.Sprintf("INSERT INTO %s (id, v) VALUES (?, ?)", table)
	require.NoError(t, primary.Session().Query(stmt, "k1", "v1").Mirror().ExecContext(t.Context()))

	// Create the table on the mirror destination after a short delay to
	// simulate the mirror coming online late.
	go func() {
		time.Sleep(150 * time.Millisecond)
		_ = sessionB.Query(fmt.Sprintf(mirrorTableSchema, table)).Exec()
	}()

	requireEventually(t, 8*time.Second, func() bool {
		var v string
		err := sessionB.Query(fmt.Sprintf("SELECT v FROM %s WHERE id = ?", table), "k1").Scan(&v)
		return err == nil && v == "v1"
	}, "mirror destination never observed k1 after retry")
}

// TestMirrorPublisherModeEndToEnd verifies the publisher / consumer split:
// the app publishes to a NATS-backed Replayer; a separate NewMirrorWorker
// drains the same NATS stream and writes to the mirror destination.
func TestMirrorPublisherModeEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	table := createMirrorTable(t, "pub")
	sessionA, sessionB := getSharedSessions(t)

	js := testutil.StartEmbeddedNATS(t)

	natsReplayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-mirror-pub"),
		replay.WithSubjectPrefix("test.mirror.pub"),
	)
	require.NoError(t, err)
	defer natsReplayer.Close()

	app, err := helix.NewCQLClient(cqlv1.NewSession(sessionA), nil,
		helix.WithMirrorPublisher(natsReplayer,
			mirror.WithWorkers(1),
			mirror.WithQueueSize(8),
		),
	)
	require.NoError(t, err)

	mirrorClient, err := helix.NewCQLClient(cqlv1.NewSession(sessionB), nil)
	require.NoError(t, err)

	worker, err := helix.NewMirrorWorker(natsReplayer, mirrorClient,
		replay.WithPollInterval(20*time.Millisecond),
	)
	require.NoError(t, err)
	require.NoError(t, worker.Start())
	defer worker.Stop()

	stmt := fmt.Sprintf("INSERT INTO %s (id, v) VALUES (?, ?)", table)
	require.NoError(t, app.Session().Query(stmt, "k1", "v1").Mirror().ExecContext(t.Context()))

	requireEventually(t, 8*time.Second, func() bool {
		var v string
		err := sessionB.Query(fmt.Sprintf("SELECT v FROM %s WHERE id = ?", table), "k1").Scan(&v)
		return err == nil && v == "v1"
	}, "consumer worker never delivered k1 to mirror destination")
}

// TestMirrorTimestampPreservation verifies that a mirrored write uses the
// same client-generated timestamp on the mirror destination as on the
// primary, so server-side WRITETIME is consistent across the two clusters.
func TestMirrorTimestampPreservation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	table := createMirrorTable(t, "ts")
	sessionA, sessionB := getSharedSessions(t)

	mirrorClient, err := helix.NewCQLClient(cqlv1.NewSession(sessionB), nil)
	require.NoError(t, err)

	primary, err := helix.NewCQLClient(cqlv1.NewSession(sessionA), nil,
		helix.WithMirror(mirrorClient, mirror.WithWorkers(1)),
	)
	require.NoError(t, err)

	stmt := fmt.Sprintf("INSERT INTO %s (id, v) VALUES (?, ?)", table)
	require.NoError(t, primary.Session().Query(stmt, "k1", "v1").Mirror().ExecContext(t.Context()))

	requireEventually(t, 5*time.Second, func() bool {
		var v string
		err := sessionB.Query(fmt.Sprintf("SELECT v FROM %s WHERE id = ?", table), "k1").Scan(&v)
		return err == nil && v == "v1"
	}, "mirror never observed k1")

	var primaryWriteTime, mirrorWriteTime int64
	require.NoError(t, sessionA.Query(
		fmt.Sprintf("SELECT WRITETIME(v) FROM %s WHERE id = ?", table), "k1").
		Scan(&primaryWriteTime))
	require.NoError(t, sessionB.Query(
		fmt.Sprintf("SELECT WRITETIME(v) FROM %s WHERE id = ?", table), "k1").
		Scan(&mirrorWriteTime))

	require.Equal(t, primaryWriteTime, mirrorWriteTime,
		"mirror must preserve client-generated timestamp; primary=%d mirror=%d",
		primaryWriteTime, mirrorWriteTime)
}

// requireEventually polls cond until it returns true or timeout. Fails
// the test if the condition is never satisfied.
func requireEventually(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	for {
		if cond() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(msg)
		case <-time.After(20 * time.Millisecond):
		}
	}
}
