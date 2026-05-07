//go:build spike

// Throwaway spike that validates the assumptions the e2e plan rests on:
//
//  1. Does Stop+Start preserve the externally mapped port?
//  2. Does the existing v1 *gocql.Session recover automatically after Start,
//     and within what window?
//  3. Same question for v2 *gocqlv2.Session.
//  4. What error shapes do v1 and v2 emit during the outage window?
//  5. Does Pause/Unpause behave as expected (hung TCP, resumes cleanly)?
//
// Run with: go test -tags spike -timeout 5m -v -run TestSpike ./test/e2e/cql/...
//
// This file is independent of the //go:build e2e suite — once findings are
// recorded, it can be deleted.
package cql_test

import (
	"context"
	"errors"
	"testing"
	"time"

	gocqlv2 "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/gocql/gocql"

	"github.com/arloliu/helix/test/testutil"
)

func TestSpike_StopStartPortPreserved(t *testing.T) {
	ctx := context.Background()

	t.Log("Starting Scylla cluster (default options)…")
	cluster, err := testutil.StartCQLCluster(ctx, testutil.DefaultCQLClusterOptions("spike_keyspace"))
	if err != nil {
		t.Fatalf("StartCQLCluster: %v", err)
	}
	t.Cleanup(func() { _ = cluster.Terminate(ctx) })

	hostBefore := cluster.Host
	t.Logf("Cluster up at host=%s type=%s", hostBefore, cluster.Type)

	if err := cluster.Session.Query("SELECT key FROM system.local").Exec(); err != nil {
		t.Fatalf("v1 sanity query failed: %v", err)
	}
	if err := cluster.SessionV2.Query("SELECT key FROM system.local").Exec(); err != nil {
		t.Fatalf("v2 sanity query failed: %v", err)
	}
	t.Log("Sanity queries passed on both v1 and v2")

	// Use a longer grace so Cassandra can flush; SIGKILL after 1s corrupted
	// state in earlier runs and the restart failed the readiness check.
	stopStart := time.Now()
	if err := cluster.Stop(ctx, 30*time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	t.Logf("Stop took %s", time.Since(stopStart))

	startStart := time.Now()
	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Logf("Start took %s", time.Since(startStart))

	hostAfter, err := cluster.RefreshHost(ctx)
	if err != nil {
		t.Fatalf("refresh host: %v", err)
	}
	t.Logf("Host before=%s after=%s preserved=%v", hostBefore, hostAfter, hostBefore == hostAfter)
	if hostBefore != hostAfter {
		t.Log("FINDING: port NOT preserved across Stop/Start — Reconnect will be mandatory per cycle")
	} else {
		t.Log("FINDING: port preserved — auto-reconnect is feasible without rebuilding sessions")
	}

	// Auto-reconnect probe with a short budget — if port wasn't preserved
	// (which is the documented testcontainers behavior), this should fail
	// quickly. We then call Reconnect to rebuild sessions against the new
	// mapping and confirm queries succeed.
	v1Window := measureReconnectV1(t, cluster.Session, 5*time.Second)
	t.Logf("FINDING: v1 auto-reconnect window (pre-Reconnect) = %s", v1Window)

	v2Window := measureReconnectV2(t, cluster.SessionV2, 5*time.Second)
	t.Logf("FINDING: v2 auto-reconnect window (pre-Reconnect) = %s", v2Window)

	// Rebuild sessions against the new host and confirm queries succeed.
	t.Log("Calling cluster.Reconnect to rebuild sessions against the new mapping…")
	if err := cluster.Reconnect(ctx); err != nil {
		t.Fatalf("Reconnect: %v", err)
	}
	if err := cluster.Session.Query("SELECT key FROM system.local").Exec(); err != nil {
		t.Errorf("v1 sanity query post-Reconnect: %v", err)
	} else {
		t.Log("FINDING: post-Reconnect v1 query succeeded")
	}
	if err := cluster.SessionV2.Query("SELECT key FROM system.local").Exec(); err != nil {
		t.Errorf("v2 sanity query post-Reconnect: %v", err)
	} else {
		t.Log("FINDING: post-Reconnect v2 query succeeded")
	}

	t.Log("Capturing error shapes during a fresh outage…")
	if err := cluster.Stop(ctx, 1*time.Second); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	v1Err := probeErrorV1(cluster.Session)
	v2Err := probeErrorV2(cluster.SessionV2)
	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("second Start: %v", err)
	}

	classifyError(t, "v1", v1Err)
	classifyError(t, "v2", v2Err)
}

func TestSpike_PauseUnpause(t *testing.T) {
	ctx := context.Background()

	cluster, err := testutil.StartCQLCluster(ctx, testutil.CQLClusterOptions{
		Keyspace:       "spike_pause_keyspace",
		PreferScyllaDB: true,
		ScyllaDBImage:  "scylladb/scylla:6.2",
		CassandraImage: "cassandra:4.1",
		ScyllaDBMemory: "512M",
		ScyllaDBSMP:    1,
		// Tight timeouts so the paused-query test bounds in seconds.
		SessionTimeout: 3 * time.Second,
		ConnectTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("StartCQLCluster: %v", err)
	}
	t.Cleanup(func() { _ = cluster.Terminate(ctx) })

	// Always unpause on cleanup so we don't leave a frozen container behind.
	t.Cleanup(func() { _ = cluster.Unpause(context.Background()) })

	if err := cluster.Pause(ctx); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	start := time.Now()
	v1Err := cluster.Session.Query("SELECT key FROM system.local").Exec()
	t.Logf("v1 paused-query wall=%s err=%T: %v", time.Since(start), v1Err, v1Err)

	start = time.Now()
	v2Err := cluster.SessionV2.Query("SELECT key FROM system.local").Exec()
	t.Logf("v2 paused-query wall=%s err=%T: %v", time.Since(start), v2Err, v2Err)

	classifyError(t, "v1-paused", v1Err)
	classifyError(t, "v2-paused", v2Err)

	if err := cluster.Unpause(ctx); err != nil {
		t.Fatalf("Unpause: %v", err)
	}

	v1Window := measureReconnectV1(t, cluster.Session, 30*time.Second)
	v2Window := measureReconnectV2(t, cluster.SessionV2, 30*time.Second)
	t.Logf("FINDING: post-unpause v1 recovery=%s v2 recovery=%s", v1Window, v2Window)
}

// --- helpers ---------------------------------------------------------------

func measureReconnectV1(t *testing.T, s *gocql.Session, budget time.Duration) time.Duration {
	t.Helper()
	deadline := time.Now().Add(budget)
	start := time.Now()
	attempts := 0
	for time.Now().Before(deadline) {
		attempts++
		err := s.Query("SELECT key FROM system.local").Exec()
		if err == nil {
			t.Logf("v1 recovered after %d attempts in %s", attempts, time.Since(start))
			return time.Since(start)
		}
		time.Sleep(250 * time.Millisecond)
	}
	// Non-recovery is a documented finding for the spike, not a failure.
	t.Logf("v1 did not recover within %s (attempts=%d)", budget, attempts)

	return budget
}

func measureReconnectV2(t *testing.T, s *gocqlv2.Session, budget time.Duration) time.Duration {
	t.Helper()
	deadline := time.Now().Add(budget)
	start := time.Now()
	attempts := 0
	for time.Now().Before(deadline) {
		attempts++
		err := s.Query("SELECT key FROM system.local").Exec()
		if err == nil {
			t.Logf("v2 recovered after %d attempts in %s", attempts, time.Since(start))
			return time.Since(start)
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Logf("v2 did not recover within %s (attempts=%d)", budget, attempts)

	return budget
}

func probeErrorV1(s *gocql.Session) error {
	return s.Query("SELECT key FROM system.local").Exec()
}

func probeErrorV2(s *gocqlv2.Session) error {
	return s.Query("SELECT key FROM system.local").Exec()
}

func classifyError(t *testing.T, label string, err error) {
	t.Helper()
	if err == nil {
		t.Logf("%s: no error", label)
		return
	}
	t.Logf("%s: type=%T msg=%q", label, err, err.Error())

	// v1-only sentinels — useful as a baseline; an errors.Is hit on a v2 error
	// would prove the chain crosses driver boundaries (unlikely but worth seeing).
	for _, target := range []error{gocql.ErrConnectionClosed, gocql.ErrNoConnections, gocql.ErrTimeoutNoResponse} {
		if errors.Is(err, target) {
			t.Logf("%s: errors.Is %v ✓", label, target)
		}
	}
}
