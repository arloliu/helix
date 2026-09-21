//go:build e2e && multinode

package cql_test

// Every other e2e scenario runs one container per cluster,
// so "a node is down" and "the cluster is down" are the same event
// and the boundary between them has never been exercised.
// This scenario runs cluster A as one ScyllaDB cluster of two nodes at replication factor 2,
// which is the smallest shape where the two differ:
// one node can fail while the cluster still answers.
//
// Cluster B is the shared single-node cluster the rest of the package uses.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/adapter/cql"
	cqlv1 "github.com/arloliu/helix/adapter/cql/v1"
	cqlv2 "github.com/arloliu/helix/adapter/cql/v2"
	"github.com/arloliu/helix/internal/test/testutil"
	"github.com/arloliu/helix/policy"
	htypes "github.com/arloliu/helix/types"
)

// twoNodeBreakerThreshold is how many consecutive failures cluster A is given before the breaker opens.
// It is above one on purpose:
// a read that fails while the breaker is still closed is returned to the caller,
// which is what makes "the request fails" observable separately from "the client moved to the other cluster".
const twoNodeBreakerThreshold = 3

// twoNodeReadRounds is how many reads the node-fault phase issues.
// One read proves little,
// because a routing policy that still holds the paused node hands it out only some of the time.
const twoNodeReadRounds = 20

// twoNodeBaselineRounds is how many reads run while both nodes are up.
// Two would be enough to reach both of them through the drivers' round-robin routing;
// the rest are margin.
const twoNodeBaselineRounds = 10

// twoNodeMembershipTimeout bounds the wait for the cluster's own failure detector
// to agree on how many nodes are up.
// Gossip takes about twenty-five seconds to mark a frozen node down.
const twoNodeMembershipTimeout = 90 * time.Second

var (
	twoNodeOnce     sync.Once
	twoNodeCluster  *testutil.TwoNodeCQLCluster
	errTwoNodeStart error
)

// twoNodeDriver pairs the two clusters' sessions for one adapter version.
// Cluster A is the two-node cluster this file starts;
// cluster B is the package's shared single-node cluster.
type twoNodeDriver struct {
	name  string
	wrapA func(*testutil.TwoNodeCQLCluster) cql.Session
	wrapB func(*testutil.CQLCluster) cql.Session
}

// twoNodeDrivers runs the scenario on both adapters.
// The sessions are wrapped in noCloseSession
// so that closing the client does not close a session the next subtest still needs.
var twoNodeDrivers = []twoNodeDriver{
	{
		name: "v1",
		wrapA: func(c *testutil.TwoNodeCQLCluster) cql.Session {
			return &noCloseSession{Session: cqlv1.NewSession(c.Session)}
		},
		wrapB: func(c *testutil.CQLCluster) cql.Session {
			return &noCloseSession{Session: cqlv1.NewSession(c.Session)}
		},
	},
	{
		name: "v2",
		wrapA: func(c *testutil.TwoNodeCQLCluster) cql.Session {
			return &noCloseSession{Session: cqlv2.NewSession(c.SessionV2)}
		},
		wrapB: func(c *testutil.CQLCluster) cql.Session {
			return &noCloseSession{Session: cqlv2.NewSession(c.SessionV2)}
		},
	},
}

// TestS_TwoNodeCluster_NodeFaultIsNotClusterFault pins the line
// between a node-level fault and a cluster-level one.
//
// With one of cluster A's two nodes frozen:
//
//   - a read at consistency One is still answered by the surviving node.
//     Helix must not count that as a cluster failure:
//     no failover, no breaker failures, no trip.
//   - a read at consistency All cannot be answered at all,
//     because replication factor 2 needs both nodes.
//     Helix must count that as a cluster failure:
//     the request fails while the breaker is closed,
//     and once the breaker opens the read moves to cluster B.
//
// The same knob produces both outcomes,
// so the first claim cannot pass by the client being blind to the fault.
//
// Cluster A's drivers retry on the next node,
// which is what a driver against a real multi-node cluster has to be configured to do.
// Without it the node going down costs one request outright,
// and Helix has no way to tell that request apart from a cluster error;
// see [testutil.TwoNodeCQLClusterOptions] for the whole story.
//
// Two things this test does not pin.
// It does not pin a driver without that retry policy,
// where a node fault does reach Helix as a cluster failure.
// It does not pin the time before the cluster's own failure detector marks the frozen node down,
// about twenty-five seconds,
// during which reads at consistency One still go to the frozen node and time out;
// the node-fault phase waits that window out and asserts the steady state after it.
func TestS_TwoNodeCluster_NodeFaultIsNotClusterFault(t *testing.T) {
	_, b := sharedClusters(t)
	a := sharedTwoNodeCluster(t)

	table := createKVTableOnTwoNodeAndB(t, b, "two_node")

	for _, d := range twoNodeDrivers {
		t.Run(d.name, func(t *testing.T) {
			breaker := policy.NewCircuitBreaker(
				policy.WithThreshold(twoNodeBreakerThreshold),
				policy.WithResetTimeout(time.Minute),
			)
			mc := testutil.NewTestMetricsCollector()

			client, err := helix.NewCQLClient(d.wrapA(a), d.wrapB(b),
				helix.WithReadStrategy(policy.NewStickyRead(
					policy.WithPreferredCluster(htypes.ClusterA),
				)),
				helix.WithFailoverPolicy(breaker),
				helix.WithMetrics(mc),
				helix.WithLogger(testutil.NewTestLogger(t)),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			key := "k-" + d.name
			seedTwoNode(t, client, table, key, "v")

			// Restore the cluster even if an assertion below fails.
			t.Cleanup(func() {
				_ = a.UnpauseNode(context.Background(), 1)
			})

			assertHealthyBaseline(t, client, mc, table, key)

			require.NoError(t, a.PauseNode(ctx, 1))
			waitForUpNodes(t, a, 1)
			settleAfterMembershipChange(t, d.wrapA(a))

			assertNodeFaultStaysLocal(t, client, breaker, mc, table, key)
			assertClusterFaultEscalates(t, client, breaker, mc, table, key)

			require.NoError(t, a.UnpauseNode(ctx, 1))
			waitForUpNodes(t, a, 2)
			assertClusterWhole(t, a, table, key)
		})
	}
}

// assertHealthyBaseline reads the seeded row back while both of cluster A's nodes are up.
//
// It is the scenario's steady state, and it is also what puts the statement
// in each node's prepared-statement cache.
// gocql prepares a statement per node on first use,
// and it runs that prepare on the connection's own context rather than the caller's:
// a prepare that lands on a node the driver is taking out of its pool comes back as
// "context canceled" to a caller whose context is alive,
// and the driver then declines to retry it because it reads that as the caller giving up.
// Preparing here, before anything is paused, keeps that race out of the scenario.
func assertHealthyBaseline(
	t *testing.T,
	client *helix.CQLClient,
	mc *testutil.TestMetricsCollector,
	table, key string,
) {
	t.Helper()

	for i := range twoNodeBaselineRounds {
		var got string
		err := client.Query("SELECT value FROM "+table+" WHERE key = ?", key).
			Consistency(helix.Quorum).
			Scan(&got)
		require.NoError(t, err, "baseline read %d must succeed while both nodes are up", i)
		require.Equal(t, "v", got, "baseline read %d returned the wrong value", i)
	}

	require.Zero(t, mc.GetTotalFailovers(), "a healthy cluster A must not produce a failover")
}

// assertNodeFaultStaysLocal drives reads the surviving node can answer on its own,
// and requires that none of them reached Helix as a cluster fault.
//
// The counters are read after the last read has returned,
// which is the point where nothing in the client can still move them:
// every read is synchronous and no failover ran that could leave work behind.
func assertNodeFaultStaysLocal(
	t *testing.T,
	client *helix.CQLClient,
	breaker *policy.CircuitBreaker,
	mc *testutil.TestMetricsCollector,
	table, key string,
) {
	t.Helper()

	for i := range twoNodeReadRounds {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		var got string
		err := client.Query("SELECT value FROM "+table+" WHERE key = ?", key).
			Consistency(helix.One).
			ScanContext(ctx, &got)
		cancel()
		require.NoError(t, err, "read %d at consistency One must be answered by the surviving node", i)
		require.Equal(t, "v", got, "read %d returned the wrong value", i)
	}

	require.Zero(t, mc.GetTotalFailovers(),
		"a node fault inside cluster A must not move reads to cluster B")
	require.Zero(t, breaker.Failures(htypes.ClusterA),
		"a node fault inside cluster A must not be recorded against the cluster")
	require.Zero(t, mc.CircuitBreakerTrips[htypes.ClusterA],
		"cluster A's breaker must not trip on a node fault")
	require.Zero(t, mc.CircuitBreakerState[htypes.ClusterA],
		"cluster A's breaker must stay closed on a node fault")
	require.False(t, breaker.ShouldFailover(htypes.ClusterA, nil),
		"cluster A must still be eligible for reads")
}

// assertClusterFaultEscalates drives reads no single node can answer,
// and requires that Helix treats them as cluster failures.
//
// While the breaker is closed the failure is the caller's to see;
// once it opens, the read moves to cluster B.
func assertClusterFaultEscalates(
	t *testing.T,
	client *helix.CQLClient,
	breaker *policy.CircuitBreaker,
	mc *testutil.TestMetricsCollector,
	table, key string,
) {
	t.Helper()

	read := func() (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		var got string
		err := client.Query("SELECT value FROM "+table+" WHERE key = ?", key).
			Consistency(helix.All).
			ScanContext(ctx, &got)

		return got, err
	}

	got, err := read()
	require.Error(t, err,
		"consistency All needs both of cluster A's nodes, so this read cannot succeed on A")
	require.ErrorIs(t, err, htypes.ErrClusterUnreachable,
		"an unsatisfiable consistency level is a cluster fault, not a statement error")
	require.Empty(t, got)

	// Keep reading until the breaker has seen enough consecutive failures to open,
	// at which point the read moves to cluster B and succeeds there.
	movedToB := false
	for range twoNodeBreakerThreshold + 1 {
		got, err = read()
		if err == nil {
			movedToB = true

			break
		}
		require.ErrorIs(t, err, htypes.ErrClusterUnreachable)
	}
	require.True(t, movedToB,
		"once cluster A's breaker opens the read must move to cluster B")
	require.Equal(t, "v", got, "the failed-over read must return the row cluster B holds")

	require.True(t, breaker.ShouldFailover(htypes.ClusterA, nil),
		"cluster A's breaker must be open after a run of cluster faults")
	require.GreaterOrEqual(t, mc.CircuitBreakerTrips[htypes.ClusterA], int64(1),
		"the trip must be reported")
	require.GreaterOrEqual(t, mc.GetTotalFailovers(), int64(1),
		"the escalation must be reported as a failover")
}

// assertClusterWhole checks that cluster A serves a read needing both nodes once the frozen node is back.
//
// The check goes through the cluster's own session rather than the client,
// because the client's preference has legitimately moved to cluster B by now,
// and a read through it would say nothing about cluster A.
func assertClusterWhole(t *testing.T, a *testutil.TwoNodeCQLCluster, table, key string) {
	t.Helper()

	var got string
	err := a.Session.Query("SELECT value FROM "+table+" WHERE key = ?", key).
		Consistency(gocql.All).
		Scan(&got)
	require.NoError(t, err, "cluster A must answer a read needing both nodes once the node is back")
	require.Equal(t, "v", got)
}

// sharedTwoNodeCluster returns the package's two-node cluster,
// starting it on first use and handing it back through the package teardown.
// Every scenario in this file shares one cluster;
// starting a second one would cost another boot and another subnet.
func sharedTwoNodeCluster(t *testing.T) *testutil.TwoNodeCQLCluster {
	t.Helper()

	twoNodeOnce.Do(func() {
		opts := testutil.DefaultTwoNodeCQLClusterOptions("helix_e2e_multinode")
		opts.ReconnectInterval = 500 * time.Millisecond
		twoNodeCluster, errTwoNodeStart = testutil.StartTwoNodeCQLCluster(context.Background(), opts)
		if errTwoNodeStart == nil {
			e2eTeardowns = append(e2eTeardowns, func(ctx context.Context) {
				fmt.Println("Tearing down the e2e/cql two-node cluster…")
				_ = twoNodeCluster.Terminate(ctx)
			})
		}
	})
	require.NoError(t, errTwoNodeStart, "start the two-node cluster")

	return twoNodeCluster
}

// waitForUpNodes blocks until cluster A's surviving node reports want members up and normal.
//
// The cluster-fault phase depends on this wait.
// Until the cluster's own failure detector has marked the frozen node down,
// a coordinator answers a read at consistency All with a read timeout
// rather than with "cannot achieve consistency level",
// and only the latter is a fault Helix classifies as the cluster being unreachable.
// Taking the wait out turned that assertion red in two runs out of four.
//
// Membership lives in the containers and nothing in the test process is notified when it changes,
// which is the case the async-wait rule allows polling for.
func waitForUpNodes(t *testing.T, a *testutil.TwoNodeCQLCluster, want int) {
	t.Helper()

	require.Eventually(t, func() bool {
		up, err := a.UpNodeCount(context.Background(), 0)

		return err == nil && up == want
	}, twoNodeMembershipTimeout, 500*time.Millisecond,
		"cluster A must report %d node(s) up and normal", want)
}

// settleAfterMembershipChange waits until cluster A's session answers again
// after the cluster's view of its membership changed.
//
// A driver drops a down node's connections as soon as the cluster reports the node down,
// and a request already on one of those connections dies with them.
// This is a second gate in front of the assertions rather than the one that makes them pass:
// removing it along with the membership wait still left the node-fault reads green
// in four runs out of four.
// It stays because a pool that never refilled would end this wait red,
// instead of surfacing further down as a read failure with no obvious cause.
func settleAfterMembershipChange(t *testing.T, sess cql.Session) {
	t.Helper()

	require.Eventually(t, func() bool {
		var version string

		// The probe carries no deadline of its own:
		// the driver's own per-query timeout already bounds each attempt,
		// and cancelling a read in flight is a hazard the scenario has no reason to take on.
		return sess.Query("SELECT release_version FROM system.local").
			Consistency(htypes.One).
			Scan(&version) == nil
	}, 60*time.Second, time.Second,
		"cluster A's session must answer again once the cluster has settled")
}

// createKVTableOnTwoNodeAndB creates the shared key/value table
// on the two-node cluster and on cluster B, and truncates both afterwards.
func createKVTableOnTwoNodeAndB(t *testing.T, b *testutil.CQLCluster, prefix string) string {
	t.Helper()

	a := sharedTwoNodeCluster(t)
	table := uniqueTableName(prefix)
	stmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	key TEXT PRIMARY KEY,
	value TEXT
)`, table)
	require.NoError(t, a.Session.Query(stmt).Exec(), "create table on the two-node cluster")
	createKVTableOn(t, b, table)
	t.Cleanup(func() {
		_ = a.Session.Query("TRUNCATE " + table).Exec()
		_ = b.Session.Query("TRUNCATE " + table).Exec()
	})

	return table
}

// seedTwoNode writes one row to both clusters through the client,
// so a later read can be answered by either of them.
func seedTwoNode(t *testing.T, client *helix.CQLClient, table, key, value string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, client.Query(
		"INSERT INTO "+table+" (key, value) VALUES (?, ?)", key, value).ExecContext(ctx))
}
