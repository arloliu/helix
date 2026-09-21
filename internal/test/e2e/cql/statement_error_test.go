//go:build e2e

package cql_test

// A statement the coordinator rejects is the caller's error, not the
// cluster's. The unit matrix asserts the classification against a fake
// session; this scenario asserts it against a real coordinator's
// "unconfigured table" answer (CQL error code 0x2200), on both adapters,
// with a live breaker and a live read strategy watching.

import (
	"errors"
	"testing"
	"time"

	gocqlv2 "github.com/apache/cassandra-gocql-driver/v2"
	gocqlv1 "github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/internal/test/testutil"
	"github.com/arloliu/helix/policy"
	htypes "github.com/arloliu/helix/types"
)

// cqlErrCodeInvalid is the CQL protocol code for a statement the
// coordinator refused as invalid, which is what an unconfigured table
// produces on both ScyllaDB and Cassandra.
const cqlErrCodeInvalid = 0x2200

// requestErrCode extracts the driver's CQL error code from err through the
// exported RequestError interface of the adapter's own driver.
//
// The two adapters wrap different packages, so the assertion that the
// driver error survived Helix's wrapping has to be made per driver; there
// is no adapter-level accessor for it.
var requestErrCode = map[string]func(err error) (int, bool){
	"v1": func(err error) (int, bool) {
		var reqErr gocqlv1.RequestError
		if !errors.As(err, &reqErr) {
			return 0, false
		}

		return reqErr.Code(), true
	},
	"v2": func(err error) (int, bool) {
		var reqErr gocqlv2.RequestError
		if !errors.As(err, &reqErr) {
			return 0, false
		}

		return reqErr.Code(), true
	},
}

// createKVTableOnOne creates the standard (key TEXT PRIMARY KEY, value TEXT)
// schema on one cluster only, so a read routed at the other cluster is
// rejected by its coordinator while the same statement stays valid on this
// one.
// A failover would therefore return rows rather than an error.
//
// The cleanup is registered before the CREATE, so a failure between the two
// still drops the table; IF EXISTS covers the table that was never created.
func createKVTableOnOne(t *testing.T, cluster *testutil.CQLCluster, prefix string) string {
	t.Helper()

	tableName := uniqueTableName(prefix)
	t.Cleanup(func() {
		_ = cluster.Session.Query("DROP TABLE IF EXISTS " + tableName).Exec()
	})

	createKVTableOn(t, cluster, tableName)

	return tableName
}

// TestS_StatementRejected_IsNotAFailoverSignal drives reads at the cluster
// that lacks the table and asserts the rejection reaches the caller
// untouched by the health machinery:
//
//  1. StickyRead is pinned to cluster B; the table exists on A only, so
//     every read is rejected by B's coordinator with 0x2200.
//  2. Each read returns an error matching types.ErrStatementRejected with
//     the driver's RequestError still in the chain.
//  3. Five rejections — above the breaker's threshold of 3 — leave the
//     breaker closed, the preference on B, and the failover metric at zero.
//     The threshold is what gives these three assertions teeth: a rejection
//     routed through FailoverPolicy.RecordFailure would open the breaker on
//     the third read and move the preference. Do not raise it above five.
//  4. Cluster A is never contacted, so its read total stays zero: the read
//     did not fail over to the cluster where the statement is valid.
//  5. A control read of a table that exists on both is still served by B,
//     so the rejections left the route intact rather than merely unused.
//  6. An iterator's rejection behaves the same, surfacing at Close.
func TestS_StatementRejected_IsNotAFailoverSignal(t *testing.T) {
	a, b := sharedClusters(t)

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			// A previous scenario's pause can leave a driver with an empty
			// connection pool that fails every request at once, which would
			// look like a rejected statement's absence.
			ensureReachable(t, a, d)
			ensureReachable(t, b, d)

			codeOf, ok := requestErrCode[d.name]
			require.True(t, ok, "no RequestError accessor for driver %q", d.name)

			missing := createKVTableOnOne(t, a, "stmt_rejected")
			present := createKVTableOnBoth(t, "stmt_rejected_ctrl")
			seedKV(t, a, b, present, "k", "v")

			lcb := policy.NewLatencyCircuitBreaker(
				policy.WithLatencyAbsoluteMax(500*time.Millisecond),
				policy.WithLatencyThreshold(3),
				policy.WithLatencyResetTimeout(30*time.Second),
			)
			rs := policy.NewStickyRead(policy.WithPreferredCluster(htypes.ClusterB))
			mc := testutil.NewTestMetricsCollector()

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithReadStrategy(rs),
				helix.WithFailoverPolicy(lcb),
				helix.WithMetrics(mc),
				helix.WithLogger(testutil.NewTestLogger(t)),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			const rejectedReads = 5
			stmt := "SELECT value FROM " + missing + " WHERE key = ?"

			for i := range rejectedReads {
				var got string
				readErr := client.Query(stmt, "k").ScanContext(t.Context(), &got)

				code, hasCode := codeOf(readErr)
				t.Logf("[%s] rejected read %d: err=%v code=%#x hasRequestError=%v",
					d.name, i, readErr, code, hasCode)

				require.ErrorIs(t, readErr, htypes.ErrStatementRejected,
					"[%s] read %d must carry types.ErrStatementRejected", d.name, i)
				require.True(t, hasCode,
					"[%s] read %d must keep the driver's RequestError in the chain", d.name, i)
				require.Equal(t, cqlErrCodeInvalid, code,
					"[%s] read %d must report the coordinator's invalid-statement code", d.name, i)

				var dual *htypes.DualClusterError
				require.False(t, errors.As(readErr, &dual),
					"[%s] read %d must not be folded into a dual-cluster error: no second leg ran",
					d.name, i)
			}

			t.Logf("[%s] after %d rejections: failovers=%d breakerB=%v preferred=%s "+
				"readTotal(A)=%d readTotal(B)=%d readErrors(A)=%d readErrors(B)=%d",
				d.name, rejectedReads, mc.GetTotalFailovers(),
				lcb.ShouldFailover(htypes.ClusterB, nil), rs.Preferred(),
				mc.ReadTotal[htypes.ClusterA], mc.ReadTotal[htypes.ClusterB],
				mc.GetReadErrors(htypes.ClusterA), mc.GetReadErrors(htypes.ClusterB))

			assert.Zero(t, mc.GetTotalFailovers(),
				"[%s] a rejected statement must not trigger a failover", d.name)
			assert.False(t, lcb.ShouldFailover(htypes.ClusterB, nil),
				"[%s] %d rejections past a threshold of 3 must leave the breaker closed",
				d.name, rejectedReads)
			assert.Equal(t, htypes.ClusterB, rs.Preferred(),
				"[%s] a rejected statement must not move the read preference", d.name)

			assert.Equal(t, int64(rejectedReads), mc.GetReadErrors(htypes.ClusterB),
				"[%s] every rejection counts one read error on the cluster that returned it",
				d.name)
			assert.Zero(t, mc.GetReadErrors(htypes.ClusterA),
				"[%s] cluster A returned nothing, so it owns no read error", d.name)
			assert.Equal(t, int64(rejectedReads), mc.ReadTotal[htypes.ClusterB],
				"[%s] each rejected read is one attempt on B", d.name)
			assert.Zero(t, mc.ReadTotal[htypes.ClusterA],
				"[%s] cluster A must never be contacted: the statement is valid there", d.name)

			// Control: the route is intact, not merely unused.
			// The same client, the same preference, a table that exists on both.
			var got string
			require.NoError(t, client.Query("SELECT value FROM "+present+" WHERE key = ?", "k").
				ScanContext(t.Context(), &got),
				"[%s] a valid statement must still succeed after the rejections", d.name)
			assert.Equal(t, "v", got)
			assert.Equal(t, int64(rejectedReads+1), mc.ReadTotal[htypes.ClusterB],
				"[%s] the control read was served by B", d.name)
			assert.Zero(t, mc.ReadTotal[htypes.ClusterA],
				"[%s] the control read did not touch A either", d.name)
			assert.Equal(t, int64(rejectedReads), mc.GetReadErrors(htypes.ClusterB),
				"[%s] the control read added no read error", d.name)

			// Iterator: the rejection surfaces at Close (or at the first
			// Scan), never as a dual-cluster error, and is accounted exactly
			// once like the Scan reads.
			errorsBefore := mc.GetReadErrors(htypes.ClusterB)
			totalBefore := mc.ReadTotal[htypes.ClusterB]

			iter := client.Query(stmt, "k").IterContext(t.Context())
			scanned := iter.Scan(&got)
			closeErr := iter.Close()
			t.Logf("[%s] iterator rejection: scanned=%v closeErr=%v", d.name, scanned, closeErr)

			require.ErrorIs(t, closeErr, htypes.ErrStatementRejected,
				"[%s] an iterator's rejection must reach the caller at Close", d.name)
			code, hasCode := codeOf(closeErr)
			require.True(t, hasCode,
				"[%s] the iterator's error must keep the driver's RequestError in the chain", d.name)
			assert.Equal(t, cqlErrCodeInvalid, code,
				"[%s] the iterator must report the coordinator's invalid-statement code", d.name)

			assert.Zero(t, mc.GetTotalFailovers(),
				"[%s] an iterator's rejection must not trigger a failover either", d.name)
			assert.Equal(t, htypes.ClusterB, rs.Preferred(),
				"[%s] an iterator's rejection must not move the read preference", d.name)
			assert.Equal(t, errorsBefore+1, mc.GetReadErrors(htypes.ClusterB),
				"[%s] the iterator's rejection counts exactly one read error on B", d.name)
			assert.Equal(t, totalBefore+1, mc.ReadTotal[htypes.ClusterB],
				"[%s] the iterator's rejection counts exactly one read attempt on B", d.name)
			assert.Zero(t, mc.ReadTotal[htypes.ClusterA],
				"[%s] the iterator must not reach the cluster where the statement is valid", d.name)
		})
	}
}
