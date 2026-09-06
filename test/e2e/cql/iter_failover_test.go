//go:build e2e

package cql_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/test/testutil"
	htypes "github.com/arloliu/helix/types"
)

// iterEventKinds collects the kinds of cluster events a scenario produced.
// The client delivers them on its own dispatcher goroutine, so reads and
// writes are guarded.
type iterEventKinds struct {
	mu    sync.Mutex
	kinds []htypes.ClusterEventKind
}

func (e *iterEventKinds) handler(ev htypes.ClusterEvent) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.kinds = append(e.kinds, ev.Kind)
}

func (e *iterEventKinds) has(kind htypes.ClusterEventKind) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	return slices.Contains(e.kinds, kind)
}

// TestS_PauseA_IterFirstPageMovesToTheOtherCluster is the iterator half of
// the leg-deadline contract: with a paused cluster A, the page fetched
// inside IterContext must end on Helix's own deadline, count as A's
// failure, and — once the breaker opens — be served by B.
//
// Before the first page was a leg, every one of these reads waited for the
// caller's whole budget, recorded nothing, and never moved the preference.
//
// The sequence the assertions must respect: the hub records the failure
// before the failover branch is entered, and RecordFailure opens the
// breaker at the threshold, which ShouldFailover then observes.
// With threshold 3 and FailoverBelowThreshold left off, reads one and two
// record a failure and return ErrClusterTimeout with no failover, and read
// three opens the breaker, moves the preference, and is served by B inside
// the same read.
// The loop therefore tolerates errors, and the bound covers the sequence
// rather than the first read.
func TestS_PauseA_IterFirstPageMovesToTheOtherCluster(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, a)

	const legTimeout = time.Second

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			// Before anything else, including the schema: a previous
			// scenario's pause can leave the v2 driver with an empty pool
			// that fails every request at once.
			ensureReachable(t, a, d)
			ensureReachable(t, b, d)

			table := createKVTableOnBoth(t, "s_iter_first_page")
			seedKV(t, a, b, table, "k", "v")
			stmt := "SELECT value FROM " + table + " WHERE key = ?"

			// Warm this exact statement on both clusters.
			// A token-aware read can wait on another caller's
			// routing-metadata load on either driver, and
			// ensureReachable prepares system.local rather than
			// this read.
			warm := func(cluster *testutil.CQLCluster) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				var v string
				require.NoError(t, d.wrap(cluster).Query(stmt, "k").ScanContext(ctx, &v))
				require.Equal(t, "v", v)
			}
			warm(a)
			warm(b)

			lcb := policy.NewLatencyCircuitBreaker(
				policy.WithLatencyAbsoluteMax(500*time.Millisecond),
				policy.WithLatencyThreshold(3),
				policy.WithLatencyResetTimeout(30*time.Second),
			)
			rs := policy.NewStickyRead(policy.WithPreferredCluster(htypes.ClusterA))
			events := &iterEventKinds{}
			mc := testutil.NewTestMetricsCollector()

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithReadStrategy(rs),
				helix.WithFailoverPolicy(lcb),
				helix.WithMetrics(mc),
				helix.WithOnClusterEvent(events.handler),
				helix.WithLogger(testutil.NewTestLogger(t)),
				helix.WithClusterReadTimeout(legTimeout),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			require.NoError(t, a.Pause(ctx))
			defer func() { _ = a.Unpause(context.Background()) }()

			var (
				slowest time.Duration
				reads   int
			)
			// The loop is synchronous: nothing reads or logs after the
			// subtest has returned, which a require.Eventually condition
			// running on its own goroutine could still do.
			read := func() (string, time.Duration, error) {
				qCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
				defer cancel()

				start := time.Now()
				iter := client.Query(stmt, "k").IterContext(qCtx)
				var got string
				iter.Scan(&got)
				readErr := iter.Close()
				elapsed := time.Since(start)

				reads++
				if elapsed > slowest {
					slowest = elapsed
				}
				t.Logf("[%s] read %d took %s (value=%q err=%v)", d.name, reads, elapsed, got, readErr)

				return got, elapsed, readErr
			}

			var served string
			deadline := time.Now().Add(15 * time.Second)
			for served != "v" && time.Now().Before(deadline) {
				served, _, _ = read()
			}
			require.Equal(t, "v", served,
				"[%s] an iterator's first page must leave the frozen cluster", d.name)

			assert.Eventually(t, func() bool {
				return events.has(htypes.EventCircuitBreakerOpen)
			}, 5*time.Second, 50*time.Millisecond,
				"[%s] the leg expiries must trip the breaker", d.name)
			assert.Eventually(t, func() bool {
				return events.has(htypes.EventReadRouteChanged)
			}, 5*time.Second, 50*time.Millisecond,
				"[%s] the strategy must move its preference off the frozen cluster", d.name)

			assert.Less(t, slowest, 3*legTimeout,
				"[%s] every read is bounded by the leg deadline, not by the caller's budget", d.name)

			// Route veto is off by default, so the move is the strategy's
			// own rather than an eligibility filter's.
			assert.Equal(t, htypes.ClusterB, rs.Preferred(), "[%s] the preference moved to B", d.name)
			assert.Positive(t, mc.GetTotalFailovers(), "[%s] the read failed over", d.name)

			got, elapsed, err := read()
			require.NoError(t, err, "[%s] a read on the moved preference is served by B", d.name)
			require.Equal(t, "v", got)
			assert.Less(t, elapsed, 3*legTimeout,
				"[%s] a read after the switch must not wait on the frozen cluster", d.name)
		})
	}
}
