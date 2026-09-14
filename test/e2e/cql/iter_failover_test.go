//go:build e2e

package cql_test

import (
	"context"
	"fmt"
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

// iterEventKinds turns the client's cluster events into per-kind
// registrations a scenario can wait on. The client delivers events on its
// own dispatcher goroutine, so both fields are guarded.
//
// A scenario registers the kinds it expects through expect before the
// action that produces them, and the handler closes each registration as
// the event is delivered. The test therefore learns of an event at the
// moment it happens rather than sampling for it afterwards.
//
// kinds is the delivery record. Nothing asserts on it today; it exists so
// expect can answer for an event that arrived before it was called.
type iterEventKinds struct {
	mu       sync.Mutex
	kinds    []htypes.ClusterEventKind
	expected map[htypes.ClusterEventKind]chan struct{}
}

func newIterEventKinds() *iterEventKinds {
	return &iterEventKinds{expected: make(map[htypes.ClusterEventKind]chan struct{})}
}

// expect registers interest in kind and returns a channel closed when the
// first event of that kind arrives. A kind already delivered returns an
// already-closed channel, so registering late is safe rather than silently
// waiting forever.
func (e *iterEventKinds) expect(kind htypes.ClusterEventKind) <-chan struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()

	if ch, ok := e.expected[kind]; ok {
		return ch
	}

	ch := make(chan struct{})
	if slices.Contains(e.kinds, kind) {
		close(ch)
	}
	e.expected[kind] = ch

	return ch
}

func (e *iterEventKinds) handler(ev htypes.ClusterEvent) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.kinds = append(e.kinds, ev.Kind)
	// A kind can arrive more than once; only the first delivery closes.
	if ch, ok := e.expected[ev.Kind]; ok {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}
}

// awaitEvent waits for a registration made by expect.
// It reports a soft failure rather than stopping the subtest, so an e2e run
// that misses one event still reports the breaker state, the preference and
// the latency bounds the assertions after it cover.
func awaitEvent(t *testing.T, ch <-chan struct{}, timeout time.Duration, msg string, args ...any) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(timeout):
		assert.Fail(t, fmt.Sprintf(msg, args...))
	}
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
			events := newIterEventKinds()
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

			// Registered before the pause that produces them.
			breakerOpened := events.expect(htypes.EventCircuitBreakerOpen)
			routeChanged := events.expect(htypes.EventReadRouteChanged)

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

			awaitEvent(t, breakerOpened, 5*time.Second,
				"[%s] the leg expiries must trip the breaker", d.name)
			awaitEvent(t, routeChanged, 5*time.Second,
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
