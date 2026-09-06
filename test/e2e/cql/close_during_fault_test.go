//go:build e2e

package cql_test

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/test/testutil"
)

// TestS_PauseA_CloseReturnsDuringFault asserts that shutting a client down in the middle of a fault is bounded.
//
// Unlike every other scenario in this package,
// the client is built over driver sessions dedicated to this test rather than the harness's shared ones,
// so helix.CQLClient.Close reaches the driver's own Session.Close instead of the noCloseSession no-op.
// That is the point: while a node is paused the driver may be reconnecting,
// and a reconnect parked in a dial used to outlive Close — hanging it,
// or landing afterwards and publishing a connection into a session that had already torn down.
//
// The second, direct close of each driver session is the terminality half:
// Close must stay a no-op the second time rather than hang or panic.
func TestS_PauseA_CloseReturnsDuringFault(t *testing.T) {
	// closeBound is how long a shutdown may take while a cluster is paused.
	// It is deliberately generous: the failure it guards against was an unbounded hang, not a slow close.
	const closeBound = 10 * time.Second

	// faultWindow is how long cluster A stays paused, under read pressure, before the shutdown is measured.
	// It must outlast the v2 driver's silent-node detection (30-35 s on the pinned fork),
	// so the driver's own reconnect is actually running when Close is called.
	// A reconnect parked in a per-host dial against a paused node is what used to hold Close open.
	const faultWindow = 40 * time.Second

	// goroutineSlack is the delta the goroutine settle loop tolerates.
	// It cannot be zero: the pause and unpause go through the Docker client,
	// and the harness's own shared sessions for A reconnect against the same paused container.
	// It sits below one session, which holds 12 goroutines on the v1 driver and 13 on v2,
	// so a session left behind is not hidden, and above the largest transient delta seen so far (+5).
	const goroutineSlack = 10

	// settleSamples is how many consecutive samples must sit within goroutineSlack before the count is trusted.
	const settleSamples = 100

	a, b := sharedClusters(t)
	withRestoredCluster(t, a)

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			// A previous scenario's pause can leave a driver with an empty pool that fails every request at once.
			ensureReachable(t, a, d)
			ensureReachable(t, b, d)

			table := createKVTableOnBoth(t, "s_close_during_fault")
			seedKV(t, a, b, table, "k", "v")
			stmt := "SELECT value FROM " + table + " WHERE key = ?"

			// The baseline is taken with the shared sessions warm and nothing of this test's own started yet.
			before := runtime.NumGoroutine()

			sessA, rawCloseA, err := d.newDedicated(a)
			require.NoError(t, err, "[%s] dedicated session for A", d.name)
			closeDriverA := closeOnce(t, rawCloseA)

			sessB, rawCloseB, err := d.newDedicated(b)
			require.NoError(t, err, "[%s] dedicated session for B", d.name)
			closeDriverB := closeOnce(t, rawCloseB)

			client, err := helix.NewCQLClient(sessA, sessB,
				helix.WithClusterReadTimeout(time.Second),
				helix.WithLogger(testutil.NewTestLogger(t)),
			)
			require.NoError(t, err)
			closeClient := closeOnce(t, client.Close)

			read := func(timeout time.Duration) error {
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				var got string

				return client.Query(stmt, "k").ScanContext(ctx, &got)
			}

			// Warm the statement on both clusters while everything is healthy,
			// so the fault window is spent on the read rather than on a first preparation.
			for range 3 {
				require.NoError(t, read(5*time.Second), "[%s] warm-up read", d.name)
			}

			require.NoError(t, a.Pause(context.Background()))
			unpaused := false
			defer func() {
				if !unpaused {
					_ = a.Unpause(context.Background())
				}
			}()

			// Hold A under read pressure for the whole fault window.
			// The shutdown then starts while the session is failing and the driver is reconnecting.
			// The ticker only paces generated load; no assertion waits on it.
			// Every read runs on this goroutine, so nothing can log after the subtest returns.
			pace := time.NewTicker(500 * time.Millisecond)
			defer pace.Stop()
			windowEnd := time.After(faultWindow)
			reads := 0
		pressure:
			for {
				select {
				case <-windowEnd:
					break pressure
				case <-pace.C:
					_ = read(2 * time.Second)
					reads++
				}
			}
			t.Logf("[%s] %d reads issued during the %s pause", d.name, reads, faultWindow)

			clientClose := closeWithin(t, closeBound, closeClient, "[%s] client.Close with cluster A paused", d.name)
			t.Logf("[%s] client.Close took %s with cluster A paused", d.name, clientClose)

			// The client already closed both driver sessions through the adapters.
			// These calls prove the driver's own Close is terminal, not a hang or a panic the second time.
			driverAClose := closeWithin(t, closeBound, closeDriverA, "[%s] A's second driver Close", d.name)
			driverBClose := closeWithin(t, closeBound, closeDriverB, "[%s] B's second driver Close", d.name)
			t.Logf("[%s] second driver Close: A %s, B %s", d.name, driverAClose, driverBClose)

			require.NoError(t, a.Unpause(context.Background()))
			unpaused = true

			// Settle synchronously: a condition running on its own goroutine would inflate the count it reads.
			// The bound must hold across consecutive samples, so a momentary dip while A's pools refill cannot pass the check.
			settleEnd := time.Now().Add(10 * time.Second)
			after, held := runtime.NumGoroutine(), 0
			for held < settleSamples && time.Now().Before(settleEnd) {
				runtime.Gosched()
				after = runtime.NumGoroutine()
				if after <= before+goroutineSlack {
					held++
				} else {
					held = 0
				}
			}
			t.Logf("[%s] goroutines: %d before, %d after", d.name, before, after)
			assert.Equal(t, settleSamples, held,
				"[%s] a closed session must not leave its goroutines behind: %d before, %d after", d.name, before, after)
		})
	}
}

// closeOnce registers closeFn as a cleanup and returns a wrapper that runs it
// at most once. The test can then close the resource where it wants to measure
// the call, and still be covered if it fails before reaching that point,
// without the resource being closed twice.
// closeWithin runs closeFn on its own goroutine and fails the test if it has
// not returned after bound, so a Close that hangs again is reported at the
// bound rather than at the package timeout, with the deferred unpause still
// to come.
// It returns how long closeFn took.
func closeWithin(t *testing.T, bound time.Duration, closeFn func(), format string, args ...any) time.Duration {
	t.Helper()

	start := time.Now()
	done := make(chan struct{})
	go func() {
		defer close(done)
		closeFn()
	}()

	select {
	case <-done:
	case <-time.After(bound):
		t.Fatalf(format+": no return within %s", append(args, bound)...)
	}

	return time.Since(start)
}

func closeOnce(t *testing.T, closeFn func()) func() {
	t.Helper()

	var once sync.Once
	call := func() { once.Do(closeFn) }
	t.Cleanup(call)

	return call
}
