//go:build e2e

package cql_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/testutil"
	htypes "github.com/arloliu/helix/types"
)

// outageHold is how long B stays paused after the last write.
//
// It must outlast the driver's 2 s request timeout (SessionTimeout in e2eOptions) plus a margin.
// A write's background leg to a paused B is sent into a socket the container is not reading;
// the leg only fails, and only then enqueues its replay payload, when that timeout fires.
// Unpausing sooner lets Scylla read the buffered request, the leg succeeds, and replay never runs.
// The hold is the outage itself, not a wait for state:
// the enqueue count asserted right after it is what proves every leg failed.
const outageHold = 3 * time.Second

// convergenceTimeout bounds the wait for the replay drain to finish.
const convergenceTimeout = 30 * time.Second

// replayHarness is one driver's client, replay worker and counters for the
// overwrite-convergence scenarios.
type replayHarness struct {
	client *helix.CQLClient
	adw    *policy.AdaptiveDualWrite
	mc     *testutil.TestMetricsCollector
	worker *replay.Worker

	success      atomic.Int64
	dropped      atomic.Int64
	target       int64
	firstSuccess chan struct{}
	allSucceeded chan struct{}

	// attempts records how long each replay attempt took and whether it failed.
	attemptsMu sync.Mutex
	attempts   []attemptSample
}

// attemptSample is one replay attempt's duration and outcome.
type attemptSample struct {
	took   time.Duration
	failed bool
}

// TestS_OverwriteConvergence_ReplayCarriesTimestamp writes every key twice
// while B is paused, so B receives both versions only through replay, and
// checks that B converges on the second version with the same WRITETIME as
// A. Equal WRITETIME proves the replay carried the client timestamp of the
// original write instead of taking a new one.
//
// Sequence:
//  1. Pause B. Write each key twice (v1, then v2). The first write's B leg fails
//     synchronously and degrades B; the rest run as background legs.
//  2. Hold the outage past the driver timeout, then require exactly one
//     enqueued payload per write, so every write reaches B through replay.
//  3. Unpause B and wait until the worker has replayed every payload.
//  4. Read every key on A and B directly: v2 on both, equal WRITETIME.
func TestS_OverwriteConvergence_ReplayCarriesTimestamp(t *testing.T) {
	a, b := sharedClusters(t)
	// Registered after the table so the restore runs first and the
	// truncate reaches a reachable B.
	table := createKVTableOnBoth(t, "overwrite_conv")
	withRestoredCluster(t, b)

	const keys = 25
	const writes = 2 * keys
	insert := "INSERT INTO " + table + " (key, value) VALUES (?, ?)"

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			h := newReplayHarness(t, d, a, b, writes)
			ctx := context.Background()

			pauseB(t, b)
			for i := range keys {
				key := fmt.Sprintf("%s_k%03d", d.name, i)
				for _, v := range []string{"v1", "v2"} {
					requireWriteAccepted(t, d.name, key, h.client.Query(insert, key, v).Exec())
				}
			}
			// The outage itself; see outageHold.
			time.Sleep(outageHold)
			require.Equal(t, int64(writes), h.mc.GetReplayEnqueued(htypes.ClusterB),
				"[%s] every write's B leg must fail and enqueue before B returns", d.name)
			require.Zero(t, h.mc.GetReplayEnqueued(htypes.ClusterA),
				"[%s] A was reachable throughout", d.name)

			require.NoError(t, b.Unpause(ctx))
			waitForReconnect(t, b, d.name)
			h.waitAllSucceeded(t, d.name)
			h.stopAndRequireSettled(t, d.name, writes)

			for i := range keys {
				key := fmt.Sprintf("%s_k%03d", d.name, i)
				requireConverged(t, d.name, a, b, table, key, "v2")
			}
		})
	}
}

// TestS_OverwriteConvergence_PauseMidDrain pauses B a second time while the
// replay drain is under way and checks that nothing is dropped and every
// key still converges once B returns.
//
// The inline first attempt of each payload holds the worker's dequeue loop
// for up to the driver timeout while B is paused.
// The test logs how long the failed attempts took; it does not assert on timing.
func TestS_OverwriteConvergence_PauseMidDrain(t *testing.T) {
	a, b := sharedClusters(t)
	table := createKVTableOnBoth(t, "overwrite_middrain")
	withRestoredCluster(t, b)

	const keys = 100
	insert := "INSERT INTO " + table + " (key, value) VALUES (?, ?)"

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			h := newReplayHarness(t, d, a, b, keys)
			ctx := context.Background()

			pauseB(t, b)
			for i := range keys {
				key := fmt.Sprintf("%s_k%03d", d.name, i)
				requireWriteAccepted(t, d.name, key, h.client.Query(insert, key, "v").Exec())
			}
			// The outage itself; see outageHold.
			time.Sleep(outageHold)
			enqueued := h.mc.GetReplayEnqueued(htypes.ClusterB)
			require.Equal(t, int64(keys), enqueued,
				"[%s] every write's B leg must fail and enqueue before B returns", d.name)

			require.NoError(t, b.Unpause(ctx))
			select {
			case <-h.firstSuccess:
			case <-time.After(convergenceTimeout):
				t.Fatalf("[%s] no replay succeeded within %s of the first unpause", d.name, convergenceTimeout)
			}

			// The second outage must start while payloads are still
			// waiting; otherwise this is the plain convergence test again.
			pauseB(t, b)
			successAtPause := h.mc.GetReplaySuccess(htypes.ClusterB)
			require.Less(t, successAtPause, enqueued,
				"[%s] the drain finished before B was paused again", d.name)
			t.Logf("[%s] second pause at success=%d of %d", d.name, successAtPause, enqueued)

			// The second outage; see outageHold.
			h.resetAttempts()
			time.Sleep(outageHold)

			require.NoError(t, b.Unpause(ctx))
			waitForReconnect(t, b, d.name)
			h.waitAllSucceeded(t, d.name)
			h.logAttempts(t, d.name)
			h.stopAndRequireSettled(t, d.name, keys)

			for i := range keys {
				key := fmt.Sprintf("%s_k%03d", d.name, i)
				requireConverged(t, d.name, a, b, table, key, "v")
			}
		})
	}
}

// newReplayHarness builds a client whose AdaptiveDualWrite degrades B on
// its first failed write, and a memory replay worker that reports to the
// same metrics collector. target is the number of successful replays that
// completes the drain.
func newReplayHarness(
	t *testing.T,
	d driverCase,
	a, b *testutil.CQLCluster,
	target int64,
) *replayHarness {
	t.Helper()
	ensureReachable(t, a, d)
	ensureReachable(t, b, d)

	h := &replayHarness{
		adw: policy.NewAdaptiveDualWrite(
			policy.WithAdaptiveStrikeThreshold(1),
		),
		mc:           testutil.NewTestMetricsCollector(),
		target:       target,
		firstSuccess: make(chan struct{}),
		allSucceeded: make(chan struct{}),
	}
	memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(1000))

	client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
		helix.WithWriteStrategy(h.adw),
		helix.WithReadStrategy(policy.NewStickyRead()),
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
		helix.WithReplayer(memReplayer),
		helix.WithMetrics(h.mc),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	h.client = client

	execute := client.DefaultExecuteFunc()
	h.worker = replay.NewMemoryWorker(memReplayer,
		func(ctx context.Context, p htypes.ReplayPayload) error {
			start := time.Now()
			err := execute(ctx, p)
			h.recordAttempt(attemptSample{took: time.Since(start), failed: err != nil})

			return err
		},
		replay.WithWorkerMetrics(h.mc),
		// Short backoff so payloads that failed during an outage are
		// retried promptly once B returns.
		replay.WithRetryDelay(50*time.Millisecond),
		replay.WithMaxRetryDelay(500*time.Millisecond),
		replay.WithOnSuccess(func(_ htypes.ReplayPayload) {
			switch h.success.Add(1) {
			case 1:
				close(h.firstSuccess)
			case h.target:
				close(h.allSucceeded)
			}
		}),
		replay.WithOnDrop(func(_ htypes.ReplayPayload, _ error) {
			h.dropped.Add(1)
		}),
	)
	require.NoError(t, h.worker.Start())
	t.Cleanup(h.worker.Stop)

	return h
}

// pauseB pauses cluster B and registers an unpause, so a failing assertion
// never leaves B paused for the next subtest.
func pauseB(t *testing.T, b *testutil.CQLCluster) {
	t.Helper()
	require.NoError(t, b.Pause(context.Background()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = b.Unpause(ctx)
	})
}

// requireWriteAccepted fails the test unless the write was acknowledged,
// either by both clusters or by A with B's leg still running.
func requireWriteAccepted(t *testing.T, driver, key string, err error) {
	t.Helper()
	if err == nil || errors.Is(err, htypes.ErrWriteAsync) {
		return
	}
	var dual *htypes.DualClusterError
	require.False(t, errors.As(err, &dual), "[%s] write %s: both clusters failed: %v", driver, key, err)
	require.NotErrorIs(t, err, htypes.ErrWriteDropped, "[%s] write %s", driver, key)
	require.NoError(t, err, "[%s] write %s", driver, key)
}

// requireConverged reads key directly from both clusters and requires the
// expected value on each and the same WRITETIME on both.
func requireConverged(t *testing.T, driver string, a, b *testutil.CQLCluster, table, key, want string) {
	t.Helper()
	valueA, writeTimeA := readValueAndWriteTime(t, a, table, key)
	valueB, writeTimeB := readValueAndWriteTime(t, b, table, key)
	require.Equal(t, want, valueA, "[%s] %s on A", driver, key)
	require.Equal(t, want, valueB, "[%s] %s on B", driver, key)
	require.Equal(t, writeTimeA, writeTimeB,
		"[%s] %s: WRITETIME differs, so replay did not carry the original timestamp", driver, key)
}

func readValueAndWriteTime(t *testing.T, c *testutil.CQLCluster, table, key string) (value string, writeTime int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := c.Session.Query("SELECT value, WRITETIME(value) FROM "+table+" WHERE key = ?", key).
		WithContext(ctx).Scan(&value, &writeTime)
	require.NoError(t, err, "read %s from %s", key, c.Host)

	return value, writeTime
}

// waitAllSucceeded blocks until the worker has replayed target payloads.
func (h *replayHarness) waitAllSucceeded(t *testing.T, driver string) {
	t.Helper()
	select {
	case <-h.allSucceeded:
	case <-time.After(convergenceTimeout):
		t.Fatalf("[%s] replay did not finish within %s: success=%d of %d, dropped=%d",
			driver, convergenceTimeout, h.success.Load(), h.target, h.dropped.Load())
	}
}

// stopAndRequireSettled stops the worker and the client, so no counter can
// move any more, then requires that every enqueued payload was replayed
// exactly once and nothing was dropped.
func (h *replayHarness) stopAndRequireSettled(t *testing.T, driver string, writes int64) {
	t.Helper()
	h.worker.Stop()
	h.client.Close()

	require.Equal(t, writes, h.mc.GetReplayEnqueued(htypes.ClusterB), "[%s] enqueued(B)", driver)
	require.Equal(t, writes, h.mc.GetReplaySuccess(htypes.ClusterB), "[%s] success(B)", driver)
	require.Equal(t, writes, h.success.Load(), "[%s] OnSuccess calls", driver)
	require.Zero(t, h.dropped.Load(), "[%s] OnDrop calls", driver)
	require.Zero(t, h.mc.GetReplayDropped(htypes.ClusterB), "[%s] dropped(B)", driver)
}

func (h *replayHarness) recordAttempt(a attemptSample) {
	h.attemptsMu.Lock()
	defer h.attemptsMu.Unlock()
	h.attempts = append(h.attempts, a)
}

func (h *replayHarness) resetAttempts() {
	h.attemptsMu.Lock()
	defer h.attemptsMu.Unlock()
	h.attempts = nil
}

// logAttempts logs the replay attempts that finished since the last reset:
// how many failed, how long the failed ones took, and the longest attempt.
// Informational only.
func (h *replayHarness) logAttempts(t *testing.T, driver string) {
	t.Helper()
	h.attemptsMu.Lock()
	defer h.attemptsMu.Unlock()
	var failed int
	var failedTotal, longest time.Duration
	for _, a := range h.attempts {
		longest = max(longest, a.took)
		if a.failed {
			failed++
			failedTotal += a.took
		}
	}
	var failedMean time.Duration
	if failed > 0 {
		failedMean = failedTotal / time.Duration(failed)
	}
	t.Logf("[%s] since the second pause: %d attempts, %d failed (mean %s), longest attempt %s",
		driver, len(h.attempts), failed, failedMean, longest)
}
