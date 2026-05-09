//go:build e2e

package cql_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/testutil"
	htypes "github.com/arloliu/helix/types"
)

// TestS1_PauseA_WriteWithReplayDrain probes the write-path failover and
// replay drain after a real cluster outage. The chaos suite already
// exercises this conceptually, but only with synthetic errors above the
// driver — here we drive the gocql native timeout path AND verify the
// replay queue actually drains after the cluster comes back.
//
// Sequence:
//  1. Pause(A). Drive a burst of writes. Some should land on B and queue
//     for replay to A; some may return ErrWriteAsync.
//  2. Confirm AdaptiveDualWrite marks A degraded.
//  3. Confirm the replay queue is non-empty.
//  4. Unpause(A). Wait for the worker to drain the queue.
//  5. Verify both clusters converge on the same set of keys.
func TestS1_PauseA_WriteWithReplayDrain(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, a)

	// S1 runs alphabetically last in the e2e suite (write_replay_test.go).
	// By then both shared gocql sessions have been through dozens of
	// Pause/Unpause/Stop/Kill cycles in earlier tests, which can leave the
	// driver-side host pool, control-conn state, or backoff timers in
	// configurations that change AdaptiveDualWrite's degradation cadence
	// or the per-attempt timeout shape under PauseA. The test passes in
	// isolation but flakes under sustained suite pressure (rows on A
	// undercount because writes route through replay later than expected
	// and the worker drops them under MaxAttempts retry exhaustion).
	//
	// Rebuild the gocql sessions before the test runs so the harness
	// starts from a known-clean state regardless of preceding tests.
	resetCtx, resetCancel := context.WithTimeout(context.Background(), 30*time.Second)
	require.NoError(t, a.Reconnect(resetCtx))
	require.NoError(t, b.Reconnect(resetCtx))
	resetCancel()

	table := createKVTableOnBoth(t, "s1_write_replay")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			adw := policy.NewAdaptiveDualWrite(
				policy.WithAdaptiveStrikeThreshold(2),
				policy.WithAdaptiveDeltaThreshold(100*time.Millisecond),
			)
			memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(1000))
			mc := testutil.NewTestMetricsCollector()

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithWriteStrategy(adw),
				helix.WithReadStrategy(policy.NewStickyRead()),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
				helix.WithReplayer(memReplayer),
				helix.WithMetrics(mc),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			// Worker MaxAttempts must outlast the actual realized outage,
			// not the comment's "~5s." Under sustained suite pressure
			// AdaptiveDualWrite is observed to NOT always degrade (test
			// log shows async=0), so each Exec runs sync with a ~2s
			// gocql timeout against paused A, stretching the loop's
			// wall-clock to ~200s. The default MaxAttempts=5 is
			// catastrophically short here; even MaxAttempts=100 with the
			// default exponential-backoff schedule sits right at the
			// edge. 1000 gives a 10× margin so payloads survive any
			// reasonable scheduling jitter from prior tests. S11 covers
			// the deliberate-overflow conservation law; S1 is the
			// happy-path drain.
			var (
				workerSuccess atomic.Int32
				workerDropped atomic.Int32
				workerError   atomic.Int32
			)
			worker := replay.NewMemoryWorker(memReplayer, client.DefaultExecuteFunc(),
				replay.WithMaxAttempts(1000),
				replay.WithRetryDelay(50*time.Millisecond),
				replay.WithOnSuccess(func(_ htypes.ReplayPayload) {
					workerSuccess.Add(1)
				}),
				replay.WithOnError(func(_ htypes.ReplayPayload, _ error, _ int) {
					workerError.Add(1)
				}),
				replay.WithOnDrop(func(_ htypes.ReplayPayload, _ error) {
					workerDropped.Add(1)
				}),
			)
			require.NoError(t, worker.Start())
			t.Cleanup(worker.Stop)
			client.Config().ReplayWorker = worker

			ctx := context.Background()
			require.NoError(t, a.Pause(ctx))

			// Drive 100 writes at 50ms intervals (~5s of activity). Slow
			// enough that the replay queue (capacity 1000) cannot overflow,
			// fast enough to surface degradation. If we drove writes flat-out,
			// the replayer's drain rate vs the write rate would dominate the
			// post-Unpause result and mask the question we're actually asking.
			var (
				written     atomic.Int32
				asyncCount  atomic.Int32
				droppedCnt  atomic.Int32
				dualErrCnt  atomic.Int32
				otherErrCnt atomic.Int32
			)
			const totalWrites = 100
			for i := 0; i < totalWrites; i++ {
				key := fmt.Sprintf("k%05d", i)
				err := client.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
					key, "v").Exec()
				switch {
				case err == nil:
					written.Add(1)
				case errors.Is(err, htypes.ErrWriteAsync):
					written.Add(1)
					asyncCount.Add(1)
				case errors.Is(err, htypes.ErrWriteDropped):
					droppedCnt.Add(1)
				default:
					var dual *htypes.DualClusterError
					if errors.As(err, &dual) {
						dualErrCnt.Add(1)
					} else {
						otherErrCnt.Add(1)
					}
				}
			}
			t.Logf("[%s] writes: ok+async=%d async=%d dropped=%d dualErr=%d other=%d",
				d.name, written.Load(), asyncCount.Load(), droppedCnt.Load(),
				dualErrCnt.Load(), otherErrCnt.Load())

			assert.True(t, adw.IsDegraded(htypes.ClusterA),
				"[%s] AdaptiveDualWrite must mark A degraded after a sustained outage", d.name)
			assert.Greater(t, memReplayer.Len(), 0,
				"[%s] replay queue should be non-empty while A is paused", d.name)

			// Unpause A and wait for the replay drain to converge.
			//
			// Convergence means: every write that the caller saw return
			// nil has landed on cluster A. Naively checking
			// `memReplayer.Len() == 0` is wrong — Len() reports queue
			// depth, but the worker dequeues a payload, attempts it,
			// and re-enqueues on failure with backoff. While a payload
			// is "in-flight" (between attempts), it sits inside the
			// worker's batch buffer, not in the queue, so Len()==0 can
			// be transiently true while work remains. Under sustained
			// suite pressure where ADW does not degrade and every Exec
			// runs sync against a paused A (~2s gocql timeout each),
			// the replay path accumulates ~80+ in-flight retries
			// behind the worker's single-batch processing — a Len()==0
			// observation lands while ~70 items are mid-retry, the
			// test exits early, and only ~28 of 100 rows ever reach A.
			//
			// Use the authoritative signal — A's row count — instead.
			require.NoError(t, a.Unpause(ctx))

			converged := waitFor(60*time.Second, 200*time.Millisecond, func() bool {
				return countRows(t, a, table) == int(written.Load())
			})
			assert.True(t, converged,
				"[%s] cluster A did not catch up to %d rows within 60s after Unpause "+
					"(A=%d B=%d replayQueueLen=%d)",
				d.name, written.Load(), countRows(t, a, table),
				countRows(t, b, table), memReplayer.Len())

			// Final consistency: count rows on each cluster, expect equal.
			countA := countRows(t, a, table)
			countB := countRows(t, b, table)
			t.Logf("[%s] worker callbacks: success=%d error=%d dropped=%d",
				d.name, workerSuccess.Load(), workerError.Load(), workerDropped.Load())
			t.Logf("[%s] post-drain row counts: A=%d B=%d", d.name, countA, countB)
			assert.Equal(t, countB, countA,
				"[%s] cluster A and B disagree on row count after replay drain", d.name)
		})
	}
}

func countRows(t *testing.T, c *testutil.CQLCluster, table string) int {
	t.Helper()
	var n int
	err := c.Session.Query("SELECT COUNT(*) FROM " + table).Scan(&n)
	if err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return n
}

func waitFor(timeout, interval time.Duration, condition func() bool) bool {
	if condition() {
		return true
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-deadline.C:
			return condition()
		case <-ticker.C:
		}
		if condition() {
			return true
		}
	}
}
