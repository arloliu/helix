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

			worker := replay.NewMemoryWorker(memReplayer, client.DefaultExecuteFunc())
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
				time.Sleep(50 * time.Millisecond)
			}
			t.Logf("[%s] writes: ok+async=%d async=%d dropped=%d dualErr=%d other=%d",
				d.name, written.Load(), asyncCount.Load(), droppedCnt.Load(),
				dualErrCnt.Load(), otherErrCnt.Load())

			assert.True(t, adw.IsDegraded(htypes.ClusterA),
				"[%s] AdaptiveDualWrite must mark A degraded after a sustained outage", d.name)
			assert.Greater(t, memReplayer.Len(), 0,
				"[%s] replay queue should be non-empty while A is paused", d.name)

			// Unpause A and wait for the replay queue to drain.
			require.NoError(t, a.Unpause(ctx))

			drained := waitFor(20*time.Second, 200*time.Millisecond, func() bool {
				return memReplayer.Len() == 0
			})
			assert.True(t, drained,
				"[%s] replay queue did not drain within 20s after Unpause (len=%d)",
				d.name, memReplayer.Len())

			// Final consistency: count rows on each cluster, expect equal.
			countA := countRows(t, a, table)
			countB := countRows(t, b, table)
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
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(interval)
	}

	return condition()
}
