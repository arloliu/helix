//go:build e2e

package cql_test

// TestS1b mirrors S1's structure but exercises the production replayer
// path: NATSReplayer + NATSWorker rather than MemoryReplayer + MemoryWorker.
// The semantics under test are the same — pause one cluster, AdaptiveDualWrite
// degrades, replays accumulate, unpause, replay drains, both clusters
// converge — but the durability backend is different and exposes different
// failure modes (publish timeouts, JetStream consumer state, msgpack
// roundtripping).

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

// TestS1b_PauseA_NATSReplayerDrain runs the same scenario as S1 but with
// NATSReplayer (the production durability backend) instead of MemoryReplayer.
// Verifies the entire NATS publish→consume→execute roundtrip lands replays
// on the recovered cluster.
func TestS1b_PauseA_NATSReplayerDrain(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, a)

	table := createKVTableOnBoth(t, "s1b_nats_drain")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			js := testutil.StartEmbeddedNATS(t)

			natsReplayer, err := replay.NewNATSReplayer(js,
				replay.WithStreamName("test-s1b-"+d.name),
				replay.WithSubjectPrefix("test.s1b."+d.name),
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = natsReplayer.Close() })

			adw := policy.NewAdaptiveDualWrite(
				policy.WithAdaptiveStrikeThreshold(2),
				policy.WithAdaptiveDeltaThreshold(100*time.Millisecond),
			)
			mc := testutil.NewTestMetricsCollector()

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithWriteStrategy(adw),
				helix.WithReadStrategy(policy.NewStickyRead()),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
				helix.WithReplayer(natsReplayer),
				helix.WithMetrics(mc),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			worker := replay.NewNATSWorker(natsReplayer, client.DefaultExecuteFunc(),
				replay.WithPollInterval(50*time.Millisecond),
				replay.WithRetryDelay(50*time.Millisecond),
				replay.WithMaxAttempts(20),
				replay.WithWorkerMetrics(mc),
			)
			require.NoError(t, worker.Start())
			t.Cleanup(worker.Stop)

			ctx := context.Background()
			require.NoError(t, a.Pause(ctx))

			// Drive 50 writes at 50ms intervals (~2.5s total).
			var (
				written     atomic.Int32
				asyncCount  atomic.Int32
				droppedCnt  atomic.Int32
				dualErrCnt  atomic.Int32
				otherErrCnt atomic.Int32
			)
			const totalWrites = 50
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
				"[%s] AdaptiveDualWrite must mark A degraded", d.name)

			// Unpause A and wait for B's row count to match A's via NATS replay.
			require.NoError(t, a.Unpause(ctx))
			waitForReconnect(t, a, d.name)

			drained := waitFor(60*time.Second, 250*time.Millisecond, func() bool {
				countA, err := tryCountRows(a, table)
				if err != nil {
					return false
				}
				countB, err := tryCountRows(b, table)
				return err == nil && countA == countB
			})
			assert.True(t, drained,
				"[%s] NATS replay did not converge cluster A and B within 60s (A=%d B=%d)",
				d.name, countRowsEventually(t, a, table), countRowsEventually(t, b, table))
		})
	}
}
