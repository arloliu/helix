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

// TestS11_ReplayOverflow_ConservationLaw probes the v1 finding logged
// in SPIKE_FINDINGS §6 under deliberate sustained-load overflow.
//
// Background: an earlier S1 run drove 4898 dual-writes with cluster A
// paused and a 1000-capacity replay queue. After Unpause, only ~600
// rows reached A. We rate-limited S1 to 100 writes to dodge this and
// the original SPIKE finding §6 was marked invalidated based on a
// SMALLER unit reproducer that asserted enqueued + dropped == writes.
//
// This test is the deliberate overflow probe: drive WAY more writes
// than the queue can hold, subscribe to BOTH drop hooks
// (OnReplayDropped on the client + OnDrop on the worker), and assert
// the full conservation law:
//
//	writes_accepted = (rows finally on A) + (drops observed)
//
// If the law holds, finding §6 is fully invalidated and the
// instrumentation accounts for everything. If it doesn't, there's a
// real correctness gap (writes silently lost) that needs fixing.
//
// Scylla-only because the test relies on Pause/Unpause semantics that
// work the same on both backends, but the suite already requires
// Scylla for AIO availability.
func TestS11_ReplayOverflow_ConservationLaw(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, a)

	table := createKVTableOnBoth(t, "s11_overflow")
	d := allDrivers[0] // v1 only

	ctx := context.Background()

	// SMALL queue capacity to force deliberate overflow. Default capacity
	// is 10000; with 1000 writes we'd not overflow at all.
	const queueCapacity = 50
	memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(queueCapacity))

	// Two distinct drop counters — the test needs to verify they're
	// both emitting non-zero counts as expected, AND that their sum
	// (after deduplication via the metric) accounts for everything.
	var (
		clientDrops atomic.Int64
		workerDrops atomic.Int64
	)

	mc := testutil.NewTestMetricsCollector()
	client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
		helix.WithReadStrategy(policy.NewStickyRead()),
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
		helix.WithReplayer(memReplayer),
		helix.WithMetrics(mc),
		helix.WithOnReplayDropped(func(_ htypes.ReplayPayload, _ error) {
			clientDrops.Add(1)
		}),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	// IMPORTANT: pass the same MetricsCollector to the worker. Without
	// WithWorkerMetrics(mc), NewMemoryWorker falls back to its own
	// internal NopMetrics — every worker-side IncReplaySuccess /
	// IncReplayDropped / IncReplayError is silently discarded, leaving
	// the client's view of the metrics incomplete. This is a real
	// Helix usability gotcha (logged in tmp/session-swap-v2-plan.md
	// or similar). The OnDrop callback fires independently of the
	// metrics path so it's still visible without WithWorkerMetrics —
	// which is exactly the behavior that masks the issue: users who
	// only watch metrics see UNDERCOUNTED drops/successes; users who
	// only watch callbacks miss nothing.
	worker := replay.NewMemoryWorker(memReplayer, client.DefaultExecuteFunc(),
		replay.WithWorkerMetrics(mc),
		replay.WithOnDrop(func(_ htypes.ReplayPayload, _ error) {
			workerDrops.Add(1)
		}),
	)
	require.NoError(t, worker.Start())
	t.Cleanup(worker.Stop)

	// Phase 1: pause A. Now every dual-write returns errA=err, errB=nil.
	// The cql_client enqueues a replay payload for A on each write. With
	// a 50-capacity queue and many writes coming in fast, most will be
	// dropped at enqueue time (visible via OnReplayDropped + IncReplayDropped).
	require.NoError(t, a.Pause(ctx))

	// Drive a substantial burst — 1000 writes with no rate limiting.
	const totalWrites = 1000
	var (
		ok        atomic.Int32
		async     atomic.Int32
		dualErr   atomic.Int32
		otherErr  atomic.Int32
		dropError atomic.Int32
	)
	for i := 0; i < totalWrites; i++ {
		key := fmt.Sprintf("k%05d", i)
		err := client.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
			key, "v").ExecContext(ctx)
		var dualClusterErr *htypes.DualClusterError
		switch {
		case err == nil:
			ok.Add(1)
		case errors.Is(err, htypes.ErrWriteAsync):
			async.Add(1)
		case errors.Is(err, htypes.ErrWriteDropped):
			dropError.Add(1)
		case errors.As(err, &dualClusterErr):
			dualErr.Add(1)
		default:
			otherErr.Add(1)
		}
	}

	totalAccepted := ok.Load() + async.Load()
	t.Logf("[burst] writes accepted=%d (ok=%d async=%d) dropped=%d dualErr=%d other=%d",
		totalAccepted, ok.Load(), async.Load(),
		dropError.Load(), dualErr.Load(), otherErr.Load(),
	)

	// Most writes should have been "accepted" (B succeeded; A was
	// supposed to be enqueued for replay but the queue was tiny).
	require.Greater(t, totalAccepted, int32(0),
		"some writes must have been accepted via B")

	// Phase 2: unpause A and let the replay worker drain whatever it
	// has. The wait logic here is stricter than just "queue is empty"
	// because the queue can briefly hit Len=0 while the worker is
	// mid-backoff for a payload that will be re-enqueued — hiding
	// in-flight items from a naive check.
	require.NoError(t, a.Unpause(ctx))
	waitForReconnect(t, a, "s11-overflow")

	// Wait for the replay queue to drain.
	require.True(t, waitFor(60*time.Second, 200*time.Millisecond, func() bool {
		return memReplayer.Len() == 0
	}), "replay queue did not drain within 60s after Unpause")

	// Wait until the accounting metric STOPS MOVING. This catches the
	// case where Len()==0 is misleading because the worker has items
	// in its retry-backoff sleep that haven't been re-enqueued yet
	// (so they're not in the queue but also not yet counted as
	// success or drop). When Success+Error+Dropped stops growing, all
	// in-flight items have terminated.
	prevSettled := int64(-1)
	stableTicks := 0
	require.True(t, waitFor(30*time.Second, 200*time.Millisecond, func() bool {
		settled := mc.GetReplaySuccess(htypes.ClusterA) +
			mc.GetReplayDropped(htypes.ClusterA)
		if settled == prevSettled {
			stableTicks++
			return stableTicks >= 5 // ~1 second of no metric movement
		}
		prevSettled = settled
		stableTicks = 0

		return false
	}), "replay metrics did not stabilize within 30s after queue empty")

	// Phase 3: read final state. After the metric stabilization, we can
	// trust the accounting numbers reflect every in-flight item's
	// terminal disposition.
	rowsOnA := countRowsEventually(t, a, table)
	rowsOnB := countRowsEventually(t, b, table)

	enqueued := mc.GetReplayEnqueued(htypes.ClusterA)
	successes := mc.GetReplaySuccess(htypes.ClusterA)
	droppedMetric := mc.GetReplayDropped(htypes.ClusterA)
	cDropsCb := clientDrops.Load()
	wDropsCb := workerDrops.Load()

	t.Log("==== Replay accounting ====")
	t.Logf("writes accepted (caller-side):  %d", totalAccepted)
	t.Logf("rows on A (final):              %d", rowsOnA)
	t.Logf("rows on B (final):              %d", rowsOnB)
	t.Logf("metric: ReplayEnqueued(A):      %d", enqueued)
	t.Logf("metric: ReplaySuccess(A):       %d", successes)
	t.Logf("metric: ReplayDropped(A):       %d", droppedMetric)
	t.Logf("callback: OnReplayDropped (client side, enqueue failures):  %d", cDropsCb)
	t.Logf("callback: OnDrop          (worker side, processing drops):  %d", wDropsCb)

	// Verification gate 1: B has every accepted write (B was healthy
	// throughout, no replay needed for B).
	assert.EqualValues(t, totalAccepted, rowsOnB,
		"cluster B should have every accepted write (B was healthy)")

	// Verification gate 2: the metric and callbacks agree on total drops.
	// Without WithWorkerMetrics on the worker, the worker-side drops fire
	// OnDrop but DON'T increment IncReplayDropped — silently undercounting.
	// With WithWorkerMetrics (which we use), the metric reflects both
	// client and worker drops.
	assert.EqualValues(t, cDropsCb+wDropsCb, droppedMetric,
		"IncReplayDropped metric should equal sum of OnReplayDropped (client) + OnDrop (worker) callbacks")

	// Verification gate 3: client-side conservation. Every accepted
	// write was either enqueued for replay OR dropped at enqueue time.
	// Use cDropsCb (not droppedMetric) because droppedMetric includes
	// the worker-side drops which are SUBSET of the enqueued items
	// (re-enqueue-on-retry that hit a full queue).
	assert.EqualValues(t, totalAccepted, enqueued+cDropsCb,
		"client conservation: writes_accepted = enqueued + client_dropped (no silent loss at enqueue)")

	// Verification gate 4: worker-side conservation. Every enqueued
	// item terminates as either a successful apply or a worker-side
	// drop (re-enqueue-on-retry full).
	assert.EqualValues(t, enqueued, successes+wDropsCb,
		"worker conservation: enqueued = successes + worker_dropped (no silent loss in worker)")

	// Verification gate 5: data correctness. ReplaySuccess increments
	// for every applied replay, so rows_on_A reflects exactly that
	// (modulo ghost writes from coordinator timeouts, which are
	// idempotent and thus would still increment ReplaySuccess to the
	// same total).
	assert.EqualValues(t, successes, int64(rowsOnA),
		"rows_on_A = ReplaySuccess(A) — every applied replay creates exactly one row")

	// Verification gate 6: operator-facing summary. Every accepted
	// write that didn't reach A is visible via the (unified) drop
	// metric. There is no silent data loss on cluster A. This is the
	// invariant operators care about: count(writes that didn't make
	// it) is observable.
	//
	// IMPORTANT: this requires the worker to have been configured
	// with WithWorkerMetrics(mc) — the same metrics collector as the
	// client. Without it, worker drops/successes go to a separate
	// NopMetrics and the metric undercount silently. The OnDrop /
	// OnReplayDropped callbacks fire regardless, so a workload that
	// only watches callbacks won't be misled, but a workload that
	// only watches metrics will see undercounted drops.
	assert.EqualValues(t, totalAccepted, int64(rowsOnA)+droppedMetric,
		"operator view: writes_accepted = rows_on_A + observable_drops (unified metric)")
}
