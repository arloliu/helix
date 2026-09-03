package replay_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
)

// evictionEvents records the cluster events a worker emits.
type evictionEvents struct {
	mu     sync.Mutex
	events []types.ClusterEvent
}

func (e *evictionEvents) EmitClusterEvent(ev types.ClusterEvent) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, ev)
}

func (e *evictionEvents) evicted() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	total := 0
	for _, ev := range e.events {
		if ev.Kind == types.EventReplayEvicted {
			total += ev.Count
		}
	}

	return total
}

// The eviction watch reports messages MaxAge removed while the worker was
// held back, and reports nothing while the worker acknowledges them.
func TestNATSWorker_EvictionWatchReportsExpiredMessages(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-evict"),
		replay.WithSubjectPrefix("test.evict"),
		replay.WithMaxAge(2*time.Second), // longer than the first poll, so the watch is seeded before they expire
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = replayer.Close() })

	mc := testutil.NewTestMetricsCollector()
	em := &evictionEvents{}
	gate := &gateSwitch{}
	worker := replay.NewNATSWorker(replayer,
		func(context.Context, types.ReplayPayload) error { return nil },
		replay.WithPollInterval(20*time.Millisecond),
		replay.WithWorkerMetrics(mc),
		replay.WithWorkerLogger(testutil.NewTestLogger(t)),
		replay.WithClusterGate(gate.allow),
		replay.WithEvictionWatch(),
	)
	require.True(t, worker.EvictionWatchEnabled())
	worker.SetEventEmitter(em)
	enqueueN(t, replayer, 3, types.ClusterA)
	startWorker(t, worker)

	// The messages expire behind the closed gate.
	require.Eventually(t, func() bool { return em.evicted() == 3 }, 10*time.Second, 50*time.Millisecond,
		"the expired messages are reported with their count")
	assert.EqualValues(t, 3, mc.GetReplayEvicted())

	// With the gate open the worker acknowledges every message before it
	// expires: no further eviction is reported.
	gate.open.Store(true)
	enqueueN(t, replayer, 3, types.ClusterA)
	require.Eventually(t, func() bool {
		pending, err := replayer.PendingByCluster(t.Context(), types.ClusterA)

		return err == nil && pending == 0
	}, 5*time.Second, 20*time.Millisecond)
	require.Never(t, func() bool { return em.evicted() != 3 }, 2500*time.Millisecond, 100*time.Millisecond,
		"acknowledged messages are not evictions")
}

func TestMemoryWorker_IgnoresTheEvictionWatch(t *testing.T) {
	worker := replay.NewMemoryWorker(replay.NewMemoryReplayer(),
		func(context.Context, types.ReplayPayload) error { return nil },
		replay.WithEvictionWatch(),
	)
	require.False(t, worker.EvictionWatchEnabled())
}
