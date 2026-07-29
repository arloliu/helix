package helix_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/topology"
	"github.com/arloliu/helix/types"
)

// eventRecorder collects delivered events; waitFor blocks (with timeout)
// until a matching event arrives. Event-driven — no polling sleeps.
type eventRecorder struct {
	mu     sync.Mutex
	events []types.ClusterEvent
	notify chan struct{}
}

func newEventRecorder() *eventRecorder {
	return &eventRecorder{notify: make(chan struct{}, 1)}
}

func (r *eventRecorder) handler(ev types.ClusterEvent) {
	r.mu.Lock()
	r.events = append(r.events, ev)
	r.mu.Unlock()
	select {
	case r.notify <- struct{}{}:
	default:
	}
}

func (r *eventRecorder) waitFor(t *testing.T, pred func(types.ClusterEvent) bool) types.ClusterEvent {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		r.mu.Lock()
		for _, ev := range r.events {
			if pred(ev) {
				r.mu.Unlock()

				return ev
			}
		}
		r.mu.Unlock()
		select {
		case <-r.notify:
		case <-deadline:
			t.Fatal("timeout waiting for matching cluster event")
		}
	}
}

func TestClusterEvents_FailoverEmitted(t *testing.T) {
	failErr := errors.New("cluster A down")
	rec := newEventRecorder()

	// Pin the initial read cluster to A: NewStickyRead() with no preference
	// picks its starting cluster at random, which would make the assertion
	// on the failover direction flaky.
	client, err := helix.NewCQLClient(
		newAlwaysFailSession(failErr), newAlwaysOKSession(),
		helix.WithWriteStrategy(policy.NewSyncDualWrite()),
		helix.WithReadStrategy(policy.NewStickyRead(policy.WithPreferredCluster(types.ClusterA))),
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
		helix.WithOnClusterEvent(rec.handler),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	var v string
	_ = client.Query("SELECT v FROM t WHERE k = ?", 1).ScanContext(t.Context(), &v)

	ev := rec.waitFor(t, func(ev types.ClusterEvent) bool {
		return ev.Kind == types.EventFailover
	})
	require.Equal(t, types.ClusterA, ev.FromCluster)
	require.Equal(t, types.ClusterB, ev.ToCluster)
	require.Equal(t, types.ClusterB, ev.Cluster)
	require.ErrorIs(t, ev.Err, failErr)
	require.False(t, ev.Timestamp.IsZero())
}

func TestClusterEvents_DrainEnterAndExitEmitted(t *testing.T) {
	rec := newEventRecorder()
	watcher := topology.NewLocal()

	client, err := helix.NewCQLClient(
		newAlwaysOKSession(), newAlwaysOKSession(),
		helix.WithWriteStrategy(policy.NewSyncDualWrite()),
		helix.WithReadStrategy(policy.NewStickyRead()),
		helix.WithTopologyWatcher(watcher),
		helix.WithOnClusterEvent(rec.handler),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.NoError(t, watcher.SetDrain(t.Context(), types.ClusterB, true, "maintenance"))
	entered := rec.waitFor(t, func(ev types.ClusterEvent) bool {
		return ev.Kind == types.EventDrainEntered
	})
	require.Equal(t, types.ClusterB, entered.Cluster)

	require.NoError(t, watcher.SetDrain(t.Context(), types.ClusterB, false, "done"))
	exited := rec.waitFor(t, func(ev types.ClusterEvent) bool {
		return ev.Kind == types.EventDrainExited
	})
	require.Equal(t, types.ClusterB, exited.Cluster)
}

func TestClusterEvents_ReplayDroppedEmitted(t *testing.T) {
	rec := newEventRecorder()
	memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(1))

	client, err := helix.NewCQLClient(
		newAlwaysFailSession(errors.New("cluster A down")), newAlwaysOKSession(),
		helix.WithWriteStrategy(policy.NewSyncDualWrite()),
		helix.WithReadStrategy(policy.NewStickyRead()),
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
		helix.WithReplayer(memReplayer),
		helix.WithOnClusterEvent(rec.handler),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	for i := range 10 {
		_ = client.Query("INSERT INTO t (k, v) VALUES (?, ?)", i, "v").ExecContext(t.Context())
	}

	ev := rec.waitFor(t, func(ev types.ClusterEvent) bool {
		return ev.Kind == types.EventReplayDropped
	})
	require.Equal(t, types.ClusterA, ev.Cluster)
	require.Error(t, ev.Err)
}
