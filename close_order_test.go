package helix

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// closeRecorder collects the Close steps the test can observe, in the order
// they happen.
type closeRecorder struct {
	mu    sync.Mutex
	steps []string
}

func (r *closeRecorder) record(step string) {
	r.mu.Lock()
	r.steps = append(r.steps, step)
	r.mu.Unlock()
}

func (r *closeRecorder) recorded() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.steps...)
}

// closeOrderSession records its Close and runs onClose first, if set.
type closeOrderSession struct {
	*mockSession
	name    string
	rec     *closeRecorder
	onClose func()
}

func (s *closeOrderSession) Close() {
	if s.onClose != nil {
		s.onClose()
	}
	s.rec.record(s.name)
	s.mockSession.Close()
}

// closeOrderWorker records its Stop and runs onStop first.
type closeOrderWorker struct {
	rec    *closeRecorder
	onStop func()
}

func (w *closeOrderWorker) Start() error    { return nil }
func (w *closeOrderWorker) IsRunning() bool { return true }

func (w *closeOrderWorker) Stop() {
	w.onStop()
	w.rec.record("replay worker")
}

// closeOrderWatcher reports when the client cancels its topology context.
type closeOrderWatcher struct {
	cancelled chan struct{}
}

func (w *closeOrderWatcher) Watch(ctx context.Context) <-chan TopologyUpdate {
	context.AfterFunc(ctx, func() { close(w.cancelled) })

	return make(chan TopologyUpdate)
}

// degradedDeferredStrategy is a deferredStrategy that reports cluster A
// degraded, so the recovery probe runs against it.
type degradedDeferredStrategy struct {
	deferredStrategy
}

func (s *degradedDeferredStrategy) IsDegraded(cluster ClusterID) bool { return cluster == ClusterA }
func (s *degradedDeferredStrategy) RecordProbeSuccess(ClusterID)      {}

// loopsCancelled reports which background loops Close has cancelled.
func loopsCancelled(c *CQLClient) (topology, autoRefresh, probe bool) {
	return c.topologyCtx.Err() != nil, c.autoRefreshCtx.Err() != nil, c.recoveryProbeCtx.Err() != nil
}

// TestClose_StopsComponentsInOrder pins the order in which Close stops the
// client's background components relative to one another.
func TestClose_StopsComponentsInOrder(t *testing.T) {
	rec := &closeRecorder{}
	deferred := &manualDeferredError{}
	watcher := &closeOrderWatcher{cancelled: make(chan struct{})}
	probeEntered := make(chan struct{})
	var enterOnce sync.Once

	var client *CQLClient
	var atWorkerStop struct{ topology, autoRefresh, probe bool }
	worker := &closeOrderWorker{rec: rec, onStop: func() {
		atWorkerStop.topology, atWorkerStop.autoRefresh, atWorkerStop.probe = loopsCancelled(client)
	}}
	var dispatcherStoppedAtSessionClose bool
	sessionA := &closeOrderSession{mockSession: newMockSession(), name: "session A", rec: rec}
	sessionB := &closeOrderSession{mockSession: newMockSession(), name: "session B", rec: rec}

	client, err := NewCQLClient(sessionA, sessionB,
		WithWriteStrategy(&degradedDeferredStrategy{deferredStrategy{result: deferred}}),
		WithReplayer(&mockReplayer{}),
		WithReplayWorker(worker),
		WithTopologyWatcher(watcher),
		WithAutoRefresh(),
		WithSessionRefresher(func(context.Context, ClusterID, error) (cql.Session, error) {
			return nil, errors.New("refresh not expected")
		}),
		WithRecoveryProbe(RecoveryProbe{
			// Held until Close cancels it, so its return marks the probe's stop.
			Probe: func(ctx context.Context, _ cql.Session) error {
				enterOnce.Do(func() { close(probeEntered) })
				<-ctx.Done()
				rec.record("probe")

				return ctx.Err()
			},
			Interval: time.Millisecond,
			Timeout:  time.Hour,
		}),
		WithOnClusterEvent(func(types.ClusterEvent) {}),
	)
	require.NoError(t, err)
	sessionA.onClose = func() { dispatcherStoppedAtSessionClose = client.runtime.events.stopped.Load() }

	require.NoError(t, client.Query("INSERT INTO t (id) VALUES (1)").Exec())
	select {
	case <-probeEntered:
	case <-time.After(regressionWaitTimeout):
		t.Fatal("recovery probe did not start before Close")
	}

	closed := make(chan struct{})
	go func() {
		client.Close()
		close(closed)
	}()

	select {
	case <-watcher.cancelled:
	case <-time.After(regressionWaitTimeout):
		t.Fatal("Close did not cancel the topology watcher")
	}
	rec.record("deferred leg")
	deferred.complete(errors.New("background failure"))

	select {
	case <-closed:
	case <-time.After(regressionWaitTimeout):
		t.Fatal("Close did not return")
	}

	require.Equal(t,
		[]string{"deferred leg", "replay worker", "probe", "session A", "session B"},
		rec.recorded())
	require.True(t, atWorkerStop.topology, "topology watcher must stop before the replay worker")
	require.True(t, atWorkerStop.autoRefresh, "auto-refresh must stop before the replay worker")
	require.False(t, atWorkerStop.probe, "recovery probe still runs while the replay worker stops")
	require.True(t, dispatcherStoppedAtSessionClose, "event dispatcher must stop before sessions close")
}
