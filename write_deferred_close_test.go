package helix

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// manualDeferredError is a DeferredWriteResult completed by the test.
type manualDeferredError struct {
	mu   sync.Mutex
	done bool
	err  error
	fn   func(error)
}

func (d *manualDeferredError) Error() string        { return types.ErrWriteAsync.Error() }
func (d *manualDeferredError) Is(target error) bool { return target == types.ErrWriteAsync }

func (d *manualDeferredError) OnComplete(fn func(error)) {
	d.mu.Lock()
	if d.done {
		d.mu.Unlock()
		fn(d.err)

		return
	}
	d.fn = fn
	d.mu.Unlock()
}

func (d *manualDeferredError) complete(err error) {
	d.mu.Lock()
	d.done, d.err = true, err
	fn := d.fn
	d.mu.Unlock()
	if fn != nil {
		fn(err)
	}
}

// deferredStrategy acknowledges cluster A synchronously and reports
// cluster B through a deferred result.
type deferredStrategy struct {
	result *manualDeferredError
}

func (s *deferredStrategy) Execute(ctx context.Context, writeA, _ func(context.Context) error) (errA, errB error) {
	return writeA(ctx), s.result
}

// TestClose_WaitsForDeferredLegReplay asserts that Close waits for a
// background leg to complete so its failure is enqueued for replay before
// the replay worker stops.
func TestClose_WaitsForDeferredLegReplay(t *testing.T) {
	deferred := &manualDeferredError{}
	replayer := &mockReplayer{}
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithWriteStrategy(&deferredStrategy{result: deferred}),
		WithReplayer(replayer),
	)
	require.NoError(t, err)

	require.NoError(t, client.Query("INSERT INTO t (id) VALUES (1)").Exec())
	replayer.Lock()
	require.Empty(t, replayer.payloads, "a deferred leg is not enqueued before it completes")
	replayer.Unlock()

	closed := make(chan struct{})
	go func() {
		client.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close must wait for the deferred leg")
	case <-time.After(50 * time.Millisecond):
	}

	deferred.complete(errors.New("background failure"))
	select {
	case <-closed:
	case <-time.After(regressionWaitTimeout):
		t.Fatal("Close must return once the deferred leg completed")
	}

	replayer.Lock()
	defer replayer.Unlock()
	require.Len(t, replayer.payloads, 1, "the failed leg is enqueued before Close returns")
	require.Equal(t, ClusterB, replayer.payloads[0].TargetCluster)
}

// blockingWorker is a replay worker whose Stop blocks until released.
type blockingWorker struct {
	entered chan struct{}
	release chan struct{}
}

func (w *blockingWorker) Start() error    { return nil }
func (w *blockingWorker) IsRunning() bool { return true }

func (w *blockingWorker) Stop() {
	close(w.entered)
	<-w.release
}

// TestClose_ConcurrentCallerWaitsForShutdown asserts that a second Close
// returns only after the first one finished shutting the client down.
func TestClose_ConcurrentCallerWaitsForShutdown(t *testing.T) {
	worker := &blockingWorker{entered: make(chan struct{}), release: make(chan struct{})}
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithReplayer(&mockReplayer{}),
		WithReplayWorker(worker),
	)
	require.NoError(t, err)

	first := make(chan struct{})
	go func() {
		client.Close()
		close(first)
	}()
	<-worker.entered

	second := make(chan struct{})
	go func() {
		client.Close()
		close(second)
	}()
	select {
	case <-second:
		t.Fatal("a concurrent Close must not return while shutdown is in progress")
	case <-time.After(50 * time.Millisecond):
	}

	close(worker.release)
	for _, done := range []chan struct{}{first, second} {
		select {
		case <-done:
		case <-time.After(regressionWaitTimeout):
			t.Fatal("Close must return once shutdown finished")
		}
	}
}

// gatedStrategy blocks inside Execute until released, then reports cluster
// B through a deferred result.
type gatedStrategy struct {
	entered chan struct{}
	release chan struct{}
	result  *manualDeferredError
}

func (s *gatedStrategy) Execute(ctx context.Context, writeA, _ func(context.Context) error) (errA, errB error) {
	close(s.entered)
	<-s.release

	return writeA(ctx), s.result
}

// TestClose_WaitsForWriteInProgressToHandOffItsLegs asserts that Close
// begun while a write is still inside the strategy waits for that write to
// register its deferred leg, so the leg's failure is still enqueued.
func TestClose_WaitsForWriteInProgressToHandOffItsLegs(t *testing.T) {
	deferred := &manualDeferredError{}
	strategy := &gatedStrategy{entered: make(chan struct{}), release: make(chan struct{}), result: deferred}
	replayer := &mockReplayer{}
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithWriteStrategy(strategy),
		WithReplayer(replayer),
	)
	require.NoError(t, err)

	written := make(chan error, 1)
	go func() { written <- client.Query("INSERT INTO t (id) VALUES (1)").Exec() }()
	<-strategy.entered

	closed := make(chan struct{})
	go func() {
		client.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close must wait for the write in progress")
	case <-time.After(50 * time.Millisecond):
	}

	close(strategy.release)
	require.NoError(t, <-written)
	select {
	case <-closed:
		t.Fatal("Close must wait for the deferred leg the write handed off")
	case <-time.After(50 * time.Millisecond):
	}

	deferred.complete(errors.New("background failure"))
	select {
	case <-closed:
	case <-time.After(regressionWaitTimeout):
		t.Fatal("Close must return once the deferred leg completed")
	}

	replayer.Lock()
	defer replayer.Unlock()
	require.Len(t, replayer.payloads, 1, "the failed leg is enqueued before Close returns")
}

// failingReplayer rejects every enqueue.
type failingReplayer struct{ err error }

func (r *failingReplayer) Enqueue(context.Context, types.ReplayPayload) error { return r.err }

// bothDeferredStrategy reports both clusters through deferred results.
type bothDeferredStrategy struct {
	a, b *manualDeferredError
}

func (s *bothDeferredStrategy) Execute(context.Context, func(context.Context) error, func(context.Context) error) (errA, errB error) {
	return s.a, s.b
}

// TestWrite_DeferredLegCompletedBeforeRegistrationIsSynchronous asserts
// that a background leg which has already failed by the time the client
// registers for its result is treated like a synchronous failure: its
// replay admission error reaches the caller instead of being reported
// only through the drop callback.
func TestWrite_DeferredLegCompletedBeforeRegistrationIsSynchronous(t *testing.T) {
	errFull := errors.New("replay queue full")
	a, b := &manualDeferredError{}, &manualDeferredError{}
	a.complete(errors.New("cluster A rejected the background write"))
	b.complete(errors.New("cluster B rejected the background write"))
	var drops int
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithWriteStrategy(&bothDeferredStrategy{a: a, b: b}),
		WithReplayer(&failingReplayer{err: errFull}),
		WithAckMode(AckOnReplayAdmission),
		WithOnReplayDropped(func(types.ReplayPayload, error) { drops++ }),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	err = client.Query("INSERT INTO t (id) VALUES (1)").Exec()
	require.ErrorIs(t, err, types.ErrNoSynchronousAck, "a rejected admission before the call returns is never nil")
	var noAck *types.NoSynchronousAckError
	require.ErrorAs(t, err, &noAck)
	require.ErrorIs(t, noAck.Replay, errFull)
	require.Equal(t, 2, drops, "both rejected legs are reported")
}

// TestClose_WaitsForDeferredDropHandler asserts that Close returns only
// after the replay-dropped handler for a background leg has run.
func TestClose_WaitsForDeferredDropHandler(t *testing.T) {
	deferred := &manualDeferredError{}
	entered, release := make(chan struct{}), make(chan struct{})
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithWriteStrategy(&deferredStrategy{result: deferred}),
		WithReplayer(&failingReplayer{err: errors.New("replay queue full")}),
		WithOnReplayDropped(func(types.ReplayPayload, error) {
			close(entered)
			<-release
		}),
	)
	require.NoError(t, err)

	require.NoError(t, client.Query("INSERT INTO t (id) VALUES (1)").Exec())
	go deferred.complete(errors.New("background failure"))
	<-entered

	closed := make(chan struct{})
	go func() {
		client.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close must wait for the drop handler")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case <-closed:
	case <-time.After(regressionWaitTimeout):
		t.Fatal("Close must return once the drop handler finished")
	}
}
