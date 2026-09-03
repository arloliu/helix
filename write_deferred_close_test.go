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

// TestDeferredDropHandlerMayClose asserts that a replay-dropped handler
// invoked for a background leg can shut the client down without waiting on
// its own leg.
func TestDeferredDropHandlerMayClose(t *testing.T) {
	deferred := &manualDeferredError{}
	var client *CQLClient
	closed := make(chan struct{})
	var err error
	client, err = NewCQLClient(newMockSession(), newMockSession(),
		WithWriteStrategy(&deferredStrategy{result: deferred}),
		WithOnReplayDropped(func(types.ReplayPayload, error) {
			client.Close()
			close(closed)
		}),
	)
	require.NoError(t, err)

	require.NoError(t, client.Query("INSERT INTO t (id) VALUES (1)").Exec())
	deferred.complete(errors.New("background failure"))
	select {
	case <-closed:
	case <-time.After(regressionWaitTimeout):
		t.Fatal("the drop handler must be able to close the client")
	}
}
