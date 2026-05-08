package mirror

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// recordingExecutor returns an [ExecuteFunc] that appends every observed
// payload (under a mutex) and an error returned for each.
type recordingExecutor struct {
	mu       sync.Mutex
	captured []types.ReplayPayload
	err      error
	delay    time.Duration
}

func (r *recordingExecutor) fn() ExecuteFunc {
	return func(_ context.Context, p types.ReplayPayload) error {
		if r.delay > 0 {
			time.Sleep(r.delay)
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		r.captured = append(r.captured, p)
		return r.err
	}
}

func (r *recordingExecutor) seen() []types.ReplayPayload {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]types.ReplayPayload, len(r.captured))
	copy(out, r.captured)
	return out
}

func waitForCount(t *testing.T, getCount func() int, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if getCount() >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for count >= %d, last=%d", want, getCount())
}

func TestEngineDefaults(t *testing.T) {
	rec := &recordingExecutor{}
	e := NewEngine(rec.fn())
	require.True(t, e.Enabled())
	require.Equal(t, 0, e.Stats().QueueDepth)
	e.Stop()
}

func TestEngineEnqueueAndExecute(t *testing.T) {
	rec := &recordingExecutor{}
	e := NewEngine(rec.fn(), WithWorkers(2))
	e.Start()
	defer e.Stop()

	for i := 0; i < 5; i++ {
		require.True(t, e.TryEnqueue(types.ReplayPayload{Query: "INSERT", Timestamp: int64(i)}))
	}

	waitForCount(t, func() int { return len(rec.seen()) }, 5)

	stats := e.Stats()
	require.Equal(t, uint64(5), stats.Enqueued)
	require.Equal(t, uint64(5), stats.Success)
	require.Equal(t, uint64(0), stats.Error)
	require.Equal(t, uint64(0), stats.Dropped)
}

func TestEngineQueueFullDrops(t *testing.T) {
	// Block the worker so the queue fills up.
	block := make(chan struct{})
	executed := atomic.Int32{}
	exec := func(_ context.Context, p types.ReplayPayload) error {
		executed.Add(1)
		<-block
		return nil
	}

	var dropped []types.ReplayPayload
	var dropMu sync.Mutex
	onDrop := func(p types.ReplayPayload) {
		dropMu.Lock()
		dropped = append(dropped, p)
		dropMu.Unlock()
	}

	e := NewEngine(exec, WithWorkers(1), WithQueueSize(2), WithOnDrop(onDrop))
	e.Start()

	// First enqueue starts running and blocks. The next two fill the queue.
	require.True(t, e.TryEnqueue(types.ReplayPayload{Query: "0"}))
	// Wait for the worker to claim it.
	for executed.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	require.True(t, e.TryEnqueue(types.ReplayPayload{Query: "1"}))
	require.True(t, e.TryEnqueue(types.ReplayPayload{Query: "2"}))
	// Queue is now full; this must drop.
	require.False(t, e.TryEnqueue(types.ReplayPayload{Query: "3"}))

	close(block)
	e.Stop()

	dropMu.Lock()
	defer dropMu.Unlock()
	require.Len(t, dropped, 1)
	require.Equal(t, "3", dropped[0].Query)
	require.Equal(t, uint64(1), e.Stats().Dropped)
}

func TestEngineDisabledDrops(t *testing.T) {
	rec := &recordingExecutor{}
	e := NewEngine(rec.fn(), WithEnabled(false))
	e.Start()
	defer e.Stop()

	require.False(t, e.TryEnqueue(types.ReplayPayload{Query: "x"}))
	require.Equal(t, uint64(1), e.Stats().Dropped)
	require.Equal(t, uint64(0), e.Stats().Enqueued)
	require.Empty(t, rec.seen())
}

func TestEngineEnableResumes(t *testing.T) {
	rec := &recordingExecutor{}
	e := NewEngine(rec.fn(), WithEnabled(false))
	e.Start()
	defer e.Stop()

	require.False(t, e.TryEnqueue(types.ReplayPayload{Query: "before"}))
	e.Enable()
	require.True(t, e.TryEnqueue(types.ReplayPayload{Query: "after"}))
	waitForCount(t, func() int { return len(rec.seen()) }, 1)
	require.Equal(t, "after", rec.seen()[0].Query)
}

func TestEngineDisableMidFlightDrainsQueue(t *testing.T) {
	rec := &recordingExecutor{delay: 5 * time.Millisecond}
	e := NewEngine(rec.fn(), WithWorkers(1), WithQueueSize(16))
	e.Start()
	defer e.Stop()

	for i := 0; i < 5; i++ {
		require.True(t, e.TryEnqueue(types.ReplayPayload{Query: "drain"}))
	}
	e.Disable()
	require.False(t, e.TryEnqueue(types.ReplayPayload{Query: "rejected"}))

	waitForCount(t, func() int { return len(rec.seen()) }, 5)
	require.Equal(t, uint64(5), e.Stats().Success)
}

func TestEngineStopDrainsQueue(t *testing.T) {
	rec := &recordingExecutor{}
	e := NewEngine(rec.fn(), WithWorkers(1), WithQueueSize(16))
	e.Start()

	for i := 0; i < 7; i++ {
		require.True(t, e.TryEnqueue(types.ReplayPayload{Query: "stop"}))
	}

	e.Stop()
	require.Equal(t, uint64(7), e.Stats().Success)
}

func TestEngineStopRejectsAfterStop(t *testing.T) {
	rec := &recordingExecutor{}
	e := NewEngine(rec.fn())
	e.Start()
	e.Stop()

	require.False(t, e.TryEnqueue(types.ReplayPayload{Query: "after stop"}))
	require.Equal(t, uint64(1), e.Stats().Dropped)
}

func TestEngineExecuteErrorIncrementsCounter(t *testing.T) {
	rec := &recordingExecutor{err: errors.New("boom")}
	e := NewEngine(rec.fn(), WithWorkers(1))
	e.Start()
	defer e.Stop()

	require.True(t, e.TryEnqueue(types.ReplayPayload{Query: "boom"}))
	waitForCount(t, func() int { return int(e.Stats().Error) }, 1)
	require.Equal(t, uint64(0), e.Stats().Success)
}

func TestEngineConcurrentEnqueue(t *testing.T) {
	rec := &recordingExecutor{}
	e := NewEngine(rec.fn(), WithWorkers(4), WithQueueSize(4096))
	e.Start()
	defer e.Stop()

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Go(func() {
			for i := 0; i < 100; i++ {
				e.TryEnqueue(types.ReplayPayload{Query: "concurrent"})
			}
		})
	}
	wg.Wait()

	waitForCount(t, func() int { return int(e.Stats().Success) }, 800)
}
