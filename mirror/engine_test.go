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

func TestNewEnginePanicsOnNilExecute(t *testing.T) {
	require.PanicsWithValue(t, "mirror: NewEngine requires a non-nil execute function", func() {
		NewEngine(nil)
	})
}

func TestEngineEnqueueAndExecute(t *testing.T) {
	rec := &recordingExecutor{}
	e := NewEngine(rec.fn(), WithWorkers(2))
	e.Start()
	defer e.Stop()

	for i := range 5 {
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

	for range 5 {
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

	for range 7 {
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

func TestEngineOnErrorFiresOnExecuteFailure(t *testing.T) {
	wantErr := errors.New("kaboom")
	rec := &recordingExecutor{err: wantErr}

	var (
		mu          sync.Mutex
		gotPayloads []types.ReplayPayload
		gotErrors   []error
	)
	onError := func(p types.ReplayPayload, err error) {
		mu.Lock()
		defer mu.Unlock()
		gotPayloads = append(gotPayloads, p)
		gotErrors = append(gotErrors, err)
	}

	e := NewEngine(rec.fn(), WithWorkers(1), WithOnError(onError))
	e.Start()
	defer e.Stop()

	require.True(t, e.TryEnqueue(types.ReplayPayload{Query: "stmt", Timestamp: 12345}))
	waitForCount(t, func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(gotPayloads)
	}, 1)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, "stmt", gotPayloads[0].Query)
	require.Equal(t, int64(12345), gotPayloads[0].Timestamp)
	require.Equal(t, wantErr, gotErrors[0])
}

func TestEngineOnErrorNotFiredOnSuccess(t *testing.T) {
	rec := &recordingExecutor{}
	called := atomic.Int32{}
	onError := func(types.ReplayPayload, error) {
		called.Add(1)
	}

	e := NewEngine(rec.fn(), WithWorkers(1), WithOnError(onError))
	e.Start()
	defer e.Stop()

	require.True(t, e.TryEnqueue(types.ReplayPayload{Query: "ok"}))
	waitForCount(t, func() int { return int(e.Stats().Success) }, 1)

	require.Equal(t, int32(0), called.Load())
}

type recordingMirrorMetrics struct {
	mu             sync.Mutex
	enqueueOK      int
	enqueueDropped int
	execOK         int
	execErr        int
	execObs        []float64
	queueDepth     int
	enabled        bool
}

func (r *recordingMirrorMetrics) IncMirrorEnqueueSuccess() {
	r.mu.Lock()
	r.enqueueOK++
	r.mu.Unlock()
}

func (r *recordingMirrorMetrics) IncMirrorEnqueueDropped() {
	r.mu.Lock()
	r.enqueueDropped++
	r.mu.Unlock()
}

func (r *recordingMirrorMetrics) IncMirrorExecSuccess() {
	r.mu.Lock()
	r.execOK++
	r.mu.Unlock()
}

func (r *recordingMirrorMetrics) IncMirrorExecError() {
	r.mu.Lock()
	r.execErr++
	r.mu.Unlock()
}

func (r *recordingMirrorMetrics) ObserveMirrorExecDuration(seconds float64) {
	r.mu.Lock()
	r.execObs = append(r.execObs, seconds)
	r.mu.Unlock()
}

func (r *recordingMirrorMetrics) SetMirrorQueueDepth(depth int) {
	r.mu.Lock()
	r.queueDepth = depth
	r.mu.Unlock()
}

func (r *recordingMirrorMetrics) SetMirrorEnabled(enabled bool) {
	r.mu.Lock()
	r.enabled = enabled
	r.mu.Unlock()
}

func (r *recordingMirrorMetrics) snapshot() recordingMirrorMetrics {
	r.mu.Lock()
	defer r.mu.Unlock()
	return recordingMirrorMetrics{
		enqueueOK:      r.enqueueOK,
		enqueueDropped: r.enqueueDropped,
		execOK:         r.execOK,
		execErr:        r.execErr,
		execObs:        append([]float64(nil), r.execObs...),
		queueDepth:     r.queueDepth,
		enabled:        r.enabled,
	}
}

func TestEngineMetricsOnSuccess(t *testing.T) {
	rec := &recordingExecutor{}
	m := &recordingMirrorMetrics{}
	e := NewEngine(rec.fn(), WithWorkers(1), WithMetrics(m))
	e.Start()
	defer e.Stop()

	require.True(t, e.TryEnqueue(types.ReplayPayload{Query: "ok"}))
	waitForCount(t, func() int { return int(e.Stats().Success) }, 1)

	snap := m.snapshot()
	require.Equal(t, 1, snap.enqueueOK)
	require.Equal(t, 0, snap.enqueueDropped)
	require.Equal(t, 1, snap.execOK)
	require.Equal(t, 0, snap.execErr)
	require.Len(t, snap.execObs, 1)
	require.True(t, snap.enabled)
}

func TestEngineMetricsOnError(t *testing.T) {
	rec := &recordingExecutor{err: errors.New("boom")}
	m := &recordingMirrorMetrics{}
	e := NewEngine(rec.fn(), WithWorkers(1), WithMetrics(m))
	e.Start()
	defer e.Stop()

	require.True(t, e.TryEnqueue(types.ReplayPayload{Query: "boom"}))
	waitForCount(t, func() int { return int(e.Stats().Error) }, 1)

	snap := m.snapshot()
	require.Equal(t, 1, snap.enqueueOK)
	require.Equal(t, 1, snap.execErr)
	require.Equal(t, 0, snap.execOK)
	require.Len(t, snap.execObs, 1, "duration recorded even on error")
}

func TestEngineMetricsOnDrop(t *testing.T) {
	rec := &recordingExecutor{}
	m := &recordingMirrorMetrics{}
	e := NewEngine(rec.fn(), WithEnabled(false), WithMetrics(m))
	e.Start()
	defer e.Stop()

	require.False(t, e.TryEnqueue(types.ReplayPayload{Query: "x"}))
	require.Equal(t, 1, m.snapshot().enqueueDropped)
}

func TestEngineMetricsEnableDisable(t *testing.T) {
	rec := &recordingExecutor{}
	m := &recordingMirrorMetrics{}
	e := NewEngine(rec.fn(), WithMetrics(m))
	e.Start()
	require.True(t, m.snapshot().enabled, "Start emits enabled=true")

	e.Disable()
	require.False(t, m.snapshot().enabled)

	e.Enable()
	require.True(t, m.snapshot().enabled)

	e.Stop()
	require.False(t, m.snapshot().enabled, "Stop emits enabled=false")
}

func TestEngineConcurrentEnqueue(t *testing.T) {
	rec := &recordingExecutor{}
	e := NewEngine(rec.fn(), WithWorkers(4), WithQueueSize(4096))
	e.Start()
	defer e.Stop()

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				e.TryEnqueue(types.ReplayPayload{Query: "concurrent"})
			}
		})
	}
	wg.Wait()

	waitForCount(t, func() int { return int(e.Stats().Success) }, 800)
}
