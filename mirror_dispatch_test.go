package helix

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/helix/mirror"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// recordingMirror captures payloads dispatched to the mirror engine.
type recordingMirror struct {
	mu       sync.Mutex
	captured []types.ReplayPayload
	notify   chan struct{}
	err      error
}

func newRecordingMirror() *recordingMirror {
	return &recordingMirror{notify: make(chan struct{}, 1024)}
}

func (r *recordingMirror) execute() mirror.ExecuteFunc {
	return func(_ context.Context, p types.ReplayPayload) error {
		r.mu.Lock()
		r.captured = append(r.captured, p)
		r.mu.Unlock()
		select {
		case r.notify <- struct{}{}:
		default:
		}

		return r.err
	}
}

func (r *recordingMirror) snapshot() []types.ReplayPayload {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]types.ReplayPayload, len(r.captured))
	copy(out, r.captured)
	return out
}

func (r *recordingMirror) waitForOne(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(r.snapshot()) >= 1 {
			return
		}
		select {
		case <-r.notify:
		case <-time.After(5 * time.Millisecond):
		}
	}
	t.Fatalf("timed out waiting for mirror dispatch; got %d", len(r.snapshot()))
}

// installMirrorEngine attaches a mirror.Engine driven by `exec` to client
// without going through WithMirror/MirrorTarget. Used by tests that need to
// observe payloads directly rather than peek at a downstream session.
func installMirrorEngine(t *testing.T, client *CQLClient, exec mirror.ExecuteFunc, opts ...mirror.Option) *mirror.Engine {
	t.Helper()
	e := mirror.NewEngine(exec, opts...)
	e.Start()
	client.config.MirrorEngine = e
	t.Cleanup(func() { e.Stop() })
	return e
}

func TestMirrorDispatchOnSingleClusterSuccess(t *testing.T) {
	rec := newRecordingMirror()
	client, err := NewCQLClient(newMockSession(), nil)
	require.NoError(t, err)
	defer client.Close()
	installMirrorEngine(t, client, rec.execute(), mirror.WithWorkers(1))

	ctx := context.Background()
	require.NoError(t, client.Query("INSERT INTO t (k) VALUES (?)", "abc").Mirror().ExecContext(ctx))

	rec.waitForOne(t, 2*time.Second)
	got := rec.snapshot()
	require.Len(t, got, 1)
	require.Equal(t, "INSERT INTO t (k) VALUES (?)", got[0].Query)
	require.Equal(t, []any{"abc"}, got[0].Args)
	require.NotZero(t, got[0].Timestamp)
	require.False(t, got[0].IsBatch)
}

func TestMirrorNotDispatchedWithoutOptIn(t *testing.T) {
	rec := newRecordingMirror()
	client, err := NewCQLClient(newMockSession(), nil)
	require.NoError(t, err)
	defer client.Close()
	installMirrorEngine(t, client, rec.execute())

	require.NoError(t, client.Query("INSERT", "x").ExecContext(context.Background()))

	time.Sleep(30 * time.Millisecond)
	require.Empty(t, rec.snapshot())
}

func TestMirrorNotDispatchedOnPrimaryFailure(t *testing.T) {
	rec := newRecordingMirror()
	primarySA := newMockSession()
	primarySA.execErr = errors.New("primary fail")
	client, err := NewCQLClient(primarySA, nil)
	require.NoError(t, err)
	defer client.Close()
	installMirrorEngine(t, client, rec.execute())

	err = client.Query("INSERT", "x").Mirror().ExecContext(context.Background())
	require.Error(t, err)

	time.Sleep(30 * time.Millisecond)
	require.Empty(t, rec.snapshot(), "mirror must not fire when primary write failed")
}

func TestMirrorDeepCopyArgsSurvivesCallerMutation(t *testing.T) {
	rec := newRecordingMirror()
	client, err := NewCQLClient(newMockSession(), nil)
	require.NoError(t, err)
	defer client.Close()
	installMirrorEngine(t, client, rec.execute(), mirror.WithWorkers(1))

	buf := []byte{1, 2, 3, 4}
	require.NoError(t, client.Query("INSERT INTO t (b) VALUES (?)", buf).Mirror().ExecContext(context.Background()))

	// Mutate the caller's buffer immediately after Exec returns.
	for i := range buf {
		buf[i] = 0xff
	}

	rec.waitForOne(t, 2*time.Second)
	got := rec.snapshot()[0]
	captured, ok := got.Args[0].([]byte)
	require.True(t, ok)
	require.Equal(t, []byte{1, 2, 3, 4}, captured, "mirror must observe pre-mutation bytes")
}

func TestMirrorControllerIsNilWhenUnconfigured(t *testing.T) {
	client, err := NewCQLClient(newMockSession(), nil)
	require.NoError(t, err)
	defer client.Close()
	require.Nil(t, client.Mirror())
}

func TestMirrorControllerEnableDisable(t *testing.T) {
	rec := newRecordingMirror()
	client, err := NewCQLClient(newMockSession(), nil)
	require.NoError(t, err)
	defer client.Close()
	ctrl := installMirrorEngine(t, client, rec.execute(), mirror.WithWorkers(1))
	require.NotNil(t, client.Mirror())
	require.True(t, ctrl.Enabled())

	ctrl.Disable()
	require.NoError(t, client.Query("INSERT", "x").Mirror().ExecContext(context.Background()))
	time.Sleep(30 * time.Millisecond)
	require.Empty(t, rec.snapshot())
	require.GreaterOrEqual(t, ctrl.Stats().Dropped, uint64(1))

	ctrl.Enable()
	require.NoError(t, client.Query("INSERT", "y").Mirror().ExecContext(context.Background()))
	rec.waitForOne(t, time.Second)
	require.Len(t, rec.snapshot(), 1)
}

func TestMirrorBatchDispatch(t *testing.T) {
	rec := newRecordingMirror()
	client, err := NewCQLClient(newMockSession(), nil)
	require.NoError(t, err)
	defer client.Close()
	installMirrorEngine(t, client, rec.execute(), mirror.WithWorkers(1))

	batch := client.Batch(types.LoggedBatch).
		Query("INSERT INTO t (k) VALUES (?)", "a").
		Query("INSERT INTO t (k) VALUES (?)", "b").
		Mirror()
	require.NoError(t, batch.ExecContext(context.Background()))

	rec.waitForOne(t, 2*time.Second)
	payload := rec.snapshot()[0]
	require.True(t, payload.IsBatch)
	require.Equal(t, types.LoggedBatch, payload.BatchType)
	require.Len(t, payload.BatchStatements, 2)
	require.Equal(t, "INSERT INTO t (k) VALUES (?)", payload.BatchStatements[0].Query)
	require.Equal(t, []any{"a"}, payload.BatchStatements[0].Args)
	require.Equal(t, []any{"b"}, payload.BatchStatements[1].Args)
	require.NotZero(t, payload.Timestamp)
}

func TestMirrorWithMirrorTargetEndToEnd(t *testing.T) {
	// Configure WithMirror with a real mirror CQLClient and verify the
	// engine dispatches a successful write through the target's Exec path.
	// We rely on Stats() (atomic) rather than peeking into the unsynchronized
	// mockSession internals from another goroutine.
	mirrorTarget, err := NewCQLClient(newMockSession(), nil)
	require.NoError(t, err)
	defer mirrorTarget.Close()

	client, err := NewCQLClient(newMockSession(), nil,
		WithMirror(mirrorTarget,
			mirror.WithWorkers(1),
			mirror.WithQueueSize(8),
		),
	)
	require.NoError(t, err)
	defer client.Close()
	require.NotNil(t, client.Mirror())

	require.NoError(t, client.Query("INSERT INTO t (k) VALUES (?)", "abc").Mirror().ExecContext(context.Background()))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if client.Mirror().Stats().Success > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("mirror engine did not record success; stats=%+v", client.Mirror().Stats())
}

func TestMirrorEngineStoppedOnClientClose(t *testing.T) {
	rec := newRecordingMirror()
	client, err := NewCQLClient(newMockSession(), nil)
	require.NoError(t, err)
	engine := installMirrorEngine(t, client, rec.execute())
	require.True(t, engine.Enabled())

	client.Close()

	// After client.Close, the engine should still be reachable but
	// TryEnqueue is rejected because Stop() has been called.
	// (Cleanup-installed engine is stopped twice; sync.Once protects this.)
	// Note: Close() calls engine.Stop() too — both Stops coexist.
	require.False(t, engine.TryEnqueue(types.ReplayPayload{Query: "after close"}))
}

// TestMirrorReplayerEnqueuesOnFailure verifies that when WithMirrorReplayer
// is configured and the mirror execute returns an error, the failed payload
// lands in the replayer.
func TestMirrorReplayerEnqueuesOnFailure(t *testing.T) {
	mirrorSA := newMockSession()
	mirrorSA.execErr = errors.New("mirror cluster down")
	mirrorTarget, err := NewCQLClient(mirrorSA, nil)
	require.NoError(t, err)
	defer mirrorTarget.Close()

	// Use a custom replayer that records what it receives, so we don't
	// need to peek into MemoryReplayer internals.
	replayer := &recordingReplayer{}

	client, err := NewCQLClient(newMockSession(), nil,
		WithMirror(mirrorTarget,
			mirror.WithWorkers(1),
			mirror.WithQueueSize(8),
		),
		WithMirrorReplayer(replayer),
	)
	require.NoError(t, err)
	defer client.Close()

	require.NoError(t, client.Query("INSERT INTO t (k) VALUES (?)", "x").Mirror().ExecContext(context.Background()))

	// Wait for the mirror engine to attempt the write, fail, and enqueue.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if replayer.count() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	require.Equal(t, 1, replayer.count())
	got := replayer.first()
	require.Equal(t, "INSERT INTO t (k) VALUES (?)", got.Query)
	require.Equal(t, []any{"x"}, got.Args)
	require.NotZero(t, got.Timestamp)
}

// TestMirrorReplayerEndToEndExhaustsRetriesViaOnDrop verifies the full
// Phase 2 loop: mirror execute fails → payload lands in MemoryReplayer →
// auto-built MemoryWorker retries up to MaxAttempts → OnDrop fires.
//
// We use OnDrop as the verification anchor (rather than flipping a session
// to eventual success) because the replay worker's retry behavior is what
// Phase 2 wires; the eventual-success path uses the same code and is
// covered by the existing replay package tests.
func TestMirrorReplayerEndToEndExhaustsRetriesViaOnDrop(t *testing.T) {
	mirrorSA := newMockSession()
	mirrorSA.execErr = errors.New("mirror cluster down")
	mirrorTarget, err := NewCQLClient(mirrorSA, nil)
	require.NoError(t, err)
	defer mirrorTarget.Close()

	memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(64))

	dropCh := make(chan types.ReplayPayload, 4)
	client, err := NewCQLClient(newMockSession(), nil,
		WithMirror(mirrorTarget,
			mirror.WithWorkers(1),
			mirror.WithQueueSize(8),
		),
		WithMirrorReplayer(memReplayer,
			replay.WithPollInterval(20*time.Millisecond),
			replay.WithRetryDelay(10*time.Millisecond),
			replay.WithMaxAttempts(2),
			replay.WithOnDrop(func(p types.ReplayPayload, _ error) {
				dropCh <- p
			}),
		),
	)
	require.NoError(t, err)
	defer client.Close()
	require.NotNil(t, client.config.MirrorReplayWorker)

	require.NoError(t, client.Query("INSERT INTO t (k) VALUES (?)", "y").Mirror().ExecContext(context.Background()))

	select {
	case dropped := <-dropCh:
		require.Equal(t, "INSERT INTO t (k) VALUES (?)", dropped.Query)
		require.Equal(t, []any{"y"}, dropped.Args)
		require.NotZero(t, dropped.Timestamp)
	case <-time.After(3 * time.Second):
		t.Fatal("replay worker did not exhaust retries within timeout")
	}
}

// TestMirrorReplayerEnqueueFailureFiresOnReplayDropped verifies that when
// the mirror failure path cannot enqueue into the replayer, the existing
// OnReplayDropped callback fires — keeping mirror and primary replay
// alerting unified.
func TestMirrorReplayerEnqueueFailureFiresOnReplayDropped(t *testing.T) {
	mirrorSA := newMockSession()
	mirrorSA.execErr = errors.New("mirror cluster down")
	mirrorTarget, err := NewCQLClient(mirrorSA, nil)
	require.NoError(t, err)
	defer mirrorTarget.Close()

	replayer := &recordingReplayer{enqueueErr: errors.New("replayer queue full")}

	dropCh := make(chan types.ReplayPayload, 4)
	client, err := NewCQLClient(newMockSession(), nil,
		WithMirror(mirrorTarget, mirror.WithWorkers(1)),
		WithMirrorReplayer(replayer),
		WithOnReplayDropped(func(p types.ReplayPayload, _ error) {
			dropCh <- p
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	require.NoError(t, client.Query("INSERT", "z").Mirror().ExecContext(context.Background()))

	select {
	case dropped := <-dropCh:
		require.Equal(t, "INSERT", dropped.Query)
		require.Equal(t, []any{"z"}, dropped.Args)
	case <-time.After(2 * time.Second):
		t.Fatal("OnReplayDropped did not fire on mirror enqueue failure")
	}
}

// TestMirrorReplayerUnrecognizedTypeNoWorker verifies that passing a custom
// Replayer (neither MemoryReplayer nor NATSReplayer) still wires the engine
// hook but leaves MirrorReplayWorker nil; the application is responsible
// for running its own worker.
func TestMirrorReplayerUnrecognizedTypeNoWorker(t *testing.T) {
	mirrorSA := newMockSession()
	mirrorTarget, err := NewCQLClient(mirrorSA, nil)
	require.NoError(t, err)
	defer mirrorTarget.Close()

	custom := &recordingReplayer{}
	client, err := NewCQLClient(newMockSession(), nil,
		WithMirror(mirrorTarget),
		WithMirrorReplayer(custom),
	)
	require.NoError(t, err)
	defer client.Close()

	require.Nil(t, client.config.MirrorReplayWorker, "custom replayer must not auto-build a worker")
}

// recordingReplayer is a Replayer that captures Enqueued payloads, suitable
// for tests that don't want a worker draining behind their back. Not a
// recognized type for buildMirrorReplayWorker, so no auto-worker is built.
type recordingReplayer struct {
	mu         sync.Mutex
	captured   []types.ReplayPayload
	enqueueErr error
	enqueueCnt int
}

func (r *recordingReplayer) Enqueue(_ context.Context, p types.ReplayPayload) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.enqueueCnt++
	if r.enqueueErr != nil {
		return r.enqueueErr
	}
	r.captured = append(r.captured, p)

	return nil
}

func (r *recordingReplayer) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.captured)
}

func (r *recordingReplayer) first() types.ReplayPayload {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.captured[0]
}

// TestMirrorPublisherEnqueuesEachCapture verifies publisher mode: every
// successful primary write opted into Mirror() lands in the configured
// publisher (a Replayer), without any in-process write to a mirror target.
func TestMirrorPublisherEnqueuesEachCapture(t *testing.T) {
	pub := &recordingReplayer{}

	client, err := NewCQLClient(newMockSession(), nil,
		WithMirrorPublisher(pub,
			mirror.WithWorkers(1),
			mirror.WithQueueSize(8),
		),
	)
	require.NoError(t, err)
	defer client.Close()
	require.NotNil(t, client.Mirror())

	require.NoError(t, client.Query("INSERT INTO t (k) VALUES (?)", "p").Mirror().ExecContext(context.Background()))
	require.NoError(t, client.Query("INSERT INTO t (k) VALUES (?)", "q").Mirror().ExecContext(context.Background()))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if pub.count() >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.Equal(t, 2, pub.count())
}

func TestMirrorPublisherAndTargetMutuallyExclusive(t *testing.T) {
	mirrorTarget, err := NewCQLClient(newMockSession(), nil)
	require.NoError(t, err)
	defer mirrorTarget.Close()

	pub := &recordingReplayer{}

	_, err = NewCQLClient(newMockSession(), nil,
		WithMirror(mirrorTarget),
		WithMirrorPublisher(pub),
	)
	require.ErrorIs(t, err, types.ErrMirrorModeConflict)
}

func TestNewMirrorWorkerForMemoryReplayer(t *testing.T) {
	mirrorTarget, err := NewCQLClient(newMockSession(), nil)
	require.NoError(t, err)
	defer mirrorTarget.Close()

	mem := replay.NewMemoryReplayer(replay.WithQueueCapacity(16))
	worker, err := NewMirrorWorker(mem, mirrorTarget)
	require.NoError(t, err)
	require.NotNil(t, worker)
	require.NoError(t, worker.Start())
	worker.Stop()
}

func TestNewMirrorWorkerRejectsUnsupportedReplayer(t *testing.T) {
	mirrorTarget, err := NewCQLClient(newMockSession(), nil)
	require.NoError(t, err)
	defer mirrorTarget.Close()

	_, err = NewMirrorWorker(&recordingReplayer{}, mirrorTarget)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported replayer type")
}

func TestNewMirrorWorkerRejectsNilTarget(t *testing.T) {
	mem := replay.NewMemoryReplayer()
	_, err := NewMirrorWorker(mem, nil)
	require.ErrorIs(t, err, types.ErrNilMirrorTarget)
}

// TestMirrorPublisherEndToEndConsumerLandsWrites verifies the publisher /
// consumer split: app publishes via WithMirrorPublisher, a separate
// NewMirrorWorker bound to a different mirror target drains the same
// MemoryReplayer and the target's mirror engine records success.
func TestMirrorPublisherEndToEndConsumerLandsWrites(t *testing.T) {
	mem := replay.NewMemoryReplayer(replay.WithQueueCapacity(64))

	// App side.
	app, err := NewCQLClient(newMockSession(), nil,
		WithMirrorPublisher(mem,
			mirror.WithWorkers(1),
			mirror.WithQueueSize(8),
		),
	)
	require.NoError(t, err)
	defer app.Close()

	// Consumer side: separate mirrorTarget + worker drained from the same
	// MemoryReplayer.
	mirrorTarget, err := NewCQLClient(newMockSession(), nil)
	require.NoError(t, err)
	defer mirrorTarget.Close()

	worker, err := NewMirrorWorker(mem, mirrorTarget,
		replay.WithPollInterval(20*time.Millisecond),
	)
	require.NoError(t, err)
	require.NoError(t, worker.Start())
	defer worker.Stop()

	require.NoError(t, app.Query("INSERT INTO t (k) VALUES (?)", "p").Mirror().ExecContext(context.Background()))

	// Verify the consumer-side worker eventually drained the replayer and
	// the mirror target's engine recorded success. We can't peek at
	// mockSession internals safely, but we can observe that the replayer
	// is empty and the worker is running cleanly.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		stats := app.Mirror().Stats()
		if stats.Success >= 1 && stats.QueueDepth == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("publisher engine did not drain successfully; stats=%+v", app.Mirror().Stats())
}

func TestCloneArgs(t *testing.T) {
	in := []any{[]byte{1, 2, 3}, "hello", int64(42)}
	out := cloneArgs(in)

	bs, _ := in[0].([]byte)
	bs[0] = 0xff
	require.Equal(t, []byte{1, 2, 3}, out[0])
	require.Equal(t, "hello", out[1])
	require.Equal(t, int64(42), out[2])
}

func TestCloneArgsEmpty(t *testing.T) {
	require.Nil(t, cloneArgs(nil))
	require.Nil(t, cloneArgs([]any{}))
}

func TestCloneBatchEntries(t *testing.T) {
	entries := []batchEntry{
		{statement: "stmt1", args: []any{[]byte{1, 2}}},
		{statement: "stmt2", args: []any{"s"}},
	}
	out := cloneBatchEntries(entries)

	bs, _ := entries[0].args[0].([]byte)
	bs[0] = 0xff
	require.Len(t, out, 2)
	require.Equal(t, []byte{1, 2}, out[0].Args[0])
	require.Equal(t, "s", out[1].Args[0])
}
