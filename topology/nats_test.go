package topology_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/topology"
	"github.com/arloliu/helix/types"
)

// drainUpdates drains a topology update channel in the background.
func drainUpdates(ch <-chan helix.TopologyUpdate) {
	go func() {
		for range ch {
			_ = struct{}{} // consume item
		}
	}()
}

// createTestKV creates a test KV bucket.
func createTestKV(t *testing.T, js jetstream.JetStream, bucket string) jetstream.KeyValue {
	t.Helper()

	ctx := context.Background()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: bucket,
	})
	require.NoError(t, err)

	return kv
}

// watchFailKV is a jetstream.KeyValue test double that always fails Watch,
// so tests can exercise the kv.Watch() error path without needing a real
// NATS server misconfiguration. Get returns ErrKeyNotFound so fetchAndEmit's
// initial fetch behaves like an unconfigured bucket. All other methods are
// unused by the watcher and are left to panic via the nil embedded interface
// if ever called.
type watchFailKV struct {
	jetstream.KeyValue
	watchErr error
}

func (f *watchFailKV) Get(_ context.Context, _ string) (jetstream.KeyValueEntry, error) {
	return nil, jetstream.ErrKeyNotFound
}

func (f *watchFailKV) Watch(_ context.Context, _ string, _ ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	return nil, f.watchErr
}

// recordingLogger is a minimal types.Logger implementation that counts Warn
// calls. Sufficient for verifying that a swallowed error path now logs.
type recordingLogger struct {
	mu    sync.Mutex
	warns int
}

func (l *recordingLogger) Debug(_ string, _ ...any) {}
func (l *recordingLogger) Info(_ string, _ ...any)  {}
func (l *recordingLogger) Warn(_ string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns++
}
func (l *recordingLogger) Error(_ string, _ ...any) {}
func (l *recordingLogger) Fatal(_ string, _ ...any) {}

func (l *recordingLogger) warnCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.warns
}

// TestNATSWatchFailure_LogsAndFallsBackToPolling verifies the fix for the
// swallowed kv.Watch() error: when establishing the watch fails, the
// configured logger observes a Warn, and the watcher still falls back to
// polling (IsDraining stays observable/false, no panic, no goroutine spin).
func TestNATSWatchFailure_LogsAndFallsBackToPolling(t *testing.T) {
	logger := &recordingLogger{}
	kv := &watchFailKV{watchErr: errors.New("boom: watch unsupported")}

	watcher, err := topology.NewNATS(kv,
		topology.WithLogger(logger),
		topology.WithPollInterval(20*time.Millisecond),
	)
	require.NoError(t, err)
	defer watcher.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	updates := watcher.Watch(ctx)
	defer drainUpdates(updates)

	require.Eventually(t, func() bool {
		return logger.warnCount() >= 1
	}, time.Second, 10*time.Millisecond, "Watch() failure must be logged")

	// Fallback polling loop must still be alive and not draining anything.
	assert.False(t, watcher.IsDraining(types.ClusterA))
	assert.False(t, watcher.IsDraining(types.ClusterB))
}

// TestNATSDefaultLogger_NoPanic verifies that a watcher constructed without
// WithLogger uses a safe default and does not panic when the watch fails.
func TestNATSDefaultLogger_NoPanic(t *testing.T) {
	kv := &watchFailKV{watchErr: errors.New("boom: watch unsupported")}

	watcher, err := topology.NewNATS(kv, topology.WithPollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer watcher.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	updates := watcher.Watch(ctx)
	defer drainUpdates(updates)

	<-ctx.Done()
	assert.False(t, watcher.IsDraining(types.ClusterA))
}

// TestNATSExplicitNilLogger_NoPanic verifies that passing WithLogger(nil)
// explicitly — as opposed to never calling WithLogger — is normalized to
// the safe no-op default, per WithLogger's documented "if not set (or set
// to nil)" contract, rather than leaving config.Logger nil and panicking on
// first use.
func TestNATSExplicitNilLogger_NoPanic(t *testing.T) {
	kv := &watchFailKV{watchErr: errors.New("boom: watch unsupported")}

	watcher, err := topology.NewNATS(kv,
		topology.WithLogger(nil),
		topology.WithPollInterval(20*time.Millisecond),
	)
	require.NoError(t, err)
	defer watcher.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	updates := watcher.Watch(ctx)
	defer drainUpdates(updates)

	// If WithLogger(nil) were not normalized, watchLoop's Logger.Warn call
	// on the failed kv.Watch would panic on a nil interface, crashing this
	// test (and the whole test binary, since panics in background
	// goroutines are unrecoverable).
	<-ctx.Done()
	assert.False(t, watcher.IsDraining(types.ClusterA))
}

// fakeNilableLogger satisfies types.Logger purely by embedding the
// interface, so a nil *fakeNilableLogger assigned to a types.Logger
// parameter produces a typed-nil interface value (non-nil interface, nil
// concrete pointer) rather than an untyped nil. Mirrors replay's
// fakeNilableLogger (replay/worker_internal_test.go), used there for the
// same class of bug via TestWorkerConfigRejectsTypedNilOptions.
type fakeNilableLogger struct {
	types.Logger
}

var _ types.Logger = (*fakeNilableLogger)(nil)

// TestNATSTypedNilLogger_NoPanic verifies that a typed-nil logger (a
// non-nil types.Logger interface value wrapping a nil concrete pointer)
// passed via WithLogger is normalized to the safe no-op default, just like
// an explicit literal nil (TestNATSExplicitNilLogger_NoPanic). Without the
// fix, watchLoop's Logger.Warn call on the failed kv.Watch would panic on
// the nil receiver, crashing the background goroutine — and the whole test
// binary, since panics in background goroutines are unrecoverable.
func TestNATSTypedNilLogger_NoPanic(t *testing.T) {
	kv := &watchFailKV{watchErr: errors.New("boom: watch unsupported")}
	var nilLogger *fakeNilableLogger

	watcher, err := topology.NewNATS(kv,
		topology.WithLogger(nilLogger),
		topology.WithPollInterval(20*time.Millisecond),
	)
	require.NoError(t, err)
	defer watcher.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	updates := watcher.Watch(ctx)
	defer drainUpdates(updates)

	<-ctx.Done()
	assert.False(t, watcher.IsDraining(types.ClusterA))
}

func TestNewNATSNilKV(t *testing.T) {
	_, err := topology.NewNATS(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KeyValue store is nil")
}

func TestNewNATSRejectsNonPositivePollInterval(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-poll-interval")

	for _, d := range []time.Duration{0, -time.Second} {
		_, err := topology.NewNATS(kv, topology.WithPollInterval(d))
		require.Error(t, err, "PollInterval %v", d)
		assert.Contains(t, err.Error(), "PollInterval must be positive")
	}
}

func TestNewNATSDefaults(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-defaults")

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)
	defer watcher.Close()

	assert.Equal(t, "helix.topology.drain", watcher.Config().Key)
	assert.Equal(t, 5*time.Second, watcher.Config().PollInterval)
	assert.Equal(t, 10*time.Second, watcher.Config().InitialFetchTimeout)
}

func TestNewNATSOptions(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-options")

	watcher, err := topology.NewNATS(kv,
		topology.WithKey("custom.drain.key"),
		topology.WithPollInterval(10*time.Second),
		topology.WithInitialFetchTimeout(30*time.Second),
	)
	require.NoError(t, err)
	defer watcher.Close()

	assert.Equal(t, "custom.drain.key", watcher.Config().Key)
	assert.Equal(t, 10*time.Second, watcher.Config().PollInterval)
	assert.Equal(t, 30*time.Second, watcher.Config().InitialFetchTimeout)
}

func TestNATSDrainClusterB(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-drain-b")

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)
	defer watcher.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	updates := watcher.Watch(ctx)

	// Initially not draining
	assert.False(t, watcher.IsDraining(types.ClusterA))
	assert.False(t, watcher.IsDraining(types.ClusterB))

	// Set drain on cluster B
	drainConfig := topology.DrainConfig{
		Drain:  []types.ClusterID{types.ClusterB},
		Reason: "OS Patching",
	}
	data, _ := json.Marshal(drainConfig)
	_, err = kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	// Wait for update
	select {
	case update := <-updates:
		assert.Equal(t, types.ClusterB, update.Cluster)
		assert.True(t, update.DrainMode)
		assert.False(t, update.Available)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for topology update")
	}

	// Verify state
	assert.False(t, watcher.IsDraining(types.ClusterA))
	assert.True(t, watcher.IsDraining(types.ClusterB))
}

func TestNATSDrainBothClusters(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-drain-both")

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)
	defer watcher.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	updates := watcher.Watch(ctx)

	// Set drain on both clusters
	drainConfig := topology.DrainConfig{
		Drain:  []types.ClusterID{types.ClusterA, types.ClusterB},
		Reason: "Full maintenance",
	}
	data, _ := json.Marshal(drainConfig)
	_, err = kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	// Collect both updates
	received := make(map[types.ClusterID]helix.TopologyUpdate)
	for range 2 {
		select {
		case update := <-updates:
			received[update.Cluster] = update
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for topology update")
		}
	}

	assert.Len(t, received, 2)
	assert.True(t, received[types.ClusterA].DrainMode)
	assert.True(t, received[types.ClusterB].DrainMode)
}

func TestNATSClearDrain(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-clear-drain")

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)
	defer watcher.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Pre-set drain before watching
	drainConfig := topology.DrainConfig{
		Drain:  []types.ClusterID{types.ClusterB},
		Reason: "Upgrade",
	}
	data, _ := json.Marshal(drainConfig)
	_, err = kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	updates := watcher.Watch(ctx)

	// Wait for initial drain update
	select {
	case update := <-updates:
		assert.Equal(t, types.ClusterB, update.Cluster)
		assert.True(t, update.DrainMode)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for initial drain update")
	}

	// Delete the key to clear drain
	err = kv.Delete(ctx, "helix.topology.drain")
	require.NoError(t, err)

	// Wait for clear update
	select {
	case update := <-updates:
		assert.Equal(t, types.ClusterB, update.Cluster)
		assert.False(t, update.DrainMode)
		assert.True(t, update.Available)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for clear drain update")
	}

	assert.False(t, watcher.IsDraining(types.ClusterB))
}

func TestNATSEmptyDrainList(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-empty-drain")

	// Pre-set drain
	ctx := t.Context()
	drainConfig := topology.DrainConfig{
		Drain:  []types.ClusterID{types.ClusterA},
		Reason: "Test",
	}
	data, _ := json.Marshal(drainConfig)
	_, err := kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)
	defer watcher.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	updates := watcher.Watch(ctx)

	// Wait for initial update
	select {
	case <-updates:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for initial update")
	}

	assert.True(t, watcher.IsDraining(types.ClusterA))

	// Clear by setting empty drain list
	drainConfig = topology.DrainConfig{
		Drain:  []types.ClusterID{},
		Reason: "",
	}
	data, _ = json.Marshal(drainConfig)
	_, err = kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	// Wait for clear update
	select {
	case update := <-updates:
		assert.Equal(t, types.ClusterA, update.Cluster)
		assert.False(t, update.DrainMode)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for clear update")
	}

	assert.False(t, watcher.IsDraining(types.ClusterA))
}

func TestNATSInvalidJSON(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-invalid-json")

	logger := &recordingLogger{}
	watcher, err := topology.NewNATS(kv, topology.WithLogger(logger))
	require.NoError(t, err)
	defer watcher.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_ = watcher.Watch(ctx)

	// Put invalid JSON
	_, err = kv.Put(ctx, "helix.topology.drain", []byte("not valid json"))
	require.NoError(t, err)

	// Wait for the watcher to actually process the malformed entry (rather
	// than the trivially-already-true "not draining" state below, which
	// would race ahead of processEntry running).
	require.Eventually(t, func() bool {
		return logger.warnCount() > 0
	}, 2*time.Second, 50*time.Millisecond, "malformed JSON must be logged instead of silently swallowed")

	// Starting from no-drain, malformed JSON must preserve the no-drain
	// state (the same outcome as the previous "treat as no drain" behavior
	// for this empty starting condition).
	assert.False(t, watcher.IsDraining(types.ClusterA))
	assert.False(t, watcher.IsDraining(types.ClusterB))
}

// TestNATSInvalidJSON_PreservesDrainState verifies the fail-closed contract:
// malformed JSON written to the KV must NOT undrain a cluster that is
// currently draining. Without this guard, a single bad config push from an
// operator's tooling would silently undrain a cluster intended to stay
// offline.
func TestNATSInvalidJSON_PreservesDrainState(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-invalid-json-preserve")

	watcher, err := topology.NewNATS(kv,
		topology.WithPollInterval(50*time.Millisecond),
	)
	require.NoError(t, err)
	defer watcher.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_ = watcher.Watch(ctx)

	// Establish drain on cluster A via a valid config.
	good, err := json.Marshal(topology.DrainConfig{
		Drain:  []types.ClusterID{types.ClusterA},
		Reason: "maintenance",
	})
	require.NoError(t, err)
	_, err = kv.Put(ctx, "helix.topology.drain", good)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return watcher.IsDraining(types.ClusterA)
	}, 2*time.Second, 25*time.Millisecond, "drain must be applied")

	// Now overwrite with malformed JSON. Drain must be preserved.
	_, err = kv.Put(ctx, "helix.topology.drain", []byte("not valid json"))
	require.NoError(t, err)

	// Give the watcher time to observe the malformed update.
	time.Sleep(200 * time.Millisecond)

	assert.True(t, watcher.IsDraining(types.ClusterA),
		"malformed JSON must not undrain a previously-draining cluster")
	assert.False(t, watcher.IsDraining(types.ClusterB))
}

// TestNATSKeyDelete_ClearsDrain verifies that an explicit Delete of the
// drain key clears drain state — the fail-closed semantics for malformed
// JSON do not apply to authoritative deletes.
func TestNATSKeyDelete_ClearsDrain(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-key-delete")

	watcher, err := topology.NewNATS(kv,
		topology.WithPollInterval(50*time.Millisecond),
	)
	require.NoError(t, err)
	defer watcher.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_ = watcher.Watch(ctx)

	good, err := json.Marshal(topology.DrainConfig{
		Drain:  []types.ClusterID{types.ClusterA, types.ClusterB},
		Reason: "both",
	})
	require.NoError(t, err)
	_, err = kv.Put(ctx, "helix.topology.drain", good)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return watcher.IsDraining(types.ClusterA) && watcher.IsDraining(types.ClusterB)
	}, 2*time.Second, 25*time.Millisecond)

	require.NoError(t, kv.Delete(ctx, "helix.topology.drain"))

	require.Eventually(t, func() bool {
		return !watcher.IsDraining(types.ClusterA) && !watcher.IsDraining(types.ClusterB)
	}, 2*time.Second, 25*time.Millisecond, "Delete must clear drain")
}

func TestNATSClose(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-close")

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)

	ctx := t.Context()
	updates := watcher.Watch(ctx)

	// Close should be safe to call
	err = watcher.Close()
	require.NoError(t, err)

	// Double close should be safe
	err = watcher.Close()
	require.NoError(t, err)

	// Channel should eventually close
	select {
	case _, ok := <-updates:
		if ok {
			drainUpdates(updates)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("channel not closed after Close()")
	}
}

func TestNATSContextCancellation(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-ctx-cancel")

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)
	defer watcher.Close()

	ctx, cancel := context.WithCancel(t.Context())
	updates := watcher.Watch(ctx)

	// Cancel context
	cancel()

	// Channel should eventually close
	select {
	case _, ok := <-updates:
		if ok {
			drainUpdates(updates)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("channel not closed after context cancellation")
	}
}

func TestNATSGetDrainReason(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-drain-reason")

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)
	defer watcher.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Start watching - this is required for reason caching
	updates := watcher.Watch(ctx)
	defer drainUpdates(updates)

	// Initially no reason
	assert.Empty(t, watcher.GetDrainReason())

	// Set drain with reason
	drainConfig := topology.DrainConfig{
		Drain:  []types.ClusterID{types.ClusterB},
		Reason: "Scheduled maintenance window",
	}
	data, _ := json.Marshal(drainConfig)
	_, err = kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	// Wait for watcher to process the update and cache the reason
	require.Eventually(t, func() bool {
		return watcher.GetDrainReason() == "Scheduled maintenance window"
	}, 2*time.Second, 10*time.Millisecond)
}

func TestNATSMultipleWatchCalls(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := createTestKV(t, js, "test-multi-watch")

	watcher, err := topology.NewNATS(kv)
	require.NoError(t, err)
	defer watcher.Close()

	ctx := t.Context()

	// Call Watch multiple times - should return the same channel
	updates1 := watcher.Watch(ctx)
	updates2 := watcher.Watch(ctx)

	// Both should be the same channel
	assert.Equal(t, updates1, updates2)

	// Set drain and verify we receive update on the channel
	drainConfig := topology.DrainConfig{
		Drain:  []types.ClusterID{types.ClusterA},
		Reason: "test",
	}
	data, _ := json.Marshal(drainConfig)
	_, err = kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	select {
	case update := <-updates1:
		assert.Equal(t, types.ClusterA, update.Cluster)
		assert.True(t, update.DrainMode)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for update")
	}

	// Close should not panic (only closes channel once)
	err = watcher.Close()
	require.NoError(t, err)
}

func TestDrainConfigContainsCluster(t *testing.T) {
	tests := []struct {
		name     string
		drain    []types.ClusterID
		cluster  types.ClusterID
		expected bool
	}{
		{
			name:     "empty drain list",
			drain:    []types.ClusterID{},
			cluster:  types.ClusterA,
			expected: false,
		},
		{
			name:     "cluster A in list",
			drain:    []types.ClusterID{types.ClusterA},
			cluster:  types.ClusterA,
			expected: true,
		},
		{
			name:     "cluster A not in list",
			drain:    []types.ClusterID{types.ClusterB},
			cluster:  types.ClusterA,
			expected: false,
		},
		{
			name:     "both clusters in list",
			drain:    []types.ClusterID{types.ClusterA, types.ClusterB},
			cluster:  types.ClusterB,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := topology.DrainConfig{Drain: tt.drain}
			assert.Equal(t, tt.expected, config.ContainsCluster(tt.cluster))
		})
	}
}

// retryWatchKV is a jetstream.KeyValue test double that fails Watch a fixed
// number of times before delegating to the real bucket, so tests can verify
// that the watcher returns to watch mode after falling back to polling.
type retryWatchKV struct {
	jetstream.KeyValue
	mu       sync.Mutex
	failures int // Watch calls left to fail
	calls    int
}

func (r *retryWatchKV) Watch(ctx context.Context, key string, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	r.mu.Lock()
	r.calls++
	fail := r.failures > 0
	if fail {
		r.failures--
	}
	r.mu.Unlock()
	if fail {
		return nil, errors.New("boom: watch temporarily unavailable")
	}

	return r.KeyValue.Watch(ctx, key, opts...)
}

func (r *retryWatchKV) watchCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.calls
}

// TestNATSWatchFailure_ReturnsToWatchMode verifies that a watcher which fell
// back to polling retries Watch on each poll tick and, once Watch succeeds,
// observes KV changes through the watch again.
func TestNATSWatchFailure_ReturnsToWatchMode(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)
	kv := &retryWatchKV{KeyValue: createTestKV(t, js, "test-watch-retry"), failures: 3}
	logger := &recordingLogger{}

	watcher, err := topology.NewNATS(kv,
		topology.WithLogger(logger),
		topology.WithPollInterval(20*time.Millisecond),
	)
	require.NoError(t, err)
	defer watcher.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	updates := watcher.Watch(ctx)

	// Three failures, then the fourth call establishes the watch.
	require.Eventually(t, func() bool {
		return kv.watchCalls() >= 4
	}, 2*time.Second, 5*time.Millisecond, "Watch must be retried after falling back to polling")
	require.Equal(t, 2, logger.warnCount(), "failures one and two warn; the third is a debug line")

	// A watch delivers the change immediately, well inside one poll interval
	// on a quiet embedded server; a watcher stuck in polling would still see
	// it, so also confirm no further Watch calls were needed.
	drainConfig := topology.DrainConfig{Drain: []types.ClusterID{types.ClusterB}, Reason: "OS Patching"}
	data, _ := json.Marshal(drainConfig)
	_, err = kv.Put(ctx, "helix.topology.drain", data)
	require.NoError(t, err)

	select {
	case update := <-updates:
		assert.Equal(t, types.ClusterB, update.Cluster)
		assert.True(t, update.DrainMode)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for topology update")
	}
	assert.Equal(t, 4, kv.watchCalls(), "the established watch must stay in place")
}
