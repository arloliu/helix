package replay

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/types"
)

func TestWithClusterGate_ComposesByAND(t *testing.T) {
	cfg := DefaultWorkerConfig()
	cfg.Logger = logging.NewNopLogger()
	cfg.ClusterNames = types.DefaultClusterNames()
	require.True(t, cfg.allows(types.ClusterA), "no gate permits everything")

	WithClusterGate(func(c types.ClusterID) bool { return c == types.ClusterA })(&cfg)
	WithClusterGate(nil)(&cfg) // ignored
	require.True(t, cfg.allows(types.ClusterA))
	require.False(t, cfg.allows(types.ClusterB))

	WithClusterGate(func(types.ClusterID) bool { return false })(&cfg)
	require.False(t, cfg.allows(types.ClusterA), "a later gate cannot re-open what an earlier one closed")
}

func TestWithClusterGate_PanicCountsAsClosed(t *testing.T) {
	cfg := DefaultWorkerConfig()
	cfg.Logger = logging.NewNopLogger()
	cfg.ClusterNames = types.DefaultClusterNames()
	WithClusterGate(func(types.ClusterID) bool { panic("gate bug") })(&cfg)
	require.False(t, cfg.allows(types.ClusterA))
}

// gatedNATSMessages builds a batch whose InProgress calls are counted per
// message and whose NAKs are counted.
func gatedNATSMessages(n int, progress *[]int, naks *atomic.Int32, mu *sync.Mutex) []ReplayMessage {
	msgs := make([]ReplayMessage, n)
	for i := range n {
		msgs[i] = ReplayMessage{
			Payload:       types.ReplayPayload{TargetCluster: types.ClusterA, Query: "INSERT test"},
			ackFunc:       func() error { return nil },
			nakFunc:       func() error { naks.Add(1); return nil },
			DeliveryCount: 5,
			MaxDeliver:    5, // the last permitted delivery: a NAK would drop it
			inProgressFunc: func() error {
				mu.Lock()
				(*progress)[i]++
				mu.Unlock()

				return nil
			},
		}
	}

	return msgs
}

// TestNATSBackend_HoldsFetchedBatchWhileGated proves a gate that closes
// after a fetch keeps every unprocessed message in progress and NAKs
// nothing, then executes the whole batch once the gate opens.
func TestNATSBackend_HoldsFetchedBatchWhileGated(t *testing.T) {
	var mu sync.Mutex
	progress := make([]int, 3)
	var naks, executed atomic.Int32
	msgs := gatedNATSMessages(3, &progress, &naks, &mu)

	var open atomic.Bool
	cfg := newTestNATSBackendConfig()
	cfg.PollInterval = time.Millisecond
	WithClusterGate(func(types.ClusterID) bool { return open.Load() })(&cfg)
	ticks := make(chan time.Time)
	b := &natsBackend{
		replayer: &NATSReplayer{config: NATSReplayerConfig{AckWait: 30 * time.Second}},
		config:   &cfg,
		execute: func(context.Context, types.ReplayPayload) error {
			executed.Add(1)

			return nil
		},
		stopCh:      make(chan struct{}),
		backoffWait: func(time.Duration) <-chan time.Time { return ticks },
	}

	done := make(chan struct{})
	go func() { b.processMessages(msgs, false); close(done) }()

	// Several hold intervals pass: every message is kept in progress
	// (refreshed at most once per third of AckWait, so exactly once
	// here) and nothing executes or is NAK'd.
	for range 3 {
		ticks <- time.Time{}
	}
	mu.Lock()
	for i, n := range progress {
		require.Equal(t, 1, n, "message %d must be kept in progress, refreshed once per AckWait/3", i)
	}
	mu.Unlock()
	require.Zero(t, executed.Load())
	require.Zero(t, naks.Load(), "a gated message is never NAK'd, so its last delivery is not spent")

	open.Store(true)
	ticks <- time.Time{}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the batch did not finish after the gate opened")
	}
	require.Equal(t, int32(3), executed.Load(), "the whole batch executes once the gate opens")
	require.Zero(t, naks.Load())
}

// TestNATSBackend_HoldPollsWithinAckWait proves a gated batch is refreshed
// at a fraction of AckWait even when PollInterval is longer than AckWait.
func TestNATSBackend_HoldPollsWithinAckWait(t *testing.T) {
	var mu sync.Mutex
	progress := make([]int, 1)
	var naks atomic.Int32
	msgs := gatedNATSMessages(1, &progress, &naks, &mu)

	cfg := newTestNATSBackendConfig()
	cfg.PollInterval = time.Hour
	WithClusterGate(func(types.ClusterID) bool { return false })(&cfg)
	const ackWait = 30 * time.Second
	waits := make(chan time.Duration, 1)
	stop := make(chan struct{})
	b := &natsBackend{
		replayer: &NATSReplayer{config: NATSReplayerConfig{AckWait: ackWait}},
		config:   &cfg,
		stopCh:   stop,
		backoffWait: func(d time.Duration) <-chan time.Time {
			waits <- d

			return nil // never fires; the test stops the worker
		},
	}

	done := make(chan bool, 1)
	go func() { done <- b.holdWhileGated(msgs) }()
	select {
	case d := <-waits:
		require.LessOrEqual(t, d, ackWait/3, "the gate is polled at least once per third of AckWait")
	case <-time.After(time.Second):
		t.Fatal("holdWhileGated never waited")
	}
	close(stop)
	require.False(t, <-done)
}

// TestNATSBackend_StopWhileGatedNaksTailOnce proves that stopping while a
// batch is held NAKs each unprocessed message exactly once.
func TestNATSBackend_StopWhileGatedNaksTailOnce(t *testing.T) {
	var mu sync.Mutex
	progress := make([]int, 3)
	var naks, executed atomic.Int32
	msgs := gatedNATSMessages(3, &progress, &naks, &mu)

	cfg := newTestNATSBackendConfig()
	WithClusterGate(func(types.ClusterID) bool { return false })(&cfg)
	stopCh := make(chan struct{})
	ticks := make(chan time.Time)
	b := &natsBackend{
		replayer:    &NATSReplayer{config: NATSReplayerConfig{AckWait: 30 * time.Second}},
		config:      &cfg,
		execute:     func(context.Context, types.ReplayPayload) error { executed.Add(1); return nil },
		stopCh:      stopCh,
		backoffWait: func(time.Duration) <-chan time.Time { return ticks },
	}

	done := make(chan struct{})
	go func() { b.processMessages(msgs, false); close(done) }()
	ticks <- time.Time{} // one hold interval, so the hold loop is running
	close(stopCh)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("processMessages did not return on stop")
	}
	require.Zero(t, executed.Load())
	require.Equal(t, int32(3), naks.Load(), "each unprocessed message is NAK'd exactly once")
}
