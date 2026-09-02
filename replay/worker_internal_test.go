package replay

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/types"
)

func TestWorkerBackoffCalculation(t *testing.T) {
	tests := []struct {
		attempt    int
		retryDelay time.Duration
		maxDelay   time.Duration
		expected   time.Duration
	}{
		{1, 100 * time.Millisecond, 1 * time.Second, 100 * time.Millisecond},
		{2, 100 * time.Millisecond, 1 * time.Second, 200 * time.Millisecond},
		{3, 100 * time.Millisecond, 1 * time.Second, 400 * time.Millisecond},
		{4, 100 * time.Millisecond, 1 * time.Second, 800 * time.Millisecond},
		{5, 100 * time.Millisecond, 1 * time.Second, 1 * time.Second}, // Capped at max
		{10, 100 * time.Millisecond, 1 * time.Second, 1 * time.Second},
	}

	for _, tt := range tests {
		delay := calculateBackoff(tt.attempt, tt.retryDelay, tt.maxDelay)
		assert.Equal(t, tt.expected, delay, "attempt %d", tt.attempt)
	}
}

func TestWorkerConfigOptions(t *testing.T) {
	replayer := NewMemoryReplayer(WithQueueCapacity(10))
	defer replayer.Close()

	called := false
	worker := NewMemoryWorker(replayer, nil,
		WithBatchSize(20),
		WithPollInterval(500*time.Millisecond),
		WithRetryDelay(200*time.Millisecond),
		WithMaxRetryDelay(5*time.Second),
		WithExecuteTimeout(10*time.Second),
		WithOnSuccess(func(_ types.ReplayPayload) { called = true }),
		WithOnError(func(_ types.ReplayPayload, _ error, _ int) {}),
		WithOnDrop(func(_ types.ReplayPayload, _ error) {}),
	)

	assert.Equal(t, 20, worker.config.BatchSize)
	assert.Equal(t, 500*time.Millisecond, worker.config.PollInterval)
	assert.Equal(t, 200*time.Millisecond, worker.config.RetryDelay)
	assert.Equal(t, 5*time.Second, worker.config.MaxRetryDelay)
	assert.Equal(t, 10*time.Second, worker.config.ExecuteTimeout)
	assert.NotNil(t, worker.config.OnSuccess)
	assert.NotNil(t, worker.config.OnError)
	assert.NotNil(t, worker.config.OnDrop)

	// Verify callback works
	worker.config.OnSuccess(types.ReplayPayload{})
	assert.True(t, called)
}

func TestWorkerConfigInvalidValuesAreNormalized(t *testing.T) {
	replayer := NewMemoryReplayer(WithQueueCapacity(10))
	defer replayer.Close()

	worker := NewMemoryWorker(replayer,
		func(_ context.Context, _ types.ReplayPayload) error { return nil },
		WithBatchSize(0),
		WithPollInterval(0),
		WithRetryDelay(0),
		WithMaxRetryDelay(time.Nanosecond),
		WithExecuteTimeout(0),
		WithMaxAttempts(0),
	)

	defaults := DefaultWorkerConfig()
	assert.Equal(t, defaults.BatchSize, worker.config.BatchSize)
	assert.Equal(t, defaults.PollInterval, worker.config.PollInterval)
	assert.Equal(t, defaults.RetryDelay, worker.config.RetryDelay)
	assert.Equal(t, defaults.RetryDelay, worker.config.MaxRetryDelay)
	assert.Equal(t, defaults.ExecuteTimeout, worker.config.ExecuteTimeout)
	assert.Equal(t, 1, worker.config.MaxAttempts)
}

// fakeNilableMetrics satisfies types.MetricsCollector purely by embedding
// the interface, so a nil *fakeNilableMetrics is a valid (if unusable)
// value of the interface type. It is never invoked in tests that use it
// -- the point of TestWorkerConfigRejectsTypedNilOptions is that the
// nil-fallback guard must catch it before any method is ever called.
type fakeNilableMetrics struct {
	types.MetricsCollector
}

var _ types.MetricsCollector = (*fakeNilableMetrics)(nil)

// fakeNilableLogger satisfies types.Logger purely by embedding the
// interface; see fakeNilableMetrics for rationale.
type fakeNilableLogger struct {
	types.Logger
}

var _ types.Logger = (*fakeNilableLogger)(nil)

// TestWorkerConfigRejectsTypedNilOptions verifies that WithWorkerMetrics
// and WithWorkerLogger normalize a typed-nil interface value (a non-nil
// interface wrapping a nil concrete pointer, e.g. `var m *myCollector`)
// to the Nop fallback instead of storing an unusable nil-underlying
// value that would panic on first use.
func TestWorkerConfigRejectsTypedNilOptions(t *testing.T) {
	replayer := NewMemoryReplayer(WithQueueCapacity(10))
	defer replayer.Close()

	var nilMetrics *fakeNilableMetrics
	var nilLogger *fakeNilableLogger

	worker := NewMemoryWorker(replayer, nil,
		WithWorkerMetrics(nilMetrics),
		WithWorkerLogger(nilLogger),
	)

	_, isNopMetrics := worker.config.Metrics.(*metrics.NopMetrics)
	assert.True(t, isNopMetrics, "typed-nil metrics should fall back to NopMetrics, got %T", worker.config.Metrics)
	assert.False(t, worker.MetricsConfigured(), "typed-nil metrics should not count as explicitly configured")

	_, isNopLogger := worker.config.Logger.(*logging.NopLogger)
	assert.True(t, isNopLogger, "typed-nil logger should fall back to NopLogger, got %T", worker.config.Logger)
}

// TestNATSBackend_ProcessMessages_NaksWholeBatchOnShutdown verifies the
// shutdown contract: when stopCh fires mid-batch, every unprocessed
// message in the batch is Nak'd so the broker re-delivers them
// immediately rather than waiting for AckWait (default 30s) to expire.
//
// The test exercises processMessages directly with fabricated
// ReplayMessage values so we can observe ack/nak/term invocation without
// running an embedded NATS server.
func TestNATSBackend_ProcessMessages_NaksWholeBatchOnShutdown(t *testing.T) {
	const batchSize = 5

	stopCh := make(chan struct{})
	cfg := DefaultWorkerConfig()
	cfg.Metrics = metrics.NewNopMetrics()
	cfg.Logger = logging.NewNopLogger()
	cfg.ExecuteTimeout = time.Second

	// Track per-message outcomes via injected ack/nak/term funcs.
	var (
		mu          sync.Mutex
		ackCount    int
		nakedIdxs   []int
		termCount   int
		executedIdx atomic.Int32
	)

	msgs := make([]ReplayMessage, batchSize)
	for i := range batchSize {
		msgs[i] = ReplayMessage{
			Payload: types.ReplayPayload{
				TargetCluster: types.ClusterA,
				Query:         "INSERT test",
				Timestamp:     int64(i),
			},
			ackFunc: func() error {
				mu.Lock()
				ackCount++
				mu.Unlock()

				return nil
			},
			nakFunc: func() error {
				mu.Lock()
				nakedIdxs = append(nakedIdxs, i)
				mu.Unlock()

				return nil
			},
			termFunc: func() error {
				mu.Lock()
				termCount++
				mu.Unlock()

				return nil
			},
			DeliveryCount: 1,
			MaxDeliver:    5,
		}
	}

	// Execute func: succeed on the first message, then signal stop and
	// block briefly so processMessages observes stopCh on the next iteration.
	execute := func(_ context.Context, _ types.ReplayPayload) error {
		idx := executedIdx.Add(1)
		if idx == 1 {
			// Trigger shutdown after the first message acks.
			close(stopCh)
			// Brief pause to ensure the next loop iteration sees stopCh.
			time.Sleep(10 * time.Millisecond)
		}

		return nil
	}

	b := &natsBackend{
		config:  &cfg,
		execute: execute,
		stopCh:  stopCh,
	}

	b.processMessages(msgs, false)

	mu.Lock()
	defer mu.Unlock()

	assert.Equal(t, 1, ackCount, "exactly one message processed and ack'd before shutdown")
	assert.Equal(t, 0, termCount, "no terms — no last-attempt drops")
	require.Len(t, nakedIdxs, batchSize-1,
		"every unprocessed message in the batch must be Nak'd; tail messages can no longer rely on AckWait timeout")
	assert.Equal(t, []int{1, 2, 3, 4}, nakedIdxs,
		"Nak must hit the contiguous tail starting at the message that observed stopCh")
}

// TestNATSBackend_ProcessMessages_NakErrorsAreNonBlocking verifies that an
// error from one Nak does not prevent the rest of the batch from being
// Nak'd. Each message's ack/nak is independent.
func TestNATSBackend_ProcessMessages_NakErrorsAreNonBlocking(t *testing.T) {
	stopCh := make(chan struct{})
	close(stopCh) // already stopped

	cfg := DefaultWorkerConfig()
	cfg.Metrics = metrics.NewNopMetrics()
	cfg.Logger = logging.NewNopLogger()

	var nakedCount atomic.Int32
	mkMsg := func(failNak bool) ReplayMessage {
		return ReplayMessage{
			Payload: types.ReplayPayload{TargetCluster: types.ClusterA},
			nakFunc: func() error {
				nakedCount.Add(1)
				if failNak {
					return errors.New("nak failed")
				}

				return nil
			},
			ackFunc:  func() error { return nil },
			termFunc: func() error { return nil },
		}
	}

	msgs := []ReplayMessage{mkMsg(true), mkMsg(false), mkMsg(true)}

	b := &natsBackend{
		config:  &cfg,
		execute: func(_ context.Context, _ types.ReplayPayload) error { return nil },
		stopCh:  stopCh,
	}
	b.processMessages(msgs, false)

	assert.Equal(t, int32(3), nakedCount.Load(),
		"every message in the batch must have its Nak attempted, errors don't abort the loop")
}

func TestNATSBackend_ProcessMessages_AckFailureIsReportedAsError(t *testing.T) {
	logger := &recordingWorkerLogger{}
	cfg := DefaultWorkerConfig()
	cfg.Metrics = metrics.NewNopMetrics()
	cfg.Logger = logger

	var success atomic.Int32
	var errorCount atomic.Int32
	cfg.OnSuccess = func(_ types.ReplayPayload) { success.Add(1) }
	cfg.OnError = func(_ types.ReplayPayload, err error, _ int) {
		if errors.Is(err, errAckFailed) {
			errorCount.Add(1)
		}
	}

	msg := ReplayMessage{
		Payload: types.ReplayPayload{TargetCluster: types.ClusterA},
		ackFunc: func() error {
			return errAckFailed
		},
		DeliveryCount: 1,
		MaxDeliver:    5,
	}

	b := &natsBackend{
		config:  &cfg,
		execute: func(_ context.Context, _ types.ReplayPayload) error { return nil },
		stopCh:  make(chan struct{}),
	}
	b.processMessages([]ReplayMessage{msg}, false)

	assert.Equal(t, int32(0), success.Load(), "ack failure makes broker state uncertain; do not report success")
	assert.Equal(t, int32(1), errorCount.Load())
	assert.Equal(t, int32(1), logger.errorCount.Load())
}

func TestNATSBackend_ProcessMessages_TermFailureDoesNotReportDrop(t *testing.T) {
	logger := &recordingWorkerLogger{}
	cfg := DefaultWorkerConfig()
	cfg.Metrics = metrics.NewNopMetrics()
	cfg.Logger = logger

	var dropped atomic.Int32
	cfg.OnDrop = func(_ types.ReplayPayload, _ error) { dropped.Add(1) }

	msg := ReplayMessage{
		Payload: types.ReplayPayload{TargetCluster: types.ClusterA},
		termFunc: func() error {
			return errors.New("term failed")
		},
		DeliveryCount: 5,
		MaxDeliver:    5,
	}

	b := &natsBackend{
		config:  &cfg,
		execute: func(_ context.Context, _ types.ReplayPayload) error { return errors.New("execute failed") },
		stopCh:  make(chan struct{}),
	}
	b.processMessages([]ReplayMessage{msg}, false)

	assert.Equal(t, int32(0), dropped.Load(), "failed Term means the broker did not confirm the message was dropped")
	assert.Equal(t, int32(1), logger.errorCount.Load())
}

var errAckFailed = errors.New("ack failed")

type recordingWorkerLogger struct {
	errorCount atomic.Int32
}

func (l *recordingWorkerLogger) Debug(_ string, _ ...any) {}

func (l *recordingWorkerLogger) Info(_ string, _ ...any) {}

func (l *recordingWorkerLogger) Warn(_ string, _ ...any) {}

func (l *recordingWorkerLogger) Error(_ string, _ ...any) {
	l.errorCount.Add(1)
}

func (l *recordingWorkerLogger) Fatal(_ string, _ ...any) {}
