package replay

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/arloliu/helix/types"
)

func TestWorkerBackoffCalculation(t *testing.T) {
	replayer := NewMemoryReplayer(WithQueueCapacity(10))
	defer replayer.Close()

	worker := NewMemoryWorker(replayer, nil,
		WithRetryDelay(100*time.Millisecond),
		WithMaxRetryDelay(1*time.Second),
	)

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{5, 1 * time.Second}, // Capped at max
		{10, 1 * time.Second},
	}

	for _, tt := range tests {
		delay := worker.calculateBackoff(tt.attempt)
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
