package replay

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/types"
)

// Under RetryWhileRetained every message in a batch is marked in progress
// before it is executed, so a slow batch does not outlive AckWait and get
// redelivered mid-flight.
func TestNATSBackend_ProcessMessages_MarksInProgressBeforeEachExecute(t *testing.T) {
	const batchSize = 3

	var mu sync.Mutex
	var events []string
	msgs := make([]ReplayMessage, batchSize)
	for i := range batchSize {
		msgs[i] = ReplayMessage{
			Payload:        types.ReplayPayload{TargetCluster: types.ClusterA, Query: "INSERT test"},
			ackFunc:        func() error { return nil },
			inProgressFunc: func() error { mu.Lock(); events = append(events, "progress"); mu.Unlock(); return nil },
			DeliveryCount:  1,
			MaxDeliver:     5,
		}
	}

	cfg := newTestNATSBackendConfig()
	b := &natsBackend{
		config: &cfg,
		execute: func(context.Context, types.ReplayPayload) error {
			mu.Lock()
			events = append(events, "execute")
			mu.Unlock()

			return nil
		},
		stopCh: make(chan struct{}),
	}

	b.processMessages(msgs, true)

	assert.Equal(t, []string{"progress", "execute", "progress", "execute", "progress", "execute"}, events)
}

// A failed attempt under RetryWhileRetained is negatively acknowledged with
// the backoff delay for its delivery count, never terminated.
func TestNATSBackend_SettleRetained_NaksWithBackoff(t *testing.T) {
	var delays []time.Duration
	var termed, plainNak bool
	msg := ReplayMessage{
		Payload:          types.ReplayPayload{TargetCluster: types.ClusterA, Query: "INSERT test"},
		nakFunc:          func() error { plainNak = true; return nil },
		nakWithDelayFunc: func(d time.Duration) error { delays = append(delays, d); return nil },
		termFunc:         func() error { termed = true; return nil },
		DeliveryCount:    3,
		StreamSequence:   7,
	}

	cfg := newTestNATSBackendConfig()
	cfg.RetryPolicy = RetryWhileRetained
	cfg.RetryDelay = 10 * time.Millisecond
	cfg.MaxRetryDelay = time.Second
	cfg.MaxAttempts = 2
	cfg.Classifier = DefaultReplayClassifier
	b := &natsBackend{config: &cfg, deadLetters: make(map[uint64]int)}

	b.settleRetained(msg, errors.New("unknown failure"))

	require.Equal(t, []time.Duration{40 * time.Millisecond}, delays, "third delivery waits 10ms * 2^2")
	assert.False(t, plainNak, "a message accepts one acknowledgement: no plain Nak beside the delayed one")
	assert.False(t, termed, "an unknown error is retried, not dead-lettered")
}

// Dead-letter dispositions are counted per stream sequence and the message
// is terminated once MaxAttempts of them have been seen.
func TestNATSBackend_SettleRetained_TerminatesAfterDeadLetterBudget(t *testing.T) {
	var naks, terms int
	msg := ReplayMessage{
		Payload:          types.ReplayPayload{TargetCluster: types.ClusterB, Query: "INSERT test"},
		nakWithDelayFunc: func(time.Duration) error { naks++; return nil },
		termFunc:         func() error { terms++; return nil },
		DeliveryCount:    9, // unreachable deliveries before this one do not count
		StreamSequence:   11,
	}

	cfg := newTestNATSBackendConfig()
	cfg.RetryPolicy = RetryWhileRetained
	cfg.MaxAttempts = 2
	cfg.Classifier = DefaultReplayClassifier
	var dropped int
	cfg.OnDrop = func(types.ReplayPayload, error) { dropped++ }
	b := &natsBackend{config: &cfg, deadLetters: make(map[uint64]int)}

	poison := errors.Join(types.ErrInvalidCluster, errors.New("target C"))
	b.settleRetained(msg, poison)
	assert.Equal(t, 1, naks, "first dead-letter attempt is still retried")
	assert.Equal(t, 0, terms)

	b.settleRetained(msg, poison)
	assert.Equal(t, 1, naks)
	assert.Equal(t, 1, terms, "second dead-letter attempt exhausts MaxAttempts=2")
	assert.Equal(t, 1, dropped)
	assert.Empty(t, b.deadLetters, "the counter is released with the message")
}

func TestRedeliverySchedule(t *testing.T) {
	schedule := redeliverySchedule(2*time.Second, 30*time.Second)
	require.Equal(t, []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second}, schedule)

	assert.Equal(t, []time.Duration{30 * time.Second}, redeliverySchedule(30*time.Second, time.Second),
		"a max delay below AckWait collapses to AckWait")
	assert.Len(t, redeliverySchedule(time.Millisecond, time.Hour), maxRedeliverySteps,
		"the schedule is capped in length")
}

// The checked constructors reject an unknown policy and a non-positive
// window, and the legacy constructors fall back to the defaults.
func TestWorkerRetryPolicyValidation(t *testing.T) {
	replayer := NewMemoryReplayer()
	execute := func(context.Context, types.ReplayPayload) error { return nil }

	_, err := NewMemoryWorkerChecked(replayer, execute,
		WithRetryPolicy(ReplayRetryPolicy(42)),
		WithRetryWindow(-time.Second),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WithRetryPolicy")
	assert.Contains(t, err.Error(), "WithRetryWindow")

	w := NewMemoryWorker(replayer, execute,
		WithRetryPolicy(ReplayRetryPolicy(42)),
		WithRetryWindow(-time.Second),
	)
	assert.Equal(t, RetryWhileRetained, w.config.RetryPolicy, "an invalid policy falls back to the default")
	assert.Equal(t, defaultRetryWindow, w.config.RetryWindow)
	assert.NotNil(t, w.config.Classifier)
}
