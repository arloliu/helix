package scenarios

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Condition already true on first check — must return nil immediately without
// waiting for the first ticker tick.
func TestWaitUntil_ImmediateTrue(t *testing.T) {
	ctx := context.Background()
	err := waitUntil(ctx, 5*time.Second, func() bool { return true })
	if err != nil {
		t.Errorf("waitUntil() = %v, want nil for immediately-true condition", err)
	}
}

// Condition becomes true before the timeout — must return nil.
func TestWaitUntil_EventuallyTrue(t *testing.T) {
	ctx := context.Background()
	var count int
	err := waitUntil(ctx, 5*time.Second, func() bool {
		count++
		return count >= 3 // becomes true after two polling ticks
	})
	if err != nil {
		t.Errorf("waitUntil() = %v, want nil when condition becomes true before timeout", err)
	}
}

// Context cancelled before condition becomes true — must return ctx.Err().
func TestWaitUntil_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before calling

	err := waitUntil(ctx, 5*time.Second, func() bool { return false })
	if !errors.Is(err, context.Canceled) {
		t.Errorf("waitUntil() = %v, want context.Canceled on pre-cancelled context", err)
	}
}

// Context cancelled while polling — must return context.Canceled.
func TestWaitUntil_ContextCancelledDuringPoll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := waitUntil(ctx, 10*time.Second, func() bool { return false })
	if !errors.Is(err, context.Canceled) {
		t.Errorf("waitUntil() = %v, want context.Canceled", err)
	}
}

// Timeout fires before condition becomes true — must return context.DeadlineExceeded.
func TestWaitUntil_Timeout(t *testing.T) {
	ctx := context.Background()
	err := waitUntil(ctx, 300*time.Millisecond, func() bool { return false })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("waitUntil() = %v, want context.DeadlineExceeded on timeout", err)
	}
}

// When the parent context has a deadline shorter than the waitUntil timeout,
// the parent's deadline must win and ctx.Err() is returned.
func TestWaitUntil_ParentContextDeadlineShorterThanTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := waitUntil(ctx, 10*time.Second, func() bool { return false })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("waitUntil() = %v, want context.DeadlineExceeded from parent ctx", err)
	}
}
