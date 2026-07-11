package policy

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// BenchmarkConcurrentDualWrite_Execute measures goroutine-spawn overhead for
// the common healthy-cluster path. Write funcs are no-ops so the benchmark
// isolates Execute's own allocation/scheduling cost.
func BenchmarkConcurrentDualWrite_Execute(b *testing.B) {
	strategy := NewConcurrentDualWrite()
	ctx := context.Background()
	noop := func(_ context.Context) error { return nil }

	for b.Loop() {
		_, _ = strategy.Execute(ctx, noop, noop)
	}
}

func TestConcurrentDualWriteBothSucceed(t *testing.T) {
	strategy := NewConcurrentDualWrite()

	var callCountA, callCountB atomic.Int32

	errA, errB := strategy.Execute(
		context.Background(),
		func(_ context.Context) error {
			callCountA.Add(1)
			return nil
		},
		func(_ context.Context) error {
			callCountB.Add(1)
			return nil
		},
	)

	require.NoError(t, errA)
	require.NoError(t, errB)
	require.Equal(t, int32(1), callCountA.Load())
	require.Equal(t, int32(1), callCountB.Load())
}

// TestConcurrentDualWrite_Execute_BothWritesRunConcurrently is a regression
// test for the write_strategy.go alloc-perf finding: Execute now runs writeA
// inline on the calling goroutine (instead of spawning a second goroutine
// for it) while writeB still runs in a spawned goroutine. This must not
// turn the writes sequential — both must still be in flight at the same
// time, proven by blocking both write funcs and asserting BOTH report entry
// before either is released.
func TestConcurrentDualWrite_Execute_BothWritesRunConcurrently(t *testing.T) {
	strategy := NewConcurrentDualWrite()

	enteredA := make(chan struct{}, 1)
	enteredB := make(chan struct{}, 1)
	release := make(chan struct{})

	//nolint:unparam // must match the func(context.Context) error write-func signature Execute requires
	writeA := func(_ context.Context) error {
		enteredA <- struct{}{}
		<-release
		return nil
	}
	//nolint:unparam // must match the func(context.Context) error write-func signature Execute requires
	writeB := func(_ context.Context) error {
		enteredB <- struct{}{}
		<-release
		return nil
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = strategy.Execute(context.Background(), writeA, writeB)
	}()

	timeout := time.After(2 * time.Second)
	for range 2 {
		select {
		case <-enteredA:
		case <-enteredB:
		case <-timeout:
			t.Fatal("both writes must be in flight concurrently — timed out waiting for entry")
		}
	}

	close(release)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Execute did not return after release")
	}
}

func TestConcurrentDualWriteOneFails(t *testing.T) {
	strategy := NewConcurrentDualWrite()

	expectedErr := errors.New("cluster A failed")

	errA, errB := strategy.Execute(
		context.Background(),
		func(_ context.Context) error {
			return expectedErr
		},
		func(_ context.Context) error {
			return nil
		},
	)

	require.ErrorIs(t, errA, expectedErr)
	require.NoError(t, errB)
}

func TestConcurrentDualWriteBothFail(t *testing.T) {
	strategy := NewConcurrentDualWrite()

	errExpectedA := errors.New("cluster A failed")
	errExpectedB := errors.New("cluster B failed")

	errA, errB := strategy.Execute(
		context.Background(),
		func(_ context.Context) error {
			return errExpectedA
		},
		func(_ context.Context) error {
			return errExpectedB
		},
	)

	require.ErrorIs(t, errA, errExpectedA)
	require.ErrorIs(t, errB, errExpectedB)
}

func TestSyncDualWritePrimaryFirst(t *testing.T) {
	strategy := NewSyncDualWrite(WithPrimaryFirst())

	var order []string

	errA, errB := strategy.Execute(
		context.Background(),
		func(_ context.Context) error {
			order = append(order, "A")
			return nil
		},
		func(_ context.Context) error {
			order = append(order, "B")
			return nil
		},
	)

	require.NoError(t, errA)
	require.NoError(t, errB)
	require.Equal(t, []string{"A", "B"}, order)
}

// TestSyncDualWrite_CanceledContext_SkipsSecondWrite verifies that if the context
// is canceled after the first write returns, the second write is not called and
// ctx.Err() is returned for the skipped cluster.
func TestSyncDualWrite_CanceledContext_SkipsSecondWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	strategy := NewSyncDualWrite(WithPrimaryFirst())

	var calledB atomic.Bool

	errA, errB := strategy.Execute(ctx,
		func(_ context.Context) error {
			cancel() // cancel context during the first write
			return nil
		},
		func(_ context.Context) error {
			calledB.Store(true)
			return nil
		},
	)

	assert.NoError(t, errA, "cluster A write succeeded")
	assert.ErrorIs(t, errB, context.Canceled, "cluster B must get context.Canceled when context is canceled")
	assert.False(t, calledB.Load(), "cluster B write must not be called after context cancellation")
}

// TestSyncDualWrite_CanceledContext_SecondaryFirst verifies the same skip behavior
// when the secondary cluster writes first.
func TestSyncDualWrite_CanceledContext_SecondaryFirst(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	strategy := NewSyncDualWrite(WithSecondaryFirst())

	var calledA atomic.Bool

	errA, errB := strategy.Execute(ctx,
		func(_ context.Context) error {
			calledA.Store(true)
			return nil
		},
		func(_ context.Context) error {
			cancel() // cancel context during the first write (B goes first)
			return nil
		},
	)

	assert.ErrorIs(t, errA, context.Canceled, "cluster A must get context.Canceled when context is canceled")
	assert.NoError(t, errB, "cluster B write succeeded")
	assert.False(t, calledA.Load(), "cluster A write must not be called after context cancellation")
}

func TestSyncDualWriteSecondaryFirst(t *testing.T) {
	strategy := NewSyncDualWrite(WithSecondaryFirst())

	var order []string

	errA, errB := strategy.Execute(
		context.Background(),
		func(_ context.Context) error {
			order = append(order, "A")
			return nil
		},
		func(_ context.Context) error {
			order = append(order, "B")
			return nil
		},
	)

	require.NoError(t, errA)
	require.NoError(t, errB)
	require.Equal(t, []string{"B", "A"}, order)
}

// TestConcurrentDualWrite_PanicInA verifies that a panic in cluster A's write
// is recovered and returned as an error rather than deadlocking wg.Wait().
func TestConcurrentDualWrite_PanicInA(t *testing.T) {
	strategy := NewConcurrentDualWrite()

	errA, errB := strategy.Execute(
		context.Background(),
		func(_ context.Context) error {
			panic("driver exploded")
		},
		func(_ context.Context) error {
			return nil
		},
	)

	require.Error(t, errA)
	assert.Contains(t, errA.Error(), "panic")
	assert.Contains(t, errA.Error(), "driver exploded")
	require.NoError(t, errB)
}

// TestConcurrentDualWrite_PanicInBoth verifies both goroutines recover independently.
func TestConcurrentDualWrite_PanicInBoth(t *testing.T) {
	strategy := NewConcurrentDualWrite()

	done := make(chan struct{})
	go func() {
		defer close(done)
		errA, errB := strategy.Execute(
			context.Background(),
			func(_ context.Context) error { panic("A exploded") },
			func(_ context.Context) error { panic("B exploded") },
		)
		assert.Error(t, errA)
		assert.Error(t, errB)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Execute deadlocked after panic in both write functions")
	}
}

// TestSyncDualWrite_PanicInA verifies that a panic in the first write (A) is
// recovered and returned as an error; the second write is not called.
func TestSyncDualWrite_PanicInA(t *testing.T) {
	strategy := NewSyncDualWrite(WithPrimaryFirst())

	var calledB atomic.Bool

	errA, errB := strategy.Execute(
		context.Background(),
		func(_ context.Context) error {
			panic("A panicked")
		},
		func(_ context.Context) error {
			calledB.Store(true)
			return nil
		},
	)

	require.Error(t, errA)
	assert.Contains(t, errA.Error(), "panic")
	assert.Contains(t, errA.Error(), "A panicked")
	// Second write skipped because context is not canceled — but A returned error
	// (not nil), so B still runs. Verify B was called and succeeded.
	require.NoError(t, errB)
	assert.True(t, calledB.Load(), "B should still be called when A panics (context not canceled)")
}

// TestSyncDualWrite_PanicInB verifies that a panic in the second write (B) is
// recovered and returned as an error without affecting the first write result.
func TestSyncDualWrite_PanicInB(t *testing.T) {
	strategy := NewSyncDualWrite(WithPrimaryFirst())

	errA, errB := strategy.Execute(
		context.Background(),
		func(_ context.Context) error {
			return nil
		},
		func(_ context.Context) error {
			panic("B panicked")
		},
	)

	require.NoError(t, errA)
	require.Error(t, errB)
	assert.Contains(t, errB.Error(), "panic")
	assert.Contains(t, errB.Error(), "B panicked")
}

func TestConcurrentDualWrite_ExecuteStrict_BothSucceed(t *testing.T) {
	s := NewConcurrentDualWrite()
	errA, errB := s.ExecuteStrict(t.Context(),
		func(_ context.Context) error { return nil },
		func(_ context.Context) error { return nil },
	)
	assert.NoError(t, errA)
	assert.NoError(t, errB)
}

func TestConcurrentDualWrite_ExecuteStrict_OneFails(t *testing.T) {
	s := NewConcurrentDualWrite()
	want := errors.New("A fail")
	errA, errB := s.ExecuteStrict(t.Context(),
		func(_ context.Context) error { return want },
		func(_ context.Context) error { return nil },
	)
	assert.ErrorIs(t, errA, want)
	assert.NoError(t, errB)
}

func TestSyncDualWrite_ExecuteStrict_BothSucceed(t *testing.T) {
	s := NewSyncDualWrite()
	errA, errB := s.ExecuteStrict(t.Context(),
		func(_ context.Context) error { return nil },
		func(_ context.Context) error { return nil },
	)
	assert.NoError(t, errA)
	assert.NoError(t, errB)
}

func TestSyncDualWrite_ExecuteStrict_OneFails(t *testing.T) {
	s := NewSyncDualWrite()
	want := errors.New("B fail")
	errA, errB := s.ExecuteStrict(t.Context(),
		func(_ context.Context) error { return nil },
		func(_ context.Context) error { return want },
	)
	assert.NoError(t, errA)
	assert.ErrorIs(t, errB, want)
}

// TestSafeWrite_PanicErrorIncludesStack verifies that the recovered panic
// error embeds the captured goroutine stack, not just the panic value. The
// stack is the only useful debugging signal when the panic originated several
// frames deep in driver or caller code.
func TestSafeWrite_PanicErrorIncludesStack(t *testing.T) {
	strategy := NewConcurrentDualWrite()

	errA, _ := strategy.Execute(
		context.Background(),
		func(_ context.Context) error { panic("kaboom") },
		func(_ context.Context) error { return nil },
	)

	require.Error(t, errA)
	assert.Contains(t, errA.Error(), "kaboom")
	assert.Contains(t, errA.Error(), "goroutine ",
		"stack trace marker missing — panic recovery should include runtime.Stack output")
	assert.Contains(t, errA.Error(), "safeWrite",
		"stack trace must reference the recovery frame")
}
