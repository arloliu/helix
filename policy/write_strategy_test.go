package policy

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
