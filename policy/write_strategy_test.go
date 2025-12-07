package policy

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

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
