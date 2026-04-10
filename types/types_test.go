package types

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterError(t *testing.T) {
	cause := errors.New("unavailable")
	err := &ClusterError{
		Cluster:   "A",
		Operation: "write",
		Cause:     cause,
	}

	assert.Contains(t, err.Error(), "cluster A")
	assert.Contains(t, err.Error(), "write failed")
	assert.Contains(t, err.Error(), "unavailable")
	assert.True(t, errors.Is(err, cause))
}

func TestDualClusterError(t *testing.T) {
	errA := errors.New("cluster A down")
	errB := errors.New("cluster B down")

	err := &DualClusterError{
		ErrorA: errA,
		ErrorB: errB,
	}

	assert.Contains(t, err.Error(), "both clusters failed")
	assert.Contains(t, err.Error(), "cluster A down")
	assert.Contains(t, err.Error(), "cluster B down")

	require.True(t, errors.Is(err, ErrBothClustersFailed))

	// errors.Is propagates through both wrapped errors
	assert.True(t, errors.Is(err, errA))
	assert.True(t, errors.Is(err, errB))

	// errors.As extracts *DualClusterError from a wrapping error
	wrapped := fmt.Errorf("operation failed: %w", err)
	var target *DualClusterError
	require.True(t, errors.As(wrapped, &target))
	assert.Equal(t, errA, target.ErrorA)
	assert.Equal(t, errB, target.ErrorB)
}

func TestDualClusterError_NilErrorA(t *testing.T) {
	errB := errors.New("cluster B down")
	err := &DualClusterError{ErrorA: nil, ErrorB: errB}

	// Must not panic
	assert.NotPanics(t, func() { _ = err.Error() })
	assert.Contains(t, err.Error(), "<nil>")
	assert.Contains(t, err.Error(), "cluster B down")

	// ErrBothClustersFailed always present; nil cluster excluded from Unwrap
	assert.True(t, errors.Is(err, ErrBothClustersFailed))
	assert.True(t, errors.Is(err, errB))
}

func TestDualClusterError_NilErrorB(t *testing.T) {
	errA := errors.New("cluster A down")
	err := &DualClusterError{ErrorA: errA, ErrorB: nil}

	assert.NotPanics(t, func() { _ = err.Error() })
	assert.Contains(t, err.Error(), "cluster A down")
	assert.Contains(t, err.Error(), "<nil>")

	assert.True(t, errors.Is(err, ErrBothClustersFailed))
	assert.True(t, errors.Is(err, errA))
}

func TestDualClusterError_BothNil(t *testing.T) {
	err := &DualClusterError{}

	assert.NotPanics(t, func() { _ = err.Error() })
	assert.Contains(t, err.Error(), "both clusters failed")

	// Only ErrBothClustersFailed in Unwrap when both nil
	assert.True(t, errors.Is(err, ErrBothClustersFailed))
	assert.Equal(t, []error{ErrBothClustersFailed}, err.Unwrap())
}

func TestSentinelErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		msg  string
	}{
		{"ErrBothClustersFailed", ErrBothClustersFailed, "write failed on both clusters"},
		{"ErrBothClustersDraining", ErrBothClustersDraining, "both clusters are draining"},
		{"ErrSessionClosed", ErrSessionClosed, "session is closed"},
		{"ErrReplayQueueFull", ErrReplayQueueFull, "replay queue is full"},
		{"ErrNilSession", ErrNilSession, "session cannot be nil"},
		{"ErrWriteAsync", ErrWriteAsync, "write sent asynchronously"},
		{"ErrWriteDropped", ErrWriteDropped, "write dropped"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Contains(t, tt.err.Error(), tt.msg)
			// All sentinel errors must be checkable with errors.Is
			assert.True(t, errors.Is(tt.err, tt.err))
			// Wrapping must preserve identity
			wrapped := fmt.Errorf("context: %w", tt.err)
			assert.True(t, errors.Is(wrapped, tt.err))
		})
	}
}

func TestClusterIDConstants(t *testing.T) {
	assert.Equal(t, ClusterID("A"), ClusterA)
	assert.Equal(t, ClusterID("B"), ClusterB)
}

func TestConsistencyConstants(t *testing.T) {
	assert.Equal(t, Consistency(0x01), One)
	assert.Equal(t, Consistency(0x04), Quorum)
	assert.Equal(t, Consistency(0x06), LocalQuorum)
}

func TestBatchTypeConstants(t *testing.T) {
	assert.Equal(t, BatchType(0), LoggedBatch)
	assert.Equal(t, BatchType(1), UnloggedBatch)
	assert.Equal(t, BatchType(2), CounterBatch)
}

func TestPriorityLevelConstants(t *testing.T) {
	assert.Equal(t, PriorityLevel(0), PriorityHigh)
	assert.Equal(t, PriorityLevel(1), PriorityLow)
}
