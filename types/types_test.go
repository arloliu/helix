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

func TestClusterError_NilCause(t *testing.T) {
	err := &ClusterError{Cluster: "A", Operation: "write"}

	// Must not panic
	assert.NotPanics(t, func() { _ = err.Error() })
	assert.Contains(t, err.Error(), "cluster A")
	assert.Contains(t, err.Error(), "write failed")
	assert.Contains(t, err.Error(), "<nil>")

	// Unwrap must not panic and must return nil for a nil Cause.
	assert.NotPanics(t, func() { _ = err.Unwrap() })
	assert.Nil(t, err.Unwrap())
	assert.False(t, errors.Is(err, errors.New("anything")))
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

func TestPartialWriteError(t *testing.T) {
	cause := errors.New("write timeout")
	err := &PartialWriteError{
		Acknowledged:   ClusterA,
		Unacknowledged: ClusterB,
		Cause:          cause,
	}

	assert.Contains(t, err.Error(), string(ClusterA))
	assert.Contains(t, err.Error(), string(ClusterB))
	assert.Contains(t, err.Error(), "write timeout")

	// Unwrap exposes the cause
	assert.True(t, errors.Is(err, cause))

	// errors.As through a wrapping error
	wrapped := fmt.Errorf("operation: %w", err)
	var target *PartialWriteError
	require.True(t, errors.As(wrapped, &target))
	assert.Equal(t, ClusterA, target.Acknowledged)
	assert.Equal(t, ClusterB, target.Unacknowledged)
}

func TestPartialWriteError_SentinelCause(t *testing.T) {
	err := &PartialWriteError{
		Acknowledged:   ClusterA,
		Unacknowledged: ClusterB,
		Cause:          ErrClusterDegraded,
	}
	assert.True(t, errors.Is(err, ErrClusterDegraded))
	assert.Contains(t, err.Error(), "strict write acknowledged on A but not on B")
}

func TestAsPartialWriteError(t *testing.T) {
	cause := errors.New("timeout")
	pwe := &PartialWriteError{Acknowledged: ClusterA, Unacknowledged: ClusterB, Cause: cause}

	got, ok := AsPartialWriteError(pwe)
	require.True(t, ok)
	assert.Equal(t, pwe, got)

	// Wrapped
	wrapped := fmt.Errorf("ctx: %w", pwe)
	got, ok = AsPartialWriteError(wrapped)
	require.True(t, ok)
	assert.Equal(t, pwe, got)

	// Unrelated error
	got, ok = AsPartialWriteError(errors.New("unrelated"))
	assert.False(t, ok)
	assert.Nil(t, got)

	// nil
	got, ok = AsPartialWriteError(nil)
	assert.False(t, ok)
	assert.Nil(t, got)
}

func TestIsPartialWrite(t *testing.T) {
	pwe := &PartialWriteError{Acknowledged: ClusterA, Unacknowledged: ClusterB, Cause: errors.New("err")}

	assert.True(t, IsPartialWrite(pwe))
	assert.True(t, IsPartialWrite(fmt.Errorf("wrapped: %w", pwe)))
	assert.False(t, IsPartialWrite(errors.New("unrelated")))
	assert.False(t, IsPartialWrite(nil))
}

func TestOptionError(t *testing.T) {
	err := &OptionError{
		Component: "policy.AdaptiveDualWrite",
		Option:    "WithAdaptiveStrikeThreshold",
		Reason:    "must be positive",
	}

	assert.Contains(t, err.Error(), "policy.AdaptiveDualWrite.WithAdaptiveStrikeThreshold")
	assert.Contains(t, err.Error(), "must be positive")
}

func TestOptionError_DefaultMessageParts(t *testing.T) {
	err := &OptionError{}

	assert.Contains(t, err.Error(), "unknown")
	assert.Contains(t, err.Error(), "invalid value")
}

func TestAsOptionError(t *testing.T) {
	optErr := &OptionError{
		Component: "policy.CircuitBreaker",
		Option:    "WithThreshold",
		Reason:    "must be between 1 and 2147483647",
	}

	got, ok := AsOptionError(optErr)
	require.True(t, ok)
	assert.Equal(t, optErr, got)

	wrapped := fmt.Errorf("context: %w", optErr)
	got, ok = AsOptionError(wrapped)
	require.True(t, ok)
	assert.Equal(t, optErr, got)

	joined := errors.Join(
		errors.New("irrelevant"),
		fmt.Errorf("wrapped: %w", optErr),
	)
	got, ok = AsOptionError(joined)
	require.True(t, ok)
	assert.Equal(t, optErr, got)

	got, ok = AsOptionError(errors.New("no option error"))
	assert.False(t, ok)
	assert.Nil(t, got)

	got, ok = AsOptionError(nil)
	assert.False(t, ok)
	assert.Nil(t, got)
}

func TestIsOptionError(t *testing.T) {
	optErr := &OptionError{Component: "x", Option: "y", Reason: "z"}

	assert.True(t, IsOptionError(optErr))
	assert.True(t, IsOptionError(fmt.Errorf("wrapped: %w", optErr)))
	assert.True(t, IsOptionError(errors.Join(errors.New("other"), optErr)))
	assert.False(t, IsOptionError(errors.New("unrelated")))
	assert.False(t, IsOptionError(nil))
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
		{"ErrNotFound", ErrNotFound, "not found"},
		{"ErrClusterDegraded", ErrClusterDegraded, "cluster is degraded"},
		{"ErrClusterDraining", ErrClusterDraining, "cluster is draining"},
		{"ErrStrictUnsupported", ErrStrictUnsupported, "does not support Strict()"},
		{"ErrStrictMirrorUnsupported", ErrStrictMirrorUnsupported, "Strict() and Mirror() cannot be combined"},
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

func TestIsNotFound(t *testing.T) {
	// Sentinel itself
	assert.True(t, IsNotFound(ErrNotFound))

	// Wrapped sentinel
	wrapped := fmt.Errorf("context: %w", ErrNotFound)
	assert.True(t, IsNotFound(wrapped))

	// Unrelated errors
	assert.False(t, IsNotFound(errors.New("some other error")))
	assert.False(t, IsNotFound(nil))

	// errors.Is works directly too
	assert.True(t, errors.Is(ErrNotFound, ErrNotFound))
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

func TestNoSynchronousAckError(t *testing.T) {
	err := &NoSynchronousAckError{ResultA: ErrWriteAsync, ResultB: ErrWriteDropped}
	assert.ErrorIs(t, err, ErrNoSynchronousAck)
	assert.ErrorIs(t, err, ErrWriteAsync)
	assert.ErrorIs(t, err, ErrWriteDropped)
	assert.NotErrorIs(t, err, ErrNoReplayer)
	assert.Contains(t, err.Error(), ErrWriteAsync.Error())
	assert.NotContains(t, err.Error(), "replay:")

	var target *NoSynchronousAckError
	assert.True(t, errors.As(fmt.Errorf("wrap: %w", err), &target))

	withReplay := &NoSynchronousAckError{ResultA: ErrWriteAsync, ResultB: nil, Replay: ErrNoReplayer}
	assert.ErrorIs(t, withReplay, ErrNoReplayer)
	assert.Contains(t, withReplay.Error(), "B: <nil>")
	assert.Contains(t, withReplay.Error(), "replay: "+ErrNoReplayer.Error())
}
