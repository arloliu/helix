package types

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartialWriteError(t *testing.T) {
	cause := errors.New("connection timeout")
	err := &PartialWriteError{
		SucceededCluster: "A",
		FailedCluster:    "B",
		Cause:            cause,
	}

	assert.Contains(t, err.Error(), "partial write")
	assert.Contains(t, err.Error(), "succeeded on A")
	assert.Contains(t, err.Error(), "failed on B")
	assert.Contains(t, err.Error(), "connection timeout")
	assert.True(t, errors.Is(err, cause))
}

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
}

func TestSentinelErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		msg  string
	}{
		{"ErrBothClustersFailed", ErrBothClustersFailed, "write failed on both clusters"},
		{"ErrNoAvailableCluster", ErrNoAvailableCluster, "no cluster available for read"},
		{"ErrSessionClosed", ErrSessionClosed, "session is closed"},
		{"ErrReplayQueueFull", ErrReplayQueueFull, "replay queue is full"},
		{"ErrInvalidBatchType", ErrInvalidBatchType, "invalid batch type"},
		{"ErrNilSession", ErrNilSession, "session cannot be nil"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Contains(t, tt.err.Error(), tt.msg)
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
