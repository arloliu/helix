package policy

import (
	"errors"
	"testing"
	"time"

	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLatencyCircuitBreaker_Defaults(t *testing.T) {
	lcb := NewLatencyCircuitBreaker()

	assert.Equal(t, 2*time.Second, lcb.AbsoluteMax())
	// Verify default behavior: should NOT fail over until 3 failures
	lcb.RecordFailure(types.ClusterA)
	lcb.RecordFailure(types.ClusterA)
	assert.False(t, lcb.ShouldFailover(types.ClusterA, nil), "should not failover before threshold")
	lcb.RecordFailure(types.ClusterA)
	assert.True(t, lcb.ShouldFailover(types.ClusterA, nil), "should failover after 3 failures")
}

func TestLatencyCircuitBreaker_CustomOptions(t *testing.T) {
	lcb := NewLatencyCircuitBreaker(
		WithLatencyAbsoluteMax(500*time.Millisecond),
		WithLatencyThreshold(5),
		WithLatencyResetTimeout(10*time.Second),
	)

	assert.Equal(t, 500*time.Millisecond, lcb.AbsoluteMax())
	// Verify custom threshold: should NOT fail over until 5 failures
	for range 4 {
		lcb.RecordFailure(types.ClusterA)
	}
	assert.False(t, lcb.ShouldFailover(types.ClusterA, nil), "should not failover before threshold=5")
	lcb.RecordFailure(types.ClusterA)
	assert.True(t, lcb.ShouldFailover(types.ClusterA, nil), "should failover after 5 failures")
}

func TestLatencyCircuitBreaker_InvalidOptionsPreserveDefaults(t *testing.T) {
	lcb := NewLatencyCircuitBreaker(
		WithLatencyAbsoluteMax(0),
		WithLatencyThreshold(-1),
		WithLatencyResetTimeout(-time.Second),
		WithLatencyMetrics(nil),
		WithLatencyLogger(nil),
	)

	assert.Equal(t, 2*time.Second, lcb.AbsoluteMax())
	assert.Equal(t, 3, lcb.threshold)
	assert.Equal(t, 30*time.Second, lcb.resetTimeout)
	assert.False(t, lcb.MetricsConfigured())
	assert.False(t, lcb.LoggerConfigured())

	lcb.RecordLatency(types.ClusterA, 3*time.Second)
	lcb.RecordLatency(types.ClusterA, 3*time.Second)
	assert.False(t, lcb.ShouldFailover(types.ClusterA, nil),
		"invalid threshold must not make latency breaker fail over early")

	assert.NotPanics(t, func() {
		lcb.RecordLatency(types.ClusterA, 3*time.Second)
	})
	assert.True(t, lcb.ShouldFailover(types.ClusterA, nil))
}

func TestLatencyCircuitBreakerChecked_InvalidOptionsReturnJoinedErrors(t *testing.T) {
	lcb, err := NewLatencyCircuitBreakerChecked(
		WithLatencyAbsoluteMax(0),
		WithLatencyThreshold(0),
		WithLatencyResetTimeout(-time.Second),
	)

	require.Nil(t, lcb)
	require.Error(t, err)
	assert.True(t, types.IsOptionError(err))

	var optionErr *types.OptionError
	require.True(t, errors.As(err, &optionErr))
	assert.Equal(t, latencyCircuitComponent, optionErr.Component)

	assert.Contains(t, err.Error(), "WithLatencyAbsoluteMax")
	assert.Contains(t, err.Error(), "WithLatencyThreshold")
	assert.Contains(t, err.Error(), "WithLatencyResetTimeout")
}

func TestLatencyCircuitBreakerChecked_ValidOptions(t *testing.T) {
	lcb, err := NewLatencyCircuitBreakerChecked(
		WithLatencyAbsoluteMax(500*time.Millisecond),
		WithLatencyThreshold(4),
		WithLatencyResetTimeout(10*time.Second),
	)

	require.NoError(t, err)
	require.NotNil(t, lcb)
	assert.Equal(t, 500*time.Millisecond, lcb.absoluteMax)
	assert.Equal(t, 4, lcb.threshold)
	assert.Equal(t, 10*time.Second, lcb.resetTimeout)
}

func TestLatencyCircuitBreaker_RecordLatency_BelowThreshold(t *testing.T) {
	lcb := NewLatencyCircuitBreaker(
		WithLatencyAbsoluteMax(1*time.Second),
		WithLatencyThreshold(3),
	)

	// Record latencies below threshold - should call RecordSuccess
	lcb.RecordLatency(types.ClusterA, 100*time.Millisecond)
	lcb.RecordLatency(types.ClusterA, 200*time.Millisecond)
	lcb.RecordLatency(types.ClusterA, 500*time.Millisecond)

	// Should not trigger failover
	assert.False(t, lcb.ShouldFailover(types.ClusterA, nil))
	assert.Equal(t, 0, lcb.Failures(types.ClusterA))
}

func TestLatencyCircuitBreaker_RecordLatency_AboveThreshold(t *testing.T) {
	lcb := NewLatencyCircuitBreaker(
		WithLatencyAbsoluteMax(1*time.Second),
		WithLatencyThreshold(3),
	)

	// Record latencies above threshold - should count as failures
	lcb.RecordLatency(types.ClusterA, 1500*time.Millisecond)
	assert.Equal(t, 1, lcb.Failures(types.ClusterA))

	lcb.RecordLatency(types.ClusterA, 2000*time.Millisecond)
	assert.Equal(t, 2, lcb.Failures(types.ClusterA))

	lcb.RecordLatency(types.ClusterA, 3000*time.Millisecond)
	assert.Equal(t, 3, lcb.Failures(types.ClusterA))

	// Now should trigger failover
	assert.True(t, lcb.ShouldFailover(types.ClusterA, nil))
}

func TestLatencyCircuitBreaker_MixedLatencies(t *testing.T) {
	lcb := NewLatencyCircuitBreaker(
		WithLatencyAbsoluteMax(1*time.Second),
		WithLatencyThreshold(3),
	)

	// Two slow, then one fast (resets counter)
	lcb.RecordLatency(types.ClusterA, 1500*time.Millisecond)
	lcb.RecordLatency(types.ClusterA, 2000*time.Millisecond)
	assert.Equal(t, 2, lcb.Failures(types.ClusterA))

	lcb.RecordLatency(types.ClusterA, 500*time.Millisecond) // Fast - resets
	assert.Equal(t, 0, lcb.Failures(types.ClusterA))

	// Should not trigger failover
	assert.False(t, lcb.ShouldFailover(types.ClusterA, nil))
}

func TestLatencyCircuitBreaker_ErrorsAndLatency(t *testing.T) {
	lcb := NewLatencyCircuitBreaker(
		WithLatencyAbsoluteMax(1*time.Second),
		WithLatencyThreshold(3),
	)

	// Mix of hard failures and slow responses
	lcb.RecordFailure(types.ClusterA)                        // Hard failure
	lcb.RecordLatency(types.ClusterA, 1500*time.Millisecond) // Slow (soft failure)
	lcb.RecordFailure(types.ClusterA)                        // Hard failure

	assert.Equal(t, 3, lcb.Failures(types.ClusterA))
	assert.True(t, lcb.ShouldFailover(types.ClusterA, nil))
}

func TestLatencyCircuitBreaker_IndependentClusters(t *testing.T) {
	lcb := NewLatencyCircuitBreaker(
		WithLatencyAbsoluteMax(1*time.Second),
		WithLatencyThreshold(2),
	)

	// Cluster A is slow
	lcb.RecordLatency(types.ClusterA, 1500*time.Millisecond)
	lcb.RecordLatency(types.ClusterA, 2000*time.Millisecond)

	// Cluster B is fast
	lcb.RecordLatency(types.ClusterB, 100*time.Millisecond)
	lcb.RecordLatency(types.ClusterB, 200*time.Millisecond)

	// Only A should trigger failover
	assert.True(t, lcb.ShouldFailover(types.ClusterA, nil))
	assert.False(t, lcb.ShouldFailover(types.ClusterB, nil))
}

// TestLatencyCircuitBreaker_WithMetrics verifies that WithLatencyMetrics wires a
// real collector so circuit trip and state-change events are recorded.
func TestLatencyCircuitBreaker_WithMetrics(t *testing.T) {
	m := testutil.NewTestMetricsCollector()
	lcb := NewLatencyCircuitBreaker(
		WithLatencyAbsoluteMax(100*time.Millisecond),
		WithLatencyThreshold(2),
		WithLatencyMetrics(m),
	)

	// Two slow latencies trip the circuit.
	lcb.RecordLatency(types.ClusterA, 200*time.Millisecond)
	lcb.RecordLatency(types.ClusterA, 200*time.Millisecond)

	assert.Equal(t, int64(1), m.CircuitBreakerTrips[types.ClusterA],
		"circuit trip metric must be recorded when threshold is reached")
	assert.Equal(t, 2, m.CircuitBreakerState[types.ClusterA],
		"circuit breaker state must be 2 (open) after tripping")

	// A fast latency closes the circuit.
	lcb.RecordLatency(types.ClusterA, 50*time.Millisecond)
	assert.Equal(t, 0, m.CircuitBreakerState[types.ClusterA],
		"circuit breaker state must be 0 (closed) after recovery")
}

// TestLatencyCircuitBreaker_WithLogger verifies that WithLatencyLogger wires a
// logger so circuit events produce log output. We verify indirectly: if the
// logger were nil the call would panic; not panicking confirms the logger is used.
func TestLatencyCircuitBreaker_WithLogger(t *testing.T) {
	logged := make([]string, 0)
	lcb := NewLatencyCircuitBreaker(
		WithLatencyAbsoluteMax(100*time.Millisecond),
		WithLatencyThreshold(1),
		WithLatencyLogger(&captureLogger{messages: &logged}),
	)

	lcb.RecordLatency(types.ClusterA, 200*time.Millisecond) // trips circuit

	require.Len(t, logged, 1, "expected one log message for circuit trip")
	assert.Contains(t, logged[0], "circuit breaker tripped")
}

// captureLogger records Warn messages for test assertions.
type captureLogger struct {
	messages *[]string
}

func (l *captureLogger) Debug(_ string, _ ...any) {}
func (l *captureLogger) Info(_ string, _ ...any)  {}
func (l *captureLogger) Warn(msg string, _ ...any) {
	*l.messages = append(*l.messages, msg)
}
func (l *captureLogger) Error(_ string, _ ...any) {}
func (l *captureLogger) Fatal(_ string, _ ...any) {}

func TestLatencyCircuitBreaker_ImplementsFailoverPolicy(t *testing.T) {
	// Verify LatencyCircuitBreaker can be used where FailoverPolicy is expected
	var _ interface {
		ShouldFailover(types.ClusterID, error) bool
		RecordFailure(types.ClusterID)
		RecordSuccess(types.ClusterID)
	} = NewLatencyCircuitBreaker()
}

// TestLatencyCircuitBreaker_ZeroValueDoesNotPanic verifies that a
// LatencyCircuitBreaker built without NewLatencyCircuitBreaker (leaving the
// embedded *CircuitBreaker nil) is safe to call — mirroring the zero-value
// safety guarantee documented on CircuitBreaker itself.
func TestLatencyCircuitBreaker_ZeroValueDoesNotPanic(t *testing.T) {
	lcb := &LatencyCircuitBreaker{}

	assert.NotPanics(t, func() {
		lcb.RecordLatency(types.ClusterA, 5*time.Second)
	})
	assert.NotPanics(t, func() {
		lcb.RecordFailure(types.ClusterA)
	})
	assert.NotPanics(t, func() {
		lcb.RecordSuccess(types.ClusterA)
	})
	assert.NotPanics(t, func() {
		assert.False(t, lcb.ShouldFailover(types.ClusterA, nil))
	})
	assert.NotPanics(t, func() {
		assert.Equal(t, 0, lcb.Failures(types.ClusterA))
	})
	assert.NotPanics(t, func() {
		lcb.SetClusterNames(types.DefaultClusterNames())
	})
	assert.NotPanics(t, func() {
		assert.False(t, lcb.MetricsConfigured())
	})
	assert.NotPanics(t, func() {
		lcb.SetMetrics(testutil.NewTestMetricsCollector())
	})
	assert.NotPanics(t, func() {
		assert.False(t, lcb.LoggerConfigured())
	})
	assert.NotPanics(t, func() {
		lcb.SetLogger(&captureLogger{messages: &[]string{}})
	})
}

func TestLatencyCircuitBreaker_ResetTimeout(t *testing.T) {
	lcb := NewLatencyCircuitBreaker(
		WithLatencyAbsoluteMax(100*time.Millisecond),
		WithLatencyThreshold(3),
		WithLatencyResetTimeout(50*time.Millisecond),
	)

	// Accumulate some failures
	lcb.RecordLatency(types.ClusterA, 200*time.Millisecond)
	lcb.RecordLatency(types.ClusterA, 200*time.Millisecond)
	require.Equal(t, 2, lcb.Failures(types.ClusterA))

	// Wait for reset timeout
	time.Sleep(60 * time.Millisecond)

	// Next failure should reset counter to 1
	lcb.RecordLatency(types.ClusterA, 200*time.Millisecond)
	assert.Equal(t, 1, lcb.Failures(types.ClusterA))
}
