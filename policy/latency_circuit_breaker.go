package policy

import (
	"time"

	"github.com/arloliu/helix/types"
)

// LatencyCircuitBreaker extends CircuitBreaker with latency awareness.
//
// In addition to tracking consecutive errors, this policy also treats
// slow responses (latency > absoluteMax) as "soft failures". This helps
// detect degraded clusters that are technically responding but too slow
// to be useful.
//
// This policy implements the LatencyRecorder interface, which means the
// Helix client automatically calls RecordLatency() after successful read
// operations. No manual integration is required.
//
// Example:
//
//	lcb := policy.NewLatencyCircuitBreaker(
//	    policy.WithLatencyAbsoluteMax(2 * time.Second),
//	    policy.WithLatencyThreshold(3),
//	)
//
//	client, _ := helix.NewCQLClient(ctx, sessionA, sessionB,
//	    helix.WithFailoverPolicy(lcb),
//	)
//	// Latency is recorded automatically on each read!
type LatencyCircuitBreaker struct {
	*CircuitBreaker
	absoluteMax time.Duration
}

// LatencyCircuitBreakerOption configures a LatencyCircuitBreaker policy.
type LatencyCircuitBreakerOption func(*LatencyCircuitBreaker)

// WithLatencyAbsoluteMax sets the latency threshold for soft failures.
//
// If a successful operation takes longer than this duration, it is
// treated as a failure for circuit breaker purposes.
//
// Default: 2s
//
// Parameters:
//   - d: Maximum acceptable latency
//
// Returns:
//   - LatencyCircuitBreakerOption: Configuration option
func WithLatencyAbsoluteMax(d time.Duration) LatencyCircuitBreakerOption {
	return func(l *LatencyCircuitBreaker) {
		l.absoluteMax = d
	}
}

// WithLatencyThreshold sets the number of consecutive failures before failover.
//
// This applies to both hard failures (errors) and soft failures (slow responses).
//
// Default: 3
//
// Parameters:
//   - n: Number of failures required
//
// Returns:
//   - LatencyCircuitBreakerOption: Configuration option
func WithLatencyThreshold(n int) LatencyCircuitBreakerOption {
	return func(l *LatencyCircuitBreaker) {
		l.threshold = n
	}
}

// WithLatencyResetTimeout sets the duration after which failure count resets.
//
// Default: 30s
//
// Parameters:
//   - d: Reset timeout duration
//
// Returns:
//   - LatencyCircuitBreakerOption: Configuration option
func WithLatencyResetTimeout(d time.Duration) LatencyCircuitBreakerOption {
	return func(l *LatencyCircuitBreaker) {
		l.resetTimeout = d
	}
}

// NewLatencyCircuitBreaker creates a new LatencyCircuitBreaker policy.
//
// Defaults:
//   - absoluteMax: 2s
//   - threshold: 3 (inherited from CircuitBreaker)
//   - resetTimeout: 30s (inherited from CircuitBreaker)
//
// Parameters:
//   - opts: Optional configuration options
//
// Returns:
//   - *LatencyCircuitBreaker: A new latency-aware circuit breaker policy
func NewLatencyCircuitBreaker(opts ...LatencyCircuitBreakerOption) *LatencyCircuitBreaker {
	l := &LatencyCircuitBreaker{
		CircuitBreaker: NewCircuitBreaker(),
		absoluteMax:    2 * time.Second,
	}

	for _, opt := range opts {
		opt(l)
	}

	return l
}

// RecordLatency checks if the operation latency exceeds the absolute maximum.
//
// If latency > absoluteMax, this is treated as a "soft failure" and
// RecordFailure() is called internally. Otherwise, RecordSuccess() is called
// to reset the failure counter.
//
// This method should be called after successful operations to provide
// latency data for health tracking.
//
// Parameters:
//   - cluster: The cluster that was accessed
//   - latency: The operation latency
func (l *LatencyCircuitBreaker) RecordLatency(cluster types.ClusterID, latency time.Duration) {
	if latency > l.absoluteMax {
		l.RecordFailure(cluster)
	} else {
		l.RecordSuccess(cluster)
	}
}

// AbsoluteMax returns the configured latency threshold.
//
// Returns:
//   - time.Duration: The absolute maximum latency threshold
func (l *LatencyCircuitBreaker) AbsoluteMax() time.Duration {
	return l.absoluteMax
}
