package policy

import (
	"time"

	"github.com/arloliu/helix/types"
)

const defaultLatencyAbsoluteMax = 2 * time.Second

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
//
// Zero value: a bare LatencyCircuitBreaker{} never panics — every method is
// safe to call — but it is not functionally a circuit breaker: the embedded
// *CircuitBreaker is nil until one of the constructors runs. CircuitBreaker
// is embedded by pointer (not by value) so that copying a LatencyCircuitBreaker
// value only copies the pointer, not CircuitBreaker's mutexes/atomics —
// copying it, even before first use, would otherwise trip `go vet`
// copylocks and break existing external composite literals/selectors that
// depend on the pointer field shape. Because the embed can be nil, every
// promoted-looking method below (ShouldFailover, RecordFailure,
// RecordSuccess, Failures, SetClusterNames, MetricsConfigured, SetMetrics,
// SetEventEmitter, LoggerConfigured, SetLogger) is an explicit wrapper with a nil guard
// rather than a compiler-promoted method — use one of the constructors
// (NewLatencyCircuitBreaker / NewLatencyCircuitBreakerChecked) to get a
// fully configured, functional LatencyCircuitBreaker.
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
		if n > maxInt32 {
			l.threshold = 0
			return
		}
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

// WithLatencyMetrics sets the metrics collector for the latency circuit breaker.
//
// Without this option the circuit breaker uses a no-op collector, so circuit
// trip events are never recorded. Provide a real collector in production to
// observe open/close transitions.
//
// Parameters:
//   - m: The metrics collector
//
// Returns:
//   - LatencyCircuitBreakerOption: Configuration option
func WithLatencyMetrics(m types.MetricsCollector) LatencyCircuitBreakerOption {
	return func(l *LatencyCircuitBreaker) {
		if m == nil {
			return
		}
		l.metrics = m
		l.metricsExplicit = true
	}
}

// WithLatencyLogger sets the logger for the latency circuit breaker.
//
// Without this option the circuit breaker uses a no-op logger, so circuit
// trip/close events are never logged. Provide a real logger in production.
//
// Parameters:
//   - log: The logger
//
// Returns:
//   - LatencyCircuitBreakerOption: Configuration option
func WithLatencyLogger(log types.Logger) LatencyCircuitBreakerOption {
	return func(l *LatencyCircuitBreaker) {
		if log == nil {
			return
		}
		l.logger = log
		l.loggerExplicit = true
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
//
// For production configuration that should fail fast on invalid option values,
// use [NewLatencyCircuitBreakerChecked].
func NewLatencyCircuitBreaker(opts ...LatencyCircuitBreakerOption) *LatencyCircuitBreaker {
	l := newLatencyCircuitBreakerWithDefaults()
	applyLatencyCircuitBreakerOptions(l, opts...)
	normalizeLatencyCircuitBreakerForLegacy(l)
	finalizeCircuitBreaker(l.CircuitBreaker)

	return l
}

// NewLatencyCircuitBreakerChecked creates a new LatencyCircuitBreaker policy
// and returns a validation error when any option value is invalid.
//
// Parameters:
//   - opts: Optional configuration options
//
// Returns:
//   - *LatencyCircuitBreaker: A new latency-aware circuit breaker policy
//   - error: Joined [types.OptionError] values when one or more options are invalid
func NewLatencyCircuitBreakerChecked(opts ...LatencyCircuitBreakerOption) (*LatencyCircuitBreaker, error) {
	l := newLatencyCircuitBreakerWithDefaults()
	applyLatencyCircuitBreakerOptions(l, opts...)
	if err := validateLatencyCircuitBreaker(l); err != nil {
		return nil, err
	}
	finalizeCircuitBreaker(l.CircuitBreaker)

	return l, nil
}

func newLatencyCircuitBreakerWithDefaults() *LatencyCircuitBreaker {
	return &LatencyCircuitBreaker{
		CircuitBreaker: newCircuitBreakerWithDefaults(),
		absoluteMax:    defaultLatencyAbsoluteMax,
	}
}

func applyLatencyCircuitBreakerOptions(l *LatencyCircuitBreaker, opts ...LatencyCircuitBreakerOption) {
	for _, opt := range opts {
		opt(l)
	}
}

func validateLatencyCircuitBreaker(l *LatencyCircuitBreaker) error {
	errList := make([]error, 0, 3)

	if l.absoluteMax <= 0 {
		errList = append(errList, optionErrPositiveDuration(latencyCircuitComponent, "WithLatencyAbsoluteMax"))
	}
	if l.threshold <= 0 || l.threshold > maxInt32 {
		errList = append(errList, optionErrInt32Range(latencyCircuitComponent, "WithLatencyThreshold"))
	}
	if l.resetTimeout < 0 {
		errList = append(errList, optionErrNonNegativeDuration(latencyCircuitComponent, "WithLatencyResetTimeout"))
	}

	return joinValidationErrors(errList)
}

func normalizeLatencyCircuitBreakerForLegacy(l *LatencyCircuitBreaker) {
	normalizeCircuitBreakerForLegacy(l.CircuitBreaker)

	if l.absoluteMax <= 0 {
		l.absoluteMax = defaultLatencyAbsoluteMax
	}
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

// ShouldFailover returns true if the failure threshold has been reached
// AND the reset timeout has not yet elapsed since the last failure. See
// [CircuitBreaker.ShouldFailover] for the full half-open transition
// semantics. A zero-value LatencyCircuitBreaker (nil embedded
// *CircuitBreaker) safely returns false.
//
// Parameters:
//   - cluster: The cluster that failed
//   - err: The error (unused)
//
// Returns:
//   - bool: true if failover should occur
func (l *LatencyCircuitBreaker) ShouldFailover(cluster types.ClusterID, err error) bool {
	if l.CircuitBreaker == nil {
		return false
	}

	return l.CircuitBreaker.ShouldFailover(cluster, err)
}

// RecordFailure increments the failure counter for a cluster. See
// [CircuitBreaker.RecordFailure]. A zero-value LatencyCircuitBreaker (nil
// embedded *CircuitBreaker) safely no-ops.
//
// Parameters:
//   - cluster: The cluster that failed
func (l *LatencyCircuitBreaker) RecordFailure(cluster types.ClusterID) {
	if l.CircuitBreaker == nil {
		return
	}

	l.CircuitBreaker.RecordFailure(cluster)
}

// RecordSuccess resets the failure counter for a cluster. See
// [CircuitBreaker.RecordSuccess]. A zero-value LatencyCircuitBreaker (nil
// embedded *CircuitBreaker) safely no-ops.
//
// Parameters:
//   - cluster: The cluster that succeeded
func (l *LatencyCircuitBreaker) RecordSuccess(cluster types.ClusterID) {
	if l.CircuitBreaker == nil {
		return
	}

	l.CircuitBreaker.RecordSuccess(cluster)
}

// Failures returns the current failure count for a cluster. See
// [CircuitBreaker.Failures]. A zero-value LatencyCircuitBreaker (nil
// embedded *CircuitBreaker) safely returns 0.
//
// Parameters:
//   - cluster: The cluster to check
//
// Returns:
//   - int: Number of consecutive failures
func (l *LatencyCircuitBreaker) Failures(cluster types.ClusterID) int {
	if l.CircuitBreaker == nil {
		return 0
	}

	return l.CircuitBreaker.Failures(cluster)
}

// SetClusterNames sets custom display names for clusters in log messages.
// See [CircuitBreaker.SetClusterNames]. A zero-value LatencyCircuitBreaker
// (nil embedded *CircuitBreaker) safely no-ops.
//
// Parameters:
//   - names: The cluster names to use in log messages
func (l *LatencyCircuitBreaker) SetClusterNames(names types.ClusterNames) {
	if l.CircuitBreaker == nil {
		return
	}

	l.CircuitBreaker.SetClusterNames(names)
}

// MetricsConfigured reports whether the metrics collector was explicitly
// set via [WithLatencyMetrics]. See [CircuitBreaker.MetricsConfigured]. A
// zero-value LatencyCircuitBreaker (nil embedded *CircuitBreaker) safely
// returns false.
//
// Returns:
//   - bool: true if WithLatencyMetrics was called at construction time
func (l *LatencyCircuitBreaker) MetricsConfigured() bool {
	if l.CircuitBreaker == nil {
		return false
	}

	return l.CircuitBreaker.MetricsConfigured()
}

// SetMetrics replaces the metrics collector. See
// [CircuitBreaker.SetMetrics]. A zero-value LatencyCircuitBreaker (nil
// embedded *CircuitBreaker) safely no-ops.
//
// Parameters:
//   - m: The metrics collector to use
func (l *LatencyCircuitBreaker) SetMetrics(m types.MetricsCollector) {
	if l.CircuitBreaker == nil {
		return
	}

	l.CircuitBreaker.SetMetrics(m)
}

// SetEventEmitter sets the cluster event emitter used for circuit
// breaker open/close notifications. See [CircuitBreaker.SetEventEmitter]
// for the full contract. A zero-value LatencyCircuitBreaker (nil
// embedded *CircuitBreaker) safely no-ops, mirroring SetMetrics.
//
// Parameters:
//   - em: The event emitter; nil disables emission
func (l *LatencyCircuitBreaker) SetEventEmitter(em types.ClusterEventEmitter) {
	if l.CircuitBreaker == nil {
		return
	}

	l.CircuitBreaker.SetEventEmitter(em)
}

// LoggerConfigured reports whether the logger was explicitly set via
// [WithLatencyLogger]. See [CircuitBreaker.LoggerConfigured]. A zero-value
// LatencyCircuitBreaker (nil embedded *CircuitBreaker) safely returns false.
//
// Returns:
//   - bool: true if WithLatencyLogger was called at construction time
func (l *LatencyCircuitBreaker) LoggerConfigured() bool {
	if l.CircuitBreaker == nil {
		return false
	}

	return l.CircuitBreaker.LoggerConfigured()
}

// SetLogger replaces the logger. See [CircuitBreaker.SetLogger]. A
// zero-value LatencyCircuitBreaker (nil embedded *CircuitBreaker) safely
// no-ops.
//
// Parameters:
//   - log: The logger to use
func (l *LatencyCircuitBreaker) SetLogger(log types.Logger) {
	if l.CircuitBreaker == nil {
		return
	}

	l.CircuitBreaker.SetLogger(log)
}
