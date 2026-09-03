package helix

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// openBreaker trips cb for cluster A and backdates its last failure so the
// next probe tick may reserve it.
func openBreaker(t *testing.T, cb *policy.CircuitBreaker, threshold int) {
	t.Helper()
	for range threshold {
		cb.RecordFailure(ClusterA)
	}
	require.True(t, cb.ShouldFailover(ClusterA, nil))
}

// TestFailoverProbe_ClosesOpenBreakerWithoutCallerReads verifies that a
// client whose write strategy never asks for probes still runs the probe
// loop for its breaker, and that the probe closes the breaker with its own
// reason while ShouldFailover stays true until then.
func TestFailoverProbe_ClosesOpenBreakerWithoutCallerReads(t *testing.T) {
	cb := policy.NewCircuitBreaker(
		policy.WithThreshold(2),
		policy.WithResetTimeout(10*time.Millisecond),
	)
	events := make(chan types.ClusterEvent, 16)
	var probes atomic.Int32
	probe := RecoveryProbe{
		Probe: func(context.Context, cql.Session) error {
			probes.Add(1)

			return nil
		},
		Interval: 5 * time.Millisecond,
		Timeout:  time.Second,
	}
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithWriteStrategy(policy.NewConcurrentDualWrite()),
		WithFailoverPolicy(cb),
		WithRecoveryProbe(probe),
		WithOnClusterEvent(func(ev types.ClusterEvent) { events <- ev }),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	openBreaker(t, cb, 2)
	require.Eventually(t, func() bool { return !cb.ShouldFailover(ClusterA, nil) },
		time.Second, time.Millisecond, "the recovery probe must close the breaker")
	require.GreaterOrEqual(t, probes.Load(), int32(1))

	var reasons []string
	deadline := time.After(time.Second)
	for len(reasons) < 1 {
		select {
		case ev := <-events:
			if ev.Kind == types.EventCircuitBreakerClosed {
				reasons = append(reasons, ev.Reason)
			}
		case <-deadline:
			t.Fatal("no circuit_breaker_closed event")
		}
	}
	require.Equal(t, []string{"probe succeeded"}, reasons)
}

// TestFailoverProbe_FailedProbeKeepsBreakerOpen verifies that a failing
// probe returns the breaker to open and that only a later successful probe
// closes it.
func TestFailoverProbe_FailedProbeKeepsBreakerOpen(t *testing.T) {
	cb := policy.NewCircuitBreaker(
		policy.WithThreshold(1),
		policy.WithResetTimeout(5*time.Millisecond),
	)
	var failing atomic.Bool
	failing.Store(true)
	var failedProbes atomic.Int32
	probe := RecoveryProbe{
		Probe: func(context.Context, cql.Session) error {
			if failing.Load() {
				failedProbes.Add(1)

				return errUnreachableForTest
			}

			return nil
		},
		Interval: 5 * time.Millisecond,
		Timeout:  time.Second,
	}
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithFailoverPolicy(cb),
		WithRecoveryProbe(probe),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	openBreaker(t, cb, 1)
	require.Eventually(t, func() bool { return failedProbes.Load() >= 3 },
		time.Second, time.Millisecond, "the breaker must be probed repeatedly while the cluster fails")
	require.True(t, cb.ShouldFailover(ClusterA, nil), "failed probes keep the breaker open")

	failing.Store(false)
	require.Eventually(t, func() bool { return !cb.ShouldFailover(ClusterA, nil) },
		time.Second, time.Millisecond, "the first successful probe closes the breaker")
}

// dualRoleStrategy is a write strategy that asks for probes and a failover
// policy that reserves them, so one physical probe must serve both roles.
type dualRoleStrategy struct {
	*policy.ConcurrentDualWrite
	*policy.CircuitBreaker
	degraded     atomic.Bool
	writeCredits atomic.Int32
}

func (d *dualRoleStrategy) IsDegraded(c ClusterID) bool  { return c == ClusterA && d.degraded.Load() }
func (d *dualRoleStrategy) RecordProbeSuccess(ClusterID) { d.writeCredits.Add(1) }

// TestFailoverProbe_OneProbeServesBothAuthorities verifies that a value
// configured in both probe roles receives one write credit and one breaker
// completion per physical probe.
func TestFailoverProbe_OneProbeServesBothAuthorities(t *testing.T) {
	both := &dualRoleStrategy{
		ConcurrentDualWrite: policy.NewConcurrentDualWrite(),
		CircuitBreaker: policy.NewCircuitBreaker(
			policy.WithThreshold(1),
			policy.WithResetTimeout(5*time.Millisecond),
		),
	}
	both.degraded.Store(true)
	var probes atomic.Int32
	gate := make(chan struct{})
	probe := RecoveryProbe{
		Probe: func(ctx context.Context, _ cql.Session) error {
			probes.Add(1)
			select {
			case <-gate:
			case <-ctx.Done():
			}

			return nil
		},
		Interval: 5 * time.Millisecond,
		Timeout:  time.Second,
	}
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithWriteStrategy(both),
		WithFailoverPolicy(both),
		WithRecoveryProbe(probe),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	openBreaker(t, both.CircuitBreaker, 1)
	// Let exactly one probe run to completion.
	require.Eventually(t, func() bool { return probes.Load() >= 1 }, time.Second, time.Millisecond)
	both.degraded.Store(false)
	close(gate)
	require.Eventually(t, func() bool { return !both.ShouldFailover(ClusterA, nil) },
		time.Second, time.Millisecond, "the probe closes the breaker")
	require.Equal(t, int32(1), both.writeCredits.Load(), "the same probe credited the write strategy exactly once")
}

// TestFailoverProbe_CloseAbandonsReservation verifies that a probe blocked
// inside its implementation when the client closes releases the breaker's
// reservation without counting a failure, so another client sharing the
// breaker can reserve it at once.
func TestFailoverProbe_CloseAbandonsReservation(t *testing.T) {
	cb := policy.NewCircuitBreaker(
		policy.WithThreshold(1),
		policy.WithResetTimeout(5*time.Millisecond),
	)
	entered := make(chan struct{}, 1)
	probe := RecoveryProbe{
		Probe: func(ctx context.Context, _ cql.Session) error {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-ctx.Done()

			return ctx.Err()
		},
		Interval: 5 * time.Millisecond,
		Timeout:  time.Hour,
	}
	probes := &probeCounters{}
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithFailoverPolicy(cb),
		WithRecoveryProbe(probe),
		WithMetrics(probes),
	)
	require.NoError(t, err)

	openBreaker(t, cb, 1)
	<-entered
	failuresBefore := cb.Failures(ClusterA)

	done := make(chan struct{})
	go func() { client.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung on the in-flight probe")
	}

	require.True(t, cb.ShouldFailover(ClusterA, nil), "the breaker is still open")
	require.Equal(t, failuresBefore, cb.Failures(ClusterA), "an abandoned probe counts nothing")
	require.Zero(t, probes.failureA.Load(), "nor as a probe failure")
	_, ok := cb.TryBeginFailoverProbe(ClusterA)
	require.True(t, ok, "another client can reserve the breaker at once")
}

// belowThresholdSpy is a custom failover policy that exposes the
// below-threshold capability.
type belowThresholdSpy struct {
	*mockFailoverPolicy
	enabled bool
}

func (b *belowThresholdSpy) FailoverBelowThreshold() bool { return b.enabled }

func TestNewCQLClient_FailoverBelowThresholdWarnings(t *testing.T) {
	const warning = "the failover policy returns the first threshold-1 read failures to the caller (the v1 default); " +
		"enable failover below the threshold with the policy's WithFailoverBelowThreshold(true) or WithLatencyFailoverBelowThreshold(true)"
	cases := []struct {
		name   string
		policy FailoverPolicy
		warns  bool
	}{
		{name: "CircuitBreaker default", policy: policy.NewCircuitBreaker(), warns: true},
		{name: "CircuitBreaker explicit false", policy: policy.NewCircuitBreaker(policy.WithFailoverBelowThreshold(false)), warns: true},
		{name: "CircuitBreaker enabled", policy: policy.NewCircuitBreaker(policy.WithFailoverBelowThreshold(true)), warns: false},
		{name: "LatencyCircuitBreaker default", policy: policy.NewLatencyCircuitBreaker(), warns: true},
		{name: "LatencyCircuitBreaker enabled", policy: policy.NewLatencyCircuitBreaker(policy.WithLatencyFailoverBelowThreshold(true)), warns: false},
		{name: "custom policy without the capability", policy: newMockFailoverPolicy(true), warns: false},
		{name: "custom policy with the capability off", policy: &belowThresholdSpy{mockFailoverPolicy: newMockFailoverPolicy(true)}, warns: true},
		{name: "custom policy with the capability on", policy: &belowThresholdSpy{mockFailoverPolicy: newMockFailoverPolicy(true), enabled: true}, warns: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger := &captureLogger{}
			client, err := NewCQLClient(newMockSession(), newMockSession(),
				WithLogger(logger),
				WithFailoverPolicy(tc.policy),
				WithRecoveryProbeDisabled(),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			count := 0
			for _, w := range warnings(logger) {
				if w == warning {
					count++
				}
			}
			if tc.warns {
				require.Equal(t, 1, count, "exactly one warning per client")
			} else {
				require.Zero(t, count)
			}
		})
	}
}
