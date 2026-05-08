package helix

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// probeCounters wraps NopMetrics and tracks recovery probe counters separately
// for each cluster, satisfying both MetricsCollector and RecoveryProbeMetrics.
type probeCounters struct {
	metrics.NopMetrics
	successA, successB atomic.Int32
	failureA, failureB atomic.Int32
}

func (p *probeCounters) IncRecoveryProbeSuccess(cluster types.ClusterID) {
	if cluster == ClusterA {
		p.successA.Add(1)
	} else {
		p.successB.Add(1)
	}
}

func (p *probeCounters) IncRecoveryProbeFailure(cluster types.ClusterID) {
	if cluster == ClusterA {
		p.failureA.Add(1)
	} else {
		p.failureB.Add(1)
	}
}

var _ types.MetricsCollector = (*probeCounters)(nil)
var _ types.RecoveryProbeMetrics = (*probeCounters)(nil)

// TestRecoveryProbe_StartsForAdaptiveDualWrite verifies that a recovery probe
// goroutine starts automatically when WriteStrategy is AdaptiveDualWrite.
func TestRecoveryProbe_StartsForAdaptiveDualWrite(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	adaptive := policy.NewAdaptiveDualWrite()
	adaptive.ForceDegrade(ClusterA)

	probes := &probeCounters{}
	probeFired := make(chan struct{}, 1)
	customProbe := RecoveryProbe{
		Probe: func(_ context.Context, _ cql.Session) error {
			select {
			case probeFired <- struct{}{}:
			default:
			}
			return nil
		},
		Interval: 10 * time.Millisecond,
		Timeout:  50 * time.Millisecond,
	}

	client, err := NewCQLClient(sa, sb,
		WithWriteStrategy(adaptive),
		WithRecoveryProbe(customProbe),
		WithMetrics(probes),
	)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	select {
	case <-probeFired:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("recovery probe did not fire within 500ms")
	}
}

// TestRecoveryProbe_NoStartWithoutAdaptive verifies that no probe goroutine
// starts when the write strategy is not a probeReporter (e.g. ConcurrentDualWrite).
func TestRecoveryProbe_NoStartWithoutAdaptive(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()

	probeFired := atomic.Bool{}
	customProbe := RecoveryProbe{
		Probe: func(_ context.Context, _ cql.Session) error {
			probeFired.Store(true)
			return nil
		},
		Interval: 5 * time.Millisecond,
		Timeout:  20 * time.Millisecond,
	}

	// ConcurrentDualWrite does not implement probeReporter.
	client, err := NewCQLClient(sa, sb,
		WithWriteStrategy(policy.NewConcurrentDualWrite()),
		WithRecoveryProbe(customProbe),
	)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	time.Sleep(50 * time.Millisecond)
	assert.False(t, probeFired.Load(), "probe must not fire for non-Adaptive strategy")
}

// TestRecoveryProbe_DisabledOption verifies that WithRecoveryProbeDisabled
// suppresses the probe even when AdaptiveDualWrite is the write strategy.
func TestRecoveryProbe_DisabledOption(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	adaptive := policy.NewAdaptiveDualWrite()
	adaptive.ForceDegrade(ClusterA)

	probeFired := atomic.Bool{}
	customProbe := RecoveryProbe{
		Probe: func(_ context.Context, _ cql.Session) error {
			probeFired.Store(true)
			return nil
		},
		Interval: 5 * time.Millisecond,
		Timeout:  20 * time.Millisecond,
	}

	client, err := NewCQLClient(sa, sb,
		WithWriteStrategy(adaptive),
		WithRecoveryProbe(customProbe),
		WithRecoveryProbeDisabled(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	time.Sleep(50 * time.Millisecond)
	assert.False(t, probeFired.Load(), "probe must not fire when disabled")
}

// TestRecoveryProbe_SkipsHealthyCluster verifies that the probe does not
// execute the Probe function when the cluster is healthy (not degraded).
func TestRecoveryProbe_SkipsHealthyCluster(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	adaptive := policy.NewAdaptiveDualWrite()
	// Neither cluster is degraded.

	probeFired := atomic.Bool{}
	customProbe := RecoveryProbe{
		Probe: func(_ context.Context, _ cql.Session) error {
			probeFired.Store(true)
			return nil
		},
		Interval: 5 * time.Millisecond,
		Timeout:  20 * time.Millisecond,
	}

	client, err := NewCQLClient(sa, sb,
		WithWriteStrategy(adaptive),
		WithRecoveryProbe(customProbe),
	)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	time.Sleep(50 * time.Millisecond)
	assert.False(t, probeFired.Load(), "probe must not execute against a healthy cluster")
}

// TestRecoveryProbe_AdvancesRecovery verifies that successful probes credit
// recovery points until the cluster returns to healthy state.
func TestRecoveryProbe_AdvancesRecovery(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	adaptive := policy.NewAdaptiveDualWrite()
	adaptive.ForceDegrade(ClusterA)
	require.True(t, adaptive.IsDegraded(ClusterA), "cluster A must be degraded initially")

	customProbe := RecoveryProbe{
		Probe:    func(_ context.Context, _ cql.Session) error { return nil },
		Interval: 5 * time.Millisecond,
		Timeout:  20 * time.Millisecond,
	}

	client, err := NewCQLClient(sa, sb,
		WithWriteStrategy(adaptive),
		WithRecoveryProbe(customProbe),
	)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	require.Eventually(t, func() bool {
		return !adaptive.IsDegraded(ClusterA)
	}, 500*time.Millisecond, 10*time.Millisecond, "cluster A must recover via probe")
}

// TestRecoveryProbe_FailingProbeDoesNotRecover verifies that a probe returning
// an error each time does not advance recovery — the cluster stays degraded.
func TestRecoveryProbe_FailingProbeDoesNotRecover(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	adaptive := policy.NewAdaptiveDualWrite()
	adaptive.ForceDegrade(ClusterA)

	probes := &probeCounters{}
	probeCalls := atomic.Int32{}
	customProbe := RecoveryProbe{
		Probe: func(_ context.Context, _ cql.Session) error {
			probeCalls.Add(1)
			return errors.New("probe: cluster unreachable")
		},
		Interval: 5 * time.Millisecond,
		Timeout:  20 * time.Millisecond,
	}

	client, err := NewCQLClient(sa, sb,
		WithWriteStrategy(adaptive),
		WithRecoveryProbe(customProbe),
		WithMetrics(probes),
	)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	require.Eventually(t, func() bool {
		return probeCalls.Load() >= 3
	}, 500*time.Millisecond, 5*time.Millisecond, "probe must have fired at least 3 times")

	assert.True(t, adaptive.IsDegraded(ClusterA), "cluster A must remain degraded")
	assert.GreaterOrEqual(t, probes.failureA.Load(), int32(3),
		"IncRecoveryProbeFailure must be incremented for each failing probe")
	assert.Equal(t, int32(0), probes.successA.Load(),
		"IncRecoveryProbeSuccess must not be incremented")
}

// TestRecoveryProbe_MetricsSuccess verifies that IncRecoveryProbeSuccess is
// incremented for each successful probe against a degraded cluster.
func TestRecoveryProbe_MetricsSuccess(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	adaptive := policy.NewAdaptiveDualWrite()
	adaptive.ForceDegrade(ClusterA)

	probes := &probeCounters{}
	customProbe := RecoveryProbe{
		Probe:    func(_ context.Context, _ cql.Session) error { return nil },
		Interval: 5 * time.Millisecond,
		Timeout:  20 * time.Millisecond,
	}

	client, err := NewCQLClient(sa, sb,
		WithWriteStrategy(adaptive),
		WithRecoveryProbe(customProbe),
		WithMetrics(probes),
	)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	require.Eventually(t, func() bool {
		return probes.successA.Load() >= 1
	}, 500*time.Millisecond, 10*time.Millisecond, "IncRecoveryProbeSuccess must fire at least once")
}

// TestRecoveryProbe_DefaultProbeAutoCreated verifies that when no RecoveryProbe
// is configured and WriteStrategy is AdaptiveDualWrite, probe goroutines are
// started automatically using the default probe configuration.
func TestRecoveryProbe_DefaultProbeAutoCreated(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	adaptive := policy.NewAdaptiveDualWrite()

	client, err := NewCQLClient(sa, sb, WithWriteStrategy(adaptive))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	assert.NotNil(t, client.recoveryProbeCtx, "probe context must be non-nil for AdaptiveDualWrite")
}

// TestRecoveryProbe_CloseWaitsForGoroutines verifies that Close() completes
// without leaking probe goroutines and does not race against session teardown.
func TestRecoveryProbe_CloseWaitsForGoroutines(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	adaptive := policy.NewAdaptiveDualWrite()
	adaptive.ForceDegrade(ClusterA)

	customProbe := RecoveryProbe{
		Probe:    func(_ context.Context, _ cql.Session) error { return nil },
		Interval: 10 * time.Millisecond,
		Timeout:  50 * time.Millisecond,
	}

	client, err := NewCQLClient(sa, sb,
		WithWriteStrategy(adaptive),
		WithRecoveryProbe(customProbe),
	)
	require.NoError(t, err)

	// Give goroutines time to start ticking.
	time.Sleep(30 * time.Millisecond)

	done := make(chan struct{})
	go func() { client.Close(); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close() did not return within 2s — probe goroutine may be leaking")
	}
}

// TestRecoveryProbe_ZeroValueUsesDefaults verifies that WithRecoveryProbe(RecoveryProbe{})
// does not panic: all zero fields are filled with defaults before the goroutine starts.
func TestRecoveryProbe_ZeroValueUsesDefaults(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	adaptive := policy.NewAdaptiveDualWrite()
	adaptive.ForceDegrade(ClusterA)

	// Zero-value RecoveryProbe: Probe=nil, Interval=0, Timeout=0.
	// WithRecoveryProbe must substitute defaults so recoveryProbeLoop never calls
	// time.NewTicker(0) or a nil Probe function.
	client, err := NewCQLClient(sa, sb,
		WithWriteStrategy(adaptive),
		WithRecoveryProbe(RecoveryProbe{}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	// Probe context must be non-nil, meaning goroutines started without panicking.
	require.NotNil(t, client.recoveryProbeCtx, "probe goroutines must start for AdaptiveDualWrite")
}

// TestRecoveryProbe_NilProbeFilled verifies that a RecoveryProbe with explicit
// durations but nil Probe is filled with the default probe function.
func TestRecoveryProbe_NilProbeFilled(t *testing.T) {
	sa, sb := newMockSession(), newMockSession()
	adaptive := policy.NewAdaptiveDualWrite()
	adaptive.ForceDegrade(ClusterA)

	probeFired := make(chan struct{}, 1)
	// Wrap the zero-value path: nil Probe gets replaced by the default (system.local).
	// Override with a sentinel so we can detect the *real* call path.
	// Use non-zero durations so only Probe needs defaulting.
	p := RecoveryProbe{Interval: 10 * time.Millisecond, Timeout: 50 * time.Millisecond}
	// Verify the config stores a non-nil Probe after WithRecoveryProbe normalises it.
	opt := WithRecoveryProbe(p)
	cfg := DefaultConfig()
	opt(cfg)
	require.NotNil(t, cfg.RecoveryProbe.Probe, "WithRecoveryProbe must fill nil Probe with default")
	require.Equal(t, 10*time.Millisecond, cfg.RecoveryProbe.Interval)
	require.Equal(t, 50*time.Millisecond, cfg.RecoveryProbe.Timeout)

	// Exercise the goroutine path: replace the filled Probe with a sentinel so
	// we can assert the loop actually calls it instead of panicking.
	cfg.RecoveryProbe.Probe = func(_ context.Context, _ cql.Session) error {
		select {
		case probeFired <- struct{}{}:
		default:
		}
		return nil
	}
	cfg.WriteStrategy = adaptive

	client, clientErr := NewCQLClient(sa, sb, func(c *ClientConfig) { *c = *cfg })
	require.NoError(t, clientErr)
	t.Cleanup(func() { client.Close() })

	select {
	case <-probeFired:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("probe did not fire within 500ms")
	}
}
