package helix

import (
	"errors"
	"testing"
	"time"

	"github.com/arloliu/helix/adapter/cql"

	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

func TestNewCQLClient_RejectsAutoMemoryWorkerWithCallerReplayer(t *testing.T) {
	_, err := NewCQLClient(newMockSession(), newMockSession(),
		WithReplayer(replay.NewMemoryReplayer()),
		WithAutoMemoryWorker(0),
	)
	require.Error(t, err)
	require.True(t, types.IsOptionError(err))
	require.ErrorContains(t, err, "WithAutoMemoryWorker")
	require.ErrorContains(t, err, "WithReplayer")
}

func TestNewCQLClient_RejectsAutoMemoryWorkerWithCallerWorker(t *testing.T) {
	_, err := NewCQLClient(newMockSession(), newMockSession(),
		WithReplayWorker(newMockReplayWorker(nil)),
		WithAutoMemoryWorker(0),
	)
	require.Error(t, err)
	require.True(t, types.IsOptionError(err))
	require.ErrorContains(t, err, "WithReplayWorker")
}

func TestNewCQLClient_WarnsWhenMirrorReplayerHasNoMirror(t *testing.T) {
	logger := &captureLogger{}
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithLogger(logger),
		WithMirrorReplayer(replay.NewMemoryReplayer()),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.Contains(t, warnings(logger), "WithMirrorReplayer has no effect without WithMirror; failed mirror writes are only retried in target mode")
}

func TestNewCQLClient_WarnsWhenRecoveryProbeHasNoReportingStrategy(t *testing.T) {
	logger := &captureLogger{}
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithLogger(logger),
		WithWriteStrategy(&recordingWriteStrategy{}),
		WithRecoveryProbe(DefaultRecoveryProbe()),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.Contains(t, warnings(logger), "WithRecoveryProbe has no effect: neither the write strategy nor the failover policy asks for probes, so no probe will run")
}

func TestNewCQLClient_RouteVetoWarnings(t *testing.T) {
	t.Run("policy can veto but the option is off", func(t *testing.T) {
		logger := &captureLogger{}
		client, err := NewCQLClient(newMockSession(), newMockSession(),
			WithLogger(logger),
			WithFailoverPolicy(policy.NewLatencyCircuitBreaker()),
		)
		require.NoError(t, err)
		t.Cleanup(client.Close)

		require.Contains(t, warnings(logger), "the failover policy can veto routes but WithRouteVeto is off (the v1 default): "+
			"reads keep going to a cluster whose breaker is open; enable it with WithRouteVeto(true)")
	})
	t.Run("option on but the policy cannot veto", func(t *testing.T) {
		logger := &captureLogger{}
		client, err := NewCQLClient(newMockSession(), newMockSession(),
			WithLogger(logger),
			WithFailoverPolicy(policy.NewCircuitBreaker()),
			WithRouteVeto(true),
		)
		require.NoError(t, err)
		t.Cleanup(client.Close)

		require.Contains(t, warnings(logger), "WithRouteVeto has no effect: the failover policy cannot veto routes")
	})
	t.Run("option on with a vetoing policy warns nothing", func(t *testing.T) {
		logger := &captureLogger{}
		client, err := NewCQLClient(newMockSession(), newMockSession(),
			WithLogger(logger),
			WithFailoverPolicy(policy.NewLatencyCircuitBreaker()),
			WithRouteVeto(true),
		)
		require.NoError(t, err)
		t.Cleanup(client.Close)

		for _, w := range warnings(logger) {
			require.NotContains(t, w, "RouteVeto")
		}
	})
	t.Run("option on but the recovery probe is disabled", func(t *testing.T) {
		logger := &captureLogger{}
		client, err := NewCQLClient(newMockSession(), newMockSession(),
			WithLogger(logger),
			WithFailoverPolicy(policy.NewLatencyCircuitBreaker()),
			WithBehaviorProfile(Safe),
			WithRecoveryProbeDisabled(),
		)
		require.NoError(t, err)
		t.Cleanup(client.Close)

		require.Contains(t, warnings(logger), warnRouteVetoWithoutProbe)
	})
	t.Run("recovery probe disabled with the option off does not warn about the veto", func(t *testing.T) {
		logger := &captureLogger{}
		client, err := NewCQLClient(newMockSession(), newMockSession(),
			WithLogger(logger),
			WithFailoverPolicy(policy.NewLatencyCircuitBreaker()),
			WithRecoveryProbeDisabled(),
		)
		require.NoError(t, err)
		t.Cleanup(client.Close)

		require.NotContains(t, warnings(logger), warnRouteVetoWithoutProbe)
	})
	t.Run("option on with a zero latency reset timeout", func(t *testing.T) {
		logger := &captureLogger{}
		client, err := NewCQLClient(newMockSession(), newMockSession(),
			WithLogger(logger),
			WithFailoverPolicy(policy.NewLatencyCircuitBreaker(policy.WithLatencyResetTimeout(0))),
			WithRouteVeto(true),
		)
		require.NoError(t, err)
		t.Cleanup(client.Close)

		require.Contains(t, warnings(logger), warnRouteVetoWithoutResetTimeout)
	})
	t.Run("option on with a zero reset timeout on the embedded breaker", func(t *testing.T) {
		logger := &captureLogger{}
		client, err := NewCQLClient(newMockSession(), newMockSession(),
			WithLogger(logger),
			// The plain CircuitBreaker has no VetoRoute of its own, so the
			// veto reaches WithResetTimeout only through a policy that
			// embeds it and adds one.
			WithFailoverPolicy(&embeddedBreakerVetoPolicy{CircuitBreaker: policy.NewCircuitBreaker(policy.WithResetTimeout(0))}),
			WithRouteVeto(true),
		)
		require.NoError(t, err)
		t.Cleanup(client.Close)

		require.Contains(t, warnings(logger), warnRouteVetoWithoutResetTimeout)
	})
	t.Run("option on with the default reset timeouts warns nothing", func(t *testing.T) {
		for name, p := range map[string]FailoverPolicy{
			"latency breaker":  policy.NewLatencyCircuitBreaker(),
			"embedded breaker": &embeddedBreakerVetoPolicy{CircuitBreaker: policy.NewCircuitBreaker()},
		} {
			t.Run(name, func(t *testing.T) {
				logger := &captureLogger{}
				client, err := NewCQLClient(newMockSession(), newMockSession(),
					WithLogger(logger),
					WithFailoverPolicy(p),
					WithRouteVeto(true),
				)
				require.NoError(t, err)
				t.Cleanup(client.Close)

				for _, w := range warnings(logger) {
					require.NotContains(t, w, "RouteVeto")
				}
			})
		}
	})
	t.Run("option on with a policy the probe cannot reserve", func(t *testing.T) {
		logger := &captureLogger{}
		client, err := NewCQLClient(newMockSession(), newMockSession(),
			WithLogger(logger),
			WithFailoverPolicy(newVetoPolicy()),
			WithRouteVeto(true),
		)
		require.NoError(t, err)
		t.Cleanup(client.Close)

		require.Contains(t, warnings(logger), warnRouteVetoWithoutProbeReporter)
	})
}

// embeddedBreakerVetoPolicy is the shape a caller reaches for when they want
// the plain CircuitBreaker to steer ordinary reads: the breaker supplies the
// policy and the probe reservation, the wrapper supplies the veto.
type embeddedBreakerVetoPolicy struct {
	*policy.CircuitBreaker
}

func (p *embeddedBreakerVetoPolicy) VetoRoute(cluster ClusterID) bool {
	return p.ShouldFailover(cluster, errBreakerOpen)
}

var errBreakerOpen = errors.New("breaker open")

func TestNewCQLClient_NoWarningWhenRecoveryProbeHasAdaptiveStrategy(t *testing.T) {
	logger := &captureLogger{}
	// The leg deadlines are set so the assertion below stays "no warnings at
	// all": a dual-cluster client without them is warned about separately
	// (see TestNewCQLClient_LegDeadlineWarnings).
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithLogger(logger),
		WithWriteStrategy(policy.NewAdaptiveDualWrite()),
		WithRecoveryProbe(DefaultRecoveryProbe()),
		WithAutoMemoryWorker(0),
		WithClusterReadTimeout(time.Second),
		WithClusterWriteTimeout(time.Second),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.Empty(t, warnings(logger))
}

const (
	warnRouteVetoWithoutProbe = "WithRecoveryProbeDisabled with WithRouteVeto on (also set by WithBehaviorProfile(Safe)): " +
		"a vetoed cluster is reopened only by a recovery probe or by a failover leg landing on it, " +
		"so a breaker that opens while the other cluster stays healthy never closes and reads stay single-cluster; " +
		"drop WithRecoveryProbeDisabled or set WithRouteVeto(false)"
	warnRouteVetoWithoutProbeReporter = "WithRouteVeto on (also set by WithBehaviorProfile(Safe)) with a failover policy the recovery probe cannot reserve " +
		"(it does not implement helix.FailoverProbeReporter): " +
		"a vetoed cluster is reopened only by a recovery probe or by a failover leg landing on it, " +
		"so a breaker that opens while the other cluster stays healthy never closes and reads stay single-cluster; " +
		"implement FailoverProbeReporter on the policy or set WithRouteVeto(false)"
	warnRouteVetoWithoutResetTimeout = "WithResetTimeout(0) or WithLatencyResetTimeout(0) with WithRouteVeto on (also set by WithBehaviorProfile(Safe)): " +
		"the failover policy schedules no recovery probe, and a vetoed cluster is reopened only by a recovery probe " +
		"or by a failover leg landing on it, " +
		"so a breaker that opens while the other cluster stays healthy never closes and reads stay single-cluster; " +
		"set a positive reset timeout or set WithRouteVeto(false)"
	warnNoReadLegDeadline = "dual-cluster mode with no ClusterReadTimeout: " +
		"a read leg that never answers is bounded only by the caller's context and is never attributed to the cluster; " +
		"set one with WithClusterReadTimeout"
	warnNoWriteLegDeadline = "dual-cluster mode with no ClusterWriteTimeout: " +
		"a write leg that never answers is bounded only by the caller's context and is never attributed to the cluster; " +
		"set one with WithClusterWriteTimeout"
)

// TestNewCQLClient_LegDeadlineWarnings pins who is told that a leg has no
// deadline of its own: a dual-cluster client that left one unset, and
// nobody else.
func TestNewCQLClient_LegDeadlineWarnings(t *testing.T) {
	tests := []struct {
		name      string
		sessionB  cql.Session
		opts      []Option
		wantRead  bool
		wantWrite bool
	}{
		{
			name:      "dual-cluster with neither deadline warns about both",
			sessionB:  newMockSession(),
			wantRead:  true,
			wantWrite: true,
		},
		{
			name:     "dual-cluster with both deadlines warns about neither",
			sessionB: newMockSession(),
			opts: []Option{
				WithClusterReadTimeout(time.Second),
				WithClusterWriteTimeout(time.Second),
			},
		},
		{
			name:     "single-cluster with neither deadline warns about neither",
			sessionB: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := &captureLogger{}
			opts := append([]Option{WithLogger(logger)}, tt.opts...)
			client, err := NewCQLClient(newMockSession(), tt.sessionB, opts...)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			got := warnings(logger)
			if tt.wantRead {
				require.Contains(t, got, warnNoReadLegDeadline)
			} else {
				require.NotContains(t, got, warnNoReadLegDeadline)
			}
			if tt.wantWrite {
				require.Contains(t, got, warnNoWriteLegDeadline)
			} else {
				require.NotContains(t, got, warnNoWriteLegDeadline)
			}
		})
	}
}

// warnings returns the warning messages the logger captured so far.
func warnings(logger *captureLogger) []string {
	logger.Lock()
	defer logger.Unlock()

	return append([]string(nil), logger.warnMsgs...)
}

func TestNewCQLClient_RejectsUnknownAckMode(t *testing.T) {
	_, err := NewCQLClient(newMockSession(), newMockSession(), WithAckMode(AckMode(99)))
	require.Error(t, err)
	require.True(t, types.IsOptionError(err))
	require.ErrorContains(t, err, "WithAckMode")
}
