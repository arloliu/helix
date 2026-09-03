package helix

import (
	"testing"

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

	require.Contains(t, warnings(logger), "WithRecoveryProbe has no effect: the write strategy does not report degraded clusters, so no probe will run")
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
}

func TestNewCQLClient_NoWarningWhenRecoveryProbeHasAdaptiveStrategy(t *testing.T) {
	logger := &captureLogger{}
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithLogger(logger),
		WithWriteStrategy(policy.NewAdaptiveDualWrite()),
		WithRecoveryProbe(DefaultRecoveryProbe()),
		WithAutoMemoryWorker(0),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.Empty(t, warnings(logger))
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
