package helix_test

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/testutil"
)

type warnCollector struct {
	mu    sync.Mutex
	warns []string
}

func (l *warnCollector) Debug(string, ...any) {}
func (l *warnCollector) Info(string, ...any)  {}
func (l *warnCollector) Error(string, ...any) {}
func (l *warnCollector) Fatal(string, ...any) {}
func (l *warnCollector) Warn(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, msg)
}

func (l *warnCollector) joined() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return strings.Join(l.warns, "\n")
}

func newClientWithNATSReplayer(t *testing.T, logger *warnCollector, opts ...replay.NATSReplayerOption) {
	t.Helper()
	js := testutil.StartEmbeddedNATS(t)
	replayer, err := replay.NewNATSReplayer(js, append([]replay.NATSReplayerOption{
		replay.WithStreamName("test-warn-" + strings.ReplaceAll(t.Name(), "/", "-")),
		replay.WithSubjectPrefix("test.warn"),
	}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = replayer.Close() })

	client, err := helix.NewCQLClient(newAlwaysOKSession(), newAlwaysOKSession(),
		helix.WithWriteStrategy(policy.NewSyncDualWrite()),
		helix.WithReadStrategy(policy.NewStickyRead()),
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
		helix.WithReplayer(replayer),
		helix.WithLogger(logger),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
}

// A NATS replayer keeping the stream defaults that narrow the recovery
// window is called out once at construction.
func TestNewCQLClient_WarnsAboutReplayStreamDefaults(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		logger := &warnCollector{}
		newClientWithNATSReplayer(t, logger)

		warns := logger.joined()
		require.Contains(t, warns, "single replica")
		require.Contains(t, warns, "evicts the oldest")
	})

	t.Run("safe settings", func(t *testing.T) {
		logger := &warnCollector{}
		// The embedded server is a single node, so keep Replicas at 1 but
		// prove the discard warning is independent of it.
		newClientWithNATSReplayer(t, logger, replay.WithRejectNewOnLimit())

		warns := logger.joined()
		require.Contains(t, warns, "single replica")
		require.NotContains(t, warns, "evicts the oldest")
	})
}
