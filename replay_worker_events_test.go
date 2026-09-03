package helix

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/types"
)

// stubReplayWorker is a ReplayWorker that records whether the client
// installed an emitter before starting it and reports a configurable
// eviction watch.
type stubReplayWorker struct {
	*mockReplayWorker
	watch          bool
	emitterAtStart types.ClusterEventEmitter
}

func (w *stubReplayWorker) EvictionWatchEnabled() bool                   { return w.watch }
func (w *stubReplayWorker) SetEventEmitter(em types.ClusterEventEmitter) { w.emitterAtStart = em }

func TestReplayWorker_ReceivesTheDispatcherBeforeStartAndClassifiesEvictions(t *testing.T) {
	for _, watch := range []bool{false, true} {
		w := &stubReplayWorker{mockReplayWorker: newMockReplayWorker(nil), watch: watch}
		logger := &captureLogger{}
		client, err := NewCQLClient(newMockSession(), newMockSession(),
			WithReplayer(&mockReplayer{}),
			WithReplayWorker(w),
			WithLogger(logger),
			WithOnClusterEvent(func(types.ClusterEvent) {}),
		)
		require.NoError(t, err)
		require.NotNil(t, w.emitterAtStart, "the worker holds the dispatcher before it starts")
		listed := logger.unreachableKindsField(t)
		if watch {
			require.NotContains(t, listed, "replay_evicted", "the watch makes the kind reachable")
		} else {
			require.Contains(t, listed, "replay_evicted")
		}
		client.Close()
	}
}
