package helix

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/policy"
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

// A worker whose Start fails is disconnected from the dispatcher the
// constructor is about to stop, and a client without an event handler
// never touches a caller-supplied worker's emitter.
func TestReplayWorker_EmitterIsClearedOnFailedStartAndUntouchedWithoutHandler(t *testing.T) {
	failing := &stubReplayWorker{mockReplayWorker: newMockReplayWorker(errors.New("boom"))}
	_, err := NewCQLClient(newMockSession(), newMockSession(),
		WithReplayer(&mockReplayer{}),
		WithReplayWorker(failing),
		WithOnClusterEvent(func(types.ClusterEvent) {}),
	)
	require.Error(t, err)
	require.Nil(t, failing.emitterAtStart, "a failed Start clears the injected dispatcher")

	own := &recordingEmitterStub{}
	untouched := &stubReplayWorker{mockReplayWorker: newMockReplayWorker(nil), emitterAtStart: own}
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithReplayer(&mockReplayer{}),
		WithReplayWorker(untouched),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	require.Same(t, own, untouched.emitterAtStart, "no handler, no dispatcher: the worker keeps its own emitter")
}

// recordingEmitterStub is a caller-owned emitter.
type recordingEmitterStub struct{}

func (*recordingEmitterStub) EmitClusterEvent(types.ClusterEvent) {}

// A typed-nil read strategy is left alone by the injections, as the
// constructor's own validation reports it.
func TestReadStrategy_TypedNilIsNotInjected(t *testing.T) {
	var sticky *policy.StickyRead
	require.NotPanics(t, func() {
		client, err := NewCQLClient(newMockSession(), nil,
			WithReadStrategy(sticky),
			WithMetrics(&routeGaugeStub{}),
			WithOnClusterEvent(func(types.ClusterEvent) {}),
		)
		if err == nil {
			client.Close()
		}
	})
}

// routeGaugeStub is a collector that would make a typed-nil strategy panic
// if the injection reached it.
type routeGaugeStub struct{ metrics.NopMetrics }

func (*routeGaugeStub) SetReadPreferred(types.ClusterID, bool) {}

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
