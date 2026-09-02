package replay

// Regression tests for a bug where a JetStream batch Fetch that is
// interrupted mid-stream (consumer deleted, ctx cancel, transport error)
// returns a non-empty []ReplayMessage together with a non-nil error, and
// that non-nil error caused the messages to be silently discarded before
// they were ever Ack'd/Nak'd/Term'd:
//
//   - dequeueLowFirst/dequeueHighFirst returned only {err: err} on the
//     first DequeueByPriority call, dropping any msgs already decoded.
//   - natsBackend.start checked result.err != nil and continued the poll
//     loop before ever looking at result.msgs, so even a correctly
//     forwarded non-empty batch was never passed to processMessages.
//   - dequeueHighFirst's error branch also returned the OLD highProcessed
//     counter even when msgs was non-empty, so repeated partial-error high
//     fetches never advanced toward HighPriorityRatio even though their
//     messages were being processed, keeping the ratio scheduler stuck in
//     high-first mode and starving low priority.
//
// Each occurrence burned a JetStream delivery attempt toward MaxDeliver
// without execute() running, and repeated partial-fetch errors could
// exhaust MaxDeliver and Term a payload that was never actually attempted.
//
// These tests use small fakes for the jetstream.Consumer/MessageBatch/Msg
// interfaces so the "messages + trailing error" scenario can be reproduced
// deterministically, in-memory, without racing a real NATS server to hit a
// mid-batch failure.

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/types"
)

// testEvent is one entry in the single, strictly ordered log of everything
// natsBackend.start's worker goroutine does that the tests below care
// about: each Fetch call, each execute() invocation, and each message
// settlement (Ack/Nak/Term). All three are only ever produced by that one
// goroutine, so appending them to a single channel captures true program
// order — no extra synchronization or sleeping is needed to prove "X
// happened before Y".
type testEvent struct {
	kind string // "fetch", "execute", "ack", "nak", "term", or "backoff"
	id   int    // message/payload identity for "execute"/"ack"/"nak"/"term"; unused for "fetch"/"backoff"
}

// eventTimeout bounds every wait on the worker goroutine's event log. It is
// deliberately generous — the event-driven tests below normally complete in
// microseconds, so the timeout only limits how long a genuinely broken
// worker can hang the test run.
const eventTimeout = 2 * time.Second

// recvEvent receives the next event from ch, failing the test if none
// arrives within eventTimeout. Used instead of time.Sleep polling to observe
// state transitions of the worker goroutine under test.
func recvEvent(t *testing.T, ch <-chan testEvent, what string) testEvent {
	t.Helper()

	select {
	case ev := <-ch:
		return ev
	case <-time.After(eventTimeout):
		t.Fatalf("timed out waiting for %s", what)

		return testEvent{}
	}
}

// backoffGate is a test seam that stands in for natsBackend.backoffWait,
// letting a test observe and control backoff entry deterministically instead
// of inferring it from wall-clock silence.
//
// wait reports a "backoff" event on events the instant start() calls it —
// before start()'s select ever blocks — and then returns release for
// start() to wait on. Because "backoff" is reported through the same events
// channel as fetch/execute/ack/nak/term, receiving it establishes true
// program order: the worker goroutine cannot have done anything after
// calling wait() except enter its select on (stopCh, release). A spin-loop
// regression that skips the backoff wait entirely never calls wait(), so it
// never produces this event, making a missing "backoff" event deterministic
// proof of regression rather than a timing signal.
//
// Once a test has received the "backoff" event, the worker is provably
// parked on the unreleased release channel (or stopCh): it cannot emit
// another fetch, execute, ack, nak, or term event until the test sends on
// release, because wait() already returned and start()'s select has no
// other way forward. That makes "no fetch event is queued yet" a
// zero-timing-window fact, not a race, and lets a test send on release to
// deterministically unblock exactly one backoff wait and observe the
// resulting next Fetch.
type backoffGate struct {
	events  chan testEvent
	release chan time.Time
}

// wait implements the natsBackend.backoffWait seam. See backoffGate.
func (g *backoffGate) wait(time.Duration) <-chan time.Time {
	g.events <- testEvent{kind: "backoff"}

	return g.release
}

// requireNoFetchQueued fails the test if a fetch event is already queued on
// ch. It never blocks: called only after a "backoff" event has been
// received from the same backoffGate, the worker is provably parked on the
// gate's unreleased release channel (see backoffGate), so a queued fetch
// here can only mean a spin-loop regression bypassed the gate — not a
// scheduling race, since there is no way for a correct worker to produce
// one before release is sent.
func requireNoFetchQueued(t *testing.T, ch <-chan testEvent) {
	t.Helper()

	select {
	case ev := <-ch:
		t.Fatalf("expected worker parked in backoff wait, but observed %+v", ev)
	default:
	}
}

// workerHarness owns a natsBackend worker goroutine started by a test and
// guarantees it cannot leak past the test, even if an early require/assert
// failure aborts the test body before it reaches its own shutdown
// assertions. stop is idempotent — safe to call once explicitly from a test
// body (to assert prompt shutdown as part of the test's own assertions) and
// once more from t.Cleanup as a fallback.
//
// stop drains the events channel concurrently with waiting for the
// goroutine to exit. Without that, a worker still mid-send on a full events
// buffer (e.g. still settling messages, or still fetching, at the moment
// the test stops calling recvEvent) could never reach its next stopCh
// check — closing stopCh does not by itself unblock a pending unbuffered
// channel send — and wg.Wait() would hang until eventTimeout fails the
// test instead of letting it clean up.
type workerHarness struct {
	stopCh chan struct{}
	wg     *sync.WaitGroup
	events chan testEvent
	once   sync.Once
}

// startWorker launches b.start(types.ClusterA) in its own goroutine (the
// only cluster these tests exercise), wires it to stopCh/wg, and registers
// the harness's stop method as a t.Cleanup so the goroutine is always asked
// to stop and drained, even if the test body never reaches an explicit
// stop() call of its own.
func startWorker(t *testing.T, b *natsBackend, stopCh chan struct{}, wg *sync.WaitGroup, events chan testEvent) *workerHarness {
	t.Helper()

	h := &workerHarness{stopCh: stopCh, wg: wg, events: events}

	wg.Add(1)
	go b.start(types.ClusterA)

	t.Cleanup(func() { h.stop(t) })

	return h
}

// stop closes stopCh (once) and waits for the worker goroutine to exit via
// wg, failing the test if it does not do so within eventTimeout. See
// workerHarness for why events must be drained concurrently with the wait.
func (h *workerHarness) stop(t *testing.T) {
	t.Helper()

	h.once.Do(func() { close(h.stopCh) })

	done := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(done)
	}()

	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for {
			select {
			case <-h.events:
			case <-done:
				return
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(eventTimeout):
		t.Fatal("worker did not stop promptly after stopCh was closed")
	}
	<-drainDone
}

// fakeFetchConsumer is a minimal jetstream.Consumer fake. Only Fetch is
// exercised by fetchReplayMessages; the remaining interface methods are
// satisfied by the embedded nil jetstream.Consumer and are never called.
//
// It returns batches in sequence, one per Fetch call, repeating the final
// batch once the sequence is exhausted — this lets a test script multiple
// distinct Fetch outcomes (e.g. a partial-error batch followed by quieter
// ones) for the one worker goroutine that drives natsBackend.start's poll
// loop. If events is non-nil, every Fetch call is also reported on it,
// enabling deterministic (non-sleeping) observation of individual Fetch
// invocations, ordered relative to the execute/ack events the same
// goroutine produces.
//
// Fetch is only ever called by the single natsBackend worker goroutine in
// these tests, so no locking is needed around the batches slice.
type fakeFetchConsumer struct {
	jetstream.Consumer
	batches []jetstream.MessageBatch
	events  chan testEvent
}

// Info satisfies the worker's periodic backlog-depth refresh; the fake has
// no server-side state to report.
func (f *fakeFetchConsumer) Info(context.Context) (*jetstream.ConsumerInfo, error) {
	return &jetstream.ConsumerInfo{}, nil
}

func (f *fakeFetchConsumer) Fetch(int, ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	batch := f.batches[0]
	if len(f.batches) > 1 {
		f.batches = f.batches[1:]
	}

	if f.events != nil {
		f.events <- testEvent{kind: "fetch"}
	}

	return batch, nil
}

// fakeMessageBatch is a minimal jetstream.MessageBatch fake: a pre-filled,
// closed channel of messages plus a trailing error, mirroring what a
// real MessageBatch looks like after a fetch is interrupted mid-stream.
type fakeMessageBatch struct {
	ch  chan jetstream.Msg
	err error
}

func (b *fakeMessageBatch) Messages() <-chan jetstream.Msg { return b.ch }

func (b *fakeMessageBatch) Error() error { return b.err }

// fakeMsg is a minimal jetstream.Msg fake carrying pre-encoded payload
// bytes. Ack/Nak/Term are no-ops; this test only asserts on what gets
// returned/executed, not on broker acknowledgement. Remaining interface
// methods are satisfied by the embedded nil jetstream.Msg and are never
// called by the exercised code paths.
type fakeMsg struct {
	jetstream.Msg
	data []byte
}

func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: 1}, nil
}

func (m *fakeMsg) Data() []byte { return m.data }

func (m *fakeMsg) Ack() error { return nil }

func (m *fakeMsg) Nak() error { return nil }

func (m *fakeMsg) NakWithDelay(time.Duration) error { return nil }

func (m *fakeMsg) InProgress() error { return nil }

func (m *fakeMsg) Term() error { return nil }

// fakeTrackedMsg is a fakeMsg variant that reports every Ack/Nak/Term call
// on events, tagged with id, so a test can match a specific message's
// settlement to the execute() call that ran it and observe the order of
// both relative to subsequent Fetch calls.
type fakeTrackedMsg struct {
	jetstream.Msg
	id     int
	data   []byte
	events chan testEvent
}

func (m *fakeTrackedMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: 1}, nil
}

func (m *fakeTrackedMsg) Data() []byte { return m.data }

func (m *fakeTrackedMsg) Ack() error {
	m.events <- testEvent{kind: "ack", id: m.id}

	return nil
}

func (m *fakeTrackedMsg) NakWithDelay(time.Duration) error { return m.Nak() }

func (m *fakeTrackedMsg) InProgress() error { return nil }

func (m *fakeTrackedMsg) Nak() error {
	m.events <- testEvent{kind: "nak", id: m.id}

	return nil
}

func (m *fakeTrackedMsg) Term() error {
	m.events <- testEvent{kind: "term", id: m.id}

	return nil
}

// fakeBatchWithMessagesAndError builds a jetstream.MessageBatch that yields n
// validly encoded messages and then reports batchErr from Error(), simulating
// a Fetch that was interrupted after partially succeeding.
func fakeBatchWithMessagesAndError(t *testing.T, n int, batchErr error) jetstream.MessageBatch {
	t.Helper()

	ch := make(chan jetstream.Msg, n)
	for i := range n {
		natsMsg := makeSimpleNATSMsg(t, "INSERT partial", i)
		data, err := natsMsg.MarshalMsg(nil)
		require.NoError(t, err)
		ch <- &fakeMsg{data: data}
	}
	close(ch)

	return &fakeMessageBatch{ch: ch, err: batchErr}
}

// fakeBatchWithTrackedMessages is the settlement-tracking counterpart of
// fakeBatchWithMessagesAndError: each message's query text encodes its
// index (see payloadTestID) and its Ack/Nak/Term calls are reported on
// events, so a test can assert exactly-once execution and settlement
// ordering without depending on msgp argument round-tripping.
func fakeBatchWithTrackedMessages(t *testing.T, n int, batchErr error, events chan testEvent) jetstream.MessageBatch {
	t.Helper()

	ch := make(chan jetstream.Msg, n)
	for i := range n {
		natsMsg := makeSimpleNATSMsg(t, fmt.Sprintf("INSERT partial %d", i))
		data, err := natsMsg.MarshalMsg(nil)
		require.NoError(t, err)
		ch <- &fakeTrackedMsg{id: i, data: data, events: events}
	}
	close(ch)

	return &fakeMessageBatch{ch: ch, err: batchErr}
}

// payloadTestID extracts the numeric identity encoded by
// fakeBatchWithTrackedMessages into payload.Query, so execute() can report
// which tracked message it ran.
func payloadTestID(t *testing.T, payload types.ReplayPayload) int {
	t.Helper()

	const prefix = "INSERT partial "
	require.True(t, strings.HasPrefix(payload.Query, prefix), "unexpected query: %q", payload.Query)

	id, err := strconv.Atoi(strings.TrimPrefix(payload.Query, prefix))
	require.NoError(t, err)

	return id
}

// newReplayerWithFakeConsumers builds a *NATSReplayer whose consumer cache is
// pre-populated with the given fakes, bypassing any real NATS/JetStream
// connection. getOrCreateConsumer checks this cache before touching
// n.stream, so Dequeue/DequeueByPriority calls resolve entirely against the
// fakes.
func newReplayerWithFakeConsumers(consumers map[string]jetstream.Consumer) *NATSReplayer {
	return &NATSReplayer{
		consumers: consumers,
		config:    NATSReplayerConfig{MaxDeliver: 5},
	}
}

// newReplayerWithFakeConsumer is the single-consumer convenience form of
// newReplayerWithFakeConsumers.
func newReplayerWithFakeConsumer(consumerName string, consumer jetstream.Consumer) *NATSReplayer {
	return newReplayerWithFakeConsumers(map[string]jetstream.Consumer{consumerName: consumer})
}

func newTestNATSBackendConfig() WorkerConfig {
	cfg := DefaultWorkerConfig()
	cfg.Metrics = metrics.NewNopMetrics()
	cfg.Logger = logging.NewNopLogger()
	cfg.ExecuteTimeout = time.Second
	cfg.PollInterval = 10 * time.Millisecond

	return cfg
}

// TestDequeueLowFirstForwardsMessagesFetchedBeforeError is the regression
// test for the discard bug in dequeueLowFirst's first DequeueByPriority
// call: messages already decoded before a mid-batch error must still be
// returned, not dropped.
func TestDequeueLowFirstForwardsMessagesFetchedBeforeError(t *testing.T) {
	batchErr := errors.New("simulated transport error mid-fetch")
	consumer := &fakeFetchConsumer{batches: []jetstream.MessageBatch{fakeBatchWithMessagesAndError(t, 2, batchErr)}}
	replayer := newReplayerWithFakeConsumer("helix-worker-low-A", consumer)

	cfg := newTestNATSBackendConfig()
	b := &natsBackend{replayer: replayer, config: &cfg}

	result := b.dequeueLowFirst(t.Context(), types.ClusterA)

	require.ErrorIs(t, result.err, batchErr)
	assert.Len(t, result.msgs, 2,
		"messages fetched before the mid-batch error must be forwarded, not discarded")
}

// TestDequeueHighFirstForwardsMessagesFetchedBeforeError mirrors
// TestDequeueLowFirstForwardsMessagesFetchedBeforeError for
// dequeueHighFirst's first DequeueByPriority call.
func TestDequeueHighFirstForwardsMessagesFetchedBeforeError(t *testing.T) {
	batchErr := errors.New("simulated transport error mid-fetch")
	consumer := &fakeFetchConsumer{batches: []jetstream.MessageBatch{fakeBatchWithMessagesAndError(t, 3, batchErr)}}
	replayer := newReplayerWithFakeConsumer("helix-worker-high-A", consumer)

	cfg := newTestNATSBackendConfig()
	b := &natsBackend{replayer: replayer, config: &cfg}

	result := b.dequeueHighFirst(t.Context(), types.ClusterA, 0)

	require.ErrorIs(t, result.err, batchErr)
	assert.Len(t, result.msgs, 3,
		"messages fetched before the mid-batch error must be forwarded, not discarded")
}

// TestDequeueStrictForwardsMessagesFetchedBeforeError verifies that
// dequeueStrict's existing "err != nil || len(msgs) > 0" forwarding
// contract still holds. dequeueStrict itself was not changed by this fix,
// but natsBackend.start's bug (checking result.err before ever looking at
// result.msgs) discarded whichever dequeue variant's partial batch reached
// it, including dequeueStrict's — so this locks in the same
// messages-plus-error contract for the StrictPriority code path.
func TestDequeueStrictForwardsMessagesFetchedBeforeError(t *testing.T) {
	batchErr := errors.New("simulated transport error mid-fetch")
	consumer := &fakeFetchConsumer{batches: []jetstream.MessageBatch{fakeBatchWithMessagesAndError(t, 2, batchErr)}}
	replayer := newReplayerWithFakeConsumer("helix-worker-high-A", consumer)

	cfg := newTestNATSBackendConfig()
	b := &natsBackend{replayer: replayer, config: &cfg}

	result := b.dequeueStrict(t.Context(), types.ClusterA)

	require.ErrorIs(t, result.err, batchErr)
	assert.Len(t, result.msgs, 2,
		"messages fetched before the mid-batch error must be forwarded, not discarded")
}

// TestDequeueHighFirstErrorCounterTransition is the regression test for
// P1-1: dequeueHighFirst's error branch previously returned the OLD
// highProcessed counter even when msgs was non-empty, while its
// error-free success branch advanced it (highProcessed + 1). Since
// natsBackend.start processes and forwards result.msgs regardless of
// result.err, this meant repeated partial-error high fetches with messages
// never advanced toward HighPriorityRatio, so the ratio scheduler stayed
// in high-first mode indefinitely — starving low priority even though its
// messages were, in a sense, "stealing" processing time via the batch
// error path.
//
// The fix must advance the counter when msgs is non-empty and leave it
// unchanged when the error carried zero messages (nothing was processed).
// See TestDequeueWithRatioSelectsLowFirstAfterPartialErrorsAdvanceCounter
// for the downstream scheduling effect.
func TestDequeueHighFirstErrorCounterTransition(t *testing.T) {
	batchErr := errors.New("simulated transport error mid-fetch")

	tests := []struct {
		name               string
		msgCount           int
		startHighProcessed int
		wantHighProcessed  int
	}{
		{
			name:               "messages forwarded alongside the error advance the counter",
			msgCount:           2,
			startHighProcessed: 3,
			wantHighProcessed:  4,
		},
		{
			name:               "zero-message error preserves the counter",
			msgCount:           0,
			startHighProcessed: 3,
			wantHighProcessed:  3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			consumer := &fakeFetchConsumer{
				batches: []jetstream.MessageBatch{fakeBatchWithMessagesAndError(t, tt.msgCount, batchErr)},
			}
			replayer := newReplayerWithFakeConsumer("helix-worker-high-A", consumer)

			cfg := newTestNATSBackendConfig()
			b := &natsBackend{replayer: replayer, config: &cfg}

			result := b.dequeueHighFirst(t.Context(), types.ClusterA, tt.startHighProcessed)

			require.ErrorIs(t, result.err, batchErr)
			assert.Len(t, result.msgs, tt.msgCount)
			assert.Equal(t, tt.wantHighProcessed, result.highProcessed)
		})
	}
}

// TestDequeueWithRatioSelectsLowFirstAfterPartialErrorsAdvanceCounter is the
// downstream scheduling assertion for the P1-1 fix: after enough
// partial-error high fetches (each forwarding a message) to reach
// HighPriorityRatio, dequeueWithRatio must switch to dequeueLowFirst. Before
// the fix, the counter would have stayed at 0 forever in this scenario and
// the scheduler would never have switched, starving low priority.
func TestDequeueWithRatioSelectsLowFirstAfterPartialErrorsAdvanceCounter(t *testing.T) {
	const ratio = 2

	batchErr := errors.New("simulated transport error mid-fetch")
	highConsumer := &fakeFetchConsumer{batches: []jetstream.MessageBatch{
		fakeBatchWithMessagesAndError(t, 1, batchErr),
		fakeBatchWithMessagesAndError(t, 1, batchErr),
	}}
	lowConsumer := &fakeFetchConsumer{batches: []jetstream.MessageBatch{
		fakeBatchWithMessagesAndError(t, 5, nil),
	}}
	replayer := newReplayerWithFakeConsumers(map[string]jetstream.Consumer{
		"helix-worker-high-A": highConsumer,
		"helix-worker-low-A":  lowConsumer,
	})

	cfg := newTestNATSBackendConfig()
	b := &natsBackend{replayer: replayer, config: &cfg}

	// Two partial-error high fetches, each forwarding one message: the
	// fixed dequeueHighFirst must advance the counter both times.
	r1 := b.dequeueHighFirst(t.Context(), types.ClusterA, 0)
	require.ErrorIs(t, r1.err, batchErr)
	require.Equal(t, 1, r1.highProcessed)

	r2 := b.dequeueHighFirst(t.Context(), types.ClusterA, r1.highProcessed)
	require.ErrorIs(t, r2.err, batchErr)
	require.Equal(t, ratio, r2.highProcessed)

	// highProcessed has now reached ratio: dequeueWithRatio must route to
	// the low-priority consumer, resolving entirely against lowConsumer's
	// 5-message, no-error batch without touching highConsumer again.
	result := b.dequeueWithRatio(t.Context(), types.ClusterA, r2.highProcessed, ratio)

	require.NoError(t, result.err)
	assert.Len(t, result.msgs, 5,
		"dequeueWithRatio must select low-first once the counter reaches ratio")
}

// backoffTestFixture holds the fresh backend/consumer/events/gate wiring
// built per subtest by
// TestNATSBackendStartBacksOffWithoutProcessingOnZeroMessageError, so
// mutable fake state (the consumer's batches slice, the gate's release
// channel) is never shared across subtests.
type backoffTestFixture struct {
	events   chan testEvent
	executed *atomic.Int32
	backend  *natsBackend
	gate     *backoffGate
	stopCh   chan struct{}
	wg       *sync.WaitGroup
}

// TestNATSBackendStartBacksOffWithoutProcessingOnZeroMessageError verifies
// the complementary, non-regressed path: when a dequeue error carries zero
// messages (the common case — the fetch failed before decoding anything),
// start() must not invoke execute at all, its backoff wait must actually
// park (not spin) and must honor stopCh, and the loop must actually resume
// with another Fetch once backoff is released.
//
// This used to be one test that measured the real wall-clock gap between
// two observed Fetch events against PollInterval. That is flaky in both
// directions: the fake consumer's Fetch enqueues its "fetch" event to a
// buffered channel before returning, so a correct worker can enqueue both
// Fetch events before the test goroutine is even scheduled to receive the
// first one (near-zero measured gap -> false FAIL), while a spin-loop
// regression descheduled between fetches by the Go scheduler or GC could by
// chance exceed the gap threshold (false PASS). A later revision replaced
// the gap measurement with a bounded silence window (assertNoEventWithin):
// that removed the false-FAIL direction, but a spin-regressed worker merely
// descheduled for longer than the window between fetch1 and fetch2 would
// still false-PASS.
//
// It is split into two subtests, both driven by natsBackend.backoffWait
// (see backoffGate) instead of any wall-clock oracle, so neither direction
// of flake is possible:
//   - "does not spin and honors stop during backoff" uses a one-hour
//     PollInterval (retained for documentation continuity, though the gate
//     ignores the value) and requires the "backoff" event the gate reports
//     the instant start() calls it — a spin-loop regression never reaches
//     that call, so a missing event is deterministic proof of regression,
//     not a timing signal. It then requires no fetch is already queued
//     (zero timing window: the worker is provably parked on the gate's
//     unreleased release channel) and stops the worker with the gate still
//     unreleased, proving the backoff select honors stopCh promptly.
//   - "resumes with another fetch after backoff release" proves the other
//     half: after the same backoff-entered proof, it releases the gate and
//     requires a second Fetch, proving the loop actually resumes rather
//     than getting stuck.
func TestNATSBackendStartBacksOffWithoutProcessingOnZeroMessageError(t *testing.T) {
	batchErr := errors.New("simulated transport error, nothing fetched")

	setup := func(t *testing.T) *backoffTestFixture {
		t.Helper()

		events := make(chan testEvent, 32)
		consumer := &fakeFetchConsumer{
			batches: []jetstream.MessageBatch{fakeBatchWithMessagesAndError(t, 0, batchErr)},
			events:  events,
		}
		replayer := newReplayerWithFakeConsumer("helix-worker-high-A", consumer)

		cfg := newTestNATSBackendConfig()
		cfg.PollInterval = time.Hour

		var executed atomic.Int32
		execute := func(_ context.Context, _ types.ReplayPayload) error {
			executed.Add(1)

			return nil
		}

		gate := &backoffGate{events: events, release: make(chan time.Time)}

		stopCh := make(chan struct{})
		var wg sync.WaitGroup
		b := &natsBackend{
			replayer:    replayer,
			config:      &cfg,
			execute:     execute,
			stopCh:      stopCh,
			wg:          &wg,
			backoffWait: gate.wait,
		}

		return &backoffTestFixture{events: events, executed: &executed, backend: b, gate: gate, stopCh: stopCh, wg: &wg}
	}

	t.Run("does not spin and honors stop during backoff", func(t *testing.T) {
		fx := setup(t)
		h := startWorker(t, fx.backend, fx.stopCh, fx.wg, fx.events)

		fetch1 := recvEvent(t, fx.events, "first Fetch")
		require.Equal(t, "fetch", fetch1.kind)

		backoffEv := recvEvent(t, fx.events, "backoff entry")
		require.Equal(t, "backoff", backoffEv.kind,
			"a spin-loop regression never calls backoffWait, so this must be observed before anything else")

		requireNoFetchQueued(t, fx.events)
		assert.Equal(t, int32(0), fx.executed.Load(),
			"execute must never run when the dequeue error carries zero messages")

		// The worker is parked on the gate's unreleased release channel;
		// closing stopCh must unblock it promptly, proving the backoff
		// select honors stop.
		h.stop(t)
	})

	t.Run("resumes with another fetch after backoff release", func(t *testing.T) {
		fx := setup(t)
		startWorker(t, fx.backend, fx.stopCh, fx.wg, fx.events)

		fetch1 := recvEvent(t, fx.events, "first Fetch")
		require.Equal(t, "fetch", fetch1.kind)

		backoffEv := recvEvent(t, fx.events, "backoff entry")
		require.Equal(t, "backoff", backoffEv.kind)

		fx.gate.release <- time.Now()

		fetch2 := recvEvent(t, fx.events, "second Fetch")
		require.Equal(t, "fetch", fetch2.kind)

		assert.Equal(t, int32(0), fx.executed.Load(),
			"execute must never run when the dequeue error carries zero messages")
	})
}

// settlesTestFixture holds the fresh backend/consumer/events/gate wiring
// built per subtest by
// TestNATSBackendStartSettlesMessagesBeforeBackingOffOnPartialFetchError, so
// mutable fake state (the consumer's batches slice, the gate's release
// channel) is never shared across subtests.
type settlesTestFixture struct {
	events  chan testEvent
	backend *natsBackend
	gate    *backoffGate
	stopCh  chan struct{}
	wg      *sync.WaitGroup
}

// TestNATSBackendStartSettlesMessagesBeforeBackingOffOnPartialFetchError is
// the end-to-end regression test for nats_worker.go's start(): a dequeue
// result carrying both messages and an error must have its messages
// executed and settled before the error causes the poll loop to log and
// back off. Without the original fix, result.err != nil short-circuited
// the loop before processMessages ever ran, burning a JetStream delivery
// attempt with execute() never called.
//
// The exact-ordered-prefix assertion — fetch(1) -> execute(0) -> ack(0) ->
// execute(1) -> ack(1) — is unchanged from the original test and proves
// each payload executes exactly once, each processed message is Ack'd, and
// settlement happens before the loop attempts a second Fetch.
//
// What changed is how "the second Fetch does not happen before the
// PollInterval backoff gate" is proven. The original test measured the real
// wall-clock gap between two observed Fetch events against PollInterval,
// which is flaky in both directions — see
// TestNATSBackendStartBacksOffWithoutProcessingOnZeroMessageError's comment
// for why (a later, still-flawed revision replaced it with a bounded
// silence window, which removed the false-FAIL direction but left a
// spin-regressed-and-then-descheduled worker able to false-PASS). It is
// replaced with two subtests, both driven by natsBackend.backoffWait (see
// backoffGate) sharing the same ordered-prefix helper, with zero wall-clock
// oracles:
//   - "does not spin during backoff, settles messages first" requires the
//     "backoff" event after the ordered prefix (a spin-loop regression
//     never calls backoffWait, so this is deterministic proof either way,
//     not a timing signal), then requires no fetch is already queued (zero
//     timing window: the worker is provably parked on the gate's
//     unreleased release channel), then stops the worker with the gate
//     still unreleased, proving the backoff select honors stopCh.
//   - "resumes with another fetch after backoff release" proves the other
//     half: after the same ordered-prefix-then-backoff proof, it releases
//     the gate and requires a second Fetch, proving the loop actually
//     resumes.
func TestNATSBackendStartSettlesMessagesBeforeBackingOffOnPartialFetchError(t *testing.T) {
	const wantExecuted = 2

	batchErr := errors.New("simulated transport error mid-fetch")
	quietErr := errors.New("simulated transient fetch error, nothing to process")

	setup := func(t *testing.T) *settlesTestFixture {
		t.Helper()

		events := make(chan testEvent, 32)

		// Default WorkerConfig has StrictPriority=false and
		// HighPriorityRatio=10, so with highProcessed starting at 0,
		// start() takes the dequeueHighFirst path on every iteration
		// below; the low-priority consumer is never looked up, so it
		// does not need a fake registered.
		consumer := &fakeFetchConsumer{
			batches: []jetstream.MessageBatch{
				fakeBatchWithTrackedMessages(t, wantExecuted, batchErr, events),
				// Every fetch after the first still errors with zero
				// messages, so dequeueHighFirst keeps short-circuiting
				// before it would ever fall through to the
				// (unregistered) low-priority fetch.
				fakeBatchWithMessagesAndError(t, 0, quietErr),
			},
			events: events,
		}
		replayer := newReplayerWithFakeConsumer("helix-worker-high-A", consumer)

		cfg := newTestNATSBackendConfig()
		cfg.PollInterval = time.Hour

		execute := func(_ context.Context, payload types.ReplayPayload) error {
			events <- testEvent{kind: "execute", id: payloadTestID(t, payload)}

			return nil
		}

		gate := &backoffGate{events: events, release: make(chan time.Time)}

		stopCh := make(chan struct{})
		var wg sync.WaitGroup
		b := &natsBackend{
			replayer:    replayer,
			config:      &cfg,
			execute:     execute,
			stopCh:      stopCh,
			wg:          &wg,
			backoffWait: gate.wait,
		}

		return &settlesTestFixture{events: events, backend: b, gate: gate, stopCh: stopCh, wg: &wg}
	}

	// assertOrderedPrefix consumes and asserts the fetch(1) -> execute(0) ->
	// ack(0) -> execute(1) -> ack(1) prefix shared by both subtests below.
	assertOrderedPrefix := func(t *testing.T, events chan testEvent) {
		t.Helper()

		fetch1 := recvEvent(t, events, "first Fetch")
		require.Equal(t, "fetch", fetch1.kind)

		for _, id := range []int{0, 1} {
			exec := recvEvent(t, events, "execute of message")
			require.Equal(t, "execute", exec.kind)
			assert.Equal(t, id, exec.id, "messages must execute in fetch order, exactly once each")

			ack := recvEvent(t, events, "ack of message")
			require.Equal(t, "ack", ack.kind)
			assert.Equal(t, id, ack.id, "each processed message must be Ack'd, matching the message just executed")
		}
	}

	t.Run("does not spin during backoff, settles messages first", func(t *testing.T) {
		fx := setup(t)
		h := startWorker(t, fx.backend, fx.stopCh, fx.wg, fx.events)

		assertOrderedPrefix(t, fx.events)

		// The second Fetch must not be observed before both messages
		// above have been executed and Ack'd: start() only reaches the
		// error backoff (and therefore the next loop iteration's Fetch)
		// after processMessages returns.
		backoffEv := recvEvent(t, fx.events, "backoff entry")
		require.Equal(t, "backoff", backoffEv.kind,
			"a spin-loop regression never calls backoffWait, so this must be observed after the ordered prefix")

		requireNoFetchQueued(t, fx.events)

		// The worker is parked on the gate's unreleased release channel;
		// closing stopCh must unblock it promptly, proving the backoff
		// select honors stop.
		h.stop(t)
	})

	t.Run("resumes with another fetch after backoff release", func(t *testing.T) {
		fx := setup(t)
		startWorker(t, fx.backend, fx.stopCh, fx.wg, fx.events)

		assertOrderedPrefix(t, fx.events)

		backoffEv := recvEvent(t, fx.events, "backoff entry")
		require.Equal(t, "backoff", backoffEv.kind)

		fx.gate.release <- time.Now()

		fetch2 := recvEvent(t, fx.events, "second Fetch")
		require.Equal(t, "fetch", fetch2.kind)
	})
}
