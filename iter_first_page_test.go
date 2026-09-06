package helix

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/types"
)

// firstPageCtx is unexported, so the interface assertion lives here per the
// public-package rule.
var _ context.Context = (*firstPageCtx)(nil)

// ─────────────────────────────────────────────
// firstPageCtx state machine
// ─────────────────────────────────────────────

// legTimeoutForTest is long enough that no test's wall clock reaches it:
// every ordering is driven by calling legExpired, callerEnded and finish
// directly, so the state machine's tests never race a real timer.
const legTimeoutForTest = time.Hour

func TestFirstPageCtx_DelegatesToTheCaller(t *testing.T) {
	type ctxKey string
	const key ctxKey = "k"

	deadline := time.Now().Add(time.Minute)
	parent, cancel := context.WithDeadline(context.WithValue(t.Context(), key, "v"), deadline)
	t.Cleanup(cancel)

	fp := newFirstPageCtx(parent, legTimeoutForTest)
	t.Cleanup(fp.finish)

	got, ok := fp.Deadline()
	require.True(t, ok, "the caller's deadline is the only one advertised")
	require.Equal(t, deadline, got, "the leg deadline must never reach the driver")
	require.Equal(t, "v", fp.Value(key), "Value delegates to the caller")
	require.NoError(t, fp.Err(), "an armed context follows a live caller")

	done := fp.Done()
	require.Equal(t, done, fp.Done(), "Done returns one channel for the context's lifetime")

	fp.legExpired()
	require.Equal(t, done, fp.Done(), "the channel survives the latch")

	plain := newFirstPageCtx(t.Context(), legTimeoutForTest)
	t.Cleanup(plain.finish)
	_, ok = plain.Deadline()
	require.False(t, ok, "a caller without a deadline is reported as having none")
}

// lockProbeParent reports whether the leg context still held its lock
// when it asked the caller for an error.
type lockProbeParent struct {
	ctx    context.Context
	fp     *firstPageCtx
	probed bool
	held   bool
}

func (p *lockProbeParent) Deadline() (time.Time, bool) { return p.ctx.Deadline() }
func (p *lockProbeParent) Done() <-chan struct{}       { return p.ctx.Done() }
func (p *lockProbeParent) Value(key any) any           { return p.ctx.Value(key) }

func (p *lockProbeParent) Err() error {
	if p.fp != nil && !p.probed {
		p.probed = true
		if p.fp.mu.TryLock() {
			p.fp.mu.Unlock()
		} else {
			p.held = true
		}
	}

	return p.ctx.Err()
}

func TestFirstPageCtx_ErrReadsTheCallerUnderOneLock(t *testing.T) {
	inner, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	parent := &lockProbeParent{ctx: inner}
	fp := newFirstPageCtx(parent, legTimeoutForTest)
	// Cleanups run last-in-first-out, so finish detaches the registration
	// before the parent is cancelled and callerEnded never runs: the probe
	// below is the only concurrent-free reader.
	t.Cleanup(fp.finish)
	parent.fp = fp

	require.NoError(t, fp.Err(), "an armed context follows a live caller")
	require.True(t, parent.probed, "Err consulted the caller")
	require.True(t, parent.held,
		"the latched error and the caller's must be read under one critical section: "+
			"a latch landing between them would let one reader report the caller's error and the next the leg's")
}

func TestFirstPageCtx_LegTimerBeatsDisarm(t *testing.T) {
	fp := newFirstPageCtx(t.Context(), legTimeoutForTest)
	t.Cleanup(fp.finish)

	fp.legExpired()
	require.False(t, fp.disarm(), "the leg expired before the driver answered")
	requireClosed(t, fp, "an expired leg ends its context")
	require.ErrorIs(t, fp.Err(), context.DeadlineExceeded)
	require.False(t, fp.disarm(), "a second disarm still reports the loss")
	require.ErrorIs(t, fp.Err(), context.DeadlineExceeded)
}

func TestFirstPageCtx_DisarmBeatsLegTimer(t *testing.T) {
	fp := newFirstPageCtx(t.Context(), legTimeoutForTest)
	t.Cleanup(fp.finish)

	require.True(t, fp.disarm(), "the driver answered in time")
	requireOpen(t, fp, "a disarmed context follows the caller alone")
	require.NoError(t, fp.Err())

	// The callback that was already running when disarm took the mutex.
	fp.legExpired()
	requireOpen(t, fp, "a timer that lost the race must not end the iterator's context")
	require.NoError(t, fp.Err())
	require.True(t, fp.disarm(), "the win stands")
}

func TestFirstPageCtx_CallerEndsWhileArmed(t *testing.T) {
	parent := newManualParent()
	fp := newFirstPageCtx(parent, legTimeoutForTest)
	t.Cleanup(fp.finish)

	// The parent's registration is driven by hand, so the callback under
	// test is the only one that runs.
	parent.end()
	fp.callerEnded()

	require.True(t, fp.disarm(), "a caller that gave up is not a leg timeout")
	requireClosed(t, fp, "the caller's end reaches the driver")
	require.ErrorIs(t, fp.Err(), context.Canceled)

	fp.legExpired()
	require.ErrorIs(t, fp.Err(), context.Canceled, "the caller's error is the one that stands")
}

func TestFirstPageCtx_LegTimerNeverOverridesTheCallersError(t *testing.T) {
	parent, cancel := context.WithCancel(t.Context())
	fp := newFirstPageCtx(parent, legTimeoutForTest)
	t.Cleanup(fp.finish)

	// The schedule the drivers can produce: the caller ends, the leg timer
	// fires before the parent registration runs.
	cancel()
	require.ErrorIs(t, fp.Err(), context.Canceled, "an unlatched context reports the caller's error")

	fp.legExpired()
	require.ErrorIs(t, fp.Err(), context.Canceled, "a latch taken under a dead caller copies its error")

	fp.callerEnded()
	require.ErrorIs(t, fp.Err(), context.Canceled, "the registration changes nothing after the fact")
	require.True(t, fp.disarm(), "a cancellation is the caller's, so the leg did not lose")
}

func TestFirstPageCtx_FinishLatchesCancelAndDetaches(t *testing.T) {
	parent, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	fp := newFirstPageCtx(parent, legTimeoutForTest)

	require.True(t, fp.disarm())
	fp.finish()
	requireClosed(t, fp, "finish ends the context the driver still holds")
	require.ErrorIs(t, fp.Err(), context.Canceled)
	require.False(t, fp.stopParent(), "finish released the parent registration")

	cancel()
	require.ErrorIs(t, fp.Err(), context.Canceled, "a caller ending afterwards changes nothing")

	fp.finish()
	require.ErrorIs(t, fp.Err(), context.Canceled, "a second finish is a no-op")
}

func TestFirstPageCtx_FinishKeepsAnEarlierError(t *testing.T) {
	expired := newFirstPageCtx(t.Context(), legTimeoutForTest)
	expired.legExpired()
	expired.finish()
	require.ErrorIs(t, expired.Err(), context.DeadlineExceeded, "finish does not overwrite the leg's expiry")

	parent, cancel := context.WithCancel(t.Context())
	cancelled := newFirstPageCtx(parent, legTimeoutForTest)
	cancel()
	cancelled.callerEnded()
	cancelled.finish()
	require.ErrorIs(t, cancelled.Err(), context.Canceled)
}

// opaqueParent hides whatever context it wraps: it answers no Value
// lookup, so context.AfterFunc cannot find the standard cancellable
// ancestor to register with and has to park a goroutine on Done instead.
type opaqueParent struct{ ctx context.Context }

func (o opaqueParent) Deadline() (time.Time, bool) { return o.ctx.Deadline() }
func (o opaqueParent) Done() <-chan struct{}       { return o.ctx.Done() }
func (o opaqueParent) Err() error                  { return o.ctx.Err() }
func (o opaqueParent) Value(_ any) any             { return nil }

// manualParent never closes Done and ends only when the test says so, so
// its registration never fires on its own and a test drives callerEnded
// itself.
type manualParent struct {
	done chan struct{}

	mu  sync.Mutex
	err error
}

func newManualParent() *manualParent {
	return &manualParent{done: make(chan struct{})}
}

func (p *manualParent) Deadline() (time.Time, bool) { return time.Time{}, false }
func (p *manualParent) Done() <-chan struct{}       { return p.done }
func (p *manualParent) Value(_ any) any             { return nil }

func (p *manualParent) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.err
}

// end makes the parent report a caller cancellation.
func (p *manualParent) end() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = context.Canceled
}

func TestFirstPageCtx_NonStandardParentPropagates(t *testing.T) {
	inner, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	fp := newFirstPageCtx(opaqueParent{ctx: inner}, legTimeoutForTest)
	t.Cleanup(fp.finish)
	require.True(t, fp.disarm())

	requireOpen(t, fp, "a live caller keeps the leg context open")
	cancel()
	waitDone(t, fp, "a custom parent still ends the leg context")
	require.ErrorIs(t, fp.Err(), context.Canceled)
}

func TestFirstPageCtx_NonStandardParentIsDetachedByFinish(t *testing.T) {
	inner, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	self := goroutineID()
	require.Zero(t, parkedAfterFuncs(self))
	fp := newFirstPageCtx(opaqueParent{ctx: inner}, legTimeoutForTest)
	require.True(t, fp.disarm())
	require.Equal(t, 1, parkedAfterFuncs(self),
		"a parent that hides the cancel key makes context.AfterFunc park a goroutine")

	// The caller is still live, so only finish can release that goroutine.
	fp.finish()
	deadline := time.Now().Add(5 * time.Second)
	for parkedAfterFuncs(self) > 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	require.Zero(t, parkedAfterFuncs(self),
		"finish must release the registration parked on a non-standard parent")
	require.False(t, fp.stopParent(), "finish already stopped it")
}

// goroutineID returns the id of the calling goroutine, read from the header
// of its own stack dump.
func goroutineID() string {
	buf := make([]byte, 64)
	n := runtime.Stack(buf, false)

	return strings.Fields(string(buf[:n]))[1]
}

// parkedAfterFuncs counts the goroutines context.AfterFunc parked on behalf
// of the goroutine with id creator.
//
// A parent that hides the cancel key makes propagateCancel wait on Done in a
// goroutine of its own, and the dump names who spawned it. Counting only
// those is blind to goroutines a sibling test left exiting; a bare
// runtime.NumGoroutine comparison is not, which is what made the assertion
// above flake.
func parkedAfterFuncs(creator string) int {
	// The trailing newline anchors the match to the end of the "created by"
	// line, so goroutine 7 does not also match goroutine 71.
	marker := "propagateCancel in goroutine " + creator + "\n"

	buf := make([]byte, 64<<10)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Count(string(buf[:n]), marker)
		}
		buf = make([]byte, 2*len(buf)) // dump was truncated; retry bigger
	}
}

func TestFirstPageCtx_AbandonedContextIsReleasedByItsParent(t *testing.T) {
	parent, cancel := context.WithCancel(t.Context())
	fp := newFirstPageCtx(parent, legTimeoutForTest)
	require.True(t, fp.disarm())

	// No finish: the caller walked away from the iterator.
	cancel()
	waitDone(t, fp, "the caller's own cancellation releases an abandoned registration")
	require.ErrorIs(t, fp.Err(), context.Canceled)
}

func requireClosed(t *testing.T, fp *firstPageCtx, msg string) {
	t.Helper()
	select {
	case <-fp.Done():
	default:
		t.Fatal(msg)
	}
}

func requireOpen(t *testing.T, fp *firstPageCtx, msg string) {
	t.Helper()
	select {
	case <-fp.Done():
		t.Fatal(msg)
	default:
	}
}

// waitDone blocks until the context ends, so a propagation that runs on
// context.AfterFunc's own goroutine is asserted without polling.
func waitDone(t *testing.T, fp *firstPageCtx, msg string) {
	t.Helper()
	select {
	case <-fp.Done():
	case <-time.After(5 * time.Second):
		t.Fatal(msg)
	}
}

// ─────────────────────────────────────────────
// Test doubles for the first-page flow
// ─────────────────────────────────────────────

// iterMode picks what a firstPageSession does when a first page is fetched.
type iterMode uint8

const (
	// iterAnswers hands back an iterator at once, as a healthy cluster does.
	iterAnswers iterMode = iota
	// iterHangs waits for the leg context to end and then hands back an
	// iterator that reports that context's error: a frozen cluster.
	iterHangs
	// iterAnswersLate waits for the leg context to end and then hands back a
	// healthy iterator: the handoff race, made deterministic.
	iterAnswersLate
)

// firstPageSession is a cql.Session whose iterator behaviour each test
// picks, and which records every query built on it.
type firstPageSession struct {
	mode      iterMode
	rows      []string // one value per Scan
	token     []byte   // driver paging token the iterator reports
	closeErr  error    // what the iterator's Close returns
	afterWake func()   // runs once the leg context ended, before answering
	entered   chan struct{}
	enterOnce sync.Once

	mu      sync.Mutex
	queries []*firstPageQuery
	closes  int
	scans   int
}

func newFirstPageSession(mode iterMode) *firstPageSession {
	return &firstPageSession{mode: mode, entered: make(chan struct{})}
}

func (s *firstPageSession) Query(stmt string, values ...any) cql.Query {
	q := &firstPageQuery{session: s, statement: stmt, values: values}
	s.mu.Lock()
	s.queries = append(s.queries, q)
	s.mu.Unlock()

	return q
}

func (s *firstPageSession) Batch(_ cql.BatchType) cql.Batch { return nil }
func (s *firstPageSession) Close()                          {}

// lastQuery returns the most recent query built on this session.
func (s *firstPageSession) lastQuery(t *testing.T) *firstPageQuery {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	require.NotEmpty(t, s.queries, "the session was never queried")

	return s.queries[len(s.queries)-1]
}

func (s *firstPageSession) queryCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.queries)
}

func (s *firstPageSession) counts() (closes, scans int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closes, s.scans
}

type firstPageQuery struct {
	session           *firstPageSession
	statement         string
	values            []any
	consistency       *cql.Consistency
	serialConsistency *cql.Consistency
	pageSize          *int

	mu  sync.Mutex
	ctx context.Context
}

// legContext returns the context the driver was handed for the first page.
func (q *firstPageQuery) legContext() context.Context {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.ctx
}

func (q *firstPageQuery) Consistency(c cql.Consistency) cql.Query {
	q.consistency = &c
	return q
}

func (q *firstPageQuery) SerialConsistency(c cql.Consistency) cql.Query {
	q.serialConsistency = &c
	return q
}

func (q *firstPageQuery) PageSize(n int) cql.Query {
	q.pageSize = &n
	return q
}

func (q *firstPageQuery) PageState(_ []byte) cql.Query    { return q }
func (q *firstPageQuery) WithTimestamp(_ int64) cql.Query { return q }
func (q *firstPageQuery) Statement() string               { return q.statement }
func (q *firstPageQuery) Values() []any                   { return q.values }
func (q *firstPageQuery) Release()                        {}
func (q *firstPageQuery) Exec() error                     { return nil }
func (q *firstPageQuery) ExecContext(_ context.Context) error {
	return nil
}
func (q *firstPageQuery) Scan(_ ...any) error { return nil }

func (q *firstPageQuery) ScanContext(ctx context.Context, _ ...any) error {
	q.mu.Lock()
	q.ctx = ctx
	q.mu.Unlock()
	if q.session.mode == iterAnswers {
		return nil
	}
	q.session.enter()
	<-ctx.Done()

	return ctx.Err()
}

func (q *firstPageQuery) Iter() cql.Iter { return q.IterContext(context.Background()) }

func (q *firstPageQuery) IterContext(ctx context.Context) cql.Iter {
	q.mu.Lock()
	q.ctx = ctx
	q.mu.Unlock()

	if q.session.mode != iterAnswers {
		q.session.enter()
		<-ctx.Done()
		if q.session.afterWake != nil {
			q.session.afterWake()
		}
	}
	if q.session.mode == iterHangs {
		return &firstPageIter{session: q.session, err: ctx.Err()}
	}

	return &firstPageIter{
		session: q.session,
		rows:    q.session.rows,
		token:   q.session.token,
		err:     q.session.closeErr,
	}
}

func (q *firstPageQuery) MapScan(_ map[string]any) error { return nil }
func (q *firstPageQuery) MapScanContext(ctx context.Context, _ map[string]any) error {
	return q.ScanContext(ctx)
}
func (q *firstPageQuery) ScanCAS(_ ...any) (bool, error) { return false, nil }
func (q *firstPageQuery) ScanCASContext(_ context.Context, _ ...any) (bool, error) {
	return false, nil
}
func (q *firstPageQuery) MapScanCAS(_ map[string]any) (bool, error) { return false, nil }
func (q *firstPageQuery) MapScanCASContext(_ context.Context, _ map[string]any) (bool, error) {
	return false, nil
}

// enter signals, once, that a read reached the session and is about to
// wait for its context.
func (s *firstPageSession) enter() {
	s.enterOnce.Do(func() { close(s.entered) })
}

type firstPageIter struct {
	session *firstPageSession
	rows    []string
	token   []byte
	err     error
	idx     int
}

func (i *firstPageIter) Scan(dest ...any) bool {
	i.session.mu.Lock()
	i.session.scans++
	i.session.mu.Unlock()
	if i.idx >= len(i.rows) {
		return false
	}
	if len(dest) > 0 {
		if p, ok := dest[0].(*string); ok {
			*p = i.rows[i.idx]
		}
	}
	i.idx++

	return true
}

func (i *firstPageIter) Close() error {
	i.session.mu.Lock()
	i.session.closes++
	i.session.mu.Unlock()

	return i.err
}

func (i *firstPageIter) MapScan(_ map[string]any) bool       { return false }
func (i *firstPageIter) SliceMap() ([]map[string]any, error) { return nil, i.err }
func (i *firstPageIter) PageState() []byte                   { return i.token }
func (i *firstPageIter) NumRows() int                        { return len(i.rows) }
func (i *firstPageIter) Columns() []cql.ColumnInfo           { return nil }
func (i *firstPageIter) Scanner() cql.Scanner                { return &firstPageScanner{iter: i} }
func (i *firstPageIter) Warnings() []string                  { return nil }

// firstPageScanner mirrors the drivers: Err closes the iterator it came
// from and releases it, so a second call is the driver's business and not
// something Helix does on the caller's behalf.
type firstPageScanner struct {
	iter   *firstPageIter
	closed bool
}

func (s *firstPageScanner) Next() bool          { return s.iter.Scan() }
func (s *firstPageScanner) Scan(_ ...any) error { return nil }

func (s *firstPageScanner) Err() error {
	if s.closed {
		return s.iter.err
	}
	s.closed = true

	return s.iter.Close()
}

// legMetrics records the read-path metrics one leg attempt produces.
type legMetrics struct {
	sync.Mutex
	readTotal    map[ClusterID]int
	readError    map[ClusterID]int
	durations    map[ClusterID]int
	callerExpiry map[ClusterID]int
	failovers    int
	sink         *callSink // optional gating-order recorder
}

func newLegMetrics() *legMetrics {
	return &legMetrics{
		readTotal:    make(map[ClusterID]int),
		readError:    make(map[ClusterID]int),
		durations:    make(map[ClusterID]int),
		callerExpiry: make(map[ClusterID]int),
	}
}

func (m *legMetrics) count(mp map[ClusterID]int, c ClusterID) {
	m.Lock()
	defer m.Unlock()
	mp[c]++
}

func (m *legMetrics) get(mp map[ClusterID]int, c ClusterID) int {
	m.Lock()
	defer m.Unlock()

	return mp[c]
}

func (m *legMetrics) totalFailovers() int {
	m.Lock()
	defer m.Unlock()

	return m.failovers
}

func (m *legMetrics) IncReadTotal(c ClusterID) {
	m.sink.record("read_total:" + string(c))
	m.count(m.readTotal, c)
}

func (m *legMetrics) IncReadError(c ClusterID) {
	m.sink.record("read_error:" + string(c))
	m.count(m.readError, c)
}

func (m *legMetrics) ObserveReadDuration(c ClusterID, _ float64) { m.count(m.durations, c) }

// IncReadCallerExpired and IncWriteCallerExpired make this collector a
// types.CallerContextMetrics, the optional interface the hub reports a
// caller-expired attempt through.
func (m *legMetrics) IncReadCallerExpired(c ClusterID)  { m.count(m.callerExpiry, c) }
func (m *legMetrics) IncWriteCallerExpired(_ ClusterID) {}

func (m *legMetrics) IncFailoverTotal(from, to ClusterID) {
	m.sink.record("failover:" + string(from) + "->" + string(to))
	m.Lock()
	defer m.Unlock()
	m.failovers++
}

func (m *legMetrics) IncReadDivergence(_ ClusterID)                {}
func (m *legMetrics) IncWriteTotal(_ ClusterID)                    {}
func (m *legMetrics) IncWriteError(_ ClusterID)                    {}
func (m *legMetrics) IncWriteAsync(_ ClusterID)                    {}
func (m *legMetrics) IncWriteDropped(_ ClusterID)                  {}
func (m *legMetrics) ObserveWriteDuration(_ ClusterID, _ float64)  {}
func (m *legMetrics) SetCircuitBreakerState(_ ClusterID, _ int)    {}
func (m *legMetrics) IncCircuitBreakerTrip(_ ClusterID)            {}
func (m *legMetrics) IncReplayEnqueued(_ ClusterID)                {}
func (m *legMetrics) IncReplaySuccess(_ ClusterID)                 {}
func (m *legMetrics) IncReplayError(_ ClusterID)                   {}
func (m *legMetrics) IncReplayDropped(_ ClusterID)                 {}
func (m *legMetrics) SetReplayQueueDepth(_ ClusterID, _ int)       {}
func (m *legMetrics) ObserveReplayDuration(_ ClusterID, _ float64) {}
func (m *legMetrics) SetClusterDraining(_ ClusterID, _ bool)       {}
func (m *legMetrics) IncDrainModeEntered(_ ClusterID)              {}
func (m *legMetrics) IncDrainModeExited(_ ClusterID)               {}

// latencyPolicy is a FailoverPolicy that also implements LatencyRecorder,
// so a test can prove no first page ever records a latency sample.
type latencyPolicy struct {
	sync.Mutex
	allowFailover bool
	latencies     []ClusterID
	successes     []ClusterID
	failures      []ClusterID
}

func (p *latencyPolicy) ShouldFailover(_ ClusterID, _ error) bool { return p.allowFailover }

func (p *latencyPolicy) RecordFailure(c ClusterID) {
	p.Lock()
	defer p.Unlock()
	p.failures = append(p.failures, c)
}

func (p *latencyPolicy) RecordSuccess(c ClusterID) {
	p.Lock()
	defer p.Unlock()
	p.successes = append(p.successes, c)
}

func (p *latencyPolicy) RecordLatency(c ClusterID, _ time.Duration) {
	p.Lock()
	defer p.Unlock()
	p.latencies = append(p.latencies, c)
}

func (p *latencyPolicy) recorded() (latencies, successes, failures []ClusterID) {
	p.Lock()
	defer p.Unlock()

	return p.latencies, p.successes, p.failures
}

// iterEvents collects cluster events, which are delivered on the client's
// own dispatcher goroutine.
type iterEvents struct {
	mu     sync.Mutex
	events []types.ClusterEvent
	notify chan struct{}
}

func newIterEvents() *iterEvents {
	return &iterEvents{notify: make(chan struct{}, 1)}
}

func (r *iterEvents) handler(ev types.ClusterEvent) {
	r.mu.Lock()
	r.events = append(r.events, ev)
	r.mu.Unlock()
	select {
	case r.notify <- struct{}{}:
	default:
	}
}

// count reports how many events of the given kind have arrived so far.
func (r *iterEvents) count(kind types.ClusterEventKind) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, ev := range r.events {
		if ev.Kind == kind {
			n++
		}
	}

	return n
}

// waitFor blocks until an event of the given kind arrives and returns it.
func (r *iterEvents) waitFor(t *testing.T, kind types.ClusterEventKind) types.ClusterEvent {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		r.mu.Lock()
		for _, ev := range r.events {
			if ev.Kind == kind {
				r.mu.Unlock()

				return ev
			}
		}
		r.mu.Unlock()
		select {
		case <-r.notify:
		case <-deadline:
			t.Fatalf("timeout waiting for a %s event", kind)
		}
	}
}

// iterTestClient builds a dual-cluster client over the two sessions with a
// short leg deadline, a pinned read strategy, and recording doubles.
type iterTestClient struct {
	client   *CQLClient
	metrics  *legMetrics
	strategy *trackingReadStrategy
	policy   *trackingFailoverPolicy
	events   *iterEvents
}

func newIterTestClient(t *testing.T, sa, sb cql.Session, opts ...Option) *iterTestClient {
	t.Helper()
	h := &iterTestClient{
		metrics:  newLegMetrics(),
		strategy: &trackingReadStrategy{preferred: ClusterA},
		policy:   &trackingFailoverPolicy{ShouldFailoverAllow: true},
		events:   newIterEvents(),
	}
	base := []Option{
		WithClusterReadTimeout(20 * time.Millisecond),
		WithReadStrategy(h.strategy),
		WithFailoverPolicy(h.policy),
		WithMetrics(h.metrics),
		WithOnClusterEvent(h.events.handler),
	}
	client, err := NewCQLClient(sa, sb, append(base, opts...)...)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	h.client = client

	return h
}

// newAnsweringClient builds a client over sa and an answering alternative,
// under a leg deadline no test's wall clock reaches: the shape every test
// of a first page the driver served in time needs.
func newAnsweringClient(t *testing.T, sa *firstPageSession) *iterTestClient {
	t.Helper()

	return newIterTestClient(t, sa, newFirstPageSession(iterAnswers),
		WithClusterReadTimeout(legTimeoutForTest))
}

// legCtx returns the leg context the driver was handed for sa's last
// first page.
func legCtx(t *testing.T, sa *firstPageSession) *firstPageCtx {
	t.Helper()
	fp, ok := sa.lastQuery(t).legContext().(*firstPageCtx)
	require.True(t, ok, "an armed leg hands the driver its own context")

	return fp
}

// query is the statement every first-page test reads.
func (h *iterTestClient) query() Query {
	return h.client.Query("SELECT v FROM t WHERE k = ?", 1)
}

// ─────────────────────────────────────────────
// IterContext: the first page as a read leg
// ─────────────────────────────────────────────

func TestIterFirstPage_TimeoutInactiveKeepsTheCallerContext(t *testing.T) {
	t.Run("dual cluster with no leg deadline", func(t *testing.T) {
		sa := newFirstPageSession(iterAnswers)
		h := newIterTestClient(t, sa, newFirstPageSession(iterAnswers), WithClusterReadTimeout(0))

		ctx := t.Context()
		iter := h.query().IterContext(ctx)
		t.Cleanup(func() { _ = iter.Close() })

		require.True(t, sa.lastQuery(t).legContext() == ctx,
			"without a leg deadline the driver keeps the caller's own context object")
		require.Zero(t, h.metrics.get(h.metrics.readTotal, ClusterA), "no leg attempt is counted")
	})

	t.Run("single cluster with a leg deadline", func(t *testing.T) {
		sa := newFirstPageSession(iterAnswers)
		h := newIterTestClient(t, sa, nil)

		ctx := t.Context()
		iter := h.query().IterContext(ctx)
		t.Cleanup(func() { _ = iter.Close() })

		require.True(t, sa.lastQuery(t).legContext() == ctx,
			"a single cluster has no alternative to preserve budget for")
		require.Zero(t, h.metrics.get(h.metrics.readTotal, ClusterA), "no leg attempt is counted")
	})
}

func TestIterFirstPage_ExpiredLegWithoutFailoverReportsClusterTimeout(t *testing.T) {
	sa, sb := newFirstPageSession(iterHangs), newFirstPageSession(iterAnswers)
	h := newIterTestClient(t, sa, sb)
	h.policy.ShouldFailoverAllow = false

	iter := h.query().IterContext(t.Context())
	require.Nil(t, iter.PageState(), "a failed read has no cursor to resume")
	require.ErrorIs(t, iter.Close(), types.ErrClusterTimeout, "the leg ended on Helix's own deadline")

	require.Equal(t, 1, h.metrics.get(h.metrics.readTotal, ClusterA), "the attempt is counted")
	require.Equal(t, 1, h.metrics.get(h.metrics.durations, ClusterA))
	require.Equal(t, 1, h.metrics.get(h.metrics.readError, ClusterA), "the expiry is A's failure")
	require.Equal(t, []ClusterID{ClusterA}, h.policy.RecordFailureCalls)
	require.Equal(t, int32(1), h.client.statsForCluster(ClusterA).consecutiveFailures.Load())
	require.Zero(t, sb.queryCount(), "a refused failover never contacts the alternative")
	require.Zero(t, h.metrics.get(h.metrics.readTotal, ClusterB))
}

func TestIterFirstPage_ExpiredLegIsServedByTheAlternative(t *testing.T) {
	sa, sb := newFirstPageSession(iterHangs), newFirstPageSession(iterAnswers)
	sb.rows = []string{"from-b"}
	sb.token = []byte("driver-token")
	h := newIterTestClient(t, sa, sb)

	iter := h.query().Consistency(Quorum).SerialConsistency(LocalSerial).PageSize(7).
		IterContext(t.Context())

	var got string
	require.True(t, iter.Scan(&got), "the alternative serves the first page")
	require.Equal(t, "from-b", got)

	inner, ok := iter.(*cqlIter)
	require.True(t, ok, "a served read hands back a real iterator")
	require.Equal(t, ClusterB, inner.cluster, "the iterator belongs to the cluster that answered")
	require.Same(t, h.client.holderFor(ClusterB), inner.holder)

	cluster, raw := decodePageState(iter.PageState())
	require.Equal(t, ClusterB, cluster, "the cursor names the cluster that issued it")
	require.Equal(t, []byte("driver-token"), raw)

	require.NoError(t, iter.Close())
	require.Equal(t, []ClusterID{ClusterB}, h.strategy.OnSuccessCalls, "Close credits the alternative")
	require.Equal(t, []ClusterID{ClusterB}, h.policy.RecordSuccessCalls)
	require.Equal(t, 1, h.metrics.totalFailovers())

	ev := h.events.waitFor(t, types.EventFailover)
	require.Equal(t, ClusterA, ev.FromCluster)
	require.Equal(t, ClusterB, ev.ToCluster)
	require.Equal(t, 1, h.events.count(types.EventFailover), "one failover event for one moved read")

	alt := sb.lastQuery(t)
	require.Equal(t, "SELECT v FROM t WHERE k = ?", alt.statement, "the alternative repeats the same read")
	require.Equal(t, []any{1}, alt.values)
	require.Equal(t, Quorum, *alt.consistency)
	require.Equal(t, LocalSerial, *alt.serialConsistency)
	require.Equal(t, 7, *alt.pageSize)
}

func TestIterFirstPage_LateIteratorIsClosedAndDiscarded(t *testing.T) {
	sa, sb := newFirstPageSession(iterAnswersLate), newFirstPageSession(iterAnswers)
	sa.rows = []string{"too-late"}
	sb.rows = []string{"from-b"}
	h := newIterTestClient(t, sa, sb)

	iter := h.query().IterContext(t.Context())

	var got string
	require.True(t, iter.Scan(&got))
	require.Equal(t, "from-b", got, "the caller never sees the late iterator's rows")

	closes, scans := sa.counts()
	require.Equal(t, 1, closes, "the late iterator is disposed of exactly once")
	require.Zero(t, scans, "the late iterator is never scanned")

	require.NoError(t, iter.Close())
	closes, _ = sa.counts()
	require.Equal(t, 1, closes, "closing the served iterator does not touch the discarded one")
	require.Equal(t, 1, h.metrics.get(h.metrics.readError, ClusterA), "the lost leg is reported once")
}

func TestIterFirstPage_BothLegsExpireReportBothErrors(t *testing.T) {
	sa, sb := newFirstPageSession(iterHangs), newFirstPageSession(iterHangs)
	h := newIterTestClient(t, sa, sb)

	err := h.query().IterContext(t.Context()).Close()

	var dual *types.DualClusterError
	require.ErrorAs(t, err, &dual, "both clusters failed, so the caller sees both errors")
	require.ErrorIs(t, dual.ErrorA, types.ErrClusterTimeout)
	require.ErrorIs(t, dual.ErrorB, types.ErrClusterTimeout)
	require.Equal(t, 1, h.metrics.get(h.metrics.readError, ClusterA))
	require.Equal(t, 1, h.metrics.get(h.metrics.readError, ClusterB))
	require.Equal(t, []ClusterID{ClusterA, ClusterB}, h.policy.RecordFailureCalls)
}

func TestIterFirstPage_PageStateNeverMovesCluster(t *testing.T) {
	tests := []struct {
		name    string
		token   []byte
		read    ClusterID
		untouch ClusterID
	}{
		{
			name:    "a token issued by A",
			token:   encodePageState(ClusterA, []byte("driver-token")),
			read:    ClusterA,
			untouch: ClusterB,
		},
		{
			name:    "a token issued by B",
			token:   encodePageState(ClusterB, []byte("driver-token")),
			read:    ClusterB,
			untouch: ClusterA,
		},
		{
			name:    "a raw driver token",
			token:   []byte("a-raw-driver-token-with-no-helix-header"),
			read:    ClusterA,
			untouch: ClusterB,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sa, sb := newFirstPageSession(iterHangs), newFirstPageSession(iterHangs)
			h := newIterTestClient(t, sa, sb)
			sessions := map[ClusterID]*firstPageSession{ClusterA: sa, ClusterB: sb}

			err := h.query().PageState(tt.token).IterContext(t.Context()).Close()
			require.ErrorIs(t, err, types.ErrClusterTimeout, "an expired paged leg stops there")

			require.Equal(t, 1, sessions[tt.read].queryCount(), "the issuing cluster is the one read")
			require.Zero(t, sessions[tt.untouch].queryCount(), "a cursor is never replayed elsewhere")
			require.Equal(t, 1, h.metrics.get(h.metrics.readError, tt.read), "the failure is still recorded")
			require.Zero(t, h.metrics.totalFailovers(), "a paged read emits no failover")
		})
	}
}

func TestIterFirstPage_OverrideRoutesTheSecondAttempt(t *testing.T) {
	t.Run("both clusters allowed", func(t *testing.T) {
		sa, sb := newFirstPageSession(iterHangs), newFirstPageSession(iterAnswers)
		sb.rows = []string{"from-b"}
		h := newIterTestClient(t, sa, sb, WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterA, ClusterB}
		}))

		iter := h.query().IterContext(t.Context())
		var got string
		require.True(t, iter.Scan(&got))
		require.Equal(t, "from-b", got, "the snapshot's fallback serves the read")
		require.NoError(t, iter.Close())

		require.Empty(t, h.strategy.OnSuccessCalls, "an override freezes the read strategy")
		require.Equal(t, []ClusterID{ClusterB}, h.policy.RecordSuccessCalls, "the policy still hears it")
	})

	t.Run("the alternative fenced off", func(t *testing.T) {
		sa, sb := newFirstPageSession(iterHangs), newFirstPageSession(iterAnswers)
		h := newIterTestClient(t, sa, sb, WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterA}
		}))

		err := h.query().IterContext(t.Context()).Close()
		require.ErrorIs(t, err, types.ErrClusterTimeout)
		require.Zero(t, sb.queryCount(), "a fenced-off cluster is never contacted")
		require.Zero(t, h.metrics.totalFailovers())
	})
}

func TestIterFirstPage_CallerCancellationReportsNothing(t *testing.T) {
	sa, sb := newFirstPageSession(iterHangs), newFirstPageSession(iterAnswers)
	// A leg deadline far beyond the test: the caller is the one who gives up.
	h := newIterTestClient(t, sa, sb, WithClusterReadTimeout(legTimeoutForTest))

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		<-sa.entered
		cancel()
	}()

	err := h.query().IterContext(ctx).Close()
	require.ErrorIs(t, err, context.Canceled, "the caller's own error reaches the caller")

	require.Equal(t, 1, h.metrics.get(h.metrics.readTotal, ClusterA), "the attempt is still counted")
	require.Equal(t, 1, h.metrics.get(h.metrics.durations, ClusterA))
	require.Zero(t, h.metrics.get(h.metrics.readError, ClusterA), "a cancellation is not the cluster's fault")
	require.Empty(t, h.policy.RecordFailureCalls)
	require.Empty(t, h.strategy.OnFailureCalls)
	require.Equal(t, int32(0), h.client.statsForCluster(ClusterA).consecutiveFailures.Load())
	require.Zero(t, h.metrics.totalFailovers(), "a dead caller cannot succeed on the other cluster")
	require.Zero(t, sb.queryCount())
	require.Equal(t, 1, h.metrics.get(h.metrics.callerExpiry, ClusterA),
		"the caller-expired counter is the only signal such an attempt leaves")
}

// TestIterFirstPage_DiscardedLegCountsTheCallerExpiry covers the leg the
// timer latched and the caller then abandoned: the driver's iterator is
// thrown away, no health is reported, and the errorIter reports nothing at
// Close, so the caller-expired counter is the attempt's only trace — the
// same accounting attemptRead gives a Scan.
func TestIterFirstPage_DiscardedLegCountsTheCallerExpiry(t *testing.T) {
	t.Run("the primary leg", func(t *testing.T) {
		sa, sb := newFirstPageSession(iterHangs), newFirstPageSession(iterAnswers)
		h := newIterTestClient(t, sa, sb)

		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		// The caller gives up in the window between the leg's expiry and
		// the driver handing its iterator back.
		sa.afterWake = cancel

		iter := h.query().IterContext(ctx)
		require.ErrorIs(t, iter.Close(), context.Canceled)
		require.ErrorIs(t, iter.Close(), context.Canceled, "a second Close adds nothing")

		require.Equal(t, 1, h.metrics.get(h.metrics.callerExpiry, ClusterA))
		require.Zero(t, h.metrics.get(h.metrics.readError, ClusterA), "the caller is not a cluster fault")
		require.Empty(t, h.policy.RecordFailureCalls)
		require.Zero(t, sb.queryCount(), "a dead caller cannot succeed on the other cluster")
	})

	t.Run("the fallback leg", func(t *testing.T) {
		sa, sb := newFirstPageSession(iterHangs), newFirstPageSession(iterHangs)
		h := newIterTestClient(t, sa, sb)

		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		sb.afterWake = cancel

		iter := h.query().IterContext(ctx)
		require.ErrorIs(t, iter.Close(), context.Canceled)
		require.ErrorIs(t, iter.Close(), context.Canceled, "a second Close adds nothing")

		require.Zero(t, h.metrics.get(h.metrics.callerExpiry, ClusterA),
			"the primary leg expired with a live caller")
		require.Equal(t, 1, h.metrics.get(h.metrics.readError, ClusterA))
		require.Equal(t, 1, h.metrics.get(h.metrics.callerExpiry, ClusterB))
		require.Zero(t, h.metrics.get(h.metrics.readError, ClusterB), "the caller is not a cluster fault")
	})
}

func TestIterFirstPage_MetricsCountEveryLegAttempt(t *testing.T) {
	t.Run("a first page that answers", func(t *testing.T) {
		sa := newFirstPageSession(iterAnswers)
		h := newAnsweringClient(t, sa)

		require.NoError(t, h.query().IterContext(t.Context()).Close())
		require.Equal(t, 1, h.metrics.get(h.metrics.readTotal, ClusterA))
		require.Equal(t, 1, h.metrics.get(h.metrics.durations, ClusterA))
		require.Zero(t, h.metrics.get(h.metrics.readError, ClusterA))
	})

	t.Run("a first page that fails over", func(t *testing.T) {
		sa, sb := newFirstPageSession(iterHangs), newFirstPageSession(iterAnswers)
		h := newIterTestClient(t, sa, sb)

		require.NoError(t, h.query().IterContext(t.Context()).Close())
		require.Equal(t, 1, h.metrics.get(h.metrics.readTotal, ClusterA))
		require.Equal(t, 1, h.metrics.get(h.metrics.readTotal, ClusterB))
		require.Equal(t, 1, h.metrics.get(h.metrics.durations, ClusterA))
		require.Equal(t, 1, h.metrics.get(h.metrics.durations, ClusterB))
		require.Equal(t, 1, h.metrics.get(h.metrics.readError, ClusterA), "only the lost leg is an error")
		require.Zero(t, h.metrics.get(h.metrics.readError, ClusterB))
	})

	t.Run("no latency sample is taken for a first page", func(t *testing.T) {
		sa, sb := newFirstPageSession(iterHangs), newFirstPageSession(iterAnswers)
		policy := &latencyPolicy{allowFailover: true}
		metrics := newLegMetrics()
		client, err := NewCQLClient(sa, sb,
			WithClusterReadTimeout(20*time.Millisecond),
			WithReadStrategy(&trackingReadStrategy{preferred: ClusterA}),
			WithFailoverPolicy(policy),
			WithMetrics(metrics),
		)
		require.NoError(t, err)
		t.Cleanup(client.Close)

		require.NoError(t, client.Query("SELECT v FROM t").IterContext(t.Context()).Close())

		latencies, successes, failures := policy.recorded()
		require.Empty(t, latencies, "an iterator has no single latency sample; Close records the success")
		require.Equal(t, []ClusterID{ClusterB}, successes, "Close credits the cluster that answered")
		require.Equal(t, []ClusterID{ClusterA}, failures)
	})
}

func TestIterFirstPage_AnsweredPageKeepsTodaysIterator(t *testing.T) {
	t.Run("drains and reports once", func(t *testing.T) {
		sa := newFirstPageSession(iterAnswers)
		sa.rows = []string{"r1", "r2"}
		h := newAnsweringClient(t, sa)

		iter := h.query().IterContext(t.Context())
		var got []string
		var v string
		for iter.Scan(&v) {
			got = append(got, v)
		}
		require.Equal(t, []string{"r1", "r2"}, got)

		require.NoError(t, iter.Close())
		require.NoError(t, iter.Close(), "a second Close returns the same error")
		require.Equal(t, []ClusterID{ClusterA}, h.strategy.OnSuccessCalls, "the read is reported once")
		require.Equal(t, []ClusterID{ClusterA}, h.policy.RecordSuccessCalls)
		closes, _ := sa.counts()
		require.Equal(t, 1, closes, "the driver's iterator is closed once")
	})

	t.Run("the retained context outlives the leg deadline", func(t *testing.T) {
		sa := newFirstPageSession(iterAnswers)
		sa.rows = []string{"r1"}
		h := newAnsweringClient(t, sa)

		iter := h.query().IterContext(t.Context())
		t.Cleanup(func() { _ = iter.Close() })

		fp := legCtx(t, sa)

		// The disarmed timer's callback, driven directly: a page served
		// after the leg deadline passed must still reach the caller.
		fp.legExpired()
		require.NoError(t, fp.Err(), "a disarmed leg context never expires")

		var v string
		require.True(t, iter.Scan(&v), "later pages run on the caller's context")
		require.Equal(t, "r1", v)
	})

	t.Run("a caller cancellation after the handoff reaches the driver", func(t *testing.T) {
		sa := newFirstPageSession(iterAnswers)
		h := newAnsweringClient(t, sa)

		ctx, cancel := context.WithCancel(t.Context())
		iter := h.query().IterContext(ctx)
		t.Cleanup(func() { _ = iter.Close() })

		fp := legCtx(t, sa)

		cancel()
		waitDone(t, fp, "the caller's cancellation must still reach the driver's pages")
		require.ErrorIs(t, fp.Err(), context.Canceled)
	})
}

func TestIterFirstPage_ScannerErrReleasesTheLegContext(t *testing.T) {
	sa := newFirstPageSession(iterAnswers)
	h := newAnsweringClient(t, sa)

	iter := h.query().IterContext(t.Context())
	fp := legCtx(t, sa)

	scanner := iter.Scanner()
	require.NoError(t, scanner.Err())
	require.ErrorIs(t, fp.Err(), context.Canceled, "Err releases the leg context the first page ran under")
	require.False(t, fp.stopParent(), "the parent registration is detached")

	closes, _ := sa.counts()
	require.Equal(t, 1, closes, "the driver's own Scanner.Err closed the iterator; Helix adds no second close")

	require.Equal(t, []ClusterID{ClusterA}, h.strategy.OnSuccessCalls, "Err ends the read and reports it, as Close does")
	require.Equal(t, []ClusterID{ClusterA}, h.policy.RecordSuccessCalls)
	require.Empty(t, h.policy.RecordFailureCalls)
}

func TestIterFirstPage_ScannerErrReportsAClusterFailure(t *testing.T) {
	sa := newFirstPageSession(iterAnswers)
	sa.rows = []string{"r1"}
	sa.closeErr = errors.New("cluster A gave up mid-stream")
	h := newAnsweringClient(t, sa)

	// A Scanner consumer that never calls Close: Err is the only place the
	// failure can reach the policy, and a cluster that fails every read
	// must not keep a clean record.
	iter := h.query().IterContext(t.Context())
	scanner := iter.Scanner()
	for scanner.Next() {
		require.NoError(t, scanner.Scan())
	}
	require.ErrorIs(t, scanner.Err(), sa.closeErr)

	require.Equal(t, []ClusterID{ClusterA}, h.policy.RecordFailureCalls,
		"the failover policy sees a Scanner consumer's cluster failure")
	require.Empty(t, h.policy.RecordSuccessCalls)
	require.Empty(t, h.strategy.OnSuccessCalls)

	require.ErrorIs(t, iter.Close(), sa.closeErr,
		"Close after Err returns the error the read ended with, and neither re-reports nor re-closes")
	closes, _ := sa.counts()
	require.Equal(t, 1, closes)
	require.Len(t, h.policy.RecordFailureCalls, 1, "the read is reported exactly once")
}

func TestIterFirstPage_ScannerErrThenCloseReportsOneCleanRead(t *testing.T) {
	sa := newFirstPageSession(iterAnswers)
	sa.rows = []string{"r1"}
	h := newAnsweringClient(t, sa)

	// The everyday idiom: a Scanner loop under a deferred Close.
	iter := h.query().IterContext(t.Context())
	scanner := iter.Scanner()
	for scanner.Next() {
		require.NoError(t, scanner.Scan())
	}
	require.NoError(t, scanner.Err())
	require.NoError(t, iter.Close(), "releasing the leg context must not turn a clean close into an error")

	require.Equal(t, []ClusterID{ClusterA}, h.strategy.OnSuccessCalls, "the read is reported once, as a success")
	require.Equal(t, []ClusterID{ClusterA}, h.policy.RecordSuccessCalls)
	require.Empty(t, h.policy.RecordFailureCalls)
}

func TestIterFirstPage_ClosedClientAndResolveErrorArmNothing(t *testing.T) {
	t.Run("a closed client", func(t *testing.T) {
		sa := newFirstPageSession(iterAnswers)
		h := newIterTestClient(t, sa, newFirstPageSession(iterAnswers))
		h.client.Close()

		require.ErrorIs(t, h.query().IterContext(t.Context()).Close(), types.ErrSessionClosed)
		require.Zero(t, sa.queryCount(), "a closed client never reaches a session")
		require.Zero(t, h.metrics.get(h.metrics.readTotal, ClusterA))
	})

	t.Run("an unresolvable read target", func(t *testing.T) {
		sa := newFirstPageSession(iterAnswers)
		h := newIterTestClient(t, sa, newFirstPageSession(iterAnswers),
			WithAllowedClusters(func() []ClusterID { return []ClusterID{"Z"} }))

		require.ErrorIs(t, h.query().IterContext(t.Context()).Close(), types.ErrInvalidClusterOverride)
		require.Zero(t, sa.queryCount(), "a fail-closed read arms no leg")
		require.Zero(t, h.metrics.get(h.metrics.readTotal, ClusterA))
	})
}

func TestIterFirstPage_ObservationsStayOnTheHolderTheLegUsed(t *testing.T) {
	sa := newFirstPageSession(iterAnswers)
	sa.closeErr = fmt.Errorf("%w: cluster A broke mid-stream", types.ErrClusterUnreachable)
	h := newAnsweringClient(t, sa)

	used := h.client.holderFor(ClusterA)
	iter := h.query().IterContext(t.Context())

	_, err := h.client.SwapSession(ClusterA, newFirstPageSession(iterAnswers))
	require.NoError(t, err)
	installed := h.client.holderFor(ClusterA)
	require.NotSame(t, used, installed, "the swap installed a new holder")

	require.Error(t, iter.Close())
	require.Equal(t, int32(1), used.stats.consecutiveFailures.Load(),
		"the outcome lands on the holder the leg ran on")
	require.Equal(t, int32(0), installed.stats.consecutiveFailures.Load())
}

// ─────────────────────────────────────────────
// The gating sequence, shared with Scan
// ─────────────────────────────────────────────

// callSink records the ordered gating callbacks of one read.
type callSink struct {
	mu    sync.Mutex
	calls []string
}

func (s *callSink) record(call string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, call)
}

func (s *callSink) recorded() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.calls...)
}

// gatingPolicy and gatingStrategy record their calls into a shared sink and
// let a test cancel the caller from inside a callback.
type gatingPolicy struct {
	sink   *callSink
	allow  bool
	onCall func()
}

func (p *gatingPolicy) ShouldFailover(c ClusterID, _ error) bool {
	p.sink.record("should_failover:" + string(c))
	if p.onCall != nil {
		p.onCall()
	}

	return p.allow
}
func (p *gatingPolicy) RecordFailure(c ClusterID) { p.sink.record("record_failure:" + string(c)) }
func (p *gatingPolicy) RecordSuccess(c ClusterID) { p.sink.record("record_success:" + string(c)) }

type gatingStrategy struct {
	sink *callSink
}

func (s *gatingStrategy) Select(_ context.Context) ClusterID { return ClusterA }
func (s *gatingStrategy) OnSuccess(c ClusterID)              { s.sink.record("on_success:" + string(c)) }
func (s *gatingStrategy) OnFailure(c ClusterID, _ error) (ClusterID, bool) {
	s.sink.record("on_failure:" + string(c))

	return ClusterB, true
}

func TestFailoverGating_IteratorFollowsTheScanSequence(t *testing.T) {
	tests := []struct {
		name       string
		allow      bool
		drainB     bool
		drainA     bool
		cancelInCB bool
		want       []string
	}{
		{
			name:  "failover allowed",
			allow: true,
			want: []string{
				"read_total:A", "read_error:A", "record_failure:A",
				"should_failover:A", "on_failure:A",
				"failover:A->B", "read_total:B", "read_error:B", "record_failure:B",
			},
		},
		{
			name:  "a refusing policy",
			allow: false,
			want: []string{
				"read_total:A", "read_error:A", "record_failure:A",
				"should_failover:A",
			},
		},
		{
			name:   "a draining alternative",
			allow:  true,
			drainB: true,
			want: []string{
				"read_total:A", "read_error:A", "record_failure:A",
				"should_failover:A", "on_failure:A",
			},
		},
		{
			name:   "both clusters draining",
			allow:  true,
			drainA: true,
			drainB: true,
			want: []string{
				"read_total:A", "read_error:A", "record_failure:A",
				"should_failover:A", "on_failure:A",
				"failover:A->B", "read_total:B", "read_error:B", "record_failure:B",
			},
		},
		{
			name:       "the caller ends inside a policy callback",
			allow:      true,
			cancelInCB: true,
			want: []string{
				"read_total:A", "read_error:A", "record_failure:A",
				"should_failover:A", "on_failure:A",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, read := range []string{"scan", "iterator"} {
				t.Run(read, func(t *testing.T) {
					sink := &callSink{}
					metrics := newLegMetrics()
					metrics.sink = sink
					ctx, cancel := context.WithCancel(t.Context())
					t.Cleanup(cancel)

					policy := &gatingPolicy{sink: sink, allow: tt.allow}
					if tt.cancelInCB {
						policy.onCall = cancel
					}
					client, err := NewCQLClient(
						newFirstPageSession(iterHangs), newFirstPageSession(iterHangs),
						WithClusterReadTimeout(20*time.Millisecond),
						WithReadStrategy(&gatingStrategy{sink: sink}),
						WithFailoverPolicy(policy),
						WithMetrics(metrics),
					)
					require.NoError(t, err)
					t.Cleanup(client.Close)
					client.drainA.Store(tt.drainA)
					client.drainB.Store(tt.drainB)

					q := client.Query("SELECT v FROM t WHERE k = ?", 1)
					if read == "scan" {
						var v string
						_ = q.ScanContext(ctx, &v)
					} else {
						_ = q.IterContext(ctx).Close()
					}

					require.Equal(t, tt.want, sink.recorded(),
						"the iterator's first page follows the gating a Scan follows")
				})
			}
		})
	}
}
