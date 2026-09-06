package helix

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/arloliu/helix/types"
)

// firstPageCtx bounds the page an iterator fetches synchronously inside
// IterContext by [ClientConfig.ClusterReadTimeout] without bounding the
// pages the caller drains afterwards.
//
// Both drivers keep the context they were handed for the whole iteration —
// the first page, every auto-paged page after it, and the prefetch
// goroutine — so a [context.WithTimeout] child would cut the caller's own
// draining short. firstPageCtx instead carries the leg deadline itself: it
// is armed while the first page is in flight and disarmed the moment the
// driver answers, after which it follows the caller's context alone.
//
// Deadline and Value delegate to the caller, so a driver that consults
// either sees exactly what it sees today; Done closes when the leg timer
// expires while armed, when the caller's context ends, or when the
// iterator is finished with; and Err reports the one latched terminal
// error, or the caller's while none is latched.
type firstPageCtx struct {
	parent     context.Context
	done       chan struct{}
	stopParent func() bool // context.AfterFunc's stop

	mu       sync.Mutex
	timer    *time.Timer
	armed    bool
	expired  bool  // the leg timer latched, with the caller still live
	finalErr error // latched once; nil while the context follows the caller
}

// newFirstPageCtx returns an armed leg context for parent that ends in d
// unless it is disarmed first.
//
// Parameters:
//   - parent: The caller's context; Deadline and Value delegate to it, and
//     its own end latches through to this context
//   - d: The leg deadline, [ClientConfig.ClusterReadTimeout]
//
// Returns:
//   - *firstPageCtx: An armed context; the caller must disarm it and, on
//     every path reaching Close, finish it
func newFirstPageCtx(parent context.Context, d time.Duration) *firstPageCtx {
	fp := &firstPageCtx{
		parent: parent,
		done:   make(chan struct{}),
		armed:  true,
	}

	// The registrations run under mu so a timer that fires before the
	// constructor returns cannot read fp.timer while it is being written;
	// either callback simply blocks until the context is complete.
	fp.mu.Lock()
	defer fp.mu.Unlock()
	fp.timer = time.AfterFunc(d, fp.legExpired)
	fp.stopParent = context.AfterFunc(parent, fp.callerEnded)

	return fp
}

// Deadline returns the caller's deadline and never advertises the leg
// deadline: a driver that arms its own request timer from the deadline it
// was given keeps behaving exactly as it does without a leg context.
func (fp *firstPageCtx) Deadline() (time.Time, bool) {
	return fp.parent.Deadline()
}

// Done returns the same channel for the lifetime of the context.
func (fp *firstPageCtx) Done() <-chan struct{} {
	return fp.done
}

// Err returns the latched terminal error when there is one, and otherwise
// the caller's own error, so a driver waking on Done sees a context error
// with the provenance Helix intends.
//
// Both reads happen under one critical section.
// Releasing the lock between them would let a latch land in the gap: a
// reader that found no error yet would go on to report the caller's error
// while the next reader reports the leg's, and [context.Context] requires
// Err to keep the one error it first returned.
func (fp *firstPageCtx) Err() error {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	if fp.finalErr != nil {
		return fp.finalErr
	}

	return fp.parent.Err()
}

// Value delegates to the caller's context.
func (fp *firstPageCtx) Value(key any) any {
	return fp.parent.Value(key)
}

// latch records the one terminal error of this context and closes Done.
//
// It is the only writer of finalErr, expired and done, and it decides
// under mu, so the single caller that finds no error latched yet is the
// one that closes the channel.
// A caller whose own context already ended keeps that error rather than
// being handed a leg timeout, which is what stops a cancellation from
// being reported as a cluster failure.
//
// Parameters:
//   - err: The terminal error to latch
//   - fromTimer: True only for the leg timer, whose latch is ignored once
//     the context is disarmed
func (fp *firstPageCtx) latch(err error, fromTimer bool) {
	fp.mu.Lock()
	if fp.finalErr != nil || (fromTimer && !fp.armed) {
		fp.mu.Unlock()
		return
	}
	if perr := fp.parent.Err(); perr != nil { // the caller got there first
		err, fromTimer = perr, false
	}
	fp.finalErr, fp.expired, fp.armed = err, fromTimer, false
	fp.timer.Stop() // a terminal context must not be held alive by a pending callback
	fp.mu.Unlock()

	close(fp.done)
}

// legExpired is the leg timer's callback.
func (fp *firstPageCtx) legExpired() {
	fp.latch(context.DeadlineExceeded, true)
}

// callerEnded is the parent registration's callback.
func (fp *firstPageCtx) callerEnded() {
	fp.latch(fp.parent.Err(), false)
}

// disarm stops the leg timer and reports whether the leg beat it.
//
// Returns:
//   - bool: True when the leg did not expire, false when the timer latched
//     first and the driver's answer must be discarded
func (fp *firstPageCtx) disarm() bool {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	fp.armed = false
	fp.timer.Stop()

	return !fp.expired
}

// finish is the terminal cleanup for an iterator that is done with this
// context, run from Close or Scanner.Err.
//
// It is idempotent: a second call finds the error already latched and
// returns without touching Done, and the parent registration is already
// stopped, so the caller needs no guard of its own.
//
// It cancels before it detaches, because the driver may still hold this
// context for a prefetch it started: with a caller-supplied deadline the
// driver arms no request timer of its own, so detaching alone would leave
// that fetch with no cancellation source.
// Detaching then releases the registration this context holds on the
// caller's.
func (fp *firstPageCtx) finish() {
	fp.latch(context.Canceled, false)
	fp.stopParent()
}

// iterFirstPage fetches an iterator's first page as a bounded read leg on
// the resolved cluster, and re-fetches it on the alternative when the leg
// expires and the failover gating allows it.
//
// Only [CQLClient.legTimeoutActive] clients reach it; the pages the caller
// drains afterwards run on the caller's context and are reported at Close,
// exactly as they are without a leg deadline.
func (q *cqlQuery) iterFirstPage(ctx context.Context, rt readTarget) Iter {
	iter, kind, err := q.firstPageLeg(ctx, rt.cluster, rt.snap.active)
	if kind == readOK {
		return iter
	}

	// A caller-context error is the caller's doing, and a paging cursor is
	// only meaningful on the cluster that issued it: neither may move.
	if kind != readClusterErr || q.pageState != nil {
		return &errorIter{err: err}
	}

	fallback, ok := q.client.failoverTarget(ctx, rt, err)
	if !ok {
		return &errorIter{err: err}
	}
	q.client.announceFailover(rt.cluster, fallback, err)

	altIter, altKind, altErr := q.firstPageLeg(ctx, fallback, rt.snap.active)
	if altKind == readOK {
		return altIter
	}
	// An alternative leg the caller's own context ended reports nothing,
	// mirroring tryFallbackCluster's non-health-signal branch.
	if altKind == readCtxErr {
		return &errorIter{err: altErr}
	}

	return &errorIter{err: dualReadError(rt.cluster, err, altErr)}
}

// firstPageLeg runs one armed first-page attempt against cluster.
//
// The attempt is counted for the cluster whether it wins or loses, as
// attemptRead counts one.
// A leg the driver answers in time hands its iterator over unchanged,
// with the leg context attached so Close can finish it.
// A leg the timer beats disposes of whatever the driver returned
// afterwards, releases the leg context, and reports the failure to the
// observation hub unless the caller's own context ended first.
//
// Returns:
//   - Iter: The iterator to hand the caller, or nil when the leg lost
//   - readErrKind: readOK, readCtxErr for a caller that gave up, or
//     readClusterErr for a leg the deadline ended
//   - error: nil on readOK, the caller's error, or [types.ErrClusterTimeout]
func (q *cqlQuery) firstPageLeg(ctx context.Context, cluster ClusterID, overrideActive bool) (Iter, readErrKind, error) {
	holder := q.client.holderFor(cluster)
	query := holder.s.Query(q.statement, q.values...)
	query = q.applyConfig(query)
	fpCtx := newFirstPageCtx(ctx, q.client.config.ClusterReadTimeout)

	start := time.Now()
	driverIter := query.IterContext(fpCtx)
	elapsed := time.Since(start).Seconds()
	won := fpCtx.disarm()

	q.client.config.Metrics.IncReadTotal(cluster)
	q.client.config.Metrics.ObserveReadDuration(cluster, elapsed)

	if won {
		return &cqlIter{
			iter:           driverIter,
			client:         q.client,
			cluster:        cluster,
			holder:         holder,
			ctx:            ctx,
			overrideActive: overrideActive,
			firstPage:      fpCtx,
		}, readOK, nil
	}

	// The driver may still answer after the leg expired.
	// Close what it handed back so the framer is released, and drop that
	// close error: the leg's outcome is already known and this iterator is
	// never scanned, let alone handed to the caller.
	_ = driverIter.Close()
	fpCtx.finish()

	// A failure observed after the caller's context ended belongs to the
	// caller, the rule clusterTimeoutIfExpired and classifyReadErr share.
	// It leaves no other trace, so the caller-expired counter is the only
	// signal this attempt produces, as it is in attemptRead.
	if ctx.Err() != nil {
		q.client.health.readCallerExpired(cluster)

		return nil, readCtxErr, ctx.Err()
	}

	// clusterTimeoutIfExpired itself does not fit here: it wraps an error
	// the driver returned, and a driver handing back an iterator returns
	// none.
	// The composed error has the shape it would produce.
	err := fmt.Errorf("%w: %w", types.ErrClusterTimeout, context.DeadlineExceeded)
	q.client.health.readFailed(holder, cluster, readClusterErr, err)

	return nil, readClusterErr, err
}
