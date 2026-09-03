package helix

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/types"
)

// safeCQLWrite calls write and recovers from panics, converting them to
// errors so a panic in one dual-write leg cannot unwind past the calling
// goroutine before its sibling is joined and the error-aggregation /
// replay-enqueue path below runs. Mirrors policy/write_strategy.go's
// safeWrite: the captured stack is included in the returned error for
// post-mortem debugging.
func safeCQLWrite(ctx context.Context, write func(context.Context) error, cluster string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			err = fmt.Errorf("helix: panic in cluster %s write: %v\n%s", cluster, r, buf[:n])
		}
	}()

	return write(ctx)
}

// writeContext holds information about a write operation for replay purposes.
type writeContext struct {
	statement    string
	args         []any
	timestamp    int64
	priority     PriorityLevel
	isBatch      bool
	batchType    BatchType
	batchEntries []batchEntry // Internal format, converted lazily for replay
	strict       bool         // if true: no replay, returns PartialWriteError on partial failure
	// nonIdempotent marks a statement that must not be applied twice; it
	// implies strict and is carried to a mirror destination.
	nonIdempotent bool
	// consistency and serialConsistency are the levels set on the query
	// or batch, nil for the session default; replay applies the same ones.
	consistency       *Consistency
	serialConsistency *Consistency
}

// resolveWriteTimestamp returns the client-side timestamp for a write: the
// per-statement override when set, otherwise the provider's value. Zero is
// rejected: the drivers treat it as "use the current time", which would let
// a replayed write outrank data written after the original.
func resolveWriteTimestamp(override *int64, provider TimestampProvider) (int64, error) {
	var ts int64
	if override != nil {
		ts = *override
	} else {
		ts = provider()
	}
	if ts == 0 {
		return 0, types.ErrInvalidTimestamp
	}

	return ts, nil
}

// writeLegErrKind classifies the result of one cluster leg of a dual write.
type writeLegErrKind uint8

const (
	// legOK is an acknowledged write.
	legOK writeLegErrKind = iota
	// legAsync is [types.ErrWriteAsync]: the write is in flight on a degraded cluster.
	legAsync
	// legDropped is [types.ErrWriteDropped]: the write was never attempted (concurrency limit).
	legDropped
	// legDraining is [types.ErrClusterDraining]: the leg was skipped because the cluster is draining.
	legDraining
	// legSkipped is [types.ErrClusterDegraded]: a strict write skipped a degraded cluster.
	legSkipped
	// legCanceled is a failure observed after the caller's context was
	// cancelled or expired: the caller's doing, not the cluster's. The leg
	// is still unacknowledged and is replayed like a failed leg, but it is
	// neither counted as a write error nor as a health signal.
	legCanceled
	// legFailed is any other error: the cluster rejected or failed the write.
	legFailed
)

// classifyWriteLeg assigns the kind of one leg's result for a write issued
// with ctx. Classification is by provenance: once ctx is done, a failure is
// attributed to the caller rather than to the cluster.
func classifyWriteLeg(ctx context.Context, err error) writeLegErrKind {
	switch {
	case err == nil:
		return legOK
	case errors.Is(err, types.ErrWriteAsync):
		return legAsync
	case errors.Is(err, types.ErrWriteDropped):
		return legDropped
	case errors.Is(err, types.ErrClusterDraining):
		return legDraining
	case errors.Is(err, types.ErrClusterDegraded):
		return legSkipped
	case ctx.Err() != nil:
		return legCanceled
	default:
		return legFailed
	}
}

// failed reports whether the leg's result is a failure the caller must see
// when the other leg gave no acknowledgement either.
func (k writeLegErrKind) failed() bool {
	return k == legFailed || k == legCanceled
}

// recordWriteLegMetrics emits the per-leg write metrics for one cluster.
// startNano is 0 when the leg never started (skipped or draining), in which
// case no duration is observed.
func (c *CQLClient) recordWriteLegMetrics(cluster ClusterID, leg writeLegErrKind, startNano, nowNano int64) {
	c.config.Metrics.IncWriteTotal(cluster)
	if startNano > 0 {
		c.config.Metrics.ObserveWriteDuration(cluster, float64(nowNano-startNano)/float64(time.Second))
	}
	switch leg {
	case legOK, legCanceled:
	case legAsync:
		c.config.Metrics.IncWriteAsync(cluster)
	case legDropped:
		c.config.Metrics.IncWriteDropped(cluster)
	case legDraining, legSkipped:
		if sm, ok := c.config.Metrics.(types.StrictMetrics); ok {
			sm.IncWriteSkipped(cluster)
		}
	case legFailed:
		c.config.Metrics.IncWriteError(cluster)
	}
}

// recordWriteOutcome feeds one leg's outcome to the auto-refresh detector.
// A failure observed after the caller's context ended is the caller's
// doing, not the cluster's, and is not recorded.
func (c *CQLClient) recordWriteOutcome(ctx context.Context, cluster ClusterID, err error) {
	c.recordWriteOutcomeAt(ctx, cluster, err, 0)
}

// recordWriteOutcomeAt is recordWriteOutcome with a caller-captured clock.
func (c *CQLClient) recordWriteOutcomeAt(ctx context.Context, cluster ClusterID, err error, nowNano int64) {
	if err != nil && ctx.Err() != nil {
		return
	}
	c.recordOpOutcomeAt(cluster, err, nowNano)
}

// executeWriteWithReplay performs a write operation with optional dual-write and replay support.
//
// In single-cluster mode, the write is executed directly on sessionA.
// In dual-cluster mode, writes are executed concurrently on both clusters with replay
// for partial failures.
//
// A draining cluster's leg is skipped: its write closure returns
// [types.ErrClusterDraining] without contacting the session, the write
// strategy sees that result like any other skipped leg, and the write is
// enqueued for replay to that cluster. If both clusters are draining, the
// write fails with ErrBothClustersDraining.
func (c *CQLClient) executeWriteWithReplay(
	ctx context.Context,
	wc writeContext,
	writeFunc func(context.Context, cql.Session) error,
) error {
	if c.closed.Load() {
		return types.ErrSessionClosed
	}

	// Single-cluster mode: direct execution, no dual-write logic.
	// recordOpOutcome must run here so the auto-refresh detector sees
	// cluster-A outcomes — no other code path observes err for stats.
	if c.IsSingleCluster() {
		err := writeFunc(ctx, c.loadSessionA())
		c.recordWriteOutcome(ctx, ClusterA, err)

		return err
	}

	drainA, drainB := c.getDrainStates()

	// If both clusters are draining, fail immediately
	if drainA && drainB {
		if wc.strict {
			if sm, ok := c.config.Metrics.(types.StrictMetrics); ok {
				sm.IncWriteSkipped(ClusterA)
				sm.IncWriteSkipped(ClusterB)
			}
			return &types.DualClusterError{
				ErrorA: types.ErrClusterDraining,
				ErrorB: types.ErrClusterDraining,
			}
		}

		return types.ErrBothClustersDraining
	}

	return c.executeDualWrite(ctx, wc, writeFunc, drainA, drainB)
}

// writeLegs builds the per-cluster write closures shared by the replaying
// and strict dual-write paths.
//
// Session refs are resolved at call time inside the closure body so a
// concurrent SwapSession or RefreshSession is observed by the next
// dispatch. In-flight closures that have already loaded their session
// continue against that captured ref; this preserves "the write was
// dispatched to cluster X" semantics for fire-and-forget strategies.
//
// A draining cluster's closure returns [types.ErrClusterDraining] before
// touching the session or the start time, so the leg is neither timed nor
// counted as an error; the callers classify it as a skipped leg.
func (c *CQLClient) writeLegs(
	writeFunc func(context.Context, cql.Session) error,
	drainA, drainB bool,
	startA, startB *atomic.Int64,
) (writeA, writeB func(context.Context) error) {
	return c.writeLeg(writeFunc, drainA, startA, c.loadSessionA),
		c.writeLeg(writeFunc, drainB, startB, c.loadSessionB)
}

// writeLeg builds one cluster's leg: a draining cluster is skipped, the
// start time is stamped, and the write runs under legContext.
func (c *CQLClient) writeLeg(
	writeFunc func(context.Context, cql.Session) error,
	draining bool,
	start *atomic.Int64,
	loadSession func() cql.Session,
) func(context.Context) error {
	return func(ctx context.Context) error {
		if draining {
			return types.ErrClusterDraining
		}
		start.Store(time.Now().UnixNano())
		ctx, cancel := c.legContext(ctx)
		defer cancel()

		return writeFunc(ctx, loadSession())
	}
}

// legContext bounds one write leg by [ClientConfig.ClusterWriteTimeout].
// The leg's own deadline expiring leaves the parent context live, so
// classifyWriteLeg attributes the failure to the cluster.
func (c *CQLClient) legContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.config.ClusterWriteTimeout <= 0 {
		return ctx, noopCancel
	}

	return context.WithTimeout(ctx, c.config.ClusterWriteTimeout)
}

// noopCancel is the cancel function of a leg that runs on the caller's context.
func noopCancel() {}

// executeDualWrite performs the normal dual-cluster write.
//
// When no [WriteStrategy] is configured, both cluster legs run through
// safeCQLWrite: a panic on either leg is recovered and converted into that
// cluster's error, exactly like the panic-to-error conversion the built-in
// policy strategies already apply (see policy/write_strategy.go's
// safeWrite). This keeps the two legs symmetric — the sibling write is
// always joined via wg.Wait and participates in the same metrics/replay
// aggregation below regardless of whether a leg errored or panicked.
func (c *CQLClient) executeDualWrite(
	ctx context.Context,
	wc writeContext,
	writeFunc func(context.Context, cql.Session) error,
	drainA, drainB bool,
) error {
	if wc.strict {
		return c.executeStrictDualWrite(ctx, writeFunc, drainA, drainB)
	}
	// Hold the client open until every leg has been enqueued or handed to
	// a completion callback, so Close cannot stop the replay worker in
	// between.
	if !c.deferred.add() {
		return types.ErrSessionClosed
	}
	defer c.deferred.done()

	// Dual-cluster mode: concurrent writes with replay support
	// Note: We capture start times outside the write functions to avoid data races
	// when WriteStrategy uses fire-and-forget (background goroutines).
	var startA, startB atomic.Int64
	writeA, writeB := c.writeLegs(writeFunc, drainA, drainB, &startA, &startB)

	var errA, errB error

	if c.config.WriteStrategy != nil {
		errA, errB = c.config.WriteStrategy.Execute(ctx, writeA, writeB)
	} else {
		// Default: concurrent dual write. Only writeB is dispatched to a
		// spawned goroutine; writeA runs inline on the calling goroutine,
		// which blocks on wg.Wait() immediately afterward, so A and B still
		// execute concurrently — this halves the per-write goroutine-spawn
		// count versus spawning both. Both legs go through safeCQLWrite so a
		// panic in either becomes that cluster's error instead of unwinding
		// past this function (which would skip wg.Wait, metrics, and the
		// aggregation/replay path below) or crashing the process (an
		// unrecovered panic in the spawned goroutine is fatal regardless of
		// which goroutine raised it).
		var wg sync.WaitGroup

		wg.Go(func() {
			errB = safeCQLWrite(ctx, writeB, "B")
		})

		errA = safeCQLWrite(ctx, writeA, "A")

		wg.Wait()
	}

	// Classify results: distinguish operational sentinel states from real errors.
	// ErrWriteAsync     — write is in flight via fire-and-forget (not a cluster error).
	// ErrWriteDropped   — write was not attempted due to concurrency limit (not a cluster error).
	// ErrClusterDraining — leg skipped because the cluster is draining (not a cluster error).
	// A failure after the caller's context ended is the caller's, not the cluster's.
	legA := classifyWriteLeg(ctx, errA)
	legB := classifyWriteLeg(ctx, errB)

	// Record metrics for both clusters.
	// Use atomic loads to safely read start times that may have been set by fire-and-forget goroutines.
	now := time.Now()
	nowNano := now.UnixNano()

	c.recordWriteLegMetrics(ClusterA, legA, startA.Load(), nowNano)
	c.recordWriteLegMetrics(ClusterB, legB, startB.Load(), nowNano)

	// Auto-refresh stat tracking — invoked PER cluster so partial-success
	// (A=ok, B=err) correctly advances A's lastSuccess while accumulating
	// failures on B. recordOpOutcomeAt internally skips ErrWriteAsync /
	// ErrWriteDropped / ErrNotFound so operational states don't poison
	// the failure counters, and a caller-cancelled leg is skipped here.
	// Reuse the already-captured nowNano so the helper does not re-sample
	// the clock.
	c.recordWriteOutcomeAt(ctx, ClusterA, errA, nowNano)
	c.recordWriteOutcomeAt(ctx, ClusterB, errB, nowNano)

	// Both succeeded definitively.
	if errA == nil && errB == nil {
		return nil
	}

	// Both clusters had real (non-operational) failures — hard error, no replay.
	if legA.failed() && legB.failed() {
		return &types.DualClusterError{ErrorA: errA, ErrorB: errB}
	}

	// At least one cluster had a non-nil result (error, async, dropped, or draining).
	// Enqueue replay for each affected cluster to ensure eventual consistency.
	//
	// ErrWriteAsync:      write is in flight; replayed only if the background leg reports
	//                     failure when the strategy defers its result, otherwise enqueued now
	//                     as a safety net (idempotent for Cassandra because both attempts use
	//                     the same client-generated timestamp).
	// ErrWriteDropped:    write was never attempted; replay is required for reconciliation.
	// ErrClusterDraining: leg was skipped; replay delivers the write once the drain lifts.
	// Real error:         write definitively failed; replay is required.
	replayErrA := c.replayLeg(ctx, wc, ClusterA, errA, legA)
	replayErrB := c.replayLeg(ctx, wc, ClusterB, errB, legB)

	// Partial success is success from the caller's perspective: one cluster
	// holds the write and replay carries it to the other.
	if legA == legOK || legB == legOK {
		return nil
	}

	// No cluster acknowledged the write. It exists, at best, in the replay
	// queue; the caller decides through AckMode whether that counts.
	replayErr := replayErrA
	if replayErr == nil {
		replayErr = replayErrB
	}
	if c.config.AckMode == AckOnReplayAdmission && replayErr == nil {
		return nil
	}

	return &types.NoSynchronousAckError{ResultA: errA, ResultB: errB, Replay: replayErr}
}

// replayLeg arranges replay for one leg. A leg whose strategy reports its
// result later (see [DeferredWriteResult]) is replayed only if that result
// is a failure; every other unacknowledged leg is enqueued now.
func (c *CQLClient) replayLeg(
	ctx context.Context,
	wc writeContext,
	cluster ClusterID,
	err error,
	kind writeLegErrKind,
) error {
	if kind == legOK {
		return nil
	}
	var deferred DeferredWriteResult
	if kind != legAsync || !errors.As(err, &deferred) {
		return c.enqueueReplayPayload(ctx, c.replayPayload(wc, cluster), err, kind)
	}

	// Snapshot the write now: the caller may reuse its buffers as soon as
	// this call returns, while the leg completes later.
	// The enqueue then runs on a context that keeps the caller's values
	// but not its deadline, as an immediate enqueue does.
	payload := c.replayPayload(wc, cluster)
	bg := context.WithoutCancel(ctx)
	if !c.deferred.add() {
		// Close has begun and will not wait for this leg, so enqueue now:
		// a failure reported after the worker stopped would otherwise be lost.
		return c.enqueueReplayPayload(bg, payload, err, kind)
	}
	// A leg that has already completed runs the callback inline, on this
	// goroutine, before OnComplete returns; its admission result is then a
	// synchronous outcome the caller must see.
	// The admission is published before the flag so a later completion
	// on another goroutine never races with the read below.
	var admission struct {
		err  error
		done atomic.Bool
	}
	deferred.OnComplete(func(legErr error) {
		var dropErr error
		if legErr != nil {
			dropErr = c.admitReplayPayload(bg, payload, legErr, classifyWriteLeg(bg, legErr))
		}
		admission.err = dropErr
		admission.done.Store(true)
		// Report before releasing the leg, so Close returns only after the
		// drop handler ran and the event was admitted.
		defer c.deferred.done()
		if dropErr != nil {
			c.emitReplayDropped(payload.TargetCluster, payload, dropErr)
		}
	})
	if admission.done.Load() {
		return admission.err
	}

	return nil
}

// deferredLegs counts the dual writes in progress and the background legs
// whose result a strategy reports later (see [DeferredWriteResult]), so
// Close can wait for every replay to be enqueued before it stops the
// replay worker.
type deferredLegs struct {
	mu      sync.Mutex
	active  int
	closing bool
	idle    chan struct{}
}

// add registers one holder. It reports false once wait has finished:
// while a holder is still registered, wait is blocked and a new holder
// (a leg the holder hands to a completion callback) may still join.
func (d *deferredLegs) add() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing && d.active == 0 {
		return false
	}
	d.active++

	return true
}

// done unregisters one holder and releases wait once the last one finished.
func (d *deferredLegs) done() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.active--
	if d.closing && d.active == 0 {
		close(d.idle)
	}
}

// wait blocks until every registered holder finished, then refuses new ones.
func (d *deferredLegs) wait() {
	d.mu.Lock()
	d.closing = true
	if d.active == 0 {
		d.mu.Unlock()

		return
	}
	d.idle = make(chan struct{})
	d.mu.Unlock()
	<-d.idle
}

// replayPayload builds the replay payload for one leg of wc. Byte-slice
// args are copied because the caller may reuse its buffers as soon as the
// write returns, while the payload is replayed later.
func (c *CQLClient) replayPayload(wc writeContext, cluster ClusterID) types.ReplayPayload {
	return types.ReplayPayload{
		TargetCluster:     cluster,
		Query:             wc.statement,
		Args:              cloneArgs(wc.args),
		IsBatch:           wc.isBatch,
		BatchType:         wc.batchType,
		BatchStatements:   cloneBatchEntries(wc.batchEntries),
		Timestamp:         wc.timestamp,
		Priority:          wc.priority,
		Consistency:       cloneConsistency(wc.consistency),
		SerialConsistency: cloneConsistency(wc.serialConsistency),
		NonIdempotent:     wc.nonIdempotent,
	}
}

// cloneConsistency copies an optional consistency level so a payload never
// aliases the query it was built from.
func cloneConsistency(c *Consistency) *Consistency {
	if c == nil {
		return nil
	}
	v := *c

	return &v
}

// enqueueReplayPayload enqueues one replay payload for a leg whose result
// was err, classified as kind. kind selects the log message: async means the
// write is in flight; dropped means it was never attempted because the
// concurrency limit was full; draining means the leg was skipped.
//
// It returns nil when the payload was enqueued, and the enqueue error, or
// [types.ErrNoReplayer] when the client has no replayer, otherwise. Either
// failure is counted as a dropped replay and reported through the
// replay-dropped callback and event.
func (c *CQLClient) enqueueReplayPayload(
	ctx context.Context,
	payload types.ReplayPayload,
	err error,
	kind writeLegErrKind,
) error {
	dropErr := c.admitReplayPayload(ctx, payload, err, kind)
	if dropErr != nil {
		c.emitReplayDropped(payload.TargetCluster, payload, dropErr)
	}

	return dropErr
}

// admitReplayPayload is enqueueReplayPayload without the drop callback and
// event: it enqueues, counts, and logs, and returns the reason a payload was
// not admitted so the caller can report it once it is safe to do so.
func (c *CQLClient) admitReplayPayload(
	ctx context.Context,
	payload types.ReplayPayload,
	err error,
	kind writeLegErrKind,
) error {
	cluster := payload.TargetCluster
	if c.config.Replayer == nil {
		c.config.Metrics.IncReplayDropped(cluster)
		c.config.Logger.Error("write not acknowledged by cluster and no replayer is configured; it will not be reconciled",
			"cluster", c.clusterName(cluster),
			"writeError", err.Error(),
		)

		return types.ErrNoReplayer
	}

	// Use context.WithoutCancel so the enqueue succeeds even if the request context is cancelled.
	enqueueErr := c.config.Replayer.Enqueue(context.WithoutCancel(ctx), payload)
	if enqueueErr == nil {
		c.config.Metrics.IncReplayEnqueued(cluster)
		switch kind {
		case legDropped:
			c.config.Logger.Info("write dropped (concurrency limit reached) on degraded cluster, enqueued for replay",
				"cluster", c.clusterName(cluster),
			)
		case legAsync:
			c.config.Logger.Info("write dispatched asynchronously to degraded cluster, enqueued for replay",
				"cluster", c.clusterName(cluster),
			)
		case legDraining:
			c.config.Logger.Info("write skipped on draining cluster, enqueued for replay",
				"cluster", c.clusterName(cluster),
			)
		case legOK, legSkipped, legCanceled, legFailed:
			c.config.Logger.Warn("write failed on cluster, enqueued for replay",
				"cluster", c.clusterName(cluster),
				"error", err.Error(),
			)
		}

		return nil
	}

	c.config.Metrics.IncReplayDropped(cluster)
	c.config.Logger.Error("failed to enqueue write for replay",
		"cluster", c.clusterName(cluster),
		"writeError", err.Error(),
		"enqueueError", enqueueErr.Error(),
	)

	return enqueueErr
}

// executeStrictDualWrite performs dual-cluster writes with Strict() semantics:
// no replay enqueue, no fire-and-forget. Returns [*types.PartialWriteError] on
// partial failure or [*types.DualClusterError] when both clusters fail.
//
// A nil WriteStrategy uses the same inline concurrent write as the default
// non-strict path, including the same safeCQLWrite panic-to-error
// conversion on both legs described on [CQLClient.executeDualWrite]. A
// non-nil WriteStrategy that does not implement [StrictWriter] surfaces as
// [types.ErrStrictUnsupported].
func (c *CQLClient) executeStrictDualWrite(
	ctx context.Context,
	writeFunc func(context.Context, cql.Session) error,
	drainA, drainB bool,
) error {
	var startA, startB atomic.Int64
	writeA, writeB := c.writeLegs(writeFunc, drainA, drainB, &startA, &startB)

	var errA, errB error

	if sw, ok := c.config.WriteStrategy.(StrictWriter); ok {
		errA, errB = sw.ExecuteStrict(ctx, writeA, writeB)
	} else if c.config.WriteStrategy != nil {
		return types.ErrStrictUnsupported
	} else {
		// See executeDualWrite's default branch: only writeB is spawned;
		// writeA runs inline on the calling goroutine, which still blocks on
		// wg.Wait() right after, preserving A/B concurrency with one fewer
		// goroutine spawn per write. Both legs go through safeCQLWrite so a
		// panic in either becomes that cluster's error (joined via wg.Wait,
		// then classified below) instead of unwinding past this function or
		// crashing the process.
		var wg sync.WaitGroup
		wg.Go(func() { errB = safeCQLWrite(ctx, writeB, "B") })
		errA = safeCQLWrite(ctx, writeA, "A")
		wg.Wait()
	}

	now := time.Now()
	nowNano := now.UnixNano()

	c.recordWriteLegMetrics(ClusterA, classifyWriteLeg(ctx, errA), startA.Load(), nowNano)
	c.recordWriteLegMetrics(ClusterB, classifyWriteLeg(ctx, errB), startB.Load(), nowNano)

	c.recordWriteOutcomeAt(ctx, ClusterA, errA, nowNano)
	c.recordWriteOutcomeAt(ctx, ClusterB, errB, nowNano)

	if errA == nil && errB == nil {
		return nil
	}
	if errA != nil && errB != nil {
		return &types.DualClusterError{ErrorA: errA, ErrorB: errB}
	}
	if errA != nil {
		return &types.PartialWriteError{
			Acknowledged:   ClusterB,
			Unacknowledged: ClusterA,
			Cause:          errA,
		}
	}

	return &types.PartialWriteError{
		Acknowledged:   ClusterA,
		Unacknowledged: ClusterB,
		Cause:          errB,
	}
}
