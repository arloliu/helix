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
}

// executeWriteWithReplay performs a write operation with optional dual-write and replay support.
//
// In single-cluster mode, the write is executed directly on sessionA.
// In dual-cluster mode, writes are executed concurrently on both clusters with replay
// for partial failures.
//
// If a cluster is in drain mode, writes to that cluster are skipped and enqueued
// for replay instead. If both clusters are draining, the write fails with ErrBothClustersDraining.
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
		c.recordOpOutcome(ClusterA, err)

		return err
	}

	// Check drain mode
	drainA := c.drainA.Load()
	drainB := c.drainB.Load()

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

	// If only one cluster is draining, write to the healthy one and enqueue replay
	if drainA || drainB {
		return c.executeWriteWithDrain(ctx, wc, writeFunc, drainA, drainB)
	}

	// Normal dual-cluster mode: concurrent writes with replay support
	return c.executeDualWrite(ctx, wc, writeFunc)
}

// executeWriteWithDrain handles writes when one cluster is draining.
// Invariant: Exactly one of drainA or drainB is true when this function is called.
func (c *CQLClient) executeWriteWithDrain(
	ctx context.Context,
	wc writeContext,
	writeFunc func(context.Context, cql.Session) error,
	drainA, _ bool,
) error {
	var session cql.Session
	var healthyCluster, drainingCluster ClusterID

	if drainA {
		session = c.loadSessionB()
		healthyCluster = ClusterB
		drainingCluster = ClusterA
	} else {
		session = c.loadSessionA()
		healthyCluster = ClusterA
		drainingCluster = ClusterB
	}

	// Execute write on the healthy cluster
	start := time.Now()
	err := writeFunc(ctx, session)
	elapsed := time.Since(start).Seconds()

	if err != nil {
		c.config.Metrics.IncWriteTotal(healthyCluster)
		c.config.Metrics.IncWriteError(healthyCluster)
		c.config.Metrics.ObserveWriteDuration(healthyCluster, elapsed)
		c.recordOpOutcome(healthyCluster, err)

		if wc.strict {
			// Healthy cluster also failed: draining cluster was still skipped.
			if sm, ok := c.config.Metrics.(types.StrictMetrics); ok {
				sm.IncWriteSkipped(drainingCluster)
			}
			if drainA {
				return &types.DualClusterError{ErrorA: types.ErrClusterDraining, ErrorB: err}
			}

			return &types.DualClusterError{ErrorA: err, ErrorB: types.ErrClusterDraining}
		}

		return err
	}

	c.config.Metrics.IncWriteTotal(healthyCluster)
	c.config.Metrics.ObserveWriteDuration(healthyCluster, elapsed)
	c.recordOpOutcome(healthyCluster, nil)

	if wc.strict {
		// Strict: draining cluster is a skip, not a replay target.
		if sm, ok := c.config.Metrics.(types.StrictMetrics); ok {
			sm.IncWriteSkipped(drainingCluster)
		}
		return &types.PartialWriteError{
			Acknowledged:   healthyCluster,
			Unacknowledged: drainingCluster,
			Cause:          types.ErrClusterDraining,
		}
	}

	// Enqueue for replay to the draining cluster
	if c.config.Replayer != nil {
		payload := types.ReplayPayload{
			TargetCluster:   drainingCluster,
			Query:           wc.statement,
			Args:            cloneArgs(wc.args),
			IsBatch:         wc.isBatch,
			BatchType:       wc.batchType,
			BatchStatements: cloneBatchEntries(wc.batchEntries),
			Timestamp:       wc.timestamp,
			Priority:        wc.priority,
		}
		// Use context.WithoutCancel to ensure replay is enqueued even if the request context is cancelled
		if enqueueErr := c.config.Replayer.Enqueue(context.WithoutCancel(ctx), payload); enqueueErr == nil {
			c.config.Metrics.IncReplayEnqueued(drainingCluster)
		} else {
			c.config.Metrics.IncReplayDropped(drainingCluster)
			c.config.Logger.Error("failed to enqueue write for replay during drain",
				"cluster", c.clusterName(drainingCluster),
				"enqueueError", enqueueErr.Error(),
			)
			c.emitReplayDropped(drainingCluster, payload, enqueueErr)
		}
	}

	return nil
}

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
) error {
	if wc.strict {
		return c.executeStrictDualWrite(ctx, writeFunc)
	}
	// Dual-cluster mode: concurrent writes with replay support
	// Note: We capture start times outside the write functions to avoid data races
	// when WriteStrategy uses fire-and-forget (background goroutines).
	var startA, startB atomic.Int64

	// Session refs are resolved at call time inside the closure body so a
	// concurrent SwapSession or RefreshSession is observed by the next
	// dispatch. In-flight closures that have already loaded their session
	// continue against that captured ref; this preserves "the write was
	// dispatched to cluster X" semantics for fire-and-forget strategies.
	writeA := func(ctx context.Context) error {
		startA.Store(time.Now().UnixNano())
		return writeFunc(ctx, c.loadSessionA())
	}
	writeB := func(ctx context.Context) error {
		startB.Store(time.Now().UnixNano())
		return writeFunc(ctx, c.loadSessionB())
	}

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
	// ErrWriteAsync  — write is in flight via fire-and-forget (not a cluster error).
	// ErrWriteDropped — write was not attempted due to concurrency limit (not a cluster error).
	isAsyncA := errors.Is(errA, types.ErrWriteAsync)
	isDroppedA := errors.Is(errA, types.ErrWriteDropped)
	isAsyncB := errors.Is(errB, types.ErrWriteAsync)
	isDroppedB := errors.Is(errB, types.ErrWriteDropped)

	// Record metrics for both clusters.
	// Use atomic loads to safely read start times that may have been set by fire-and-forget goroutines.
	now := time.Now()
	nowNano := now.UnixNano()

	c.config.Metrics.IncWriteTotal(ClusterA)
	if startANano := startA.Load(); startANano > 0 {
		c.config.Metrics.ObserveWriteDuration(ClusterA, float64(nowNano-startANano)/float64(time.Second))
	}
	switch {
	case isAsyncA:
		c.config.Metrics.IncWriteAsync(ClusterA)
	case isDroppedA:
		c.config.Metrics.IncWriteDropped(ClusterA)
	case errA != nil:
		c.config.Metrics.IncWriteError(ClusterA)
	}

	c.config.Metrics.IncWriteTotal(ClusterB)
	if startBNano := startB.Load(); startBNano > 0 {
		c.config.Metrics.ObserveWriteDuration(ClusterB, float64(nowNano-startBNano)/float64(time.Second))
	}
	switch {
	case isAsyncB:
		c.config.Metrics.IncWriteAsync(ClusterB)
	case isDroppedB:
		c.config.Metrics.IncWriteDropped(ClusterB)
	case errB != nil:
		c.config.Metrics.IncWriteError(ClusterB)
	}

	// Auto-refresh stat tracking — invoked PER cluster so partial-success
	// (A=ok, B=err) correctly advances A's lastSuccess while accumulating
	// failures on B. recordOpOutcomeAt internally skips ErrWriteAsync /
	// ErrWriteDropped / ErrNotFound so operational states don't poison
	// the failure counters. Reuse the already-captured nowNano so the
	// helper does not re-sample the clock.
	c.recordOpOutcomeAt(ClusterA, errA, nowNano)
	c.recordOpOutcomeAt(ClusterB, errB, nowNano)

	// Both succeeded definitively.
	if errA == nil && errB == nil {
		return nil
	}

	// Both clusters had real (non-operational) failures — hard error, no replay.
	realErrA := errA != nil && !isAsyncA && !isDroppedA
	realErrB := errB != nil && !isAsyncB && !isDroppedB
	if realErrA && realErrB {
		return &types.DualClusterError{ErrorA: errA, ErrorB: errB}
	}

	// At least one cluster had a non-nil result (error, async, or dropped).
	// Enqueue replay for each affected cluster to ensure eventual consistency.
	//
	// ErrWriteAsync:   write is in flight; replay is a safety net (idempotent for Cassandra
	//                  because both attempts use the same client-generated timestamp).
	// ErrWriteDropped: write was never attempted; replay is required for reconciliation.
	// Real error:      write definitively failed; replay is required.
	if c.config.Replayer != nil {
		c.enqueueReplayIfNeeded(ctx, wc, ClusterA, errA, isAsyncA, isDroppedA)
		c.enqueueReplayIfNeeded(ctx, wc, ClusterB, errB, isAsyncB, isDroppedB)
	}

	// Partial success (or all-async) is still success from the caller's perspective.
	return nil
}

// enqueueReplayIfNeeded enqueues a replay payload when a cluster write had a non-nil result.
// isAsync and isDropped distinguish the two operational sentinel states so the log message
// accurately reflects what happened: async means the write is in flight; dropped means the
// write was never attempted because the concurrency limit was full.
func (c *CQLClient) enqueueReplayIfNeeded(
	ctx context.Context,
	wc writeContext,
	cluster ClusterID,
	err error,
	isAsync, isDropped bool,
) {
	if err == nil {
		return
	}

	// Byte-slice args are copied because the caller may reuse its buffers
	// as soon as the write returns, while the payload is replayed later.
	payload := types.ReplayPayload{
		TargetCluster:   cluster,
		Query:           wc.statement,
		Args:            cloneArgs(wc.args),
		IsBatch:         wc.isBatch,
		BatchType:       wc.batchType,
		BatchStatements: cloneBatchEntries(wc.batchEntries),
		Timestamp:       wc.timestamp,
		Priority:        wc.priority,
	}

	// Use context.WithoutCancel so the enqueue succeeds even if the request context is cancelled.
	if enqueueErr := c.config.Replayer.Enqueue(context.WithoutCancel(ctx), payload); enqueueErr == nil {
		c.config.Metrics.IncReplayEnqueued(cluster)
		switch {
		case isDropped:
			c.config.Logger.Info("write dropped (concurrency limit reached) on degraded cluster, enqueued for replay",
				"cluster", c.clusterName(cluster),
			)
		case isAsync:
			c.config.Logger.Info("write dispatched asynchronously to degraded cluster, enqueued for replay",
				"cluster", c.clusterName(cluster),
			)
		default:
			c.config.Logger.Warn("write failed on cluster, enqueued for replay",
				"cluster", c.clusterName(cluster),
				"error", err.Error(),
			)
		}
	} else {
		c.config.Metrics.IncReplayDropped(cluster)
		c.config.Logger.Error("failed to enqueue write for replay",
			"cluster", c.clusterName(cluster),
			"writeError", err.Error(),
			"enqueueError", enqueueErr.Error(),
		)
		c.emitReplayDropped(cluster, payload, enqueueErr)
	}
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
) error {
	var startA, startB atomic.Int64

	writeA := func(ctx context.Context) error {
		startA.Store(time.Now().UnixNano())
		return writeFunc(ctx, c.loadSessionA())
	}
	writeB := func(ctx context.Context) error {
		startB.Store(time.Now().UnixNano())
		return writeFunc(ctx, c.loadSessionB())
	}

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

	isSkippedA := errors.Is(errA, types.ErrClusterDegraded) || errors.Is(errA, types.ErrClusterDraining)
	isSkippedB := errors.Is(errB, types.ErrClusterDegraded) || errors.Is(errB, types.ErrClusterDraining)

	sm, _ := c.config.Metrics.(types.StrictMetrics)

	c.config.Metrics.IncWriteTotal(ClusterA)
	if startANano := startA.Load(); startANano > 0 {
		c.config.Metrics.ObserveWriteDuration(ClusterA, float64(nowNano-startANano)/float64(time.Second))
	}
	if errA != nil && !isSkippedA {
		c.config.Metrics.IncWriteError(ClusterA)
	} else if isSkippedA && sm != nil {
		sm.IncWriteSkipped(ClusterA)
	}

	c.config.Metrics.IncWriteTotal(ClusterB)
	if startBNano := startB.Load(); startBNano > 0 {
		c.config.Metrics.ObserveWriteDuration(ClusterB, float64(nowNano-startBNano)/float64(time.Second))
	}
	if errB != nil && !isSkippedB {
		c.config.Metrics.IncWriteError(ClusterB)
	} else if isSkippedB && sm != nil {
		sm.IncWriteSkipped(ClusterB)
	}

	c.recordOpOutcomeAt(ClusterA, errA, nowNano)
	c.recordOpOutcomeAt(ClusterB, errB, nowNano)

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
