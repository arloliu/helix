package helix

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/types"
)

// autoRefreshLoop runs in a background goroutine when WithAutoRefresh is
// enabled and a SessionRefresher is registered. It ticks every
// AutoRefresh.CheckInterval and asks maybeAutoRefresh to evaluate each
// cluster.
//
// Started by NewCQLClient, stopped by Close.
func (c *CQLClient) autoRefreshLoop() {
	t := time.NewTicker(c.config.AutoRefresh.CheckInterval)
	defer t.Stop()

	for {
		select {
		case <-c.autoRefreshCtx.Done():
			return
		case <-t.C:
			c.maybeAutoRefresh(ClusterA)
			if !c.singleCluster {
				c.maybeAutoRefresh(ClusterB)
			}
		}
	}
}

// maybeAutoRefresh evaluates the trigger condition for the given cluster
// and invokes RefreshSession if all three predicates hold:
//
//  1. consecutiveFailures >= AutoRefresh.FailureThreshold
//  2. now - lastSuccess  >= AutoRefresh.SustainedFailureWindow
//  3. now - lastRefresh  >= AutoRefresh.MinRetryInterval (throttle)
//
// Each predicate is evaluated against atomic loads — no lock. The
// throttle stamp (lastRefreshNanos) is set BEFORE invoking the refresher
// so a hung refresher cannot cause re-entrant double-fire on the next
// tick. On successful refresh, consecutiveFailures is reset (so a
// fresh-session-also-dead scenario must re-accumulate threshold-many
// failures before re-triggering); lastSuccessNanos is left stale until
// the next genuinely successful op proves the new session is healthy.
//
// Exported for tests via direct invocation; production callers should
// not call this — the goroutine drives it.
func (c *CQLClient) maybeAutoRefresh(cluster ClusterID) {
	if c.closed.Load() {
		return
	}
	if c.config.SessionRefresher == nil {
		return // can't refresh; the loop is moot for this cluster
	}

	s := c.statsForCluster(cluster)
	lastRefresh := c.lastRefreshFor(cluster)
	now := c.config.NowProvider()
	cfg := c.config.AutoRefresh

	// Capture the failure count once: it feeds both the trigger predicate
	// and the attempt event below, so the event reports the exact count
	// that qualified this refresh even if concurrent successes reset the
	// live counter before the event is read downstream.
	//
	// FailureThreshold is an int but compared against an int32 atomic;
	// widen both sides so gosec doesn't flag the narrowing conversion.
	failures := s.consecutiveFailures.Load()
	if int64(failures) < int64(cfg.FailureThreshold) {
		return
	}
	if now-s.lastSuccessNanos.Load() < int64(cfg.SustainedFailureWindow) {
		return
	}
	if now-lastRefresh.Load() < int64(cfg.MinRetryInterval) {
		return
	}

	// Throttle stamp first — any future tick within MinRetryInterval will
	// see this and bail at the predicate above, even if RefreshSession
	// hangs or panics.
	lastRefresh.Store(now)

	refreshMetrics, _ := c.config.Metrics.(types.SessionRefreshMetrics)
	if refreshMetrics != nil {
		refreshMetrics.IncSessionRefreshAttempt(cluster)
	}
	c.emitClusterEvent(types.ClusterEvent{
		Kind:    types.EventSessionRefreshAttempt,
		Cluster: cluster,
		Count:   int(failures),
	})
	c.config.Logger.Info("auto-refresh: session believed dead, invoking refresher",
		"cluster", c.clusterName(cluster),
		"consecutiveFailures", failures,
		"secondsSinceLastSuccess", (now-s.lastSuccessNanos.Load())/int64(time.Second),
	)

	ctx, cancel := context.WithTimeout(c.autoRefreshCtx, cfg.RefreshTimeout)
	defer cancel()

	if err := c.RefreshSession(ctx, cluster); err != nil {
		if refreshMetrics != nil {
			refreshMetrics.IncSessionRefreshError(cluster)
		}
		c.emitClusterEvent(types.ClusterEvent{
			Kind:    types.EventSessionRefreshError,
			Cluster: cluster,
			Err:     err,
		})
		c.config.Logger.Warn("auto-refresh: session refresh failed",
			"cluster", c.clusterName(cluster),
			"error", err.Error(),
		)

		return
	}

	if refreshMetrics != nil {
		refreshMetrics.IncSessionRefreshSuccess(cluster)
	}
	c.emitClusterEvent(types.ClusterEvent{
		Kind:    types.EventSessionRefreshSuccess,
		Cluster: cluster,
	})
	c.config.Logger.Info("auto-refresh: session refreshed",
		"cluster", c.clusterName(cluster),
	)
	// The new session carries fresh stats on its holder: the detector
	// requires a genuinely successful op against it before considering it
	// healthy, and the throttle above prevents a spurious re-fire.
}

// watchTopology monitors topology updates and updates drain state.
//
// Cancellation does not rely solely on the configured [TopologyWatcher]
// closing its update channel when topologyCtx is cancelled — a nil channel,
// or a custom/misbehaving TopologyWatcher that never closes it, would
// otherwise block this goroutine forever and defeat [CQLClient.Close]'s
// cancellation of topologyCtx. The select loop below (mirroring
// autoRefreshLoop) gives topologyCtx.Done() an independent exit path.
func (c *CQLClient) watchTopology() {
	updates := c.config.TopologyWatcher.Watch(c.topologyCtx)
	if updates == nil {
		return
	}

	for {
		select {
		case <-c.topologyCtx.Done():
			return
		case update, ok := <-updates:
			if !ok {
				return
			}

			var previousDrain bool
			switch update.Cluster {
			case ClusterA:
				previousDrain = c.drainA.Swap(update.DrainMode)
			case ClusterB:
				previousDrain = c.drainB.Swap(update.DrainMode)
			}

			// Record drain mode transitions
			if !previousDrain && update.DrainMode {
				c.config.Metrics.IncDrainModeEntered(update.Cluster)
				c.config.Metrics.SetClusterDraining(update.Cluster, true)
				c.config.Logger.Warn("cluster entering drain mode",
					"cluster", c.clusterName(update.Cluster),
				)
				c.emitClusterEvent(types.ClusterEvent{
					Kind:    types.EventDrainEntered,
					Cluster: update.Cluster,
				})
			} else if previousDrain && !update.DrainMode {
				c.config.Metrics.IncDrainModeExited(update.Cluster)
				c.config.Metrics.SetClusterDraining(update.Cluster, false)
				c.config.Logger.Info("cluster exiting drain mode",
					"cluster", c.clusterName(update.Cluster),
				)
				c.emitClusterEvent(types.ClusterEvent{
					Kind:    types.EventDrainExited,
					Cluster: update.Cluster,
				})
			}
		}
	}
}

// IsDraining returns whether the specified cluster is currently in drain mode.
//
// When a cluster is draining:
//   - Writes to the cluster are skipped and enqueued for replay
//   - Reads (Scan, Iter, slice reads, and CAS) are routed to the
//     non-draining cluster, except a paged read that carries the draining
//     cluster's own cursor
//   - A FallbackRead probe does not contact it unless
//     [WithFallbackReadOnDrainingCluster] is set
//   - With both clusters draining, writes fail with
//     [types.ErrBothClustersDraining] and reads proceed on the selected
//     cluster
//
// Parameters:
//   - cluster: The cluster to check
//
// Returns:
//   - bool: true if the cluster is being drained
func (c *CQLClient) IsDraining(cluster ClusterID) bool {
	switch cluster {
	case ClusterA:
		return c.drainA.Load()
	case ClusterB:
		return c.drainB.Load()
	}

	return false
}

// Close marks the client closed, stops background components, and closes the
// currently installed sessions.
//
// The topology watcher, auto-refresh detector, and replay worker are stopped
// first. After Close is called, the client cannot be reused; new public
// operations including SwapSession and RefreshSession return
// [types.ErrSessionClosed].
//
// Close does not wait for in-flight reads, strict writes, or detached
// fire-and-forget writes to finish.
// Work that already captured a session may continue racing with shutdown
// and can fail when that session is closed.
// Close does wait for a replaying dual write in progress to hand its legs
// to the replay queue, and for a background leg whose strategy reports its
// result through [DeferredWriteResult] to complete, so every replay is
// enqueued before the worker stops.
// Close sets no bound of its own on that wait: it lasts as long as
// [WriteStrategy.Execute], the strategy's background legs, the replayer's
// Enqueue (called with a context that ignores the caller's cancellation),
// and the synchronous replay-dropped handler and logger take to return.
// A [WithOnReplayDropped] handler must not call Close: like a cluster
// event handler, it would wait for the write that invoked it.
//
// Close DOES wait for the configured ReplayWorker to finish via its Stop()
// method, which blocks until the worker's in-flight batch returns.
// Bound that batch's wall time via the worker's own timeouts if you need
// a hard upper bound on Close latency.
//
// A concurrent Close returns once the first one has finished.
//
// When a handler is registered via [WithOnClusterEvent], Close also stops
// event intake, drains the buffered events to that handler, and waits for
// the in-flight handler invocation to return; the final drop report is
// written through the configured Logger. A handler that does not return, or
// a Logger that blocks, therefore blocks Close. For the same reason the
// handler must never call Close itself — it would wait for its own
// invocation to finish and deadlock. Trigger shutdown from another
// goroutine instead.
//
// Calling Close concurrently with SwapSession or RefreshSession is undefined:
// Close may end up closing either the old or the new session depending on
// scheduling, and the caller of SwapSession still owns the session it
// received. Synchronize externally if you need a deterministic order. The
// underlying [cql.Session] adapters bundled with Helix (adapter/cql/v1 and
// adapter/cql/v2) make Close idempotent so a double-Close on the same
// session does not panic; custom [cql.Session] implementations must follow
// the same contract or callers must serialize Close with their own swap.
func (c *CQLClient) Close() {
	if !c.closed.CompareAndSwap(false, true) {
		<-c.closeDone

		return
	}
	defer close(c.closeDone)

	// Stop the topology watcher and the auto-refresh detector, then
	// wait for both goroutines so nothing of theirs runs after Close returns.
	// A refresh in flight sees its context cancelled and returns;
	// a refresher that ignores its context delays Close.
	if c.topologyClose != nil {
		c.topologyClose()
	}
	if c.autoRefreshClose != nil {
		c.autoRefreshClose()
	}
	c.topologyWG.Wait()
	c.autoRefreshWG.Wait()

	// Wait for replaying writes in progress and for background legs whose
	// failure would be enqueued for replay, so nothing is enqueued after
	// the worker stops.
	c.deferred.wait()

	// Stop the mirror engine first so it stops generating new failure
	// captures, then drain any failures that landed in the mirror
	// replayer through its worker.
	c.stopMirrorComponents()

	// Stop replay worker
	if c.config.ReplayWorker != nil {
		c.config.ReplayWorker.Stop()
	}

	// Cancel recovery probe goroutines and wait for them to exit before
	// closing sessions so a probe in flight cannot race against a closed session.
	if c.recoveryProbeClose != nil {
		c.recoveryProbeClose()
		c.recoveryProbeWG.Wait()
	}

	// Stop the event dispatcher: intake halts, buffered events drain to
	// the handler, and the in-flight handler invocation is awaited. The
	// topology and auto-refresh goroutines were joined above, so every
	// event of theirs is already buffered.
	c.runtime.events.stop()

	c.retired.closeAll()
	c.loadSessionA().Close()
	if !c.singleCluster {
		c.loadSessionB().Close()
	}
}

// SwapSession atomically replaces the live session for the given cluster
// with newSession and returns the previous session. The caller is
// responsible for closing the returned session once any in-flight work
// using it has drained.
//
// SwapSession is the lowest-level escape hatch for callers who have already
// built a fresh [cql.Session] (typically because they detected the live one
// is unrecoverable — e.g., the cluster restarted at a different endpoint).
// For most callers, [CQLClient.RefreshSession] combined with
// [WithSessionRefresher] is more ergonomic.
//
// Behavior:
//   - The swap is lock-free on the read path; concurrent Query/Batch/Iter
//     callers see either the old or the new session, never a partial state.
//   - Operations that have already resolved their session (in-flight Iter
//     or CAS, mid-execution synchronous calls, fire-and-forget writes
//     captured into a goroutine) continue against the session they
//     captured. Only operations that resolve the session after the swap
//     observe the new one.
//   - This method does NOT change cluster cardinality. Calling it with
//     ClusterB on a single-cluster client returns [types.ErrInvalidCluster].
//   - The returned old session is NOT closed by this method. The caller
//     decides when in-flights are quiet and calls [cql.Session.Close]
//     accordingly. Calling Close on the returned session before in-flights
//     drain may abort them.
//
// Parameters:
//   - cluster: The cluster whose session to replace.
//   - newSession: The replacement session. Must not be nil.
//
// Returns:
//   - cql.Session: The previous session for the given cluster.
//   - error: [types.ErrSessionClosed] if the client has been closed,
//     [types.ErrNilSession] if newSession is nil,
//     [types.ErrInvalidCluster] for ClusterB on a single-cluster client
//     or any unrecognized ClusterID.
func (c *CQLClient) SwapSession(cluster ClusterID, newSession cql.Session) (cql.Session, error) {
	if c.closed.Load() {
		return nil, types.ErrSessionClosed
	}
	if newSession == nil {
		return nil, types.ErrNilSession
	}

	slot, err := c.sessionSlot(cluster)
	if err != nil {
		return nil, err
	}

	old := slot.Swap(c.newSessionHolder(newSession))

	return old.s, nil
}

// RefreshSession invokes the [SessionRefresher] registered via
// [WithSessionRefresher], atomically installs the returned session in place
// of the existing one for the given cluster, and closes the old session
// once a grace period has passed.
//
// This is the high-level recovery entry point for the case where the live
// session is permanently unrecoverable (e.g., the cluster restarted at a
// new endpoint and gocql cannot reconnect). The decoupling principle —
// Helix does not know how to construct a [cql.Session] for any specific
// driver version — is preserved by delegating session construction to the
// caller-supplied refresher.
//
// Unlike [CQLClient.SwapSession], RefreshSession DOES close the old session
// because the refresh contract implies the old one is dead. If the
// refresher returns an error or a nil session, no swap occurs and the
// existing session remains live.
//
// The old session is closed after a grace period of
// [AutoRefreshConfig.RefreshTimeout] so in-flight operations that already
// captured it can finish; [CQLClient.Close] closes any such session at
// once. Without [WithAutoRefresh] the grace period is zero and the old
// session closes immediately after the swap, and drivers that fail
// outstanding work on Close will abort in-flight operations that captured
// it. Use RefreshSession only when the old session is already
// non-functional; to control the teardown yourself, use SwapSession and
// close the returned session once the in-flights are quiet.
//
// Behavior:
//   - The refresher is invoked synchronously on the calling goroutine.
//     A long-running rebuild (e.g., re-handshake against a slow cluster)
//     blocks the caller; respect the passed context.
//   - If the refresher succeeds but the swap subsequently fails (the client
//     was closed, or someone else installed a session for the cluster
//     while the refresher ran), the newly-built session is closed before
//     returning the error so no connection is leaked, and the session
//     that is installed stays untouched.
//   - The lastErr passed to the refresher is the most recently observed
//     failure error against this cluster (or nil if no op has failed
//     yet). Refreshers can inspect it to tailor reconnection strategy.
//
// Parameters:
//   - ctx: Context for the refresher and any timeouts the refresher honors.
//   - cluster: The cluster to refresh.
//
// Returns:
//   - error: [types.ErrSessionClosed] if the client has been closed,
//     [types.ErrNoSessionRefresher] if no refresher was configured,
//     [types.ErrInvalidCluster] for an unsupported cluster on this client,
//     [types.ErrNilSession] if the refresher returned a nil session,
//     [types.ErrSessionReplaced] if another session was installed for the
//     cluster while the refresher ran (the refresher's session is closed
//     and the newer one kept), or a wrapped error from the refresher.
func (c *CQLClient) RefreshSession(ctx context.Context, cluster ClusterID) error {
	if c.closed.Load() {
		return types.ErrSessionClosed
	}
	if c.config.SessionRefresher == nil {
		return types.ErrNoSessionRefresher
	}

	// Validate cluster before calling the (potentially expensive) refresher.
	slot, err := c.sessionSlot(cluster)
	if err != nil {
		return err
	}

	// Thread the most recently observed failure for this cluster through
	// to the refresher so it can tailor reconnection strategy to the
	// observed failure mode (e.g. "no hosts available" suggests a hard
	// reachability issue while "timeout" suggests a slow but reachable
	// cluster). Nil if the cluster has had no recorded failures.
	var lastErr error
	if e := c.statsForCluster(cluster).lastErr.Load(); e != nil {
		lastErr = *e
	}

	// Capture the holder the refresher is replacing. If anyone installs a
	// different session while the refresher runs, that session is newer
	// than what the refresher saw and must not be closed underneath them.
	holder := slot.Load()

	newSession, err := c.config.SessionRefresher(ctx, cluster, lastErr)
	if err != nil {
		return fmt.Errorf("session refresher: %w", err)
	}
	if newSession == nil {
		return types.ErrNilSession
	}

	if c.closed.Load() {
		// Don't leak the just-built session when the client closed while
		// the refresher ran.
		newSession.Close()

		return types.ErrSessionClosed
	}
	if !slot.CompareAndSwap(holder, c.newSessionHolder(newSession)) {
		newSession.Close()

		return types.ErrSessionReplaced
	}

	// Refresh contract: the old session is dead, so close it on the
	// caller's behalf. SwapSession's contract differs ("caller closes")
	// because the caller may have other references they want to drain;
	// RefreshSession owns the swap end-to-end so it owns the close too,
	// after a grace period that lets in-flight operations finish.
	if holder.s != nil {
		c.retired.add(holder.s, c.config.AutoRefresh.RefreshTimeout)
	}

	return nil
}

// retiredSessions holds sessions that RefreshSession replaced and closes
// each once its grace period elapses, or all of them at once on Close.
type retiredSessions struct {
	mu      sync.Mutex
	closing bool
	entries []*retiredSession
	wg      sync.WaitGroup
}

// retiredSession is one replaced session; closed guards its single Close
// between the grace timer and closeAll.
type retiredSession struct {
	s      cql.Session
	timer  *time.Timer
	closed atomic.Bool
}

// closeOnce closes the session on the first call only.
func (e *retiredSession) closeOnce() {
	if e.closed.CompareAndSwap(false, true) {
		e.s.Close()
	}
}

// add schedules s to close after grace. A non-positive grace, or a client
// that is already closing, closes s at once.
func (r *retiredSessions) add(s cql.Session, grace time.Duration) {
	r.mu.Lock()
	if r.closing || grace <= 0 {
		r.mu.Unlock()
		s.Close()

		return
	}
	entry := &retiredSession{s: s}
	r.wg.Add(1)
	entry.timer = time.AfterFunc(grace, func() {
		defer r.wg.Done()
		entry.closeOnce()
	})
	r.entries = append(r.entries, entry)
	r.mu.Unlock()
}

// closeAll closes every pending session now and waits for any grace timer
// that is already running.
func (r *retiredSessions) closeAll() {
	r.mu.Lock()
	r.closing = true
	entries := r.entries
	r.entries = nil
	r.mu.Unlock()
	for _, entry := range entries {
		if entry.timer.Stop() {
			// The timer never fired, so its callback will not release the
			// wait group; do both here.
			entry.closeOnce()
			r.wg.Done()
		}
	}
	r.wg.Wait()
}

// sessionSlot returns the atomic holder for cluster, or
// [types.ErrInvalidCluster] for an unknown cluster or for ClusterB on a
// single-cluster client.
func (c *CQLClient) sessionSlot(cluster ClusterID) (*atomic.Pointer[sessionHolder], error) {
	switch cluster {
	case ClusterA:
		return &c.sessionA, nil
	case ClusterB:
		if c.singleCluster {
			return nil, types.ErrInvalidCluster
		}

		return &c.sessionB, nil
	default:
		return nil, types.ErrInvalidCluster
	}
}
