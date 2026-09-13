package helix

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/types"
)

// readErrKind classifies the result of one read attempt against one cluster.
// classifyReadErr is the only place that assigns a kind;
// every read entry point switches on the kind rather than on the error.
type readErrKind uint8

const (
	// readOK is a successful read.
	readOK readErrKind = iota
	// readNotFound is [types.ErrNotFound]: data, not a health signal,
	// and the only kind that may trigger a FallbackRead probe.
	readNotFound
	// readRowLimit is [types.ErrRowLimitExceeded]: an application cap,
	// never a health signal, never retried on another cluster.
	readRowLimit
	// readCallerNotFound is a not-found the caller's own scan callback returned.
	// It terminates the read without a health signal and without a FallbackRead probe.
	readCallerNotFound
	// readCtxErr is an error observed after the caller's context was
	// cancelled or expired. It is the caller's doing, not the cluster's:
	// it is surfaced verbatim, never counted against the cluster's health,
	// and never followed by a failover attempt.
	readCtxErr
	// readClusterErr is any other error: a cluster fault. A context error
	// returned by the driver while the caller's context is still live is a
	// driver-side timeout and falls in this kind.
	readClusterErr
)

// classifyReadErr assigns the read-pipeline kind of err for a read issued
// with ctx. Classification is by provenance: when ctx is already done, any
// error is attributed to the caller rather than to the cluster.
func classifyReadErr(ctx context.Context, err error) readErrKind {
	switch {
	case err == nil:
		return readOK
	case errors.Is(err, types.ErrNotFound):
		return readNotFound
	case errors.Is(err, types.ErrRowLimitExceeded):
		return readRowLimit
	case isShieldedScanFnNotFound(err):
		return readCallerNotFound
	case ctx.Err() != nil:
		return readCtxErr
	default:
		return readClusterErr
	}
}

// isHealthSignal reports whether the kind counts against the cluster's health.
func (k readErrKind) isHealthSignal() bool {
	return k == readClusterErr
}

// getDrainStates returns the current drain state for both clusters.
func (c *CQLClient) getDrainStates() (drainA, drainB bool) {
	return c.drainA.Load(), c.drainB.Load()
}

// clusterIsDraining checks if the given cluster is draining based on cached states.
func (c *CQLClient) clusterIsDraining(cluster ClusterID, drainA, drainB bool) bool {
	if cluster == ClusterA {
		return drainA
	}

	return drainB
}

// alternativeCluster returns the other cluster.
func (c *CQLClient) alternativeCluster(cluster ClusterID) ClusterID {
	if cluster == ClusterA {
		return ClusterB
	}

	return ClusterA
}

// selectClusterForCAS returns the cluster to use for CAS (lightweight transaction)
// operations. CAS operations are single-cluster, non-replicated conditional writes
// and are NOT affected by the AllowedClusters override, but a draining
// cluster is avoided when the other one is not draining.
func (c *CQLClient) selectClusterForCAS(ctx context.Context) ClusterID {
	return c.avoidDraining(c.normalSelect(ctx))
}

// avoidDraining returns the other cluster when selected is draining and
// the other is not; otherwise selected. With both clusters draining the
// selection stands (best effort). A single-cluster client has nowhere
// else to go.
func (c *CQLClient) avoidDraining(selected ClusterID) ClusterID {
	drainA, drainB := c.getDrainStates()

	return c.reselect(selected, func(cluster ClusterID) bool {
		return !c.clusterIsDraining(cluster, drainA, drainB)
	})
}

// reselect keeps selected when it is eligible, moves to the other cluster
// when only that one is eligible, and otherwise lets the selection stand.
// A single-cluster client has nowhere else to go.
func (c *CQLClient) reselect(selected ClusterID, eligible func(ClusterID) bool) ClusterID {
	if c.IsSingleCluster() || eligible(selected) {
		return selected
	}
	if alt := c.alternativeCluster(selected); eligible(alt) {
		return alt
	}

	return selected
}

// overrideSnapshot is the resolved override state for a single operation.
// Returned by value — zero heap allocation.
type overrideSnapshot struct {
	active   bool      // true if override is in effect
	primary  ClusterID // first valid cluster (for routing)
	fallback ClusterID // second valid cluster (for failover), empty if none
}

// readTarget is the resolved cluster selection for a single read operation.
// Combines the selected cluster with the override snapshot so both come from
// a single atomic resolution — no divergence possible.
type readTarget struct {
	cluster ClusterID        // the cluster to read from
	snap    overrideSnapshot // override state for this operation
	err     error            // non-nil on fail-closed conditions
}

// callAllowedClusters invokes the AllowedClustersFunc with panic recovery.
// On panic, it returns ErrClusterOverridePanic.
func callAllowedClusters(fn AllowedClustersFunc) (raw []ClusterID, err error) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			err = fmt.Errorf("%w: %v\n%s", types.ErrClusterOverridePanic, r, buf[:n])
		}
	}()

	return fn(), nil
}

// resolveReadTarget is the single entry point for all read paths.
// It returns the selected cluster and override snapshot as one unit.
// Called exactly once per operation — no downstream function re-evaluates.
//
// Without an override the strategy's selection is moved away from a
// draining cluster when the other one is not draining, so every entry
// point (Scan, Iter, slice reads) avoids a draining cluster the same way.
//
// When opts.preserveSelectedCluster is true, drain-aware re-selection is
// skipped, and the dual-cluster override path returns the first known
// override entry as-is; if it is currently draining, the resolver fails
// closed with types.ErrNoValidClusters rather than shipping a paging cursor
// to a different cluster.
func (c *CQLClient) resolveReadTarget(ctx context.Context, opts readOptions) readTarget {
	if opts.pinnedCluster != "" {
		return c.resolveReadTargetPinned(opts.pinnedCluster)
	}

	raw, err := c.allowedClusters()
	if err != nil {
		return readTarget{err: err}
	}

	// No override function, nil, or empty = normal behavior
	if len(raw) == 0 {
		return c.resolveReadTargetNormal(ctx, opts)
	}

	// Single-cluster guard
	if c.IsSingleCluster() {
		for _, entry := range raw {
			if entry != ClusterA {
				if c.shouldLogOverrideErr() {
					c.config.Logger.Error("cluster override targets unconfigured cluster in single-cluster mode",
						"cluster", string(entry),
					)
				}

				return readTarget{err: types.ErrInvalidClusterOverride}
			}
		}

		c.overrideErrSeq.Store(0)

		return readTarget{
			cluster: ClusterA,
			snap:    overrideSnapshot{active: true, primary: ClusterA},
		}
	}

	// Dual-cluster paged-slice path: take the first known override entry
	// as-is, skip drain-filter fallback. Sending the next page's cursor to
	// a different cluster is unsound regardless of what triggered the swap.
	if opts.preserveSelectedCluster {
		return c.resolveReadTargetPreserved(raw)
	}

	// Dual-cluster: iterate once, dedup + filter known IDs + apply drain
	drainA, drainB := c.getDrainStates()
	var primary, fallback ClusterID
	hadKnown := false

	for _, entry := range raw {
		if entry != ClusterA && entry != ClusterB {
			continue // skip unknown
		}
		hadKnown = true
		if c.clusterIsDraining(entry, drainA, drainB) {
			continue // drain filters
		}
		if primary == "" {
			primary = entry
		} else if fallback == "" && entry != primary {
			fallback = entry
		}
	}

	if primary == "" {
		if hadKnown {
			if c.shouldLogOverrideErr() {
				c.config.Logger.Error("cluster override conflicts with drain state — no valid clusters for read",
					"overrideClusters", raw,
					"drainA", drainA,
					"drainB", drainB,
				)
			}

			return readTarget{err: types.ErrNoValidClusters} // drain conflict
		}

		if c.shouldLogOverrideErr() {
			c.config.Logger.Error("cluster override returned only unknown cluster IDs",
				"overrideClusters", raw,
			)
		}

		return readTarget{err: types.ErrInvalidClusterOverride} // all unknown
	}

	c.overrideErrSeq.Store(0)

	return readTarget{
		cluster: primary,
		snap:    overrideSnapshot{active: true, primary: primary, fallback: fallback},
	}
}

// allowedClusters calls the AllowedClusters override, if any, with panic
// recovery and rate-limited logging. An empty result means no override is
// in effect.
func (c *CQLClient) allowedClusters() ([]ClusterID, error) {
	fn := c.config.AllowedClusters
	if fn == nil {
		return nil, nil
	}
	raw, err := callAllowedClusters(fn)
	if err != nil && c.shouldLogOverrideErr() {
		c.config.Logger.Error("cluster override function panicked",
			"error", err.Error(),
		)
	}

	return raw, err
}

// resolveReadTargetNormal is the no-override branch of resolveReadTarget:
// the strategy's selection, moved away from a draining cluster unless the
// caller asked to preserve it (paged reads must not move the cursor).
func (c *CQLClient) resolveReadTargetNormal(ctx context.Context, opts readOptions) readTarget {
	selected := c.normalSelect(ctx)
	if !opts.preserveSelectedCluster {
		selected = c.avoidIneligible(selected)
	}

	return readTarget{cluster: selected}
}

// avoidIneligible moves an ordinary read off a cluster that is draining or
// vetoed by the failover policy when the other cluster is neither. Drain
// and veto form one eligibility decision, so the two can never bounce the
// selection between each other; when neither cluster is eligible the
// strategy's selection stands.
func (c *CQLClient) avoidIneligible(selected ClusterID) ClusterID {
	drainA, drainB := c.getDrainStates()

	return c.reselect(selected, func(cluster ClusterID) bool {
		return !c.clusterIsDraining(cluster, drainA, drainB) && !c.routeVetoed(cluster)
	})
}

// routeVetoed reports whether the failover policy vetoes reads to cluster;
// false when route vetoes are not enabled.
func (c *CQLClient) routeVetoed(cluster ClusterID) bool {
	return c.routeVeto != nil && c.routeVeto.VetoRoute(cluster)
}

// resolveReadTargetPinned routes a read that carries a paging token to the
// cluster that issued it. The read strategy and drain state are not
// consulted: the token is meaningless anywhere else. An AllowedClusters
// override still fences the read: when the issuing cluster is excluded the
// read fails closed with types.ErrNoValidClusters rather than shipping the
// cursor to a different cluster.
func (c *CQLClient) resolveReadTargetPinned(cluster ClusterID) readTarget {
	if c.IsSingleCluster() && cluster != ClusterA {
		return readTarget{err: types.ErrInvalidCluster}
	}

	raw, err := c.allowedClusters()
	if err != nil {
		return readTarget{err: err}
	}
	if len(raw) == 0 {
		return readTarget{cluster: cluster}
	}
	if !slices.Contains(raw, cluster) {
		if c.shouldLogOverrideErr() {
			c.config.Logger.Error("cluster override excludes the cluster that issued the paging cursor; refusing to ship it elsewhere",
				"cluster", string(cluster),
				"overrideClusters", raw,
			)
		}

		return readTarget{err: types.ErrNoValidClusters}
	}
	c.overrideErrSeq.Store(0)

	return readTarget{
		cluster: cluster,
		snap:    overrideSnapshot{active: true, primary: cluster},
	}
}

// resolveReadTargetPreserved implements the preserveSelectedCluster=true
// branch of resolveReadTarget: take the first known override entry as-is,
// fail closed with ErrNoValidClusters if it is draining, and never fall
// over to a different cluster. Used by paged slice reads where the next
// page's cursor must stay on the cluster that issued it.
func (c *CQLClient) resolveReadTargetPreserved(raw []ClusterID) readTarget {
	var first ClusterID
	for _, entry := range raw {
		if entry == ClusterA || entry == ClusterB {
			first = entry
			break
		}
	}

	if first == "" {
		if c.shouldLogOverrideErr() {
			c.config.Logger.Error("cluster override returned only unknown cluster IDs",
				"overrideClusters", raw,
			)
		}

		return readTarget{err: types.ErrInvalidClusterOverride}
	}

	drainA, drainB := c.getDrainStates()
	if c.clusterIsDraining(first, drainA, drainB) {
		if c.shouldLogOverrideErr() {
			c.config.Logger.Error("cluster override first entry is draining; refusing to ship paging cursor to a different cluster",
				"overrideClusters", raw,
				"drainA", drainA,
				"drainB", drainB,
			)
		}

		return readTarget{err: types.ErrNoValidClusters}
	}

	c.overrideErrSeq.Store(0)

	return readTarget{
		cluster: first,
		snap:    overrideSnapshot{active: true, primary: first},
	}
}

// normalSelect delegates to the ReadStrategy or defaults to ClusterA.
func (c *CQLClient) normalSelect(ctx context.Context) ClusterID {
	if c.IsSingleCluster() || c.config.ReadStrategy == nil {
		return ClusterA
	}
	return c.config.ReadStrategy.Select(ctx)
}

// readOptions holds per-read options resolved from the three-level hierarchy:
// per-query FallbackRead() > context WithFallbackRead(ctx) > client DefaultFallbackRead.
//
// preserveSelectedCluster suppresses every cluster-switching step a paged
// slice read must avoid (PageState cursors are opaque per-cluster):
// drain-aware initial rerouting and the AllowedClusters override drain-
// filter fallback. The zero value preserves the pre-existing behavior for
// non-slice callers.
//
// fallbackOpts customizes the executeFallbackRead alt-leg semantics for
// slice reads (drain-skip on alt, ctx-error propagation, ctx-error health
// suppression). The zero value reproduces today's Scan / MapScan behavior:
// no drain skip, suppress all real alt errors to ErrNotFound, record health
// on all real alt errors. See fallbackReadOptions.
type readOptions struct {
	fallbackRead            bool
	preserveSelectedCluster bool
	// pinnedCluster, when set, is the cluster that issued the paging token
	// this read carries; the read goes there regardless of the strategy.
	pinnedCluster ClusterID
	fallbackOpts  fallbackReadOptions
}

// resolveReadOptions derives the readOptions for one query: the FallbackRead
// hierarchy (per-query > context > client default) and the PageState routing
// fields, so Scan, MapScan, and the slice reads all route a paging token to
// the cluster that issued it.
func (c *CQLClient) resolveReadOptions(ctx context.Context, q *cqlQuery) readOptions {
	enabled := q.fallbackRead || hasFallbackRead(ctx) || c.config.DefaultFallbackRead

	opts := readOptions{
		fallbackRead: enabled,
		fallbackOpts: fallbackReadOptions{readDrainingAlt: c.config.FallbackReadOnDrainingCluster},
	}
	q.applyPagedRouting(&opts)

	return opts
}

// primaryReadOutcome is what a primary read leaves for its caller once the read has been reported to the observation hub.
//
// done means the read is finished and err is the caller's result:
// a pre-attempt failure, a success, a data sentinel, a caller-context error, or whatever the FallbackRead probe returned.
// done=false means the primary failed with a health signal that runPrimaryRead already reported, and err is that failure;
// target is the routing state a caller needs to retry the request on the alternative.
//
// The cluster the attempt targeted is target.cluster, which is not necessarily the one that produced err:
// a FallbackRead probe answers from the alternative and reports its own outcome against that cluster.
type primaryReadOutcome struct {
	done   bool
	err    error
	target readTarget
}

// runPrimaryRead takes a read as far as it can go without deciding routing:
// pre-attempt fail-closed checks,
// cluster selection through resolveReadTarget,
// the once-per-attempt IncReadTotal / ObserveReadDuration metrics,
// the terminal signal for a success or a health-signalling failure,
// and the FallbackRead probe a not-found may trigger.
//
// Each fires exactly once per primary attempt, because this is the only place the primary's outcome is written.
// The alternative's own outcome is reported by tryFallbackCluster and executeFallbackRead, against that cluster.
//
// Failover is deliberately not here:
// a FallbackRead probe resolves the same read on the alternative, while failover retries the request,
// and only the latter is the caller's decision.
func (c *CQLClient) runPrimaryRead(
	ctx context.Context,
	opts readOptions,
	readFunc func(context.Context, cql.Session) error,
) primaryReadOutcome {
	if c.closed.Load() {
		return primaryReadOutcome{done: true, err: types.ErrSessionClosed}
	}

	rt := c.resolveReadTarget(ctx, opts)
	if rt.err != nil {
		return primaryReadOutcome{done: true, err: rt.err, target: rt}
	}

	// Single-cluster mode applies whether AllowedClusters is nil, returns nil/empty, or returns [ClusterA]:
	// resolveReadTarget returns ClusterA and holderFor maps every cluster to the only session.
	// Otherwise resolveReadTarget already moved the selection away from an ineligible cluster where the options allow it.
	selected := rt.cluster
	if c.IsSingleCluster() {
		selected = ClusterA
	}
	holder, elapsed, err := c.attemptRead(ctx, selected, readFunc)

	if err == nil {
		c.health.readSucceeded(holder, selected, rt.snap.active, elapsed)

		return primaryReadOutcome{done: true, target: rt}
	}

	kind := classifyReadErr(ctx, err)
	// Data sentinels and caller-context errors are not health signals —
	// the cluster responded correctly, or the caller gave up.
	// That includes a not-found the caller's own scan callback returned,
	// which SliceScanContext unwraps at the public boundary.
	// Only a genuine not-found triggers the FallbackRead probe,
	// and only in dual-cluster mode (single-cluster has no alternative session).
	if !kind.isHealthSignal() {
		if kind == readNotFound && opts.fallbackRead && !c.IsSingleCluster() {
			err = c.executeFallbackRead(ctx, rt.snap, selected, readFunc, opts.fallbackOpts)
		}

		return primaryReadOutcome{done: true, err: err, target: rt}
	}

	// Real error: the hub records the metric, the stats, and the policy failure once.
	// What happens next is routing, and that is the caller's.
	c.health.readFailed(holder, selected, kind, err)

	return primaryReadOutcome{err: err, target: rt}
}

// readLegContext bounds one read leg by [ClientConfig.ClusterReadTimeout].
// The leg's own deadline expiring leaves the caller's context live, so
// classifyReadErr sees a live context and attributes the failure to the
// cluster. Single-cluster mode has no alternative to preserve budget for,
// so a leg there runs on the caller's context unchanged.
func (c *CQLClient) readLegContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if !c.legTimeoutActive() {
		return ctx, noopCancel
	}

	return context.WithTimeout(ctx, c.config.ClusterReadTimeout)
}

// legTimeoutActive reports whether a read leg gets a deadline of its own.
// It is the single gate both [CQLClient.readLegContext] and the iterator's
// first page consult, so the two cannot drift: a single-cluster client has
// no alternative to preserve budget for, and an unset timeout keeps every
// leg on the caller's context.
func (c *CQLClient) legTimeoutActive() bool {
	return c.config.ClusterReadTimeout > 0 && !c.IsSingleCluster()
}

// attemptRead runs readFunc once against the installed session for
// cluster and records the per-attempt metrics. It returns the holder the
// attempt used so the outcome can be reported against it.
//
// attemptRead is the only place a read reaches a session, so it is also
// the only place a read leg's own deadline is applied. The leg context
// never escapes: every caller keeps classifying against the caller's
// context, and a leg that expires under Helix's deadline arrives there as
// [types.ErrClusterTimeout].
func (c *CQLClient) attemptRead(
	ctx context.Context,
	cluster ClusterID,
	readFunc func(context.Context, cql.Session) error,
) (holder *sessionHolder, elapsed float64, err error) {
	holder = c.holderFor(cluster)
	legCtx, cancel := c.readLegContext(ctx)
	defer cancel()

	start := time.Now()
	err = readFunc(legCtx, holder.s)
	elapsed = time.Since(start).Seconds()

	// A leg ended by Helix's own deadline while the caller was still
	// waiting is a connectivity failure, not an arbitrary driver error.
	err = clusterTimeoutIfExpired(legCtx, ctx, err)

	c.config.Metrics.IncReadTotal(cluster)
	c.config.Metrics.ObserveReadDuration(cluster, elapsed)

	// Every caller classifies the returned error against this same ctx, so
	// this is also the one place a caller-expired attempt can be counted
	// exactly once — on the cluster the attempt actually targeted.
	if err != nil && classifyReadErr(ctx, err) == readCtxErr {
		c.health.readCallerExpired(cluster)
	}

	return holder, elapsed, err
}

// executeRead performs a read operation with optional sticky routing and failover.
//
// In single-cluster mode, the read is executed directly on sessionA.
// In dual-cluster mode, reads use sticky routing with failover to the alternative cluster.
// Clusters in drain mode are skipped unless both clusters are draining.
//
// When an AllowedClusters override is active, the ReadStrategy is bypassed and
// the override list directly controls routing. The strategy's internal state is
// frozen (no OnSuccess/OnFailure calls) until the override is removed.
//
// Not-found results (types.ErrNotFound) are never recorded as cluster failures.
// If opts.fallbackRead is true and the selected cluster returns not-found,
// executeFallbackRead silently tries the other cluster before returning not-found.
//
// types.ErrRowLimitExceeded is treated identically to ErrNotFound for health
// purposes (no IncReadError, no RecordFailure) but never triggers
// FallbackRead empty-retry — it propagates as-is.
func (c *CQLClient) executeRead(
	ctx context.Context,
	opts readOptions,
	readFunc func(context.Context, cql.Session) error,
) error {
	out := c.runPrimaryRead(ctx, opts, readFunc)
	if out.done {
		return out.err
	}

	// A real primary error, already reported.
	// All that is left is routing, and single-cluster mode has no failover target.
	if c.IsSingleCluster() {
		return out.err
	}

	fallback, ok := c.failoverTarget(ctx, out.target, out.err)
	if !ok {
		return out.err
	}

	return c.tryFallbackCluster(ctx, out.target, fallback, out.err, readFunc)
}

// executeReadNoFailover runs a primary read and never enters standard failover.
// Used by paged slice reads where re-running the readFunc on the alternative would leak an opaque PageState cursor,
// or (for SliceScan) re-invoke the caller's scanFn after partial accumulator mutation.
//
// Terminal signalling and the FallbackRead probe are inherited unchanged from [CQLClient.runPrimaryRead],
// so per-cluster health stays consistent with the failover path.
func (c *CQLClient) executeReadNoFailover(
	ctx context.Context,
	opts readOptions,
	readFunc func(context.Context, cql.Session) error,
) error {
	// A reported primary failure and a finished read are the same answer here:
	// with no failover to decide, the outcome's error is the caller's result either way.
	return c.runPrimaryRead(ctx, opts, readFunc).err
}

// failoverTarget returns the cluster a read that failed on rt.cluster may
// retry on, and whether it may retry at all.
// An active AllowedClusters override names the alternative from its own
// snapshot and leaves the ReadStrategy frozen; without one the strategy
// names it. Every read that fails over passes through here, so the two
// gatings cannot drift apart.
//
// Returns:
//   - ClusterID: The cluster to retry on; empty when the read may not retry
//   - bool: Whether a retry is allowed
func (c *CQLClient) failoverTarget(ctx context.Context, rt readTarget, primaryErr error) (ClusterID, bool) {
	if rt.snap.active {
		return c.overrideFailoverTarget(ctx, rt, primaryErr)
	}

	return c.normalFailoverTarget(ctx, rt.cluster, primaryErr)
}

// overrideFailoverTarget gates failover while an AllowedClusters override
// is active: the target comes from the override snapshot rather than from
// the ReadStrategy, the FailoverPolicy still has a veto, and a caller
// whose context already ended cannot succeed on the other cluster either.
func (c *CQLClient) overrideFailoverTarget(ctx context.Context, rt readTarget, primaryErr error) (ClusterID, bool) {
	selected := rt.cluster

	// No failover target in the override list
	if rt.snap.fallback == "" || rt.snap.fallback == selected {
		return "", false
	}

	// FailoverPolicy still gates failover
	if c.config.FailoverPolicy != nil &&
		!c.config.FailoverPolicy.ShouldFailover(selected, primaryErr) {
		return "", false
	}

	// A dead caller context cannot succeed on the other cluster either.
	if ctx.Err() != nil {
		return "", false
	}

	return rt.snap.fallback, true
}

// failoverAllowed reports whether a read failure on selectedCluster may be
// acted on: the FailoverPolicy decides whether the failure may move the
// read, and a draining alternative is skipped unless the read came from a
// draining cluster too.
//
// It is the request-independent half of the failover gate, so the iterator
// close path shares it.
// Both callers run it before [ReadStrategy.OnFailure], which moves the
// strategy's preference as it answers: a preference that moves for a
// failover the client would have refused sends the routing gauge and the
// event stream to a cluster no read follows.
//
// The hub has already recorded the failure by the time this runs,
// so the policy now only decides whether that failure may be acted on.
// Every bundled FailoverPolicy answers ShouldFailover from state it does
// not change, so asking it here observes without disturbing the breaker.
//
// Parameters:
//   - selectedCluster: The cluster the read failed on
//   - primaryErr: The error that cluster returned
//
// Returns:
//   - bool: true when the failure may move the read to the other cluster
func (c *CQLClient) failoverAllowed(selectedCluster ClusterID, primaryErr error) bool {
	if c.config.FailoverPolicy != nil && !c.config.FailoverPolicy.ShouldFailover(selectedCluster, primaryErr) {
		return false
	}

	// Don't failover to a draining cluster unless we came from a draining cluster too.
	// With two clusters the alternative is fixed, so the gate needs no answer from the strategy.
	drainA, drainB := c.getDrainStates()

	return !c.clusterIsDraining(c.alternativeCluster(selectedCluster), drainA, drainB) ||
		c.clusterIsDraining(selectedCluster, drainA, drainB)
}

// normalFailoverTarget gates failover with no override active: the
// FailoverPolicy decides whether this request may retry, a draining
// alternative is skipped unless the read came from a draining cluster too,
// a caller whose context already ended cannot succeed elsewhere, and only
// then is the ReadStrategy asked to name the alternative.
// The strategy is consulted last because [ReadStrategy.OnFailure] moves
// its preference as it answers; asking it for a failover the client then
// refuses would move the preference, the gauge, and the event stream to a
// cluster no read follows.
func (c *CQLClient) normalFailoverTarget(
	ctx context.Context,
	selectedCluster ClusterID,
	primaryErr error,
) (ClusterID, bool) {
	if !c.failoverAllowed(selectedCluster, primaryErr) {
		return "", false
	}

	// A dead caller context cannot succeed on the other cluster either.
	if ctx.Err() != nil {
		return "", false
	}

	// Ask strategy for alternative.
	if c.config.ReadStrategy == nil {
		return c.alternativeCluster(selectedCluster), true
	}

	alternativeCluster, shouldFailover := c.config.ReadStrategy.OnFailure(selectedCluster, primaryErr)
	if !shouldFailover {
		return "", false
	}

	return alternativeCluster, true
}

// tryFallbackCluster executes a read on the fallback cluster after the primary
// cluster failed. It records metrics, handles not-found, and returns a
// DualClusterError when both clusters fail.
//
// rt is the primary's resolved target, carrying both the cluster the read
// failed on and the override state that named fallback: an override freezes
// the ReadStrategy, so a success here is reported to the hub but not to the
// strategy. Taking the pair as resolved keeps them from disagreeing.
func (c *CQLClient) tryFallbackCluster(
	ctx context.Context,
	rt readTarget,
	fallback ClusterID,
	primaryErr error,
	readFunc func(context.Context, cql.Session) error,
) error {
	c.announceFailover(rt.cluster, fallback, primaryErr)

	holder, elapsed, err := c.attemptRead(ctx, fallback, readFunc)
	if err == nil {
		c.health.readSucceeded(holder, fallback, rt.snap.active, elapsed)
		return nil
	}

	// Data sentinels (ErrNotFound, ErrRowLimitExceeded) and caller-context
	// errors propagate as-is — none describes a cluster fault, so we do not
	// record health and we do not wrap them in DualClusterError.
	// ErrRowLimitExceeded reaching this site means the failover cluster
	// also exceeded the application cap; the caller wants to see that, not
	// a wrapped two-cluster error.
	kind := classifyReadErr(ctx, err)
	if !kind.isHealthSignal() {
		return err
	}

	c.health.readFailed(holder, fallback, kind, err)

	return dualReadError(rt.cluster, primaryErr, err)
}

// announceFailover publishes the decision to retry a failed read on
// fallback: the failover metric, a warning naming both clusters, and an
// [types.EventFailover] cluster event.
// It is called once per failover attempt, before the alternative leg runs.
func (c *CQLClient) announceFailover(selected, fallback ClusterID, primaryErr error) {
	c.config.Metrics.IncFailoverTotal(selected, fallback)
	c.config.Logger.Warn("read failed, failing over to alternative cluster",
		"fromCluster", c.clusterName(selected),
		"toCluster", c.clusterName(fallback),
		"error", primaryErr.Error(),
	)
	c.emitClusterEvent(types.ClusterEvent{
		Kind:        types.EventFailover,
		Cluster:     fallback,
		FromCluster: selected,
		ToCluster:   fallback,
		Err:         primaryErr,
	})
}

// dualReadError pairs the error of the leg that ran on selected with the
// error of the leg that ran on the alternative, in cluster order, so the
// caller reads ErrorA and ErrorB as the clusters they name.
func dualReadError(selected ClusterID, primaryErr, altErr error) error {
	if selected == ClusterA {
		return &types.DualClusterError{ErrorA: primaryErr, ErrorB: altErr}
	}

	return &types.DualClusterError{ErrorA: altErr, ErrorB: primaryErr}
}

// fallbackReadOptions customizes executeFallbackRead's alt-leg semantics.
// The zero value reproduces Scan / MapScan behavior — no drain skip, suppress
// real alt errors to ErrNotFound. Slice methods pass non-zero values to opt
// into drain-aware skip and error propagation.
//
//   - readDrainingAlt: when true, executeFallbackRead contacts the alt
//     session even while it is draining. By default a draining alt returns
//     ErrNotFound immediately — no IncReadTotal, no ObserveReadDuration, no
//     health calls — because drain is the operator's "do not read here"
//     signal and a draining cluster may hold stale rows. Scan / MapScan set
//     it from WithFallbackReadOnDrainingCluster; slice methods never do,
//     because a multi-row read on a draining cluster can return partial
//     state.
//   - propagateAltErr: when non-nil and it returns true for a given alt
//     error, executeFallbackRead returns that error to the caller instead of
//     suppressing it to ErrNotFound. Health is recorded for every real alt
//     error regardless of propagation; caller-context errors never reach
//     the predicate because classifyReadErr returns them to the caller first.
type fallbackReadOptions struct {
	readDrainingAlt bool
	propagateAltErr func(error) bool
}

// executeFallbackRead attempts a single silent read on the alternative cluster
// after the selected cluster returned not-found.
//
// This is a one-shot check — it does NOT re-enter the main failover sequence.
// A draining alternative is not contacted unless opts.readDrainingAlt is
// set (see fallbackReadOptions and WithFallbackReadOnDrainingCluster).
//
// When override IS active, the alternative must be in the allowed set.
// If the alternative is fenced off, ErrNotFound is returned immediately.
//
// Returns:
//   - nil when the alternative cluster has the data (divergence metric emitted)
//   - types.ErrNotFound when both clusters confirm the row is absent, OR when
//     the alternative cluster is unreachable AND opts.propagateAltErr did not
//     opt into propagation (health metrics are still recorded on the
//     unreachable cluster)
//   - the alt's error verbatim when opts.propagateAltErr returns true, or
//     when the caller's context ended during the probe
//   - the caller's context error when it had already ended before the probe
func (c *CQLClient) executeFallbackRead(
	ctx context.Context,
	snap overrideSnapshot,
	selectedCluster ClusterID,
	readFunc func(context.Context, cql.Session) error,
	opts fallbackReadOptions,
) error {
	alternativeCluster := c.alternativeCluster(selectedCluster)

	// A caller whose context has ended gets that error, as it would from
	// the probe itself; the alternative is not contacted with a dead context.
	if err := ctx.Err(); err != nil {
		return err
	}

	// Override fence: don't probe a cluster excluded by the override.
	if snap.active && alternativeCluster != snap.primary && alternativeCluster != snap.fallback {
		return types.ErrNotFound
	}

	// Drain-skip: a draining cluster is the operator's "do not read here",
	// and for multi-row results it could expose partial state. Skip without
	// any alt-side telemetry — this is a routing decision, not a fault.
	if !opts.readDrainingAlt {
		drainA, drainB := c.getDrainStates()
		if c.clusterIsDraining(alternativeCluster, drainA, drainB) {
			return types.ErrNotFound
		}
	}
	// A vetoed alternative is skipped the same way: its breaker is open.
	if c.routeVetoed(alternativeCluster) {
		return types.ErrNotFound
	}

	c.config.Logger.Debug("fallback read: selected cluster returned not-found, trying alternative",
		"fromCluster", c.clusterName(selectedCluster),
		"toCluster", c.clusterName(alternativeCluster),
	)

	alternative, elapsed, err := c.attemptRead(ctx, alternativeCluster, readFunc)
	if err == nil {
		// Found the data on the alternative cluster — divergence (replay lag).
		c.health.readSucceeded(alternative, alternativeCluster, snap.active, elapsed)
		c.config.Metrics.IncReadDivergence(selectedCluster)
		c.config.Logger.Debug("fallback read: found data on alternative cluster",
			"staleCluster", c.clusterName(selectedCluster),
		)
		c.emitClusterEvent(types.ClusterEvent{
			Kind:    types.EventReadDivergence,
			Cluster: selectedCluster,
			Reason:  "row found on alternative cluster after not-found",
		})

		return nil
	}

	kind := classifyReadErr(ctx, err)
	switch kind {
	case readNotFound:
		// Both clusters confirmed the row is absent — definitively not found.
		c.config.Logger.Debug("fallback read: alternative cluster also returned not-found",
			"cluster", c.clusterName(alternativeCluster),
		)
		return err
	case readRowLimit, readCallerNotFound, readCtxErr:
		// ErrRowLimitExceeded is an application-level cap, not a cluster fault.
		// Propagate as-is: no IncReadError, no recordOpOutcome failure, no
		// RecordFailure. Suppressing it to ErrNotFound would silently truncate
		// when the partition genuinely contains more rows than MaxRows. The
		// primary's empty-result already triggered fallback, so the alt is the
		// one that overflowed — surface it.
		//
		// A not-found returned by the caller's own scan callback on the alt
		// leg is likewise the caller's data, not an alt-cluster fault. The
		// caller still sees it: SliceScanContext's propagateAltErr always
		// returns true once scanFn ran on the alt, and unwraps the shield
		// before returning.
		//
		// A caller whose context ended while the alternative was being
		// asked sees its own context error, again with no health impact.
		return err
	case readOK, readClusterErr:
	}

	// Alternative returned a real error: record health on the alt.
	c.health.readFailed(alternative, alternativeCluster, kind, err)

	// Propagation is governed independently: opts.propagateAltErr lets the
	// SliceScan caller surface "scanFn was invoked on alt" to the caller.
	// Default (nil) preserves Scan / MapScan suppression to ErrNotFound —
	// primary already returned a healthy not-found, so the fallback must
	// not decrease availability.
	if opts.propagateAltErr != nil && opts.propagateAltErr(err) {
		c.config.Logger.Warn("fallback read: alternative cluster returned error, propagating to caller",
			"cluster", c.clusterName(alternativeCluster),
			"error", err.Error(),
		)
		return err
	}

	c.config.Logger.Warn("fallback read: alternative cluster returned error, returning primary not-found",
		"cluster", c.clusterName(alternativeCluster),
		"error", err.Error(),
	)

	return types.ErrNotFound
}
