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

func (c *CQLClient) resolveReadOptions(ctx context.Context, q *cqlQuery) readOptions {
	enabled := q.fallbackRead || hasFallbackRead(ctx) || c.config.DefaultFallbackRead

	return readOptions{
		fallbackRead: enabled,
		fallbackOpts: fallbackReadOptions{readDrainingAlt: c.config.FallbackReadOnDrainingCluster},
	}
}

// primaryAttemptResult captures the outcome of one primary-cluster read
// attempt. It carries enough state for either the failover-enabled or the
// no-failover wrapper to record terminal signals correctly without each
// reimplementing the resolve-and-attempt sequence.
//
// attempted=false means no read was sent to any cluster (client closed,
// resolveReadTarget failed, etc.); err carries the pre-attempt error and
// wrappers MUST NOT emit cluster metrics or call cluster-health functions.
// attempted=true means a read was sent and runPrimaryRead recorded
// IncReadTotal + ObserveReadDuration; err is nil on success or the
// cluster's error.
type primaryAttemptResult struct {
	attempted bool
	target    readTarget
	selected  ClusterID
	holder    *sessionHolder // the session the attempt used
	elapsed   float64
	err       error
}

// runPrimaryRead executes the single-attempt portion of a read: pre-
// attempt fail-closed checks, cluster selection through resolveReadTarget,
// and the once-per-attempt IncReadTotal / ObserveReadDuration metrics.
//
// runPrimaryRead intentionally reports nothing to the observation hub.
// Those terminal signals are caller-owned so each fires exactly once per
// attempt.
func (c *CQLClient) runPrimaryRead(
	ctx context.Context,
	opts readOptions,
	readFunc func(context.Context, cql.Session) error,
) primaryAttemptResult {
	if c.closed.Load() {
		return primaryAttemptResult{err: types.ErrSessionClosed}
	}

	rt := c.resolveReadTarget(ctx, opts)
	if rt.err != nil {
		return primaryAttemptResult{err: rt.err, target: rt}
	}

	// Single-cluster mode applies whether AllowedClusters is nil, returns
	// nil/empty, or returns [ClusterA]: resolveReadTarget returns ClusterA
	// and holderFor maps every cluster to the only session. Otherwise
	// resolveReadTarget already moved the selection away from an
	// ineligible cluster where the options allow it.
	selected := rt.cluster
	if c.IsSingleCluster() {
		selected = ClusterA
	}
	holder, elapsed, err := c.attemptRead(ctx, selected, readFunc)

	return primaryAttemptResult{
		attempted: true,
		target:    rt,
		selected:  selected,
		holder:    holder,
		elapsed:   elapsed,
		err:       err,
	}
}

// readLegContext bounds one read leg by [ClientConfig.ClusterReadTimeout].
// The leg's own deadline expiring leaves the caller's context live, so
// classifyReadErr sees a live context and attributes the failure to the
// cluster. Single-cluster mode has no alternative to preserve budget for,
// so a leg there runs on the caller's context unchanged.
func (c *CQLClient) readLegContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.config.ClusterReadTimeout <= 0 || c.IsSingleCluster() {
		return ctx, noopCancel
	}

	return context.WithTimeout(ctx, c.config.ClusterReadTimeout)
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
	res := c.runPrimaryRead(ctx, opts, readFunc)
	if !res.attempted {
		return res.err
	}

	if res.err == nil {
		c.health.readSucceeded(res.holder, res.selected, res.target.snap.active, res.elapsed)
		return nil
	}

	kind := classifyReadErr(ctx, res.err)
	// Data sentinels and caller-context errors are not health signals —
	// the cluster responded correctly, or the caller gave up. Only
	// not-found triggers the FallbackRead probe, and only in dual-cluster
	// mode (single-cluster has no alternative session).
	if !kind.isHealthSignal() {
		if kind == readNotFound && opts.fallbackRead && !c.IsSingleCluster() {
			return c.executeFallbackRead(ctx, res.target.snap, res.selected, readFunc, opts.fallbackOpts)
		}
		return res.err
	}

	// Real error path: the hub records the metric, the stats, and the
	// policy failure once; the failover branch below only decides routing.
	c.health.readFailed(res.holder, res.selected, kind, res.err)

	// Single-cluster real-error has no failover target.
	if c.IsSingleCluster() {
		return res.err
	}

	if res.target.snap.active {
		return c.executeOverrideFailover(ctx, res.target, res.err, readFunc)
	}

	return c.executeNormalFailover(ctx, res.selected, res.err, readFunc)
}

// executeReadNoFailover wraps runPrimaryRead with full terminal-signal
// recording but never enters standard failover. Used by paged slice reads
// where re-running the readFunc on the alternative would leak an opaque
// PageState cursor or (for SliceScan) re-invoke the caller's scanFn after
// partial accumulator mutation.
//
// On a real primary error, executeReadNoFailover reports the failure to
// the observation hub exactly as executeRead does, so per-cluster health
// stays consistent with the failover path. The returned error is the
// primary's error verbatim.
//
// opts.fallbackRead is honored; single-cluster mode skips the fallback
// invocation and returns the primary error directly.
func (c *CQLClient) executeReadNoFailover(
	ctx context.Context,
	opts readOptions,
	readFunc func(context.Context, cql.Session) error,
) error {
	res := c.runPrimaryRead(ctx, opts, readFunc)
	if !res.attempted {
		return res.err
	}

	if res.err == nil {
		c.health.readSucceeded(res.holder, res.selected, res.target.snap.active, res.elapsed)
		return nil
	}

	kind := classifyReadErr(ctx, res.err)
	// Data sentinels, a not-found returned by the caller's own scan
	// callback, and caller-context errors terminate the read without a
	// health signal. Only a genuine not-found triggers the FallbackRead
	// probe; SliceScanContext unwraps the caller's not-found at the public
	// boundary.
	if !kind.isHealthSignal() {
		if kind == readNotFound && opts.fallbackRead && !c.IsSingleCluster() {
			return c.executeFallbackRead(ctx, res.target.snap, res.selected, readFunc, opts.fallbackOpts)
		}
		return res.err
	}

	// Real error path: there is no failover branch, so the hub is the one
	// place every terminal signal is recorded.
	c.health.readFailed(res.holder, res.selected, kind, res.err)

	return res.err
}

// executeOverrideFailover handles failover when an AllowedClusters override is active.
// The ReadStrategy is NOT consulted — failover target comes from the override snapshot.
func (c *CQLClient) executeOverrideFailover(
	ctx context.Context,
	rt readTarget,
	primaryErr error,
	readFunc func(context.Context, cql.Session) error,
) error {
	selected := rt.cluster

	// No failover target in the override list
	if rt.snap.fallback == "" || rt.snap.fallback == selected {
		return primaryErr
	}

	// FailoverPolicy still gates failover
	if c.config.FailoverPolicy != nil &&
		!c.config.FailoverPolicy.ShouldFailover(selected, primaryErr) {
		return primaryErr
	}

	// A dead caller context cannot succeed on the other cluster either.
	if ctx.Err() != nil {
		return primaryErr
	}

	return c.tryFallbackCluster(ctx, selected, rt.snap.fallback, primaryErr, true, readFunc)
}

// executeNormalFailover handles failover using the ReadStrategy (no override active).
func (c *CQLClient) executeNormalFailover(
	ctx context.Context,
	selectedCluster ClusterID,
	primaryErr error,
	readFunc func(context.Context, cql.Session) error,
) error {
	// The hub has already recorded the failure; the policy now only decides
	// whether this request may retry on the other cluster.
	if c.config.FailoverPolicy != nil && !c.config.FailoverPolicy.ShouldFailover(selectedCluster, primaryErr) {
		return primaryErr
	}

	// Ask strategy for alternative.
	var alternativeCluster ClusterID
	var shouldFailover bool

	if c.config.ReadStrategy != nil {
		alternativeCluster, shouldFailover = c.config.ReadStrategy.OnFailure(selectedCluster, primaryErr)
	} else {
		alternativeCluster = c.alternativeCluster(selectedCluster)
		shouldFailover = true
	}

	// Don't failover to a draining cluster unless we came from a draining cluster too.
	drainA, drainB := c.getDrainStates()
	if shouldFailover && c.clusterIsDraining(alternativeCluster, drainA, drainB) {
		if !c.clusterIsDraining(selectedCluster, drainA, drainB) {
			shouldFailover = false
		}
	}

	if !shouldFailover {
		return primaryErr
	}

	// A dead caller context cannot succeed on the other cluster either.
	if ctx.Err() != nil {
		return primaryErr
	}

	return c.tryFallbackCluster(ctx, selectedCluster, alternativeCluster, primaryErr, false, readFunc)
}

// tryFallbackCluster executes a read on the fallback cluster after the primary
// cluster failed. It records metrics, handles not-found, and returns a
// DualClusterError when both clusters fail. Used by both override and normal
// failover paths.
func (c *CQLClient) tryFallbackCluster(
	ctx context.Context,
	selected, fallback ClusterID,
	primaryErr error,
	overrideActive bool,
	readFunc func(context.Context, cql.Session) error,
) error {
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

	holder, elapsed, err := c.attemptRead(ctx, fallback, readFunc)
	if err == nil {
		c.health.readSucceeded(holder, fallback, overrideActive, elapsed)
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

	if selected == ClusterA {
		return &types.DualClusterError{ErrorA: primaryErr, ErrorB: err}
	}

	return &types.DualClusterError{ErrorA: err, ErrorB: primaryErr}
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
