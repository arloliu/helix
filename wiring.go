package helix

import (
	"context"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
)

// autoInjectMetricsAndLogger threads the client-level metrics and logger
// into components that opt into auto-injection via the [Instrumentable] /
// [LoggerSetter] interfaces.
//
// The replay worker auto-injects metrics so worker-side replay metrics
// land in the same collector the client uses. Write strategies follow the
// same pattern — AdaptiveDualWrite emits IncWriteError + Warn from its
// fire-and-forget background goroutine, which would otherwise hit a
// NopMetrics / NopLogger and be invisible.
func autoInjectMetricsAndLogger(config *ClientConfig) {
	if config.Metrics != nil {
		if config.ReplayWorker != nil {
			if mw, ok := config.ReplayWorker.(Instrumentable); ok && !mw.MetricsConfigured() {
				mw.SetMetrics(config.Metrics)
			}
		}
		if config.WriteStrategy != nil {
			if ws, ok := config.WriteStrategy.(Instrumentable); ok && !ws.MetricsConfigured() {
				ws.SetMetrics(config.Metrics)
			}
		}
		if config.FailoverPolicy != nil {
			if fp, ok := config.FailoverPolicy.(Instrumentable); ok && !fp.MetricsConfigured() {
				fp.SetMetrics(config.Metrics)
			}
		}
	}

	if config.Logger != nil {
		if config.WriteStrategy != nil {
			if ws, ok := config.WriteStrategy.(LoggerSetter); ok {
				ws.SetLogger(config.Logger)
			}
		}
		if config.FailoverPolicy != nil {
			if fp, ok := config.FailoverPolicy.(LoggerSetter); ok {
				fp.SetLogger(config.Logger)
			}
		}
	}
}

// warnNoEffectOptions logs one warning per option that the rest of the
// configuration renders inert, so a misconfiguration is discovered at
// startup rather than during an incident.
func warnNoEffectOptions(config *ClientConfig) {
	if config.MirrorReplayer != nil && !config.mirrorTargetSet {
		config.Logger.Warn("WithMirrorReplayer has no effect without WithMirror; failed mirror writes are only retried in target mode")
	}
	if config.RecoveryProbe != nil && !config.recoveryProbeOff {
		if _, ok := config.WriteStrategy.(ProbeReporter); !ok {
			config.Logger.Warn("WithRecoveryProbe has no effect: the write strategy does not report degraded clusters, so no probe will run")
		}
	}
}

// createEventDispatcher installs the client's event dispatcher when a handler
// is registered, and does nothing otherwise.
//
// NewCQLClient calls this before mirror setup so the mirror error handler can
// capture the dispatcher when it is built. Delivery does not begin here: the
// dispatcher buffers events until startEventDelivery runs, which happens after
// the constructor's last step that can fail, so an error path never leaves a
// delivery goroutine behind.
func (c *CQLClient) createEventDispatcher() {
	config := c.config
	if config.OnClusterEvent == nil {
		return
	}

	c.runtime.events = newEventDispatcher(config.OnClusterEvent, config.Logger)
	// Attach the optional drop-total metric. The dispatcher reconciles it
	// from its own goroutine, never from the emit hot path, so an
	// arbitrary collector implementation cannot slow emitters down.
	if cem, ok := config.Metrics.(types.ClusterEventMetrics); ok {
		c.runtime.events.metrics = cem
	}
}

// orderedClusterEventKinds lists every cluster event kind in the order
// docs/cluster-events.md documents them. eventKindUnreachable's switch is
// exhaustive-lint enforced, so adding a kind to types without classifying
// it here fails lint.
var orderedClusterEventKinds = []types.ClusterEventKind{
	types.EventFailover,
	types.EventReadDivergence,
	types.EventCircuitBreakerOpen,
	types.EventCircuitBreakerClosed,
	types.EventWriteDegraded,
	types.EventWriteRecovered,
	types.EventDrainEntered,
	types.EventDrainExited,
	types.EventReplayDropped,
	types.EventMirrorReplayDropped,
	types.EventSessionRefreshAttempt,
	types.EventSessionRefreshSuccess,
	types.EventSessionRefreshError,
}

// eventKindUnreachable reports whether kind can never fire given the rest
// of the configuration. Reachability mirrors the Requires column of
// docs/cluster-events.md: most kinds come from an optional component, and
// seven additionally require dual-cluster mode. read_divergence is treated
// as reachable in dual-cluster mode because FallbackRead is a per-read
// runtime opt-in the constructor cannot see. mirror_replay_dropped is
// best-effort: a caller-supplied mirror.WithOnError also suppresses it,
// but mirror options are opaque here.
func eventKindUnreachable(kind types.ClusterEventKind, config *ClientConfig, dualCluster bool) bool {
	switch kind {
	case types.EventFailover, types.EventReadDivergence:
		return !dualCluster
	case types.EventCircuitBreakerOpen, types.EventCircuitBreakerClosed:
		_, ok := config.FailoverPolicy.(EventEmitterSetter)
		return !ok || !dualCluster
	case types.EventWriteDegraded, types.EventWriteRecovered:
		_, ok := config.WriteStrategy.(EventEmitterSetter)
		return !ok || !dualCluster
	case types.EventDrainEntered, types.EventDrainExited:
		return config.TopologyWatcher == nil
	case types.EventReplayDropped:
		return !dualCluster || config.Replayer == nil
	case types.EventMirrorReplayDropped:
		return !config.mirrorTargetSet || config.MirrorReplayer == nil
	case types.EventSessionRefreshAttempt, types.EventSessionRefreshSuccess, types.EventSessionRefreshError:
		return !config.AutoRefresh.Enabled || config.SessionRefresher == nil
	}

	return false
}

// unreachableEventKinds returns the cluster event kinds that can never
// fire given the rest of the configuration, in docs order.
func unreachableEventKinds(config *ClientConfig, dualCluster bool) []string {
	var kinds []string
	for _, kind := range orderedClusterEventKinds {
		if eventKindUnreachable(kind, config, dualCluster) {
			kinds = append(kinds, string(kind))
		}
	}

	return kinds
}

// logUnreachableEventKinds emits one construction-time Info line listing
// the event kinds the registered handler can never receive, so a handler
// written for an unconfigured kind is discovered at startup rather than
// during an incident. No-op when every kind is reachable or no handler
// is registered. Follows the "dual-cluster mode with no Replayer" warning
// precedent: one concise line, not one per kind.
func (c *CQLClient) logUnreachableEventKinds(dualCluster bool) {
	if c.runtime.events == nil {
		return
	}
	config := c.config
	kinds := unreachableEventKinds(config, dualCluster)
	if len(kinds) == 0 {
		return
	}
	config.Logger.Info("cluster event handler registered but some kinds are unreachable with the current configuration - see docs/cluster-events.md for per-kind prerequisites",
		"unreachableKinds", strings.Join(kinds, ","),
	)
}

// startEventDelivery installs the emitter into the components that accept one
// and starts the dispatcher's delivery goroutine. No-op when no handler is
// registered.
//
// NewCQLClient calls this after its last step that can fail, so no error path
// leaks the dispatcher goroutine, and before the auto-refresh and
// recovery-probe goroutines start: a probe that succeeds on its first tick
// reports a recovery, and injecting the emitter afterwards would race that
// report and lose the event.
func (c *CQLClient) startEventDelivery() {
	if c.runtime.events == nil {
		return
	}

	c.autoInjectEventEmitter()
	c.runtime.events.start()
}

// autoInjectEventEmitter threads the client's event dispatcher into
// components that opt in by exposing a SetEventEmitter method — the same
// type-assertion pattern autoInjectMetricsAndLogger uses. Called after the
// constructor's last step that can fail and before any background goroutine
// starts, so a component never observes a half-installed emitter.
func (c *CQLClient) autoInjectEventEmitter() {
	config := c.config
	if config.WriteStrategy != nil {
		if ws, ok := config.WriteStrategy.(EventEmitterSetter); ok {
			ws.SetEventEmitter(c.runtime.events)
		}
	}
	if config.FailoverPolicy != nil {
		if fp, ok := config.FailoverPolicy.(EventEmitterSetter); ok {
			fp.SetEventEmitter(c.runtime.events)
		}
	}
}

// shouldLogOverrideErr returns true on the first occurrence and on every
// power-of-2 occurrence thereafter (1, 2, 4, 8, 16 …). This prevents a
// misconfigured AllowedClusters provider from flooding the log at high QPS
// while still ensuring the error is visible immediately and periodically.
func (c *CQLClient) shouldLogOverrideErr() bool {
	seq := c.overrideErrSeq.Add(1)
	return seq == 1 || seq&(seq-1) == 0
}

// emitClusterEvent forwards ev to the event dispatcher. Safe when no handler
// is registered: the dispatcher is then nil and every method no-ops.
func (c *CQLClient) emitClusterEvent(ev types.ClusterEvent) {
	c.runtime.events.EmitClusterEvent(ev)
}

// emitReplayDropped invokes the legacy OnReplayDropped callback, if any, and
// then emits an EventReplayDropped cluster event for a write that could not
// be enqueued for replay. Both write-path call sites that hit this failure
// share the same callback-then-event sequence.
func (c *CQLClient) emitReplayDropped(cluster ClusterID, payload types.ReplayPayload, enqueueErr error) {
	if c.config.OnReplayDropped != nil {
		c.config.OnReplayDropped(payload, enqueueErr)
	}
	c.emitClusterEvent(types.ClusterEvent{
		Kind:    types.EventReplayDropped,
		Cluster: cluster,
		Err:     enqueueErr,
	})
}

// abortEventDispatcher stops the event dispatcher created earlier in
// NewCQLClient on a constructor error path, so any event already buffered is
// counted as dropped instead of sitting undelivered forever. Safe to call on
// a never-started dispatcher.
func (c *CQLClient) abortEventDispatcher() {
	c.runtime.events.stop()
}

// NewCQLClient creates a new Helix CQL client.
//
// The client supports two modes:
//   - Single-cluster mode: Pass sessionB as nil. Operations are executed directly
//     on sessionA without dual-write or failover logic. This provides a drop-in
//     replacement for existing single-cluster applications.
//   - Dual-cluster mode: Pass both sessions. Operations use configured strategies
//     for dual writes, sticky reads, and failover.
//
// If a ReplayWorker is configured, it will be started automatically.
// The worker will be stopped when Close() is called.
//
// If a TopologyWatcher is configured, it will be started automatically to
// monitor drain mode signals. Writes to draining clusters are skipped and
// enqueued for replay. Reads are failed over away from draining clusters.
//
// Parameters:
//   - sessionA: CQL session for cluster A (required)
//   - sessionB: CQL session for cluster B (optional, nil for single-cluster mode)
//   - opts: Optional configuration options
//
// Returns:
//   - *CQLClient: A new CQL client
//   - error: [types.ErrNilSession] if sessionA is nil, joined
//     [types.OptionError] values for invalid options, sentinel errors such as
//     [types.ErrMirrorModeConflict], [types.ErrNilMirrorTarget], or
//     [types.ErrNilMirrorPublisher] for invalid mirror configuration, or an
//     error from starting configured background components such as replay
//     workers or topology watchers
func NewCQLClient(sessionA, sessionB cql.Session, opts ...Option) (*CQLClient, error) {
	client, err := buildCQLClient(sessionA, sessionB, opts...)
	if err != nil {
		return nil, err
	}

	return client, nil
}

// buildCQLClient is NewCQLClient without the final nil-on-error guard: when
// a step after the client value exists fails, the partially built client is
// returned together with the error so its already-stopped components can be
// inspected. Only NewCQLClient and this package's tests call it.
func buildCQLClient(sessionA, sessionB cql.Session, opts ...Option) (*CQLClient, error) {
	if sessionA == nil {
		return nil, types.ErrNilSession
	}

	config := DefaultConfig()
	for _, opt := range opts {
		opt(config)
	}

	// Ensure metrics is never nil
	if config.Metrics == nil {
		config.Metrics = metrics.NewNopMetrics()
	}

	// Ensure logger is never nil
	if config.Logger == nil {
		config.Logger = logging.NewNopLogger()
	}

	// Ensure timestamp provider is never nil
	if config.TimestampProvider == nil {
		config.TimestampProvider = DefaultTimestampProvider
	}

	// Ensure NowProvider is never nil — auto-refresh + stats helpers
	// dereference it on every recorded op outcome.
	if config.NowProvider == nil {
		config.NowProvider = DefaultNowProvider
	}

	if err := validateNewCQLClientConfig(config); err != nil {
		return nil, err
	}

	client := &CQLClient{
		config:        config,
		singleCluster: sessionB == nil,
	}
	client.storeSessionA(sessionA)
	// Store sessionB even if nil; in single-cluster mode the holder wraps a
	// nil cql.Session so loadSessionB() returns nil safely without a
	// nil-pointer-deref-risk on the holder pointer itself.
	client.storeSessionB(sessionB)

	// Create auto memory worker if configured. The auto-created worker
	// inherits the client's metrics collector by default so client and
	// worker metrics are unified — this prevents the gotcha where
	// worker-side IncReplaySuccess/Dropped/Error go into a separate
	// NopMetrics and are silently invisible to the client's collector.
	// Caller-supplied AutoMemoryWorkerOpts that include WithWorkerMetrics
	// override this default (last-option-wins).
	if config.AutoMemoryWorker {
		memReplayer := replay.NewMemoryReplayer(
			replay.WithQueueCapacity(config.AutoMemoryCapacity),
		)
		config.Replayer = memReplayer
		workerOpts := append(
			[]replay.WorkerOption{replay.WithWorkerMetrics(config.Metrics)},
			config.AutoMemoryWorkerOpts...,
		)
		worker, workerErr := replay.NewMemoryWorkerChecked(
			memReplayer,
			client.DefaultExecuteFunc(),
			workerOpts...,
		)
		if workerErr != nil {
			return nil, workerErr
		}
		config.ReplayWorker = worker
	}

	// Propagate cluster names to components that support it. This happens
	// after strict constructor validation, including auto-created worker
	// validation, so rejected configurations do not mutate caller-owned
	// components.
	propagateClusterNames(config)

	// Warn about missing Replayer in dual-cluster mode
	if sessionB != nil && config.Replayer == nil {
		config.Logger.Warn("dual-cluster mode with no Replayer configured - partial write failures will be lost and cannot be reconciled")
	}
	warnNoEffectOptions(config)
	warnReplayStreamDefaults(config.Logger, "replayer", config.Replayer)
	warnReplayStreamDefaults(config.Logger, "mirror replayer", config.MirrorReplayer)

	// Must run before mirror setup: the mirror error handler captures the
	// dispatcher by value when it is built below.
	client.createEventDispatcher()
	client.logUnreachableEventKinds(sessionB != nil)

	// Auto-inject client metrics/logger into components that opt in via
	// type-assertion-based interfaces (replay.Worker, AdaptiveDualWrite).
	// This keeps client and component instrumentation unified without
	// expanding the public ReplayWorker / WriteStrategy interfaces.
	autoInjectMetricsAndLogger(config)

	if err := client.setupMirror(); err != nil {
		client.abortEventDispatcher()

		return client, err
	}

	// Start topology watcher if configured. The cancel function is stashed so
	// that it is called on any subsequent initialization error, preventing the
	// watchTopology goroutine from leaking.
	if config.TopologyWatcher != nil {
		ctx, cancel := context.WithCancel(context.Background())
		client.topologyCtx = ctx
		client.topologyClose = cancel
		client.topologyWG.Go(client.watchTopology)
	}

	// Start replay worker if configured. On failure, clean up the topology
	// watcher and any mirror components setupMirror already started above,
	// following the same shutdown order as Close: topology watcher, then
	// mirror engine, then mirror replay worker.
	if config.ReplayWorker != nil {
		if err := config.ReplayWorker.Start(); err != nil {
			if client.topologyClose != nil {
				client.topologyClose()
			}
			client.stopMirrorComponents()
			client.abortEventDispatcher()

			return client, err
		}
	}

	// Must run after every constructor step above that can still return an
	// error (so no error path leaves a delivery goroutine running) and
	// before the auto-refresh and recovery-probe goroutines below start
	// (so a fast successful probe cannot race the emitter installation and
	// lose an event).
	client.startEventDelivery()

	// Start the auto-refresh detector if enabled AND a refresher is
	// registered. Without a refresher the detector cannot do anything
	// useful, so we skip the goroutine entirely.
	if config.AutoRefresh.Enabled && config.SessionRefresher != nil {
		ctx, cancel := context.WithCancel(context.Background())
		client.autoRefreshCtx = ctx
		client.autoRefreshClose = cancel
		client.autoRefreshWG.Go(client.autoRefreshLoop)
	}

	// Start background recovery probe goroutines when AdaptiveDualWrite is
	// detected and the probe has not been explicitly disabled. The probe
	// advances cluster recovery without requiring live dual-writes.
	client.startRecoveryProbes()

	return client, nil
}

// DefaultExecuteFunc returns an ExecuteFunc for use with replay workers.
//
// This is a convenience method that creates an executor which routes replay
// payloads to the appropriate cluster session. It handles both single queries
// and batch operations, preserving the original timestamp for idempotency.
//
// The returned function:
//   - Routes to sessionA or sessionB based on payload.TargetCluster
//   - Handles batch operations (IsBatch=true) with proper BatchType
//   - Preserves the original write timestamp for idempotent replays
//   - Respects context cancellation and timeouts via ExecContext
//
// Example:
//
//	client, _ := helix.NewCQLClient(sessionA, sessionB, helix.WithReplayer(replayer))
//
//	// Create worker with the default executor
//	worker := replay.NewMemoryWorker(replayer, client.DefaultExecuteFunc(),
//	    replay.WithOnSuccess(func(p types.ReplayPayload) {
//	        log.Printf("Replay succeeded for cluster %s", p.TargetCluster)
//	    }),
//	)
//
// Returns:
//   - replay.ExecuteFunc: A function that executes replay payloads
func (c *CQLClient) DefaultExecuteFunc() replay.ExecuteFunc {
	return func(ctx context.Context, payload types.ReplayPayload) error {
		if payload.TargetCluster != ClusterA && payload.TargetCluster != ClusterB {
			return fmt.Errorf("%w: replay target %q", types.ErrInvalidCluster, payload.TargetCluster)
		}
		session := c.getSession(payload.TargetCluster)

		if payload.IsBatch {
			batch := session.Batch(payload.BatchType)
			for _, stmt := range payload.BatchStatements {
				batch = batch.Query(stmt.Query, stmt.Args...)
			}

			return batch.WithTimestamp(payload.Timestamp).ExecContext(ctx)
		}

		return session.Query(payload.Query, payload.Args...).
			WithTimestamp(payload.Timestamp).
			ExecContext(ctx)
	}
}

// warnReplayStreamDefaults logs once per client when a NATS replayer keeps a
// stream default that silently narrows the replay recovery window: a single
// replica on file storage, or evicting the oldest unreplayed messages when
// the stream is full.
func warnReplayStreamDefaults(logger types.Logger, component string, replayer Replayer) {
	nr, ok := replayer.(*replay.NATSReplayer)
	if !ok {
		return
	}
	cfg := nr.Config()
	if cfg.Replicas == 1 {
		logger.Warn("replay stream has a single replica on file storage; losing that node's disk loses the whole replay backlog",
			"component", component,
			"stream", cfg.StreamName,
			"fix", "replay.WithReplicas(3)",
		)
	}
	if cfg.DiscardPolicy == jetstream.DiscardOld {
		logger.Warn("replay stream evicts the oldest unreplayed messages when MaxMsgs or MaxBytes is reached",
			"component", component,
			"stream", cfg.StreamName,
			"fix", "replay.WithRejectNewOnLimit()",
		)
	}
}
