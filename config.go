package helix

import (
	"context"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/mirror"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
)

// RecoveryProbe configures the background recovery probe that accelerates
// [policy.AdaptiveDualWrite] cluster recovery.
//
// When a cluster is degraded, the probe fires at each Interval and executes
// Probe against that cluster's live session. A nil (success) result credits
// one recovery point via [policy.AdaptiveDualWrite.RecordProbeSuccess]; the
// cluster returns to healthy state once it accumulates the strategy's
// recovery threshold of consecutive successes.
//
// The probe is intentionally lightweight: the default reads a single cell
// from system.local to verify the driver can reach the cluster, without
// write traffic or schema dependencies. Operators with write-path-specific
// failure modes may supply a custom Probe function (e.g., a test write to a
// dedicated probe table).
//
// Recovery probe is default-on for [policy.AdaptiveDualWrite] users. Use
// [WithRecoveryProbeDisabled] to opt out.
type RecoveryProbe struct {
	// Probe is the health check executed against a degraded cluster's live
	// session. Return nil to credit a recovery point; return non-nil to
	// leave the cluster in its current degraded state.
	//
	// The context is bound by Timeout. If nil, the default probe queries
	// SELECT release_version FROM system.local.
	Probe func(ctx context.Context, session cql.Session) error

	// Interval is the period between probe checks. Default: 2s.
	Interval time.Duration

	// Timeout bounds each individual probe call. Default: 1s.
	Timeout time.Duration
}

// DefaultRecoveryProbe returns a RecoveryProbe with production-safe defaults:
// a system.local read probe, 2-second interval, and 1-second per-probe timeout.
func DefaultRecoveryProbe() RecoveryProbe {
	return RecoveryProbe{
		Probe:    systemLocalProbe,
		Interval: 2 * time.Second,
		Timeout:  1 * time.Second,
	}
}

// systemLocalProbe is the default RecoveryProbe.Probe implementation: it
// reads release_version from system.local to verify the driver can reach the
// cluster, without write traffic or schema dependencies.
func systemLocalProbe(ctx context.Context, session cql.Session) error {
	var ver string
	return session.Query("SELECT release_version FROM system.local").ScanContext(ctx, &ver)
}

// TimestampProvider generates timestamps for write operations.
//
// The default provider uses time.Now().UnixMicro(). A provider must never
// return zero: [NewCQLClient] samples it once and rejects a zero result with
// a [types.OptionError], and a write whose timestamp is zero fails with
// [types.ErrInvalidTimestamp]. Timestamps decide last-write-wins across
// both clusters and across replays, so independent clients writing the same
// row rely on their clocks agreeing to within the spacing of their writes.
type TimestampProvider func() int64

// DefaultTimestampProvider returns the current time in microseconds.
func DefaultTimestampProvider() int64 {
	return time.Now().UnixMicro()
}

// AckMode selects when a dual-cluster write that no cluster acknowledged
// synchronously is reported as success.
//
// See [WithAckMode].
type AckMode int

const (
	// RequireSynchronousAck is the default: a write returns nil only when at
	// least one cluster acknowledged it before the call returned. A write
	// whose every leg was dispatched in the background, dropped, skipped,
	// or failed returns a [types.NoSynchronousAckError] even when it was
	// enqueued for replay.
	RequireSynchronousAck AckMode = iota

	// AckOnReplayAdmission returns nil for a write with no synchronous
	// acknowledgement as long as every leg that needed replay was enqueued.
	// The write then exists only in the replay queue until the worker
	// delivers it, so this mode is only sound with a durable replayer.
	// A leg still running in the background (see [DeferredWriteResult])
	// counts as admitted provisionally: nil is returned before that leg
	// completes, its failure is enqueued when it does, and an enqueue
	// failure at that point is reported only through [WithOnReplayDropped]
	// and [types.EventReplayDropped], never through the returned error.
	// Until the leg completes, the write exists only in that background
	// attempt and is lost if the process exits.
	// A replay enqueue that fails before the call returns is always an error.
	AckOnReplayAdmission
)

// ReplayDroppedHandler is called when a replay payload cannot be enqueued.
// This callback allows applications to handle potential data loss scenarios.
//
// Parameters:
//   - payload: The payload that could not be enqueued
//   - err: The error from the enqueue attempt
type ReplayDroppedHandler func(payload types.ReplayPayload, err error)

// ClientConfig holds configuration for Helix clients.
type ClientConfig struct {
	ReadStrategy      ReadStrategy
	WriteStrategy     WriteStrategy
	FailoverPolicy    FailoverPolicy
	Replayer          Replayer
	ReplayWorker      ReplayWorker
	TimestampProvider TimestampProvider
	TopologyWatcher   TopologyWatcher
	Metrics           MetricsCollector
	Logger            types.Logger
	ClusterNames      types.ClusterNames
	OnReplayDropped   ReplayDroppedHandler
	OnClusterEvent    ClusterEventHandler

	// AllowedClusters, when set, overrides automatic read routing with an
	// operator-controlled cluster list. This is the external control for
	// preventing reads from a recovering cluster whose replay backfill has
	// not yet completed.
	//
	// When the function returns a non-empty list, the read strategy's Select()
	// is NOT called — the list directly controls routing. The strategy's
	// internal state (OnSuccess/OnFailure) is frozen until the override is
	// removed.
	//
	// Iterator paths defer override errors to Close(). Always call Close()
	// and check its error.
	//
	// CAS operations (ScanCAS, MapScanCAS, batch ExecCAS) are not affected
	// by this override — they are write-like operations controlled by
	// ForceDegrade/ForceRecover.
	//
	// See [AllowedClustersFunc] for return-value semantics.
	AllowedClusters AllowedClustersFunc

	// DefaultFallbackRead enables FallbackRead for every eligible query on this
	// client when true. Equivalent to calling [Query.FallbackRead] on every query.
	// See [Query.FallbackRead] for the full list of eligible methods.
	//
	// FallbackRead is best-effort: if the alternative cluster is unreachable,
	// callers receive [ErrNotFound] (not the network error). See
	// [Query.FallbackRead] for full semantics.
	//
	// Default: false (opt-in per query or per context).
	DefaultFallbackRead bool

	// FallbackReadOnDrainingCluster lets a FallbackRead probe on Scan and
	// MapScan contact the alternative cluster while it is draining. Default
	// false: a draining alternative answers not-found without being asked.
	// Set via [WithFallbackReadOnDrainingCluster].
	FallbackReadOnDrainingCluster bool

	// DefaultMaxRows is the client-wide row cap for [Query.SliceMap] and
	// [Query.SliceScan] queries. A per-query [Query.MaxRows] override wins when
	// non-zero; calling MaxRows(0) on a query clears the override and falls back
	// to this default.
	//
	// When N > 0, each slice method aborts with [types.ErrRowLimitExceeded] upon
	// detecting the (N+1)th row and clamps the underlying query page size to N+1
	// to bound the driver's per-page network fetch.
	//
	// 0 means unbounded. Must be in [0, math.MaxInt32-1].
	//
	// Default: 0.
	DefaultMaxRows int

	// AckMode selects whether a write with no synchronous acknowledgement
	// may return nil. Default: RequireSynchronousAck. Set via [WithAckMode].
	AckMode AckMode

	// AutoMemoryWorker enables automatic in-process replay with MemoryReplayer.
	// When true, a MemoryReplayer and Worker are created automatically.
	AutoMemoryWorker bool

	// AutoMemoryCapacity is the queue capacity for auto-created MemoryReplayer.
	// Default: 10000
	AutoMemoryCapacity int

	// AutoMemoryWorkerOpts are options passed to the auto-created Worker.
	AutoMemoryWorkerOpts []replay.WorkerOption

	// NowProvider returns the current time as Unix nanoseconds. Defaults to
	// DefaultNowProvider (which calls time.Now().UnixNano()). Tests can
	// substitute a deterministic clock so the auto-refresh detector's
	// time-based conditions can be exercised without wall-clock dependence.
	//
	// Mirrors the [TimestampProvider] pattern above.
	NowProvider NowProvider

	// AutoRefresh configures the auto-refresh detector. When Enabled is
	// true AND a SessionRefresher is registered, a background goroutine
	// monitors per-cluster op outcomes and invokes [CQLClient.RefreshSession]
	// when a cluster's session is observed to be permanently dead.
	//
	// Defaults are conservative — see [DefaultAutoRefreshConfig]. Use
	// [WithAutoRefresh] to enable; tune individual knobs with the
	// AutoRefresh*Option helpers.
	AutoRefresh AutoRefreshConfig

	// MirrorTarget is the destination helix CQLClient that receives async
	// mirror writes for queries / batches opted in via [Query.Mirror] /
	// [Batch.Mirror]. When nil, Mirror() is a no-op. Set via [WithMirror].
	MirrorTarget *CQLClient

	// MirrorOptions configures the mirror engine constructed when
	// MirrorTarget is non-nil. Set via [WithMirror].
	MirrorOptions []mirror.Option

	// mirrorTargetSet tracks whether [WithMirror] was called. Used to
	// distinguish "user passed nil" from "option was never called" —
	// without it, WithMirror(nil) silently disables mirroring instead of
	// surfacing the bug.
	mirrorTargetSet bool

	// mirrorPublisherSet tracks whether [WithMirrorPublisher] was called.
	mirrorPublisherSet bool

	// MirrorPublisher is the replayer that captured mirror writes are
	// published to in publisher mode. Mutually exclusive with MirrorTarget.
	// Set via [WithMirrorPublisher].
	MirrorPublisher Replayer

	// MirrorReplayer holds failed mirror writes for durable retry. Set via
	// [WithMirrorReplayer].
	MirrorReplayer Replayer

	// MirrorReplayWorkerOpts configures the auto-built mirror replay worker.
	// Set via [WithMirrorReplayer].
	MirrorReplayWorkerOpts []replay.WorkerOption

	// SessionRefresher is an optional caller-supplied factory used by
	// [CQLClient.RefreshSession] to build a replacement [cql.Session] for a
	// cluster whose live session is broken (e.g., the cluster restarted at a
	// different endpoint and the existing session cannot reconnect).
	//
	// Helix is decoupled from any specific gocql driver version, so it cannot
	// build a session itself — only the caller knows whether to wrap a
	// gocql v1, gocql v2, chaos-injecting, or test-mock implementation. The
	// refresher receives the target ClusterID and the most recently observed
	// failure error (or nil if no op has failed yet) and returns a fresh
	// session.
	//
	// If unset, [CQLClient.RefreshSession] returns [types.ErrNoSessionRefresher].
	// The lower-level [CQLClient.SwapSession] does not require a refresher
	// because the caller passes the new session directly.
	SessionRefresher SessionRefresher

	// RecoveryProbe configures the background probe that runs against degraded
	// clusters to accelerate recovery for [policy.AdaptiveDualWrite]. When nil
	// and the write strategy is AdaptiveDualWrite, a default probe is used
	// automatically. When recoveryProbeOff is true, no probe is started.
	//
	// Set via [WithRecoveryProbe]; disable via [WithRecoveryProbeDisabled].
	RecoveryProbe *RecoveryProbe

	// recoveryProbeOff disables the recovery probe even when AdaptiveDualWrite
	// is detected. Set via [WithRecoveryProbeDisabled].
	recoveryProbeOff bool
}

// SessionRefresher builds a fresh [cql.Session] for the given cluster.
//
// Implementations are caller-provided and are responsible for choosing the
// concrete adapter (cqlv1.NewSession, cqlv2.NewSession, etc.) — Helix never
// imports a specific gocql driver and so cannot construct a session itself.
//
// lastErr is the most recently observed failure error against this cluster
// at the time the refresher is invoked (or nil if no failure has been
// recorded — typical for caller-driven RefreshSession invocations done
// before any op has failed). Refreshers may inspect it to tailor
// reconnection strategy to the observed failure mode (e.g., "no hosts
// available" implies a hard reachability issue while a timeout implies
// a slow but reachable cluster).
type SessionRefresher func(ctx context.Context, cluster ClusterID, lastErr error) (cql.Session, error)

// DefaultConfig returns a ClientConfig with sensible defaults.
//
// The default configuration provides minimal, non-nil infrastructure:
//   - TimestampProvider: Uses time.Now().UnixMicro() for idempotent writes
//   - Metrics: No-op collector (silent, no overhead)
//   - Logger: No-op logger (silent, no overhead)
//   - ClusterNames: "A" and "B" (see [types.DefaultClusterNames])
//
// Strategy and policy defaults (all nil):
//   - ReadStrategy: nil - Falls back to ClusterA only (no load balancing)
//   - WriteStrategy: nil - Concurrent dual-write to both clusters
//   - FailoverPolicy: nil - Always attempts failover on read failure
//   - Replayer: nil - CAUTION: Partial write failures will be lost!
//
// Production recommendations:
//   - ReadStrategy: policy.NewStickyRead() for cache-efficient reads
//   - WriteStrategy: policy.NewAdaptiveDualWrite() for latency-aware writes
//   - FailoverPolicy: policy.NewActiveFailover() for immediate failover
//   - Replayer: replay.NewNATSReplayer() for durable failure recovery
//
// A warning is logged during client creation if dual-cluster mode is used
// without a Replayer configured.
//
// Returns:
//   - *ClientConfig: Configuration with default settings
func DefaultConfig() *ClientConfig {
	return &ClientConfig{
		TimestampProvider: DefaultTimestampProvider,
		Metrics:           metrics.NewNopMetrics(),
		Logger:            logging.NewNopLogger(),
		ClusterNames:      types.DefaultClusterNames(),
	}
}

// Option configures a ClientConfig.
type Option func(*ClientConfig)

// WithReadStrategy sets the read routing strategy.
//
// Parameters:
//   - strategy: The read strategy to use (e.g., StickyRead)
//
// Returns:
//   - Option: Configuration option
func WithReadStrategy(strategy ReadStrategy) Option {
	return func(c *ClientConfig) {
		c.ReadStrategy = strategy
	}
}

// WithWriteStrategy sets the write execution strategy.
//
// Parameters:
//   - strategy: The write strategy to use (e.g., ConcurrentDualWrite)
//
// Returns:
//   - Option: Configuration option
func WithWriteStrategy(strategy WriteStrategy) Option {
	return func(c *ClientConfig) {
		c.WriteStrategy = strategy
	}
}

// WithFailoverPolicy sets the failover policy for reads.
//
// Parameters:
//   - policy: The failover policy to use
//
// Returns:
//   - Option: Configuration option
func WithFailoverPolicy(policy FailoverPolicy) Option {
	return func(c *ClientConfig) {
		c.FailoverPolicy = policy
	}
}

// WithSessionRefresher registers a factory used by [CQLClient.RefreshSession]
// to build a replacement [cql.Session] for a cluster whose live session has
// become permanently unrecoverable. Without this option, RefreshSession
// returns [types.ErrNoSessionRefresher]; the lower-level [CQLClient.SwapSession]
// remains available without it.
//
// The factory is invoked synchronously by RefreshSession on the calling
// goroutine; long-running connection establishment should respect the passed
// context.
//
// Parameters:
//   - fn: The session factory; receives the target cluster and the most
//     recently observed failure error against that cluster (nil if no op
//     has failed yet).
//
// Returns:
//   - Option: Configuration option
func WithSessionRefresher(fn SessionRefresher) Option {
	return func(c *ClientConfig) {
		c.SessionRefresher = fn
	}
}

// WithRecoveryProbe configures a custom recovery probe for [policy.AdaptiveDualWrite].
//
// The probe is called on each Interval against a degraded cluster's live session.
// A nil return credits one recovery point; a non-nil return leaves the cluster
// degraded. Zero Interval or Timeout values are replaced with the defaults from
// [DefaultRecoveryProbe]; negative values are invalid and cause [NewCQLClient]
// to return a [types.OptionError].
//
// When not set, a default probe (system.local read, 2s interval, 1s timeout) runs
// automatically for any client whose WriteStrategy is [policy.AdaptiveDualWrite].
// Use [WithRecoveryProbeDisabled] to suppress all probing.
//
// The probe only runs for a write strategy that reports degraded clusters
// (see [ProbeReporter]); with any other strategy this option has no effect
// and [NewCQLClient] logs a warning.
//
// Returns:
//   - Option: Configuration option
func WithRecoveryProbe(p RecoveryProbe) Option {
	def := DefaultRecoveryProbe()
	if p.Probe == nil {
		p.Probe = def.Probe
	}
	// Only the documented "unset" zero value is defaulted here; negative
	// values are left untouched so validateRootRecoveryProbe (called from
	// NewCQLClient) can reject them with a types.OptionError instead of
	// silently masking the misconfiguration.
	if p.Interval == 0 {
		p.Interval = def.Interval
	}
	if p.Timeout == 0 {
		p.Timeout = def.Timeout
	}

	return func(c *ClientConfig) {
		c.RecoveryProbe = &p
	}
}

// WithRecoveryProbeDisabled disables the background recovery probe even when
// [policy.AdaptiveDualWrite] is the configured write strategy. Use this when
// you prefer fully manual recovery via [policy.AdaptiveDualWrite.ForceRecover].
//
// Returns:
//   - Option: Configuration option
func WithRecoveryProbeDisabled() Option {
	return func(c *ClientConfig) {
		c.recoveryProbeOff = true
	}
}

// WithMirror configures the helix CQLClient to asynchronously mirror writes
// opted in via [Query.Mirror] / [Batch.Mirror] to a second helix dual-cluster
// pair represented by target.
//
// The target must be a fully constructed helix CQLClient pointing at the
// mirror destination (e.g., a new pair stood up for a migration). Mirror
// writes preserve the original client-generated timestamp so server-side
// WRITETIME semantics on the mirror cluster match the primary cluster.
//
// Mirroring is fire-and-forget: the mirror leg never surfaces an error to
// the caller. When the queue is full or the engine is disabled, captures
// are dropped (with metrics, logs, and an optional [mirror.WithOnDrop]
// callback). Mirror engine behavior is configured via mirror.Option values.
//
// The mirror engine is started during NewCQLClient and stopped during
// CQLClient.Close. Runtime control is available via CQLClient.Mirror()
// (Enable / Disable / Enabled / Stats).
//
// Example:
//
//	mirrorClient, _ := helix.NewCQLClient(newA, newB,
//	    helix.WithWriteStrategy(helix.ConcurrentDualWrite),
//	)
//	client, _ := helix.NewCQLClient(currentA, currentB,
//	    helix.WithWriteStrategy(helix.ConcurrentDualWrite),
//	    helix.WithMirror(mirrorClient,
//	        mirror.WithQueueSize(8192),
//	        mirror.WithWorkers(4),
//	    ),
//	)
//
// Parameters:
//   - target: The destination helix CQLClient.
//   - opts:   Mirror engine options.
//
// Returns:
//   - Option: Configuration option.
//
// Passing a nil target is rejected at construction with
// [types.ErrNilMirrorTarget] — see [WithMirrorPublisher] for the publisher
// equivalent.
func WithMirror(target *CQLClient, opts ...mirror.Option) Option {
	return func(c *ClientConfig) {
		c.mirrorTargetSet = true
		c.MirrorTarget = target
		c.MirrorOptions = opts
	}
}

// WithMirrorPublisher configures the helix CQLClient to mirror writes by
// publishing captures to a [Replayer] (typically a [replay.NATSReplayer])
// instead of writing them directly to a mirror destination from this
// process. The actual writes against the mirror clusters are performed by
// a separate consumer binary that runs a worker built via
// [NewMirrorWorker].
//
// This mode is the recommended production deployment for cluster
// migrations: the app does not depend on the mirror clusters' availability,
// captures are durable from the moment they are published (they survive
// app restart), and the consumer can be scaled and tuned independently.
//
// The captures pass through helix's bounded in-memory ring buffer (the
// same engine queue used in target mode) before reaching the publisher.
// When the buffer is full, captures are dropped per the engine's
// drop-on-full policy. When the publisher itself returns an error, the
// engine accounts the failure but does not retry — durability is the
// publisher's responsibility (NATS JetStream persistence, etc.).
//
// Engine knobs (queue size, worker count, drop callback) are configured
// via mirror.Option values passed as opts. Pair with [mirror.WithOnError]
// to observe publisher.Enqueue failures (e.g. NATS publish error rate);
// [WithOnReplayDropped] is not invoked for publisher errors — durability
// is the publisher's responsibility.
//
// Mutually exclusive with [WithMirror]. NewCQLClient returns
// [types.ErrMirrorModeConflict] if both are configured.
//
// Example (app side):
//
//	natsReplayer, _ := replay.NewNATSReplayer(ctx, replay.NATSReplayerConfig{ ... })
//	client, _ := helix.NewCQLClient(currentA, currentB,
//	    helix.WithMirrorPublisher(natsReplayer,
//	        mirror.WithQueueSize(8192),
//	        mirror.WithWorkers(4),
//	    ),
//	)
//	// session.Query(...).Mirror().ExecContext(ctx) — same opt-in as target mode.
//
// Example (consumer binary):
//
//	mirrorTarget, _ := helix.NewCQLClient(newA, newB)
//	worker, _ := helix.NewMirrorWorker(natsReplayer, mirrorTarget)
//	_ = worker.Start()
//	defer worker.Stop()
//	defer mirrorTarget.Close()
//
// Parameters:
//   - publisher: The replayer to publish captures to. Must not be nil.
//   - opts:      Mirror engine options.
//
// Returns:
//   - Option: Configuration option.
func WithMirrorPublisher(publisher Replayer, opts ...mirror.Option) Option {
	return func(c *ClientConfig) {
		c.mirrorPublisherSet = true
		c.MirrorPublisher = publisher
		c.MirrorOptions = opts
	}
}

// WithMirrorReplayer enables durable retry of failed mirror writes by
// pushing each error-returning capture onto a [Replayer] and (when the
// replayer's concrete type is recognized) automatically constructing and
// running a [ReplayWorker] that drains the replayer back into the mirror
// destination.
//
// Recognized replayer types (auto-worker):
//   - [replay.MemoryReplayer] — uses [replay.NewMemoryWorker]
//   - [replay.NATSReplayer]   — uses [replay.NewNATSWorker]
//
// For other replayer implementations, the engine still pushes failures to
// the replayer but no worker is auto-built; the application is expected to
// run its own worker. A warning is logged in that case.
//
// The auto-built worker reuses the same execute function the mirror engine
// uses, so timestamps, dual-write strategy, and per-cluster routing on the
// mirror destination are preserved on retry.
//
// workerOpts are applied after helix's metrics auto-injection so
// caller-supplied options win on conflict.
//
// If a mirror failure cannot be enqueued (queue full, transport down) the
// configured [WithOnReplayDropped] callback is invoked just like the
// primary replay path — set it once to alert / persist for both paths.
//
// Operational notes:
//   - With [replay.NATSReplayer] the per-failure Enqueue is bounded by
//     `NATSReplayerConfig.PublishTimeout`. A degraded NATS therefore
//     throttles the mirror engine's worker pool. Size
//     [mirror.WithWorkers] accordingly when migrating against unsteady
//     NATS infra.
//   - The auto-built [replay.MemoryWorker] uses the replay package's own
//     concurrency defaults, which are tuned for primary replay (one
//     pair). For migration the mirror destination is often newer and
//     less elastic — consider [replay.WithMaxAttempts] and lower
//     concurrency via the appropriate [replay.WorkerOption].
//
// Has no effect if [WithMirror] is not also configured; [NewCQLClient]
// logs a warning in that case.
//
// Parameters:
//   - replayer:   The durable store for failed mirror writes.
//   - workerOpts: Options applied to the auto-built worker (no effect for
//     unrecognized replayer types).
//
// Returns:
//   - Option: Configuration option.
func WithMirrorReplayer(replayer Replayer, workerOpts ...replay.WorkerOption) Option {
	return func(c *ClientConfig) {
		c.MirrorReplayer = replayer
		c.MirrorReplayWorkerOpts = workerOpts
	}
}

// WithAckMode selects when a write with no synchronous acknowledgement
// returns nil.
//
// With the default [RequireSynchronousAck], nil means at least one cluster
// acknowledged the write before the call returned; a write whose legs were
// all dispatched in the background, dropped, skipped, or failed returns a
// [types.NoSynchronousAckError] that names each leg's result and whether
// the write was enqueued for replay. [AckOnReplayAdmission] restores the
// previous behaviour of returning nil once every leg that needed replay
// was enqueued, or is still running in the background with its failure to
// be enqueued on completion; use it only with a durable replayer such as
// the NATS replayer, because the write then exists nowhere but in the
// queue or in that background attempt.
//
// Parameters:
//   - mode: The acknowledgement mode
//
// Returns:
//   - Option: Configuration option
//
// Example:
//
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithReplayer(natsReplayer),
//	    helix.WithAckMode(helix.AckOnReplayAdmission),
//	)
func WithAckMode(mode AckMode) Option {
	return func(c *ClientConfig) {
		c.AckMode = mode
	}
}

// WithReplayer sets the replayer for failed writes.
//
// Parameters:
//   - replayer: The replayer implementation
//
// Returns:
//   - Option: Configuration option
func WithReplayer(replayer Replayer) Option {
	return func(c *ClientConfig) {
		c.Replayer = replayer
	}
}

// WithReplayWorker sets the replay worker for processing failed writes.
//
// The worker will be started automatically when the client is created
// and stopped when the client is closed.
//
// Parameters:
//   - worker: The replay worker implementation (e.g., MemoryWorker, NATSWorker)
//
// Returns:
//   - Option: Configuration option
func WithReplayWorker(worker ReplayWorker) Option {
	return func(c *ClientConfig) {
		c.ReplayWorker = worker
	}
}

// WithOnReplayDropped sets a callback for when replay payloads cannot be enqueued.
//
// This callback is invoked when a write succeeds on one cluster but fails on the other,
// and the failed write cannot be enqueued for replay (e.g., queue is full).
// Use this to implement custom alerting or fallback persistence strategies.
//
// It covers both replay paths. With [WithMirror] and [WithMirrorReplayer]
// configured, a mirror capture that cannot be enqueued for mirror replay
// invokes this same callback — set it once to alert or persist for both. The
// signature does not distinguish them: mirror payloads carry a fixed
// conventional TargetCluster, so a handler that re-drives dropped payloads
// against this client's clusters would also re-drive mirror-destined ones.
// [types.EventMirrorReplayDropped] (see [WithOnClusterEvent]) is the signal
// that tells the two apart.
//
// The handler runs on the goroutine that failed to enqueue, which may be a
// write in progress that [CQLClient.Close] waits for; it must therefore
// never call Close itself. Trigger shutdown from another goroutine.
//
// Parameters:
//   - handler: Function called with the dropped payload and the enqueue error
//
// Returns:
//   - Option: Configuration option
//
// Example:
//
//	helix.WithOnReplayDropped(func(payload types.ReplayPayload, err error) {
//	    log.Error("replay queue full, potential data loss",
//	        "cluster", payload.TargetCluster,
//	        "query", payload.Query,
//	        "error", err,
//	    )
//	    // Optionally persist to a fallback store
//	    fallbackStore.Save(payload)
//	})
func WithOnReplayDropped(handler ReplayDroppedHandler) Option {
	return func(c *ClientConfig) {
		c.OnReplayDropped = handler
	}
}

// WithOnClusterEvent registers a handler for cluster-health events.
//
// The handler receives a [types.ClusterEvent] for operationally
// significant transitions: read failover, read divergence, circuit
// breaker open/close, adaptive-write degrade/recover, drain enter/exit,
// replay drops, mirror replay drops (unless a caller-supplied
// mirror.WithOnError overrides the internal handler), and session
// refresh attempts. Use it to drive alerting, paging, or operational
// dashboards without polling metrics.
//
// Registering the handler does NOT make every kind reachable. The emitter
// is installed into the configured WriteStrategy and FailoverPolicy, both
// nil by default, and most other kinds come from an optional component:
// circuit-breaker kinds need a [policy.CircuitBreaker] or
// [policy.LatencyCircuitBreaker] via [WithFailoverPolicy], adaptive-write
// kinds need [policy.AdaptiveDualWrite] via [WithWriteStrategy],
// EventReplayDropped needs a Replayer, drain kinds need a TopologyWatcher,
// and session-refresh kinds need [WithAutoRefresh] plus
// [WithSessionRefresher]. Registering a handler while some kinds are
// unreachable is not an error, but the constructor logs one Info line
// listing the unreachable kinds so the gap is visible at startup. See
// docs/cluster-events.md for the full table.
//
// Delivery is asynchronous and BEST-EFFORT on a dedicated goroutine:
// invocations never overlap and never block read/write operations. If
// the handler cannot keep up, newest events are dropped; drops are
// counted exactly, logged, and — when the configured collector implements
// [types.ClusterEventMetrics] — exposed as a counter
// (contrib/metrics/vm: {prefix}_cluster_events_dropped_total) so the
// application can alert on event loss. Circuit-breaker and adaptive-write
// events arrive in per-cluster transition order, per policy instance;
// events from independent producers arrive in enqueue order with no
// cross-kind causal guarantee. This is a notification stream, not a
// durable audit log — every kind has a metric counterpart, so prefer the
// metrics collector for rates and state and use the event as the push
// notification.
//
// Shutdown semantics: [CQLClient.Close] stops event intake, drains
// buffered events to the handler, and waits for the in-flight handler
// invocation to return. Consequently the handler MUST NOT call Close
// synchronously (deadlock) — trigger shutdown from another goroutine and
// return. Events emitted concurrently with Close (including terminal
// session-refresh or drain events from in-flight background work) may be
// dropped; every drop is counted, and totals are logged while the
// dispatcher runs plus once at shutdown — drops occurring after that
// final report are counted but not logged. Handler-panic recovery and
// the final drop report use the configured Logger, so a Logger that
// blocks can stall delivery and delay Close.
//
// Parameters:
//   - handler: Function called with each cluster event
//
// Returns:
//   - Option: Configuration option
//
// Example:
//
//	helix.WithOnClusterEvent(func(ev types.ClusterEvent) {
//	    switch ev.Kind {
//	    case types.EventWriteDegraded, types.EventCircuitBreakerOpen:
//	        alerting.Page("helix cluster issue",
//	            "kind", string(ev.Kind),
//	            "cluster", string(ev.Cluster),
//	            "reason", ev.Reason,
//	        )
//	    case types.EventReplayDropped, types.EventMirrorReplayDropped:
//	        alerting.Page("helix potential data loss", "error", ev.Err)
//	    }
//	})
func WithOnClusterEvent(handler ClusterEventHandler) Option {
	return func(c *ClientConfig) {
		c.OnClusterEvent = handler
	}
}

// WithAutoMemoryWorker enables automatic in-process replay with MemoryReplayer.
//
// This is a convenience option that creates both a MemoryReplayer and a Worker
// automatically, eliminating boilerplate setup code. The worker uses the client's
// DefaultExecuteFunc() to route replays to the correct cluster.
//
// Use this for:
//   - Development and testing environments
//   - Simple deployments where in-process replay is acceptable
//
// For production with durable replay, use WithReplayer() with NATSReplayer instead.
// NATS workers typically run as separate consumer services.
//
// This option owns both the replayer and the worker: combining it with
// [WithReplayer] or [WithReplayWorker] is rejected by [NewCQLClient] with a
// [types.OptionError].
//
// Parameters:
//   - queueCapacity: Maximum pending replays (0 uses default of 10000)
//   - workerOpts: Optional worker configuration (poll interval, callbacks, etc.)
//
// Returns:
//   - Option: Configuration option
//
// Example:
//
//	// Simple setup with defaults
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithAutoMemoryWorker(0),
//	)
//
//	// Custom capacity and callbacks
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithAutoMemoryWorker(50000,
//	        replay.WithPollInterval(50*time.Millisecond),
//	        replay.WithOnSuccess(func(p types.ReplayPayload) {
//	            log.Printf("Replay succeeded: cluster=%s", p.TargetCluster)
//	        }),
//	    ),
//	)
func WithAutoMemoryWorker(queueCapacity int, workerOpts ...replay.WorkerOption) Option {
	return func(c *ClientConfig) {
		c.AutoMemoryWorker = true
		if queueCapacity > 0 {
			c.AutoMemoryCapacity = queueCapacity
		} else {
			c.AutoMemoryCapacity = 10000 // default
		}
		c.AutoMemoryWorkerOpts = workerOpts
	}
}

// WithTimestampProvider sets the timestamp generator.
//
// Parameters:
//   - fn: Function that returns current timestamp in microseconds
//
// Returns:
//   - Option: Configuration option
func WithTimestampProvider(fn TimestampProvider) Option {
	return func(c *ClientConfig) {
		c.TimestampProvider = fn
	}
}

// WithTopologyWatcher sets the topology watcher for drain mode support.
//
// Parameters:
//   - watcher: The topology watcher implementation
//
// Returns:
//   - Option: Configuration option
func WithTopologyWatcher(watcher TopologyWatcher) Option {
	return func(c *ClientConfig) {
		c.TopologyWatcher = watcher
	}
}

// WithMetrics sets the metrics collector.
//
// If not set, a no-op collector is used that discards all metrics.
// Use contrib/metrics/vm.New() for VictoriaMetrics integration.
//
// Parameters:
//   - collector: The metrics collector implementation
//
// Returns:
//   - Option: Configuration option
//
// Example:
//
//	import vmmetrics "github.com/arloliu/helix/contrib/metrics/vm"
//
//	collector := vmmetrics.New(vmmetrics.WithPrefix("myapp"))
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithMetrics(collector),
//	)
func WithMetrics(collector MetricsCollector) Option {
	return func(c *ClientConfig) {
		c.Metrics = collector
	}
}

// WithLogger sets the structured logger.
//
// If not set, a no-op logger is used that discards all messages.
// The logger interface is compatible with zap.SugaredLogger.
//
// Parameters:
//   - logger: The logger implementation
//
// Returns:
//   - Option: Configuration option
//
// Example:
//
//	logger, _ := zap.NewProduction()
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithLogger(logger.Sugar()),
//	)
func WithLogger(logger types.Logger) Option {
	return func(c *ClientConfig) {
		c.Logger = logger
	}
}

// WithClusterNames sets custom display names for the clusters.
//
// These names are used in metrics labels and log messages instead of the
// default "A" and "B". Names must be Prometheus-compatible (alphanumeric
// with underscores, starting with letter or underscore, max 32 chars).
//
// If not set, defaults to "A" and "B".
//
// Parameters:
//   - nameA: Display name for cluster A (e.g., "us_east", "primary", "dc1")
//   - nameB: Display name for cluster B (e.g., "us_west", "secondary", "dc2")
//
// Returns:
//   - Option: Configuration option
//
// Example:
//
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithClusterNames("us_east", "us_west"),
//	)
//
// This will produce metrics like:
//
//	helix_read_total{cluster="us_east"} 100
//	helix_read_total{cluster="us_west"} 95
func WithClusterNames(nameA, nameB string) Option {
	return func(c *ClientConfig) {
		c.ClusterNames = types.ClusterNames{A: nameA, B: nameB}
	}
}

// WithAllowedClusters sets an operator-driven function that controls which
// clusters are eligible for reads.
//
// Scope: READS ONLY. Dual-writes (Exec/ExecContext, batch Exec) and CAS
// operations (ScanCAS, MapScanCAS, batch ExecCAS/MapExecCAS) are not
// affected — writes always go to both clusters per the configured
// WriteStrategy, and CAS is single-cluster controlled by ForceDegrade /
// ForceRecover on the write side. To fence a cluster from writes, drain
// it via the [TopologyWatcher] / [TopologyOperator] — drain skips writes
// to the affected cluster and enqueues them for replay.
//
// When the function returns a non-empty list, the read strategy is bypassed
// and the list directly controls read routing. The first element is the
// primary read target; subsequent elements are failover candidates in order.
// The read strategy's internal state (OnSuccess/OnFailure) is frozen during
// the override so it resumes cleanly when the override is removed.
//
// When the function returns nil or an empty slice, normal strategy + drain
// behavior applies.
//
// Fail-closed: if the list contains only unknown cluster IDs, or all valid
// clusters are draining, the read returns an error (ErrInvalidClusterOverride
// or ErrNoValidClusters). A panicking function returns ErrClusterOverridePanic.
// This prevents silent misrouting from stale or misconfigured flags.
//
// Iterator paths (IterContext) defer override errors to Close(). Always call
// Close() and check its error.
//
// Parameters:
//   - fn: Function returning the ordered list of allowed clusters for reads
//
// Returns:
//   - Option: Configuration option
//
// Example:
//
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithReadStrategy(policy.NewStickyRead(...)),
//	    helix.WithAllowedClusters(func() []helix.ClusterID {
//	        if featureFlag.IsClusterExcluded("A") {
//	            return []helix.ClusterID{helix.ClusterB}
//	        }
//	        return nil // all allowed, normal strategy behavior
//	    }),
//	)
func WithAllowedClusters(fn AllowedClustersFunc) Option {
	return func(c *ClientConfig) {
		c.AllowedClusters = fn
	}
}

// WithFallbackReadOnDrainingCluster lets a FallbackRead probe read from a
// draining cluster.
//
// By default a FallbackRead probe skips the alternative cluster while it is
// draining and returns not-found: drain is the operator's signal that the
// cluster should not serve reads, typically because it is being backfilled
// or repaired and may return stale rows. Enable this only when a draining
// cluster is known to hold current data and a false not-found is worse than
// a stale row. It applies to Scan and MapScan; SliceMap and SliceScan never
// read a draining alternative because a multi-row result could be partial.
//
// Parameters:
//   - enabled: true to contact a draining alternative
//
// Returns:
//   - Option: Configuration option
func WithFallbackReadOnDrainingCluster(enabled bool) Option {
	return func(c *ClientConfig) {
		c.FallbackReadOnDrainingCluster = enabled
	}
}

// WithDefaultFallbackRead enables FallbackRead for all eligible queries on this
// client when enabled is true. See [Query.FallbackRead] for the full list of
// eligible methods and per-method semantics.
//
// When true, every query executed on this client behaves as if [Query.FallbackRead]
// was called on it: a not-found result on the selected cluster triggers a silent
// best-effort attempt on the other cluster.
//
// Important: FallbackRead is best-effort. If the alternative cluster is
// unreachable, the caller receives [ErrNotFound] (the healthy cluster's answer),
// not the network error. Health metrics and failure tracking are still recorded
// on the unreachable cluster internally. See [Query.FallbackRead] for details.
//
// Use this when the majority of reads on this client are critical read-after-write
// operations. For mixed workloads (e.g., critical user data + bulk time-series),
// use per-query [Query.FallbackRead] or context-level [WithFallbackRead] instead,
// or create separate clients with different configurations.
//
// Default: false (opt-in per query or per context).
//
// Parameters:
//   - enabled: Whether to enable FallbackRead for all queries on this client
//
// Returns:
//   - Option: Configuration option
func WithDefaultFallbackRead(enabled bool) Option {
	return func(c *ClientConfig) {
		c.DefaultFallbackRead = enabled
	}
}

// WithDefaultMaxRows sets the client-wide row cap for [Query.SliceMap] and
// [Query.SliceScan].
//
// When n > 0, each slice method aborts with [types.ErrRowLimitExceeded] upon
// detecting the (n+1)th row and clamps the underlying query page size to n+1
// to bound the driver's per-page network fetch.
//
// A per-query [Query.MaxRows] override wins when non-zero; calling MaxRows(0)
// on a query clears the per-query override and falls back to this default.
//
// n must be in [0, math.MaxInt32-1]. Negative values and values ≥ math.MaxInt32
// are rejected with an option error from [NewCQLClient].
//
// Default: 0 (unbounded).
//
// Parameters:
//   - n: Row cap applied to all slice queries on this client. 0 means unbounded.
//
// Returns:
//   - Option: Configuration option
func WithDefaultMaxRows(n int) Option {
	return func(c *ClientConfig) {
		c.DefaultMaxRows = n
	}
}

// propagateClusterNames sets cluster names on components that implement ClusterNamer.
//
// This function is called during client initialization to propagate cluster names
// configured via WithClusterNames to all components that support custom naming.
func propagateClusterNames(c *ClientConfig) {
	names := c.ClusterNames

	// Propagate to metrics collector
	if namer, ok := c.Metrics.(types.ClusterNamer); ok {
		namer.SetClusterNames(names)
	}

	// Propagate to failover policy
	if namer, ok := c.FailoverPolicy.(types.ClusterNamer); ok {
		namer.SetClusterNames(names)
	}

	// Propagate to replay worker
	if namer, ok := c.ReplayWorker.(types.ClusterNamer); ok {
		namer.SetClusterNames(names)
	}

	// Propagate to write strategy
	if namer, ok := c.WriteStrategy.(types.ClusterNamer); ok {
		namer.SetClusterNames(names)
	}

	// Propagate to read strategy
	if namer, ok := c.ReadStrategy.(types.ClusterNamer); ok {
		namer.SetClusterNames(names)
	}
}

// AutoRefreshConfig configures the auto-refresh detector for the
// [CQLClient]. Auto-refresh is opt-in via [WithAutoRefresh] and requires
// a [SessionRefresher] registered via [WithSessionRefresher].
//
// The detector runs as a background goroutine that ticks every
// CheckInterval and inspects per-cluster op-outcome stats. A cluster is
// considered "session permanently dead" when ALL of:
//
//   - consecutiveFailures >= FailureThreshold
//   - now - lastSuccess >= SustainedFailureWindow
//   - now - lastRefresh >= MinRetryInterval (throttle)
//
// On match, the detector invokes [CQLClient.RefreshSession] with a
// RefreshTimeout-bound context. RefreshTimeout caps the wall-clock
// budget for a misbehaving refresher.
//
// Defaults are intentionally conservative: refresh storms (an aggressive
// detector firing repeatedly) are operationally far worse than slow
// recovery. Use [WithAutoRefresh] with no per-knob options to get them.
type AutoRefreshConfig struct {
	// Enabled gates the entire detector. False by default — auto-refresh
	// is strictly opt-in. Set automatically by [WithAutoRefresh].
	Enabled bool

	// FailureThreshold is the consecutive-failures count that must be
	// reached before the detector considers refresh. Default: 10.
	FailureThreshold int

	// SustainedFailureWindow is the minimum duration since the most
	// recent successful op for the detector to fire. Punctuating
	// failures with even one success resets this. Default: 5 minutes.
	SustainedFailureWindow time.Duration

	// MinRetryInterval is the minimum duration between successive
	// refresh attempts on the same cluster. Bounds the rate at which
	// auto-refresh can fire even under sustained failure with a
	// failing refresher. Default: 1 minute.
	MinRetryInterval time.Duration

	// CheckInterval is the period at which the detector goroutine
	// evaluates per-cluster state. Detector lag is bounded by this.
	// Default: 30 seconds.
	CheckInterval time.Duration

	// RefreshTimeout is the per-call timeout context applied around
	// [CQLClient.RefreshSession] when the detector fires. The refresher
	// should respect this deadline. Default: 30 seconds.
	RefreshTimeout time.Duration
}

// DefaultAutoRefreshConfig returns the production defaults applied by
// [WithAutoRefresh] when called with no per-knob options. Exposed as a
// symbol so callers can construct an AutoRefreshConfig literal with
// these defaults and then mutate select fields.
func DefaultAutoRefreshConfig() AutoRefreshConfig {
	return AutoRefreshConfig{
		Enabled:                true,
		FailureThreshold:       10,
		SustainedFailureWindow: 5 * time.Minute,
		MinRetryInterval:       1 * time.Minute,
		CheckInterval:          30 * time.Second,
		RefreshTimeout:         30 * time.Second,
	}
}

// AutoRefreshOption is a per-knob configurator for [WithAutoRefresh].
type AutoRefreshOption func(*AutoRefreshConfig)

// WithAutoRefresh enables the auto-refresh detector with conservative
// defaults (see [DefaultAutoRefreshConfig]). A registered
// [SessionRefresher] is required for the detector to do anything;
// without one, the detector silently no-ops.
//
// Per-knob tuning:
//
//	helix.WithAutoRefresh(
//	    helix.WithAutoRefreshFailureThreshold(20),
//	    helix.WithAutoRefreshSustainedFailureWindow(2*time.Minute),
//	)
//
// Returns:
//   - Option: Configuration option
func WithAutoRefresh(opts ...AutoRefreshOption) Option {
	return func(c *ClientConfig) {
		c.AutoRefresh = DefaultAutoRefreshConfig()
		for _, opt := range opts {
			opt(&c.AutoRefresh)
		}
	}
}

// WithAutoRefreshFailureThreshold overrides AutoRefreshConfig.FailureThreshold.
func WithAutoRefreshFailureThreshold(n int) AutoRefreshOption {
	return func(c *AutoRefreshConfig) { c.FailureThreshold = n }
}

// WithAutoRefreshSustainedFailureWindow overrides AutoRefreshConfig.SustainedFailureWindow.
func WithAutoRefreshSustainedFailureWindow(d time.Duration) AutoRefreshOption {
	return func(c *AutoRefreshConfig) { c.SustainedFailureWindow = d }
}

// WithAutoRefreshMinRetryInterval overrides AutoRefreshConfig.MinRetryInterval.
func WithAutoRefreshMinRetryInterval(d time.Duration) AutoRefreshOption {
	return func(c *AutoRefreshConfig) { c.MinRetryInterval = d }
}

// WithAutoRefreshCheckInterval overrides AutoRefreshConfig.CheckInterval.
func WithAutoRefreshCheckInterval(d time.Duration) AutoRefreshOption {
	return func(c *AutoRefreshConfig) { c.CheckInterval = d }
}

// WithAutoRefreshRefreshTimeout overrides AutoRefreshConfig.RefreshTimeout.
func WithAutoRefreshRefreshTimeout(d time.Duration) AutoRefreshOption {
	return func(c *AutoRefreshConfig) { c.RefreshTimeout = d }
}
