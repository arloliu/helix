package helix

import (
	"context"
	"errors"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/mirror"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
)

// RecoveryProbe configures the background recovery probe that accelerates
// [policy.AdaptiveDualWrite] cluster recovery and closes an open
// [policy.CircuitBreaker] or [policy.LatencyCircuitBreaker].
//
// The probe fires at each Interval. A nil result means the cluster was
// reachable; each authority then decides what that is worth. While the
// write strategy reports a cluster degraded, the probe's latency goes to
// [policy.AdaptiveDualWrite.RecordProbeLatency] (or a nil result to
// RecordProbeSuccess for a strategy without latency judgement), and the
// strategy credits a recovery point only if it judges the probe fast
// enough; the cluster returns to healthy state once it accumulates the
// strategy's recovery threshold. While the failover policy has an open
// breaker whose reset timeout has elapsed, the same probe reserves the
// breaker (half-open) and a nil result closes it while an error re-opens
// it, so no caller's read is sacrificed to test the cluster. One tick runs
// at most one probe per cluster for both.
//
// The probe is intentionally lightweight: the default reads a single cell
// from system.local to verify the driver can reach the cluster, without
// write traffic or schema dependencies. Operators with write-path-specific
// failure modes may supply a custom Probe function (e.g., a test write to a
// dedicated probe table).
//
// The recovery probe is default-on whenever either authority is
// configured. Use [WithRecoveryProbeDisabled] to opt out.
type RecoveryProbe struct {
	// Probe is the health check executed against the live session of a
	// cluster an authority asks about. Return nil to report the cluster
	// reachable; return non-nil to leave it degraded or its breaker open.
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

// BehaviorProfile selects a set of defaults for the options whose v1 default
// is kept for compatibility. See [WithBehaviorProfile].
type BehaviorProfile int

const (
	// Legacy keeps every v1 default. It is the default profile.
	Legacy BehaviorProfile = iota

	// Safe selects the defaults a future major version will adopt for the
	// options the client itself owns: [WithRouteVeto] on. Options owned by
	// a policy or replayer constructor keep their own settings; the client
	// logs a startup warning naming the option to change for each one it
	// can observe (WithFailoverBelowThreshold, WithLatencyFailoverBelowThreshold,
	// and the replay stream settings).
	Safe
)

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

	// RouteVeto lets a failover policy that implements [RouteVeto] steer
	// ordinary reads away from a cluster. See [WithRouteVeto].
	//
	// Default: false.
	RouteVeto bool

	// ReplayGate is the operator's replay control: replay to a cluster
	// runs only while it returns true (and the cluster is not draining).
	// nil permits every cluster. See [WithReplayGate].
	ReplayGate func(cluster ClusterID) bool

	// ClusterWriteTimeout bounds each cluster's leg of a dual write
	// independently of the caller's context. See [WithClusterWriteTimeout].
	//
	// 0 disables the per-leg timeout. Must be >= 0.
	//
	// Default: 0.
	ClusterWriteTimeout time.Duration

	// ClusterReadTimeout bounds each cluster's leg of a read independently
	// of the caller's context. See [WithClusterReadTimeout].
	//
	// 0 disables the per-leg timeout. Must be >= 0.
	//
	// Default: 0.
	ClusterReadTimeout time.Duration

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

	// RecoveryProbe configures the background probe that serves a write
	// strategy reporting degraded clusters ([ProbeReporter]) and a failover
	// policy reserving its open breaker ([FailoverProbeReporter]). When nil
	// and either authority is configured, a default probe is used
	// automatically. When recoveryProbeOff is true, no probe is started.
	//
	// Set via [WithRecoveryProbe]; disable via [WithRecoveryProbeDisabled].
	RecoveryProbe *RecoveryProbe

	// recoveryProbeOff disables the recovery probe even when an authority
	// would use it. Set via [WithRecoveryProbeDisabled].
	recoveryProbeOff bool

	// profile is the [BehaviorProfile] selected via [WithBehaviorProfile];
	// kept so validation can reject an unknown value.
	profile BehaviorProfile
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

// WithBehaviorProfile applies a [BehaviorProfile]: a named set of values
// for the client-owned options whose v1 default is kept only for
// compatibility.
//
// It is pure option expansion: [Safe] is exactly WithRouteVeto(true) and
// [Legacy] exactly WithRouteVeto(false), so the last profile or option in
// the same NewCQLClient call wins. It adds no behaviour of its own.
//
// Parameters:
//   - profile: [Legacy] (the default) or [Safe]
//
// Returns:
//   - Option: Configuration option
//
// Example:
//
//	client, err := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithBehaviorProfile(helix.Safe),
//	    helix.WithFailoverPolicy(policy.NewLatencyCircuitBreaker(
//	        policy.WithLatencyFailoverBelowThreshold(true),
//	    )),
//	)
func WithBehaviorProfile(profile BehaviorProfile) Option {
	return func(c *ClientConfig) {
		c.profile = profile
		switch profile {
		case Safe:
			c.RouteVeto = true
		case Legacy:
			c.RouteVeto = false
		default:
			// Rejected by NewCQLClient; leave the knobs untouched.
		}
	}
}

// WithReplayGate installs the operator's replay control: replay to a
// cluster executes only while the predicate returns true for it.
//
// The client composes the predicate with drain (a draining cluster never
// receives replay) and installs the result as the cluster gate of the
// replay worker it builds for [WithAutoMemoryWorker]: queued payloads for a
// gated cluster stay queued without consuming attempts or their retry
// window, and execution resumes within the worker's poll interval once the
// gate opens. Use it to quarantine a cluster during a repair or a schema
// change, or to hold replay back until a returning cluster is ready for
// writes. Mirror workers are never gated by the source client, and a
// worker supplied through [WithReplayWorker] must carry its own
// [replay.WithClusterGate]; the client logs a startup warning in that case.
//
// The predicate runs before every replay attempt, so it must be cheap,
// non-blocking, and safe for concurrent use. A predicate that panics
// counts as closed. As with any other option, a later WithReplayGate in
// the same call replaces an earlier one.
//
// Parameters:
//   - allow: Returns true when replay to the cluster may execute; nil
//     permits every cluster
//
// Returns:
//   - Option: Configuration option
//
// Example:
//
//	var quarantined atomic.Bool
//	client, err := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithAutoMemoryWorker(10000),
//	    helix.WithReplayGate(func(c helix.ClusterID) bool {
//	        return c != helix.ClusterA || !quarantined.Load()
//	    }),
//	)
func WithReplayGate(allow func(cluster ClusterID) bool) Option {
	return func(c *ClientConfig) {
		c.ReplayGate = allow
	}
}

// WithRouteVeto lets a failover policy that implements [RouteVeto] steer
// ordinary reads away from a cluster whose breaker is open.
//
// Without it a [policy.LatencyCircuitBreaker] that has opened on a slow
// cluster only decides whether a failed read retries on the other cluster;
// every new read is still sent to the slow cluster first. With it, the
// client consults the policy after the read strategy's selection and moves
// an ordinary read to the other cluster while the selected one is vetoed
// (see [RouteVeto] for the exact precedence). Off by default in v1; a
// client whose failover policy implements RouteVeto logs a startup Warn
// while the option is off.
//
// Parameters:
//   - enabled: true to consult the policy's veto on ordinary reads
//
// Returns:
//   - Option: Configuration option
//
// Example:
//
//	client, err := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithFailoverPolicy(policy.NewLatencyCircuitBreaker()),
//	    helix.WithRouteVeto(true),
//	)
func WithRouteVeto(enabled bool) Option {
	return func(c *ClientConfig) {
		c.RouteVeto = enabled
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

// WithRecoveryProbe configures the background recovery probe.
//
// One probe per cluster serves two authorities: a write strategy that
// reports degraded clusters (see [ProbeReporter], [policy.AdaptiveDualWrite])
// and a failover policy that reserves its open breaker for a probe (see
// [FailoverProbeReporter], [policy.CircuitBreaker] and
// [policy.LatencyCircuitBreaker]). On each Interval the probe runs against
// the live session of a cluster either authority asks about. A nil return
// reports the cluster reachable: it closes a reserved breaker, and the
// write strategy credits a recovery point if it judges the probe fast
// enough (see [RecoveryProbe]). A non-nil return leaves the cluster
// degraded and the breaker open. Zero Interval
// or Timeout values are replaced with the defaults from
// [DefaultRecoveryProbe]; negative values are invalid and cause
// [NewCQLClient] to return a [types.OptionError].
//
// When not set, a default probe (system.local read, 2s interval, 1s timeout)
// runs automatically whenever either authority is configured. Use
// [WithRecoveryProbeDisabled] to suppress all probing.
//
// With neither a probe-reporting write strategy nor a probe-reporting
// failover policy, this option has no effect.
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

// WithRecoveryProbeDisabled disables the background recovery probe for both
// authorities it serves. A degraded [policy.AdaptiveDualWrite] then recovers
// only through fast background writes or [policy.AdaptiveDualWrite.ForceRecover],
// and an open [policy.CircuitBreaker] closes only when an ordinary operation
// against the cluster succeeds; with route veto on, reads avoid that cluster
// until then.
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
//   - The auto-built [replay.Worker] uses the replay package's own
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
// If not set, a no-op logger is used that discards all messages, so every
// startup warning, circuit breaker transition and replay drop is silent.
//
// [types.Logger] wants methods of the form
// Debug(msg string, keysAndValues ...any).
// *log/slog.Logger matches that for every level except Fatal, which the
// bundled contrib/log/slog adapter supplies.
// *zap.SugaredLogger does not match: its Debug takes Debug(args ...any),
// so a zap user wraps it and forwards to Debugw and the other "w"
// methods. See [types.Logger] for the wrapper.
//
// Parameters:
//   - logger: The logger implementation
//
// Returns:
//   - Option: Configuration option
//
// Example:
//
//	import (
//	    "log/slog"
//
//	    helixslog "github.com/arloliu/helix/contrib/log/slog"
//	)
//
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithLogger(helixslog.New(slog.Default())),
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

// ExcludeWhileReplayBacklog returns an [AllowedClustersFunc] that keeps
// reads away from a cluster while its replay backlog exceeds threshold.
//
// A cluster that has just returned from an outage answers queries before
// the replay worker has finished writing the backlog it missed, so a read
// strategy that recovers on responsiveness alone can serve stale rows. This
// helper excludes a cluster from reads while depth reports more than
// threshold pending payloads for it, and lets normal routing resume once
// the backlog has drained. When both clusters are over the threshold, or
// neither is, it returns nil and the read strategy routes as usual.
//
// depth runs on every read, so it must be cheap and non-blocking.
// [replay.MemoryReplayer.PendingByCluster] can be passed directly.
// [replay.NATSReplayer.PendingByCluster] queries JetStream, so sample it
// from a background goroutine into per-cluster atomics and pass a function
// that reads the atomics.
//
// Parameters:
//   - depth: Returns the number of replay payloads pending for a cluster
//   - threshold: Backlog size above which the cluster is excluded from reads
//
// Returns:
//   - AllowedClustersFunc: For use with [WithAllowedClusters]
//
// Example:
//
//	replayer := replay.NewMemoryReplayer()
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithReplayer(replayer),
//	    helix.WithAllowedClusters(helix.ExcludeWhileReplayBacklog(replayer.PendingByCluster, 100)),
//	)
func ExcludeWhileReplayBacklog(depth func(cluster ClusterID) int, threshold int) AllowedClustersFunc {
	return func() []ClusterID {
		overA := depth(ClusterA) > threshold
		overB := depth(ClusterB) > threshold
		switch {
		case overA && !overB:
			return []ClusterID{ClusterB}
		case overB && !overA:
			return []ClusterID{ClusterA}
		default:
			return nil
		}
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

// WithClusterWriteTimeout bounds each cluster's leg of a dual write.
//
// Without it a write waits for its slowest leg: a cluster that accepts
// connections but answers slowly holds every caller for as long as the
// caller's own deadline allows, and a strategy that writes the clusters in
// sequence may never reach the second one. With it, each leg runs under its
// own deadline of d. A leg that expires counts as that cluster's failure:
// it is replayed like any other failed leg, the other leg's acknowledgement
// stands, and the expiry is reported as [types.ErrClusterTimeout] — a health
// signal for the slow cluster because the deadline is Helix's own, not the
// caller's. A failure observed after the caller's context ended is still
// attributed to the caller.
//
// The timeout applies to the normal and strict dual-write legs, including
// the background legs a degraded [policy.AdaptiveDualWrite] dispatches.
// A background leg is replayed only when it reports a failure — the
// expiry of its deadline included: a strategy that returns
// [types.ErrWriteAsync] through [DeferredWriteResult] defers the decision,
// so a background leg that later succeeds is never enqueued for replay and
// its statement is not applied a second time.
// It does not apply to single-cluster writes issued through the client's own
// API, nor to reads or mirror writes.
// It does apply to each replay attempt a worker built on
// [CQLClient.DefaultExecuteFunc] makes, which is a cluster leg of the same
// write.
// A replay attempt runs outside the client's health accounting, so its
// expiry reaches the worker's replay classifier as [types.ErrClusterTimeout]
// and records no cluster failure.
//
// The deadline bounds the leg the driver honours it on: a leg waiting for
// an acknowledgement on a live connection ends at d, but a leg whose
// driver has fallen back to re-establishing the connection can return
// later, on the driver's own request timeout, because cancelling the leg
// does not interrupt that path. Give callers a deadline that outlives the
// driver's request timeout as well, or such a leg finishes after the
// caller's context ended and is attributed to the caller instead of the
// cluster — it then records no health failure, so a strategy that degrades
// on failures never sees it. Outliving it is enough for the strategies
// that run the legs together; [policy.SyncDualWrite] runs them one after
// another and skips the second once the context has ended, so it needs
// about r+d, where r is the driver's request timeout — the second leg
// still needs its own d after the first has spent r.
//
// Parameters:
//   - d: Per-leg deadline. 0 disables the timeout. Negative values are
//     rejected by NewCQLClient.
//
// Returns:
//   - Option: Configuration option
//
// Example:
//
//	client, err := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithClusterWriteTimeout(2*time.Second),
//	    helix.WithReplayer(replayer),
//	)
func WithClusterWriteTimeout(d time.Duration) Option {
	return func(c *ClientConfig) {
		c.ClusterWriteTimeout = d
	}
}

// WithClusterReadTimeout bounds each cluster's leg of a read.
//
// Without it a leg runs for as long as the caller's own deadline allows,
// and how soon a slow cluster is given up on is decided by the driver's
// connection-level timeout rather than by Helix. A driver that lets a
// caller's deadline override that timeout leaves the first leg free to
// consume the whole request budget, so failover never reaches the second
// cluster. With d set, each leg runs under its own deadline of d: a leg
// that expires counts as that cluster's failure, is a health signal for
// it, and leaves the rest of the caller's budget for the alternative.
//
// The deadline is Helix's own, so an expiry is reported as
// [types.ErrClusterTimeout]. A failure observed after the caller's context
// ended is still attributed to the caller.
//
// The deadline bounds the leg the driver honours it on: a leg waiting for
// an answer on a live connection ends at d, but a leg whose driver has
// fallen back to re-establishing the connection can return later, on the
// driver's own request timeout, because cancelling the leg does not
// interrupt that path. Give callers a deadline that outlives the driver's
// request timeout as well, or such a leg finishes after the caller's
// context ended and is attributed to the caller instead of the cluster.
//
// The timeout applies to every cluster leg of a read — the selected
// cluster, the failover attempt, and the FallbackRead probe — for both
// single-row and slice reads.
// It does not apply to single-cluster reads, where there is no
// alternative to preserve budget for, nor to writes.
//
// An iterator's first page is a leg like any other: the page
// [Query.IterContext] fetches before it hands the iterator over is bounded
// by d, counts as its cluster's failure when it expires, and is eligible
// for one attempt on the alternative.
// The pages the caller drains afterwards run on the caller's own context
// and are reported at Close, so d never cuts a drain short.
// A read carrying a PageState never moves cluster: its first page is
// bounded and counted like any other, but a paging cursor is only
// meaningful on the cluster that issued it.
//
// One residual is worth knowing: d bounds the wait for the cluster's
// answer, not every wait inside the driver. A token-aware first page can
// block on another caller's in-flight routing-metadata load before its own
// context is consulted, because neither driver makes that cache
// cancellable, so such a leg can overrun d and end on the driver's own
// request timeout instead.
//
// Size d by how long a healthy cluster may take to answer rather than by
// the caller's budget. A caller whose deadline is shorter than 2*d can
// still complete both legs when they answer quickly; what it cannot do is
// give both legs their full allowance, so a first leg that uses all of d
// leaves the alternative short. A caller that must survive the reconnect
// case above needs about r+d, where r is the driver's request timeout: the
// alternative still needs its own d after the first leg has spent r.
//
// Parameters:
//   - d: Per-leg deadline. 0 disables the timeout. Negative values are
//     rejected by NewCQLClient.
//
// Returns:
//   - Option: Configuration option
//
// Example:
//
//	client, err := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithClusterReadTimeout(2*time.Second),
//	)
func WithClusterReadTimeout(d time.Duration) Option {
	return func(c *ClientConfig) {
		c.ClusterReadTimeout = d
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
// The sustained-failure window is armed from the moment a session is
// installed: a freshly built or swapped-in session counts as having
// succeeded at that instant, so a burst of failures on a new client cannot
// replace it before the window has elapsed.
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
	// RefreshTimeout also serves as the grace period after which
	// [CQLClient.RefreshSession] closes the session it replaced.
	RefreshTimeout time.Duration

	// FailureClassifier decides which errors count toward
	// FailureThreshold. Only connectivity-class failures should count: a
	// schema or query error proves the session is reachable, and counting
	// it would replace a healthy session. nil selects
	// [DefaultAutoRefreshFailureClassifier]. Set via
	// [WithAutoRefreshFailureClassifier].
	FailureClassifier func(error) bool
}

// DefaultAutoRefreshFailureClassifier is the default
// [AutoRefreshConfig.FailureClassifier]: an error counts toward the
// failure threshold when it wraps [types.ErrClusterUnreachable] (the
// bundled adapters mark driver connectivity errors with it) or
// [types.ErrClusterTimeout] (a Helix-owned write-leg, read-leg, or probe
// deadline expired). Every other error says the session is reachable and
// counts for nothing.
//
// A custom adapter that does not normalise driver errors never triggers
// auto-refresh under this default; give it a classifier that recognises
// its driver's connectivity errors, or restore the previous behaviour with
// WithAutoRefreshFailureClassifier(func(error) bool { return true }).
//
// Parameters:
//   - err: The error an operation against the cluster returned
//
// Returns:
//   - bool: true when the error indicates the cluster could not be reached
func DefaultAutoRefreshFailureClassifier(err error) bool {
	return errors.Is(err, types.ErrClusterUnreachable) || errors.Is(err, types.ErrClusterTimeout)
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
		FailureClassifier:      DefaultAutoRefreshFailureClassifier,
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

// WithAutoRefreshFailureClassifier overrides AutoRefreshConfig.FailureClassifier.
//
// The classifier runs on every failed operation, so it must be cheap and
// must not block. See [DefaultAutoRefreshFailureClassifier] for the
// default and for the one-line restore of the previous every-error
// behaviour.
func WithAutoRefreshFailureClassifier(fn func(error) bool) AutoRefreshOption {
	return func(c *AutoRefreshConfig) { c.FailureClassifier = fn }
}

// WithAutoRefreshCheckInterval overrides AutoRefreshConfig.CheckInterval.
func WithAutoRefreshCheckInterval(d time.Duration) AutoRefreshOption {
	return func(c *AutoRefreshConfig) { c.CheckInterval = d }
}

// WithAutoRefreshRefreshTimeout overrides AutoRefreshConfig.RefreshTimeout.
func WithAutoRefreshRefreshTimeout(d time.Duration) AutoRefreshOption {
	return func(c *AutoRefreshConfig) { c.RefreshTimeout = d }
}
