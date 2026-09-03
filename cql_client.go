package helix

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/mirror"
)

// CQLClient is the main Helix CQL client for single or dual-cluster operations.
//
// It wraps one or two CQL sessions and orchestrates reads and writes according
// to configured strategies. When only one session is provided (sessionB is nil),
// the client operates in single-cluster mode with pass-through behavior.
//
// Single-cluster mode is ideal for:
//   - Migrating existing applications to Helix incrementally
//   - Development and testing environments
//   - Applications that don't need dual-cluster redundancy yet
//
// # Thread Safety
//
// CQLClient is safe for concurrent use from multiple goroutines. A single client
// instance can be shared across your application:
//
//	// Create once, share everywhere
//	client, err := helix.NewCQLClient(sessionA, sessionB, ...)
//	defer client.Close()
//
//	// Use from multiple goroutines safely
//	go func() { client.Query("INSERT ...").Exec() }()
//	go func() { client.Query("SELECT ...").Scan(&result) }()
//
// All internal state is protected by atomic operations or appropriate locking.
//
// # Lifecycle
//
// Create a client with NewCQLClient() and clean up resources with Close():
//
//	client, err := helix.NewCQLClient(sessionA, sessionB, opts...)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()  // Always close to release resources
//
// After Close() is called:
//   - New public operations return [types.ErrSessionClosed]
//   - Replay worker is stopped (enqueued replays are lost if using MemoryReplayer)
//   - Topology watcher and auto-refresh detector are stopped
//   - The currently installed underlying sessions are closed
//   - Close waits for replaying dual writes in progress and for background
//     legs reported through [DeferredWriteResult], so their replay is
//     enqueued first; it does not wait for reads, strict writes, or other
//     fire-and-forget writes, which may race with shutdown and fail
//   - The client cannot be reused
type CQLClient struct {
	// sessionA / sessionB hold the live cql.Session references behind an
	// atomic.Pointer so they can be replaced at runtime via SwapSession or
	// RefreshSession without locking the read path. The wrapper struct is
	// required because cql.Session is an interface — atomic.Pointer needs a
	// concrete type, and atomic.Value would panic when successive Stores
	// have different dynamic types (which we explicitly support, e.g.
	// swapping a chaos-wrapped session for a plain one).
	//
	// Both pointers always reference a non-nil holder after NewCQLClient.
	// In single-cluster mode, sessionB's holder wraps a nil cql.Session;
	// callers should consult IsSingleCluster (or the singleCluster bool) to
	// distinguish modes rather than nil-checking the loaded session.
	sessionA atomic.Pointer[sessionHolder]
	sessionB atomic.Pointer[sessionHolder]

	// singleCluster captures the construction-time mode and is immutable
	// for the lifetime of the client. SwapSession / RefreshSession cannot
	// promote a single-cluster client to dual-cluster.
	singleCluster bool

	config *ClientConfig
	closed atomic.Bool
	// closeDone is closed once the first Close call has finished, so a
	// concurrent Close returns only after shutdown completed.
	closeDone chan struct{}

	// deferred counts replaying dual writes in progress and the background
	// legs whose result a strategy reports later; Close waits for them
	// before stopping the replay worker.
	deferred deferredLegs

	// runtime holds the components NewCQLClient builds from the
	// configuration. They are owned by the client, not by the caller, and
	// are never visible through Config.
	runtime clientRuntime

	// Drain mode state
	drainA        atomic.Bool
	drainB        atomic.Bool
	topologyCtx   context.Context
	topologyClose context.CancelFunc
	topologyWG    sync.WaitGroup // joins watchTopology on Close

	// routeVeto is the failover policy's route veto when WithRouteVeto is
	// enabled and the policy implements RouteVeto; nil otherwise.
	routeVeto RouteVeto

	// overrideErrSeq counts consecutive override errors for power-of-2 log backoff.
	// Prevents log storms when the AllowedClusters provider is misconfigured.
	overrideErrSeq atomic.Uint64

	// health is the observation hub every health observation enters
	// through; it writes the liveness stats on the session holders.
	health clusterHealth

	// retired holds sessions RefreshSession replaced and will close once
	// their grace period elapses; Close closes them at once.
	retired retiredSessions

	// lastRefreshA / lastRefreshB throttle the auto-refresh detector:
	// stamped before each refresh attempt so a hung refresher cannot cause
	// a re-entrant double-fire on the next detector tick. They outlive
	// the holder a refresh replaces, so they live on the client.
	lastRefreshA atomic.Int64
	lastRefreshB atomic.Int64

	// autoRefreshCtx / autoRefreshClose control the auto-refresh
	// background goroutine's lifetime. Nil if WithAutoRefresh was not
	// configured (or if no SessionRefresher was registered, since the
	// detector cannot do anything useful without a refresher).
	autoRefreshCtx   context.Context
	autoRefreshClose context.CancelFunc
	autoRefreshWG    sync.WaitGroup // joins autoRefreshLoop on Close

	// recoveryProbeCtx / recoveryProbeClose / recoveryProbeWG control the
	// lifecycle of the background recovery probe goroutines (one per cluster).
	// Nil if no probe is running (strategy is not AdaptiveDualWrite, probe
	// is disabled, or client is in single-cluster mode).
	// Close() cancels the probe context and waits for all goroutines to exit
	// before closing sessions so a probe cannot race against a closed session.
	recoveryProbeCtx   context.Context
	recoveryProbeClose context.CancelFunc
	recoveryProbeWG    sync.WaitGroup
}

// clientRuntime holds the components NewCQLClient constructs and Close
// tears down.
type clientRuntime struct {
	// events is the dispatcher created when OnClusterEvent is set. It is
	// created before mirror setup so mirror callbacks can capture it, and
	// started only after the constructor's last step that can fail and
	// before the background probe and auto-refresh goroutines launch.
	// Nil when no handler is registered; every method no-ops on nil.
	events *eventDispatcher

	// mirrorEngine is the mirror engine built when WithMirror or
	// WithMirrorPublisher is set; otherwise nil.
	mirrorEngine *mirror.Engine

	// mirrorReplayWorker drains the mirror replayer in target mode when
	// WithMirrorReplayer names a recognised replayer; otherwise nil.
	mirrorReplayWorker ReplayWorker
}

// sessionHolder wraps a cql.Session so it can live in an atomic.Pointer,
// together with the liveness stats observed against that session.
// The indirection is required because cql.Session is an interface; keeping
// the stats on the holder means a swap installs fresh stats atomically and
// a report from an attempt that used the old session lands on the old
// holder.
type sessionHolder struct {
	s     cql.Session
	stats clusterStats
}

// clusterStats holds the op-outcome counters observed against one session.
// All fields are atomics — no lock needed on the read or write side; the
// observation hub ([clusterHealth]) is the only writer.
//
// The auto-refresh detector (see [WithAutoRefresh]) reads the installed
// holder's stats to decide when a cluster's session is permanently dead
// and needs a RefreshSession call. lastErr captures the most recently
// observed failure error and is threaded through to the SessionRefresher's
// lastErr parameter so refreshers can tailor reconnection strategy to the
// observed failure mode.
type clusterStats struct {
	consecutiveFailures atomic.Int32
	lastSuccessNanos    atomic.Int64
	lastFailureNanos    atomic.Int64
	lastErr             atomic.Pointer[error]
}

// NowProvider returns the current time as Unix nanoseconds.
//
// The default is [DefaultNowProvider] (which calls time.Now().UnixNano()).
// Tests use a deterministic provider so the auto-refresh detector's
// time-based conditions can be exercised without wall-clock dependence.
//
// Mirrors the [TimestampProvider] pattern in config.go.
type NowProvider func() int64

// DefaultNowProvider returns time.Now().UnixNano().
func DefaultNowProvider() int64 {
	return time.Now().UnixNano()
}

// loadSessionA returns the live session for cluster A. It is wait-free; the
// returned session reference is stable for the duration of the calling
// goroutine's use, but a concurrent SwapSession may install a new session
// for the next caller.
func (c *CQLClient) loadSessionA() cql.Session {
	return c.sessionA.Load().s
}

// loadSessionB returns the live session for cluster B, or nil if the client
// was constructed in single-cluster mode.
func (c *CQLClient) loadSessionB() cql.Session {
	h := c.sessionB.Load()
	if h == nil {
		return nil
	}

	return h.s
}

// storeSessionA installs s as the cluster A session. Used by NewCQLClient
// during construction and by SwapSession at runtime.
func (c *CQLClient) storeSessionA(s cql.Session) {
	c.sessionA.Store(c.newSessionHolder(s))
}

// newSessionHolder wraps s with fresh stats whose last success is now, so
// the auto-refresh sustained-failure window is armed from the moment a
// session is installed rather than satisfied by a zero timestamp.
func (c *CQLClient) newSessionHolder(s cql.Session) *sessionHolder {
	h := &sessionHolder{s: s}
	h.stats.lastSuccessNanos.Store(c.config.NowProvider())

	return h
}

// storeSessionB installs s as the cluster B session. In single-cluster mode
// it stores a holder wrapping nil so loadSessionB() returns nil safely
// without a nil-pointer-deref on the holder pointer.
func (c *CQLClient) storeSessionB(s cql.Session) {
	c.sessionB.Store(c.newSessionHolder(s))
}

// holderFor returns the installed session holder for the given cluster.
// In single-cluster mode every cluster maps to the cluster A holder.
func (c *CQLClient) holderFor(cluster ClusterID) *sessionHolder {
	if c.singleCluster || cluster == ClusterA {
		return c.sessionA.Load()
	}

	return c.sessionB.Load()
}

// statsForCluster returns the liveness stats of the installed session for
// the given cluster.
func (c *CQLClient) statsForCluster(cluster ClusterID) *clusterStats {
	return &c.holderFor(cluster).stats
}

// lastRefreshFor returns the auto-refresh throttle stamp for the cluster.
func (c *CQLClient) lastRefreshFor(cluster ClusterID) *atomic.Int64 {
	if cluster == ClusterB {
		return &c.lastRefreshB
	}

	return &c.lastRefreshA
}

// Compile-time assertion that CQLClient implements CQLSession.
var _ CQLSession = (*CQLClient)(nil)

// Session returns the CQLClient as a CQLSession interface.
//
// This allows the CQLClient to be used as a drop-in replacement for gocql.Session
// in code that expects a session-like interface.
//
// Example:
//
//	client, _ := helix.NewCQLClient(sessionA, sessionB)
//	session := client.Session()
//
//	// Use session like a regular gocql.Session
//	err := session.Query("INSERT INTO ...").Exec()
//
// Returns:
//   - CQLSession: The client as a CQLSession interface
func (c *CQLClient) Session() CQLSession {
	return c
}

// IsSingleCluster returns true if the client is operating in single-cluster mode.
//
// In single-cluster mode, all operations are executed directly on the primary
// session without dual-write or failover logic. The mode is fixed at
// construction time; SwapSession and RefreshSession cannot promote a
// single-cluster client to dual-cluster.
func (c *CQLClient) IsSingleCluster() bool {
	return c.singleCluster
}

// getSession returns the session for the given cluster.
// In single-cluster mode, always returns sessionA.
func (c *CQLClient) getSession(cluster ClusterID) cql.Session {
	return c.holderFor(cluster).s
}

// getSession is for call sites that never report health for the session
// they use (immediate CAS, the recovery probe's session lookup, replay
// execution). A call site that reports an outcome to the hub must load the
// holder with holderFor and keep it, so the report lands on the session
// the attempt actually used.

// clusterName returns the display name for the given cluster.
func (c *CQLClient) clusterName(cluster ClusterID) string {
	return c.config.ClusterNames.Name(cluster)
}

// SessionA returns the underlying session for cluster A.
//
// Use with caution - direct access bypasses Helix's dual-cluster logic.
// Each call performs an atomic load and returns the live session, so the
// reference is current as of the call. Callers that store the returned
// reference will hold a stale pointer if SwapSession or RefreshSession is
// invoked subsequently — prefer calling SessionA() at point of use.
//
// Returns:
//   - cql.Session: The raw session for cluster A
func (c *CQLClient) SessionA() cql.Session {
	return c.loadSessionA()
}

// SessionB returns the underlying session for cluster B, or nil in
// single-cluster mode.
//
// Use with caution - direct access bypasses Helix's dual-cluster logic.
// See SessionA for the swap-vs-stored-reference caveat.
//
// Returns:
//   - cql.Session: The raw session for cluster B (nil in single-cluster mode)
func (c *CQLClient) SessionB() cql.Session {
	return c.loadSessionB()
}

// Config returns a copy of the effective client configuration.
//
// The copy reflects the options the client was built with, including the
// replayer and worker created by [WithAutoMemoryWorker]. Modifying the
// returned value has no effect on the client: the configuration is fixed
// once [NewCQLClient] returns. Slice-typed fields share their backing
// arrays with the client.
//
// Returns:
//   - *ClientConfig: A copy of the client's configuration
func (c *CQLClient) Config() *ClientConfig {
	cfg := *c.config

	return &cfg
}
