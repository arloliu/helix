# Configuration Reference

`NewCQLClient` validates root options before starting background components or mutating caller-owned strategies, policies, and workers.
Invalid root options return joined `*types.OptionError` values
that can be checked with `types.IsOptionError` or `errors.As`;
mirror mode conflicts also wrap the relevant sentinel error such as `types.ErrMirrorModeConflict`.

This page lists every option the root `helix` package accepts.
Options belonging to `policy/`, `replay/`, `topology/`, `mirror/`, and `contrib/metrics/vm` are documented in their own guides,
linked from the rows that take them.

---

## Strategies & policies

See [Strategy & Policy Guide](strategy-policy.md).

| Option | Default | Description | Validation |
|--------|---------|-------------|------------|
| `WithReadStrategy(strategy)` | `nil` — reads go to cluster A only, no load balancing | Read routing strategy, e.g. `policy.NewStickyRead()` | — |
| `WithWriteStrategy(strategy)` | `nil` — concurrent dual-write to both clusters | Write execution strategy; see [Adaptive Dual Write](adaptive-dual-write.md) and [Strict Write](strict-write.md) | — |
| `WithFailoverPolicy(policy)` | `nil` — a failed read always attempts failover | Failover policy for reads, e.g. `policy.NewActiveFailover()` | — |

---

## Replay

See [Replay System](replay-system.md).

| Option | Default | Description | Validation |
|--------|---------|-------------|------------|
| `WithReplayer(replayer)` | `nil` — partial write failures are lost | Durable store for writes that failed on one cluster | — |
| `WithReplayWorker(worker)` | `nil` | Worker that drains the replayer; started with the client and stopped on `Close` | — |
| `WithAutoMemoryWorker(queueCapacity, workerOpts...)` | off; `queueCapacity` 0 uses 10000 | Builds and runs an in-process `MemoryReplayer` plus worker for development and simple deployments | Cannot be combined with `WithReplayer`; cannot be combined with `WithReplayWorker` |
| `WithReplayGate(allow)` | `nil` — every cluster permitted | Operator predicate that holds replay back for a cluster while it returns false | — |

---

## Mirror

See [Mirror Guide](mirror.md).

| Option | Default | Description | Validation |
|--------|---------|-------------|------------|
| `WithMirror(target, opts...)` | `nil` — `Query.Mirror()` is a no-op | Mirrors opted-in writes to a second helix client in this process | `target` cannot be nil (`types.ErrNilMirrorTarget`); cannot be used with `WithMirrorPublisher` |
| `WithMirrorPublisher(publisher, opts...)` | `nil` | Publishes mirror captures to a replayer that a separate consumer binary drains | `publisher` cannot be nil (`types.ErrNilMirrorPublisher`); cannot be used with `WithMirror` (`types.ErrMirrorModeConflict`) |
| `WithMirrorReplayer(replayer, workerOpts...)` | `nil` | Durable retry for mirror writes that returned an error; auto-builds a worker for the memory and NATS replayers | — |

---

## Reads

See [Fallback Read](fallback-read.md) and [Slice Read](slice-read.md).

| Option | Default | Description | Validation |
|--------|---------|-------------|------------|
| `WithDefaultFallbackRead(enabled)` | `false` | Enables FallbackRead for every eligible query on this client | — |
| `WithFallbackReadOnDrainingCluster(enabled)` | `false` | Lets a `Scan` / `MapScan` FallbackRead probe contact the alternative cluster while it is draining | — |
| `WithDefaultMaxRows(n)` | `0` (unbounded) | Client-wide row cap for `SliceMap` and `SliceScan`; a non-zero per-query `MaxRows` wins | Must be >= 0; must be < `math.MaxInt32` |
| `WithClusterReadTimeout(d)` | `0` (disabled) | Per-leg read deadline owned by Helix; an expiry counts as that cluster's failure (`types.ErrClusterTimeout`) | Must be >= 0 |
| `WithRouteVeto(enabled)` | `false` | Lets a failover policy that implements `RouteVeto` steer ordinary reads away from a cluster whose breaker is open | — |
| `WithAllowedClusters(fn)` | `nil` | Operator-driven read routing override that bypasses the read strategy while it returns a non-empty list | — |

Per-request opt-in uses `WithFallbackRead(ctx) context.Context`, a context helper rather than an `Option`:
a per-query `FallbackRead()` beats the context,
and the context beats `WithDefaultFallbackRead(true)`.

---

## Writes

| Option | Default | Description | Validation |
|--------|---------|-------------|------------|
| `WithClusterWriteTimeout(d)` | `0` (disabled) | Per-leg write deadline owned by Helix; an expired leg is replayed and reported as `types.ErrClusterTimeout` | Must be >= 0 |
| `WithAckMode(mode)` | `RequireSynchronousAck` | Selects whether a write with no synchronous acknowledgement may return nil; see [Strict Write](strict-write.md) | Must be `RequireSynchronousAck` or `AckOnReplayAdmission` |
| `WithTimestampProvider(fn)` | `DefaultTimestampProvider` (`time.Now().UnixMicro()`) | Generates the client-side write timestamp that decides last-write-wins across clusters and replays | Must return a non-zero timestamp (sampled once at construction) |
| `WithBehaviorProfile(profile)` | `Legacy` | Pure option expansion for client-owned defaults: `Safe` is exactly `WithRouteVeto(true)`, `Legacy` exactly `WithRouteVeto(false)` | Must be `Legacy` or `Safe` |

---

## Recovery & health

See [Auto Recovery](auto-recovery.md) and [Session Refresh](session-refresh.md).

| Option | Default | Description | Validation |
|--------|---------|-------------|------------|
| `WithRecoveryProbe(p)` | default probe runs whenever a probe-reporting write strategy or failover policy is configured | Background probe that credits recovery for a degraded cluster and closes a reserved breaker; zero `Interval` / `Timeout` take the defaults | `Interval` must be >= 0; `Timeout` must be >= 0 |
| `WithRecoveryProbeDisabled()` | probe enabled | Suppresses all probing for both authorities it serves | — |
| `WithSessionRefresher(fn)` | `nil` — `RefreshSession` returns `types.ErrNoSessionRefresher` | Caller-supplied factory that builds a replacement `cql.Session` for a broken cluster session | — |
| `WithAutoRefresh(opts...)` | off | Enables the auto-refresh detector with the defaults below; needs a `SessionRefresher` to do anything | Sub-option rules below |
| `WithTopologyWatcher(watcher)` | `nil` | Topology watcher that supplies drain mode, e.g. `topology.NewNATS(...)` | — |

### Auto-refresh sub-options

`AutoRefreshOption` values are passed to `WithAutoRefresh`.
Their validation rules apply only when auto-refresh is enabled.

| Option | Default | Description | Validation |
|--------|---------|-------------|------------|
| `WithAutoRefreshFailureThreshold(n)` | `10` | Consecutive failures a cluster must reach before refresh is considered | Must be > 0 |
| `WithAutoRefreshSustainedFailureWindow(d)` | `5m` | Minimum time since the last successful op before the detector fires | Must be > 0 |
| `WithAutoRefreshMinRetryInterval(d)` | `1m` | Minimum time between successive refresh attempts on the same cluster | Must be > 0 |
| `WithAutoRefreshCheckInterval(d)` | `30s` | Period at which the detector evaluates per-cluster state | Must be > 0 |
| `WithAutoRefreshRefreshTimeout(d)` | `30s` | Per-call timeout around `RefreshSession`, and the grace period before the replaced session is closed | Must be > 0 |
| `WithAutoRefreshFailureClassifier(fn)` | `DefaultAutoRefreshFailureClassifier` | Decides which errors count toward the failure threshold; the default counts `types.ErrClusterUnreachable` and `types.ErrClusterTimeout` | — |

### `RecoveryProbe` fields

Defaults come from `DefaultRecoveryProbe()`.

| Field | Default | Description |
|-------|---------|-------------|
| `Probe func(ctx, session) error` | reads `release_version` from `system.local` | Health check run against the live session; nil result means the cluster is reachable |
| `Interval time.Duration` | `2s` | Period between probe checks |
| `Timeout time.Duration` | `1s` | Deadline bounding each individual probe call |

---

## Observability

| Option | Default | Description | Validation |
|--------|---------|-------------|------------|
| `WithLogger(logger)` | no-op logger | Structured logger wired into every component that accepts one | — |
| `WithMetrics(collector)` | no-op collector | Metrics collector, e.g. `vm.New()` from `contrib/metrics/vm` | — |
| `WithClusterNames(nameA, nameB)` | `"A"` and `"B"` | Display names used in metric labels and log messages | Each name non-empty, at most 32 characters, alphanumeric with underscores starting with a letter or underscore; the two names must differ |
| `WithOnClusterEvent(handler)` | `nil` | Handler for cluster-health events; delivery is asynchronous and best-effort. See [Cluster Events](cluster-events.md) | — |
| `WithOnReplayDropped(handler)` | `nil` | Handler for a replay payload that could not be enqueued, covering both the primary and mirror replay paths | — |
