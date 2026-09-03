# Async Mirror Writes

Async mirror writes are designed for seamless Cassandra cluster migrations.
While the application keeps writing to the *current* dual-cluster pair, helix
asynchronously mirrors selected writes to a *new* dual-cluster pair so that N
days of write history accumulate on the new pair before traffic cutover.
Mirroring is **per-statement opt-in**: callers explicitly mark which writes
should be mirrored.

## When to use mirror

- **Cluster migration**: stand up a new pair, mirror selected tables for ~7 days,
  validate regression queries against the new pair, cut traffic over.
- **Reduced-history canary**: massive time-series tables don't need to mirror;
  use blue/green canary traffic switching for those and mirror only the smaller
  reference / metadata tables.

Mirroring is **not** a replacement for:
- Dual-write durability — the existing replay system handles that on the primary path.
- Live secondary reads — the mirror is a write sink. Reads always go through the primary path.

## Two modes

```
                                 ┌─────────────────────────┐
       ┌────────────────────┐    │                         │
       │  CQLClient (primary│    │      Mirror dest        │
       │   + write strategy)│────┤   (helix CQLClient)     │
       └─────┬──────────────┘    │                         │
             │                   └─────────────────────────┘
             │
             ├──── target mode ────►   in-process worker pool ─► mirror dest
             │                                  │
             │                                  └─► (failure) ─► Replayer ─► Worker ─► mirror dest
             │
             └──── publisher mode ─►   bounded ring buffer ─► Replayer (NATS)
                                                              │
                                                              └─► consumer binary ─► mirror dest
```

### Target mode (`WithMirror`)

The mirror destination is a second helix `CQLClient` running in the same
process. The mirror engine's worker pool dispatches each captured write
directly through that client's full Exec path. Failed writes optionally land
in a `Replayer` for durable retry.

Best for: dev, test, small deployments, low-write-volume migrations.

```go
mirrorTarget, _ := helix.NewCQLClient(newA, newB,
    helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
)

client, _ := helix.NewCQLClient(currentA, currentB,
    helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
    helix.WithMirror(mirrorTarget,
        mirror.WithQueueSize(8192),
        mirror.WithWorkers(4),
    ),
    helix.WithMirrorReplayer(replay.NewMemoryReplayer()),
)

session := client.Session()
err := session.Query("INSERT INTO users (id, name) VALUES (?, ?)", id, name).
    Mirror().
    ExecContext(ctx)
```

### Publisher mode (`WithMirrorPublisher`)

The app publishes captured writes to a `Replayer` (typically `NATSReplayer`)
instead of writing them in-process. A separate consumer binary runs a worker
built via `helix.NewMirrorWorker` that drains the replayer into the mirror
destination. Captures are durable from the moment they're published — they
survive app restart.

Best for: production migrations, multi-app deployments, throughput-sensitive
mirroring.

**App side:**

```go
natsReplayer, _ := replay.NewNATSReplayer(js, replay.WithStreamName("helix-mirror"))

client, _ := helix.NewCQLClient(currentA, currentB,
    helix.WithMirrorPublisher(natsReplayer,
        mirror.WithQueueSize(8192),
        mirror.WithWorkers(4),
    ),
)

// Same per-statement opt-in:
err := session.Query(...).Mirror().ExecContext(ctx)
```

**Consumer binary:**

```go
mirrorTarget, _ := helix.NewCQLClient(newA, newB,
    helix.WithWriteStrategy(policy.NewConcurrentDualWrite()),
)

worker, err := helix.NewMirrorWorker(natsReplayer, mirrorTarget,
    replay.WithMaxAttempts(5),
    replay.WithRetryDelay(100*time.Millisecond),
    replay.WithWorkerMetrics(workerMetricsCollector),
)
if err != nil { return err }
_ = worker.Start()
defer worker.Stop()
defer mirrorTarget.Close()
```

`helix.WithMirror` and `helix.WithMirrorPublisher` are **mutually exclusive**;
configuring both returns `types.ErrMirrorModeConflict` from `NewCQLClient`.

## Per-statement opt-in

Mirror is fire-and-forget. The mirror leg never surfaces an error to the
caller; failures are logged, counted, and (in target mode with
`WithMirrorReplayer`) durably retried.

```go
// Single statement
session.Query(stmt, args...).Mirror().ExecContext(ctx)

// Whole-batch
b := session.Batch(types.LoggedBatch).
    Query("INSERT ...", a).
    Query("INSERT ...", b).
    Mirror()
b.ExecContext(ctx)
```

Mixed batches (some statements mirrored, others not) are **not supported** —
the whole batch mirrors or none does.

## Semantics

| Concern | Behavior |
|---|---|
| Trigger | Mirror fires exactly when the primary `Exec` returns `nil`. Normally that means at least one cluster acknowledged the write; with `WithAckMode(AckOnReplayAdmission)` it also covers a write no cluster acknowledged that was admitted to replay (or is still running in the background). A `NonIdempotent()` write acknowledged by one cluster returns `*types.PartialWriteError` and is therefore not mirrored. Any other non-nil result suppresses the mirror. |
| Backpressure | Engine queue is bounded; non-blocking enqueue. On full / disabled / stopped, drop with metric + rate-limited log + optional `mirror.WithOnDrop` callback. The hot path never stalls on mirror. |
| Timestamps | The client-generated timestamp is resolved when `Exec` begins, captured in the mirror payload, and applied at mirror exec via `WithTimestamp`. Server-side `WRITETIME`, LWW, and tombstone semantics on the mirror cluster match the primary. `USING TTL` does not: the server computes expiry from the time it applies the mirrored write, so a delayed or retried mirror write expires later than the primary by that delay, as with replay. |
| Args | `[]any` args (and batch entries' args) are deep-copied synchronously **before** `Exec` returns to the caller. Caller-side buffer reuse / pooling cannot corrupt mirror payloads. |
| Disable / Enable | `client.Mirror().Disable()` stops accepting new captures; the in-flight queue continues to drain. `Enable()` mid-drain resumes normal operation. Workers always process queued items regardless of the enabled flag. |
| Idempotence | Mirror writes may be retried on failure (via `WithMirrorReplayer` or by NATS redelivery in publisher mode). Counter updates and `IF`-clause LWTs are not idempotent under retry — accept divergence on those statements. A `NonIdempotent()` statement (a `CounterBatch` automatically) carries its marker in the payload, so the destination executes it on its strict path and never replays it within its own pair; only the mirror-level retry can repeat it. |
| Shutdown | `client.Close()` drains the engine synchronously: every queued capture is executed (target mode) or published (publisher mode) before `Close` returns, so a full 8192-slot queue at five seconds per publish can hold `Close` for minutes. `mirror.WithDrainTimeout(d)` bounds how long new executions keep starting: after `d` the queued captures are dropped exactly once (`mirror.WithOnDrop`, `Stats().Dropped`, `{prefix}_mirror_drain_dropped_total`) with one `Warn` line, and `Close` still waits for the executions and callbacks already running. Neither `mirror.WithOnDrop` nor `mirror.WithOnError` may call `Close` or `Stop`: both wait for the worker that invoked the callback. |
| Consistency | The consistency and serial consistency the original write set are captured and applied by the built-in executor, so the destination acknowledges the write under the same rule. A session-default write runs at the destination's default. |

## Runtime control

```go
ctrl := client.Mirror() // *mirror.Engine, or nil if neither WithMirror
                        // nor WithMirrorPublisher was configured.

ctrl.Disable()
ctrl.Enable()
enabled := ctrl.Enabled()
stats := ctrl.Stats() // Enqueued / Dropped / Success / Error / QueueDepth
```

## Observability

If your `MetricsCollector` also implements `types.MirrorMetrics` (the bundled
`contrib/metrics/vm.Collector` does), helix auto-wires the mirror engine.
Exposed metrics:

| Metric | Type | Notes |
|---|---|---|
| `helix_mirror_enqueue_success_total` | counter | Captures accepted into the queue. |
| `helix_mirror_enqueue_dropped_total` | counter | Captures dropped (disabled/stopped/full). |
| `helix_mirror_drain_dropped_total`   | counter | Captures dropped when `mirror.WithDrainTimeout` cut the shutdown drain short (optional `types.MirrorShutdownMetrics`). |
| `helix_mirror_exec_success_total`    | counter | Engine dispatch returned nil. |
| `helix_mirror_exec_errors_total`     | counter | Engine dispatch returned error. |
| `helix_mirror_exec_duration_seconds` | histogram | Engine dispatch duration. |
| `helix_mirror_queue_depth`           | gauge | Current queue depth. |
| `helix_mirror_enabled`               | gauge | 1 if accepting captures, else 0. |

Mirror metrics are **not cluster-scoped** — per-cluster routing on the mirror
destination is recorded against that destination's own metrics namespace
(provide a separate collector to the mirror's `*CQLClient` if you need that).

## Known parity gaps

- **Per-query consistency is preserved only through the built-in executor.**
  The mirror payload carries the consistency and serial consistency the
  original write set, and the executor built by `WithMirror` applies them
  on the destination. A custom `mirror.ExecuteFunc` receives them in
  `types.ReplayPayload` and decides for itself.
- **Writes that succeed only via the *primary's* replay path are not mirrored.**
  When both current clusters fail at `Exec` time and the primary returns an
  error, the primary replay system later lands the write — but the mirror
  engine never sees it. For 7-day regression-query use cases this is rare and
  the gap is acceptable. A bridge from primary-replay-success back into the
  mirror engine is deferred past v1.4.0.
- **Schema superset**: the mirror destination must contain a superset of any
  mirrored statement's referenced keyspaces, tables, and columns. Mismatch
  means mirror writes pile up in the replayer (target mode) or in the NATS
  stream (publisher mode). Validate before enabling.
- **Cross-instance ordering (publisher mode)**: multiple app instances
  publishing to the same NATS stream provide no global ordering. Dependent
  writes on the same partition key may apply at the mirror cluster out of
  order.

## Failure observability

| Path | Where it lands |
|---|---|
| Engine queue full | `mirror_enqueue_dropped_total` + log + `mirror.WithOnDrop`. |
| Drain timeout at shutdown | `mirror_drain_dropped_total` + one log line + `mirror.WithOnDrop` per capture; see the Shutdown row above. |
| Target-mode dispatch error | `mirror_exec_errors_total` + log + `mirror.WithOnError`. With `WithMirrorReplayer`, also pushed onto the replayer. |
| Replayer enqueue failure | `helix.WithOnReplayDropped` (shared with primary replay) + the `mirror_replay_dropped` cluster event + `mirror_replay_dropped_total` (optional `types.MirrorReplayMetrics`). |
| Publisher-mode `publisher.Enqueue` error | `mirror_exec_errors_total` + log + `mirror.WithOnError`. **Not** routed through `WithOnReplayDropped`. |

## Examples

See [`examples/mirror`](../examples/mirror) for runnable code covering both
modes.

## Related

- [Replay System](replay-system.md) — the durability primitive used for
  mirror retry and publisher transport.
- [Strategy Policy](strategy-policy.md) — write strategies that govern how
  the primary `Exec` lands data; mirror is orthogonal to all of them.
- [Cluster Events](cluster-events.md) — the `mirror_replay_dropped` event and
  why a caller-supplied `mirror.WithOnError` suppresses it.
