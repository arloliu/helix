# Cluster Event Notification Guide

`helix.WithOnClusterEvent` gives you one registration point for operationally
significant cluster-health events: read failover, read divergence, circuit
breaker open/close, adaptive-write degrade/recover, drain enter/exit, replay
drops, mirror replay drops, and session refresh attempt/success/error.

Most of these are state transitions, so their volume is bounded by how often
the state flips. Two are not: `failover` and `read_divergence` fire once per
affected read. See [Per-operation kinds](#per-operation-kinds).

This is a push-based, best-effort notification stream for driving alerting,
paging, or an operational dashboard. A handler that cannot keep up loses
events; a metrics counter does not. Where an event has a metric counterpart,
read rates and current state from the metric and use the event only as the
push notification.

Three kinds have no metric counterpart: `write_degraded`, `write_recovered`,
and `mirror_replay_dropped`. For those the event is the only machine-readable
signal, and a dropped event cannot be recovered from metrics. See
[Events and Metrics](#events-and-metrics) for the full comparison, including
one kind whose event and metric count different things.

---

## Quick Start

```go
client, err := helix.NewCQLClient(sessionA, sessionB,
    // Each kind handled below comes from an optional component. Configure
    // the ones whose events you want; see the Requires column below.
    helix.WithWriteStrategy(policy.NewAdaptiveDualWrite()), // write_degraded / write_recovered
    helix.WithFailoverPolicy(policy.NewCircuitBreaker()),   // circuit_breaker_open / _closed
    helix.WithReplayer(replayer),                           // replay_dropped
    // mirror_replay_dropped additionally needs WithMirror + WithMirrorReplayer.

    helix.WithOnClusterEvent(func(ev types.ClusterEvent) {
        switch ev.Kind {
        case types.EventWriteDegraded, types.EventCircuitBreakerOpen:
            alerting.Page("helix cluster issue",
                "kind", string(ev.Kind),
                "cluster", string(ev.Cluster),
                "reason", ev.Reason,
            )
        case types.EventReplayDropped, types.EventMirrorReplayDropped:
            alerting.Page("helix potential data loss", "error", ev.Err)
        }
    }),
)
if err != nil {
    log.Fatal(err)
}
defer client.Close()
```

There is no separate start call: registering the handler is enough, and Helix
installs the emitter into the configured write strategy and failover policy
for you.

**Which kinds you receive depends on what else you configure.** The emitter
reaches only the write strategy and the failover policy, both unset by
default, and most other kinds come from a component that is also optional. A
dual-cluster client configured with nothing but the handler can produce only
`failover`, plus `read_divergence` on queries that opt into FallbackRead. Every
other kind stays silent, and nothing tells you so: registering a handler for an
unreachable kind is not an error and produces no warning. From the handler's
side, "receiving nothing" and "everything is healthy" look identical.

(The one related warning Helix does log is unrelated to events: a dual-cluster
client built without a `Replayer` logs "dual-cluster mode with no Replayer
configured" at construction.)

The **Requires** column in the next section lists the configuration each kind
needs. Check it against your own options before you build an alert on a kind.

---

## Event Reference

Every event is a `types.ClusterEvent` with a `Kind`. `Kind` and `Timestamp` are
always set. Of the remaining fields, only the ones relevant to that kind are
populated; the rest hold zero values.

**Requires** lists the configuration the kind needs beyond registering the
handler.

Seven kinds additionally require dual-cluster mode (a non-nil `sessionB`):
`failover`, `read_divergence`, `circuit_breaker_open`, `circuit_breaker_closed`,
`write_degraded`, `write_recovered`, and `replay_dropped`. A single-cluster
client writes straight to cluster A with no write strategy, no replay enqueue,
and no failover, so none of those producers ever runs. Drain, session-refresh,
and mirror events work in either mode.

| Kind | Requires | Fires when | Populated fields | Suggested action |
|---|---|---|---|---|
| `failover` (`EventFailover`) | Nothing — on by default. A configured `FailoverPolicy` can gate it. | A read fails on the selected cluster and is retried on the alternative cluster. Fires **once per failing read**, not once per outage. | `Cluster` (= `ToCluster`), `FromCluster`, `ToCluster`, `Err` | Investigate the `FromCluster` if it recurs. Read the rate from `{prefix}_failover_total`, not from event counts. |
| `read_divergence` (`EventReadDivergence`) | A read that opted into FallbackRead: `Query.FallbackRead()`, `helix.WithFallbackRead(ctx)`, or `helix.WithDefaultFallbackRead(true)`. | A fallback read finds the row on the alternative cluster after the selected cluster returned not-found. Fires **once per divergent read**. | `Cluster` (the cluster missing the row), `Reason` | Watch for a rising rate — it signals replay lag on `Cluster`. Read the rate from `{prefix}_read_divergence_total`. |
| `circuit_breaker_open` (`EventCircuitBreakerOpen`) | `helix.WithFailoverPolicy` holding a `policy.CircuitBreaker` or `policy.LatencyCircuitBreaker`. `policy.ActiveFailover` produces no events. | A circuit breaker trips open for a cluster. | `Cluster`, `Count` (consecutive failures at trip) | Page — reads are being routed away from `Cluster`. |
| `circuit_breaker_closed` (`EventCircuitBreakerClosed`) | Same as `circuit_breaker_open`. | A previously open circuit breaker closes. `Reason` `"operation succeeded"` means a read on that cluster succeeded — this is always the reason on a success-driven close. `Reason` `"reset timeout elapsed"` means a subsequent failure observed that the gap since the last recorded failure had exceeded `resetTimeout`; this reason only ever appears on a failure-driven close, on the *next* `RecordFailure` that observes the expired interval, not when the timeout itself elapses. See [Circuit Breaker Close Timing](#circuit-breaker-close-timing). | `Cluster`, `Reason` (`"operation succeeded"`, or `"reset timeout elapsed"`) | Split by `Reason`. `"operation succeeded"`: the cluster served a read; clear the alert. `"reset timeout elapsed"`: the open span ended on a timer with no successful read, so the cluster is not known to be healthy; keep the alert. |
| `write_degraded` (`EventWriteDegraded`) | `helix.WithWriteStrategy(policy.NewAdaptiveDualWrite(...))`. | `AdaptiveDualWrite` moves a cluster into degraded (fire-and-forget) mode. | `Cluster`, `Count` (slow-strike count), `Reason` (`"slow-strike threshold reached"`, or `"manual"` for `ForceDegrade`) | Page — writes to `Cluster` are no longer synchronous and rely on replay. No metric reports this; the event is the only signal. |
| `write_recovered` (`EventWriteRecovered`) | Same as `write_degraded`. | `AdaptiveDualWrite` moves a cluster back to healthy (synchronous) mode. | `Cluster`, `Reason` (`"fast-strike recovery"`, or `"manual"` / `"manual reset"` for `ForceRecover` / `Reset`) | Clear any related alert. `"fast-strike recovery"` covers both a fast write from ordinary traffic and a successful recovery probe; the two are not distinguishable from the event. No metric reports this either. |
| `drain_entered` (`EventDrainEntered`) | `helix.WithTopologyWatcher`. | A cluster enters drain mode via the topology watcher. | `Cluster` | Confirm this matches an expected maintenance window. |
| `drain_exited` (`EventDrainExited`) | Same as `drain_entered`. | A cluster exits drain mode. | `Cluster` | Informational. |
| `replay_dropped` (`EventReplayDropped`) | A configured replayer: `helix.WithReplayer` or `helix.WithAutoMemoryWorker`. **Without one, a failed write is lost and no event fires at all.** | A failed write cannot be enqueued for replay (queue full or unavailable) — potential data loss. | `Cluster` (replay target), `Err` (enqueue error) | Page immediately; this is the primary data-loss signal for the enqueue step. For payload access (not just the fact of a drop), also register `helix.WithOnReplayDropped`. |
| `mirror_replay_dropped` (`EventMirrorReplayDropped`) | `helix.WithMirror` **and** `helix.WithMirrorReplayer`, and no caller-supplied `mirror.WithOnError`. | A failed mirror write cannot be enqueued for mirror replay — potential mirror-target data loss. `Cluster` is unset: mirror payloads target a logical sink, not one of this client's clusters. | `Err` (enqueue error), `Reason` | Page; treat like `replay_dropped` but for the mirror destination. No metric reports this. See [Relationship to Other Hooks](#relationship-to-other-hooks) for when this event does not fire. |
| `session_refresh_attempt` (`EventSessionRefreshAttempt`) | `helix.WithAutoRefresh` **and** `helix.WithSessionRefresher`. | The auto-refresh detector decides a cluster's session is permanently dead and invokes the `SessionRefresher`. | `Cluster`, `Count` (qualifying consecutive-failure count) | Informational; a refresh is starting. |
| `session_refresh_success` (`EventSessionRefreshSuccess`) | Same as `session_refresh_attempt`. | A session refresh attempt succeeds. | `Cluster` | Informational; clear any related alert. |
| `session_refresh_error` (`EventSessionRefreshError`) | Same as `session_refresh_attempt`. | A session refresh attempt fails. | `Cluster`, `Err` | Page — the cluster's session is still considered dead. |

### Per-operation kinds

`failover` and `read_divergence` are the two kinds that are not state
transitions: they fire once per affected read. Under a sustained cluster
outage the handler is invoked at read rate, so at 10,000 reads per second the
128-event buffer holds about 13 ms of events and the rest are dropped.

The consequence is that a rate built from these events falls apart exactly
during the incident it is supposed to measure. Use `{prefix}_failover_total`
and `{prefix}_read_divergence_total` for rates, and treat the events as
notifications that an outage has started.

Every other kind fires on a state change, so its volume is bounded by how
often the cluster's state actually flips.

### Circuit Breaker Close Timing

`resetTimeout` expiry has two separate effects, and they do not happen at the
same moment:

- **Routing changes immediately.** Once `resetTimeout` has elapsed since the
  last recorded failure, `ShouldFailover` returns false, so the next read is
  routed to the previously failing cluster as a probe.
- **The close transition happens later.** The `circuit_breaker_closed` event,
  the "circuit breaker closed" log line, and the `{prefix}_circuit_breaker_state`
  gauge write all happen on the *next* `RecordFailure` or `RecordSuccess` that
  observes the expired interval. A failure closes with `Reason`
  `"reset timeout elapsed"`; a success closes with `Reason`
  `"operation succeeded"`.

If no further call is ever recorded for that cluster — the application stops
reading, or routes every read to the other cluster — the open span never ends.
The gauge keeps reporting open and no `circuit_breaker_closed` arrives. This is
an accepted limitation: closing requires an observation, and the breaker has
nothing to observe.

With `threshold` set to 1, the same call that closes on the elapsed timeout
also brings the fresh count straight back to the threshold, so it emits
`circuit_breaker_closed` and then `circuit_breaker_open`, in that order, and
the breaker ends up open.

---

## Delivery Semantics

- **Single dedicated goroutine.** Your handler is called on one goroutine;
  invocations never overlap, so the handler itself does not need to be safe
  for concurrent calls.
- **Never blocks a read or write.** Emission is a non-blocking buffered send.
  If the handler falls behind and the buffer (128 events) fills, the newest
  events are dropped.
- **Handler panics are recovered.** A panicking handler cannot crash the
  process or stop further delivery; the panic is logged and delivery
  continues with the next event.
- **A handler may call back into Helix.** Calling the client or a policy from
  inside the handler — `ForceRecover`, `Reset`, a read, a write — is safe. No
  policy lock is held while your handler runs, and an event produced by that
  reentrant call is enqueued with a non-blocking send rather than recursing.
  It may itself be dropped if the buffer is full. The one prohibition is
  `Close`; see [Shutdown Semantics](#shutdown-semantics).

### What you can observe about drops

Drops are counted exactly, but the count is internal. There is no exported
accessor and no metric, so an application cannot alert on "my event stream is
dropping events". Plan for that when you design alerting on this stream.

What you do get:

- **Dispatcher drops** (handler too slow, or emission racing `Close`) produce a
  `Warn` log line through the configured `Logger` carrying the running total.
  The line is not written per drop or on a schedule: it is written on the first
  drop and then only once the total has at least doubled since the last line,
  plus once more at shutdown. After a million drops the last line may read
  524288.
- **Policy-side drops** produce nothing at all. The per-policy queue described
  in [Standalone Policy Usage](#standalone-policy-usage) counts its drops
  internally and never logs them.

### Ordering scope

Circuit-breaker events and adaptive-write events are each delivered in
**per-cluster transition order**, per policy instance. For one cluster of one
`CircuitBreaker` you see `circuit_breaker_open` before the matching
`circuit_breaker_closed`, and for one cluster of one `AdaptiveDualWrite` you
see `write_degraded` before the matching `write_recovered`.

The two policy types do not share a queue, so there is **no ordering between
them**: a `write_degraded` and a `circuit_breaker_open` for the same cluster
can arrive in either order. Two separate policy instances are likewise
unordered with respect to each other.

Ordering holds among the events that are actually **delivered**. Because the
stream is lossy, a drop can remove either end of a pair, so a handler that
maintains alert state must tolerate a `circuit_breaker_closed` with no
preceding `circuit_breaker_open`, and an `open` whose `closed` never arrives.
Do not write a state machine that assumes pairing.

Everything else — failover, read divergence, replay drops, mirror replay
drops, drain enter/exit, session refresh — is produced by independent
goroutines and delivered in enqueue order, with **no cross-kind causal
guarantee**. Do not assume, for example, that a `failover` event always
precedes a later `circuit_breaker_open` for the same cluster, even if that is
usually what happens operationally.

Metric updates and log lines for the same transition may become visible
before or after your handler runs — do not build logic that depends on their
relative order.

### `AdaptiveDualWrite.Reset`

`Reset` delivers its recovery events once after both clusters are reset, so an
uncontended handler sees both clusters healthy. Delivery is shared, so a
concurrent transition can deliver cluster A's recovery before cluster B is
reset, and a handler racing a `Reset` may observe a partially applied reset.
Per-cluster order always holds.

A handler that re-enables traffic on a `write_recovered` should therefore call
`IsDegraded` for both clusters rather than infer global state from one event.

---

## Shutdown Semantics

`CQLClient.Close` stops event intake, drains any buffered events to the
handler, and waits for the in-flight handler invocation to return before
`Close` itself returns.

**The handler must never call `Close` synchronously.** `Close` waits for the
current handler call to finish, so calling `Close` from inside that call
waits on itself forever. Trigger shutdown from another goroutine instead:

```go
helix.WithOnClusterEvent(func(ev types.ClusterEvent) {
    if ev.Kind == types.EventReplayDropped {
        go client.Close() // fine — runs outside the handler invocation
    }
}),
```

Events emitted concurrently with `Close` — including terminal
session-refresh or drain events from in-flight background work — may be
dropped. Every drop is still counted internally. Totals are logged while the
dispatcher is running and once more at shutdown; drops that occur after that
final report are counted but not logged, since there is no dispatcher
goroutine left to log them.

The final drop report and handler-panic recovery both use the configured
`Logger`. A `Logger` call that blocks therefore delays delivery and, in turn,
delays `Close`.

---

## Relationship to Other Hooks

Cluster events are one of several observability surfaces. Use the one that
fits what you need:

| Hook | Use it for |
|---|---|
| `helix.WithOnClusterEvent` | Alerting/paging across all 13 event kinds in one place. |
| `helix.WithOnReplayDropped` | Access to the full dropped `types.ReplayPayload` (query, args, target cluster) — not just the fact that a drop happened. It covers **both** replay paths: it fires alongside `EventReplayDropped` for a primary-path drop and alongside `EventMirrorReplayDropped` for a mirror-path drop. Nothing in the callback signature tells the two apart — mirror payloads carry a fixed conventional `TargetCluster`, so a handler that re-drives dropped payloads against the primary clusters would also re-drive mirror-destined ones. |
| `mirror.WithOnError` | Full control over mirror write failures. Supplying this option **replaces** Helix's internal mirror error handler entirely — and with it, `EventMirrorReplayDropped` stops firing, because that event is emitted by the internal handler you just replaced. This is existing "caller options win" behavior, not a special case for events. |
| `replay.WithOnDrop` | Worker-side permanent drops (a replay exhausted its retry budget), a different failure mode from enqueue-time drops. |
| A `types.MetricsCollector` (e.g. `contrib/metrics/vm`) | Rates, current state, and dashboards, for the kinds that have a metric counterpart. See [Events and Metrics](#events-and-metrics). |

---

## Events and Metrics

Metrics do not lose anything, and events do, so a metric is the better source
wherever one exists. But the coverage is not complete, and one pair does not
line up.

| Event kind | Metric counterpart |
|---|---|
| `failover` | `{prefix}_failover_total{from,to}` — same call site, same meaning. |
| `read_divergence` | `{prefix}_read_divergence_total{cluster}` — same call site, same meaning. |
| `circuit_breaker_open` / `_closed` | `{prefix}_circuit_breaker_trips_total{cluster}` and the `{prefix}_circuit_breaker_state{cluster}` gauge. |
| `drain_entered` / `drain_exited` | `{prefix}_drain_mode_entered_total{cluster}` / `{prefix}_drain_mode_exited_total{cluster}`. |
| `session_refresh_attempt` / `_success` / `_error` | `{prefix}_session_refresh_attempt_total{cluster}` and siblings. |
| `replay_dropped` | `{prefix}_replay_dropped_total{cluster}` — **counts more than the event does**, see below. |
| `write_degraded` / `write_recovered` | **None.** `AdaptiveDualWrite` writes no transition counter and no degraded-state gauge. `{prefix}_write_async_total` is the nearest proxy, but it counts individual fire-and-forget writes, so it needs live write traffic and reports neither a transition count nor current state. |
| `mirror_replay_dropped` | **None.** `{prefix}_mirror_enqueue_dropped_total` is a different failure — the mirror engine's own ring buffer rejecting a capture, not the replay enqueue failing. |

For the three kinds with no counterpart, the event is the only machine-readable
signal, and it is lost if the handler falls behind. Two of them —
`write_degraded` and `mirror_replay_dropped` — are what the Quick Start pages
on. If you need a durable record, log them from inside the handler.

**`replay_dropped` and `{prefix}_replay_dropped_total` do not agree.** The
event fires only for an enqueue-time drop in the client. The metric is
incremented there *and* by the replay workers when a payload exhausts its
retry budget — a worker-side permanent drop, which emits no cluster event.
An operator comparing the two will find the counter higher, and the gap is not
evidence that events were dropped. To observe worker-side drops, register
`replay.WithOnDrop`.

---

## Standalone Policy Usage

`helix.NewCQLClient` wires the event dispatcher into your configured
`WriteStrategy` and `FailoverPolicy` automatically when you register
`WithOnClusterEvent`. If you construct a policy directly — outside a
`CQLClient`, for example in a test harness or a custom strategy — call
`SetEventEmitter` yourself:

```go
cb := policy.NewCircuitBreaker(policy.WithThreshold(3))
cb.SetEventEmitter(myEmitter) // myEmitter implements types.ClusterEventEmitter

adw := policy.NewAdaptiveDualWrite()
adw.SetEventEmitter(myEmitter)
```

`SetEventEmitter` is available on `CircuitBreaker`, `LatencyCircuitBreaker`,
and `AdaptiveDualWrite`. The emitter is always invoked outside any policy
state lock, so a slow emitter cannot deadlock or stall a policy transition.
An emitter that reenters the policy it is receiving events from (calling back
into it from inside `EmitClusterEvent`) is safe but discouraged.

**Cost of a slow emitter.** It delays the read/write goroutine that performed
the transition, and by more than one emitter call. Each policy holds a queue
of pending events, and one goroutine at a time delivers from it: whichever
goroutine wins that role delivers everything queued, then re-checks and
delivers again if more arrived while it was working. So under concurrent
transitions a single unlucky goroutine can end up making other goroutines'
emitter calls as well, and its delay is not bounded by one call.

**Queue limit.** Each policy queue holds 64 pending events. Past that, the
newest event is dropped. Drops here are counted internally and never logged or
exposed, so a wedged custom emitter loses events silently. The same counter
also absorbs two other cases: events left with nowhere to go when
`SetEventEmitter(nil)` removes the emitter while a delivery is in progress,
and an event whose emitter call panics — the panic is contained and delivery
continues with the next event, but the panic value is not logged.

Helix's own dispatcher, installed by `WithOnClusterEvent`, is non-blocking, so
none of this applies to the in-client path. It matters when you supply your
own emitter.

---

## See Also

- [Strategy & Policy Guide](strategy-policy.md) — `CircuitBreaker`, `LatencyCircuitBreaker`, and `AdaptiveDualWrite` configuration behind most of these events.
- [Session Refresh Guide](session-refresh.md) — the detector behind `session_refresh_*` events.
- [Replay System](replay-system.md) — the durability story behind `replay_dropped`.
- [Mirror Guide](mirror.md) — mirror writes and `mirror_replay_dropped`.
- [Auto-Recovery Guide](auto-recovery.md) — how failover, circuit breakers, and adaptive writes fit together operationally.
- [AdaptiveDualWrite Guide](adaptive-dual-write.md) — degrade/recover thresholds behind `write_degraded` / `write_recovered`.
