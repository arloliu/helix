---
type: Mechanic
title: Client lifecycle order
description: What fixes the position of each background component in NewCQLClient's start sequence, in a failed constructor's unwind, and in Close.
tags: [root, lifecycle, close, shutdown, events, replay, mirror, recovery-probe]
status: draft
generated: {by: "claude/opus-5.5", at: 2026-09-24T12:00:00Z}
sources:
  - {resource: wiring.go, digest: sha256:56554ceeda09d27e, revision: 3b38f02}
  - {resource: session_lifecycle.go, digest: sha256:d4c019cea9db4a23, revision: 3b38f02}
  - {resource: background_loop.go, digest: sha256:8a2019a45f53ba18, revision: 3b38f02}
  - {resource: recovery_probe.go, digest: sha256:8f5abaeb20268c68, revision: 3b38f02}
  - {resource: mirror_dispatch.go, digest: sha256:2cfa64f1379d0b49, revision: 3b38f02}
  - {resource: cql_client.go, digest: sha256:a5f7a894a7a06b54, revision: 3b38f02}
---

# What it does

Answers: why each background component starts and stops where it does, and why the two sequences are not mirror images.
The comment table in `Close` covers the shutdown half and its reasons.
It leaves out the start half,
whose constraints are spread over `buildCQLClient`, `createEventDispatcher`, and `startEventDelivery`,
and the constructor's error unwind, which stops less than `Close` does.

# How it works

`buildCQLClient` starts components in this order, each position forced by a neighbour:

1. `createEventDispatcher` builds the dispatcher without starting delivery.
   It must precede mirror setup, because the mirror error handler captures the dispatcher by value when it is built.
2. `setupMirror` starts the mirror engine, then the mirror replay worker; it can fail.
3. `topology.start(watchTopology)`, when a `TopologyWatcher` is configured; it cannot fail.
4. `startReplayWorker` hands the worker the dispatcher and starts it; it can fail.
5. `startEventDelivery` injects the emitter into the strategy and policy and starts the dispatcher goroutine.
   It follows the last step that can fail, so no error path leaves a delivery goroutine behind,
   and precedes the auto-refresh and probe goroutines,
   so a probe that succeeds on its first tick cannot race the emitter installation and lose its event.
6. `autoRefresh.start(autoRefreshLoop)`, only with auto-refresh enabled and a `SessionRefresher` registered.
7. `startRecoveryProbes` starts one loop per cluster through `recoveryProbe.start`.

`Close` stops them by dependency, not in reverse:
the topology, auto-refresh, and probe loops are all cancelled and then all joined;
then `deferred.wait`;
then the mirror engine and mirror replay worker;
then the replay worker;
then the dispatcher;
then retired and installed sessions.
`deferred.wait` and the sessions have no start step, which is one reason a single start/stop list does not fit.

The three loops share `backgroundLoop`:
`start` creates the context and launches the goroutines, `stop` cancels, `wait` joins.
`stop` and `wait` are separate so `Close` can cancel all three loops before joining any of them.
The loops read `backgroundLoop.ctx` directly rather than taking it as a parameter.

# Invariants

- The dispatcher is created before `setupMirror` and started after `startReplayWorker`.
- `startEventDelivery` runs before `autoRefresh.start` and `startRecoveryProbes`.
- In `Close`, every component that can emit an event is joined before `runtime.events.stop`.
- In `Close`, `deferred.wait` returns before the replay worker stops,
  so a write still in progress enqueues its replay first.
- In `Close`, the recovery probe is joined before `deferred.wait`,
  so the probe cannot lift a degraded cluster's replay hold (`degradedWriteReporter.holdsReplay`) during shutdown.
  A deferred leg that completes fast still credits recovery and can lift it.
- A step that can fail in `buildCQLClient` unwinds only what started before it;
  nothing after the last failable step can be reached on an error path.

# Failure modes

- A `startReplayWorker` failure joins the topology watcher before `buildCQLClient` returns.
  A `TopologyWatcher.Watch` call that never returns therefore blocks a failed constructor,
  as it already blocks `Close`.
- A `setupMirror` failure aborts only the dispatcher;
  `setupMirrorTargetMode` stops its own engine when the mirror replay worker fails to start.
- A custom `RecoveryProbe.Probe` or `SessionRefresher` that ignores its context blocks `Close` at the first join,
  before any write, mirror, or replay work drains.
- Moving the dispatcher's `start` earlier than `startReplayWorker` leaks the delivery goroutine on a worker start failure.

# Where to look

- start sequence: `wiring.go` → `buildCQLClient`
- dispatcher creation and delivery: `wiring.go` → `createEventDispatcher`, `startEventDelivery`, `abortEventDispatcher`
- replay worker start: `wiring.go` → `startReplayWorker`
- replay hold: `wiring.go` → `degradedWriteReporter.holdsReplay`
- mirror start and stop: `mirror_dispatch.go` → `setupMirror`, `setupMirrorTargetMode`, `stopMirrorComponents`
- probe start: `recovery_probe.go` → `startRecoveryProbes`
- loop lifecycle: `background_loop.go` → `backgroundLoop`
- loop fields: `cql_client.go` → `CQLClient`
- shutdown sequence: `session_lifecycle.go` → `Close`
