---
type: Mechanic
title: Replay outcome classification
description: How a normal dual write turns per-cluster results into a caller result and replay work.
tags: [root, writes, replay, adaptive-write, drain]
status: draft
generated: {by: "claude/opus-5", at: 2026-09-18T00:00:00Z}
sources:
  - {resource: write_path.go, digest: sha256:7be28d0c3c60adc1, revision: 6352f94}
---

# What it does

Answers: how does the CQL dual-write path classify a pair of cluster outcomes, count them,
pick the caller's result, and decide which legs to replay?
`docs/strategy-policy.md` and `docs/strict-write.md` document the public result shapes,
but not the per-leg kinds, where each counter is emitted, or when a background leg's replay is decided.

# How it works

`(*CQLClient).executeWriteWithReplay` is the entry.
A closed client returns `ErrSessionClosed`; single-cluster mode writes cluster A directly.
With both clusters draining it returns before any strategy runs:
each leg goes through `recordWriteLegMetrics` as `legDraining`
(`IncWriteTotal` and, for a `types.StrictMetrics` collector, `IncWriteSkipped`; no duration),
then a plain write returns `ErrBothClustersDraining` and a strict one a `DualClusterError` of two `ErrClusterDraining`.
Nothing is replayed and the observation hub hears nothing.

Otherwise `executeDualWrite` builds both legs with `writeLegs`.
A draining cluster's leg returns `ErrClusterDraining` before touching the session or its start time.
A leg that runs records, when it returns, whether the caller's context was already done.
The legs run through the configured `WriteStrategy`, or inline concurrently through `safeCQLWrite` when none is set.

`reportWriteLegs` then classifies each result into a `writeLegErrKind`
(`legOK`, `legAsync`, `legDropped`, `legDraining`, `legSkipped`, `legCanceled`, `legFailed`),
records per-leg metrics through `recordWriteLegMetrics`, and reports each leg to the observation hub.
A failure recorded after the caller's context ended is `legCanceled`: replayed, but neither a write error nor a health signal.
A draining leg that a fire-and-forget strategy handed back as `ErrWriteAsync` is `legDraining`, not `legAsync`:
`writeLeg` stamps `writeLegState.draining` when it builds the leg, and `classify` checks it before any error kind,
because the leg's closure returned before touching the session and nothing is in flight.

The non-strict path aggregates on the kinds.
Both nil returns nil.
Both `failed()` (`legFailed` or `legCanceled`) returns `DualClusterError` and replays nothing.
Otherwise `replayLeg` runs for each leg, and any `legOK` returns nil.
With no acknowledgement at all, `AckOnReplayAdmission` returns nil if a replay was admitted;
every other case returns `NoSynchronousAckError` carrying both results and the replay error.

`replayLeg` enqueues every non-OK leg immediately, except a `legAsync` leg whose error implements `DeferredWriteResult`.
For that one it snapshots the payload now and registers an `OnComplete` callback on `c.deferred`.
The callback classifies the late result on `context.WithoutCancel(ctx)`, reports it to the hub through `deferredWriteLeg`,
and admits a replay only if the late result is an error.
If the leg had already finished, the callback runs inline and its admission error is returned synchronously.

`admitReplayPayload` counts `IncReplayDropped` and returns `ErrNoReplayer` without a replayer.
With one it enqueues on `context.WithoutCancel(ctx)`,
counting `IncReplayEnqueued` and logging by kind on success, or `IncReplayDropped` on failure.
`enqueueReplayPayload` adds the replay-dropped callback and event on failure.

The strict path, `executeStrictDualWrite`, shares `writeLegs` and `reportWriteLegs` but discards the kinds.
It aggregates on the raw results, never replays, and never fire-and-forgets.

# Invariants

- `reportWriteLegs` is the only place a synchronous dual-write leg is counted or reported,
  so the plain and strict paths cannot drift.
- A draining leg never contacts its session and is never timed.
- A both-draining write counts each leg once in `IncWriteTotal` and once in `IncWriteSkipped`, plain and strict alike.
- A draining leg is never `legAsync`, whatever the strategy returned, so it is counted as skipped and replayed at once.
- A `legAsync` leg observes no duration in the foreground; its late result only feeds the hub and its replay decision.
- Replay admission never depends on the caller's context staying live.
- `Close` waits on `c.deferred` until every background leg's callback has run.

# Failure modes

- Without a replayer every unacknowledged leg is counted as a dropped replay and reported through the drop callback.
- A panic in user code inside the completion callback still releases the `c.deferred` hold, so `Close` does not hang.

# Where to look

- entry and both-draining return: `write_path.go` → `(*CQLClient).executeWriteWithReplay`
- per-leg classification and metrics: `write_path.go` → `(*CQLClient).reportWriteLegs`, `(*CQLClient).recordWriteLegMetrics`, `(*writeLegState).classify`, `classifyWriteErr`
- aggregation: `write_path.go` → `(*CQLClient).executeDualWrite`
- background-leg replay: `write_path.go` → `(*CQLClient).replayLeg`
- enqueue and cancellation boundary: `write_path.go` → `(*CQLClient).admitReplayPayload`
