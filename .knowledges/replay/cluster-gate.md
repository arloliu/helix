---
type: Mechanic
title: Cluster gate
description: How a replay worker holds execution back per cluster without spending attempts, retry windows, or NATS delivery budget.
tags: [replay, drain, worker]
status: draft
generated: {by: "claude/fable-5.1", at: 2026-09-03T00:00:00Z}
sources:
  - {resource: replay/worker.go}
  - {resource: replay/memory_worker.go}
  - {resource: replay/memory_retained.go}
  - {resource: replay/nats_worker.go}
  - {resource: wiring.go}
---

# What it does

Answers: what happens to queued replay for a cluster the operator has drained or quarantined, and why nothing is lost or charged while it waits.

# How it works

`WorkerConfig.ClusterGate` (set by `WithClusterGate`, composing by AND) is consulted through `WorkerConfig.allows`, which treats a panicking gate as closed.
The memory dequeue rotation skips a gated cluster (the gate is evaluated before the replayer's mutex is taken).
A payload the gate refuses between dequeue and execution is put back with `Enqueue` under `RetryBounded`, or parked in the retained scheduler with `gatedSince` set under `RetryWhileRetained`; when it finally runs, `firstAt` is moved forward by the parked time so the retry window is not consumed.
A bounded retry waits in `waitUngated`, polling every `PollInterval`, without counting an attempt.

The NATS loop skips the fetch for a gated cluster so messages stay server-side.
A batch already fetched when the gate closes is held by `holdWhileGated`: the gate is polled every `PollInterval`, every unprocessed message's `InProgress` is refreshed once per `max(PollInterval, AckWait/3)`, nothing is NAK'd, and a stop NAKs each unprocessed message once through `nakTail`.

The client composes drain with `WithReplayGate` in `replayAllowed` and appends `WithClusterGate(replayAllowed)` after the caller's options on the worker it builds for `WithAutoMemoryWorker`; mirror workers and supplied workers receive no gate.

# Invariants

- A gated payload never reaches the executor, never counts an attempt, and never consumes a delivery.
- Reopening is observed within `PollInterval`.
- Every gate wait selects on `stopCh`, so `Worker.Stop` cannot hang on a closed gate.

# Where to look

- `replay/worker.go` → `WithClusterGate`, `(*WorkerConfig).allows`
- `replay/memory_retained.go` → `park`, `attemptRetained`
- `replay/nats_worker.go` → `holdWhileGated`, `nakTail`
- `wiring.go` → `(*CQLClient).replayAllowed`
