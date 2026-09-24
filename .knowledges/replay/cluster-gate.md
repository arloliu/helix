---
type: Mechanic
title: Cluster gate
description: How a replay worker holds execution back per cluster without spending attempts, retry windows, or NATS delivery budget, and what the client feeds into that gate.
tags: [replay, drain, worker, degraded]
status: draft
generated: {by: "claude/opus-5.5", at: 2026-09-24T00:00:00Z}
sources:
  - {resource: replay/worker.go, digest: sha256:2fa9560aa0c91db9, revision: 6d03193}
  - {resource: replay/memory_worker.go, digest: sha256:be7d973871ab0fbf, revision: 6d03193}
  - {resource: replay/memory_retained.go, digest: sha256:63bfa5172d68dfdd, revision: 6d03193}
  - {resource: replay/memory.go, digest: sha256:11fc60209e78cf28, revision: 6d03193}
  - {resource: replay/nats_worker.go, digest: sha256:e888ae3e8ddf16c8, revision: 6d03193}
  - {resource: wiring.go, digest: sha256:56554ceeda09d27e, revision: 6d03193}
  - {resource: recovery_probe.go, digest: sha256:8f5abaeb20268c68, revision: 6d03193}
  - {resource: cql_client.go, digest: sha256:a5f7a894a7a06b54, revision: 6d03193}
  - {resource: policy/adaptive_write.go, digest: sha256:3d13eb9178c1e948, revision: 6d03193}
---

# What it does

Answers: what happens to queued replay for a cluster the operator has drained or quarantined, or the write strategy has marked degraded,
and why nothing is lost or charged while it waits.

# How it works

`WorkerConfig.ClusterGate` (set by `WithClusterGate`, composing by AND) is consulted through `WorkerConfig.allows`, which treats a panicking gate as closed.
The memory backend runs one dequeue loop per cluster, and each loop passes `tryDequeueRetained` an own-cluster filter (`memoryBackend.ownCluster`) that refuses the sibling before consulting `allows`, so in the dequeue rotation a cluster's gate is called by its own loop only.
The rotation still evaluates that filter for both queue indexes, before the replayer's mutex is taken, and skips the one it refuses.
A payload the gate refuses between dequeue and execution is put back at the tail of its queue by `requeueGated` under `RetryBounded`,
which calls `(*MemoryReplayer).requeue` rather than `Enqueue`:
the worker still holds the payload's capacity slot, so no slot is reserved and the requeue cannot fail admission.
Under `RetryWhileRetained` it is instead parked in the retained scheduler with `gatedSince` set;
when it finally runs, `firstAt` is moved forward by the parked time so the retry window is not consumed.
A bounded retry waits in `waitUngated`, polling every `PollInterval`, without counting an attempt.

The NATS loop skips the fetch for a gated cluster so messages stay server-side.
A batch already fetched when the gate closes is held by `holdWhileGated`: every unprocessed message's `InProgress` is refreshed at once and then once per `AckWait/3`,
and the gate is polled every `min(PollInterval, AckWait/3)`, so a `PollInterval` longer than `AckWait` cannot let a lease expire;
with `AckWait` unset both cadences fall back to `PollInterval`.
Nothing is NAK'd while held, and a stop NAKs each unprocessed message once through `nakTail`.

Per-cluster loops do not make the gate redundant: they keep a hung cluster from holding its sibling's payloads, while the gate is what stops the worker spending attempts, log volume and (under `RetryBounded`) the attempt budget on a cluster already known to be bad.
Two couplings survive both: the 100-slot retry pool is shared, so when both clusters fail a hung cluster's retries delay (retained) or drop (bounded, `retry_pool_saturated`) the other's; and queue capacity is shared, so a hung cluster's backlog can reach the limit and fail the healthy cluster's enqueues with `types.ErrReplayQueueFull`.

The client composes drain, the write strategy's degraded state, and `WithReplayGate` in `replayAllowed`,
and appends `WithClusterGate(replayAllowed)` after the caller's options on the worker it builds for `WithAutoMemoryWorker`;
mirror workers and supplied workers receive no gate.

`newDegradedWriteReporter` resolves the degraded authority once at construction, before the gate is installed.
It returns nil under exactly the conditions that leave `startRecoveryProbes` with no probe for the write strategy:
`recoveryProbeOff`, single-cluster mode, or a `WriteStrategy` that is not a `ProbeReporter`.
The probe is the only release path independent of caller traffic, so without it the hold would wait for an operator.
`degradedWriteReporter.holdsReplay` is `IsDegraded(cluster) && !IsLatched(cluster)`, the same rule `recoveryProbeLoop` uses to decide whether to probe;
a nil receiver means "does not hold", while a nil `LatchReporter` means "never latched", so degraded alone holds replay back.
The latch term is what lets the `ForceDegrade` → drain → `ForceRecover` workflow finish:
a latched cluster keeps receiving replay.
The two atomics are read separately,
so a `ForceDegrade` in progress can be seen as degraded-and-unlatched for one `PollInterval`.

# Invariants

- A gated payload never reaches the executor, never counts an attempt, and never consumes a delivery.
- A cluster's gate is never consulted by the sibling cluster's dequeue loop.
  It is consulted by its own loop and, on the memory backend, by the retry goroutines holding that cluster's payloads (`waitUngated`, `attemptRetained`).
- Reopening is observed within `PollInterval`.
- Every gate wait selects on `stopCh`, so `Worker.Stop` cannot hang on a closed gate.
- The degraded condition is never the only thing holding a cluster back where no recovery probe runs for the write strategy.
- A latched cluster is never held back by the degraded condition.

# Where to look

- `replay/worker.go` → `WithClusterGate`, `(*WorkerConfig).allows`
- `replay/memory_worker.go` → `(*memoryBackend).ownCluster`, `(*memoryBackend).dequeueLoop`, `(*memoryBackend).requeueGated`, `(*memoryBackend).waitUngated`
- `replay/memory.go` → `(*MemoryReplayer).tryDequeueWithPriority`, `(*MemoryReplayer).requeue`
- `replay/memory_retained.go` → `(*memoryBackend).park`, `(*memoryBackend).attemptRetained`
- `replay/nats_worker.go` → `(*natsBackend).holdWhileGated`, `(*natsBackend).nakTail`
- `wiring.go` → `(*CQLClient).replayAllowed`, `newDegradedWriteReporter`, `(*degradedWriteReporter).holdsReplay`
- `cql_client.go` → `degradedWriteReporter`
- `recovery_probe.go` → `(*CQLClient).startRecoveryProbes`, `probeReporters`, `(*CQLClient).recoveryProbeLoop`
- `policy/adaptive_write.go` → `(*AdaptiveDualWrite).ForceDegrade`, `(*AdaptiveDualWrite).IsLatched`
