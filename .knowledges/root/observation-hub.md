---
type: Mechanic
title: Observation hub
description: How every health observation reaches the read strategy, failover policy, latency recorder, and session-liveness stats in one fixed order.
tags: [root, reads, writes, auto-refresh, failover]
status: draft
generated: {by: "claude/fable-5.1", at: 2026-09-17T00:00:00Z}
sources:
  - {resource: health.go, digest: sha256:7ad4310eb455affe, revision: a11afcd}
  - {resource: cql_client.go, digest: sha256:8a9e4cf21e072973, revision: a11afcd}
  - {resource: wiring.go, digest: sha256:25307e6ab996ef5d, revision: a11afcd}
  - {resource: read_path.go, digest: sha256:1066e8ece11e92fc, revision: a11afcd}
---

# What it does

Answers: which authority hears about a read, iterator close, or write leg, in what order, and why a report that arrives after a session swap cannot poison the new session's auto-refresh stats.
`docs/session-refresh.md` documents the detector's predicates but not who writes its inputs.

# How it works

`clusterHealth` (`health.go`) is the only writer of `clusterStats`, and the stats live on the `sessionHolder` an attempt used rather than on the client.
Every read attempt (`runPrimaryRead`, the failover retry in `tryFallbackCluster`, the `FallbackRead` probe in `executeFallbackRead`), every iterator close, and every write leg loads the holder through `holderFor` and reports through one typed entry point:

- `readSucceeded`: `ReadStrategy.OnSuccess` unless an `AllowedClusters` override froze the strategy; `LatencyRecorder.RecordLatency` when the policy implements it, else `FailoverPolicy.RecordSuccess`; then the holder's stats.
- `readFailed`: `IncReadError` first, for every kind `readErrKind.isReadError()` accepts —
  a cluster error or a statement the coordinator rejected.
  It then returns unless `kind.isHealthSignal()`, which holds only for a cluster error:
  a rejected statement stops at the metric, reaching neither the holder's stats nor `FailoverPolicy.RecordFailure`.
  `ReadStrategy.OnFailure` is not an observation and stays in the failover flow after `ShouldFailover`.
- `iterClosed`: stats first (except a caller-context error or a rejected statement), then strategy and policy;
  the policy gets `RecordSuccess`, never `RecordLatency`.
  A rejected statement is its own case:
  only `IncReadError`, the same metric-only stop `readFailed` takes,
  because the cluster answered and its health is not in question.
  It is the only entry point that calls `ReadStrategy.OnFailure`, because an iterator cannot be retried and so has no failover flow to leave it to.
  It asks the client's `failoverAllowed` gate first, so a close applies the same policy and drain rules a failing `Scan` does before moving the preference.
- `writeLeg`: stats only; an async, dropped, skipped, or caller-cancelled leg records nothing.
- `deferredWriteLeg`: `writeLeg` for a leg a strategy completed in the background.
- `probe`: a recovery probe's outcome; success and failure reach the stats, an abandoned probe records nothing.

A failure reaches the stats only if the auto-refresh failure classifier (`AutoRefreshConfig.FailureClassifier`, default `DefaultAutoRefreshFailureClassifier`) counts it as a connectivity failure.
A schema or query error proves the session reachable, so it leaves the stats untouched.
Every entry point that records a failure passes it through, and it gates the stats alone.
A failing read runs the metric first, then the classifier and stats, then `FailoverPolicy.RecordFailure` — so the policy hears about a failure the classifier kept out of the stats.
This sequence is for a cluster error only:
a rejected statement's `readFailed` call returns after the metric and never reaches the classifier, the stats, or the policy.

The entry points deliberately do not share one clock, and the differences are load-bearing rather than drift.
Reads, an iterator's close, and a probe sample the configured `NowProvider` as they report, because reporting is when the outcome is known.
`writeLeg` is given the clock its caller captured once the strategy returned, shared by both legs, so the hub cannot re-sample it after the caller has aggregated the results.
A leg that finished early is reported at that shared time rather than its own; what is fixed at the return of a leg that ran is the caller-cancelled provenance, not the timestamp.
A leg whose write panics never reaches that record, because the panic-to-error recovery sits in the caller
(`safeCQLWrite`, or the strategy's own `safeWrite`) outside the closure;
such a leg carries the zero provenance and is classified as the cluster's failure.
The single-cluster fast paths in `query.go` and `batch.go` call `writeLeg` directly and pass `NowProvider` as they report,
having no sibling leg to share a time with.
`deferredWriteLeg` takes the process clock, because it runs while `Close` waits on the leg's deferred registration and so must not call a user-supplied `NowProvider` that `Close` could be blocking.

A holder is optional only for `writeLeg`, whose leg may never have reached a session.
Every other entry point is reached with the holder its attempt used, loaded through `holderFor` and dereferenced immediately.

A single-cluster client updates stats but calls no policy, and calls the strategy only for a clean iterator close.
Classification (`classifyReadErr`, `classifyWriteLeg`) runs at the call site while the context is live; the hub receives the kind and the original error.

# Invariants

- Write outcomes never reach the failover policy or the read strategy.
- A report from an attempt that used a replaced holder lands on that holder; the installed holder's stats hold only its own observations.
- `maybeAutoRefresh` reads the installed holder's stats; the refresh throttle (`lastRefreshA/B`) stays on the client so it survives the swap it caused.
- The hub never chooses a failover target; that is the failover flow's. Its only say in routing is asking the client's gate whether an iterator's close may move the strategy's preference.
- The gate is a constructor argument, so a hub cannot exist without one.
- Every kind `isHealthSignal` accepts is also accepted by `isReadError`, but not the reverse:
  a rejected statement is a read error (`IncReadError` fires) without being a health signal (nothing else does).

# Where to look

- `health.go` → `clusterHealth`, `clusterStats.succeeded`, `clusterStats.failed`
- `cql_client.go` → `sessionHolder`, `holderFor`, `statsForCluster`
- `write_path.go` → `writeLegState`, `writeLeg`, `reportWriteLegs`
- `read_path.go` → `runPrimaryRead`, `primaryReadOutcome`, `attemptRead`
- `wiring.go` → `newClusterHealth` (binds the client's `failoverAllowed` gate)
