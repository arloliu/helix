---
type: Mechanic
title: Observation hub
description: How every health observation reaches the read strategy, failover policy, latency recorder, and session-liveness stats in one fixed order.
tags: [root, reads, writes, auto-refresh, failover]
status: draft
generated: {by: "claude/fable-5.1", at: 2026-09-03T00:00:00Z}
sources:
  - {resource: health.go}
  - {resource: cql_client.go}
  - {resource: wiring.go}
---

# What it does

Answers: which authority hears about a read, iterator close, or write leg, in what order, and why a report that arrives after a session swap cannot poison the new session's auto-refresh stats.
`docs/session-refresh.md` documents the detector's predicates but not who writes its inputs.

# How it works

`clusterHealth` (`health.go`) is the only writer of `clusterStats`, and the stats live on the `sessionHolder` an attempt used rather than on the client.
Every read attempt (`runPrimaryRead`, the failover retry in `tryFallbackCluster`, the `FallbackRead` probe in `executeFallbackRead`), every iterator close, and every write leg loads the holder through `holderFor` and reports through one typed entry point:

- `readSucceeded`: `ReadStrategy.OnSuccess` unless an `AllowedClusters` override froze the strategy; `LatencyRecorder.RecordLatency` when the policy implements it, else `FailoverPolicy.RecordSuccess`; then the holder's stats.
- `readFailed`: `IncReadError`; the holder's stats when the kind is a cluster error; `FailoverPolicy.RecordFailure`. `ReadStrategy.OnFailure` is not an observation and stays in the failover flow after `ShouldFailover`.
- `iterClosed`: stats first (except a caller-context error), then strategy and policy; the policy gets `RecordSuccess`, never `RecordLatency`.
  It is the only entry point that calls `ReadStrategy.OnFailure`, because an iterator cannot be retried and so has no failover flow to leave it to.
  It asks the client's `failoverAllowed` gate first, so a close applies the same policy and drain rules a failing `Scan` does before moving the preference.
- `writeLeg`: stats only; an async, dropped, skipped, or caller-cancelled leg records nothing.
- `deferredWriteLeg`: `writeLeg` for a leg a strategy completed in the background.
- `probe`: a recovery probe's outcome; success and failure reach the stats, an abandoned probe records nothing.

A failure reaches the stats only if the auto-refresh failure classifier (`AutoRefreshConfig.FailureClassifier`, default `DefaultAutoRefreshFailureClassifier`) counts it as a connectivity failure.
A schema or query error proves the session reachable, so it leaves the stats untouched.
Every entry point that records a failure passes it through, and it gates the stats alone: a read's `IncReadError` and `FailoverPolicy.RecordFailure` have already happened by then.

The entry points deliberately do not share one clock, and the differences are load-bearing rather than drift.
Reads, an iterator's close, and a probe sample the configured `NowProvider` as they report, because reporting is when the outcome is known.
`writeLeg` is given the clock its caller captured when the leg returned, so a leg whose result is aggregated later keeps the time it actually ended.
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

# Where to look

- `health.go` → `clusterHealth`, `clusterStats.succeeded`, `clusterStats.failed`
- `cql_client.go` → `sessionHolder`, `holderFor`, `statsForCluster`
- `write_path.go` → `writeLegState`, `writeLeg`, `reportWriteLegs`
- `read_path.go` → `runPrimaryRead`, `primaryReadOutcome`, `attemptRead`
- `wiring.go` → `newClusterHealth` (binds the client's `failoverAllowed` gate)
