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
- `writeLeg`: stats only; an async, dropped, skipped, or caller-cancelled leg records nothing.

A single-cluster client updates stats but calls no policy, and calls the strategy only for a clean iterator close.
Classification (`classifyReadErr`, `classifyWriteLeg`) runs at the call site while the context is live; the hub receives the kind and the original error.

# Invariants

- Write outcomes never reach the failover policy or the read strategy.
- A report from an attempt that used a replaced holder lands on that holder; the installed holder's stats hold only its own observations.
- `maybeAutoRefresh` reads the installed holder's stats; the refresh throttle (`lastRefreshA/B`) stays on the client so it survives the swap it caused.

# Where to look

- `health.go` → `clusterHealth`, `clusterStats.succeeded`, `clusterStats.failed`
- `cql_client.go` → `sessionHolder`, `holderFor`, `statsForCluster`
- `write_path.go` → `writeLegState`, `writeLeg`
