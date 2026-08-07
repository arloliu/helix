---
type: Mechanic
title: Replay outcome classification
description: How a normal dual write turns per-cluster results into a caller result and replay work.
tags: [root, writes, replay, adaptive-write]
status: stable
generated: {by: "codex/gpt-5.6", at: 2026-08-05T15:02:01Z}
verified:
  - {by: "codex/gpt-5.6-sol", at: 2026-08-07T12:58:14Z}
sources:
  - {resource: cql_client.go, digest: sha256:530b8caaf3e3c9bb, revision: 864ff2e}
---

# What it does

Answers: how does the normal CQL dual-write path classify a pair of cluster outcomes before returning and optionally enqueueing replay? `docs/strategy-policy.md` documents the public success/partial-failure shape, but omits the distinct handling of asynchronous and concurrency-dropped legs and the enqueue context's cancellation behavior.

# How it works

`(*CQLClient).executeDualWrite` delegates its two legs to the configured strategy, or runs them concurrently with panic-to-error conversion when no strategy is set. It separates `ErrWriteAsync` and `ErrWriteDropped` from real errors, records metrics and auto-refresh state for both legs, and returns a `DualClusterError` only when both results are real errors.

For any other non-nil result, it returns success to the caller. If a replayer is configured, it calls `enqueueReplayIfNeeded` once per affected cluster. The replay payload preserves the statement, bound values, batch representation, timestamp, and priority; its enqueue uses `context.WithoutCancel(ctx)`, so caller cancellation does not prevent reconciliation admission.

# Invariants

- A leg reporting `ErrWriteAsync` or `ErrWriteDropped` is not treated as a cluster failure for the dual-error decision.
- Two real errors return `DualClusterError` and do not enqueue replay from this path.
- Every other non-nil leg result is eligible for one replay enqueue when a replayer exists.
- A successful enqueue is insulated from cancellation of the original request context.

# Failure modes

- Without a configured replayer, partial, asynchronous, and dropped outcomes still return success but are not reconciled.
- A replay enqueue failure increments the dropped metric, is logged, and emits the replay-dropped notification instead of changing the write result.

# Where to look

- outcome classification: `cql_client.go` → `(*CQLClient).executeDualWrite`
- payload construction and cancellation boundary: `cql_client.go` → `(*CQLClient).enqueueReplayIfNeeded`
