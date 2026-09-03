---
type: Mechanic
title: Circuit breaker probe reservation
description: How an open breaker is closed by a client-run probe instead of a caller's read, and how a reservation token keeps a stale probe from settling a breaker that moved on.
tags: [policy, failover, circuit-breaker, recovery-probe]
status: draft
generated: {by: "claude/fable-5.1", at: 2026-09-03T00:00:00Z}
sources:
  - {resource: policy/failover_policy.go}
  - {resource: recovery_probe.go}
---

# What it does

Answers: after a breaker trips, what closes it, and why `ShouldFailover` stays true after `resetTimeout` has elapsed.
`docs/strategy-policy.md` documents the state diagram; this entry records the token and ordering mechanics.

# How it works

Each cluster's state (`breakerState`) holds `tripped`, `halfOpen`, an atomic `open` snapshot, the failure count, the last-failure stamp, and a transition sequence `seq`.
`RecordFailure` counts and stamps; at the threshold it trips and emits `circuit_breaker_open`.
It never closes anything: the timed close of earlier versions is gone.
`RecordSuccess` closes an open or half-open breaker with reason `"operation succeeded"`.

The recovery probe loop (`recoveryProbeLoop`) asks `TryBeginFailoverProbe` on every tick.
The reservation succeeds only when the breaker is open, no probe is in flight, `resetTimeout > 0`, and it has elapsed since the last failure; it sets `halfOpen`, bumps `seq`, reports gauge 1 and a log line, emits no event, and returns `seq` as the token.
`CompleteFailoverProbe(cluster, token, outcome)` settles only while `halfOpen` and `token == seq`: succeeded closes with `"probe succeeded"`, failed returns to open and restamps the last failure (no event, no trip), abandoned (the client cancelled the probe) returns to open without touching counters or the stamp.
An ordinary `RecordSuccess` during a reservation closes the breaker and makes the token stale; an ordinary `RecordFailure` counts and leaves the reservation valid.

`report` is the single choke point for side effects: the trip counter is cumulative and always written, the gauge and log line are written only while `seq` is still the latched value (under `reportMu`), and events drain last.

# Invariants

- `open` mirrors `tripped`; half-open keeps `open` true, so `ShouldFailover` and `VetoRoute` stay true until a close.
- At most one reservation per cluster; a completion with a stale token has no effect.
- A breaker that is never probed (`resetTimeout == 0`, probe disabled, single-cluster client) stays open until a successful operation.

# Where to look

- `policy/failover_policy.go` → `breakerState`, `TryBeginFailoverProbe`, `CompleteFailoverProbe`, `report`
- `recovery_probe.go` → `recoveryProbeLoop`, `probeDemand`
