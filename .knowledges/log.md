# Log

## 2026-08-05
* **Creation**: [Replay outcome classification](/root/replay-outcome-classification.md) documents normal dual-write result handling below the public contract.

## 2026-09-13
* **Update**: [Observation hub](/root/observation-hub.md) adds the two entry points it omitted (`deferredWriteLeg`, `probe`), why the entry points do not share one clock, when a holder may be nil, and that the failover gate is a constructor argument.

## 2026-09-17
* **Update**: [Observation hub](/root/observation-hub.md) documents the new `readStatementErr` kind —
  `readFailed` and `iterClosed` now gate on `isReadError()` for the metric and `isHealthSignal()` for everything else,
  so a rejected statement counts a read error without touching stats, the failover policy, or the read strategy.
  Cited `read_path.go` as a source (it was already an uncited pointer target) and backfilled digest/revision for all four sources.

## 2026-09-18
* **Update**: [Observation hub](/root/observation-hub.md) refreshes the `read_path.go` digest;
  the later edits to that file are comments and the routing the entry already describes, so the prose is unchanged.
* **Update**: [Replay outcome classification](/root/replay-outcome-classification.md) rewritten against `write_path.go`;
  it still cited `cql_client.go` and `enqueueReplayIfNeeded`, which no longer exist.
  Now covers the leg kinds, `AckMode` and `NoSynchronousAckError`, deferred background legs,
  a draining leg classified as draining even when the strategy returned it as async,
  and the both-draining return counting each leg in `write_total` and `write_skipped`.
  Status drops to draft pending an independent verification.

## 2026-09-24
* **Creation**: [Client lifecycle order](/root/client-lifecycle-order.md) records the start sequence, the constructor error unwind, and how both relate to the shutdown order Close documents.
* **Update**: [Client lifecycle order](/root/client-lifecycle-order.md) a replay worker start failure now joins the topology watcher instead of only cancelling it.
