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
