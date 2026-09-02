# Design consensus (Claude ⇄ Codex, 2 rounds, 2026-09-02)

Status: converged between the two reviewers; awaiting the maintainer's confirmation.
Sources: round1-claude.md, round1-codex.md, round2-claude.md, round2-codex.md.

## Compatibility framework (Q1)

Three categories, applied per item. `WithBehaviorProfile(Safe)` is pure option-expansion sugar
(no separate code path) and becomes the v2 default.

| Category | Rule | Members |
|---|---|---|
| 1 Contract bug (behaviour contradicts current docs) | Fixed by default in the next minor. Opt-out only where a plausible dependent exists. | zero-sync-ack `nil` → `NoSynchronousAckError` (restore: `AckOnReplayAdmission`); ctx errors counted as cluster failure; `ForceDegrade` auto-cleared; `TargetCluster` fall-through to B; MemoryReplayer arg aliasing; `Iter`+`PageState` cross-cluster |
| 2 Retry/retention defaults bounded by existing caps | Changed in the next minor; CHANGELOG "Behavior change" with the old value as a one-line restore. Must ship with delayed backoff, wall-clock window, and outcome metrics, not just a bigger integer. | `MaxAttempts`, `MaxDeliver` (→ -1 + delayed NAK), memory retry window, NATS `BackOff` |
| 3 Infrastructure-affecting defaults | Unchanged in v1; startup `Warn` once per client; flipped in v2 or via `Safe`. | `Replicas`, `DiscardOld`/`DiscardNew`, route-veto on/off, `WithFailoverBelowThreshold` |

## Release sequencing (Q2, N8)

- `v1.6.1` (patch): arg cloning, diagnostics, doc corrections, startup warnings, opt-in `ReplayRetryPolicy` + delayed-NAK/backoff mechanics. No default change.
- `v1.7.0` (same cycle): Category 1 + 2 defaults; Phase 2a pure file moves only.
- Then: Phase 2b small internal changes → path correctness → per-cluster replay queues → gating/authorities → observability → performance. Failing tests co-land with their fixes.

## Replay (Q3, Q4, Q9, N2, N3, N6, N7, N9)

- Three separate budgets: admission/retention; transient deferral schedule; poison (dead-letter) policy.
- `ReplayDisposition {Defer, Retry, DeadLetter}`; typed unwrap-preserving sentinels from adapters (e.g. `ErrClusterUnreachable`); worker-level `func(error) ReplayDisposition` override; unknown → `Retry`; no string matching. Replay classifier and session-refresh classifier are separate policies sharing the sentinels.
- NATS: consumer `MaxDeliver=-1`, `NakWithDelay` with backoff, worker-owned dead-letter counter keyed by stream sequence (only for `DeadLetter` dispositions; lost on restart is acceptable).
- Memory: capacity reservation held across queued + executing + delayed-retry until success/expiry/shutdown/dead-letter (fixes the release-at-dequeue gap); wall-clock retry window; new admission fails loudly at capacity.
- Replay stays topology-neutral; root injects a per-cluster execution gate driven by drain/quarantine (not by write-degraded state unless an independent resume probe exists). Per-cluster memory queues land before gating.
- Wire: versioned envelope, worker-first dual-read rollout, explicit absent-field semantics; v1 public `ReplayPayload` preserved; subjects/consumers versioned when v2 semantics are emitted.
- Backlog follows the logical cluster slot across `SwapSession`; unknown `TargetCluster` rejected at enqueue/decode/execute; v1 rule: deployment-isolated stream prefixes.
- Idempotency: assume timestamp-LWW replayability; `NonIdempotent()` marker on Query/Batch disables replay; `CounterBatch` implicitly non-idempotent; no CQL parsing.
- Safe profile: `DiscardNew` + bounded retention + dead-letter; one-ack gap stays success + high-severity signal.

## Write acknowledgement (Q8, N4)

- Any pair with zero synchronous acks (real+async, real+dropped, async+async, async+dropped, dropped+dropped) returns `NoSynchronousAckError` carrying both leg results. Default `RequireSynchronousAck`; `AckOnReplayAdmission` returns `nil` only if every required enqueue succeeded.
- `AdaptiveDualWrite`: optional `ExecuteWithDeferredResult` capability; root snapshots args and enqueues replay only when the deferred leg reports failure. `WriteStrategy` unchanged.

## Health signals (Q5, Q6, Q7, N1)

- Timeout classification by provenance: parent ctx cancelled/expired → not health, no failover; Helix-owned per-leg/probe timeout → health; driver timeout with live parent → classifier.
- `ForceDegrade` is a sticky operator latch cleared only by `ForceRecover`/`Reset`; probe and fast-write observations cannot restore synchronous writes while latched; `ForceDegradeFor(cluster, d)` later.
- `FailoverPolicy.ShouldFailover` keeps meaning "retry this request"; built-in `CircuitBreaker` gains `WithFailoverBelowThreshold(bool)` (default false in v1); separate optional route-veto interface for future-request selection, implemented by `LatencyCircuitBreaker`, never synthesizes `OnFailure`; half-open recovery moves to a root-owned policy-probe path.
- Observation hub with separate authorities (operator intent, session liveness, read routing, write degradation, replay); cross-domain translation is explicit policy.

## Observability (N5)

No new methods on `MetricsCollector`; new optional interfaces, new series, new event kinds only; no new labels on existing counters.

## Roadmap corrections to apply

1. Reword "outage of any length" → "within configured retention".
2. Split Phase 2 into 2a (pure moves) and 2b (Config copy, drain sentinel, validation, exported interfaces).
3. Phase 1: NATS `MaxDeliver=-1` + worker dead-letter counter, not "unreachable does not consume an attempt".
4. Phase 3.8: replay envelope versioning + worker-first rollout note.
5. Per-cluster replay queues move from Phase 6 to before Phase 4 gating.
6. Vocabulary: "authorities sharing observations" instead of "five state machines".
