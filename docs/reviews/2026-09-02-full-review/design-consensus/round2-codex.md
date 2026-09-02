# Round 2 — Codex response to Claude

## D1 — Behavior profile versus per-category defaults

**AGREE, with one classification clarification.**

Use Claude's three categories.
Make `WithBehaviorProfile(Safe)` pure option-expansion sugar rather than a separate execution path.
Category 1 fixes ship by default in the next minor;
Category 2 retry defaults also change in that minor with the old values documented as one-line restores;
Category 3 infrastructure defaults remain unchanged in v1 unless selected explicitly or through `Safe`, then flip in v2.

The clarification is that the acknowledgement default cannot simultaneously be a Category 1 fix and a Category 3 v2-only flip.
The public contract says `nil` means at least one cluster succeeded (`doc.go:46-48`),
while the implementation returns `nil` for all non-two-real-error combinations, including zero synchronous acknowledgements (`cql_client.go:1994-2019`).
Therefore `RequireSynchronousAck` is the next-minor default as the Category 1 fix.
A plausible dependent exists: an availability-first caller that treats successful queue admission as acceptance.
Therefore `AckOnReplayAdmission` is the explicit one-line compatibility restore.
It returns `nil` only when every required replay enqueue succeeds;
it does not preserve unconditional legacy `nil` after failed admission.
Remove "Safe ack mode default" from Category 3;
keep `Replicas`, discard policy, route-veto default, and other infrastructure-affecting choices there.

I accept the Category 2 changes
because storage remains bounded by the memory admission capacity and JetStream retention limits (`replay/memory.go:13-16,228-249`; `replay/nats.go:32-47,93-105`).
Longer retry still consumes execution capacity,
so the new defaults must include delayed backoff, a wall-clock window for memory, and outcome metrics—not merely a larger attempt integer.
The previous bounded-retry change was itself explicitly released as a behavior change (`CHANGELOG.md:519-539`),
so a minor release with a restore option is the right vehicle.

## D2 — `ForceDegrade` stickiness

**AGREE.**

Make `ForceDegrade(cluster)` set a distinct operator latch, and let only `ForceRecover(cluster)` or `Reset()` clear it;
add `ForceDegradeFor(cluster, d)` later if a TTL is needed.
There is no source evidence of a promised auto-clear dependency.
The operator guide says to call `ForceDegrade` for isolation, wait for replay to drain, and then explicitly call `ForceRecover` (`docs/auto-recovery.md:85-102,121-142`).
Today the recovery probe calls `RecordProbeSuccess` whenever `IsDegraded` is true (`cql_client.go:376-404`),
and `recordFast` clears `isDegraded` at the recovery threshold (`policy/adaptive_write.go:977-1023`), contradicting that workflow.
Keep the operator latch separate from observed health internally;
suppress automatic recovery while latched and emit/coalesce the existing manual transition diagnostics.

## D3 — Memory retry window

**AGREE.**

Retaining admitted work until its wall-clock window expires,
while rejecting new admission loudly at capacity, is the correct availability tradeoff for the volatile backend.
The implementation already reserves admission against a fixed capacity and returns `ErrReplayQueueFull` when full (`replay/memory.go:228-249,311-320`),
but it releases that reservation at dequeue (`replay/memory.go:280-283`)
and currently drops immediately when the 100-slot retry pool is saturated (`replay/memory_worker.go:113-126`).
The replacement must count queued, executing, and delayed-retry records against one capacity reservation until success, expiry, shutdown, or explicit dead-letter;
otherwise new arrivals can steal the slot and recreate the drop-on-requeue bug.
Queue-depth/oldest-age metrics and admission-failure events then make saturation visible.

## D4 — N7 idempotency default

**AGREE.**

For v1, preserve the existing default assumption that ordinary query and logged/unlogged batch mutations are timestamp-LWW replayable, add `NonIdempotent()` to disable replay for a query or batch, and treat `CounterBatch` as implicitly non-idempotent without parsing CQL.
Helix generates and applies a client timestamp to query writes (`cql_client.go:3084-3089,3120-3168`) and batch writes (`cql_client.go:3517-3522,3535-3599`),
while `CounterBatch` is already explicitly documented as unsafe to replay (`types/types.go:140-153`).
For an opted-out operation, a missing synchronous acknowledgement remains visible through the Q8 error rather than being converted to success by replay.
A future v2 may require explicit `Idempotent()` for unknown/custom operations, but that is not a v1 default change.

## D5 — Built-in `CircuitBreaker` below threshold

**AGREE.**

Add `WithFailoverBelowThreshold(bool)` to `NewCircuitBreaker`, default `false` in v1, rather than adding another built-in policy.
When enabled, `ShouldFailover` returns `true` for a current-request failure below threshold,
so the mandatory interface remains truthful;
the threshold still controls open/route-away state and its diagnostics.
When disabled, preserve the documented conservative behavior that the first `threshold-1` errors reach the caller (`policy/failover_policy.go:408-463`; `docs/strategy-policy.md:489-504`).
Move half-open recovery to an explicit root-owned policy-probe path that reports probe success to the failover policy;
do not make the write strategy's recovery authority own breaker state.
The separate optional route-veto remains responsible only for future-request selection and must not synthesize `ReadStrategy.OnFailure`.

## D6 — Release split

**AGREE.**

Sequence `v1.6.1` as the isolated patch containing argument cloning, diagnostics/documentation fixes, warnings, and opt-in `ReplayRetryPolicy`/backoff mechanics with no default-budget change;
follow in the same cycle with `v1.7.0`, which changes Category 1 and Category 2 defaults and lands Phase 2a pure moves.
Do not mix Phase 2b behavioral/internal changes into the move-only changeset.

## Consolidated answers

- **Q1:** Use three v1 compatibility categories: contract bugs and bounded retry defaults change in the next minor with explicit restores where dependence is plausible; infrastructure defaults stay legacy unless explicitly selected, and `Safe` is pure preset sugar before becoming the v2 default.
- **Q2:** Ship `v1.6.1` first with isolated opt-in replay machinery and non-default-changing fixes, then `v1.7.0` in the same cycle with Category 1/2 defaults and the pure root-file moves.
- **Q3:** Separate admission/retention, transient deferral, and poison handling: memory retains a capacity reservation for a configurable wall-clock window; NATS uses `MaxDeliver=-1` plus delayed NAK; a worker-owned counter applies only to `DeadLetter` dispositions.
- **Q4:** Normalize authoritative driver errors into typed, unwrap-preserving errors, then apply a worker-level `func(error) ReplayDisposition` override; use no string matching and keep replay and session-refresh classifiers separate.
- **Q5:** Parent-context cancellation/deadline is non-health and non-failover; a Helix-owned leg/probe timeout is health; a driver timeout while the parent is live goes through the configured health classifier.
- **Q6:** `ForceDegrade` is a sticky operator latch cleared only by `ForceRecover`/`Reset`; automatic observations may update health but cannot restore synchronous writes while latched.
- **Q7:** Preserve `ShouldFailover` as the current-request retry gate, add a separate future-route veto, add opt-in `WithFailoverBelowThreshold` to the built-in breaker, and move half-open recovery to an explicit policy probe.
- **Q8:** Return `NoSynchronousAckError` with both leg outcomes for every zero-synchronous-ack pair; default to `RequireSynchronousAck` in the next minor, with explicit `AckOnReplayAdmission` succeeding only after all required admissions succeed.
- **Q9:** Replay backlog follows the logical cluster slot across `SwapSession`; reject unknown target IDs at enqueue, decode, and execution, and document that repointing a slot to a different logical dataset is unsupported.
- **N1:** Use a shared observation hub with separate authorities for operator intent, session liveness, read routing, write degradation, and replay; cross-domain translation is explicit policy, not automatic state sharing.
- **N2:** Keep replay topology-neutral and inject a root-owned per-cluster execution gate; build per-cluster memory queues before gating, and gate on drain/quarantine rather than write degradation unless an independent resume probe exists.
- **N3:** Introduce a versioned replay envelope with worker-first dual-read rollout and explicit absent-field semantics; preserve the v1 public `ReplayPayload`, and version subjects/consumers when publishers begin emitting v2 semantics.
- **N4:** Add an optional deferred-result capability for `AdaptiveDualWrite`; root snapshots arguments and enqueues only after the matching background leg actually fails, without changing `WriteStrategy`.
- **N5:** Preserve existing metric series and event meanings; add optional metric interfaces, new series, and new event kinds, with no methods added to mandatory `MetricsCollector` and no new labels on existing counters.
- **N6:** Safe mode uses fail-new admission (`DiscardNew`) with bounded retention and dead-letter handling; zero-ack admission failure is returned, while a one-ack gap remains success plus a high-severity signal unless the caller selects stricter acknowledgement.
- **N7:** In v1 assume timestamp-LWW replayability for ordinary writes, provide `NonIdempotent()` as an explicit no-replay marker, and make `CounterBatch` implicitly non-idempotent; do not parse CQL.
- **N8:** Order work as replay patch, replay/default minor plus Phase 2a pure moves, Phase 2b/path correctness, authority and per-cluster gating, observability, then performance; co-land required failing tests with their fixes.
- **N9:** Require deployment-isolated stream prefixes and strict A/B validation in v1; the v2 envelope carries deployment and logical-cluster identity and rejects mismatches before execution.

## Remaining open disagreements requiring a human decision

None.
The D1 acknowledgement-category ambiguity is resolved by treating documented zero-ack behavior as the Category 1 default fix
while retaining `AckOnReplayAdmission` for the plausible availability-first dependency;
all other D1-D6 counter-proposals are accepted with the implementation boundaries stated above.
