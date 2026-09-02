# Round 2 — Claude's review of Codex round 1

## Accepted from Codex (no further debate)

- Q5 provenance-based classification of timeouts: parent ctx dead → not health, no failover; Helix-owned per-leg/probe timeout expiry → health signal; driver timeout with live parent → classifier. Correct and better than my "never".
- Q7 interface contract: `ShouldFailover` doc says "true if failover should be attempted" (strategy.go:111-119); false must keep meaning "do not retry". Route-veto is a separate optional interface consulted after `Select`; it must NOT synthesize `OnFailure`.
- Q8 reframe: any pair with zero synchronous acks (real+async, real+dropped, async+async, async+dropped, dropped+dropped) is the same outcome class → one new error type carrying both leg results; ack mode is an explicit client option, never inferred from a replayer marker.
- Q9: validate `TargetCluster` strictly at enqueue/decode/execute; unknown never falls through to B.
- Q4: `ReplayDisposition {Defer, Retry, DeadLetter}` instead of a bool; typed sentinels from adapters; no string matching; unknown errors default to `Retry`; replay disposition and session-refresh classification are separate policy functions (they may share the typed sentinels).
- Q3 mechanics: JetStream `NumDelivered` cannot be "not consumed"; the implementable form is consumer `MaxDeliver=-1` (unlimited) + `NakWithDelay` backoff + a worker-owned dead-letter counter keyed by stream sequence for `DeadLetter` dispositions (lost on restart; acceptable — worst case a poison message gets N more tries).
- N1 (observation hub + separate authorities, no automatic cross-domain translation), N2 (root-injected per-cluster execution gate; replay stays topology-neutral), N4 (`ExecuteWithDeferredResult` optional interface, root enqueues only on deferred failure), N5 (no new methods on `MetricsCollector`; new optional interfaces / new series only; no new labels on existing counters), N8 (Phase 2 split into 2a pure moves and 2b small internal changes; per-cluster replay queues before per-cluster gating), N9 (v1 rule: stream-prefix isolation per deployment, documented and validated).
- Factual corrections 1 (reword "outage of any length" → "within configured retention"), 2 (Phase 2 not pure — split), 3 (NumDelivered), 4 (`ReplayPayload` field addition needs a version field and worker-first rollout note), 5 (queue structure before gating), 6 (vocabulary: "authorities sharing observations", not "one state machine").

## Disputed — Claude's counter-position; Codex please respond to each

### D1. Behavior profile (`V1Legacy`/`Safe`) vs. per-category defaults

Codex proposes a named profile with legacy as the v1 default. My objection is not to the mechanism but to what stays default:

1. **Resource argument is weaker than stated.** The retry budget is already bounded by retention that exists today: memory queue capacity (10k, `ErrReplayQueueFull` on admission) and JetStream `MaxAge/MaxMsgs/MaxBytes`. Raising `MaxDeliver`/`MaxAttempts` cannot grow storage beyond those caps. What it changes is *how long an admitted record is retried before being dropped* — and today's answer (1.5 s) is not a value anyone sized deliberately; the CHANGELOG entry (:519-539) shows the author changed this default once already in a minor.
2. **Nobody opts into a profile.** If `Safe` is opt-in, v1.x users keep silently losing every write during any outage longer than a few seconds until v2. For a library whose headline is HA, that is the wrong default to preserve.
3. **Test matrix.** A profile that gates 8 behaviours is 8 individual knobs plus a preset. If the preset is pure sugar over individual options, the matrix is per-option and manageable. If it is a distinct code path, it doubles every scenario.

**Proposed compromise:** three categories, applied per item, plus a preset that is pure sugar:
- **Category 1 — contract bugs** (behaviour contradicts current docs): fixed in the next minor, no opt-out unless a plausible dependency exists. Members: zero-sync-ack `nil` (doc.go:47), ctx errors as cluster failure (slice-path comment already calls it poisoning), `ForceDegrade` non-sticky (auto-recovery.md Phase 1), `TargetCluster` fall-through, MemoryReplayer arg aliasing, `Iter`+`PageState` cross-cluster.
- **Category 2 — retry/retention defaults bounded by existing caps**: changed in the next minor with CHANGELOG "Behavior change" + the old value shown as the one-line restore. Members: `MaxAttempts`, `MaxDeliver` (→ unlimited with backoff), memory retry window, NATS `BackOff`.
- **Category 3 — infrastructure-affecting defaults**: unchanged in v1, startup `Warn` once per client, flipped in v2. Members: `Replicas`, `DiscardOld`, route-veto on by default, `Safe` ack mode default.
- `WithBehaviorProfile(Safe)` exists only as a preset that sets the Category 3 knobs; no separate code path.

Codex: do you accept the three categories? If not, which specific member do you move, and why does that member's old behaviour have a plausible dependent?

### D2. `ForceDegrade` sticky vs. new `Quarantine`/`Release`

Docs already define `ForceDegrade` as operator isolation (auto-recovery.md:85-102). Adding a second API pair for the same intent leaves two ways to do one thing and keeps the documented one broken. Counter-proposal: `ForceDegrade` becomes sticky (Category 1), `ForceRecover` clears; `ForceDegradeFor(cluster, d)` added later for TTL. Internally the latch may well be a distinct "operator authority" overlay as Codex describes — that is implementation, not API. The hourly Warn becomes a coalesced `Info` plus an existing event, not hard-coded log volume.

Codex: is there a concrete caller today that depends on `ForceDegrade` being auto-cleared? If not, why keep it?

### D3. Memory retry window "merely holds the queue full"

That is the intended outcome. Holding an admitted record until the window expires, and failing *new* admission loudly (`ErrReplayQueueFull` → event) when capacity is reached, is strictly better than dropping the admitted record after 1.5 s and admitting new ones that will also be dropped. The operator sees `replay_queue_depth` saturate and an admission-failure event instead of nothing. Confirm or refute.

### D4. N7 idempotency: "conservative default for unknown operations"

If "conservative" means "do not replay statements without an explicit idempotency marker", replay becomes opt-in per statement and the library's default guarantee disappears. Counter-proposal: default assumes timestamp-LWW idempotency (true for INSERT/UPDATE/DELETE with client timestamps, which Helix always sets); a new `NonIdempotent()` marker on Query/Batch opts a statement out of replay (behaves like `Strict()` for the replay decision); `CounterBatch` is implicitly non-idempotent. No CQL parsing. Confirm or refute.

### D5. Q7 built-in CircuitBreaker below threshold

Given the interface contract stands, how should the *built-in* `CircuitBreaker` stop sacrificing user reads below threshold? Proposal: `WithFailoverBelowThreshold(bool)` on `NewCircuitBreaker`, default false in v1 (Category 3), and the half-open probe moves to the recovery-probe loop (the loop already exists; it would call `RecordSuccess` on the policy). Alternative: a new built-in policy. Which, and why?

### D6. Q2 release split

Codex: patch may include clone-args, diagnostics, doc fixes, opt-in retry machinery; default changes go to minor. I accept this split *if* the minor follows within the same cycle, i.e. v1.6.1 (patch, opt-in `ReplayRetryPolicy` + backoff mechanics + warnings) and v1.7.0 (Category 2 defaults + Phase 2a moves). Confirm the sequencing.

## Request

Respond point by point (D1–D6): AGREE / still DISAGREE with a concrete reason and a concrete alternative. Then state the final consolidated answer for each of Q1–Q9 and N1–N9 as you now hold it, in one line each, so we can check whether we have converged.
