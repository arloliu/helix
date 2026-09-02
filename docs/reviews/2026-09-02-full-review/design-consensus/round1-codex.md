# Independent design opinion — round 1

## Compatibility rule used in this review

For Helix, backward compatibility is larger than exported Go method sets.
It includes return-value semantics, default retry and retention budgets, read routing, operator control, emitted event meanings, metric names/labels, and the replay wire format.
The package documentation promises that a non-strict write returns `nil` only when at least one cluster succeeded (`doc.go:46-48`), while the implementation expressly returns `nil` for “all-async” outcomes (`cql_client.go:1994-2019`).
That is a serious contract defect, but existing users may still rely on the implementation.
My default rule for v1.x is therefore: preserve legacy defaults, add a named safe behavior profile and precise per-feature overrides, warn on unsafe legacy combinations, and flip defaults only in v2.
A patch may fix corruption or an implementation that cannot plausibly be relied upon, but it must not silently redefine resource budgets or success.

## Q1 — v1.x compatibility policy

**Verdict: REFRAME.**

**Chosen answer:**
None of A/B/C as stated.
Use strict v1 compatibility for existing defaults, plus an additive, versioned behavior profile: `V1Legacy` remains the v1.x default;
`Safe` opts into the corrected acknowledgement, context-health, replay-retention, and topology-gating behavior.
Individual explicit options should override the profile.
Make `Safe` the v2 default and retain a documented v1 migration profile for one major-version transition.

**Reasoning:**
“Documented-contract-first” is too coarse.
The documentation and code contradict each other on write success (`doc.go:46-48`, `cql_client.go:2006-2019`), but default retry counts and stream limits are also deliberately documented configuration (`replay/worker.go:42-51`, `replay/nats.go:32-55,75-79`).
Changing any of them can increase memory retention, JetStream storage, recovery load, request latency, or returned errors.
Exported interfaces are another constraint: `WriteStrategy`, `FailoverPolicy`, `Replayer`, and `ReplayWorker` already define extension seams used by custom implementations (`strategy.go:32-48,83-131,222-237`).
A blanket “bugs may change in a minor” policy does not tell maintainers which of those interfaces may change.

**Backward compatibility and migration:**
API impact is additive: introduce a behavior-profile option and feature-level enums, not booleans.
Default values and observable behavior remain unchanged throughout v1 unless the caller selects `Safe`.
Users migrate by enabling `Safe` in staging, watching the new diagnostics, then removing the explicit profile after upgrading to v2.
Release notes must list every behavior gated by the profile.
For data-risky legacy configurations, emit a once-per-client startup warning;
do not silently change them.

**What Claude misses:**
B recognizes documentation contradictions but not source compatibility of exported structs, custom strategy semantics, metric/event compatibility, mixed-version workers, or the operational cost of longer retention.
“CHANGELOG behavior change” is disclosure, not compatibility.

## Q2 — release vehicle for replay durability

**Verdict: REFRAME.**

**Chosen answer:**
Ship replay work before the root refactor, but split it into two release classes.
A `v1.6.1` patch may clone mutable arguments, add diagnostics, correct false documentation, and add opt-in retry-policy machinery.
Any change to default `MaxAttempts`, `MaxDeliver`, `Replicas`, discard policy, or effective retention belongs in a minor release under the safe profile, not in a patch.
It still should not wait for the file split.

**Reasoning:**
The replay defect is independent and urgent: the memory worker drops after five attempts and drops immediately when its retry pool is full (`replay/memory_worker.go:96-126,139-160`);
NATS terminates on total JetStream delivery count and uses immediate `Nak()` (`replay/nats_worker.go:231-267,304-313`).
But those current caps were introduced and called out as a behavior change (`CHANGELOG.md:519-539`).
Raising them or making delivery effectively unbounded changes resource and outage behavior just as surely as changing a return value.
`Replicas=3` is especially unsuitable as a patch default because the current default is one (`replay/nats.go:49-55,93-105`) and a single-node JetStream deployment cannot satisfy three replicas.

**Backward compatibility and migration:**
The patch has no default or caller-visible success change.
The minor release adds `Safe`/new retry policy opt-in;
existing `WithMaxAttempts` and `WithMaxDeliver` retain legacy meaning.
Users migrate by sizing retention, capacity, and dead-letter handling explicitly before enabling the policy.

**What Claude misses:**
“Patch first” and “default durability changes” are separate decisions.
Isolation from the root refactor is correct;
patch-level default changes are not.

## Q3 — contract of `MaxDeliver` / `MaxAttempts`

**Verdict: REFRAME.**

**Chosen answer:**
Separate three budgets instead of making one counter mean two things:

1. **Retention/admission budget:** how long and how much admitted work may remain queued.
2. **Transient retry schedule:** backoff for unavailable, overloaded, drained, or temporarily incompatible targets;
these failures do not consume a poison budget.
3. **Permanent-failure policy:** immediate dead-letter, or a small configurable number of confirmations before dead-letter.

Use a richer `ReplayDisposition` (`Defer`, `Retry`, `DeadLetter`) rather than “unreachable bool.” NATS should not use `Metadata.NumDelivered` as the permanent-failure count, because it is incremented before the worker learns the error class (`replay/nats.go:621-625`).
If permanent failures need N confirmations, track that count in message metadata or a worker-owned store.
Memory should retain an internal envelope with enqueue time and retry state rather than overloading `ReplayPayload`.

**Reasoning:**
The docs already describe the caps as poison handling (`docs/replay-system.md:692-704`), while the code applies them to every execution error.
Claude's option C is closer, but a default “NATS 24h / memory 1h” still mixes unrelated durability models: NATS has explicit time and size retention (`replay/nats.go:32-47`), while memory is volatile and bounded (`replay/memory.go:18-44`).
A one-hour memory retry window can merely hold the queue full for an hour;
it does not make the queue durable.

**Backward compatibility and migration:**
Keep existing options and exact defaults in legacy mode.
Add a new `ReplayRetryPolicy` option;
map old options to the legacy all-errors counter.
Safe mode requires an explicit retention/capacity decision and defaults to fail-loud admission.
Users migrate by wiring `OnDrop`/dead-letter storage, sizing queues, then enabling the new policy.

**What Claude misses:**
A JetStream delivery attempt cannot literally be “not consumed” after delivery.
It can be ignored for poison accounting, but `NumDelivered` still advances.
It also misses queue admission and capacity, which bound outage survival even if retries are unlimited.

## Q4 — location and shape of replay error classification

**Verdict: DISAGREE.**

**Chosen answer:**
Use typed normalization where a driver adapter has authoritative knowledge, followed by a worker-level `func(error) ReplayDisposition` override.
Do not use error-string matching.
Do not reuse the replay classifier for auto-refresh;
replayability and “replace this session” are different decisions.

**Reasoning:**
Adapter normalization is already an established seam: both adapters translate driver not-found to a Helix sentinel (`adapter/cql/v1/adapter.go:14-18`, `adapter/cql/v2/adapter.go:15-19`).
The replay seam, however, accepts arbitrary `ExecuteFunc` implementations that return only `error` (`replay/worker.go:14-17`), so adapters cannot be the only source of classification.
Define typed errors/sentinels for authoritative cases, preserve the underlying error through `Unwrap`, and let custom executors supply a disposition classifier.
“Unconfigured table” is not universally poison: during a rolling schema deployment it can be transient.
String matching would freeze driver wording into Helix's interface and is unsafe.

Auto-refresh asks whether a session object is stale or permanently unusable.
Replay asks whether repeating a particular statement later can help.
A syntax error should not refresh a session;
a target outage should be retried and may justify refresh;
a schema mismatch may be retryable without refresh.
One boolean cannot encode this matrix.

**Backward compatibility and migration:**
New typed errors and classifier options are additive.
Adapter normalization must preserve `errors.As`/`Unwrap` behavior and preferably the original error text.
Legacy mode classifies all execution failures as today.
Users with custom sessions or executors migrate by supplying the classifier;
no existing interface method is added.

**What Claude misses:**
Its heuristic fallback is the dangerous part, and reusing the same classifier for auto-refresh creates false session replacement on query-specific failures.

## Q5 — whether `DeadlineExceeded` is a health signal

**Verdict: REFRAME.**

**Chosen answer:**
Classify by timeout provenance, not error identity:

- If the parent request context is canceled or expired, return that error, do not fail over, and do not record cluster health.
- If a Helix-created per-leg or probe timeout expires while the parent context is still alive, record a cluster latency/availability signal.
- If a driver returns a timeout while the parent context remains live, pass it through the configured health classifier.

**Reasoning:**
Today `isCtxErr` checks only `errors.Is` (`cql_client.go:2573-2581`) and is used only by slice fallback options (`cql_client.go:3369-3379`).
The main read path records all other errors as cluster failures and attempts failover with the same context (`cql_client.go:2360-2376,2471-2509,2537-2564`).
Adaptive writes count any non-operational error as a strike (`policy/adaptive_write.go:765-784`).
Conversely, `LatencyCircuitBreaker.RecordLatency` only receives successful-operation latency (`policy/latency_circuit_breaker.go:232-249`);
a deadline error is currently an ordinary failure, not a measured latency sample.
Checking `ctx.Err()` at the orchestration seam distinguishes caller-budget exhaustion from cluster-generated timeout much better than globally excluding `context.DeadlineExceeded`.

**Backward compatibility and migration:**
No exported API change is required for parent-context handling, but metrics, breaker counts, sticky routing, and refresh behavior change.
Gate that behavior under `Safe` in v1;
provide an additive per-leg timeout option whose expiry is explicitly a health signal.
Users migrate by adjusting alerts that currently count caller cancellations as cluster errors.

**What Claude misses:**
“No context error is ever a health signal” would also suppress a future Helix-owned per-leg deadline, exactly where a health signal is intended.
It also describes LCB deadline handling as latency recording when the code records failure instead.

## Q6 — `ForceDegrade` stickiness

**Verdict: REFRAME.**

**Chosen answer:**
Manual quarantine must be sticky, but do not silently change the existing `ForceDegrade` method in v1.
Add an explicit `Quarantine(cluster)` / `Release(cluster)` (names illustrative) authority distinct from automatic degraded state.
Probes and fast writes may update observed health while quarantined, but they must not restore synchronous traffic.
In v2, either redefine `ForceDegrade` as sticky or remove the ambiguity.

**Reasoning:**
The documented recovery procedure presents `ForceDegrade` as operator isolation and says read and write controls are separate (`docs/auto-recovery.md:85-102`).
The code stores only `isDegraded` (`policy/adaptive_write.go:1101-1152`), while both successful background writes and probes call the same `recordFast` recovery path (`policy/adaptive_write.go:721-725,977-1023,1279-1305`;
`cql_client.go:392-409`).
Thus automatic observation currently overwrites operator intent.
The clean model is not another flag inside generic “health”;
it is a higher-authority quarantine overlay.

**Backward compatibility and migration:**
The new methods/interface are additive and legacy `ForceDegrade` retains auto-recovery in v1.
Operators needing isolation migrate to `Quarantine` and must explicitly `Release`.
Add new event reasons/kinds for quarantine;
do not reuse automatic recovered events for operator release.
Safe mode may make `ForceDegrade` sticky only if the behavior profile says so explicitly.

**What Claude misses:**
A latch inside `clusterWriteState` fixes the immediate bug but still conflates observed write health and operator authority.
An hourly warning is a policy choice with log-volume consequences and should be configurable/coalesced, not hard-coded.

## Q7 — purpose of the read-side circuit breaker

**Verdict: DISAGREE.**

**Chosen answer:**
In v1, preserve `FailoverPolicy.ShouldFailover` as the gate for retrying the current request, because that is its documented interface (`strategy.go:105-131`) and the root currently obeys it (`cql_client.go:2462-2468,2478-2484`).
Add a separate optional, exported routing-veto capability for future requests, implemented by `LatencyCircuitBreaker`.
Do not call `ReadStrategy.OnFailure` when no read failed.
If “always retry the healthy alternative” is desired, provide a new built-in policy or explicit option;
do not impose it on custom policies.

**Reasoning:**
Claude's C changes `ShouldFailover(false)` from “do not retry” to “retry but do not change preference.” That breaks every custom policy that uses false to control amplification, consistency, cost, or authorization.
It also makes the existing `FailoverPolicy` interface lie.
A route veto is genuinely separate: LCB can become open on a successful but slow read (`policy/latency_circuit_breaker.go:232-249`), yet `runPrimaryRead` currently selects and executes without consulting policy (`cql_client.go:2239-2307`).
Routing away should be an overlay on selection, not a fabricated failure sent into `StickyRead`.

**Backward compatibility and migration:**
The optional capability is additive;
custom policies keep exact behavior.
Keep routing-veto off in legacy mode and enable it in safe mode or through a constructor option.
Users wanting immediate alternative retry choose `ActiveFailover` or the new explicit policy.
Existing `ShouldFailover` implementations need no change.

**What Claude misses:**
C is not backward-compatible merely because `RouteAway` is optional;
the “retry alternative always” half changes the semantics of the mandatory existing interface.

## Q8 — return value with no synchronous acknowledgement

**Verdict: REFRAME.**

**Chosen answer:**
Return a new `NoSynchronousAckError` carrying both leg outcomes whenever neither leg returns `nil`, not only when both are async/dropped.
This includes real+async, real+dropped, async+async, async+dropped, and dropped+dropped.
Whether successful replay admission is allowed to convert that outcome to `nil` must be an explicit client acknowledgement mode (`RequireSynchronousAck` versus `AckOnReplayAdmission`), never inferred from a “durable replayer” marker.

**Reasoning:**
The documented contract is “at least one cluster succeeded” (`doc.go:46-48`).
The current code returns an error only for two real errors and returns `nil` for every other non-nil pair (`cql_client.go:1994-2019`).
A `DualClusterError` would inaccurately say two clusters failed when an async write is still in flight, so a distinct error is justified.
A durability marker is too weak: `Replayer.Enqueue` promises only successful queue admission (`strategy.go:32-48`), while the production-recommended NATS implementation defaults to `MaxAge=24h`, `DiscardOld`, and one replica (`replay/nats.go:32-55,88-112`).
Admission can succeed and the record can still be evicted or lost before execution.

**Backward compatibility and migration:**
Adding the error type/sentinel and acknowledgement-mode option is API-additive.
In v1 legacy mode, retain current `nil`;
safe mode returns the new error by default.
Callers migrate by handling `errors.Is(err, ErrNoSynchronousAck)` and choosing `AckOnReplayAdmission` only after accepting the configured retention, replication, eviction, and dead-letter contract.
Metrics should distinguish “zero sync ack” from cluster write errors.

**What Claude misses:**
It considers only both-async/dropped and omits real+operational combinations, which also have zero synchronous copies.
Its option B does not state whether enqueue success changes the result, and its rejected marker option still leaks into the roadmap.

## Q9 — pending replay after `SwapSession`

**Verdict: AGREE, with a stronger invariant.**

**Chosen answer:**
Backlog follows the logical cluster slot.
`SwapSession(A, ...)` replaces transport/session generation for logical A;
it must not invalidate A's reconciliation work.
Document that repointing a slot to a different logical dataset is unsupported.
Validate `TargetCluster` strictly at enqueue, decode, and execution;
unknown values must not fall through to B.

**Reasoning:**
`SwapSession` is documented as replacing the live session for a specified cluster, usually after endpoint change (`cql_client.go:1231-1267`).
`DefaultExecuteFunc` resolves the current session at execution time (`cql_client.go:1436-1452`), which is correct for session refresh.
A generation fence would make precisely the backlog needed after a dead-session replacement unexecutable.
The real bug is that `getSession` maps every non-A value to B (`cql_client.go:1465-1472`), while `SwapSession` itself validates cluster IDs (`cql_client.go:1275-1286`).
Replay should have the same validation.

**Backward compatibility and migration:**
No API/default change for valid A/B payloads.
Invalid or foreign messages become dead-letter/corrupt events instead of executing on B—an observable correctness fix.
Mixed deployments sharing a stream must use stable logical A/B mapping or separate subject prefixes before upgrading.

**What Claude misses:**
“Document it” is insufficient without validating logical identity and unknown cluster IDs, especially for shared NATS streams.

## New design questions that must be settled

### N1 — What is the authority model for “health”?

**Options:**
(A) one authoritative `clusterHealth` state machine;
(B) an observation hub plus separate controllers;
(C) keep all direct wiring.

**Recommendation:**
B.
Centralize error classification and observations, but retain distinct authoritative domains: operator quarantine/drain, session liveness, read-route policy, write degradation, and replay availability.
The hub may publish observations;
it must not automatically translate one domain's state into another without an explicit policy.
Drain is operator/topology intent (`cql_client.go:89-93`), `clusterStats` exists to replace sessions (`cql_client.go:99-147`), and StickyRead stores routing preference (`policy/read_strategy.go:29-34,102-115`);
these are not interchangeable health states.

**Compatibility:**
Internal observation wiring can be API-neutral.
Any cross-domain forwarding must be opt-in in v1 because it changes routing, session refresh, and replay timing.
Migration is to enable forwarding policies one at a time with transition metrics.

### N2 — Should replay know topology?

**Options:**
(A) make `replay` import topology/root state;
(B) expose public `PauseCluster`/`ResumeCluster`;
(C) inject a per-cluster execution gate and optionally expose a small pauser capability.

**Recommendation:**
C.
Keep the replay module topology-neutral.
Root owns drain/quarantine knowledge and drives an injected gate.
Hard operator drain should pause dequeue for that cluster;
transient execution failure should use deferred redelivery with a probe/backoff.
Move per-cluster memory queues ahead of this work: memory currently has only high/low mixed channels (`replay/memory.go:18-28,51-55`), so a real per-cluster pause cannot be implemented cleanly today.
Do not pause merely because AdaptiveDualWrite is degraded unless an independent probe can resume it.

**Compatibility:**
Additive optional capability;
`ReplayWorker` remains unchanged (`strategy.go:222-237`).
Legacy mode leaves workers ungated;
safe mode wires the gate for built-in workers.
Custom workers continue unchanged and receive a startup warning if topology gating was requested but unsupported.

### N3 — How is replay wire evolution rolled out?

**Options:**
(A) append fields to the current msgp map;
(B) add a version field in the same subjects;
(C) version the envelope and subjects/consumers, with a staged worker-first rollout.

**Recommendation:**
C for semantic fields such as consistency.
The generated decoder skips unknown map keys (`replay/nats_message_gen.go:141-255`), so additive fields are byte-compatible, but old workers would silently ignore new consistency fields and new workers cannot distinguish an absent field from an intentional zero without a schema version.
More importantly, adding fields to exported `types.ReplayPayload` (`types/types.go:175-205`) breaks external unkeyed composite literals.
Introduce a v2 replay record/envelope and an optional v2 enqueue/execution seam;
built-ins support both.
Deploy readers for v1+v2 first, then switch publishers to a versioned subject.

**Compatibility:**
Existing `Replayer` and `ReplayPayload` stay intact in v1.
Old messages retain legacy session-default consistency;
new behavior is available only when both publisher and worker support v2.
Migration requires a documented rolling order and backlog drain policy.

### N4 — How does AdaptiveDualWrite report deferred completion?

**Options:**
(A) add a callback to `WriteStrategy.Execute`;
(B) install a mutable/global callback on AdaptiveDualWrite;
(C) add an optional structural interface returning per-leg deferred completion handles.

**Recommendation:**
C.
Do not change `WriteStrategy` (`strategy.go:83-103`).
Root can type-assert an unexported capability implemented by an exported `AdaptiveDualWrite.ExecuteWithDeferredResult` method whose common result types live in `types`.
An async leg returns a buffered completion handle;
a dropped leg returns no handle.
Root snapshots replay arguments before returning to the application, waits for the deferred result, and enqueues only on failure.
This avoids global callback races and associates completion with the correct statement.
The current background path loses the real result after logging (`policy/adaptive_write.go:668-690`) and root eagerly enqueues on `ErrWriteAsync` (`cql_client.go:2006-2015`).

**Compatibility:**
Mandatory interfaces do not change;
custom strategies retain existing behavior.
Enable deferred-result handling for the built-in strategy under safe mode.
Migration for custom async strategies is optional implementation of the new capability;
otherwise legacy eager replay remains.

### N5 — What are the event and metric compatibility rules?

**Options:**
(A) repurpose existing counters/events with new reason labels;
(B) extend `MetricsCollector`;
(C) keep existing series and event meanings, add optional interfaces/new series/new event kinds.

**Recommendation:**
C.
`MetricsCollector` is a large mandatory interface (`types/metrics.go:19-140`);
adding a method breaks every custom collector.
Do not add a `reason` label to an existing counter because that changes queries and cardinality.
Keep `replay_dropped_total` stable and add a separate optional `ReplayOutcomeMetrics` interface with new reasoned series.
Preserve `EventReplayDropped` as enqueue failure, which is its documented meaning (`types/cluster_event.go:51-55`);
use new event kinds for eviction, expiry, poison, quarantine, and zero-sync acknowledgement.
Events remain best-effort, not an audit log (`types/cluster_event.go:80-103`).

**Compatibility:**
API-additive optional interfaces and new metric names.
Existing collectors compile and retain their series.
Users migrate dashboards deliberately to the new series;
no existing alert changes meaning silently.

### N6 — What happens when replay admission or retention is exhausted?

**Options:**
(A) availability-first eviction (`DiscardOld`);
(B) fail new admission but preserve old records (`DiscardNew`);
(C) block writes.

**Recommendation:**
B as the safe profile, with explicit bounded retention and dead-letter handling.
Never block indefinitely.
If no leg has a synchronous acknowledgement, failed replay admission must be part of `NoSynchronousAckError`.
If one leg did acknowledge, preserve the existing write success result but emit a high-severity gap event/metric;
an optional stricter acknowledgement policy may return an error.
The current NATS default knowingly evicts old work (`replay/nats.go:44-47,93-105`), and the current enqueue path logs a drop but still returns `nil` (`cql_client.go:2048-2074`).

**Compatibility:**
Legacy keeps `DiscardOld` and current returns;
safe mode selects fail-loud admission.
Migration requires capacity sizing and caller handling for admission errors.

### N7 — What is the supported idempotency contract?

**Options:**
(A) rely on documentation and `Strict()`;
(B) parse CQL heuristically;
(C) make idempotency an explicit query/batch property and default unknown operations conservatively in safe mode.

**Recommendation:**
C.
Do not parse arbitrary CQL with regexes.
Preserve replay for explicitly idempotent timestamped mutations;
require `Strict()` or explicit acknowledgement of at-least-once semantics for counters, collection append/prepend, and unknown custom statements.
Deferred completion removes the guaranteed double execution but cannot remove ambiguity when an attempt times out after applying.

**Compatibility:**
Additive fluent option/type.
Legacy retains current replay behavior.
Safe mode rejects or returns a typed error for non-idempotent operations that would otherwise be replayed.
Users migrate by annotating known-idempotent statements or choosing strict writes.

### N8 — Is the roadmap phase order correct?

**Options:**
(A) current order unchanged;
(B) correctness changes before any split;
(C) replay release, truly pure root split, path correctness, authority/gating, observability, then performance.

**Recommendation:**
C, with two corrections.
First, Phase 2 must contain only file moves and behavior-neutral private helpers;
config-copy behavior, validation rejection, drain routing, and exported capability interfaces move to later behavioral/API phases.
Second, move per-cluster replay queue structure and gating ahead of the planned pause feature;
concurrency width can remain later.
Tests that intentionally fail should be co-landed with their fix or held outside required CI—`t.Skip` is documentation, not a safety net.

**Compatibility:**
Replay patch/minor split follows Q2.
Pure moves have no compatibility impact.
Each later behavior ships behind the v1 profile/options described above, then becomes default in v2.

### N9 — What identifies a cluster in a shared replay stream?

**Options:**
(A) continue using `A`/`B`;
(B) require stream/prefix isolation per deployment;
(C) carry a stable deployment and logical-cluster identity in the versioned envelope.

**Recommendation:**
C, with B as the immediate v1 rule.
`TargetCluster` is only a letter in the public payload (`types/types.go:175-179`) and in the NATS subject/message (`replay/nats.go:432-455`).
Two applications sharing a prefix can disagree about which physical cluster is A.
The v2 envelope should carry a configured deployment ID plus logical cluster ID and reject mismatches before execution.

**Compatibility:**
No v1 wire change;
document prefix isolation now and validate A/B.
The versioned envelope is opt-in and follows the staged rollout in N3.

## Factual problems in the review or roadmap

1. **“An outage of any length ... must converge without data loss” is impossible under the stated bounded designs.**
The roadmap makes that absolute claim (`docs/plans/roadmap.md:12-13`), while NATS has finite `MaxAge`, `MaxMsgs`, and `MaxBytes` and defaults to `DiscardOld` (`replay/nats.go:32-47,93-105`), and memory replay is explicitly lost on restart (`replay/memory.go:37-44`).
The goal must say “within configured retention/capacity, for admitted supported operations.”

2. **Phase 2 is not “no behaviour change.”** The roadmap labels it that way (`docs/plans/roadmap.md:109-112`) but changes drain execution and metrics, makes `Config()` return a copy, rejects previously accepted option combinations, and exports new interfaces (`docs/plans/roadmap.md:118-122`).
Today `Config()` returns the live pointer (`cql_client.go:3852-3858`), and drain bypasses `WriteStrategy` (`cql_client.go:1784-1790,1793-1882`);
both proposed changes are observable.

3. **“Unreachable errors do not consume a delivery attempt” is not implementable using current JetStream delivery count.**
The roadmap assumes it (`docs/plans/roadmap.md:93`), but the count is read from message metadata before execution classification (`replay/nats.go:621-625`) and termination compares that total count after the error (`replay/nats_worker.go:231-253`).
The correct statement is “transient deliveries do not consume the worker's poison budget,” which requires separate accounting.

4. **Phase 3.8 contradicts the roadmap's “no breaking API” claim.**
The roadmap says there is no breaking API in scope (`docs/plans/roadmap.md:59-62`) and then adds fields to exported `ReplayPayload` (`docs/plans/roadmap.md:143`).
External unkeyed literals can stop compiling, and same-subject mixed-version workers can silently ignore the new semantics.
The current map decoder's unknown-field skip (`replay/nats_message_gen.go:141-255`) provides byte compatibility, not semantic rolling-upgrade safety.

5. **The per-cluster pause is ordered before the memory backend can support it cleanly.**
Pause/resume is Phase 4 (`docs/plans/roadmap.md:170`), but per-cluster queues are deferred to Phase 6 (`docs/plans/roadmap.md:201`).
The current memory queue is partitioned only by priority (`replay/memory.go:18-28,51-55`), so Phase 4 would need dequeue/requeue churn or head-of-line blocking.

6. **“Five health state machines” is an inaccurate model and leads the roadmap toward the wrong ownership.**
Drain is operator/topology intent, StickyRead is routing preference, clusterStats is a session-replacement detector, the breaker is a read-failure policy, and AdaptiveDualWrite is a write-latency policy (`cql_client.go:89-147`;
`policy/read_strategy.go:29-34`;
`policy/latency_circuit_breaker.go:232-249`;
`policy/adaptive_write.go:925-1023`).
They need shared observations and explicit translation policies, not one authoritative health state.

## Overall position

Helix needs the replay and zero-ack work urgently, but the safest design is not a larger shared state machine or a “durable” marker.
Preserve separate authorities, centralize typed observations, separate replay admission/retention/transient retry/poison handling, and make acknowledgement policy explicit.
Ship corruption fixes and opt-in replay machinery before the refactor;
then land a genuinely pure file split, path correctness, per-cluster replay structure and gating, and only afterward cross-domain recovery policies.
Keep v1 defaults stable behind a named legacy profile, provide a complete safe profile for early adopters, and make that safe profile the v2 default.
