# Roadmap: Cleanup, Correctness, and Fault Tolerance

## Goal

Turn the findings of the [2026-09-02 full review](../reviews/2026-09-02-full-review/README.md)
into an ordered, releasable sequence of work. Priorities, in order:

1. **Correctness** — a write acknowledged with `nil` must exist on at least one cluster and
   must eventually exist on both; a replayed statement must mean what the original meant.
2. **Stability** — health state must not flip on caller-side noise; operator overrides must not
   be silently undone; no component may starve another.
3. **Fault tolerance** — an outage of either cluster or of the replay transport must converge
   back to a consistent state, without data loss, for any outage that fits within the configured
   replay retention once the fault clears.
4. **Maintainability** — the root package must be reviewable in scoped diffs, with one
   classification function per concern instead of six hand-copied tables.
5. **Performance** — only after the above; Helix's per-op overhead is already under 1 µs.

## Context

The review confirmed (by reading code and by reproducing with throwaway tests) three root causes
from which most other findings derive:

- **Replay is a seconds-long retry buffer, not an outage backlog.** `MemoryWorker` drops a payload
  1.5 s after enqueue; the NATS worker `Nak()`s with no delay and `Term()`s after five deliveries.
  Any outage longer than a few seconds leaves the returning cluster permanently divergent.
- **Several health authorities share no observations.** The circuit breaker (read failures),
  adaptive-write degraded state (write latency), `clusterStats` (session liveness), drain flags
  (operator), and read-strategy state (routing) each own a legitimate decision, but none sees the
  others' evidence. The latency breaker opens but never reroutes; auto-refresh is starved under
  `AdaptiveDualWrite`; the recovery probe reverts `ForceDegrade` within ~10 s.
- **Caller context cancellation is counted as cluster failure** on every path except the
  slice-read fallback leg, where `isCtxErr` already exists.

Finding IDs below (`R-1`, `FB-2`, `A-3`, …) refer to the tables in the review README. IDs are a
planning aid only: they must not appear in code, comments, commit messages, or user docs.

The design questions raised by the review were settled in a two-round exchange between two
independent reviewers; the record is in
[`design-consensus/`](../reviews/2026-09-02-full-review/design-consensus/consensus.md) and the
outcome is summarised in the Design Decisions section below. Those decisions are binding for the
items in this roadmap.

## Design Decisions

### Compatibility categories (v1.x)

Every behavioural change is assigned to exactly one category before implementation.

| Category | Rule | Members |
|---|---|---|
| **1 — Contract bug**: behaviour contradicts current documentation | Fixed by default in the next minor. An opt-out exists only where a plausible dependent is identified, and it restores the documented-adjacent behaviour, not the bug. | Zero-synchronous-ack writes returning `nil`; caller context errors counted as cluster failure; `ForceDegrade` auto-cleared by the probe; unknown `TargetCluster` falling through to cluster B; `MemoryReplayer` aliasing caller args; `Iter` with `PageState` crossing clusters |
| **2 — Retry/retention default bounded by an existing cap** | Changed in the next minor. `CHANGELOG.md` "Behavior change" entry shows the old value as a one-line restore. Ships together with delayed backoff, a wall-clock window, and outcome metrics, never as a bare larger integer. | `MaxAttempts`, `MaxDeliver` (consumer `-1` plus delayed NAK), memory retry window, NATS consumer `BackOff` |
| **3 — Infrastructure-affecting default** | Unchanged in v1. Startup `Warn` once per client when the legacy value is in effect. Flipped in v2, or selected in v1 through `WithBehaviorProfile(Safe)`. | `Replicas`, `DiscardOld` vs `DiscardNew`, route-veto enabled, `WithFailoverBelowThreshold` |

`WithBehaviorProfile(Safe)` is pure option expansion over the Category 3 knobs. It is not a
separate execution path and adds nothing to the test matrix beyond the individual options.

### Write acknowledgement

- Any leg pair with zero synchronous acknowledgements (real+async, real+dropped, async+async,
  async+dropped, dropped+dropped) returns a new `NoSynchronousAckError` carrying both leg results.
  This matches the documented contract that `nil` means at least one cluster succeeded.
- Default acknowledgement mode is `RequireSynchronousAck`. The explicit restore
  `AckOnReplayAdmission` returns `nil` only when every required replay enqueue succeeded; failed
  admission with zero synchronous acks is always an error.
- A one-ack partial failure remains `nil` plus a high-severity replay signal.

### Replay budgets and classification

- Three separate mechanisms: admission/retention capacity; transient deferral schedule; poison
  (dead-letter) policy. One counter never serves two of them.
- Error classification is `ReplayDisposition {Defer, Retry, DeadLetter}`. Adapters normalise
  authoritative driver errors into typed, `Unwrap`-preserving sentinels (for example
  `ErrClusterUnreachable`, alongside the existing `ErrNotFound`). A worker-level
  `func(error) ReplayDisposition` override is available for custom executors. Unknown errors are
  `Retry`. No string matching on driver messages.
- The replay classifier and the session-refresh classifier are separate policy functions that
  share the typed sentinels; replayability and "replace this session" are different questions.
- NATS: consumer `MaxDeliver = -1`, `NakWithDelay` with the already-validated
  `RetryDelay`/`MaxRetryDelay`, and a worker-owned dead-letter counter keyed by stream sequence
  that applies only to `DeadLetter` dispositions. Losing that counter on restart is acceptable.
- Memory: the capacity reservation is held across queued, executing, and delayed-retry states
  until success, window expiry, shutdown, or dead-letter. New admission fails loudly at capacity.
- Replay stays topology-neutral. Root injects a per-cluster execution gate driven by drain and
  operator quarantine, not by write-degraded state. Per-cluster memory queues land before gating.
- Idempotency: ordinary writes are assumed timestamp-LWW replayable. A `NonIdempotent()` marker
  on `Query` and `Batch` excludes a statement from replay. `CounterBatch` is implicitly
  non-idempotent. No CQL parsing.
- Wire evolution: adding semantic fields (consistency level) uses a versioned envelope with
  worker-first dual-read rollout and explicit absent-field semantics. The v1 public
  `ReplayPayload` shape is preserved; subjects and consumer names are versioned when publishers
  start emitting v2 semantics.
- Backlog follows the logical cluster slot across `SwapSession`. Unknown `TargetCluster` values
  are rejected at enqueue, decode, and execution. Deployments sharing a JetStream must use
  isolated subject prefixes; this is documented and validated in v1.

### Health signals and authorities

- Timeout classification is by provenance, not error identity: a cancelled or expired parent
  context is neither a health signal nor a failover trigger; expiry of a Helix-owned per-leg or
  probe timeout is a health signal; a driver timeout while the parent is live goes through the
  configured classifier.
- `ForceDegrade` is a sticky operator latch cleared only by `ForceRecover` or `Reset`. Probe and
  fast-write observations may update observed health but cannot restore synchronous writes while
  latched. `ForceDegradeFor(cluster, d)` may be added later for a TTL.
- `FailoverPolicy.ShouldFailover` keeps its documented meaning: `false` means do not retry the
  current request. The built-in `CircuitBreaker` gains `WithFailoverBelowThreshold(bool)`
  (Category 3, default `false` in v1). Half-open recovery moves to a root-owned policy-probe path
  that reports success to the failover policy.
- A separate optional route-veto interface, implemented by `LatencyCircuitBreaker`, is consulted
  after `ReadStrategy.Select` for future-request routing. It never synthesises
  `ReadStrategy.OnFailure`.
- The root gains an observation hub through which every health observation flows. Authorities
  (operator intent, session liveness, read routing, write degradation, replay availability) remain
  distinct; any translation from one authority's state to another is an explicit, named policy.
- `AdaptiveDualWrite` exposes an optional deferred-result capability. Root snapshots replay
  arguments and enqueues only when the deferred leg reports failure. `WriteStrategy` is unchanged.

### Observability compatibility

No methods are added to `MetricsCollector`. New signals arrive as optional collector interfaces,
new series, or new event kinds. Existing series keep their meaning and gain no new labels.

## Design Principles

- **Ship the data-loss fix first, in isolation.** The replay fix lives in `replay/` plus one small
  call site in root. It does not depend on the root refactor and is the single highest-risk gap.
- **Move, then change.** Pure file moves land in their own PR with zero behaviour change. A move
  PR is reviewed with `git diff --color-moved`; any non-move hunk is rejected.
- **One classification function per concern.** Read-error classification, write-leg
  classification, replay disposition, and health observation each get exactly one
  implementation that every entry point calls.
- **Red before green.** Each fix lands together with the regression test that reproduces it.
- **Minimum change.** No abstractions beyond what a listed item needs. No sub-package extraction:
  `topology` imports root and the strategy interfaces live in root, so extraction would ripple into
  public import paths for no gain.
- **Every phase ends with `make lint`, `make test`, and a `/simplify` pass** before it is
  committed. Cleanup is not batched to the end of a release.

## Non-Goals

- No v2 / breaking API in this roadmap. Items that need one are listed in the last section.
- No rewrite of `cql_client.go` into new packages.
- No performance work before Phase 6 unless it falls out of a correctness fix for free.

---

## Phase 0 — Safety net (no product code changes)

| Item | Finding | Work |
|---|---|---|
| 0.1 | R-11 | Run `test/simulation` `complete-failure` and `replay-saturation` with `OnDrop` instrumented. Record whether they fail today; if they pass, find what masks the loss and fix the harness. |
| 0.2 | R-1, R-2, R-3, R-4, FO-1, FO-2, FO-3, FB-1, FB-2, FB-5, FB-3, D-3, D-5 | Port the review's scratch tests into the repo as regression tests, skipped with a reason until the matching item lands. Root tests use the existing mock session; replay tests use the embedded NATS server. |
| 0.3 | A-1 (F-22) | Table-driven test over the read classification matrix: six entry points × {nil, not-found, row-limit, ctx-error, cluster-error} × {override, drain, fallback}. Initially documents current behaviour. |
| 0.4 | A-1 (F-22) | Wire `make test-e2e` and the simulation quick profile into a nightly CI job. |

**Exit criteria:** every Phase 1–4 finding has a skipped or failing test named in plain language;
nightly CI runs simulation.

**Status (2026-09-02):**

- 0.1 done.
  With the default memory worker both scenarios pass their own checks,
  but the end-of-run consistency check fails
  (`quick`: 1412 rows missing on A after the 15 s outage; `comprehensive`: 1410 missing on A, 125 on B).
  Nothing masks the loss; the scenario-level "queue drained" check was simply satisfied by drops.
  The harness now wires the worker logger and both scenarios fail on any replay drop,
  so the loss is attributable per scenario.
- 0.2 done for R-2, R-3, R-4, FO-1, FO-2, FO-3, FB-1, FB-2, FB-3, FB-5, D-3
  (skipped tests in the root and replay packages).
  R-1 and D-5 land with their fixes in Phase 1.
- 0.3 done: `cql_client_read_matrix_test.go` pins all six entry points × five results × four modes.
- 0.4 done: `make test-simulation` and `.github/workflows/nightly.yml` (e2e + simulation quick profile).

---

## Phase 1 — Replay durability, patch release (`v1.6.1`)

**Scope:** `replay/` plus the enqueue call site in root. No default value changes; no
caller-visible success semantics change.

| Item | Finding | Work |
|---|---|---|
| 1.1 | D-5 | `enqueueReplayIfNeeded` clones `[]byte` args (reuse `cloneArgs` / `cloneBatchEntries`). Failure path only. |
| 1.2 | R-1 | Opt-in replay retry policy: `ReplayDisposition`, typed unreachable sentinel from both adapters, worker-level classifier override. NATS: `NakWithDelay` with backoff and consumer `BackOff`; `MaxDeliver = -1` when the policy is selected; worker-owned dead-letter counter. Memory: capacity reservation held across retry states; wall-clock window. Legacy behaviour unchanged when the policy is not selected. |
| 1.3 | R-6 | NATS worker calls `msg.InProgress()` before each execute in a batch. |
| 1.4 | Q9 | Reject unknown `TargetCluster` at enqueue, decode, and execution. |
| 1.5 | D-2, D-4 | Startup `Warn` for `Replicas == 1` on file storage and for `DiscardOld`. Workers call `SetReplayQueueDepth`; add `replay_oldest_age_seconds{cluster}`; split dropped reasons into a new series. |
| 1.6 | D-10, R-1 | Fix `docs/replay-system.md` (NATS backoff is not "controlled by AckWait"), the `worker.go` jitter claim, the `vm/doc.go` gauge claim; document each backend's effective survival window under legacy and new policy; document stream-prefix isolation and the `SwapSession` backlog rule. |

**Verification:** with the new policy selected, a 30 s outage with default capacity loses zero
payloads on both backends; with the policy not selected, existing tests are unchanged.

**Status (2026-09-03):** 1.1 to 1.6 implemented on branch `fix/replay-durability`.
The policy is `replay.WithRetryPolicy(replay.RetryWhileRetained)`;
the poison budget is `MaxAttempts` on both backends under that policy.
Simulation `comprehensive` profile with `test/simulation/configs/quick-retained.yaml`
passes every scenario and strategy group with zero replay drops;
with the default policy it still fails as recorded under Phase 0.

---

## Phase 2 — Root package restructure (`v1.7.0`)

### 2a — Pure file moves (zero behaviour change)

| Item | Finding | Work |
|---|---|---|
| 2a.1 | A-2 | Split `cql_client.go` into `session_lifecycle.go`, `recovery_probe.go`, `wiring.go`, `read_path.go`, `write_path.go`, `slice_read.go`, `query.go`, `batch.go`, `iter.go`. Package stays `helix`; nothing exported moves. One PR, reviewed with `--color-moved`. |

### 2b — Small internal changes (each noted in CHANGELOG where observable)

| Item | Finding | Work |
|---|---|---|
| 2b.1 | A-2 | `classifyReadErr(err) readErrKind` with kinds `{ok, notFound, rowLimit, ctxErr, clusterErr}`; all six read entry points call it. `ctxErr` still treated as `clusterErr` here; Phase 3 flips it. |
| 2b.2 | A-5 | Drain as a per-leg sentinel returned by the `writeA`/`writeB` closures, classified like `ErrClusterDegraded`. Delete `executeWriteWithDrain`. Skipped-leg metrics now flow through the normal path (observable; CHANGELOG). |
| 2b.3 | A-4 | Split `ClientConfig` into user options and an unexported runtime struct; `Config()` returns a copy (observable; CHANGELOG). Update `test/simulation`. |
| 2b.4 | A-7 | `root_validation.go`: reject `AutoMemoryWorker` combined with a user `Replayer`/`ReplayWorker`; warn on the two no-effect option combinations (observable; CHANGELOG). |
| 2b.5 | A-1 (F-17) | Delete redundant validation branches. |
| 2b.6 | A-3 | Export the capability interfaces (`ProbeReporter`, `EventEmitterSetter`, `Instrumentable`) next to `StrictWriter`; add compile-time assertions in `policy/`. |
| 2b.7 | A-10 | Fix documentation drift (`DefaultConfig` cluster names, `mirror/` godoc, `.agents/rules/100-overview.md`, `.knowledges/root/index.md`). |

**Verification:** 2a shows an empty behavioural diff in the Phase 0 matrix; each 2b PR states its
observable effect.

**Status (2026-09-03):** 2a and 2b.1 to 2b.7 done on branch `feat/root-restructure`.
The read matrix was unchanged by 2a and 2b.1.
Observable 2b changes (drain leg metrics, `Config()` copy, removed runtime fields,
new option rejection and warnings, exported capability interfaces) are recorded in
`CHANGELOG.md` under Unreleased.

---

## Phase 3 — Category 1 and 2 defaults, path correctness (`v1.7.0`)

| Item | Finding | Work |
|---|---|---|
| 3.1 | FB-1 | Provenance-based timeout classification: parent-context errors are non-health and skip failover on every leg; apply in `classifyReadErr`, `recordOpOutcomeAt`, and `AdaptiveDualWrite` strike accounting. |
| 3.2 | R-2 | `NoSynchronousAckError` for every zero-sync-ack pair; `RequireSynchronousAck` default; `AckOnReplayAdmission` option. When `Replayer == nil` and a leg failed, emit `IncReplayDropped` and `EventReplayDropped` with a distinct reason. |
| 3.3 | R-1 | Category 2 defaults: the Phase 1 retry policy becomes the default for both backends; old values documented as one-line restores. |
| 3.4 | FO-2 | `AdaptiveDualWrite` deferred-result capability; root enqueues replay only on deferred failure. Document that counters and collection ops require `Strict()` or `NonIdempotent()` under `AdaptiveDualWrite`. |
| 3.5 | N7 | `NonIdempotent()` on `Query` and `Batch`; `CounterBatch` implicitly non-idempotent. |
| 3.6 | FB-2 | `IterContext` and batch `IterContext` pin the cluster when `pageState != nil`. Follow-up: encode the cluster ID into the returned `PageState`. |
| 3.7 | FB-5 | `cqlIter.Close`: non-ctx error → `RecordFailure` and `OnFailure` (result ignored); clean close → `RecordSuccess`. |
| 3.8 | A-6, FB-6, FB-8 | Drain-aware re-selection inside `resolveReadTarget` so `Iter`, batch iter, and CAS honour it; drain-skip default for every `FallbackRead` leg with explicit opt-in; document CAS. |
| 3.9 | A-8 | `recordReadSuccess` calls `RecordSuccess` then `RecordLatency`. |
| 3.10 | D-6, N3 | Versioned replay envelope carrying consistency and serial consistency; worker-first dual-read rollout; `DefaultExecuteFunc` applies them. Document TTL drift. |
| 3.11 | D-3 | Explicit encoders for `*big.Int`, `*inf.Dec`, `net.IP`, `gocql.Duration`; empty `[]byte` distinct from nil; one unsupported-type check at enqueue in both backends. |
| 3.12 | FB-7 | Reject `WithTimestamp(0)` and a provider returning 0 at validation time. |
| 3.13 | A-1 (F-08), FO-8 | `Close` joins the auto-refresh and topology goroutines; `RefreshSession` compares the holder captured before the refresher call and closes the refresher's session, returning an error, if the holder changed. |

**Verification:** the Phase 0 regression tests for the listed findings pass; the classification
matrix shows parent-context rows as non-health on every entry point.

**Status (2026-09-03):** 3.1 to 3.13 done on branch `feat/root-restructure`.
The read matrix now has a caller-context column (non-health, no failover on every entry point)
and a driver-timeout column (a cluster error), and the R-2, FB-1, FB-2, FB-5, FO-2, D-3
regression tests are enabled.
Two items deviate from the table:

- 3.9 is documented rather than changed: calling `RecordSuccess` before `RecordLatency` would
  let the success reset the slow-read count the latency breaker accumulates, so `LatencyRecorder`
  now states that `RecordLatency` is the success signal for the read path.
- 3.10 versions the envelope (version field plus optional consistency levels, absent for
  version 1 messages) and documents the worker-first rollout, but keeps the subjects and
  consumer names unchanged: a work-queue stream forbids two consumers with overlapping filters,
  so a versioned consumer name would require migrating the durable consumer, and an older worker
  reading a version 2 message behaves exactly as it does today.

---

## Phase 4 — Authorities, per-cluster replay, auto-recovery (`v1.8.0`)

| Item | Finding | Work |
|---|---|---|
| 4.1 | N2 | Per-cluster memory replay queues (currently priority-partitioned only). Prerequisite for gating. |
| 4.2 | A-1 | Observation hub in root: `observe(cluster, op, err, latency)`; route `recordOpOutcome`, policy `RecordFailure`/`RecordSuccess`/`RecordLatency`, probe outcomes, and deferred-write outcomes through it. Observation only; no cross-authority forwarding. |
| 4.3 | FO-3, R-3 | Auto-refresh consumes hub observations: seed `lastSuccess` at construction and on swap; deferred-write and probe failures count; `WithAutoRefreshFailureClassifier` defaulting to the typed connectivity sentinels. Delayed close of the old session after a swap. |
| 4.4 | R-4 | `ForceDegrade` sticky operator latch; probe skips latched clusters; `recordFast` cannot clear the latch. Document in `docs/auto-recovery.md`. |
| 4.5 | R-5 | Probe credits recovery only when probe latency is within `absoluteMax` (or sibling plus delta); minimum degraded dwell time; exponential re-degrade backoff; flapping event. |
| 4.6 | FO-1 | Optional route-veto interface consulted after `Select`; `LatencyCircuitBreaker` implements it; disabled by default in v1 (Category 3). Fix the `circuit_breaker_open` guidance in `docs/cluster-events.md`. |
| 4.7 | FB-3, FO-9 | `WithFailoverBelowThreshold(bool)` on `NewCircuitBreaker` (default `false`); half-open recovery via the root policy-probe path; lazy reset evaluation so the gauge cannot stick at "open". |
| 4.8 | FB-4, FO-5 | `StickyRead`: swap during cooldown when the current preferred is the failing cluster; `Reset()`/`SetPreferred()`. `PrimaryOnlyRead`: single-flight the recovery probe. |
| 4.9 | R-7 | Per-cluster replay execution gate injected by root, driven by drain and operator quarantine. |
| 4.10 | R-8 | Built-in `AllowedClusters` helper excluding a cluster while its replay backlog exceeds a threshold. |
| 4.11 | FO-4 | `WithClusterWriteTimeout`: per-leg timeout inside the `writeA`/`writeB` closures; its expiry is a health signal by provenance. |
| 4.12 | R-9, R-10 | Probe failures logged at `Warn` on first failure and power-of-two counts; topology watcher retries `Watch` after falling back to polling. |

**Verification:** the review's scenario matrix S1–S9 re-run against the simulation harness; every
"Partial" for state convergence becomes "Yes"; S1 with `PrimaryOnlyRead` and the backlog helper
returns reads to A only after the queue drains.

---

## Phase 5 — Observability (`v1.8.0`)

| Item | Finding | Work |
|---|---|---|
| 5.1 | FO-6 | Preferred-cluster gauge and a route-change event emitted by the read strategies through a new optional interface. |
| 5.2 | FO-7 | Event dispatcher reserves slots for state-transition kinds or coalesces per-read kinds; policy outbox drops fold into the existing dropped-events total. |
| 5.3 | D-2 | Eviction event from `stream.Info()` polling when `FirstSeq` advances without acknowledgement. |
| 5.4 | D-8 | Count corrupt-message terminations and failed `Term()` calls in new series. |
| 5.5 | D-7 | `Nats-Msg-Id` from timestamp, cluster, and statement hash. |
| 5.6 | D-9, A-1 (F-20) | Document synchronous mirror drain in `Close()` with an optional drain timeout; document the no-`Close`-from-callback rule on the replay and mirror callbacks. |

---

## Phase 6 — Performance (`v1.9.0`)

| Item | Finding | Work |
|---|---|---|
| 6.1 | P-1 | Bounded executor pool in both replay backends (`WithReplayConcurrency`, default 8–16) on top of the per-cluster queues; memory backend uses blocking `Dequeue(ctx)`. |
| 6.2 | P-2 | `WithAsyncPublish` for `NATSReplayer` with bounded pending and a background acknowledgement consumer; encode-buffer pool. Synchronous publish stays the default. |
| 6.3 | P-3 | `CircuitBreaker.RecordSuccess` lock-free fast path when `failures == 0`. |
| 6.4 | P-4, P-6 | Atomic strike counters with a fast path in `AdaptiveDualWrite`; one job struct per dual write (no `sync.Pool`). |
| 6.5 | P-5 | Lock-free histogram storage in `contrib/metrics/vm`. |
| 6.6 | P-7 | Value fields plus a set-bitmask in `cqlQuery` / `cqlBatch` setters. |
| 6.7 | P-8 | gocql v1 adapter: `Release()` the original query after `WithContext`; benchmark against a live session first. |
| 6.8 | P-9 | Remaining low items: single clock read per op, single timer per replay batch, `Columns()` alias, batch entry capacity hint, `gocql.UUID` reflect path, mirror enqueue lock. |

**Verification:** benchmark numbers before and after each item in the PR description.

---

## Release Mapping

| Release | Phases | Headline |
|---|---|---|
| `v1.6.1` | 0, 1 | Opt-in outage-surviving replay; arg cloning; queue depth and age observable; corrected docs |
| `v1.7.0` | 2, 3 | Root restructured; zero-ack writes return an error; ctx errors no longer poison health; retry defaults survive outages; replay envelope versioned |
| `v1.8.0` | 4, 5 | Observation hub; sticky `ForceDegrade`; per-cluster replay gating; breaker and sticky-read fixes; route changes observable |
| `v1.9.0` | 6 | Replay throughput, async publish, contention fixes |

## Working Conventions

- One PR per numbered item unless two items touch the same lines; then combine and say so.
- Each PR states which item it closes, its compatibility category, and which review scenario
  verifies it.
- Behaviour changes are recorded in `CHANGELOG.md` under "Behavior change" with the restore
  option, in the same PR.
- Finding IDs stay in this file and the review; code, comments, tests, and commits describe the
  behaviour in plain language.

## Deferred to a Future Major Version

- Category 3 defaults flip: `Replicas = 3`, `DiscardNew`, route-veto on, `WithFailoverBelowThreshold = true`; `Safe` becomes the default profile.
- Single constructor returning `error` for every policy and replayer — A-9.
- Collapse the optional metrics interfaces into one extended collector — A-3.
- Move `TopologyUpdate` / `TopologyWatcher` / `TopologyOperator` to `types` — A-10.
- Require an explicit `Idempotent()` marker for unknown or custom operations — N7.
- Replay envelope carries deployment and logical-cluster identity and rejects mismatches — N9.
- Dedicated `WithCASCluster` routing for lightweight transactions — FB-8.
