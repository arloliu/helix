# 03 — Read Fallback and Failover: Fresh Review

Scope: read strategies, failover policies, fallback execution, stale-read risk, write-side failover, session auto-refresh, failover observability.
Method: source read of `cql_client.go`, `policy/*.go`, `types/*.go`, `adapter/cql/v1`, `events.go`, `config.go`; gocql v1.7.0 source for `WithTimestamp` semantics; ten throwaway tests run against a scratch copy of the repo (`-race`, all pass). Each finding is tagged CONFIRMED (code read and, where noted, test executed) or SUSPECTED.

Throwaway test file (not in repo): `/tmp/claude-1000/-home-arlo-projects-helix/bca19f30-1dd3-4e0c-b506-6561606e930b/scratchpad/helixcopy/fresh_review_test.go`.

---

## (a) Behavior summary

### A read, from first error on cluster A to steady state

```
Scan/MapScan/SliceMap/SliceScan (executeRead / executeReadNoFailover)
 │
 ├─ resolveReadTarget            AllowedClusters? → override list, strategy frozen
 │                               else ReadStrategy.Select(ctx)
 ├─ runPrimaryRead               drain-aware swap (non-override, non-paged only)
 │                               readFunc(ctx, sessionA) ; IncReadTotal, ObserveReadDuration
 │
 ├─ err == nil ─────────────────► ReadStrategy.OnSuccess, FailoverPolicy.RecordSuccess
 │                               (or RecordLatency → may RecordFailure if slow), recordOpOutcome(ok)
 │
 ├─ ErrNotFound / ErrRowLimitExceeded ► not a health signal.
 │        FallbackRead on? → executeFallbackRead: one silent try on the other cluster
 │        (drain BYPASSED for Scan/MapScan; drain SKIPPED for slice methods)
 │
 └─ any other error (incl. context.Canceled / DeadlineExceeded, syntax errors)
          IncReadError, recordOpOutcome(fail)          ← feeds auto-refresh
          FailoverPolicy.RecordFailure(A)              ← feeds breaker
          FailoverPolicy.ShouldFailover(A, err)?       ← nil policy = always yes
             no  → return err to caller (no second attempt)
             yes → ReadStrategy.OnFailure(A, err) → (alt, ok)   [override: snap.fallback]
                    ok && alt not draining → tryFallbackCluster
                          IncFailoverTotal, Warn log, EventFailover{Err}
                          readFunc(SAME ctx, sessionB)
                          ok  → OnSuccess(B), RecordSuccess(B)
                          err → RecordFailure(B), DualClusterError{A,B}
```

Steady state after A fails, per strategy:

| Strategy | Steady state while A is down | Return to A |
|---|---|---|
| `StickyRead` | First qualifying failure swaps `preferred` to B (unless within cooldown, in which case each read tries B-then... no: tries *preferred* first, then alt). Reads go straight to B; no further failover events. | Never automatically. Only a failure of B (outside cooldown) swaps back, or rebuild the client. |
| `PrimaryOnlyRead` | `failedOver=true`; reads go to B. | After `recoveryTimeout` every `Select` returns A until one read succeeds (clears) or fails (timer reset). Without timeout: never, unless B fails. |
| `RoundRobinRead` | Stateless. 50% of reads still hit A first, then fail over per read. | n/a |

Failover policy gating (all strategies):

| Policy | First N-1 failures on A | Failure N.. | After `resetTimeout` gap |
|---|---|---|---|
| `nil` / `ActiveFailover` | fail over | fail over | fail over |
| `CircuitBreaker(N)` | **returned to caller, no failover** | fail over | next failure on A is **returned to caller** (counter reset to 1), then N-1 more |
| `LatencyCircuitBreaker` | as CB; slow successes also count as failures | as CB | as CB |

### A write when cluster A fails

```
Exec (executeWriteWithReplay)
 ├─ both draining → ErrBothClustersDraining (strict: DualClusterError{Draining,Draining})
 ├─ one draining  → write healthy only; enqueue replay for draining (strict: PartialWriteError)
 └─ executeDualWrite
      WriteStrategy.Execute(ctx, writeA, writeB)
        Concurrent: both in parallel, wait both
        Sync:       A then B (or B then A); second skipped if ctx already dead
        Adaptive:   healthy legs waited; degraded leg = fire-and-forget → ErrWriteAsync
                    (or ErrWriteDropped if semaphore full)
      both nil                         → nil
      both real errors                 → DualClusterError, NO replay
      one nil / one {err|Async|Dropped}→ enqueue replay for the non-nil cluster(s), return nil
                                         (enqueue uses context.WithoutCancel)
      Replayer nil                     → partial failure silently lost (warned at construction only)
```

FailoverPolicy is not consulted on writes. The read strategies are not told about write failures.

### Trigger → component → action

| Trigger | Component | Action |
|---|---|---|
| Read returns non-sentinel error | `executeRead` | `IncReadError`, `recordOpOutcome(fail)`, `RecordFailure` |
| `ShouldFailover` false | `executeNormalFailover` | return primary error, no retry |
| `ShouldFailover` true | `ReadStrategy.OnFailure` | pick alt; `StickyRead` swaps preferred if outside cooldown |
| Alt cluster draining, primary not | `executeNormalFailover` | deny failover, return primary error |
| Alt read succeeds | `tryFallbackCluster` | `IncFailoverTotal`, `EventFailover`, `OnSuccess(alt)`, `RecordSuccess(alt)` |
| Alt read fails | `tryFallbackCluster` | `RecordFailure(alt)`, `DualClusterError` |
| `ErrNotFound` + FallbackRead | `executeFallbackRead` | one try on alt; `IncReadDivergence` + `EventReadDivergence` if found |
| Successful read slower than `absoluteMax` | `LatencyCircuitBreaker.RecordLatency` | `RecordFailure` (breaker may open); routing unchanged |
| `consecutiveFailures>=10 && now-lastSuccess>=5m && now-lastRefresh>=1m` | `maybeAutoRefresh` | call refresher, `SwapSession`, **close old session** |
| Topology `DrainMode` flip | `watchTopology` | set `drainA/B`, metric, `EventDrainEntered/Exited` |
| `AllowedClusters` returns list | `resolveReadTarget` | strategy frozen; list order = routing; drain intersected; fail-closed on empty |
| Write leg fails / async / dropped | `executeDualWrite` | replay enqueue for that cluster (if Replayer) |
| Both write legs fail (real) | `executeDualWrite` | `DualClusterError`, no replay |
| Adaptive degraded cluster gets a write | `AdaptiveDualWrite.fireAndForget` | background goroutine writes AND caller enqueues replay |
| Recovery probe succeeds on degraded cluster | `recoveryProbeLoop` | `RecordProbeSuccess` → may recover |

---

## (b) Findings, ranked

### HIGH

#### H1. Caller-side context cancellation/deadline is classified as a cluster failure — CONFIRMED (test T2, T5)

Evidence:
- `cql_client.go:449-452` `isReadTerminalNonHealth` excludes only `ErrNotFound` and `ErrRowLimitExceeded`. Everything else, including `context.Canceled` / `context.DeadlineExceeded`, reaches `IncReadError` (`:2363`), `recordOpOutcome` and `RecordFailure` (`:2437`, `:2481` path, `:2563`).
- `isCtxErr` (`:2579`) exists but is wired only into the *FallbackRead alt leg* of slice methods (`:3304-3305`, `:3376-3378`). The primary leg, the `Scan`/`MapScan` paths, and `tryFallbackCluster` do not use it.
- gocql returns `ctx.Err()` verbatim on cancellation (`query_executor.go:75-76,124-125`), so this is the real-world error shape.

Failure scenario (T2): `StickyRead(preferred=A)`, `CircuitBreaker(threshold=1)`. One caller cancels its request before the read. Result: `DualClusterError{A: canceled, B: canceled}`, `sticky.Preferred()==B` for the 5-minute cooldown, `cb.Failures(A)==1`, `cb.Failures(B)==1` — B was "failed" without ever being given a live context. With the default `nil` policy the flip happens on the first cancel. In a fleet, ordinary request-timeout noise randomly reshuffles which cluster each client reads from, and any workload with a nontrivial cancellation rate will keep both breakers near their thresholds and inflate `consecutiveFailures` for auto-refresh (see H4).

Failure scenario (T5): A hangs, caller deadline 30 ms. A returns `DeadlineExceeded` at 30 ms; Helix "fails over" to B with the same, already-expired context; B fails instantly; both clusters get `RecordFailure`; caller gets `DualClusterError`. The fallback attempt can never succeed in the most common outage mode (hung, not refusing). With `RoundRobinRead` this means 50% of reads fail for the whole outage.

Improvement direction:
1. Treat `isCtxErr(err)` as non-health on every leg (primary, failover, fallback), and do not call `OnFailure`/`RecordFailure` for it. Keep surfacing it to the caller.
2. Give the failover leg a budget: either require `ctx.Err()==nil` before attempting the alt (skip the pointless attempt and don't record B), or expose a per-attempt timeout option (`WithReadAttemptTimeout`) so the primary attempt is bounded below the caller's deadline. Document that the driver's own query timeout must be shorter than the caller deadline for failover to be useful.

#### H2. `AdaptiveDualWrite` degraded mode applies the same statement twice on the degraded cluster — CONFIRMED (test T4)

Evidence:
- `policy/adaptive_write.go:592/601` degraded leg → `fireAndForget`, which spawns a goroutine that executes the write (`:678-700`) and returns `ErrWriteAsync` (`:728`).
- `cql_client.go:2005-2016` then enqueues a replay for the `ErrWriteAsync` cluster ("replay is a safety net").
- `DefaultExecuteFunc` (`:1436-1452`) re-executes the payload on the same cluster.

T4: `ForceDegrade(B)`; one `UPDATE counters SET hits = hits + 1` → B receives 1 execution from fire-and-forget and 1 from replay. For counters, list appends, and any `USING TTL`-less non-idempotent statement, this is a guaranteed double-apply in steady degraded state — not a race, the design. `docs/replay-system.md:633-642` warns about counters for *failed* writes; it does not say that a *degraded* cluster double-applies every write. `types.go:145-149` warns on `CounterBatch` only.

Improvement direction: for `ErrWriteAsync`, enqueue the replay only when the background write reports failure (move the enqueue into the fire-and-forget completion path, using a callback the client passes in), or add a `WithAdaptiveAsyncReplay(false)` knob, and document loudly that counters must use `Strict()` or single-cluster with `AdaptiveDualWrite`.

#### H3. An open `LatencyCircuitBreaker` never moves traffic — CONFIRMED (test T3)

Evidence: `recordReadSuccess` → `RecordLatency` (`cql_client.go:2213`) → `RecordFailure` on slow success (`latency_circuit_breaker.go:245-246`). Nothing consults breaker state on the success path, and `Select` never consults the breaker. `ShouldFailover` is only reached from the error path.

T3: A answers every read successfully in 2 ms with `absoluteMax=1µs, threshold=1`. After 5 reads: `lcb.ShouldFailover(A)==true` (open), `failuresA==5`, and all 5 reads went to A, 0 to B. The `circuit_breaker_open` event fires and `docs/cluster-events.md:100` tells the operator "Page — reads are being routed away from Cluster", which is false. The package doc promise "helps detect degraded clusters that are technically responding but too slow to be useful" only materialises if a hard error later happens.

Improvement direction: either (a) have the client ask the policy before selecting — e.g. an optional `RouteAway(cluster) bool` interface the read path checks after `Select` and, if true, swaps to the alternative (respecting drain) and calls `OnFailure` once so sticky state follows; or (b) rename/document LCB as "pre-arms failover" and fix the event doc.

#### H4. Auto-refresh can close a healthy session on user/schema errors; window is unarmed at startup — CONFIRMED (test T11)

Evidence:
- `clusterStats.lastSuccessNanos` (`cql_client.go:144`) is never seeded; only `recordOpOutcomeAt` stores it on success (`:490`). Predicate `now - 0 >= 5m` (`:270`) is true from construction until the first success.
- `recordOpOutcomeAt` counts every non-sentinel error (`:509-518`), including schema errors, invalid-query errors, ctx errors (H1).
- `RefreshSession` closes the old session unconditionally (`:1386`).

T11: ten `INSERT` failures with "Unconfigured table" (cluster reachable), then one detector tick → refresher invoked, original session A closed. In production: a deploy that ships a query against a not-yet-migrated table, on a low-traffic service (no reads succeed in 5 min), tears down a perfectly good connection pool every `MinRetryInterval`, aborting in-flight ops each time. Even after the first success, ctx-cancel storms plus user errors satisfy the count predicate; the time predicate only needs an idle 5 minutes.

Improvement direction: seed `lastSuccessNanos = now` at construction and on `SwapSession`; classify errors — only connectivity-class errors (`gocql.ErrNoConnections`, `ErrConnectionClosed`, dial/EOF, driver timeouts) should count toward auto-refresh; expose a `WithAutoRefreshFailureClassifier(func(error) bool)` since Helix cannot import driver error types. Consider making the refresher's old-session close deferred (`SwapSession` semantics + delayed close) so in-flight ops on a false positive survive.

### MEDIUM

#### M1. `CircuitBreaker` denies failover for the first `threshold-1` failures and again after every `resetTimeout` gap — CONFIRMED (tests T1, T8)

Evidence: `failover_policy.go:451` returns false below threshold; `:457-459` returns false once the gap exceeds `resetTimeout`. `executeNormalFailover` (`cql_client.go:2481`) then returns the primary error with no second attempt.

T1: threshold 3, A down, B healthy: reads 1 and 2 return errors to the caller, read 3 fails over. T8: `RoundRobinRead` + threshold 2 / reset 10 ms, steady A outage, 40 reads → 4 user-visible failures (one per reset window per A-targeted read). The "half-open probe" is a real user request that is denied its fallback. This is documented ("prevents flapping"), but for idempotent reads the thing being prevented — a retry on the healthy cluster — is exactly what the caller wants; the breaker protects nothing on the read side because Helix never *avoids* sending traffic to an open cluster (H3). Net effect: `CircuitBreaker` strictly increases user-facing read errors relative to `ActiveFailover`.

Improvement direction: separate "may I retry this read on the other cluster" (should be yes whenever the alt is not draining) from "should the strategy change its preferred cluster" (threshold-gated). Concretely: always attempt the alt in `executeNormalFailover`, and pass the breaker's verdict to `OnFailure` so `StickyRead` only swaps preferred when the breaker is open. Half-open probing should then be a background probe (the recovery-probe loop already exists for writes) rather than a sacrificed user read.

#### M2. `StickyRead` cooldown pins every read to a dead cluster for up to 5 minutes — CONFIRMED (test T10)

Evidence: `read_strategy.go:166-169,175-179` return the alternative for the current request without changing `preferred` while in cooldown; `Select` (`:109`) always returns `preferred`.

T10: A blips once → preferred=B (cooldown starts). B then goes hard-down. Ten reads: all ten hit B first (each paying B's failure latency/timeout), then A. Reads "succeed", but every read for the remaining cooldown carries a full failed attempt, and with `CircuitBreaker` the first `threshold-1` of them fail outright (M1). The cooldown was meant to stop oscillation; here it prevents a single, clearly-correct swap. Also noted: cooldown expiry alone never returns reads to the original preferred (documented), so a fleet accumulates a random A/B distribution over time.

Improvement direction: allow the swap during cooldown when the *current* preferred is the one failing (that is not oscillation, that is both clusters having failed in sequence), or use a per-cluster consecutive-failure count instead of a wall-clock cooldown. Add `StickyRead.Reset()/SetPreferred()` for operators.

#### M3. `Iter`/`IterContext` with `PageState` can ship a cursor to the other cluster — CONFIRMED (test T6)

Evidence: `IterContext` (`cql_client.go:3209`) calls `resolveReadTarget(ctx, readOptions{})` — `preserveSelectedCluster` is never set, unlike the slice paths (`sliceReadOpts` `:3067-3075`) which the code itself describes as necessary because "sending the next page's cursor to a different cluster is unsound".

T6: page 1 via `Iter()` goes to A (preferred). A concurrent read failure flips sticky to B. Page 2, same `PageState`, goes to B with the A-issued cursor. Also true for `PrimaryOnlyRead` recovery probes, drain flips, and `AllowedClusters` changes between pages. Outcome ranges from a driver error to silently skipped/duplicated rows depending on driver/cluster (Cassandra vs ScyllaDB paging-state formats differ).

Improvement direction: when `q.pageState != nil`, resolve with `preserveSelectedCluster: true`, or better, encode the cluster ID into the `PageState` Helix hands back (`cqlIter.PageState()`) and route on it.

#### M4. Iterator errors are invisible to the failover policy and read strategy — CONFIRMED (test T7)

Evidence: `cqlIter.Close` (`cql_client.go:3731-3742`) calls `recordOpOutcome` and `OnSuccess` only; never `RecordFailure`/`RecordSuccess`/`OnFailure`. T7: five consecutive `Iter().Close()` errors on A with `CircuitBreaker(1)` → `Failures(A)==0`, sticky still prefers A, B never contacted. An `Iter`-heavy service never fails over, never trips the breaker, and (since `RecordSuccess` isn't called either) a breaker opened by `Scan` traffic is not closed by successful `Iter` traffic. Only auto-refresh sees iterator failures.

Improvement direction: on `Close` error that is not a ctx error, call `FailoverPolicy.RecordFailure` and `ReadStrategy.OnFailure` (ignore the returned alt — no retry is possible), and call `RecordSuccess` on clean close, so health state is consistent across read APIs. Document that `Iter` still cannot retry.

#### M5. Stale reads after recovery: no built-in signal, and `PrimaryOnlyRead` recovery is a thundering-herd probe — CONFIRMED by code read

Evidence: `docs/strategy-policy.md:586-600` acknowledges strategies have no replay-backlog visibility; the only mitigation is the operator-driven `WithAllowedClusters`. `PrimaryOnlyRead.Select` (`read_strategy.go:273-277`) returns A for *every* concurrent caller once the timeout elapses until one of them finishes. If A is hung (not refusing), every read issued during a full driver-timeout window is sacrificed to the probe, and each failing one resets the timer (`:317`) so the next herd arrives one `recoveryTimeout` later.

Improvement direction: single-flight the probe (CAS a `probing` flag; only one caller gets A; others get B). For stale-read protection, expose the replay queue depth (`SetReplayQueueDepth` already exists as a metric) to a built-in `AllowedClusters` helper, e.g. `helix.ExcludeWhileReplayBacklog(worker, threshold)`, so auto-recovery back to a cluster waits for replay drain without a bespoke feature-flag pipeline.

#### M6. FallbackRead on `Scan`/`MapScan` reads from a draining cluster — CONFIRMED by code read

Evidence: `executeFallbackRead` skips the draining alt only when `opts.skipDrainingAlt` (`cql_client.go:2645-2651`), which only slice methods set. Documented in `docs/fallback-read.md` ("bypasses drain state"). Drain is the operator's "do not read here" signal — typically because the cluster is being backfilled/repaired and may return stale rows. A `Scan` that gets not-found on the healthy cluster (row genuinely deleted) can be answered with a stale, resurrected row from the draining cluster. The slice methods and the `AllowedClusters` fence both refuse this; single-row reads are the inconsistent case.

Improvement direction: make drain-skip the default for all FallbackRead legs and offer an explicit `FallbackReadIncludingDraining()` opt-in.

### LOW

#### L1. `WithTimestamp(0)` / a `TimestampProvider` returning 0 silently breaks replay LWW — CONFIRMED (gocql source)

`getTimestamp` (`cql_client.go:3084`) forwards a caller's explicit `WithTimestamp(0)` (or a provider returning 0) into `ReplayPayload.Timestamp`. gocql treats `defaultTimestampValue==0` as "use now" (`frame.go:1547-1553`), so the replayed write gets a fresh timestamp at replay time and overwrites any newer direct write. Guard: reject 0 in `WithTimestamp`/`TimestampProvider` at validation time. Also note `DefaultTimestampProvider` is wall-clock microseconds across independent clients — LWW correctness depends on fleet clock skew < inter-write spacing; worth one sentence in docs.

#### L2. Read-strategy state changes are unobservable — CONFIRMED

`policy/read_strategy.go` imports no logger, metrics, or event emitter. A `StickyRead` preferred swap or `PrimaryOnlyRead` failover/probe emits nothing. `EventFailover` fires only on the read that failed over (per-read, buffer of 128 at `events.go:21`, dropped under load), after which all reads go silently to B. There is no "preferred cluster" gauge in `types/metrics.go`. An operator can see `read_total{cluster}` shift and, if not dropped, one `failover` event with `Err`, but cannot answer "which cluster is this client sticky to right now, and why did it move" from telemetry. Fix: add `SetReadPreferredCluster(cluster)` gauge + `EventReadRouteChanged{From,To,Reason}` emitted from the strategies (they already receive `SetClusterNames`; the same injection path can carry the emitter, as `CircuitBreaker` does).

#### L3. `RefreshSession` closes whatever is installed at swap time — SUSPECTED

`RefreshSession` (`cql_client.go:1338-1390`) calls the refresher, then `SwapSession`, then closes `old`. If an operator's manual `SwapSession` lands between the refresher call and the swap, `old` is the operator's fresh session and gets closed. Documented as "undefined" for concurrent `Close`, not for concurrent `SwapSession`. Fix: CAS on the holder pointer captured before the refresher call; if it changed, close the refresher's session instead and return `ErrSessionSwappedConcurrently`.

#### L4. Both-draining asymmetry — CONFIRMED by code read

Reads with both clusters draining proceed on the selected cluster (`cql_client.go:2286-2287`) and may fail over to the other draining cluster; writes fail fast with `ErrBothClustersDraining` (`:1770`). Reasonable, but undocumented in `IsDraining`'s doc ("Reads are failed over to the non-draining cluster").

#### L5. CAS routing ignores override and drain — CONFIRMED by code read

`selectClusterForCAS` (`cql_client.go:1501-1507`) uses `ReadStrategy.Select` directly: a draining or operator-excluded cluster still receives LWTs, and a `StickyRead` flipped by a read blip (H1) silently moves *all* CAS writes to the other cluster — which, since CAS is not replicated, is a correctness hazard for anyone relying on LWT serialisation on one cluster. Documented in `strategy-policy.md:679`, but the coupling to read-side blips is not. Consider a dedicated `WithCASCluster` or at least drain-awareness.

---

## (c) What is already solid

- **Single-resolution read target.** `resolveReadTarget` snapshots override + drain once per operation; the override path is fail-closed on empty/unknown/panicking provider output, with rate-limited error logs. Override freezes strategy state (no `OnFailure` on an excluded cluster) — the right call.
- **Not-found and row-limit are never health signals**, consistently across primary, failover, fallback, iterator and auto-refresh paths; the `scanFnNotFoundShieldError` handling for user-returned `ErrNotFound` in `SliceScan` is careful and correct.
- **Slice reads with `PageState`** correctly pin the cluster, disable failover and empty-retry, and discard partial buffers on any error (`drainIterToSliceMapWithLimit` nil-on-error contract). `SliceScan` correctly refuses to re-invoke the caller's `scanFn` after partial mutation.
- **FallbackRead alt-leg semantics for slice methods** (ctx errors propagate and are non-health; alt skipped when draining; `ErrRowLimitExceeded` never masked) are well reasoned.
- **`CircuitBreaker` internals**: per-cluster mutex around the compound update, transition sequence numbers so a superseded reporter never writes a stale gauge, events queued under the state lock and delivered with no locks held, exactly-once trip metric, reentrancy-safe emitter. Concurrency tests exist and pass.
- **Write path**: `RecordFailure`-free; partial failures always enqueue with `context.WithoutCancel`; replay drops are surfaced by metric + event + `WithOnReplayDropped`; `Strict()` gives callers a no-replay, `PartialWriteError` contract with `Acknowledged/Unacknowledged` for compensation. Panics in either leg are converted to that leg's error so the sibling is always joined.
- **Session swap** is a wait-free atomic pointer swap; closures resolve the session at dispatch time so fire-and-forget legs keep "dispatched to cluster X" semantics; single-cluster mode cannot be promoted by accident.
- **Event dispatcher**: non-blocking emit, exact drop accounting, drop total reconciled into a metric, handler panics contained.

## (d) Open questions

1. Is `CircuitBreaker` on the *read* side meant to protect the clusters (it does not — traffic is never withheld) or the caller (it costs the caller reads)? If the intent is to gate *strategy state changes* rather than *retries*, M1's split resolves both H3 and M1.
2. Was double-execution under `AdaptiveDualWrite` degraded mode (H2) a conscious "at-least-once" choice? If so it needs to be a headline warning next to counters/collections, and `Strict()` needs to be the documented path for them.
3. What are the intended semantics of ctx errors on the primary leg? The slice-path design explicitly says ctx errors "would otherwise poison the failover-policy view of cluster health" — that reasoning applies equally to every other leg.
4. Should `AllowedClusters` and drain apply to CAS? The current doc rationale ("override is a read-safety mechanism") does not address a draining cluster receiving LWTs.
5. For `Iter` + `PageState`: is cross-cluster paging-state portability assumed for the supported drivers/clusters, or is M3 simply a gap left when the slice methods were hardened?
6. Fleet behaviour: `StickyRead`'s random initial choice plus never-return-to-A means the A/B read split drifts with each incident. Is a deterministic seed (e.g. hash of hostname) or a periodic rebalance desired, or is the drift acceptable because both clusters are equal peers?
