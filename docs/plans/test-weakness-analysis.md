# Test weakness analysis — 2026-09-14

A measurement pass over the test suite,
asking three questions in order:
what is actually uncovered,
where the covered parts are covered weakly,
and what no coverage number can see.

Nothing here was a request to change production code.
Where a finding names a production bug or a missing seam,
it is recorded as a finding,
and the decision to act on it is taken separately —
as it was for S7, which is closed by a one-line fix decided after the fact.

## What was measured

Unit coverage alone understates the library badly,
because `make coverage` runs only `TEST_DIRS`,
which excludes `test/integration/` and the `e2e` build tag —
and the adapters, the `Iter` methods and the slice-read path are exercised almost entirely there.

The numbers below merge a unit profile with
`go test ./test/integration/... -coverpkg=./...`,
taking the maximum count per block.
Statement-weighted, non-test packages only:

| package | unit only | merged | merged, after S1+S6 | statements |
|---|---|---|---|---|
| root (`helix`) | 94.3% | **94.7%** | 94.7% | 1919 |
| `replay` | 82.6% | **82.9%** | 82.9% | 1926 |
| `policy` | 94.3% | **95.9%** | 95.9% | 1021 |
| `contrib/metrics/vm` | 59.1% | **59.1%** | 59.1% | 254 |
| `adapter/cql/v2` | 42.9% | **58.2%** | 61.5% | 182 |
| `adapter/cql/v1` | 33.7% | **70.9%** | 74.4% | 172 |
| `topology` | 97.7% | **97.7%** | 97.7% | 172 |
| `mirror` | 97.1% | **100.0%** | 100.0% | 103 |
| `types` | 70.3% | **90.6%** | 100.0% | 64 |
| **total** | | **87.7%** | **88.1%** | 5833 |

The first two columns are the measurement this analysis was built on.
The third re-runs the merge after the tests added for S1 and S6 below,
so the two are comparable.

Two of the alarming unit-only numbers dissolve on merge:
`types` at 70.3% and `adapter/cql/v1` at 33.7% are instrumentation artifacts,
not gaps.
Three do not: `contrib/metrics/vm`, `adapter/cql/v2`, and `replay`.

`adapter/cql/v2` scoring *below* `adapter/cql/v1` is an inversion worth naming,
because the README steers new users to v2.
The integration suite builds v2 sessions (`getSharedSessionsV2`),
so this is not a wiring gap —
v2 simply carries more surface that no tier reaches.

## Findings

### S1 — Driver enum conversions are unguarded numeric casts — CLOSED

`adapter/cql/v1/helpers.go` and `adapter/cql/v2/helpers.go` convert consistency levels, batch types and serial consistency
by casting the integer straight across:

```go
func ToGocqlConsistency(c cql.Consistency) gocql.Consistency {
	return gocql.Consistency(c)
}
```

All twelve of these functions were at **0% coverage** in both tiers profiled here, unit and integration.
The e2e tier was not profiled;
a grep for callers found only the `doc.go` references,
so nothing internal reaches them there either.
They are exported API, documented in both `doc.go` files as the supported way to interoperate with the raw driver.

The cast is correct today.
A probe across all three enums confirms helix, gocql v1 and cassandra-gocql-driver v2 agree on every value
(`Quorum`=4, `LocalQuorum`=6, `LocalOne`=10, `Serial`=8, `LocalSerial`=9,
batch types 0/1/2).

The hazard is that nothing holds it there.
If either driver renumbers or reorders a constant in a future release,
`Quorum` silently becomes `One`,
the compiler says nothing,
no test fails,
and a dual-write library quietly stops writing at the durability level its caller asked for.
This is the single cheapest high-value gap in the repo:
pure functions, no Docker, no fixtures.

**Closed** by `adapter/cql/v1/helpers_test.go`
and `adapter/cql/v2/helpers_test.go`:
a table per enum pinning every constant to its named driver counterpart —
`gocql.Quorum`, not the literal `4` —
so a renumber on the driver side is what breaks the test.
Both directions of each conversion are asserted.

Verified against an injected defect —
adding `+ 1` to each cast fails 14 subtests per adapter;
restoring passes.

### S2 — No goroutine-leak guard anywhere — CLOSED

The library's product is background goroutines:
the event dispatcher (`events.go:229`),
the replay worker and its per-cluster backends
(`replay/worker.go:490-498`, `:676`),
the memory backend's retained-retry spawns
(`replay/memory_retained.go:240`, `replay/memory_worker.go:94`, `:206`),
the mirror engine's worker pool (`mirror/engine.go:278`),
both topology watchers (`topology/local.go:67`, `topology/nats.go:128`),
and the adaptive-write background leg (`policy/adaptive_write.go:938`).

Shutdown correctness is asserted indirectly —
26 quiescence assertions now hang off `Stop()`/`Close()`
(see [eventually-exactness-guards.md](eventually-exactness-guards.md)) —
but nothing asserts the goroutines actually *exited*.
`go.uber.org/goleak` does not appear in `go.mod` or `go.sum`.

A probe answered the immediate question:
a `TestMain` taking a goroutine-stack snapshot before `m.Run()`
and diffing after, with a settle-retry,
found **zero leaked goroutines** across root, `replay`, `mirror`, `topology` and `policy`.

So this was never an open bug.
It was an unguarded invariant:
the shutdown paths are correct today
and nothing would have noticed if a future change broke them.

**Closed** by `test/testutil/leak`,
wired into the five packages that own goroutines.
No new dependency:
rule 300-testing already names a goroutine's existence,
read out of `runtime.Stack`,
as a case where polling is the correct tool,
so the dependency-free route needed no approval —
whereas `go.uber.org/goleak` would have needed one
under rule 100-overview §3.
The helper got the full result on its own,
so the dependency would have bought convenience, not capability.

The package sits under `test/testutil/` rather than in `testutil` itself
because `testutil` imports helix,
which would make it unimportable from helix's own in-package tests.

Two entry points.
`leak.TestMain(m)` guards a whole package
and is what the five `leak_main_test.go` files call.
`leak.Check(t)` guards one test,
for a lifecycle case where naming the test matters more
than naming the binary;
nothing calls it yet.

Both settle before reporting:
a goroutine released by `Close` still has to be scheduled before it returns,
so an instantaneous snapshot would call every clean shutdown a leak.
The wait is bounded at 5s and costs wall clock only on the failing path.

Verified against three injected defects,
one per bucket:

- a bare `go func() { time.Sleep(time.Hour) }()` —
  caught, and the report names the test that started it;
- a test that calls `Engine.Start()` and forgets `Stop()` —
  caught, both workers reported, stacks naming `(*Engine).worker`;
- the same forgotten `Stop` under `leak.Check(t)` —
  caught, attributed to the test rather than the binary.

Restoring each passes.
`test/testutil/leak` also carries its own tests
for the settle, ignore and reporting logic.

One thing the probe cannot do:
removing `wg.Wait()` from `Engine.Stop` was tried as a fourth, more realistic
defect and had to be abandoned —
an existing drain test blocks forever without the join,
so the binary hangs to its timeout rather than reaching the leak check.
That is the drain test doing its job,
and it is worth knowing that the two guards overlap there.

### S3 — `MemoryReplayer.Dequeue` blocking path was untested — CLOSED

`replay/memory.go:306 Dequeue` sits at **39.1%**.
Uncovered: the uninitialized guard, the pre-check `ctx.Done()`,
the closed-and-drained exit,
and **all four arms of the blocking select** —
the path a caller takes when the queue is empty and it waits for work.

The worker does not use it;
`memoryBackend.dequeueLoop` polls `tryDequeueRetained` instead
(`dequeueWithPriority` and `dequeueStrict` belong to the NATS backend
and never touch `MemoryReplayer`).
That is exactly why it rots:
`Dequeue` is exported API a user calling `MemoryReplayer` directly will reach,
and the library itself never walks it.

Neighbouring numbers in the same file tell the same story:
`tryDequeueRetained` 60.0%, `Enqueue` 75.0%, `IsClosed` 0.0%,
`normalizeMemoryReplayerForLegacy` 50.0%,
`memory_worker.go:270 requeueGated` 40.0%.

**Closed** by the `TestMemoryReplayerDequeue*` tests in `replay/memory_test.go`,
which take `Dequeue` from 39.1% to 100%.
One test per non-blocking bucket —
the uninitialized guard, an already-cancelled context,
and the closed-and-drained exit —
plus a subtest per blocking arm and the two blocked-caller cases.
Cancel-while-blocked was already covered
by the existing `TestMemoryReplayerDequeueBlocking`.

The arm subtests turn on one hazard worth recording.
`tryDequeueWithPriority` runs before the blocking select and returns the same
`(payload, true)`, so a test that enqueues and then calls `Dequeue`
passes through the try-path and never touches the arm it claims to cover.
Each subtest therefore starts `Dequeue` on an empty replayer,
waits until a goroutine is actually parked on a select inside `Dequeue`
— read out of `runtime.Stack`, matching the parked state and the frame together,
which `Dequeue`'s other select cannot satisfy because it carries a `default` —
and only then enqueues one payload into one (cluster, priority) slot.
Any second ready channel would destroy attribution,
since `select` picks at random among ready arms.
Each subtest asserts the arm's `noteDequeuedLocked` bookkeeping,
not just the payload.

Verified against injected defects, one per bucket and one per arm.
Mis-assigning `idx` inside a single arm fails that arm's subtest and no other;
flipping only `high`, leaving `idx` correct,
fails on the `highProcessed` assertion,
which is what proves that assertion is load-bearing rather than dead.
Inverting the uninitialized guard or the closed-and-drained guard
parks the caller forever and the subtest dies at the package timeout.
Restoring each passes.

`replay` unit coverage moved 82.6% to 83.3%.
The measurement is unit-only;
`replay` is the one package where unit and merged coverage sit within 0.3%,
so the table was left as measured rather than re-running the Docker tier.

### S4 — `contrib/metrics/vm` at 59.1%, unmoved by any tier

254 statements, the largest untested block outside `replay`.
Unit and merged coverage are identical,
meaning integration does not touch it at all.

Per memory, `vm` now implements all 13 optional metric interfaces.
Those interfaces are how helix decides at runtime which observations to emit;
a wrong or missing one is invisible in production except as a metric that never appears.
Individual recorders sit at 0%:
`AddMirrorDrainDropped`, `AddReplayEvicted`, `AddClusterEventsDropped`.

**Closes when** each optional interface assertion is pinned
(`var _ helix.XxxMetrics = (*Metrics)(nil)`)
and each recorder is called once with the emitted series name asserted.

### S5 — Adaptive-write latency sampling has no clock seam

`policy/adaptive_write_test.go` holds **42 of the suite's 94 `time.Sleep`s**.
They are not condition-waits — the async-wait rule already covers those —
they are the test *simulating* write latency:

```go
func(ctx context.Context) error {
	time.Sleep(60 * time.Millisecond) // Exceeds 50ms
	return nil
},
```

The strategy has an injectable clock for hysteresis
(`nowNanos()` honours `a.now`, `adaptive_write.go:1047`),
but the latency measurement itself does not:
`time.Now()` is called directly at
`adaptive_write.go:798`, `:804`, `:812`, `:824`, `:949`, `:1689`, `:1695`, `:1702`, `:1713`.

The consequence is that every threshold assertion in that file is a wall-clock race against the machine running it.
A 30ms-vs-100ms comparison is comfortable;
a 5ms-vs-30ms one (`adaptive_write_test.go:430`) on a loaded CI box is not.
The file also costs ~5.8s of the suite's runtime almost entirely in sleeps.

This is a testability finding, not a correctness one.
No flake has been observed.
Extending the existing `a.now` seam to cover latency sampling would let all 42 sleeps become instant clock steps —
but that is a production change, and the call is the maintainer's.

### S6 — `ClusterNames.Validate` rejection branches untested — CLOSED

`types/types.go:59 Validate` and `:108 validateClusterName` sat at 57.1%.
The happy path was covered; none of the five rejections were.
Cluster names become metric label values,
so a name that slips through turns up as a malformed series long after the client was constructed.

**Closed** by `types/cluster_names_test.go`:
a table over both fields covering empty, over-32-characters,
leading digit, hyphen, dot, space and identical-names,
plus the A-before-B ordering of the error.
`types` merged coverage moved 90.6% → 100%
(unit-only 70.3% → 98.4%).

Verified against injected defects —
widening the length bound to 64 and neutering the identical-names check fails 3 subtests; restoring passes.

Note on what this finding originally said:
the root re-exports `client.go:105 AsPartialWriteError`
and `:111 IsPartialWrite` are also at 0% in both profiled tiers,
but the `types` functions they forward to are at 100%,
so they belong in the out-of-scope list below,
not here.

### S7 — `UnwrapSession(nil)` panicked in v1, returned nil in v2 — CLOSED

Found while writing the S1 tests, not by coverage.

Both adapters shipped `UnwrapSession` unguarded.
`ac831a4` added the nil guard and the "may be nil" wording to v2,
along with v2's `TestUnwrapSessionNil`,
and did not touch v1 —
so v1 kept dereferencing the pointer it was handed:

```go
func UnwrapSession(s *Session) *gocql.Session {
	return s.session
}
```

`v1.UnwrapSession(nil)` was a nil-pointer dereference.
v1 had no equivalent of v2's test,
which is why the asymmetry survived.

**Closed** by adding the guard to v1 so the two adapters agree,
with `TestUnwrapSessionNil` in `adapter/cql/v1/adapter_test.go`
mirroring v2's test of the same name.
The v1 doc comment, which promised a session unconditionally,
now carries v2's "may be nil" wording.
A sweep of both `helpers.go` files found no other guard v2 has and v1 lacks.

Verified against the defect it fixes —
the test was written first and failed with a nil-pointer dereference
in the unguarded `return s.session`;
adding the guard passes.

`gorelease`'s criterion makes this patch-level:
no exported signature moved,
and the behaviour lands on the contract v2 already documents.
The CHANGELOG gained an `[Unreleased]` section with a Fixed entry.
Releasing stays the maintainer's call.

### S8 — a `Dequeue` parked when `Close` runs is never released

Found while closing S3, not by coverage.

`Close` (`replay/memory.go`) stores an atomic flag and deliberately leaves the
channels open, so concurrent `Enqueue` calls cannot panic on a closed channel.
The consequence is that a caller already parked in `Dequeue`'s blocking select
does not wake when the replayer closes.
It stays parked until a payload arrives or its own context is cancelled.

The `Dequeue` doc comment promises more than that:

> Returns false if the context is cancelled or the replayer is closed and empty.

That holds for a call entering `Dequeue` after `Close` —
the closed-and-drained check near the top returns immediately —
but not for one already blocked.
A caller that dequeues with `context.Background()` and relies on `Close`
to end its loop hangs for good.
The library's own worker does not take this path —
`memoryBackend.dequeueLoop` polls `tryDequeueRetained` on its own interval
and never blocks in `Dequeue` —
which is why the gap survived.

The current behaviour is pinned by
`TestMemoryReplayerDequeueCloseWhileBlockedDoesNotWake`,
with a comment saying it is the contract as it stands, not as it should be.

Recorded, not fixed.
Both routes are production changes and the call is the maintainer's:
release blocked callers on `Close`,
or narrow the doc comment to say that `Close` does not release a blocked caller
and that a cancellable context is the only way out.

Releasing them is more than one new select arm.
The broadcast itself is easy — a `done` channel closed once by `Close`,
since closing the payload channels would reintroduce
the concurrent-`Enqueue` panic `Close` exists to avoid.
But a `done` arm alone would break the promise it is meant to keep:
`select` picks at random among ready arms,
so a closed-but-not-yet-drained replayer would return `false`
with payloads still queued.
Waking on `done` has to re-run the try-path
and return `false` only once the queues are genuinely empty.

## Explicitly out of scope — not gaps

Recorded so the percentages are not mistaken for debt later:

- **Delegating setters.** `batch.go` `SetConsistency`/`SerialConsistency`/
  `Size`/`WithContext`/`WithPriority`/`NonIdempotent`,
  `query.go:73/87/111`.
  One-line field writes.
- **Deprecated compat shims.** `adapter/cql/v{1,2}/adapter.go`
  `NewBatch`/`ExecuteBatch`/`ExecuteBatchCAS`/`MapExecuteBatchCAS` each forward one call to the non-deprecated method,
  which is covered.
- **Nop implementations.** `internal/logging/nop.go`,
  `mirror/engine.go:264-270` nop-metrics shims.
- **Root re-exports of `types` helpers.**
  `client.go` `AsPartialWriteError`/`IsPartialWrite`/`IsNotFound`/`IsRowLimitExceeded` each forward one call;
  the `types` originals are all at 100%.
- **`test/testutil`, `test/simulation`.**
  Harness code;
  its coverage number measures which scenarios a given run selected,
  not test quality.

## Structural notes

**1244 top-level tests, 52k test LOC against 34k production LOC.**
23 tests carry no assertion; all 23 are deliberate —
compile-time interface checks (`TestSessionImplementsInterface` and kin)
or race-detector-only concurrency tests
(`TestCircuitBreaker_ConcurrentSuccessAndFailure_NoRace`).
The latter are inert under `make test-quick` and `make coverage`,
both of which drop `-race`; `make test-unit` and `make ci` keep it.
No action — worth knowing before trusting a `test-quick` green.

**One `t.Parallel()` in the whole suite.**
A throughput matter, not a correctness one.
Given the shared containers in the integration tier and the wall-clock coupling in S5, leaving it alone is defensible.

**Packages with no test file at all:**
`internal/metrics` (1 statement, covered transitively),
`internal/typeutil` (6 statements, 83.3% transitively),
`adapter/cql` (interface definitions only),
and the five `examples/` mains.
None warrant their own suite.

## Status and suggested order

S1, S2, S3, S6 and S7 are closed.
Remaining, in the order they are worth doing:

**S4** — mechanical, no decisions needed.

**S8** — a production change or a doc correction; the choice is the maintainer's.

**S5** — a design question to answer before it is a test task.

Per the standing rule, each new guard must be shown to fail against an injected defect before it counts as closing anything.
Green and inert look identical.
