# Test weakness analysis — 2026-09-14

A measurement pass over the test suite,
asking three questions in order:
what is actually uncovered,
where the covered parts are covered weakly,
and what no coverage number can see.

Nothing here is a request to change production code.
Where a finding names a production bug or a missing seam,
it is recorded as a finding,
and the decision to act on it is separate.

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

### S2 — No goroutine-leak guard anywhere

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

A throwaway probe answered the immediate question:
a `TestMain` taking a goroutine-stack snapshot before `m.Run()`
and diffing after, with a settle-retry,
found **zero leaked goroutines** across root, `replay`, `mirror`, `topology` and `policy`.
The probe was verified to be capable of failing —
injecting a single `go func() { time.Sleep(2*time.Hour) }()` into the `mirror` package
produced `ZZLEAK: 1 leaked goroutine(s)`
and a red build.

So this is not an open bug.
It is an unguarded invariant:
the shutdown paths are correct today
and nothing would notice if a future change broke them.

**Closes when** the probe becomes permanent.
Two options, and they are not equally gated.
Adding `go.uber.org/goleak` as a test-only dependency needs the maintainer's approval first
(rule 100-overview §3 requires asking before adding deps).
Keeping the dependency-free `runtime.Stack` diff
as a shared `TEST_DIRS`-resident helper needs no approval at all:
rule 300-testing already names a goroutine's existence,
read out of `runtime.Stack`,
as a case where `require.Eventually` is the correct tool.
The dependency-free version got the full result here,
so the dependency buys convenience, not capability.

### S3 — `MemoryReplayer.Dequeue` blocking path is untested

`replay/memory.go:306 Dequeue` sits at **39.1%**.
Uncovered: the uninitialized guard, the pre-check `ctx.Done()`,
the closed-and-drained exit,
and **all four arms of the blocking select** —
the path a caller takes when the queue is empty and it waits for work.

The worker does not use it; it goes through
`dequeueWithPriority` / `dequeueStrict` instead.
That is exactly why it rots:
`Dequeue` is exported API a user calling `MemoryReplayer` directly will reach,
and the library itself never walks it.

Neighbouring numbers in the same file tell the same story:
`tryDequeueRetained` 60.0%, `Enqueue` 75.0%, `IsClosed` 0.0%,
`normalizeMemoryReplayerForLegacy` 50.0%,
`memory_worker.go:270 requeueGated` 40.0%.

**Closes when** each of the four blocking arms is driven
(payload arriving on high and low, on each cluster),
plus cancel-while-blocked and close-while-blocked.

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

### S7 — `UnwrapSession(nil)` panics in v1, returns nil in v2

Found while writing the S1 tests, not by coverage.

`adapter/cql/v2/helpers.go` guards the nil receiver
and documents the parameter as "may be nil".
`adapter/cql/v1/helpers.go:121` does not:

```go
func UnwrapSession(s *Session) *gocql.Session {
	return s.session
}
```

`v1.UnwrapSession(nil)` is a nil-pointer dereference.
The v2 package has `TestUnwrapSessionNil` asserting the guarded behaviour;
v1 has no equivalent, which is why the asymmetry survived.

Recorded, not fixed —
this is a production change and the call is the maintainer's.
Adding the guard makes the two adapters agree
and costs one branch;
leaving it means the v1 doc comment should stop implying the v2 contract.
No test was added for either behaviour,
because pinning the panic would cement it.

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

S1 and S6 are closed.
Remaining, in the order they are worth doing:

**S2** — the only finding that would catch a regression nobody is currently able to see.
Needs a decision on `goleak` versus the dependency-free diff first.

**S3**, then **S4** — mechanical, no decisions needed.

**S7** — a one-line production change or a doc correction;
the choice is the maintainer's.

**S5** — a design question to answer before it is a test task.

Per the standing rule, each new guard must be shown to fail against an injected defect before it counts as closing anything.
Green and inert look identical.
