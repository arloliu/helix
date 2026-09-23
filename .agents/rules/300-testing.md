# 300 - Testing Guidelines

## Organization
- **Unit:** Co-located in `*_test.go`. Same package or `_test` suffix.
- **Integration:** `internal/test/integration/` directory.
  Package `integration_test`.
  Requires running Cassandra/ScyllaDB via testcontainers.
- **Simulation:** `internal/test/simulation/` directory.
  Long-running dual-cluster behavior scenarios.
- **Two-node e2e:** `internal/test/e2e/cql/` behind the build tags `e2e multinode`.
  One ScyllaDB cluster of two nodes at replication factor 2,
  which is the only tier where a node-level fault is not also a cluster-level fault.
  Nightly only, via `make test-e2e-multinode`; `make test-e2e` does not build it.
- **Test Utilities:** `internal/test/testutil/` — shared helpers for integration and simulation tests.

## Rules
- **No Emojis:** Do not use emojis in test log messages.
- **Context:** Use `t.Context()`.
- **Env:** Use `t.Setenv()` (not `os.Setenv`).
- **Benchmarks:** Use `for b.Loop()` (Go 1.24+).
- **Assertions:** Use `testify` (`require`, `assert`).
- **Containers:** Use `testcontainers-go` for Cassandra/ScyllaDB in integration tests.
- **Cleanup:** Always use `t.Cleanup()` or `defer` for resource cleanup.

## Async Testing (CRITICAL)
- ❌ **NEVER** use `time.Sleep()` to wait for state.
- ❌ **NEVER** use `assert.Eventually`, or its `f`/`WithT`/`WithTf` spellings, anywhere.
  A soft-failing wait lets the rest of the test run against state it has just failed to establish.
  `forbidigo` enforces all four; there is no case for them, inside the exception below or out.
- ✅ Use event-driven collectors (the one exception is below) that:
    1. Subscribe BEFORE triggering action.
    2. Collect all state transitions.
    3. Assert on complete history.

**Reference implementation:** `gatedStateCollector` in `policy/failover_policy_test.go`.
It closes a channel from inside the metrics call it wraps,
so the test learns of a transition at the earliest point that transition exists
instead of sampling for it afterwards.

### The one exception: no subscribe point exists

Some state has nothing to subscribe to at all.
Not "no hook is wired yet" — nothing anywhere fires when it changes.
Three shapes recur:

- **State held outside the process.** A row landing in Cassandra/ScyllaDB, a container's state.
- **A deadline whose only observer is the condition.** A recovery timeout or cooldown
  where `Select()` or `TryBeginFailoverProbe()` is the thing that notices the clock has passed;
  nothing fires when it does.
- **A goroutine's existence**, read out of `runtime.Stack`.

Poll with `require.Eventually` there.

The exception is the absence of a subscribe point, not the convenience of polling.
Where a hook exists — a callback, a metrics collector, a logger, an event handler the code already calls —
`require.Eventually` on a counter breaks all three rules above:
it samples a final state, so `counter == 1` passes the instant the counter reaches 1
and never observes a second increment the assertion claims cannot happen.
An exactness claim therefore does not belong in the wait at all.
Wait for `>=`, then assert the exact value at a point where the counter can no
longer move: `Worker.Stop()` joins the worker goroutines, `CQLClient.Close()`
waits for the background legs, a settled payload leaves an empty queue with
nothing to dispatch twice. Quiescence costs no wall clock.
Read any backlog *before* stopping a worker — `Stop` drains the queue and drops
what is left, which would take the parked payloads with it.
Where the file already has a completion seam — an `OnSuccess` channel, an
`OnComplete` callback — subscribe to that instead: it is cheaper than a
lifecycle join and it is the event-driven shape this rule asks for.
Only where no such point exists, pair the wait with `require.Never` on the
over-count (`replay/eviction_nats_test.go` does this), sized against the retry
or backoff a spurious increment would arrive on. It costs its whole window on
the passing path, so it is the last resort, not the first.

Do not add a subscribe point to production code to satisfy this rule.
A channel that exists only for a test is a seam the code under test can leave before the send,
which hangs the test to the package timeout rather than failing it;
if one is unavoidable, guard every send with a `select` on a done channel.

### What the `time.Sleep` ban does and does not cover

It bans sleeping *to wait for state*. Two shapes are not that:

- **Fixture latency.** `time.Sleep` inside a fake write function so the code under test
  has a real duration to measure is producing the input, not waiting for an outcome.
  Prefer a clock seam where one exists: `AdaptiveDualWrite` takes `latencyNow`, and
  `policy/adaptive_latency_clock_test.go` steps it instead of sleeping. The one surviving
  sleep of this shape, in `TestAdaptiveDualWrite_RelativeDeltaDegradation`, is kept on
  purpose, as the only test whose assertion depends on the seam's real-clock fallback
  measuring real elapsed time.
- **Asserting nothing happened.** There is no event to wait for when the claim is an absence,
  so a bounded wait followed by the assertion is the only shape available
  (`replay_gate_test.go` waits, then requires the replay count is still zero).
  Keep the wait as short as the claim allows.

## Test Patterns
**Table-Driven** — Use ONLY for multiple cases:
```go
tests := []struct { name string; input X; want Y }{ ... }
for _, tt := range tests { t.Run(tt.name, func(t *testing.T) { ... }) }
```

**Simple** — For single cases:
```go
func TestOneThing(t *testing.T) {
    got := Do()
    require.Equal(t, want, got)
}
```

## Running Tests
```bash
make test              # Unit + integration tests with race detector
make test-unit         # Unit tests only with race detector
make test-quick        # Unit tests without race detector (fast)
make test-integration  # Integration tests (requires Docker for testcontainers)
make test-all          # Unit + integration
make coverage          # Generate coverage report
```
