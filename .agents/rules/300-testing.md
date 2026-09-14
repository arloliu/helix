# 300 - Testing Guidelines

## Organization
- **Unit:** Co-located in `*_test.go`. Same package or `_test` suffix.
- **Integration:** `test/integration/` directory. Package `integration_test`. Requires running Cassandra/ScyllaDB via testcontainers.
- **Simulation:** `test/simulation/` directory. Long-running dual-cluster behavior scenarios.
- **Test Utilities:** `test/testutil/` — shared helpers for integration and simulation tests.

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
- ❌ **NEVER** use `assert.Eventually` anywhere.
  A soft-failing wait lets the rest of the test run against state it has just failed to establish.
  `forbidigo` enforces this; there is no case for it, inside the exception below or out.
- ✅ **ALWAYS** use event-driven collectors that:
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
If an exactness claim must stay on a polled counter,
pair the wait with `require.Never` on the over-count (`replay/eviction_nats_test.go` does this).

Do not add a subscribe point to production code to satisfy this rule.
A channel that exists only for a test is a seam the code under test can leave before the send,
which hangs the test to the package timeout rather than failing it;
if one is unavoidable, guard every send with a `select` on a done channel.

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
