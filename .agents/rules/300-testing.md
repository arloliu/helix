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
- ✅ **ALWAYS** use event-driven collectors that:
    1. Subscribe BEFORE triggering action.
    2. Collect all state transitions.
    3. Assert on complete history.

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
