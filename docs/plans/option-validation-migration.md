# Plan: Compatibility-Aware Migration to Error-Returning Options

## Goal

Move Helix from silent or fallback-based `WithXxx` option handling toward explicit, caller-visible configuration errors, without breaking existing v1 users abruptly.

The target state is that invalid configuration is detected at construction time and returned as an error from constructors that already return errors, while pointer-only constructors gain checked alternatives before any breaking API change is considered.

## Context

Helix uses the functional-options pattern across public packages:

- Root client: `type Option func(*ClientConfig)` used by `NewCQLClient`, which already returns `(*CQLClient, error)`.
- Policy package: `StickyReadOption`, `AdaptiveDualWriteOption`, `CircuitBreakerOption`, and `LatencyCircuitBreakerOption`, whose constructors currently return pointers only.
- Replay package: `MemoryReplayerOption`, `NATSReplayerOption`, and `WorkerOption`.
- Mirror package: `mirror.Option` for the async mirror engine.
- Topology and metrics packages: watcher and collector options.

The existing option functions cannot return errors directly, so invalid values tend to be handled in one of three ways:

1. Silently accepted, risking broken runtime behavior.
2. Ignored or clamped to defaults, preserving availability but hiding misconfiguration.
3. Sanitized later by a constructor that can return/log errors, as with auto-refresh tuning.

For a high-availability library, silent misconfiguration is dangerous: an option intended to improve resilience can accidentally disable damping, make failover immediate, remove observability, or create invalid timeouts.

## Design Principles

- Preserve source compatibility for existing v1 users where possible.
- Make new strict paths explicit rather than surprising existing callers.
- Prefer construction-time failure for safety-critical invalid values.
- Keep default constructors usable for quick starts and tests.
- Use structured errors so callers can inspect and report configuration mistakes.
- Avoid duplicating validation logic between legacy and checked constructors.

## Non-Goals

- Do not change every public option type in one v1 release.
- Do not remove existing `WithXxx` functions during the compatibility phase.
- Do not make runtime strategy methods return option validation errors.
- Do not use panics for invalid user configuration.
- Do not introduce validation that requires external systems, network calls, or database sessions.

## Proposed API Shape

### Structured Option Error

Add a shared structured error type in `types`:

```go
// OptionError reports an invalid functional option value.
type OptionError struct {
    Component string // e.g. "policy.AdaptiveDualWrite"
    Option    string // e.g. "WithAdaptiveStrikeThreshold"
    Reason    string // e.g. "must be positive"
}

func (e *OptionError) Error() string
```

Add helpers:

```go
func AsOptionError(err error) (*OptionError, bool)
func IsOptionError(err error) bool
```

When multiple options are invalid, constructors should return `errors.Join(...)` so `errors.As` still works and callers see every configuration issue in one pass.

### Internal Validator Type

Keep legacy option function signatures initially, but add internal validation wrappers per package:

```go
type optionValidator[T any] struct {
    apply    func(*T)
    validate func() error
}
```

The exact implementation can be simpler per package; the important point is that validation metadata lives next to the option and is reused by both legacy and checked constructors.

### Checked Constructors

For constructors that currently return only pointers, add checked variants:

```go
func NewAdaptiveDualWriteChecked(opts ...AdaptiveDualWriteOption) (*AdaptiveDualWrite, error)
func NewCircuitBreakerChecked(opts ...CircuitBreakerOption) (*CircuitBreaker, error)
func NewLatencyCircuitBreakerChecked(opts ...LatencyCircuitBreakerOption) (*LatencyCircuitBreaker, error)
func NewStickyReadChecked(opts ...StickyReadOption) (*StickyRead, error)
func NewMemoryReplayerChecked(opts ...MemoryReplayerOption) (*MemoryReplayer, error)
func NewWorkerChecked(replayer Replayer, execute ExecuteFunc, opts ...WorkerOption) (*Worker, error)
```

Legacy constructors remain:

```go
func NewAdaptiveDualWrite(opts ...AdaptiveDualWriteOption) *AdaptiveDualWrite
```

Legacy constructors should continue preserving defaults for invalid values during v1, but their documentation must say they are compatibility constructors and recommend checked constructors for production configuration.

### Root Client Constructor

`NewCQLClient` already returns an error, so it can adopt strict validation earlier:

```go
client, err := helix.NewCQLClient(sessionA, sessionB,
    helix.WithClusterNames("us_east", "us-west"),
)
// err contains *types.OptionError for invalid cluster names.
```

Root options should validate at construction time for:

- nil required dependencies when an option explicitly opts into a mode, e.g. `WithMirror(nil)`, `WithMirrorPublisher(nil)`.
- invalid cluster names.
- invalid auto-refresh values.
- nil metrics/logger only if the option is explicitly declared strict; otherwise nil can mean "use default no-op" for compatibility.
- invalid combinations, e.g. mutually exclusive mirror modes.

## Validation Taxonomy

| Category | Examples | Legacy Constructor | Checked Constructor / `NewCQLClient` |
|---|---|---|---|
| Positive integer required | thresholds, worker counts, batch sizes | preserve default | return `OptionError` |
| Non-negative duration allowed | cooldowns where zero disables delay | preserve default on negative | return `OptionError` on negative |
| Positive duration required | timeouts, polling intervals | preserve default | return `OptionError` on `<= 0` |
| Optional callback/interface | logger, metrics, hooks | nil means no-op/default | nil accepted unless required by mode |
| Required dependency after opting in | mirror target, publisher, replayer for worker | constructor-specific | return `OptionError` or existing sentinel |
| Enum/domain value | cluster IDs, discard policy, batch type | ignore invalid or default | return `OptionError` |
| Cross-field relation | timeout < interval, min <= max | sanitize/log where legacy | return `OptionError` |

## Rollout Plan

### Phase 1 — Inventory and Normalize Validation

- Audit all `type XOption func(...)` definitions in root, `policy`, `replay`, `mirror`, `topology`, and `contrib/metrics/vm`.
- For every option, classify validation using the taxonomy above.
- Add missing tests proving legacy constructors do not panic and do not silently create unsafe state.
- Document the legacy behavior for invalid values: preserve default, no-op, or explicit constructor error.

Success criteria:

- Every public option has a documented invalid-value behavior.
- Tests cover invalid values for high-impact HA knobs.
- `make lint` passes.

### Phase 2 — Add Structured Option Errors

- Add `types.OptionError` and helpers.
- Add package-local helper constructors for consistent messages:

```go
func newOptionError(component, option, reason string) error
```

- Use `errors.Join` when applying multiple invalid options.
- Add tests for `errors.As` and joined option errors.

Success criteria:

- Callers can inspect invalid configuration with `errors.As`.
- Multiple invalid options are returned together.

### Phase 3 — Checked Constructors for Pointer-Only Types

- Add checked constructors for policy and replay types first, because these control failover, write behavior, and replay safety.
- Keep existing constructors as compatibility wrappers that apply the same defaults and ignore invalid options as documented.
- Update production examples to prefer checked constructors where they fit naturally.

Example:

```go
strategy, err := policy.NewAdaptiveDualWriteChecked(
    policy.WithAdaptiveStrikeThreshold(0),
)
if err != nil {
    return err
}
```

Success criteria:

- Existing code keeps compiling.
- New code can fail fast on invalid policy/replay configuration.
- Checked constructors share validation with legacy constructors.

### Phase 4 — Root Constructor Strict Validation

- Make `NewCQLClient` collect validation errors from root options.
- Preserve existing sentinel errors where already documented, such as `ErrNilMirrorTarget` and mirror mode conflict errors.
- Wrap or join sentinel errors with `OptionError` only if doing so does not break `errors.Is`.

Success criteria:

- `NewCQLClient` returns explicit errors for invalid root options.
- `errors.Is` still works for existing sentinel errors.
- Existing tests for mirror/replay/topology setup still pass.

### Phase 5 — Deprecation Notices and Documentation

- Add Godoc notes to legacy constructors:

```go
// For production configuration that should fail fast on invalid options,
// use NewAdaptiveDualWriteChecked.
```

- Update README and strategy/replay docs with checked-constructor examples.
- Add a migration guide section describing how to move from fallback behavior to strict construction.

Success criteria:

- Users can discover the checked path from package Godoc.
- Docs explain when legacy fallback behavior still exists.

### Phase 6 — Future Major Version

In a future v2, consider changing option signatures directly:

```go
type AdaptiveDualWriteOption func(*AdaptiveDualWrite) error
type Option func(*ClientConfig) error
```

At that point, pointer-only constructors should either return errors or be removed in favor of checked constructors.

This should only happen in a major release because it breaks custom option implementations and callers that depend on pointer-only constructors.

## Package Priorities

### First Priority: `policy`

Policy options directly affect availability, failover damping, latency behavior, and background writes.

Validate strictly in checked constructors:

- `WithAdaptiveDeltaThreshold`: `> 0`
- `WithAdaptiveAbsoluteMax`: `> 0`
- `WithAdaptiveMinFloor`: `>= 0`
- `WithAdaptiveStrikeThreshold`: `1 <= n <= math.MaxInt32`
- `WithAdaptiveRecoveryThreshold`: `1 <= n <= math.MaxInt32`
- `WithAdaptiveFireForgetTimeout`: `> 0`
- `WithAdaptiveFireForgetLimit`: `1 <= n <= math.MaxInt32`
- `WithThreshold`: `1 <= n <= math.MaxInt32`
- `WithResetTimeout`: `>= 0` because zero intentionally disables timed half-open transitions
- `WithLatencyAbsoluteMax`: `> 0`
- `WithLatencyThreshold`: `1 <= n <= math.MaxInt32`
- `WithLatencyResetTimeout`: `>= 0`
- `WithPreferredCluster`: cluster must be `ClusterA` or `ClusterB`
- `WithStickyReadCooldown`: `>= 0`
- `WithPrimaryOnlyRecoveryTimeout`: any value accepted; `<= 0` intentionally disables auto-recovery

### Second Priority: `replay`

Replay controls data repair and memory/backpressure behavior.

Validate strictly in checked constructors:

- queue capacity must be positive unless documented as defaulting
- high-priority ratios must be non-negative
- batch sizes and max attempts must be positive
- retry delays, poll intervals, and execute timeouts must be positive
- NATS stream names and subject prefixes must be non-empty and syntactically safe
- NATS consumer limits must be positive where the server requires positive values

### Third Priority: root `helix`

Root options should fail fast when they create invalid client modes:

- invalid cluster names
- nil mirror target when mirror mode is enabled
- nil mirror publisher when publisher mode is enabled
- both mirror modes configured simultaneously
- invalid auto-refresh values
- invalid recovery probe relation, e.g. timeout greater than interval if that becomes a documented requirement

### Fourth Priority: `mirror`, `topology`, and metrics

These are still important, but their invalid values usually affect observability or side-channel behavior rather than the core dual-write/read path.

## Error Semantics

Use these rules consistently:

- Return `*types.OptionError` for a single invalid option value.
- Return `errors.Join` for multiple invalid option values.
- Preserve `errors.Is` for existing sentinel errors.
- Do not log and return the same validation error unless the constructor cannot return an error for compatibility reasons.
- Do not treat nil optional callbacks as errors.

Example joined error behavior:

```go
strategy, err := policy.NewAdaptiveDualWriteChecked(
    policy.WithAdaptiveStrikeThreshold(0),
    policy.WithAdaptiveFireForgetTimeout(0),
)

var optionErr *types.OptionError
if errors.As(err, &optionErr) {
    // true
}
```

## Compatibility Strategy

Existing constructors keep their current signatures through v1:

```go
strategy := policy.NewAdaptiveDualWrite(policy.WithAdaptiveStrikeThreshold(0))
```

The legacy constructor should preserve defaults for invalid values rather than returning impossible errors. This avoids breaking users on upgrade.

New production code should prefer checked constructors:

```go
strategy, err := policy.NewAdaptiveDualWriteChecked(policy.WithAdaptiveStrikeThreshold(3))
if err != nil {
    return fmt.Errorf("configure adaptive write: %w", err)
}
```

For root configuration, because `NewCQLClient` already returns `error`, strict validation can be introduced earlier, but release notes must call out any newly rejected invalid options.

## Testing Plan

- Unit tests for every checked constructor with invalid values.
- Unit tests for legacy constructors preserving documented fallback/no-op behavior.
- `errors.As` tests for `*types.OptionError` and joined errors.
- Regression tests that nil metrics/loggers do not panic in background goroutines.
- Root `NewCQLClient` tests for invalid root options and option combinations.
- Race tests remain focused on runtime state machines, not option validation.

Required commands before merging implementation:

```bash
go test ./policy ./replay ./mirror ./topology ./contrib/metrics/vm
go test ./...
make lint
```

## Documentation Updates

- README production examples should show checked constructors only where they do not make the quick start noisy.
- Package Godoc should state whether a constructor is legacy/fallback or checked/strict.
- Strategy and replay docs should include a short "Configuration validation" section.
- Release notes should include a migration table from legacy constructors to checked constructors.

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| API surface grows with duplicate constructors | Keep naming consistent and document checked constructors as the production path |
| Validation logic diverges between legacy and checked paths | Put validation metadata next to each option and share helpers |
| New strict root validation breaks existing users | Start with clearly invalid values and document in release notes |
| Joined errors are unfamiliar to some callers | Provide examples using `errors.As` and include readable error strings |
| v2 option signature change is too disruptive | Treat v2 as optional future work, not a requirement for v1 migration |

## Open Questions

- Should checked constructors be named `NewXChecked`, `NewXStrict`, or `NewXWithValidation`?
- Should invalid nil metrics/logger be accepted everywhere as "use no-op", or should checked constructors reject nil when explicitly passed?
- Should `NewCQLClient` validate nested policy/replay options, or only validate root options and rely on checked nested constructors?
- Should `types.OptionError` include the invalid value, or avoid that to prevent leaking sensitive strings into logs?
- Should legacy constructors log warnings on invalid values, or remain silent to avoid surprising tests and noisy applications?

## Recommended First Implementation Slice

1. Add `types.OptionError` and tests.
2. Add checked constructors for `policy.AdaptiveDualWrite`, `policy.CircuitBreaker`, and `policy.LatencyCircuitBreaker`.
3. Reuse the validation rules already introduced for legacy policy options.
4. Update `docs/strategy-policy.md` with checked-constructor examples.
5. Keep root `NewCQLClient` strict-validation changes for a second slice, after the policy API shape is proven.
