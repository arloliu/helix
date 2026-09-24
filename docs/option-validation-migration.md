# Migrating to Checked Constructors

Helix's functional-option constructors come in two forms.
The legacy form (`NewXxx`) returns a bare pointer and silently falls back to a
default whenever an option value is invalid — a negative threshold, an unknown
cluster, a zero timeout.
The checked form (`NewXxxChecked`) returns `(*Xxx, error)` and instead rejects
the invalid value, joined with any others, as a `*types.OptionError`.

Both forms stay supported.
The legacy form is not deprecated and will not be
removed in v1 — it exists for quick starts, tests, and code that has already
audited its option values.
The checked form is the recommended path for production configuration,
where a typo or a bad default should fail construction instead of
silently degrading resilience, failover, or replay behavior.

> This guide covers the two-form pattern itself.
> For what each individual
> option validates, see the package guide it belongs to —
> [Strategy & Policy Guide](strategy-policy.md), [Replay System](replay-system.md) —
> or the option's own Godoc.

---

## Which constructors have a checked form

| Package | Legacy | Checked |
|---|---|---|
| `policy` | `NewAdaptiveDualWrite` | `NewAdaptiveDualWriteChecked` |
| `policy` | `NewCircuitBreaker` | `NewCircuitBreakerChecked` |
| `policy` | `NewLatencyCircuitBreaker` | `NewLatencyCircuitBreakerChecked` |
| `policy` | `NewStickyRead` | `NewStickyReadChecked` |
| `policy` | `NewPrimaryOnlyRead` | `NewPrimaryOnlyReadChecked` |
| `replay` | `NewMemoryReplayer` | `NewMemoryReplayerChecked` |
| `replay` | `NewMemoryWorker` | `NewMemoryWorkerChecked` |
| `replay` | `NewNATSWorker` | `NewNATSWorkerChecked` |

Every legacy constructor above documents its checked sibling directly in its
own Godoc (`For production configuration ... use [NewXxxChecked]`).

Three `policy` constructors take no options with values to validate, so they
have no checked form: `NewRoundRobinRead`, `NewActiveFailover`, and
`NewConcurrentDualWrite`.
`NewSyncDualWrite`'s two options each take no arguments, for the same reason.

Two constructors already return `(*T, error)` and need no checked sibling:

- `helix.NewCQLClient` validates every root option — cluster names,
  auto-refresh, cluster timeouts, mirror mode, recovery probe, replay wiring —
  before starting any background component.
  See the [Configuration Reference](configuration.md).
- `replay.NewNATSReplayer` validates its stream and subject configuration
  against the JetStream API it calls during construction.

---

## Migrating one constructor

Before:

```go
strategy := policy.NewAdaptiveDualWrite(
    policy.WithAdaptiveStrikeThreshold(0), // invalid: silently kept at the default
)
```

After:

```go
strategy, err := policy.NewAdaptiveDualWriteChecked(
    policy.WithAdaptiveStrikeThreshold(0), // invalid: now rejected
)
if err != nil {
    return fmt.Errorf("configure adaptive write: %w", err)
}
```

No other call site changes: the checked constructor takes the same option
type (`AdaptiveDualWriteOption`) and, for valid input, builds the identical
`*AdaptiveDualWrite` the legacy constructor would have.

---

## Reading a validation error

An invalid option returns `*types.OptionError`; more than one invalid option
returns them joined with `errors.Join`, so `errors.As` still finds the first
one and `err.Error()` lists every one:

```go
_, err := policy.NewAdaptiveDualWriteChecked(
    policy.WithAdaptiveStrikeThreshold(0),
    policy.WithAdaptiveFireForgetTimeout(0),
)

var optionErr *types.OptionError
if errors.As(err, &optionErr) {
    log.Printf("invalid option: %s.%s: %s",
        optionErr.Component, optionErr.Option, optionErr.Reason)
}
// err.Error() reports both WithAdaptiveStrikeThreshold and
// WithAdaptiveFireForgetTimeout in one message.
```

`types.IsOptionError(err)` reports whether an error contains at least one
`*types.OptionError`, without needing the pointer itself.

---

## What does not change

- **Legacy constructors keep their exact current behavior.**
  An invalid value
  is still silently clamped or defaulted, never a panic and never an error —
  switching a legacy call to a checked one is the only way to start catching
  those values, not an automatic side effect of upgrading Helix.
- **The two forms build identical values for valid input.**
  A checked
  constructor is not a stricter variant of the type; it is the same
  construction logic with a validation gate in front of it.
- **Nil optional callbacks — loggers, metrics collectors, hooks — are never a
  validation error** in either form; a nil callback means "use the default."
