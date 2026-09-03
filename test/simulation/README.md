# Helix Simulation Test Suite

End-to-end behavioral test harness for the Helix dual-cluster client. It spins up two independent Cassandra/ScyllaDB containers via Testcontainers, drives live read/write traffic, injects controlled failures, and verifies that every write was eventually replicated to both clusters.

## Directory structure

```
test/simulation/
├── cmd/main.go          # Entry point — flags, cluster startup, scenario registration
├── simulation.go        # Orchestrator: environment setup, workload, scenario dispatch
├── chaos/               # cql.Session wrapper that injects latency, drops, and errors
├── config/              # YAML config loader with defaults
├── scenarios/           # One file per scenario; shared wait.go utility
├── types/               # Environment struct and Scenario interface
├── workload/            # WriteTracker (consistency oracle) and WorkloadStats
└── configs/
    ├── quick.yaml       # 5-minute sanity run (1 worker)
    └── soak.yaml        # 2-hour stability run (4 workers)
```

## Running

Requires Docker. All profiles use the same entry point.

```bash
# Quick sanity check (~5 min, basic scenarios only)
go run ./test/simulation/cmd/main.go -profile quick -config test/simulation/configs/quick.yaml

# Full behavioral coverage (~10-15 min, all scenarios + strategy groups)
go run ./test/simulation/cmd/main.go -profile comprehensive -config test/simulation/configs/quick.yaml

# Long-running stability run (2 h by default)
go run ./test/simulation/cmd/main.go -profile soak -config test/simulation/configs/soak.yaml

# Targeted FallbackRead verification (~5 min)
go run ./test/simulation/cmd/main.go -profile fallback -config test/simulation/configs/quick.yaml
```

`make test-simulation` runs the quick profile with `configs/quick.yaml`;
override the profile or config with `SIM_PROFILE=comprehensive` or `SIM_CONFIG=...`.
The nightly GitHub Actions workflow (`.github/workflows/nightly.yml`) runs this target
together with `make test-e2e`.

Override individual flags without a config file:

```bash
go run ./test/simulation/cmd/main.go -profile quick -duration 2m -seed 123
```

A pprof server starts automatically on `127.0.0.1:6060` for profiling during soak runs.

## Profiles

| Profile | Scenarios | Duration (default) |
|---|---|---|
| `quick` | `degraded-cluster`, `adaptive-recovery`, `complete-failure` | 5 min |
| `comprehensive` | All quick + 5 more + 6 strategy groups | config-driven |
| `soak` | All comprehensive + `dual-cluster-degradation` | 2 h |
| `fallback` | 3 baseline scenarios + `fallback-read` strategy group | 5 min |

`fallback` is a targeted profile for verifying `FallbackRead` divergence detection in isolation. It runs the 3 baseline scenarios to warm up both clusters, then exercises `FallbackReadDivergence` via a dedicated strategy group configured with `WithDefaultFallbackRead(true)` and `StickyRead` pinned to Cluster B.

## Configuration reference

```yaml
simulation:
  duration: 5m          # Total run time
  seed: 42              # RNG seed (for reproducibility)
  report_dir: ./reports # Future: where JSON reports land
  console_interval: 10s # Progress log interval

workload:
  workers: 1            # Parallel traffic goroutines
  interval: 10ms        # Per-worker write tick
  payload_size: 100     # Bytes per INSERT
  read_ratio: 0.2       # Fraction of ops that are reads  (0.0–1.0)
  batch_ratio: 0.1      # Fraction of write ops that use LOGGED BATCH

helix:
  write_strategy:
    type: adaptive              # adaptive | concurrent | single
    delta_threshold: 100ms      # Latency gap before a "strike" is counted
    strike_threshold: 3         # Strikes before fire-and-forget mode
    recovery_threshold: 5       # Consecutive fast writes needed to recover
    fire_forget_timeout: 30s    # Background goroutine timeout
    fire_forget_limit: 100      # Max concurrent fire-and-forget goroutines

  read_strategy:
    type: sticky        # sticky | primary_only | round_robin
    cooldown: 1m        # Time to stay on failover cluster before probing primary
    preferred: random   # A | B | random (initial preferred cluster)

  failover_policy:
    type: active        # active | circuit | latency_circuit
    threshold: 3        # Consecutive failures to open circuit
    reset_timeout: 30s  # How long circuit stays open
    absolute_max: 2s    # (latency_circuit only) max acceptable latency

  replay:
    type: memory        # memory | nats
    queue_size: 1000    # Max queued payloads
```

## Writing a new scenario

1. Create `scenarios/my_scenario.go` implementing `types.Scenario`:

```go
package scenarios

import (
    "context"
    "fmt"
    "time"

    "github.com/arloliu/helix/test/simulation/types"
)

type MyScenario struct{}

func (s *MyScenario) Name() string        { return "my-scenario" }
func (s *MyScenario) Description() string { return "What this scenario verifies" }

func (s *MyScenario) Run(ctx context.Context, env *types.Environment) error {
    // Inject chaos
    env.ChaosA.SetErrorRate(1.0)

    // Wait for an observable effect
    err := waitUntil(ctx, 10*time.Second, func() bool {
        _, _, drops := env.ChaosA.Counters()
        return drops > 50
    })
    if err != nil {
        return fmt.Errorf("cluster A drops did not accumulate: %w", err)
    }

    // Recover
    env.ChaosA.SetErrorRate(0)

    // Assert strategy state via the canonical policy API, not timing heuristics
    // e.g. adw.IsDegraded(htypes.ClusterA)

    return nil
}
```

2. Register in `cmd/main.go`:

```go
sim.RegisterScenario(&scenarios.MyScenario{})
```

Add it to a specific profile gate if it should not run in `quick`:

```go
if profile == "comprehensive" || profile == "soak" {
    sim.RegisterScenario(&scenarios.MyScenario{})
}
```

**Assertion guidelines**

- Prefer policy API methods (`IsDegraded`, `ShouldFailover`, `Select`) over counter deltas — they are the source of truth and don't have timing windows.
- Use `waitUntil(ctx, timeout, condition)` for all polling; never `time.Sleep`.
- Propagate `waitUntil` errors on gates that precondition an assertion — if the gate times out, return the error rather than discarding it with `_ =`. A silent timeout silently skips the assertion that follows.
- Use `env.ChaosA.Counters()` / `env.ChaosB.Counters()` for exec/scan/drop observability.
- When spawning extra goroutines (e.g. flood workers), cancel them with a `context.CancelFunc` and then call `wg.Wait()` before returning. Relying on context cancellation alone can leave goroutines running after the scenario returns.
- Chaos state is reset between scenarios by the orchestrator. Multi-phase scenarios that need to clear chaos mid-scenario to avoid interfering with later phases are an exception — clear it explicitly before returning on any error path.

## Strategy groups

A strategy group runs a set of scenarios against a client configured with a specific combination of write strategy, read strategy, and failover policy. Groups share the same underlying Cassandra containers but get a fresh `CQLClient` and truncated table.

| Group | Failover policy | Scenario | Profile |
|---|---|---|---|
| `circuit-breaker` | `CircuitBreaker` (3-strike, 15 s reset) | `circuit-breaker-trip` | comprehensive+ |
| `latency-cb` | `LatencyCircuitBreaker` (500 ms max, 3-strike) | `latency-circuit-breaker-trip` | comprehensive+ |
| `primary-only` | `ActiveFailover` | `primary-only-read-recovery` | comprehensive+ |
| `round-robin` | `ActiveFailover` | `round-robin-read-balance` | comprehensive+ |
| `sticky-cooldown` | `ActiveFailover` | `sticky-cooldown` | comprehensive+ |
| `fallback-read` | `ActiveFailover` | `fallback-read-divergence` | comprehensive+, fallback |

Register a group in `cmd/main.go`:

```go
sim.RegisterStrategyGroup(simulation.StrategyGroup{
    Name: "my-group",
    SetupFunc: makeStrategyGroupClient(
        policy.NewAdaptiveDualWrite(...),
        policy.NewRoundRobinRead(),
        policy.NewLatencyCircuitBreaker(...),
    ),
    Scenarios: []simtypes.Scenario{
        &scenarios.MyGroupScenario{},
    },
})
```

The orchestrator:
1. Drains the previous group's replay queue
2. Truncates `test_data` on both clusters
3. Creates a new client via `SetupFunc`
4. Starts a fresh workload goroutine and waits for 50 tracked writes (warm-up)
5. Runs each scenario in order, resetting chaos state between them
6. Verifies consistency across all keys written during the group

## Chaos injection API

`chaos.Session` wraps any `cql.Session` to inject faults:

```go
// Fixed latency on all operations
env.ChaosA.SetLatency(200 * time.Millisecond)

// Probability-based drop (returns ErrWriteDropped)
env.ChaosA.SetErrorRate(0.3)   // 30% random drops
env.ChaosA.SetErrorRate(1.0)   // complete failure

// Custom error or latency function
env.ChaosA.SetConfig(chaos.SessionConfig{
    LatencyFunc: func() time.Duration { return time.Duration(rand.Intn(500)) * time.Millisecond },
    ErrorFunc:   func() error { return io.ErrUnexpectedEOF },
})

// Read operation counters (reset between scenarios by orchestrator)
exec, scan, drop := env.ChaosA.Counters()

// Clear all chaos
env.ChaosA.SetConfig(chaos.SessionConfig{})
```

`Close()` on a chaos session is a no-op — the underlying gocql session lifetime is managed by the cluster container, not by Helix clients.

## Unit tests

The simulation infrastructure components have focused unit tests that do not require Docker:

| Test file | What it covers |
|---|---|
| `chaos/session_test.go` | Drop-rate sampling, latency injection, counter management, error propagation, no-op Close |
| `config/config_test.go` | YAML parsing, default value injection, error handling |
| `workload/tracker_test.go` | TrackWrite/Count/RandomKey, WorkloadStats.Reset, VerifyConsistency with mock sessions |
| `scenarios/wait_test.go` | waitUntil: immediate-true, timeout, context cancellation |

Run them with:

```bash
go test ./test/simulation/...
```
