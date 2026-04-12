# Helix Simulation Guide

The Helix simulation suite is an end-to-end behavioral test harness that spins up two independent Cassandra/ScyllaDB containers via Testcontainers, drives live read/write traffic, injects controlled failures, and verifies that every write was eventually replicated to both clusters.

## Prerequisites

- Go 1.25+
- Docker (for Testcontainers)

## Running

All profiles share the same entry point in `test/simulation/cmd/main.go`.

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

Override individual settings without a config file:

```bash
go run ./test/simulation/cmd/main.go -profile quick -duration 2m -seed 123
```

A pprof server starts automatically on `127.0.0.1:6060` during all runs.

## Profiles

| Profile | Scenarios | Strategy groups | Default duration |
|---|---|---|---|
| `quick` | `degraded-cluster`, `adaptive-recovery`, `complete-failure` | — | 5 min |
| `comprehensive` | All quick + 5 more | `circuit-breaker`, `latency-cb`, `primary-only`, `round-robin`, `sticky-cooldown`, `fallback-read` | config-driven |
| `soak` | All comprehensive + `dual-cluster-degradation` | Same as comprehensive | 2 h |
| `fallback` | 3 baseline scenarios | `fallback-read` | 5 min |

`fallback` is a targeted profile for verifying `FallbackRead` divergence detection in isolation. It runs the 3 baseline scenarios to warm up both clusters, then exercises `FallbackReadDivergence` via a dedicated strategy group configured with `WithDefaultFallbackRead(true)` and `StickyRead` pinned to Cluster B.

### Scenarios

| Name | Profile | What it verifies |
|---|---|---|
| `degraded-cluster` | quick+ | Adaptive write degrades gracefully under sustained latency on one cluster |
| `adaptive-recovery` | quick+ | Write strategy recovers after a flapping cluster stabilizes |
| `complete-failure` | quick+ | Replay queue absorbs writes during a total cluster outage |
| `replay-saturation` | comprehensive+ | Replay buffer handles prolonged outage without data loss |
| `drain-mode` | comprehensive+ | Replay drains cleanly after cluster returns |
| `circuit-breaker-trip` | comprehensive+ | Circuit breaker opens after consecutive failures and resets after the timeout |
| `fire-forget-limit` | comprehensive+ | Fire-and-forget semaphore is exhausted and writes are dropped with correct metrics |
| `partial-degradation` | comprehensive+ | 30% partial drop rate on one cluster; replay compensates for intermittent failures |
| `fallback-read-divergence` | comprehensive+, fallback | FallbackRead recovers rows present only on Cluster A and records divergence metrics on Cluster B |
| `dual-cluster-degradation` | soak | Simultaneous degradation of both clusters produces `DualClusterError` |

### Strategy groups

A strategy group runs its scenarios against a fresh `CQLClient` with a specific combination of write strategy, read strategy, and failover policy. Groups share the container pair but get an isolated client and truncated table.

| Group | Write strategy | Read strategy | Failover policy | Scenario |
|---|---|---|---|---|
| `circuit-breaker` | `AdaptiveDualWrite` | `PrimaryOnlyRead` (10 s recovery) | `CircuitBreaker` (3-strike, 15 s reset) | `CircuitBreakerTrip` |
| `latency-cb` | `AdaptiveDualWrite` | `StickyRead` (pinned to A) | `LatencyCircuitBreaker` (500 ms max, 3-strike) | `LatencyCircuitBreakerTrip` |
| `primary-only` | `AdaptiveDualWrite` | `PrimaryOnlyRead` (10 s recovery) | `ActiveFailover` | `PrimaryOnlyReadRecovery` |
| `round-robin` | `AdaptiveDualWrite` | `RoundRobinRead` | `ActiveFailover` | `RoundRobinReadBalance` |
| `sticky-cooldown` | `AdaptiveDualWrite` | `StickyRead` (pinned to A, 10 s cooldown) | `ActiveFailover` | `StickyCooldown` |
| `fallback-read` | `AdaptiveDualWrite` | `StickyRead` (pinned to B) | `ActiveFailover` | `FallbackReadDivergence` |

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

Implement `types.Scenario` and register it in `cmd/main.go`.

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

    // Wait for an observable effect — always propagate gate errors
    if err := waitUntil(ctx, 10*time.Second, func() bool {
        _, _, drops := env.ChaosA.Counters()
        return drops > 50
    }); err != nil {
        env.ChaosA.SetErrorRate(0)
        return fmt.Errorf("cluster A drops did not accumulate: %w", err)
    }

    // Recover
    env.ChaosA.SetErrorRate(0)

    // Assert strategy state via the canonical policy API, not timing heuristics
    // e.g. adw.IsDegraded(htypes.ClusterA)

    return nil
}
```

Register in `cmd/main.go`:

```go
sim.RegisterScenario(&scenarios.MyScenario{})
```

Profile-gate scenarios that should not run in `quick`:

```go
if profile == "comprehensive" || profile == "soak" {
    sim.RegisterScenario(&scenarios.MyScenario{})
}
```

### Assertion guidelines

- Prefer policy API methods (`IsDegraded`, `ShouldFailover`, `Select`) over counter deltas — they are the source of truth and don't have timing windows.
- Use `waitUntil(ctx, timeout, condition)` for all polling; never `time.Sleep`.
- Propagate `waitUntil` errors on gates that precondition an assertion — if the gate times out, return the error rather than discarding it with `_ =`. A silent timeout silently skips the assertion that follows.
- Use `env.ChaosA.Counters()` / `env.ChaosB.Counters()` for exec/scan/drop observability.
- When spawning extra goroutines (e.g. flood workers), cancel them with a `context.CancelFunc` and then call `wg.Wait()` before returning. Relying on context cancellation alone can leave goroutines running after the scenario returns.
- Chaos state is reset between scenarios by the orchestrator. Multi-phase scenarios that need to clear chaos mid-scenario to avoid interfering with later phases are an exception — clear it explicitly before returning on any error path.

## Writing a new strategy group

A strategy group is appropriate when you need to verify behavior that depends on a specific combination of policies — for example, testing `StickyRead` cooldown semantics requires a client configured with a known initial preferred cluster and a known cooldown duration.

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

```bash
go test ./test/simulation/...
```
