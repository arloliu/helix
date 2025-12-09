# Helix Simulation Framework

The Helix Simulation Framework provides a controlled environment for testing the resilience and behavior of the Helix client under various failure conditions. It uses `testcontainers` to spin up a Cassandra cluster and a chaos proxy to simulate network partitions, latency, and errors.

## Architecture

- **Orchestrator (`simulation.go`)**: Manages the lifecycle of the simulation, including starting containers, initializing the Helix client, and running scenarios.
- **Chaos Proxy (`chaos/`)**: Wraps the standard `gocql` session to inject faults.
- **Scenarios (`scenarios/`)**: Define specific failure patterns and validation logic.
- **Workload Generator**: Produces a steady stream of reads and writes to exercise the system.

## Running the Simulation

The simulation entry point is located in `test/simulation/cmd/main.go`.

### Prerequisites

- Go 1.22+
- Docker (for Testcontainers)

### Commands

Run the standard profile (Basic scenarios):
```bash
go run ./test/simulation/cmd/main.go -profile standard -duration 30s
```

Run the comprehensive profile (Includes Replay Saturation and Drain Mode):
```bash
go run ./test/simulation/cmd/main.go -profile comprehensive -duration 60s
```

Run the soak test profile (Long-running stability test):
```bash
go run ./test/simulation/cmd/main.go -profile soak -duration 1h
```

### Scenarios

| Scenario | Description | Profile |
|----------|-------------|---------|
| `DegradedCluster` | Simulates high latency on one cluster to verify failover. | Standard |
| `AdaptiveRecovery` | Simulates a flapping cluster to verify adaptive recovery. | Standard |
| `CompleteFailure` | Simulates a total outage of one cluster. | Standard |
| `ReplaySaturation` | Disconnects a cluster for a long period to fill the replay buffer. | Comprehensive |
| `DrainMode` | Simulates a backlog and verifies the drain process. | Comprehensive |

## Adding New Scenarios

1. Create a new file in `test/simulation/scenarios/`.
2. Implement the `types.Scenario` interface.
3. Register the scenario in `test/simulation/cmd/main.go`.

```go
type MyNewScenario struct{}

func (s *MyNewScenario) Name() string {
    return "my-new-scenario"
}

func (s *MyNewScenario) Run(ctx context.Context, env types.Environment) error {
    // Implementation
    return nil
}
```
