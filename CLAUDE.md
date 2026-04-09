# Helix — Claude Code Configuration

## What This Project Is

**Helix** (`github.com/arloliu/helix`) is a Go library for high-availability dual-database operations, designed for "Shared Nothing" architecture. It provides active-active dual writes, sticky read routing, and asynchronous reconciliation for independent Cassandra/ScyllaDB clusters.

Key public packages:
- **Root (`helix`)** — `CQLClient`, `SQLClient`, `NewCQLClient`, `NewSQLClientFromDB`, `WithXxx` options; entry point for all users
- **`adapter/cql/v1`** — gocql adapter (`NewSession`)
- **`adapter/cql/v2`** — Apache cassandra-gocql-driver adapter (`NewSession`)
- **`policy/`** — Read strategies (`StickyRead`, `PrimaryOnlyRead`, `RoundRobinRead`), write strategies (`ConcurrentDualWrite`, `SyncDualWrite`, `AdaptiveDualWrite`), failover policies (`ActiveFailover`, `CircuitBreaker`, `LatencyCircuitBreaker`)
- **`replay/`** — Replay system: `MemoryReplayer`, `NATSReplayer`, `Worker`
- **`topology/`** — Topology awareness: `Local`, `NatsKV`
- **`types/`** — Leaf package: shared interfaces, sentinel errors, `DualClusterError`, `ClusterError`

Internal packages under `internal/` are private implementation details — do not reference them in public API or docs.

## Git Conventions

**Never add `Co-Authored-By` or any other attribution trailers to git commit messages.**

## How to Work in This Codebase

All coding rules, testing conventions, documentation standards, workflow steps, and performance/security guidelines are in numbered rule files. **Read them before making changes.**

All agent skills are invocable capabilities for structured reviews — use them when asked.

@AGENTS.md

## Invoking Skills

To run a skill, ask Claude to use it by name:

- `/go-api-review [package]` — Review exported API and README for DX, discoverability, and clarity. Does not read internal source.
- `/qa-review [package]` — Review for correctness, fault tolerance, error propagation, and concurrency safety from a user perspective.
- `/doc-sync [scope]` — Audit and fix `docs/` files and Godoc to match the current API: corrects stale signatures, removes phantom symbols, adds missing entries.

All skills scope to Helix's public packages by default; you can specify a subset (e.g., `policy/`, `docs/replay-system.md`).
