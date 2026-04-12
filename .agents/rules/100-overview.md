# 100 - Project Overview & Prime Directives

## Identity
- **Project:** Helix (dual-database HA client)
- **Module:** `github.com/arloliu/helix`
- **Language:** Go >=1.25.0
- **Linting:** `golangci-lint` v2.5.0 (via `make lint`)

## What Helix Does
Helix is a high-availability dual-database client library designed for "Shared Nothing" architecture. It performs active-active dual writes to two independent Cassandra/ScyllaDB clusters, routes reads with sticky affinity for cache efficiency, and asynchronously reconciles partial write failures via an in-memory or NATS JetStream replay system.

## Project Structure
```
helix/                        # Root = main public package (CQLClient, options)
├── adapter/                  # CQL adapter shims
│   └── cql/
│       ├── v1/               # gocql adapter (NewSession)
│       └── v2/               # Apache cassandra-gocql-driver adapter (NewSession)
├── policy/                   # Read/write strategies and failover policies
│   ├── read_strategy.go      # StickyRead, PrimaryOnlyRead, RoundRobinRead
│   ├── write_strategy.go     # ConcurrentDualWrite, SyncDualWrite
│   ├── adaptive_write.go     # AdaptiveDualWrite
│   └── failover_policy.go    # ActiveFailover, CircuitBreaker, LatencyCircuitBreaker
├── replay/                   # Async reconciliation system
│   ├── memory.go             # MemoryReplayer (volatile, for dev/test)
│   ├── nats.go               # NATSReplayer (durable, for production)
│   └── worker.go             # Background replay worker
├── topology/                 # Topology awareness
│   ├── local.go              # Local/static topology
│   └── nats.go               # NATS KV-backed topology
├── types/                    # Shared contracts — leaf package (interfaces, errors, constants)
├── contrib/                  # Supplementary integrations (metrics)
├── internal/                 # Private implementation
│   ├── logging/              # Logger helpers
│   └── metrics/              # Metrics helpers
├── test/                     # Integration, simulation & test utilities
│   ├── integration/
│   ├── simulation/
│   └── testutil/
├── examples/                 # Example programs
└── docs/                     # Design & user documentation
```

## Architecture Notes
- **Import cycle prevention:** The `types/` package is a leaf — no imports from other helix packages. All shared interfaces, errors, and constants live there.
- **Dual-write semantics:** Write operations return `nil` if at least one cluster succeeds; failed writes are enqueued for replay. Both clusters failing returns `*types.DualClusterError`.
- **Adapter pattern:** `adapter/cql/v1` wraps `gocql.Session`; `adapter/cql/v2` wraps the Apache cassandra-gocql-driver. The root package is driver-agnostic.
- **Code generation:** `replay/` uses `msgp` for MessagePack serialization. Run `make generate` after changing generated types.

## Prime Directives
1. **Plan First:** Create/update `implementation_plan.md` before architectural changes. Wait for approval.
2. **Small Diffs:** Break work into small, verifiable chunks. Do not rewrite files unnecessarily.
3. **Dependencies:** Check `go.mod`. Prefer stdlib. Ask before adding new deps.
    - **Blocked dependencies** (enforced by linter):
        - `github.com/gofrs/uuid` → use `github.com/google/uuid`
        - `github.com/satori/go.uuid` → use `github.com/google/uuid`

## Key Dependencies
- **CQL:** `github.com/gocql/gocql`, `github.com/apache/cassandra-gocql-driver/v2`
- **NATS:** `github.com/nats-io/nats.go` (core + JetStream, for replay and topology)
- **Metrics:** `github.com/VictoriaMetrics/metrics`
- **Serialization:** `github.com/tinylib/msgp` (MessagePack for replay messages)
- **UUID:** `github.com/google/uuid`
- **Testing:** `github.com/stretchr/testify`, `github.com/testcontainers/testcontainers-go`
