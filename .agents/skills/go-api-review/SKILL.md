---
name: go-api-review
description: Performs a critical review focused on discoverability, clarity, and ease of use (Developer Experience) of a Go package, relying exclusively on the exported API (Godoc) and the README, without reading internal source code.
---

# Go API Review Skill

As an agent using this skill, your task is to evaluate a Go package as a potential user evaluating it. You must rely exclusively on the exported API (Godoc) and the README. **Do not read the internal source code.**

Please perform a critical review focused on **discoverability, clarity, and ease of use (Developer Experience)**, specifically addressing the following points using idiomatic Go standards:

## Scope

When reviewing specific packages, specify them by name. The reviewable packages in Helix:
- Root package (`helix`): Entry point — `CQLClient`, `SQLClient`, `NewCQLClient`, `NewSQLClientFromDB`, `WithXxx` options
- `adapter/cql/v1`: gocql adapter — `NewSession`
- `adapter/cql/v2`: Apache driver adapter — `NewSession`
- `policy/`: Strategies and policies — `NewStickyRead`, `NewPrimaryOnlyRead`, `NewRoundRobinRead`, `NewConcurrentDualWrite`, `NewSyncDualWrite`, `NewAdaptiveDualWrite`, `NewActiveFailover`, `NewCircuitBreaker`, `NewLatencyCircuitBreaker`
- `replay/`: Reconciliation — `NewMemoryReplayer`, `NewNATSReplayer`, `NewWorker`
- `topology/`: Topology awareness — `Local`, `NatsKV`
- `types/`: Shared contracts — `CQLSession`, `DualClusterError`, `ClusterError`, sentinel errors

## 1. API and Interface Quality (The "Clean" Test)

1. **Interface Design:**
    * Are the exported interfaces small, single-purpose, and named by their behavior (e.g., `ReadStrategy`, `WriteStrategy`, `FailoverPolicy`)?
    * Does the library adhere to the "Accept Interfaces, Return Structs" principle?
    * Are the interfaces in `types/` appropriately scoped, or do any try to do too much?

2. **Method Clarity & Idioms:**
    * Are all public functions and methods focused on a single responsibility?
    * Is there any ambiguity regarding when to use a **value receiver** vs. a **pointer receiver** on methods, and are pointer receivers used consistently where mutation occurs?
    * Do functional options (`WithX` pattern) follow idiomatic Go conventions?
    * Is the Query builder API (chaining `.Consistency()`, `.WithContext()`, `.Exec()`) clearly documented?

3. **Error and Context Handling:**
    * Are **`context.Context`** and standard **`error`** returns implemented idiomatically?
    * Are the dual-write error semantics clearly documented: nil if ≥1 cluster succeeds; `*types.DualClusterError` if both fail?
    * Can users reliably use `errors.Is()` with sentinel errors and `errors.As()` with `*types.DualClusterError` / `*types.ClusterError`?
    * Are all sentinel errors in `types/` comprehensive enough to cover common failure modes?

## 2. Documentation and Examples (The "Good Enough" Test)

1. **Clarity and Purpose:**
    * Does the **package-level Godoc** (`doc.go`) clearly state the package's purpose and value proposition?
    * Are there any exported functions, types, or fields that lack a clear, helpful top-level Godoc comment?
    * Does the root package doc explain the dual-write error model (nil vs. `DualClusterError`)?

2. **Examples and Learning Curve:**
    * Does the **README** contain a clear, working code example for the most common use case (CQL dual-write)?
    * Are there `Example` functions in `example_test.go` files that appear correctly in Godoc?
    * Is the relationship between `CQLClient`, `policy/`, `replay/`, and `adapter/` clear from examples alone?
    * Can a new user understand the setup flow: create sessions → choose strategies → create client → use?

## 3. Ambiguity and Misuse (The "Caveat" Test)

1. **Potential Misuse:**
    * Are there public functions or types that could be easily misused? For example:
        - Creating a dual-cluster client without a `Replayer` (partial write failures lost permanently)
        - Using CAS/LWT operations (not safe in shared-nothing dual-cluster)
        - Not calling `Close()` on the client
        - Mutating the returned values from query results
    * Is the CAS/LWT warning clearly documented?

2. **Concurrency and Lifecycle:**
    * Are there documented **thread-safety guarantees** for `CQLClient` and strategy types?
    * Is the recommended lifecycle explicitly documented: create client → use → `Close()`?
    * Is it clear that dual writes execute concurrently and each cluster gets an independent timeout context?
