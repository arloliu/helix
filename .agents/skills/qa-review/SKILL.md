---
name: qa-review
description: Perform a critical review focused on correctness, fault tolerance, and performance implications of a Go library from the perspective of external users.
---

# QA Review - Go Library Robustness and Correctness

**Assumed Role:** Quality Assurance (QA) Engineer.

**Testing Premise:** Your testing plan relies on the public API (Godoc) and the README as the primary specifications. You need to ensure the library is robust, reliable, and compliant with its published contract.

When executing this skill, perform a critical review focused on **correctness, fault tolerance, and performance implications**, specifically addressing the following points from the perspective of a user who intends to misuse the library:

## Scope

When reviewing specific packages, specify them by name. Default scope for Helix:
- Root package (`helix`): `CQLClient`, `WithXxx` options
- `adapter/cql/v1`, `adapter/cql/v2`: session adapters
- `policy/`: `StickyRead`, `PrimaryOnlyRead`, `RoundRobinRead`, `ConcurrentDualWrite`, `SyncDualWrite`, `AdaptiveDualWrite`, `ActiveFailover`, `CircuitBreaker`, `LatencyCircuitBreaker`
- `replay/`: `MemoryReplayer`, `NATSReplayer`, `Worker`
- `topology/`: `Local`, `NatsKV`
- `types/`: Shared interfaces, sentinel errors, `DualClusterError`, `ClusterError`

## 1. Functional Correctness and Compliance Testing

1. **Public API Contract Gaps:**
    * Identify any ambiguity where the Godoc describes behavior but provides insufficient detail, such as **dual-write error semantics, replay ordering guarantees, or exact side effects** upon function calls.
    * Are there **implicit, undocumented limitations** (e.g., maximum replay queue depth, CAS/LWT unsafety, required non-zero config values) that are enforced by the code but not specified in documentation?

2. **Edge Case Identification & Initialization:**
    * Identify critical **zero-value or nil-pointer dereference** risks in public methods, especially when optional strategies (`ReadStrategy`, `WriteStrategy`, `FailoverPolicy`, `Replayer`) are not configured.
    * What happens when `NewCQLClient` is called with no options? Is the default behavior documented?
    * Does `Close()` behave correctly when called multiple times?

## 2. Fault Tolerance and Error Handling

1. **Dual-Write Error Semantics:**
    * Verify the documented contract: `nil` returned if ≥1 cluster succeeds; `*types.DualClusterError` if both fail.
    * Are partial failures reliably enqueued for replay? What happens if the replay queue is full (`ErrReplayQueueFull`)?
    * Does `AdaptiveDualWrite` correctly document `ErrWriteAsync` and `ErrWriteDropped` return conditions?

2. **Error Propagation and Inspection:**
    * Analyze the **error propagation strategy**. Does the library consistently use Go's standard error wrapping (`fmt.Errorf` with `%w`) to preserve the error chain?
    * Are `errors.Is()` and `errors.As()` usable for all documented sentinel errors and error types?
    * Are cluster-specific errors consistently wrapped in `*types.ClusterError`?

3. **Resource Management Safety:**
    * For types requiring **cleanup** (`CQLClient.Close()`, `Worker.Stop()`), are these methods safe to call multiple times?
    * Are **NATS reconnection, JetStream timeouts, and context cancellation** handled gracefully without leaking goroutines?
    * Does the `NATSReplayer` handle NATS disconnects gracefully (replay messages not lost)?

## 3. Non-Functional Concerns (Concurrency and Performance)

1. **Concurrency Guarantees:**
    * Verify all **thread-safety guarantees** for exported types. `CQLClient` is used concurrently — are all state mutations properly synchronized?
    * Are callbacks (strategy selection, failover decisions) documented as to whether they are called from the caller's goroutine or a background goroutine?
    * Is `StickyRead`'s cluster-selection state safe for concurrent access from multiple goroutines?

2. **Performance and Memory:**
    * Identify functions involving **deep copying of large data structures** that could cause GC pressure under high query throughput.
    * Does `AdaptiveDualWrite`'s latency tracking use bounded data structures?
    * Is the in-memory replay queue bounded? What is the documented behavior when it fills up?
    * Look for **goroutine leaks** — does canceling a context during a dual-write properly terminate both cluster goroutines?
