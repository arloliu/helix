# 600 - Performance & Security

## Performance
Apply these in **hot paths** (dual-write execution, replay enqueueing, per-query code):

- **Allocations:**
    - Pre-allocate slices: `make([]T, 0, expectedCap)`
    - Pre-allocate maps: `make(map[K]V, expectedSize)`
    - Avoid `append` in tight loops if size is predictable.
- **Dual-Write Concurrency:** Both cluster writes run concurrently via goroutines. Keep per-write allocations minimal to avoid GC pressure under high throughput.
- **Inlining:** Keep hot functions small and simple.
- **Pointers:** Pass small structs by value. Use pointers only when mutation is needed.
- **Interfaces:** Avoid in critical paths (indirect calls have overhead).
- **Profiling:** Use `pprof` to find bottlenecks before optimizing.
- **Concurrency:** Use `sync/atomic` for simple flags/counters. Use `sync.Mutex` for complex state.
- **Replay Queue:** The in-memory replay queue is bounded — do not grow it unboundedly. Check `ErrReplayQueueFull` handling.

## Security
- **Input:** Validate ALL external input at system boundaries.
- **Secrets:** Never log secrets (credentials, keyspace passwords). Never commit secrets.
- **Transport:** HTTPS/TLS for all external connections. Use TLS config for Cassandra clusters in production.
- **NATS Auth:** Support NATS credential-based authentication for the NATSReplayer where applicable.
- **CQL Injection:** Always use parameterized queries — never concatenate user input into CQL strings.
