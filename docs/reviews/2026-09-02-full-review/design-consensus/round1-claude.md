# Design questions — Round 1 (Claude's framing and recommendations)

Facts established first:
- `doc.go:47` documents "nil: at least one cluster succeeded"; `cql_client.go:2018` comment says "all-async is still success". Contradiction.
- `docs/replay-system.md:692-711` frames MaxDeliver/MaxAttempts as poison-message handling; CHANGELOG records infinite→bounded as a behavior change. Intent was poison cap; defaults double as outage budget.

Q1 v1.x compatibility policy
  A strict semver (all observable changes opt-in until v2)
  B documented-contract-first: contradictions with docs are bugs fixable in minor; durability-affecting defaults may change in minor with CHANGELOG "Behavior change" + migration; other observable changes opt-in
  C loose
  → recommend B

Q2 release vehicle for the replay durability fix
  patch v1.6.1 first vs fold into v1.7.0 with root refactor
  → recommend patch v1.6.1

Q3 contract of MaxDeliver / MaxAttempts
  A keep poison cap, rewrite docs to say replay is not outage survival
  B outage budget: unreachable errors do not consume attempts; MaxDeliver/MaxAttempts count only rejected (poison) errors
  C B + a time bound (WithReplayRetryWindow(d); default = NATS MaxAge 24h, memory 1h)
  → recommend C

Q4 where the unreachable-vs-rejected classifier lives
  A heuristics in types (errors.Is ctx, net.Error, string match)
  B adapter normalisation to a sentinel (types.ErrClusterUnreachable), like ErrNotFound today
  C B primary, A fallback, plus WithReplayErrorClassifier(func(error) bool) override; reuse for auto-refresh
  → recommend C

Q5 is DeadlineExceeded a latency signal for AdaptiveDualWrite / LatencyCircuitBreaker
  A no ctx error is ever a health signal
  B Canceled excluded, DeadlineExceeded kept as latency signal
  C option, default false
  → recommend A (Adaptive already measures leg latency directly; deadline hits both legs at once)

Q6 ForceDegrade sticky?
  A sticky latch, cleared only by ForceRecover/Reset; probe skips latched cluster
  B sticky with TTL (new signature)
  C keep, document "disable probe first"
  → recommend A (+ hourly Warn while latched); TTL later as ForceDegradeFor

Q7 purpose of the read-side CircuitBreaker
  A gates strategy state change only: retry on alt always allowed (if not draining); verdict passed to OnFailure
  B real breaker: optional RouteAway(cluster) bool consulted after Select
  C A + B
  → recommend C; RouteAway is an optional interface so custom policies keep old behaviour

Q8 return value when both legs are async/dropped
  A always error (DualClusterError{ErrWriteAsync, ErrWriteAsync})
  B new sentinel types.ErrWriteAsyncOnly (errors.Is-able, wraps both leg states)
  C durable-marker on Replayer keeps nil
  → recommend B

Q9 pending replays after SwapSession
  A keep "backlog follows the slot", document it
  B session-generation fence in payload
  C SwapSession parameter
  → recommend A
