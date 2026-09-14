# Exactness claims on polled counters

26 async waits assert that something happened *exactly* N times
while waiting for a monotonic counter to *reach* N.
`require.Eventually` returns the moment the condition first holds,
so the wait cannot observe an increment past N —
the assertion passes whether the count ends at N or at N+3,
which is the opposite of what its message claims.

This is a correctness gap in the tests, not a style one:
each of these sites is currently unable to fail for the thing it is there to check.

## The fix

`replay/eviction_nats_test.go` already solves it and is the template.
It pairs the wait at `:71` with a `require.Never` on the over-count at `:84`
(the twelve lines between them assert on other state):

```go
require.Eventually(t, func() bool { return em.evicted() == 3 }, ...)
require.Never(t, func() bool { return em.evicted() != 3 }, 2500*time.Millisecond, 100*time.Millisecond)
```

Prefer converting to a channel wait on the hook the counter is written from
where that is mechanical — see the async section of
[`.agents/rules/300-testing.md`](../../.agents/rules/300-testing.md)
and `gatedStateCollector` in `policy/failover_policy_test.go`.
Where the wait must stay, add the `require.Never` guard.

Pick the `Never` window against what the test is bounding,
not a fixed number: it has to outlast the retry or backoff that a spurious extra
increment would arrive on.

## Sites

All 26 have a hook already wired (bucket A) unless noted.

### Root package and `policy/` (7)

| Site | Claim |
|---|---|
| `write_deferred_test.go:64` | `len(replayer.payloads) == 2` — "each failed background leg must be enqueued for replay once" |
| `write_deferred_test.go:110` | `len(spy.samplesFor(ClusterB)) == 1` — "must observe its duration once, when it completes" |
| `write_deferred_test.go:230` | `mc.writeDropped[ClusterB] == 1` — "must be dropped, not start a third pending admission" |
| `write_cluster_timeout_test.go:68` | `len(replayer.payloads) == 1` |
| `write_cluster_timeout_test.go:74` | `consecutiveFailures == 1` — no seam wired; `recovery_probe.go` offers `IncRecoveryProbeFailure` and `logProbeFailure` |
| `cql_client_recovery_probe_test.go:529` | `probes.failureA == 5 && calls > 5` — "must fail five times and then block" |
| `policy/adaptive_write_test.go:515` | `m.GetWriteErrors(A) == 1` |

### `replay/` (19)

| Site | Claim |
|---|---|
| `worker_test.go:682` | `dropped == 1` — "OnDrop must fire exactly once after MaxAttempts". The strongest case: the test already follows it with a 50ms sleep to "give the worker a moment to demonstrate it does NOT re-enqueue", which is the `require.Never` guard written by hand and without a real bound |
| `worker_test.go:214` | `processedCount == 5` |
| `worker_test.go:249` | `successCount == 1` |
| `worker_test.go:422` | `processedA == 3 && processedB == 2` |
| `worker_test.go:479` | `processedA == 3 && processedB == 3` |
| `worker_test.go:746` | `processedHealthy == 1` |
| `worker_test.go:876` | `success == 1` |
| `cluster_gate_test.go:191` | `executed == 1` — "the requeued payload runs exactly once" |
| `cluster_gate_test.go:75` | `executed == 2 && replayer.Len() == 0` |
| `cluster_gate_test.go:107` | `executed == 1` |
| `cluster_gate_test.go:116` | `executed == 2` |
| `cluster_gate_test.go:157` | `executed == 1` |
| `cluster_gate_test.go:166` | `executed == 2` |
| `cluster_gate_test.go:295` | `executed == 1` |
| `cluster_gate_test.go:326` | `executedB == 2` |
| `retained_memory_test.go:82` | `successes == payloads` |
| `retained_memory_test.go:205` | `attempts == payloads` |
| `retained_nats_test.go:179` | `successes == payloads` |
| `corrupt_nats_test.go:46` | `mc.GetReplayCorrupt(A) == 1` |

## Adjacent, decide when you get there

The wait itself uses `>=` and makes no exactness claim,
but a line or two later the test asserts an exact value on a counter that is still live.
The unsoundness moved down a line rather than being absent.

In four of the six the later assertion reads the very counter that was awaited.
In the two `cql_client_recovery_probe_test.go` entries it reads the *other* cluster's counter
(await `successB >= 3`, then assert `successA` is zero),
which is a weaker version of the same problem: nothing bounds when the zero is read.

These six sit in the 17 sound-looking `>=` sites counted as out of scope below.
Whether they belong there is the decision this section defers.

`cql_client_recovery_probe_test.go:240` (→ `:242`),
`cql_client_recovery_probe_test.go:300` (→ `:305`, `:307`),
`replay/retained_memory_test.go:78` (→ `:79`, `:80`),
`topology/nats_test.go:765` (→ `:768`),
`policy/adaptive_write_test.go:550` (→ `:555`),
`failover_probe_test.go:167` (→ `:169`).

## Out of scope

The other 65 of the 91 in-process waits:

- 17 have a wired hook but a `>=` condition and no exactness claim in the wait itself
  — sound as waits, just not idiomatic. Six of them carry the adjacent problem above.
- 19 have a seam the test does not install.
- 28 have no subscribe point at all and fall under the rule's exception.
- 1 is `replay/eviction_nats_test.go:71`, which makes an exactness claim and is already guarded.

The 37 container-backed waits in `test/e2e/cql/` (24) and `test/integration/` (13) were not classified,
except to note that 8 of them wait on an in-process hook rather than on the container
(`nats_batch_integration_test.go:233`, `:315`, `:391`;
`topology_integration_test.go:340`, `:401`, `:594`, `:675`, `:690`).

## Where this came from

A classification of all 128 `Eventually` call sites as of `main` at `45b88da`
(91 in-process + 37 container-backed; two of the e2e ones were converted away in the same PR),
run while deciding what the async rule's polling exception should cover.
The rule now names the `require.Never` guard;
this file is the list of places that need it.
