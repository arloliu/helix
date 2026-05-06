# Helix e2e/cql tests

End-to-end behavioral tests that exercise Helix against **real**
Cassandra/ScyllaDB containers driven through real container lifecycle
events (`Pause`, `Unpause`, `Stop`, `Start`).

## Why this exists alongside the simulation suite

`test/simulation` injects faults via `chaos.Session`, a wrapper that sits
**above** the gocql driver and returns synthetic Go errors. That suite is
fast and deterministic — but it cannot exercise:

- Real gocql error surfaces (`gocql.ErrTimeoutNoResponse`, `*gocql.RequestError`).
- Driver reconnect/timeout semantics.
- The behavior of a hung TCP connection (chaos uses `time.Sleep` *before*
  the real op, not an actual hung socket).
- Adapter normalization claims — does the v1 adapter and the v2 adapter
  produce the same Helix-observable outcomes for the same failure?

This suite drives the gocql v1 (`gocql.com/gocql`) and the apache
cassandra-gocql-driver v2 through their native code paths against a real
Cassandra/Scylla process, then asserts Helix-observable equivalence.

## Running

```bash
make test-e2e
# or
go test -tags e2e -timeout 30m -v ./test/e2e/...
```

Requires Docker. Not invoked by `make test`, `make test-all`, or
`go test ./...` — gated by the `//go:build e2e` build tag.

## Backend selection

`testutil.IsAIOAvailable()` decides between ScyllaDB (preferred, faster)
and Cassandra. On a host **without** AIO (most macOS, many Linux desktops
without raised `aio-max-nr`), the tests run against Cassandra.

> **Caveat:** `Stop`/`Start` of a Cassandra container in testcontainers is
> unreliable — the container restarts but Cassandra fails its readiness
> check, likely due to broadcast-address mismatch or commit-log replay
> issues. The e2e suite therefore uses **`Pause`/`Unpause`** as the
> primary failure mode, which works reliably on both backends.
> See `SPIKE_FINDINGS.md` for details.

## Conventions

- **Tests run sequentially.** Do **not** call `t.Parallel()` — these tests
  pause the package-shared cluster pair.
- Every destructive call (`Pause`, `Stop`) must have a matching restore
  registered via `withRestoredCluster(t, cluster)` **before** the call,
  so a panic between the two doesn't leave the cluster dead for the next
  test.
- Use `createTableOnBoth(t, prefix, schema)` to allocate a per-test
  table; cleanup TRUNCATEs (DROP is slow on Cassandra).
- Driver matrix: every scenario should iterate `allDrivers` to assert v1
  and v2 parity. Single-driver scenarios are an exception, not a default.

## Scenario inventory

| Test file | Scenario | Probes |
|---|---|---|
| `parity_test.go` | S6: same scenario on v1 and v2, fingerprint diff | H3: error-type / `errors.Is` divergence |
| `lcb_test.go` | S3: pause + LatencyCircuitBreaker | H1, H2: timeout & context-cancel propagation |
| `read_failover_test.go` | S2: read failover × {Sticky, PrimaryOnly, RoundRobin} × {v1, v2} | strategy state machine under real timeouts |
| `write_replay_test.go` | S1: write path with replay drain | replay queue grows, then drains after Unpause |
| `dual_fail_test.go` | S4: both paused → DualClusterError (writes) | adapter normalizes errors to Helix sentinel |
| `recovery_probe_test.go` | S5: long outage with no traffic | passive vs op-driven recovery |

Hypotheses (H1–H7) are listed in the e2e plan history in commit
messages and `SPIKE_FINDINGS.md`.

## Debugging a failing scenario

1. Check `SPIKE_FINDINGS.md` first — known limitations may apply.
2. Re-run with `-run TestS<n>_…` to isolate.
3. Tests log per-driver outcomes via `t.Logf` — diff v1 vs v2 logs to see
   exactly which fingerprint field diverged.
4. The test cluster pair persists for the duration of `TestMain`; if a
   single test leaves the cluster in a bad state, subsequent tests skip
   or fail. The package-level cleanup terminates containers regardless.
