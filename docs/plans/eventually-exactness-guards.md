# Exactness claims on polled counters

26 async waits asserted that something happened *exactly* N times
while waiting for a monotonic counter to *reach* N.
`require.Eventually` returns the moment the condition first holds,
so the wait cannot observe an increment past N —
the assertion passed whether the count ended at N or at N+3,
which is the opposite of what its message claimed.

All 26 are fixed. This file records what the fix turned out to be,
because it was not what this file originally prescribed,
and what is still open.

## What the fix turned out to be

Every wait now asks `>=`, which is all a wait can honestly claim,
and the exact value is asserted at a point where the counter can no longer move.
Three such points exist, and between them they covered all 26 sites:

**`Worker.Stop()`** closes `stopCh` and then `wg.Wait()`s,
so every worker goroutine has returned before it does.
A count read after it is final, not sampled. 14 sites.

Two orderings matter around it.
Stop drains the queue and drops what is left through `OnDrop`,
so a backlog a test means to assert on must be read *before* Stop
(`TestMemoryWorker_GateIsPerCluster` was written the other way round and failed).
That same drain is what makes the guard strong:
a payload wrongly re-enqueued shows up as an extra drop instead of going unseen.

**`CQLClient.Close()`** calls `deferred.wait()`,
which waits for the background legs whose failure would be enqueued for replay. 5 sites.
Close is idempotent, so an explicit call before the `t.Cleanup(client.Close)` is safe.

**A structural bound the test already had.** 6 sites.
An empty queue after a settled payload leaves nothing to dispatch twice;
a probe loop parked on a channel cannot start the call that would increment again.
These sites were never actually broken — the bound was there,
it just sat a line below a wait that claimed to be doing the work.
For them the fix is only the `>=` relaxation and a comment naming the real bound.

The remaining site, `policy/adaptive_write_test.go`, was converted to the
completion seam `awaitWriteLeg` that the file already uses:
the leg records `IncWriteError` before it completes, so waiting on completion
makes the count final.

**No `require.Never` was needed anywhere.**
The template in `replay/eviction_nats_test.go` remains correct for a site with
no quiescence point, but 26 for 26 had one.
Prefer quiescence: it costs no wall clock, where a `Never` costs its whole window
on the passing path. The replay package's runtime did not move.

## The adjacent six, also closed

The wait itself used `>=` and made no exactness claim,
but a line or two later the test asserted an exact value on a counter that was still live.
The unsoundness had moved down a line rather than being absent.

Four took the same quiescence treatment as the 26.
`CQLClient.Close()` cancels the recovery-probe loops and waits for them, which is what
makes "no probe ever ran against the healthy cluster" a claim about the whole run
rather than about the instant the line was read:
`cql_client_recovery_probe_test.go` (two sites) and `failover_probe_test.go`.
`policy/adaptive_write_test.go` moved to `awaitWriteLeg`, and now asserts on the warn
it means rather than on a count — the explicit logger receives two warns, the degrade
and the failed leg, and only the second one is what the test is about.

`topology/nats_test.go` needed no mechanism. The fake fails the first three `Watch`
calls and no more, so two warns is the whole run; the fix is a comment naming that cap.

`replay/retained_memory_test.go` was the one that could not take a quiescence point:
its assertion is *about* mid-flight state, the backlog held during an outage, so
stopping the worker would destroy the thing being checked.
It was also a latent flake — the outage was a 300ms deadline, and the assertion was
only valid while that deadline had not passed, racing 150 retries against the clock.
The outage now ends on a flag the test sets, so the backlog is intact by construction
rather than by winning a race, and the test no longer spends the 300ms.

## Out of scope

The other 65 of the 91 in-process waits, as classified when this list was drawn up:

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
