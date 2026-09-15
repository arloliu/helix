// Package leak fails a test binary whose goroutines outlive it.
//
// Helix's product is background goroutines:
// the event dispatcher, the replay worker and its per-cluster backends,
// the mirror engine's worker pool, both topology watchers,
// and the adaptive-write background leg.
// Shutdown is asserted indirectly today —
// a counter settles, a queue empties, Stop returns —
// none of which observes whether the goroutine behind it actually exited.
//
// A goroutine's existence has nothing to subscribe to;
// rule 300-testing names it as one of the three cases
// where polling is the correct tool.
// This package polls [runtime.Stack].
//
// It lives outside the testutil package proper because that package
// imports helix, which would make it unimportable from helix's own
// in-package tests.
package leak

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// settleTimeout bounds the wait for goroutines that are on their way out.
// A goroutine released by Close still has to be scheduled before it can
// return,
// so a snapshot taken the instant a test ends sees survivors that are not leaks.
// Every poll after the first is pure waiting,
// so this only costs wall clock on the failing path.
const settleTimeout = 5 * time.Second

// settleInterval is how often the survivors are re-counted while waiting.
const settleInterval = 50 * time.Millisecond

// ignoredFrames are stack frames belonging to the runtime or the testing
// package rather than to code under test.
// A goroutine parked in one of them is scaffolding:
// the signal handler, the test runner's own bookkeeping,
// or this package reading the stacks.
//
// Each entry must be matchable against a single goroutine's stack as
// stacks splits them, on a blank line.
// An entry spanning that boundary can never match, and would claim a
// coverage this list does not have.
var ignoredFrames = []string{
	"testing.(*M).Run",
	"testing.runTests",
	"testing.tRunner",
	"os/signal.signal_recv",
	"os/signal.loop",
	"runtime.ensureSigM",
	"test/testutil/leak.stacks",
}

// TestMain runs a package's tests and then fails the binary if any
// goroutine started during the run is still alive.
//
// Call it from the package's TestMain:
//
//	func TestMain(m *testing.M) { leak.TestMain(m) }
//
// The check is skipped when the tests themselves failed, because a test
// that failed early is expected to leave its fixtures running and the
// leak report would bury the real failure.
//
// Parameters:
//   - m: The testing.M handed to the package's TestMain
func TestMain(m *testing.M) {
	before := snapshot()

	code := m.Run()
	if code == 0 {
		if leaked := settle(before, settleTimeout); len(leaked) > 0 {
			report(os.Stderr, leaked)
			code = 1
		}
	}

	// TestMain owns the process exit status; there is no other way to fail
	// a binary whose tests all passed.
	os.Exit(code) //nolint:revive // deep-exit: TestMain is where the status code is set
}

// Check fails t at cleanup if the test leaves a goroutine behind.
//
// Use it on a test that exercises a lifecycle directly — a worker's Stop,
// a client's Close — where the package-level TestMain check would name
// the whole binary rather than the test that leaked.
//
// Register it before the code under test starts anything, so the snapshot
// predates the goroutines it should account for:
//
//	func TestWorkerStopJoins(t *testing.T) {
//		leak.Check(t)
//		w := replay.NewWorker(...)
//		...
//	}
//
// Parameters:
//   - t: The test to fail if a goroutine survives it
func Check(t *testing.T) {
	t.Helper()

	before := snapshot()
	t.Cleanup(func() {
		if t.Failed() {
			return
		}

		if leaked := settle(before, settleTimeout); len(leaked) > 0 {
			var b strings.Builder
			report(&b, leaked)
			t.Error(b.String())
		}
	})
}

// settle polls until every goroutine started since before has exited,
// or until timeout elapses.
// It returns the stacks still outstanding.
func settle(before map[uint64]struct{}, timeout time.Duration) []string {
	deadline := time.Now().Add(timeout)
	for {
		leaked := diff(before, stacks())
		if len(leaked) == 0 || time.Now().After(deadline) {
			return leaked
		}

		time.Sleep(settleInterval)
	}
}

// snapshot records the IDs of the goroutines alive right now.
func snapshot() map[uint64]struct{} {
	ids := make(map[uint64]struct{})
	for id := range stacks() {
		ids[id] = struct{}{}
	}

	return ids
}

// stacks returns every live goroutine's stack, keyed by goroutine ID.
func stacks() map[uint64]string {
	buf := make([]byte, 64<<10)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}

	out := make(map[uint64]string)
	for g := range strings.SplitSeq(string(buf), "\n\n") {
		g = strings.TrimSpace(g)
		id, ok := goroutineID(g)
		if !ok {
			continue
		}
		out[id] = g
	}

	return out
}

// goroutineID parses the ID out of a stack's "goroutine 42 [running]:" header.
func goroutineID(stack string) (uint64, bool) {
	rest, ok := strings.CutPrefix(stack, "goroutine ")
	if !ok {
		return 0, false
	}

	digits, _, ok := strings.Cut(rest, " ")
	if !ok {
		return 0, false
	}

	id, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, false
	}

	return id, true
}

// diff returns the stacks in after whose goroutines are not in before and
// are not runtime or testing scaffolding.
func diff(before map[uint64]struct{}, after map[uint64]string) []string {
	var leaked []string
	for id, stack := range after {
		if _, known := before[id]; known {
			continue
		}
		if ignored(stack) {
			continue
		}
		leaked = append(leaked, stack)
	}

	return leaked
}

// ignored reports whether a stack belongs to scaffolding rather than to
// code under test.
func ignored(stack string) bool {
	for _, frame := range ignoredFrames {
		if strings.Contains(stack, frame) {
			return true
		}
	}

	return false
}

// report writes the leaked stacks to w.
func report(w io.Writer, leaked []string) {
	_, _ = fmt.Fprintf(w, "goroutine leak: %d goroutine(s) outlived the tests\n", len(leaked))
	for _, stack := range leaked {
		_, _ = fmt.Fprintf(w, "\n%s\n", stack)
	}
}
