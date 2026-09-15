package leak

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestSettleIgnoresGoroutineThatExits verifies a goroutine still running at
// the moment of the snapshot is not reported once it returns.
// Without the settle loop every Close would look like a leak,
// because releasing a goroutine and it being scheduled are not the same instant.
func TestSettleIgnoresGoroutineThatExits(t *testing.T) {
	before := snapshot()

	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-release
	}()

	require.NotEmpty(t, settle(before, 0), "the parked goroutine should be outstanding before it is released")

	close(release)
	<-done

	require.Empty(t, settle(before, time.Second))
}

// TestSettleReportsGoroutineThatNeverExits verifies a goroutine that outlives
// the window is reported, with a stack naming the function that started it.
func TestSettleReportsGoroutineThatNeverExits(t *testing.T) {
	before := snapshot()

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	go func() { <-release }()

	leaked := settle(before, 100*time.Millisecond)
	require.Len(t, leaked, 1)
	require.Contains(t, leaked[0], "TestSettleReportsGoroutineThatNeverExits",
		"the report must name the test that started the goroutine")
}

// TestSettleIgnoresGoroutinesPresentBefore verifies the snapshot is what
// bounds the check: a goroutine already running when the snapshot was taken
// is not this test's leak.
func TestSettleIgnoresGoroutinesPresentBefore(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	go func() { <-release }()

	// Let the goroutine reach its park before snapshotting, so that it is
	// counted; a goroutine that has not started yet would not be.
	require.Eventually(t, func() bool {
		for _, stack := range stacks() {
			if strings.Contains(stack, "TestSettleIgnoresGoroutinesPresentBefore") {
				return true
			}
		}

		return false
	}, time.Second, 10*time.Millisecond)

	require.Empty(t, settle(snapshot(), 100*time.Millisecond))
}

// TestIgnoredSkipsScaffolding verifies the runtime and testing frames that
// every binary carries are not mistaken for leaks.
func TestIgnoredSkipsScaffolding(t *testing.T) {
	require.True(t, ignored("goroutine 1 [chan receive]:\ntesting.(*M).Run(...)"))
	require.True(t, ignored("goroutine 2 [syscall]:\nos/signal.signal_recv(...)"))
	require.False(t, ignored("goroutine 9 [chan receive]:\ngithub.com/arloliu/helix.(*worker).run(...)"))
}

// TestGoroutineID parses the header shapes runtime.Stack emits and rejects
// anything that is not one.
func TestGoroutineID(t *testing.T) {
	id, ok := goroutineID("goroutine 42 [running]:\nmain.main()")
	require.True(t, ok)
	require.Equal(t, uint64(42), id)

	_, ok = goroutineID("not a stack")
	require.False(t, ok)

	_, ok = goroutineID("goroutine abc [running]:")
	require.False(t, ok)
}

// TestReportNamesEveryStack verifies the failure message carries the count
// and each outstanding stack, which is what identifies the culprit when the
// package-level check fires.
func TestReportNamesEveryStack(t *testing.T) {
	var b strings.Builder
	report(&b, []string{"goroutine 7 [select]:\nfirst()", "goroutine 8 [select]:\nsecond()"})

	out := b.String()
	require.Contains(t, out, "2 goroutine(s) outlived the tests")
	require.Contains(t, out, "first()")
	require.Contains(t, out, "second()")
}

// fakeT records what registerCheck does to a test, so its failure path
// can be observed.
// A real *testing.T driven into failure would report the guard's own test
// as failing, which is why the seam exists.
type fakeT struct {
	failed   bool
	errs     []string
	cleanups []func()
}

func (f *fakeT) Helper() {}

func (f *fakeT) Cleanup(fn func()) { f.cleanups = append(f.cleanups, fn) }

func (f *fakeT) Failed() bool { return f.failed }

func (f *fakeT) Error(args ...any) {
	f.failed = true
	f.errs = append(f.errs, fmt.Sprint(args...))
}

// runCleanups runs the registered cleanups the way testing does, last
// registered first.
func (f *fakeT) runCleanups() {
	for i := len(f.cleanups) - 1; i >= 0; i-- {
		f.cleanups[i]()
	}
}

// TestCheckReportsGoroutineThatOutlivesTheTest covers Check's whole point:
// a goroutine still running when the test ends fails that test by name,
// rather than the binary.
func TestCheckReportsGoroutineThatOutlivesTheTest(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	f := &fakeT{}
	registerCheck(f, 50*time.Millisecond)

	started := make(chan struct{})
	go func() {
		close(started)
		<-release
	}()
	<-started

	f.runCleanups()

	require.True(t, f.failed, "a goroutine that outlived the test must fail it")
	require.Len(t, f.errs, 1)
	require.Contains(t, f.errs[0], "goroutine leak")
}

// TestCheckStaysSilentWhenTheGoroutineExits guards the other direction:
// releasing a goroutine and it being scheduled are not the same instant,
// so a clean shutdown must not be reported as a leak.
//
// The goroutine is parked before the cleanup runs and released only
// afterwards, so settle has to poll past its first attempt to see it go.
// Starting a goroutine that returns immediately would not test that: it
// usually exits before the cleanup looks, and then a settle window of
// zero would pass too.
func TestCheckStaysSilentWhenTheGoroutineExits(t *testing.T) {
	f := &fakeT{}
	registerCheck(f, settleTimeout)

	release := make(chan struct{})
	started := make(chan struct{})
	go func() {
		close(started)
		<-release
	}()
	<-started

	// Well inside settleTimeout, so a correct settle always sees the exit.
	timer := time.AfterFunc(20*time.Millisecond, func() { close(release) })
	t.Cleanup(func() { timer.Stop() })

	f.runCleanups()

	require.False(t, f.failed, "a goroutine that exited during the settle window is not a leak")
	require.Empty(t, f.errs)
}

// TestCheckSkipsReportWhenTheTestAlreadyFailed pins the early return: a
// test that failed early is expected to leave its fixtures running, and
// the leak report would bury the real failure.
func TestCheckSkipsReportWhenTheTestAlreadyFailed(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	f := &fakeT{}
	registerCheck(f, 50*time.Millisecond)

	started := make(chan struct{})
	go func() {
		close(started)
		<-release
	}()
	<-started

	f.failed = true
	f.errs = nil
	f.runCleanups()

	require.Empty(t, f.errs, "the leak report must not bury an earlier failure")
}

// TestCheckPassesOnARealTestWhoseGoroutineExitsLate covers the exported
// Check, which is otherwise only reached through the packages that call
// it, and pins the settle window it hands on.
//
// The goroutine is still parked when this test's cleanups begin and exits
// during the window, so a Check that passed a zero timeout would report
// this very test as leaking. A goroutine that returns immediately would
// leave that argument unpinned, since settle's first poll would already
// find nothing.
//
// Using the real timeout is what keeps this honest, and it costs nothing
// on the passing path: settle returns as soon as the goroutine is gone.
func TestCheckPassesOnARealTestWhoseGoroutineExitsLate(t *testing.T) {
	Check(t)

	release := make(chan struct{})
	started := make(chan struct{})
	go func() {
		close(started)
		<-release
	}()
	<-started

	// No t.Cleanup to stop this timer. Cleanups run last registered
	// first, so one registered here would run before the check Check
	// registered above, cancel the release, and report this test as
	// leaking the goroutine it is about to free.
	time.AfterFunc(20*time.Millisecond, func() { close(release) })
}
