package leak

import (
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
