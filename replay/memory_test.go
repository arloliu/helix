package replay

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

func TestMemoryReplayerCheckedInvalidOptionsReturnJoinedErrors(t *testing.T) {
	replayer, err := NewMemoryReplayerChecked(
		WithQueueCapacity(0),
		WithMemoryHighPriorityRatio(-1),
	)

	require.Nil(t, replayer)
	require.Error(t, err)
	require.True(t, types.IsOptionError(err))

	var optionErr *types.OptionError
	require.True(t, errors.As(err, &optionErr))
	require.Equal(t, memoryReplayerComponent, optionErr.Component)
	require.Contains(t, err.Error(), "WithQueueCapacity")
	require.Contains(t, err.Error(), "WithMemoryHighPriorityRatio")
}

func TestMemoryReplayerCheckedValidOptions(t *testing.T) {
	replayer, err := NewMemoryReplayerChecked(
		WithQueueCapacity(8),
		WithMemoryHighPriorityRatio(0),
		WithMemoryStrictPriority(true),
	)
	require.NoError(t, err)
	require.NotNil(t, replayer)
	t.Cleanup(replayer.Close)

	require.Equal(t, 8, replayer.Cap())
	require.True(t, replayer.strictPriority)
}

func TestMemoryReplayerEnqueue(t *testing.T) {
	replayer := NewMemoryReplayer(WithQueueCapacity(10))
	defer replayer.Close()

	payload := types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT INTO test (id) VALUES (?)",
		Args:          []any{1},
		Timestamp:     time.Now().UnixMicro(),
		Priority:      types.PriorityHigh,
	}

	err := replayer.Enqueue(context.Background(), payload)
	require.NoError(t, err)
	require.Equal(t, 1, replayer.Len())
}

func TestMemoryReplayerDequeue(t *testing.T) {
	replayer := NewMemoryReplayer()
	defer replayer.Close()

	payload := types.ReplayPayload{
		TargetCluster: types.ClusterB,
		Query:         "UPDATE test SET val = ? WHERE id = ?",
		Args:          []any{"test", 1},
		Timestamp:     12345,
		Priority:      types.PriorityLow,
	}

	err := replayer.Enqueue(context.Background(), payload)
	require.NoError(t, err)

	dequeued, ok := replayer.TryDequeue()
	require.True(t, ok)
	require.Equal(t, payload.TargetCluster, dequeued.TargetCluster)
	require.Equal(t, payload.Query, dequeued.Query)
	require.Equal(t, payload.Timestamp, dequeued.Timestamp)
	require.Equal(t, payload.Priority, dequeued.Priority)
	require.Equal(t, 0, replayer.Len())
}

func TestMemoryReplayerQueueFull(t *testing.T) {
	// Capacity is shared globally across both priority queues.
	replayer := NewMemoryReplayer(WithQueueCapacity(4))
	defer replayer.Close()

	highPayload := types.ReplayPayload{TargetCluster: types.ClusterA, Query: "SELECT 1", Priority: types.PriorityHigh}
	lowPayload := types.ReplayPayload{TargetCluster: types.ClusterA, Query: "SELECT 2", Priority: types.PriorityLow}

	// Fill the shared capacity entirely with high-priority items.
	require.NoError(t, replayer.Enqueue(context.Background(), highPayload))
	require.NoError(t, replayer.Enqueue(context.Background(), highPayload))
	require.NoError(t, replayer.Enqueue(context.Background(), highPayload))
	require.NoError(t, replayer.Enqueue(context.Background(), highPayload))

	// Further enqueues fail regardless of priority.
	err := replayer.Enqueue(context.Background(), highPayload)
	require.ErrorIs(t, err, types.ErrReplayQueueFull)

	err = replayer.Enqueue(context.Background(), lowPayload)
	require.ErrorIs(t, err, types.ErrReplayQueueFull)
}

func TestMemoryReplayerContextCancellation(t *testing.T) {
	replayer := NewMemoryReplayer(WithQueueCapacity(1))
	defer replayer.Close()

	// Fill the shared queue
	payload := types.ReplayPayload{TargetCluster: types.ClusterA, Query: "SELECT 1", Priority: types.PriorityHigh}
	require.NoError(t, replayer.Enqueue(context.Background(), payload))

	// Cancel context and try to enqueue (queue is full)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := replayer.Enqueue(ctx, payload)
	require.ErrorIs(t, err, context.Canceled)
}

func TestMemoryReplayerDequeueBlocking(t *testing.T) {
	replayer := NewMemoryReplayer()
	defer replayer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Dequeue should block and return false on context timeout
	_, ok := replayer.Dequeue(ctx)
	require.False(t, ok)
}

func TestMemoryReplayerTryDequeueEmpty(t *testing.T) {
	replayer := NewMemoryReplayer()
	defer replayer.Close()

	_, ok := replayer.TryDequeue()
	require.False(t, ok)
}

func TestMemoryReplayerDrainAll(t *testing.T) {
	replayer := NewMemoryReplayer()
	defer replayer.Close()

	// Enqueue multiple items with mixed priorities
	for i := range 5 {
		priority := types.PriorityHigh
		if i%2 == 1 {
			priority = types.PriorityLow
		}
		payload := types.ReplayPayload{
			TargetCluster: types.ClusterA,
			Query:         "SELECT ?",
			Args:          []any{i},
			Timestamp:     int64(i),
			Priority:      priority,
		}
		require.NoError(t, replayer.Enqueue(context.Background(), payload))
	}

	require.Equal(t, 5, replayer.Len())

	// Drain all - high priority first, then low
	payloads := replayer.DrainAll()
	require.Len(t, payloads, 5)
	require.Equal(t, 0, replayer.Len())

	// Verify high priority items come first (timestamps 0, 2, 4)
	// Then low priority items (timestamps 1, 3)
	highCount := 0
	for i, p := range payloads {
		if i < 3 {
			require.Equal(t, types.PriorityHigh, p.Priority, "First 3 should be high priority")
			highCount++
		} else {
			require.Equal(t, types.PriorityLow, p.Priority, "Last 2 should be low priority")
		}
	}
	require.Equal(t, 3, highCount)
}

func TestMemoryReplayerClose(t *testing.T) {
	replayer := NewMemoryReplayer()

	payload := types.ReplayPayload{TargetCluster: types.ClusterA, Query: "SELECT 1"}
	require.NoError(t, replayer.Enqueue(context.Background(), payload))

	replayer.Close()

	// Enqueue after close should fail
	err := replayer.Enqueue(context.Background(), payload)
	require.ErrorIs(t, err, types.ErrSessionClosed)
}

func TestMemoryReplayerCapacity(t *testing.T) {
	replayer := NewMemoryReplayer(WithQueueCapacity(100))
	defer replayer.Close()

	require.Equal(t, 100, replayer.Cap())
}

func TestMemoryReplayerOddCapacityPreserved(t *testing.T) {
	replayer := NewMemoryReplayer(WithQueueCapacity(3))
	defer replayer.Close()

	require.Equal(t, 3, replayer.Cap())

	payload := types.ReplayPayload{TargetCluster: types.ClusterA, Query: "SELECT 1", Priority: types.PriorityHigh}
	require.NoError(t, replayer.Enqueue(context.Background(), payload))
	require.NoError(t, replayer.Enqueue(context.Background(), payload))
	require.NoError(t, replayer.Enqueue(context.Background(), payload))
	require.Equal(t, 3, replayer.Len())
	require.ErrorIs(t, replayer.Enqueue(context.Background(), payload), types.ErrReplayQueueFull)
}

func TestMemoryReplayerPriorityRouting(t *testing.T) {
	replayer := NewMemoryReplayer(WithQueueCapacity(20))
	defer replayer.Close()

	// Enqueue high and low priority messages
	high := types.ReplayPayload{TargetCluster: types.ClusterA, Query: "HIGH", Priority: types.PriorityHigh}
	low := types.ReplayPayload{TargetCluster: types.ClusterA, Query: "LOW", Priority: types.PriorityLow}

	require.NoError(t, replayer.Enqueue(context.Background(), high))
	require.NoError(t, replayer.Enqueue(context.Background(), low))

	require.Equal(t, 1, replayer.HighLen())
	require.Equal(t, 1, replayer.LowLen())
	require.Equal(t, 2, replayer.Len())
}

func TestMemoryReplayerStrictPriority(t *testing.T) {
	replayer := NewMemoryReplayer(
		WithQueueCapacity(20),
		WithMemoryStrictPriority(true),
	)
	defer replayer.Close()

	// Enqueue: low, high, high
	low := types.ReplayPayload{TargetCluster: types.ClusterA, Query: "LOW", Priority: types.PriorityLow}
	high1 := types.ReplayPayload{TargetCluster: types.ClusterA, Query: "HIGH1", Priority: types.PriorityHigh}
	high2 := types.ReplayPayload{TargetCluster: types.ClusterA, Query: "HIGH2", Priority: types.PriorityHigh}

	require.NoError(t, replayer.Enqueue(context.Background(), low))
	require.NoError(t, replayer.Enqueue(context.Background(), high1))
	require.NoError(t, replayer.Enqueue(context.Background(), high2))

	// In strict mode, high priority should be drained first
	p1, ok := replayer.TryDequeue()
	require.True(t, ok)
	require.Equal(t, "HIGH1", p1.Query)

	p2, ok := replayer.TryDequeue()
	require.True(t, ok)
	require.Equal(t, "HIGH2", p2.Query)

	// Now low priority
	p3, ok := replayer.TryDequeue()
	require.True(t, ok)
	require.Equal(t, "LOW", p3.Query)

	// Empty
	_, ok = replayer.TryDequeue()
	require.False(t, ok)
}

func TestMemoryReplayerRatioBasedFairness(t *testing.T) {
	// Ratio of 2:1 - process 2 high before 1 low
	replayer := NewMemoryReplayer(
		WithQueueCapacity(20),
		WithMemoryHighPriorityRatio(2),
	)
	defer replayer.Close()

	// Enqueue 3 high and 3 low
	for i := range 3 {
		high := types.ReplayPayload{TargetCluster: types.ClusterA, Query: "HIGH", Priority: types.PriorityHigh, Timestamp: int64(i)}
		low := types.ReplayPayload{TargetCluster: types.ClusterA, Query: "LOW", Priority: types.PriorityLow, Timestamp: int64(i)}
		require.NoError(t, replayer.Enqueue(context.Background(), high))
		require.NoError(t, replayer.Enqueue(context.Background(), low))
	}

	// With 2:1 ratio, expect: HIGH, HIGH, LOW, HIGH, LOW, LOW
	results := make([]string, 0, 6)
	for range 6 {
		p, ok := replayer.TryDequeue()
		require.True(t, ok)
		results = append(results, p.Query)
	}

	// Count pattern: should have at least some interleaving
	highCount := 0
	lowCount := 0
	for _, r := range results {
		if r == "HIGH" {
			highCount++
		} else {
			lowCount++
		}
	}
	require.Equal(t, 3, highCount)
	require.Equal(t, 3, lowCount)

	// Verify that low is not starved - should appear before position 4
	firstLowIdx := -1
	for i, r := range results {
		if r == "LOW" {
			firstLowIdx = i
			break
		}
	}
	require.LessOrEqual(t, firstLowIdx, 3, "Low priority should not be starved beyond ratio")
}

func TestMemoryReplayerHighLenLowLen(t *testing.T) {
	replayer := NewMemoryReplayer(WithQueueCapacity(100))
	defer replayer.Close()

	for i := range 5 {
		p := types.ReplayPayload{TargetCluster: types.ClusterA, Query: "HIGH", Priority: types.PriorityHigh, Timestamp: int64(i)}
		require.NoError(t, replayer.Enqueue(context.Background(), p))
	}
	for i := range 3 {
		p := types.ReplayPayload{TargetCluster: types.ClusterA, Query: "LOW", Priority: types.PriorityLow, Timestamp: int64(i)}
		require.NoError(t, replayer.Enqueue(context.Background(), p))
	}

	require.Equal(t, 5, replayer.HighLen())
	require.Equal(t, 3, replayer.LowLen())
	require.Equal(t, 8, replayer.Len())
}

func TestMemoryReplayerAlternatesClusters(t *testing.T) {
	replayer := NewMemoryReplayer(WithQueueCapacity(20))
	defer replayer.Close()

	// A backlog for cluster A is queued before anything for cluster B.
	for i := range 3 {
		p := types.ReplayPayload{TargetCluster: types.ClusterA, Query: "A", Timestamp: int64(i + 1)}
		require.NoError(t, replayer.Enqueue(context.Background(), p))
	}
	for i := range 3 {
		p := types.ReplayPayload{TargetCluster: types.ClusterB, Query: "B", Timestamp: int64(i + 1)}
		require.NoError(t, replayer.Enqueue(context.Background(), p))
	}

	// Dequeue must alternate between clusters so A's backlog cannot
	// delay B's payloads.
	got := make([]types.ClusterID, 0, 6)
	for range 6 {
		p, ok := replayer.TryDequeue()
		require.True(t, ok)
		got = append(got, p.TargetCluster)
	}
	want := []types.ClusterID{
		types.ClusterA, types.ClusterB, types.ClusterA,
		types.ClusterB, types.ClusterA, types.ClusterB,
	}
	require.Equal(t, want, got)

	// Each cluster's queue stays FIFO.
	require.Equal(t, 0, replayer.Len())
	require.Equal(t, 0, replayer.PendingByCluster(types.ClusterA))
	require.Equal(t, 0, replayer.PendingByCluster(types.ClusterB))
}

func TestMemoryReplayerClusterFIFOWithinPriority(t *testing.T) {
	replayer := NewMemoryReplayer(WithQueueCapacity(20))
	defer replayer.Close()

	for i := range 3 {
		p := types.ReplayPayload{TargetCluster: types.ClusterA, Query: "A", Timestamp: int64(i + 1)}
		require.NoError(t, replayer.Enqueue(context.Background(), p))
	}
	p := types.ReplayPayload{TargetCluster: types.ClusterB, Query: "B", Timestamp: 1}
	require.NoError(t, replayer.Enqueue(context.Background(), p))

	// With B exhausted after one dequeue, A's payloads continue in order.
	var timestamps []int64
	for range 4 {
		p, ok := replayer.TryDequeue()
		require.True(t, ok)
		if p.TargetCluster == types.ClusterA {
			timestamps = append(timestamps, p.Timestamp)
		}
	}
	require.Equal(t, []int64{1, 2, 3}, timestamps)
}

// TestMemoryReplayerDequeueTryPathSucceeds covers Dequeue's own non-blocking
// try-path success branch (memory.go:320-323): a payload already waiting is
// returned without ever reaching the blocking select.
// This is not one of S3's six buckets — every other test that dequeues a
// ready payload calls TryDequeue directly — but it is the last uncovered
// branch inside Dequeue itself, so it is closed alongside them.
func TestMemoryReplayerDequeueTryPathSucceeds(t *testing.T) {
	replayer := NewMemoryReplayer()
	defer replayer.Close()

	payload := types.ReplayPayload{TargetCluster: types.ClusterA, Query: "SELECT 1", Priority: types.PriorityHigh}
	require.NoError(t, replayer.Enqueue(context.Background(), payload))

	dequeued, ok := replayer.Dequeue(context.Background())
	require.True(t, ok)
	require.Equal(t, payload, dequeued)
	require.Equal(t, 0, replayer.Len())
}

// dequeueOrFail runs Dequeue on its own goroutine and returns its result,
// failing the test if the call does not return promptly.
//
// Running it off the test goroutine is what keeps a regression legible:
// the guards below exist so that Dequeue returns early instead of parking,
// and a direct call that parked anyway would hang the whole package to its
// timeout rather than failing here.
// A bounded context would not do — Dequeue returns false off the blocking
// select's own ctx.Done() arm, so the assertions would pass with the guard
// gone.
func dequeueOrFail(t *testing.T, replayer *MemoryReplayer) (types.ReplayPayload, bool) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	type dequeueResult struct {
		payload types.ReplayPayload
		ok      bool
	}
	resultCh := make(chan dequeueResult, 1)
	go func() {
		payload, ok := replayer.Dequeue(ctx)
		resultCh <- dequeueResult{payload, ok}
	}()

	select {
	case got := <-resultCh:
		return got.payload, got.ok
	case <-time.After(time.Second):
		t.Fatal("Dequeue parked where it should have returned early")

		return types.ReplayPayload{}, false
	}
}

// TestMemoryReplayerDequeueUninitialized closes S3's uninitialized guard:
// a zero-value MemoryReplayer (never built via NewMemoryReplayer) must
// return false rather than nil-deref on its unset channels.
func TestMemoryReplayerDequeueUninitialized(t *testing.T) {
	replayer := &MemoryReplayer{}

	payload, ok := dequeueOrFail(t, replayer)
	require.False(t, ok)
	require.Equal(t, types.ReplayPayload{}, payload)
}

// TestMemoryReplayerDequeueContextAlreadyCancelled closes S3's pre-check
// select: a context cancelled before Dequeue is ever called must be caught
// before any dequeue attempt, not just by the later blocking select.
//
// A payload is enqueued first so the two guards are distinguishable: an
// already-cancelled context whose queue is empty would also be caught by
// the blocking select's own ctx.Done() arm, so that alone would not prove
// the pre-check runs.
// With a payload waiting, only the pre-check honours cancellation ahead of
// a successful dequeue — without it, the try-path below would return the
// payload instead.
func TestMemoryReplayerDequeueContextAlreadyCancelled(t *testing.T) {
	replayer := NewMemoryReplayer()
	defer replayer.Close()

	waiting := types.ReplayPayload{TargetCluster: types.ClusterA, Query: "SELECT 1", Priority: types.PriorityHigh}
	require.NoError(t, replayer.Enqueue(context.Background(), waiting))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	payload, ok := replayer.Dequeue(ctx)
	require.False(t, ok)
	require.Equal(t, types.ReplayPayload{}, payload)
	require.Equal(t, 1, replayer.Len(), "the pre-check must not consume the waiting payload")
}

// TestMemoryReplayerDequeueClosedAndDrained closes S3's closed-and-drained
// exit: once Close has been called and nothing is pending, Dequeue must
// return immediately rather than parking in the blocking select forever.
func TestMemoryReplayerDequeueClosedAndDrained(t *testing.T) {
	replayer := NewMemoryReplayer()
	replayer.Close()

	payload, ok := dequeueOrFail(t, replayer)
	require.False(t, ok)
	require.Equal(t, types.ReplayPayload{}, payload)
}

// dequeueParkedFrame is the frame runtime.Stack prints for a goroutine
// inside Dequeue.
const dequeueParkedFrame = "(*MemoryReplayer).Dequeue("

// dequeueParkedState is the state runtime.Stack prints in a goroutine's
// header when it is parked on a select.
// It is matched as a prefix because the runtime appends how long the
// goroutine has been waiting once that passes a minute ("[select, 2
// minutes]").
const dequeueParkedState = "[select"

// goroutineDump returns a complete dump of every goroutine's stack.
// The buffer doubles until the dump fits, the way internal/leak does
// it: a truncated dump could drop the very block being looked for.
func goroutineDump() string {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return string(buf[:n])
		}
		buf = make([]byte, 2*len(buf))
	}
}

// dequeueParked reports whether the dump holds a goroutine parked on a
// select inside Dequeue.
//
// Matching the state and the frame together, rather than "Dequeue"
// anywhere in the dump, is what proves the goroutine reached the blocking
// select.
// Dequeue's other select carries a default case, so it never parks; a
// goroutine still in the non-blocking prologue, or one that has already
// returned, cannot satisfy both halves.
// Pinning the pair instead of a file:line also survives edits to memory.go
// above the select.
//
// A stack dump cannot identify which goroutine is which, so this answers
// "some goroutine is parked there", not "the caller's is".
// requireNoDequeueParked closes that gap from the other side.
func dequeueParked(dump string) bool {
	for g := range strings.SplitSeq(dump, "\n\n") {
		header, body, ok := strings.Cut(g, "\n")
		if !ok {
			continue
		}
		if strings.Contains(header, dequeueParkedState) && strings.Contains(body, dequeueParkedFrame) {
			return true
		}
	}

	return false
}

// requireNoDequeueParked fails unless no goroutine is parked in Dequeue
// right now.
//
// Call it before starting the goroutine a test intends to park.
// Without it, a Dequeue left parked by an earlier failing test would
// satisfy waitDequeueParked instantly, the test's own goroutine would take
// the non-blocking try-path, and the arm subtests would pass while
// exercising nothing — the one way they can go inert without saying so.
func requireNoDequeueParked(t *testing.T) {
	t.Helper()

	require.False(t, dequeueParked(goroutineDump()),
		"a Dequeue from an earlier test is still parked, so waitDequeueParked cannot tell it from this test's own")
}

// waitDequeueParked polls until a goroutine is parked in Dequeue's blocking
// select.
// A goroutine's existence (here, its parked state) has nothing to
// subscribe to, which is the documented exception in rule 300-testing for
// using require.Eventually instead of an event-driven collector.
func waitDequeueParked(t *testing.T) {
	t.Helper()

	require.Eventually(t, func() bool {
		return dequeueParked(goroutineDump())
	}, time.Second, 5*time.Millisecond,
		"no goroutine parked in Dequeue's blocking select")
}

// TestMemoryReplayerDequeueBlockingArms closes S3's four payload arms of
// the blocking select: one subtest per (cluster, priority) pair.
//
// Each subtest starts Dequeue against an empty replayer, confirms via
// runtime.Stack that the goroutine actually reached the blocking select
// (not the earlier non-blocking try-path — see dequeueParked), then
// enqueues exactly one payload into exactly one (cluster, priority) slot.
// Enqueuing into only one slot matters: Go's select picks randomly among
// ready arms, so a second ready channel would destroy attribution of which
// arm actually fired.
//
// Each subtest asserts both the returned payload and the bookkeeping that
// arm performs (noteDequeuedLocked): nextQueue rotates to the other
// cluster, and the served cluster's highProcessed counter increments (high)
// or resets to zero (low).
func TestMemoryReplayerDequeueBlockingArms(t *testing.T) {
	tests := []struct {
		name     string
		cluster  types.ClusterID
		priority types.PriorityLevel
		wantIdx  int
		wantHigh bool
	}{
		{name: "cluster A high", cluster: types.ClusterA, priority: types.PriorityHigh, wantIdx: 0, wantHigh: true},
		{name: "cluster A low", cluster: types.ClusterA, priority: types.PriorityLow, wantIdx: 0, wantHigh: false},
		{name: "cluster B high", cluster: types.ClusterB, priority: types.PriorityHigh, wantIdx: 1, wantHigh: true},
		{name: "cluster B low", cluster: types.ClusterB, priority: types.PriorityLow, wantIdx: 1, wantHigh: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replayer := NewMemoryReplayer()
			t.Cleanup(replayer.Close)

			// Seed a non-zero, non-one counter before the goroutine starts,
			// so the assertion below can tell "incremented" from "untouched"
			// and "reset to zero" from "was already zero" — asserting 0/1
			// against a fresh replayer would pass whether or not the arm
			// touched the counter at all.
			// Safe without synchronization: the write happens-before the
			// goroutine below, which is the only other access, and it
			// takes m.mu before reading the field.
			const seededHighProcessed = 3
			replayer.queues[tt.wantIdx].highProcessed = seededHighProcessed

			requireNoDequeueParked(t)

			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)

			type dequeueResult struct {
				payload types.ReplayPayload
				ok      bool
			}
			resultCh := make(chan dequeueResult, 1)
			go func() {
				payload, ok := replayer.Dequeue(ctx)
				resultCh <- dequeueResult{payload, ok}
			}()

			waitDequeueParked(t)

			payload := types.ReplayPayload{TargetCluster: tt.cluster, Query: tt.name, Priority: tt.priority, Timestamp: 1}
			require.NoError(t, replayer.Enqueue(context.Background(), payload))

			var got dequeueResult
			select {
			case got = <-resultCh:
			case <-time.After(time.Second):
				t.Fatal("Dequeue did not return after the matching payload was enqueued")
			}

			require.True(t, got.ok)
			require.Equal(t, payload, got.payload)

			// Bookkeeping the blocking arm performs, per noteDequeuedLocked.
			require.Equal(t, 1-tt.wantIdx, replayer.nextQueue, "nextQueue should rotate away from the served cluster")
			if tt.wantHigh {
				require.Equal(t, seededHighProcessed+1, replayer.queues[tt.wantIdx].highProcessed, "high arm should increment highProcessed")
			} else {
				require.Equal(t, 0, replayer.queues[tt.wantIdx].highProcessed, "low arm should reset highProcessed to zero")
			}
		})
	}
}

// TestMemoryReplayerDequeueCloseWhileBlockedWakes closes S8: a Dequeue
// already parked in the blocking select must wake once Close runs, not
// only when a payload arrives or its own context is cancelled.
//
// The Dequeue doc comment promises "Returns false if the context is
// cancelled or the replayer is closed and empty" for every caller, not
// only one that starts after Close — a caller already parked when Close
// runs used to hang until its context expired.
// See the reported finding.
//
// Drop the done arm and this test times out its one-second wait: with a
// flag-only Close, nothing touches the blocking select and the parked
// goroutine cannot wake at all.
// A `case <-m.done:` arm that unconditionally returns false would pass
// this test — TestMemoryReplayerDequeueClosePayloadRacePrefersPayload is
// what catches that shape instead.
func TestMemoryReplayerDequeueCloseWhileBlockedWakes(t *testing.T) {
	replayer := NewMemoryReplayer()

	requireNoDequeueParked(t)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	type dequeueResult struct {
		payload types.ReplayPayload
		ok      bool
	}
	resultCh := make(chan dequeueResult, 1)
	go func() {
		payload, ok := replayer.Dequeue(ctx)
		resultCh <- dequeueResult{payload, ok}
	}()

	waitDequeueParked(t)

	replayer.Close()

	select {
	case got := <-resultCh:
		require.False(t, got.ok)
		require.Equal(t, types.ReplayPayload{}, got.payload)
	case <-time.After(time.Second):
		// Unblock the parked goroutine before failing so it cannot outlive
		// this test (replay/leak_main_test.go fails the package on a
		// surviving goroutine); Dequeue returns on ctx cancellation and the
		// buffered resultCh send cannot then block.
		cancel()
		t.Fatal("Dequeue did not wake within one second of Close")
	}
}

// TestMemoryReplayerDequeueClosePayloadRacePrefersPayload is S8's trap
// test: once the done arm exists, it must not treat "done is closed" as
// "nothing is left". requeue can land a payload after Close (it does not
// check the closed flag), and Go's select picks a ready arm at random, so
// a bare `case <-m.done: return false` loses an already-waiting payload
// roughly half the time it fires.
//
// m.mu is what turns the race deterministic instead of a coin flip: the
// done arm's own try path (tryDequeueWithPriority) must take mu before
// scanning the queues, and nothing else reachable from here does.
// Holding mu across Close (which commits the parked select to the done
// arm, since the payload channels are still empty at that instant) and
// then requeue (which therefore buffers instead of handing off directly,
// because the done arm's receiver already claimed the wake) guarantees
// the payload is sitting in the channel by the time the done arm's try
// path is allowed to run.
// A correct implementation returns the payload on every run; a bare
// `return false` fails on every run — no repeat-loop is needed to catch
// it, and one would misrepresent this as probabilistic when it isn't.
func TestMemoryReplayerDequeueClosePayloadRacePrefersPayload(t *testing.T) {
	replayer := NewMemoryReplayer()
	require.NotNil(t, replayer.done, "the constructor must build the done channel Close closes")

	requireNoDequeueParked(t)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	type dequeueResult struct {
		payload types.ReplayPayload
		ok      bool
	}
	resultCh := make(chan dequeueResult, 1)
	go func() {
		payload, ok := replayer.Dequeue(ctx)
		resultCh <- dequeueResult{payload, ok}
	}()

	waitDequeueParked(t)

	payload := types.ReplayPayload{TargetCluster: types.ClusterA, Query: "RACE", Priority: types.PriorityHigh, Timestamp: 1}

	replayer.mu.Lock()
	replayer.Close()                      // closes done; the parked select commits to the done arm here, while it is the only ready case
	requeued := replayer.requeue(payload) // buffers: the done arm's receiver already claimed the wake, so this cannot hand off directly
	replayer.mu.Unlock()                  // release the done arm's try path to look for the buffered payload

	// Assert after the unlock, never inside it: require fails through
	// runtime.Goexit, which runs deferred calls only, so an assertion here
	// would strand mu locked and leave the consumer blocked on it forever.
	// It would be parked on the mutex rather than the select, so cancelling
	// the context could not free it either, and the package would report a
	// goroutine leak instead of the real cause.
	require.True(t, requeued, "requeue must buffer the payload for the done arm to find")

	var got dequeueResult
	select {
	case got = <-resultCh:
	case <-time.After(time.Second):
		t.Fatal("Dequeue did not return after Close while a payload was held for it under mu")
	}

	require.True(t, got.ok, "a payload was buffered before the done arm's try path ran; it must not report drained")
	require.Equal(t, payload, got.payload)
}

// TestMemoryReplayerCloseIdempotent covers Close's doc promise that it is
// safe to call multiple times, including on a replayer that was never
// built through a constructor (done is nil).
//
// Sequential double-Close alone would pass even against a naive
// `if !m.closed.Load() { m.closed.Store(true); close(m.done) }`: nothing
// forces a second call to interleave inside the Load-then-Store window.
// The concurrent subtest is what actually exercises the CompareAndSwap
// the design calls for, so run this test with -race.
func TestMemoryReplayerCloseIdempotent(t *testing.T) {
	t.Run("sequential on a constructed replayer", func(t *testing.T) {
		replayer := NewMemoryReplayer()
		require.NotPanics(t, func() {
			replayer.Close()
			replayer.Close()
			replayer.Close()
		})
	})

	t.Run("zero value never constructed", func(t *testing.T) {
		replayer := &MemoryReplayer{}
		require.NotPanics(t, func() {
			replayer.Close()
			replayer.Close()
		})
	})

	t.Run("concurrent close races do not panic", func(t *testing.T) {
		// Each round gets a fresh replayer whose closers are released
		// together, so they land inside each other's Load-then-Store
		// window instead of arriving staggered enough that the first is
		// already done.
		// One round is not enough either: that window is a few
		// instructions wide, so a naive guard survives a single round
		// almost every time.
		// A double close panics on the goroutine that loses, taking the
		// binary down, so the failure is loud rather than an assertion.
		const (
			rounds  = 500
			closers = 8
		)

		for range rounds {
			// Capacity 1 keeps each round's four channels small enough
			// that 500 of them cost nothing.
			replayer := NewMemoryReplayer(WithQueueCapacity(1))

			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(closers)
			for range closers {
				go func() {
					defer wg.Done()
					<-start
					replayer.Close()
				}()
			}

			close(start)
			wg.Wait()
		}
	})
}
