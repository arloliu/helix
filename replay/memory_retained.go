package replay

import (
	"container/heap"
	"sync"
	"time"

	"github.com/arloliu/helix/types"
)

// retainedItem is a payload the memory worker keeps between attempts under
// RetryWhileRetained.
// It owns one capacity slot in the replayer from the moment it is dequeued
// until it succeeds or is dropped.
type retainedItem struct {
	gatedSince  time.Time // when the gate parked the item; zero while it is not parked
	payload     types.ReplayPayload
	attempts    int       // attempts made so far
	deadLetters int       // attempts the classifier marked dead-letter
	firstAt     time.Time // start of the retry window
	dueAt       time.Time // earliest time of the next attempt
}

// retainedQueue orders waiting items by dueAt.
type retainedQueue []*retainedItem

func (q retainedQueue) Len() int           { return len(q) }
func (q retainedQueue) Less(i, j int) bool { return q[i].dueAt.Before(q[j].dueAt) }
func (q retainedQueue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }

// Push appends an item; only *retainedItem is ever pushed.
func (q *retainedQueue) Push(x any) {
	if it, ok := x.(*retainedItem); ok {
		*q = append(*q, it)
	}
}

func (q *retainedQueue) Pop() any {
	old := *q
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	*q = old[:n-1]

	return it
}

// retainedScheduler holds the payloads waiting for their next attempt and
// wakes the scheduler goroutine when an earlier due time arrives.
type retainedScheduler struct {
	mu    sync.Mutex
	items retainedQueue
	wake  chan struct{}
}

func newRetainedScheduler() *retainedScheduler {
	return &retainedScheduler{wake: make(chan struct{}, 1)}
}

// push queues an item and nudges the scheduler in case the new item is due
// before whatever it is currently waiting for.
func (s *retainedScheduler) push(it *retainedItem) {
	s.mu.Lock()
	heap.Push(&s.items, it)
	s.mu.Unlock()

	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// next pops the earliest item if it is due.
// Otherwise it returns nil and how long to wait for the head item, or
// queued == false when nothing is waiting.
func (s *retainedScheduler) next(now time.Time) (it *retainedItem, wait time.Duration, queued bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.items) == 0 {
		return nil, 0, false
	}
	if head := s.items[0]; head.dueAt.After(now) {
		return nil, head.dueAt.Sub(now), true
	}
	it, _ = heap.Pop(&s.items).(*retainedItem)

	return it, 0, true
}

// drain removes and returns every waiting item.
func (s *retainedScheduler) drain() []*retainedItem {
	s.mu.Lock()
	defer s.mu.Unlock()

	items := make([]*retainedItem, 0, len(s.items))
	for len(s.items) > 0 {
		it, _ := heap.Pop(&s.items).(*retainedItem)
		items = append(items, it)
	}

	return items
}

// retained reports whether the backend runs under RetryWhileRetained.
func (b *memoryBackend) retained() bool {
	return b.config.RetryPolicy == RetryWhileRetained
}

// handleFirstAttemptRetained runs the first attempt inline on the dequeue
// loop; the item is only materialised when that attempt fails.
func (b *memoryBackend) handleFirstAttemptRetained(payload types.ReplayPayload) {
	if !b.config.allows(payload.TargetCluster) {
		// Park the payload without an attempt: it keeps its slot and its
		// retry window starts only when the first attempt runs.
		b.park(&retainedItem{payload: payload, firstAt: time.Now()})

		return
	}
	if err := b.runAttempt(payload, 1, b.config.MaxAttempts); err != nil {
		b.settleRetained(&retainedItem{payload: payload, attempts: 1, firstAt: time.Now()}, err)

		return
	}
	b.replayer.releaseSlot(payload.TargetCluster)
}

// attemptRetained runs one more attempt for a waiting item.
func (b *memoryBackend) attemptRetained(it *retainedItem) {
	defer b.retryWG.Done()
	defer func() { <-b.retrySem }()

	if !b.config.allows(it.payload.TargetCluster) {
		b.park(it)

		return
	}
	if !it.gatedSince.IsZero() {
		// The time spent parked is not the cluster's failure time: move the
		// window start forward by it so gating never expires the item.
		it.firstAt = it.firstAt.Add(time.Since(it.gatedSince))
		it.gatedSince = time.Time{}
	}
	it.attempts++
	b.config.observeAge(it.payload)
	if err := b.runAttempt(it.payload, it.attempts, b.config.MaxAttempts); err != nil {
		b.settleRetained(it, err)

		return
	}
	b.replayer.releaseSlot(it.payload.TargetCluster)
}

// park schedules an item the gate refused for another look after one
// PollInterval, counting no attempt against it.
func (b *memoryBackend) park(it *retainedItem) {
	now := time.Now()
	if it.gatedSince.IsZero() {
		it.gatedSince = now
	}
	it.dueAt = now.Add(b.config.PollInterval)
	b.sched.push(it)
}

// settleRetained decides what happens to an item after a failed attempt:
// drop it (poison budget exhausted, window expired, or shutdown) or queue
// its next attempt after the backoff delay.
func (b *memoryBackend) settleRetained(it *retainedItem, err error) {
	now := time.Now()
	if b.config.Classifier(err) == DispositionDeadLetter {
		it.deadLetters++
	}

	var reason string
	switch {
	case it.deadLetters >= b.config.MaxAttempts:
		reason = types.ReplayDropDeadLetter
	case now.Sub(it.firstAt) >= b.config.RetryWindow:
		reason = types.ReplayDropRetryWindowExpired
	case b.stopping():
		reason = types.ReplayDropShutdown
	}
	if reason != "" {
		b.dropRetained(it, err, reason)

		return
	}

	it.dueAt = now.Add(calculateBackoff(it.attempts, b.config.RetryDelay, b.config.MaxRetryDelay))
	b.sched.push(it)
}

// stopping reports whether Stop has been called.
func (b *memoryBackend) stopping() bool {
	select {
	case <-b.stopCh:
		return true
	default:
		return false
	}
}

// dropRetained drops an item and releases the slot it held.
func (b *memoryBackend) dropRetained(it *retainedItem, err error, reason string) {
	b.dropPayload(it.payload, err, b.config.MaxAttempts, reason)
	b.replayer.releaseSlot(it.payload.TargetCluster)
}

// runRetained dispatches due items to the bounded retry pool until the
// worker stops.
// An item popped but not dispatched because of shutdown goes back to the
// scheduler so drainRetained can report it.
func (b *memoryBackend) runRetained() {
	defer b.schedWG.Done()

	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()

	for {
		it, wait, queued := b.sched.next(time.Now())
		if it != nil {
			select {
			case b.retrySem <- struct{}{}:
			case <-b.stopCh:
				b.sched.push(it)

				return
			}
			b.retryWG.Add(1)
			go b.attemptRetained(it)

			continue
		}

		// A nil channel never fires, so an empty scheduler waits for a
		// push only.
		var due <-chan time.Time
		if queued {
			timer.Reset(wait)
			due = timer.C
		}
		select {
		case <-b.stopCh:
			return
		case <-b.sched.wake:
			timer.Stop()
		case <-due:
		}
	}
}

// drainRetained drops every item still waiting for an attempt.
// Called after the scheduler and all in-flight attempts have finished.
func (b *memoryBackend) drainRetained() {
	for _, it := range b.sched.drain() {
		b.dropRetained(it, nil, types.ReplayDropShutdown)
	}
}
