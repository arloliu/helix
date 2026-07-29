package policy

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/helix/types"
)

// outboxCap bounds pending events per outbox. Transitions are rare
// state flips, so a backlog this deep only occurs when a custom emitter
// is wedged; beyond it, newest events are dropped and counted rather
// than growing memory without bound.
const outboxCap = 64

// emitterRef wraps the configured emitter so it can be swapped
// atomically: the emitter may be replaced (or installed) while a
// transition on another goroutine is already enqueuing or draining.
type emitterRef struct {
	em types.ClusterEventEmitter
}

// eventOutbox separates deciding event order from running the emitter.
// Transitions enqueue while holding their per-cluster state mutex, so
// append order equals transition order; a single goroutine at a time
// then delivers to the emitter after all policy locks are released. A
// slow emitter blocks only the goroutine currently draining, delaying its
// return until the emitter call completes, and can back up at most one
// in-flight batch plus outboxCap pending events. A reentrant emitter (one
// that calls back into enqueue/drain) enqueues and returns without
// blocking. A panicking emitter loses at most the panicking event and
// never leaves delivery stuck for later transitions. The zero value is
// usable and disabled.
type eventOutbox struct {
	mu       sync.Mutex // guards pending only; never held while emitting
	pending  []types.ClusterEvent
	draining atomic.Bool
	emitter  atomic.Pointer[emitterRef]
	dropped  atomic.Uint64 // overflow + panic-lost events
}

// setEmitter installs (or replaces) the emitter. nil disables emission.
func (o *eventOutbox) setEmitter(em types.ClusterEventEmitter) {
	o.emitter.Store(&emitterRef{em: em})
}

// enabled reports whether an emitter is installed.
func (o *eventOutbox) enabled() bool {
	ref := o.emitter.Load()
	return ref != nil && ref.em != nil
}

// enqueue appends ev to the outbox, stamping its timestamp, and reports
// whether it appended. A caller may use the return value to skip a
// following drain() call, though calling drain() regardless is also
// safe: a drain that appended nothing simply delivers another
// goroutine's pending events. MUST be called while the transitioning
// cluster's state mutex is held — that is what makes append order equal
// transition order. Overflow past outboxCap drops the newest event
// (counted).
func (o *eventOutbox) enqueue(ev types.ClusterEvent) bool {
	if !o.enabled() {
		return false
	}
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now()
	}
	o.mu.Lock()
	if len(o.pending) >= outboxCap {
		o.mu.Unlock()
		// Dropping instead of blocking or growing without bound is safe
		// here specifically: a full queue means outboxCap earlier
		// enqueuers already committed to calling drain, so this event's
		// loss does not strand any other pending event undelivered.
		o.dropped.Add(1)
		return false
	}
	o.pending = append(o.pending, ev)
	o.mu.Unlock()

	return true
}

// drain delivers pending events to the emitter in order. MUST be called
// after the state mutex is released, and after the transition's metrics
// and log side effects have already run: drain is the last side-effect
// step, so if a handler changes state from inside the callback it can
// never observe metrics or logs that contradict the final state.
// Whichever goroutine wins the compare-and-swap on the draining flag
// delivers events; the rest return immediately and trust the winner to
// pick up their events too, which keeps delivery order intact.
func (o *eventOutbox) drain() {
	for {
		if !o.draining.CompareAndSwap(false, true) {
			return // another goroutine is already delivering; it will pick up our events
		}
		o.drainOwned()
		// Re-check: an enqueue that arrived between our last empty batch
		// and clearing the flag would otherwise be stranded until the
		// next transition calls drain again.
		o.mu.Lock()
		empty := len(o.pending) == 0
		o.mu.Unlock()
		if empty {
			return
		}
	}
}

// drainOwned runs while holding the draining flag that the caller of
// drain set via a successful CompareAndSwap. The flag is released via
// defer so an emitter panic can never leave delivery stuck for later
// transitions.
func (o *eventOutbox) drainOwned() {
	defer o.draining.Store(false)
	for {
		o.mu.Lock()
		batch := o.pending
		o.pending = nil
		o.mu.Unlock()
		if len(batch) == 0 {
			return
		}
		ref := o.emitter.Load()
		if ref == nil || ref.em == nil {
			// Emitter removed mid-flight: these events have nowhere to
			// go; count them rather than dropping silently.
			o.dropped.Add(uint64(len(batch)))
			continue
		}
		for _, ev := range batch {
			o.safeEmit(ref.em, ev)
		}
	}
}

// safeEmit invokes the emitter, converting a panic into a counted drop
// so one bad emitter cannot kill a transition path or block delivery
// for later events. The panic value itself is not logged here; surfacing
// it is left to whatever consumes the drop count.
func (o *eventOutbox) safeEmit(em types.ClusterEventEmitter, ev types.ClusterEvent) {
	defer func() {
		if recover() != nil {
			o.dropped.Add(1)
		}
	}()
	em.EmitClusterEvent(ev)
}
