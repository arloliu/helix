package types

// ClusterEventDropReporter is an OPTIONAL interface a [ClusterEventEmitter]
// may satisfy to receive the events a producer had to drop before they
// reached the emitter.
//
// The built-in policies queue their transition events in a small outbox
// and deliver them after their state locks are released; when that outbox
// overflows, or the emitter is removed or panics mid-delivery, the lost
// events are counted and forwarded here so they join the dispatcher's
// dropped total ({prefix}_cluster_events_dropped_total) instead of
// vanishing.
//
// Call discipline: the producer never calls the method while holding a
// policy lock, and it never calls it from a read or write hot path;
// the count is forwarded by the goroutine that drains the outbox.
// A panicking reporter is recovered and the forwarded count is not retried.
// The client's dispatcher (registered via helix.WithOnClusterEvent)
// implements this interface.
type ClusterEventDropReporter interface {
	// NoteClusterEventsDropped adds n (always > 0) events that a producer
	// dropped before delivery to the dropped-event total.
	NoteClusterEventsDropped(n int)
}
