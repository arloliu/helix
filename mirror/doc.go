// Package mirror provides asynchronous mirror-write support for the Helix
// CQL client.
//
// Mirroring is a side-channel write feature used for seamless cluster
// migrations. After a primary write succeeds (at least one cluster ack),
// the helix client opt-in mirrors the write to a second helix dual-cluster
// pair via an [Engine]. The engine maintains a bounded in-memory queue,
// drains it with a worker pool, and applies each capture against the
// mirror destination.
//
// # Lifecycle
//
// The [Engine] is constructed by helix's WithMirror option and is started
// and stopped automatically by the helix CQLClient. Application code does
// not call [Engine.Start] or [Engine.Stop] directly; instead it uses the
// runtime control surface ([Engine.Enable], [Engine.Disable],
// [Engine.Enabled], [Engine.Stats]) returned by CQLClient.Mirror().
//
// # Backpressure
//
// Enqueue is non-blocking. When the queue is full, the capture is dropped
// and accounted in [Stats.Dropped]; a rate-limited warning is logged and
// the optional WithOnDrop callback is invoked. The hot path never stalls
// on the mirror queue.
//
// # Durability
//
// On its own the engine retries nothing: a failed mirror write is logged,
// counted in [Stats], and handed to the configured [ErrorHandler]. The helix
// client's WithMirrorReplayer option installs a handler that pushes the
// failed payload onto a replayer and, for the bundled replayer types, runs a
// [replay.Worker] that drains it back into the mirror destination.
package mirror
