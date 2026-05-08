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
// # Durability (Phase 2)
//
// Phase 1 of v1.4.0 ships in-memory workers only; failed mirror writes are
// logged and counted but not retried. Phase 2 wires the engine to a
// [types.Replayer] so failed mirror writes are durably retried.
package mirror
