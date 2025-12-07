// Package topology provides cluster topology monitoring for drain mode support.
//
// Helix uses a NATS Key-Value store to broadcast topology constraints to all
// connected clients. This enables operations teams to gracefully drain traffic
// from a cluster before maintenance (patching, scaling, upgrades) without
// causing client-side errors.
//
// # Overview
//
// The topology package provides implementations of the [helix.TopologyWatcher]
// and [helix.TopologyOperator] interfaces:
//   - [helix.TopologyWatcher]: Monitors external signals and emits [helix.TopologyUpdate]
//     events when cluster availability changes.
//   - [helix.TopologyOperator]: Allows setting drain states programmatically.
//
// # NATS Topology
//
// [NATS] watches a NATS KV bucket for drain mode configuration:
//
//	nc, _ := nats.Connect("nats://localhost:4222")
//	js, _ := jetstream.New(nc)
//	kv, _ := js.KeyValue(ctx, "helix-config")
//
//	watcher, _ := topology.NewNATS(kv,
//	    topology.WithKey("topology.drain"),  // custom key
//	)
//
//	client, _ := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithTopologyWatcher(watcher),
//	)
//
// # Drain Configuration Format
//
// The NATS KV value is a JSON object specifying which clusters to drain:
//
//	{
//	    "drain": ["B"],
//	    "reason": "OS Patching"
//	}
//
// Valid drain values are "A", "B", or both. When a cluster is in the drain list,
// Helix clients will:
//   - Stop sending writes to the drained cluster (enqueue for replay instead)
//   - Force read preference away from the drained cluster
//   - Disable failover attempts to the drained cluster
//
// # Lifecycle
//
// Drain mode requires explicit operator actions:
//   - Start maintenance: PUT the drain configuration to NATS KV
//   - End maintenance: DELETE the key (or PUT with empty drain list)
//
// There is no automatic expiry. This is intentional to prevent race conditions
// where clients resume traffic while maintenance is still in progress.
//
// # Local Topology
//
// [Local] provides an in-memory implementation for testing. It implements both
// [helix.TopologyWatcher] and [helix.TopologyOperator]:
//
//	local := topology.NewLocal()
//	_ = local.SetDrain(ctx, helix.ClusterB, true, "maintenance")  // Simulate drain
//
//	// Later...
//	_ = local.SetDrain(ctx, helix.ClusterB, false, "")  // Clear drain mode
package topology
