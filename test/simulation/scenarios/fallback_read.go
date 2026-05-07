package scenarios

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/arloliu/helix/test/simulation/types"
	htypes "github.com/arloliu/helix/types"
)

// FallbackReadDivergence verifies that FallbackRead bridges the replay-lag window:
// when a row exists only on Cluster A (because Cluster B missed the write),
// reads routed to B return not-found and FallbackRead silently retrieves the row
// from A, recording a divergence metric on the stale cluster.
//
// This scenario must run inside a strategy group that configures
// WithDefaultFallbackRead(true) and StickyRead(WithPreferredCluster(ClusterB)),
// otherwise reads will not be deterministically routed to B first.
type FallbackReadDivergence struct{}

func (s *FallbackReadDivergence) Name() string {
	return "fallback-read-divergence"
}

func (s *FallbackReadDivergence) Description() string {
	return "Verifies FallbackRead finds rows present only on Cluster A when reads are routed to Cluster B first"
}

func (s *FallbackReadDivergence) Run(ctx context.Context, env *types.Environment) error {
	env.Logger.Info("Starting FallbackReadDivergence scenario")

	// Phase 1: Wait for live workload traffic to establish a baseline.
	startCount := env.Tracker.TotalWrites()
	if err := waitUntil(ctx, 15*time.Second, func() bool {
		return env.Tracker.TotalWrites() >= startCount+20
	}); err != nil {
		return fmt.Errorf("baseline write volume not reached: %w", err)
	}

	// Phase 2: Write rows directly to Cluster A, bypassing the Helix client and
	// the replay queue.  Cluster B never sees these writes, so they exist only
	// on A — simulating a partial-write / replay-lag window.
	const numDivergentKeys = 15
	divergentKeys := make([]gocql.UUID, 0, numDivergentKeys)
	data := make([]byte, 32)

	env.Logger.Info("Writing divergent keys directly to Cluster A", "count", numDivergentKeys)
	for range numDivergentKeys {
		id := gocql.TimeUUID()
		if err := env.ChaosA.Query("INSERT INTO test_data (id, data) VALUES (?, ?)", id, data).Exec(); err != nil {
			return fmt.Errorf("direct write to Cluster A failed: %w", err)
		}
		divergentKeys = append(divergentKeys, id)
	}

	// Phase 3: Read the divergent keys via the Helix client.
	//
	// StickyRead(PreferredCluster(B)) routes each read to B first.
	// B returns not-found (it has no record of these rows).
	// WithDefaultFallbackRead(true) causes Helix to silently retry on A, which
	// has the row, and records IncReadDivergence(ClusterB) — the stale cluster.
	env.Logger.Info("Reading divergent keys via Helix client (FallbackRead active)")
	var readErrors int
	for _, id := range divergentKeys {
		var dest gocql.UUID
		if err := env.Client.Query("SELECT id FROM test_data WHERE id = ?", id).Scan(&dest); err != nil {
			readErrors++
			env.Logger.Warn("FallbackRead did not find divergent key", "id", id, "error", err)
		}
	}
	if readErrors > 0 {
		return fmt.Errorf("FallbackRead failed to recover %d/%d divergent keys from Cluster A", readErrors, numDivergentKeys)
	}

	// Phase 4: Assert that divergence was recorded on Cluster B (the stale cluster).
	divergenceB := env.Metrics.GetReadDivergence(htypes.ClusterB)
	env.Logger.Info("FallbackRead divergence metric", "cluster_b_divergence", divergenceB)
	if divergenceB == 0 {
		return fmt.Errorf("expected ReadDivergence[ClusterB] > 0 after reading %d A-only rows, got 0", numDivergentKeys)
	}

	// Phase 5: Confirm no false divergence on keys that exist on both clusters.
	// Read a key that was written normally (present on both clusters via dual write).
	normalKey := env.Tracker.RandomKey()
	if normalKey != (gocql.UUID{}) {
		prevDivergence := env.Metrics.GetReadDivergence(htypes.ClusterB)
		var dest gocql.UUID
		if err := env.Client.Query("SELECT id FROM test_data WHERE id = ?", normalKey).Scan(&dest); err != nil {
			env.Logger.Warn("Read of normally-written key failed (non-fatal)", "error", err)
		}
		afterDivergence := env.Metrics.GetReadDivergence(htypes.ClusterB)
		if afterDivergence > prevDivergence {
			env.Logger.Warn("Unexpected divergence on normally-written key", "before", prevDivergence, "after", afterDivergence)
		}
	}

	env.Logger.Info("FallbackReadDivergence scenario completed",
		"divergent_keys", numDivergentKeys,
		"divergence_b", divergenceB,
	)

	return nil
}
