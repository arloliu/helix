package types

import (
	"context"
	"log/slog"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/simulation/chaos"
	"github.com/arloliu/helix/test/simulation/workload"
	"github.com/arloliu/helix/test/testutil"
)

// Environment holds the shared resources for the simulation.
type Environment struct {
	Client      *helix.CQLClient
	ChaosA      *chaos.Session
	ChaosB      *chaos.Session
	Tracker     *workload.WriteTracker
	Stats       *workload.WorkloadStats
	Logger      *slog.Logger
	Metrics     *testutil.TestMetricsCollector
	MemReplayer *replay.MemoryReplayer
	// ReplayWorker drains MemReplayer. The harness starts it after the
	// client exists and stops it before closing the client.
	ReplayWorker *replay.Worker
}

// Scenario defines a test scenario interface.
type Scenario interface {
	// Name returns the unique name of the scenario.
	Name() string

	// Description returns a human-readable description.
	Description() string

	// Run executes the scenario logic.
	Run(ctx context.Context, env *Environment) error
}
