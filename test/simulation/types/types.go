package types

import (
	"context"
	"log/slog"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/test/simulation/chaos"
	"github.com/arloliu/helix/test/simulation/workload"
)

// Environment holds the shared resources for the simulation.
type Environment struct {
	Client  *helix.CQLClient
	ChaosA  *chaos.Session
	ChaosB  *chaos.Session
	Tracker *workload.WriteTracker
	Logger  *slog.Logger
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
