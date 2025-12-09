package scenarios

import (
	"context"

	"github.com/arloliu/helix/test/simulation/types"
)

// Scenario defines a test scenario interface.
type Scenario interface {
	// Name returns the unique name of the scenario.
	Name() string

	// Description returns a human-readable description.
	Description() string

	// Run executes the scenario logic.
	Run(ctx context.Context, env *types.Environment) error
}
