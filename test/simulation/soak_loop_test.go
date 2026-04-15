package simulation

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	simtypes "github.com/arloliu/helix/test/simulation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubScenario is a minimal Scenario that counts how many times it runs.
type stubScenario struct {
	name     string
	runCount atomic.Int64
	runFunc  func(ctx context.Context) error
}

func (s *stubScenario) Name() string        { return s.name }
func (s *stubScenario) Description() string { return "stub" }
func (s *stubScenario) Run(ctx context.Context, _ *simtypes.Environment) error {
	s.runCount.Add(1)
	if s.runFunc != nil {
		return s.runFunc(ctx)
	}
	return nil
}

func newTestSimulation(scenarios []simtypes.Scenario) *Simulation {
	return &Simulation{
		config: Config{
			Profile: "soak",
		},
		logger:    slog.Default(),
		scenarios: scenarios,
		rng:       rand.New(rand.NewSource(42)),
	}
}

func TestSoakLoop_RunsMultipleIterations(t *testing.T) {
	s1 := &stubScenario{name: "scenario-A"}
	s2 := &stubScenario{name: "scenario-B"}

	sim := newTestSimulation([]simtypes.Scenario{s1, s2})

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	errs := sim.runSoakLoop(ctx)

	assert.Empty(t, errs, "no scenario errors expected")
	totalRuns := s1.runCount.Load() + s2.runCount.Load()
	assert.Greater(t, totalRuns, int64(1), "soak loop should run multiple iterations")
	t.Logf("scenario-A ran %d times, scenario-B ran %d times", s1.runCount.Load(), s2.runCount.Load())
}

func TestSoakLoop_StopsOnContextCancellation(t *testing.T) {
	s1 := &stubScenario{name: "scenario-A"}

	sim := newTestSimulation([]simtypes.Scenario{s1})

	ctx, cancel := context.WithCancel(t.Context())

	// Cancel after a short delay.
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	errs := sim.runSoakLoop(ctx)

	assert.Empty(t, errs, "context cancellation should not produce errors")
	assert.Greater(t, s1.runCount.Load(), int64(0), "at least one iteration should run")
}

func TestSoakLoop_CountsScenarioFailures(t *testing.T) {
	failCount := atomic.Int64{}
	s1 := &stubScenario{
		name: "flaky",
		runFunc: func(_ context.Context) error {
			// Fail on the 2nd call only.
			if failCount.Add(1) == 2 {
				return assert.AnError
			}
			return nil
		},
	}

	sim := newTestSimulation([]simtypes.Scenario{s1})

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	errs := sim.runSoakLoop(ctx)

	assert.Len(t, errs, 1, "exactly one scenario failure expected")
	assert.Contains(t, errs[0].Error(), "flaky")
	assert.Greater(t, s1.runCount.Load(), int64(2), "loop should continue after a failure")
}

func TestSoakLoop_EmptyScenarios_ReturnsImmediately(t *testing.T) {
	sim := newTestSimulation(nil)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	errs := sim.runSoakLoop(ctx)

	assert.Empty(t, errs)
}

func TestSoakLoop_ExercisesAllScenarios(t *testing.T) {
	const n = 5
	scenarios := make([]simtypes.Scenario, n)
	for i := range n {
		scenarios[i] = &stubScenario{name: "s" + string(rune('A'+i))}
	}

	sim := newTestSimulation(scenarios)

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	_ = sim.runSoakLoop(ctx)

	// With enough iterations, all scenarios should be selected at least once.
	for _, sc := range scenarios {
		stub, ok := sc.(*stubScenario)
		require.True(t, ok)
		assert.Greater(t, stub.runCount.Load(), int64(0),
			"scenario %s was never selected", stub.name)
	}
}

func TestSoakLoop_ConcurrentSafe(t *testing.T) {
	// Verify that randIntn is safe under concurrent access (the traffic
	// generator also uses rng concurrently).
	sim := newTestSimulation(nil)

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			for range 100 {
				sim.randIntn(10)
			}
		})
	}
	wg.Wait()
}

func TestSoakDeadline_WithTimeout(t *testing.T) {
	sim := newTestSimulation(nil)
	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	defer cancel()

	dl := sim.soakDeadline(ctx)
	require.False(t, dl.IsZero())
	assert.WithinDuration(t, time.Now().Add(time.Hour), dl, 5*time.Second)
}

func TestSoakDeadline_WithoutTimeout(t *testing.T) {
	sim := newTestSimulation(nil)
	dl := sim.soakDeadline(t.Context())
	assert.True(t, dl.IsZero())
}
