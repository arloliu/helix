package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // pprof is intentional for simulation
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/simulation"
	"github.com/arloliu/helix/test/simulation/config"
	"github.com/arloliu/helix/test/simulation/scenarios"
	simtypes "github.com/arloliu/helix/test/simulation/types"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/topology"
	htypes "github.com/arloliu/helix/types"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	// Parse flags
	configPath := flag.String("config", "", "Path to configuration file (optional)")
	profile := flag.String("profile", "quick", "Simulation profile (quick, comprehensive, soak, fallback)")
	duration := flag.Duration("duration", 5*time.Minute, "Total simulation duration (for soak tests)")
	seed := flag.Int64("seed", time.Now().UnixNano(), "Random seed")
	flag.Parse()

	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Load configuration if provided
	var settings *config.Config
	if *configPath != "" {
		var err error
		settings, err = config.Load(*configPath)
		if err != nil {
			logger.Error("Failed to load configuration", "path", *configPath, "error", err)
			return err
		}
		// Override flags with config values if present
		if settings.Simulation.Duration > 0 {
			*duration = settings.Simulation.Duration
		}
		if settings.Simulation.Seed != 0 {
			*seed = settings.Simulation.Seed
		}
	}

	logger.Info("Starting Helix Simulation",
		"profile", *profile,
		"seed", *seed,
		"duration", *duration,
	)

	// Start pprof server
	pprofServer := &http.Server{
		Addr:              "127.0.0.1:6060",
		ReadHeaderTimeout: 3 * time.Second,
	}
	go func() {
		logger.Info("Starting pprof server", "addr", pprofServer.Addr)
		if err := pprofServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("pprof server failed", "error", err)
		}
	}()

	// Handle signals for graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer shutdownCancel()
		if err := pprofServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("pprof server shutdown failed", "error", err)
		}
	}()

	// Start Clusters
	logger.Info("Starting Cluster A...")
	clusterA, err := testutil.StartCQLCluster(ctx, testutil.DefaultCQLClusterOptions("helix_test_a"))
	if err != nil {
		logger.Error("Failed to start Cluster A", "error", err)
		return err
	}
	defer func() {
		logger.Info("Terminating Cluster A...")
		if err := clusterA.Terminate(context.Background()); err != nil {
			logger.Error("Failed to terminate Cluster A", "error", err)
		}
	}()

	logger.Info("Starting Cluster B...")
	clusterB, err := testutil.StartCQLCluster(ctx, testutil.DefaultCQLClusterOptions("helix_test_b"))
	if err != nil {
		logger.Error("Failed to start Cluster B", "error", err)
		return err
	}
	defer func() {
		logger.Info("Terminating Cluster B...")
		if err := clusterB.Terminate(context.Background()); err != nil {
			logger.Error("Failed to terminate Cluster B", "error", err)
		}
	}()

	// Create simulation config
	simConfig := simulation.Config{
		Seed:     *seed,
		Duration: *duration,
		Profile:  *profile,
		ClusterA: clusterA,
		ClusterB: clusterB,
		Settings: settings,
	}

	// Initialize simulation orchestrator
	sim, err := simulation.New(simConfig, logger)
	if err != nil {
		logger.Error("Failed to initialize simulation", "error", err)
		return err
	}

	// Register scenarios based on profile
	registerScenarios(sim, *profile)

	// Run simulation
	if err := sim.Run(ctx); err != nil {
		logger.Error("Simulation failed", "error", err)
		return err
	}

	logger.Info("Simulation completed successfully")

	return nil
}

func registerScenarios(sim *simulation.Simulation, profile string) {
	// Basic scenarios always included
	sim.RegisterScenario(&scenarios.DegradedCluster{})
	sim.RegisterScenario(&scenarios.AdaptiveRecovery{})
	sim.RegisterScenario(&scenarios.CompleteFailure{})

	// fallback profile: 3 baseline scenarios + fallback-read group only.
	// Use this for quick, targeted verification of FallbackRead divergence detection.
	if profile == "fallback" {
		sim.RegisterStrategyGroup(fallbackReadGroup())
		return
	}

	// Add more scenarios based on profile
	if profile == "comprehensive" || profile == "soak" {
		sim.RegisterScenario(&scenarios.ReplaySaturation{})
		sim.RegisterScenario(&scenarios.DrainMode{})
		sim.RegisterScenario(&scenarios.FireForgetLimit{})
		sim.RegisterScenario(&scenarios.PartialDegradation{})

		// Strategy groups for untested policy combinations
		sim.RegisterStrategyGroup(circuitBreakerGroup())
		sim.RegisterStrategyGroup(latencyCircuitBreakerGroup())
		sim.RegisterStrategyGroup(primaryOnlyReadGroup())
		sim.RegisterStrategyGroup(roundRobinReadGroup())
		sim.RegisterStrategyGroup(stickyCooldownGroup())
		sim.RegisterStrategyGroup(fallbackReadGroup())
	}

	if profile == "soak" {
		sim.RegisterScenario(&scenarios.DualClusterDegradation{})
	}
}

// makeStrategyGroupClientWithMetrics creates a StrategyGroupSetupFunc where the
// policy factory receives the metrics collector, allowing policies (e.g.
// CircuitBreaker) to record metrics to the same collector the scenario asserts on.
func makeStrategyGroupClientWithMetrics(
	policyFactory func(mc *testutil.TestMetricsCollector) (helix.WriteStrategy, helix.ReadStrategy, helix.FailoverPolicy),
) simulation.StrategyGroupSetupFunc {
	return func(sessionA, sessionB cql.Session, mc *testutil.TestMetricsCollector) (*helix.CQLClient, *replay.MemoryReplayer, error) {
		writeStrategy, readStrategy, failoverPolicy := policyFactory(mc)
		return makeStrategyGroupClient(writeStrategy, readStrategy, failoverPolicy)(sessionA, sessionB, mc)
	}
}

func makeStrategyGroupClient(
	writeStrategy helix.WriteStrategy,
	readStrategy helix.ReadStrategy,
	failoverPolicy helix.FailoverPolicy,
) simulation.StrategyGroupSetupFunc {
	return func(sessionA, sessionB cql.Session, mc *testutil.TestMetricsCollector) (*helix.CQLClient, *replay.MemoryReplayer, error) {
		memReplayer := replay.NewMemoryReplayer()
		topo := topology.NewLocal()

		client, err := helix.NewCQLClient(sessionA, sessionB,
			helix.WithWriteStrategy(writeStrategy),
			helix.WithReadStrategy(readStrategy),
			helix.WithFailoverPolicy(failoverPolicy),
			helix.WithReplayer(memReplayer),
			helix.WithTopologyWatcher(topo),
			helix.WithMetrics(mc),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create client: %w", err)
		}

		worker := replay.NewMemoryWorker(memReplayer, client.DefaultExecuteFunc(),
			replay.WithWorkerMetrics(mc),
		)
		if err := worker.Start(); err != nil {
			client.Close()
			return nil, nil, fmt.Errorf("failed to start replay worker: %w", err)
		}
		client.Config().ReplayWorker = worker

		return client, memReplayer, nil
	}
}

func circuitBreakerGroup() simulation.StrategyGroup {
	return simulation.StrategyGroup{
		Name: "circuit-breaker",
		SetupFunc: makeStrategyGroupClientWithMetrics(
			func(mc *testutil.TestMetricsCollector) (helix.WriteStrategy, helix.ReadStrategy, helix.FailoverPolicy) {
				return policy.NewAdaptiveDualWrite(
						policy.WithAdaptiveDeltaThreshold(100*time.Millisecond),
						policy.WithAdaptiveStrikeThreshold(3),
					),
					// PrimaryOnlyRead ensures reads start on ClusterA (so the CB
					// observes A's failures) and automatically probes A after the
					// recovery timeout, calling RecordSuccess to close the CB.
					policy.NewPrimaryOnlyRead(
						policy.WithPrimaryOnlyRecoveryTimeout(10 * time.Second),
					),
					policy.NewCircuitBreaker(
						policy.WithThreshold(3),
						policy.WithResetTimeout(15*time.Second),
						policy.WithCircuitBreakerMetrics(mc),
					)
			},
		),
		Scenarios: []simtypes.Scenario{
			&scenarios.CircuitBreakerTrip{},
		},
	}
}

func latencyCircuitBreakerGroup() simulation.StrategyGroup {
	return simulation.StrategyGroup{
		Name: "latency-cb",
		SetupFunc: makeStrategyGroupClient(
			policy.NewAdaptiveDualWrite(
				policy.WithAdaptiveDeltaThreshold(100*time.Millisecond),
				policy.WithAdaptiveStrikeThreshold(3),
			),
			// Pin to ClusterA so the scenario's latency injection on ClusterA
			// is always observed via RecordLatency. StickyRead uses crypto/rand
			// for its default selection, which would otherwise route reads to
			// ClusterB 50% of the time, leaving ClusterA latency unobserved.
			policy.NewStickyRead(policy.WithPreferredCluster(htypes.ClusterA)),
			policy.NewLatencyCircuitBreaker(
				policy.WithLatencyAbsoluteMax(500*time.Millisecond),
				policy.WithLatencyThreshold(3),
				policy.WithLatencyResetTimeout(15*time.Second),
			),
		),
		Scenarios: []simtypes.Scenario{
			&scenarios.LatencyCircuitBreakerTrip{},
		},
	}
}

func primaryOnlyReadGroup() simulation.StrategyGroup {
	return simulation.StrategyGroup{
		Name: "primary-only",
		SetupFunc: makeStrategyGroupClient(
			policy.NewAdaptiveDualWrite(
				policy.WithAdaptiveDeltaThreshold(100*time.Millisecond),
				policy.WithAdaptiveStrikeThreshold(3),
			),
			policy.NewPrimaryOnlyRead(
				policy.WithPrimaryOnlyRecoveryTimeout(10*time.Second),
			),
			policy.NewActiveFailover(),
		),
		Scenarios: []simtypes.Scenario{
			&scenarios.PrimaryOnlyReadRecovery{},
		},
	}
}

func roundRobinReadGroup() simulation.StrategyGroup {
	return simulation.StrategyGroup{
		Name: "round-robin",
		SetupFunc: makeStrategyGroupClient(
			policy.NewAdaptiveDualWrite(
				policy.WithAdaptiveDeltaThreshold(100*time.Millisecond),
				policy.WithAdaptiveStrikeThreshold(3),
			),
			policy.NewRoundRobinRead(),
			policy.NewActiveFailover(),
		),
		Scenarios: []simtypes.Scenario{
			&scenarios.RoundRobinReadBalance{},
		},
	}
}

func stickyCooldownGroup() simulation.StrategyGroup {
	return simulation.StrategyGroup{
		Name: "sticky-cooldown",
		SetupFunc: makeStrategyGroupClient(
			policy.NewAdaptiveDualWrite(
				policy.WithAdaptiveDeltaThreshold(100*time.Millisecond),
				policy.WithAdaptiveStrikeThreshold(3),
			),
			// Pin initial preferred to A so phases 1–3 are deterministic.
			// Short cooldown so the test completes in reasonable time.
			policy.NewStickyRead(
				policy.WithPreferredCluster(htypes.ClusterA),
				policy.WithStickyReadCooldown(10*time.Second),
			),
			policy.NewActiveFailover(),
		),
		Scenarios: []simtypes.Scenario{
			&scenarios.StickyCooldown{},
		},
	}
}

func fallbackReadGroup() simulation.StrategyGroup {
	return simulation.StrategyGroup{
		Name: "fallback-read",
		SetupFunc: func(sessionA, sessionB cql.Session, mc *testutil.TestMetricsCollector) (*helix.CQLClient, *replay.MemoryReplayer, error) {
			memReplayer := replay.NewMemoryReplayer()
			topo := topology.NewLocal()

			client, err := helix.NewCQLClient(sessionA, sessionB,
				helix.WithWriteStrategy(policy.NewAdaptiveDualWrite(
					policy.WithAdaptiveDeltaThreshold(100*time.Millisecond),
					policy.WithAdaptiveStrikeThreshold(3),
				)),
				// Pin reads to B so FallbackRead is exercised deterministically:
				// B returns not-found for A-only rows, then FallbackRead finds them on A.
				helix.WithReadStrategy(policy.NewStickyRead(
					policy.WithPreferredCluster(htypes.ClusterB),
				)),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
				helix.WithReplayer(memReplayer),
				helix.WithTopologyWatcher(topo),
				helix.WithMetrics(mc),
				helix.WithDefaultFallbackRead(true),
			)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to create client: %w", err)
			}

			worker := replay.NewMemoryWorker(memReplayer, client.DefaultExecuteFunc(),
				replay.WithWorkerMetrics(mc),
			)
			if err := worker.Start(); err != nil {
				client.Close()
				return nil, nil, fmt.Errorf("failed to start replay worker: %w", err)
			}
			client.Config().ReplayWorker = worker

			return client, memReplayer, nil
		},
		Scenarios: []simtypes.Scenario{
			&scenarios.FallbackReadDivergence{},
		},
	}
}
