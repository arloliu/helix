package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // pprof is intentional for simulation
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/arloliu/helix/test/simulation"
	"github.com/arloliu/helix/test/simulation/config"
	"github.com/arloliu/helix/test/simulation/scenarios"
	"github.com/arloliu/helix/test/testutil"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	// Parse flags
	configPath := flag.String("config", "", "Path to configuration file (optional)")
	profile := flag.String("profile", "quick", "Simulation profile (quick, comprehensive, soak)")
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
	go func() {
		logger.Info("Starting pprof server on :6060")
		server := &http.Server{
			Addr:              ":6060",
			ReadHeaderTimeout: 3 * time.Second,
		}
		if err := server.ListenAndServe(); err != nil {
			logger.Error("pprof server failed", "error", err)
		}
	}()

	// Handle signals for graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

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

	// Add more scenarios based on profile
	if profile == "comprehensive" || profile == "soak" {
		sim.RegisterScenario(&scenarios.ReplaySaturation{})
		sim.RegisterScenario(&scenarios.DrainMode{})
		sim.RegisterScenario(&scenarios.CircuitBreakerTrip{})
		sim.RegisterScenario(&scenarios.StickyCooldown{})
		sim.RegisterScenario(&scenarios.FireForgetLimit{})
	}
}
