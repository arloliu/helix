package simulation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/adapter/cql"
	cqlv1 "github.com/arloliu/helix/adapter/cql/v1"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/simulation/chaos"
	"github.com/arloliu/helix/test/simulation/config"
	simtypes "github.com/arloliu/helix/test/simulation/types"
	"github.com/arloliu/helix/test/simulation/workload"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/topology"
	"github.com/arloliu/helix/types"
	"github.com/google/uuid"
)

// Config holds simulation configuration.
type Config struct {
	Duration time.Duration
	Seed     int64
	Profile  string
	ClusterA *testutil.CQLCluster
	ClusterB *testutil.CQLCluster
	Settings *config.Config
}

// Simulation orchestrates the test execution.
type Simulation struct {
	config       Config
	logger       *slog.Logger
	env          *simtypes.Environment
	scenarios    []simtypes.Scenario
	stopWorkload context.CancelFunc
	rng          *rand.Rand
}

// New creates a new simulation instance.
func New(cfg Config, logger *slog.Logger) (*Simulation, error) {
	return &Simulation{
		config:    cfg,
		logger:    logger,
		scenarios: make([]simtypes.Scenario, 0),
		//nolint:gosec // Simulation data, not security sensitive
		rng: rand.New(rand.NewSource(cfg.Seed)),
	}, nil
}

// RegisterScenario adds a scenario to the simulation.
func (s *Simulation) RegisterScenario(scenario simtypes.Scenario) {
	s.scenarios = append(s.scenarios, scenario)
}

// Run executes the simulation.
func (s *Simulation) Run(ctx context.Context) error {
	s.logger.Info("Initializing simulation environment...")

	if err := s.setupEnvironment(); err != nil {
		return fmt.Errorf("failed to setup environment: %w", err)
	}
	defer s.teardown()

	s.logger.Info("Starting workload generator...")
	workloadCtx, cancel := context.WithCancel(ctx)
	s.stopWorkload = cancel
	go s.generateTraffic(workloadCtx)

	// Start pruner for soak tests
	if s.config.Profile == "soak" {
		go s.runPruner(workloadCtx)
	}

	for _, scenario := range s.scenarios {
		if ctx.Err() != nil {
			break
		}

		s.logger.Info("--------------------------------------------------")
		s.logger.Info("Running Scenario", "name", scenario.Name())
		s.logger.Info("--------------------------------------------------")

		if err := scenario.Run(ctx, s.env); err != nil {
			s.logger.Error("Scenario failed", "error", err)
		} else {
			s.logger.Info("Scenario completed successfully")
		}
		time.Sleep(2 * time.Second)
	}

	s.logger.Info("Stopping workload...")
	cancel()
	time.Sleep(1 * time.Second)

	return s.verify()
}

func (s *Simulation) setupEnvironment() error {
	// Create Adapters
	adapterA := cqlv1.NewSession(s.config.ClusterA.Session)
	adapterB := cqlv1.NewSession(s.config.ClusterB.Session)

	// Wrap with Chaos
	sessionA := chaos.NewSession(adapterA)
	sessionB := chaos.NewSession(adapterB)

	// Create Helix Client
	var writeStrategy helix.WriteStrategy
	var readStrategy helix.ReadStrategy
	var failoverPolicy helix.FailoverPolicy
	var replayer helix.Replayer
	var memReplayer *replay.MemoryReplayer

	if s.config.Settings != nil {
		// Configure Write Strategy
		wsCfg := s.config.Settings.Helix.WriteStrategy
		switch wsCfg.Type {
		case "adaptive":
			opts := []policy.AdaptiveDualWriteOption{}
			if wsCfg.DeltaThreshold > 0 {
				opts = append(opts, policy.WithAdaptiveDeltaThreshold(wsCfg.DeltaThreshold))
			}
			if wsCfg.StrikeThreshold > 0 {
				opts = append(opts, policy.WithAdaptiveStrikeThreshold(wsCfg.StrikeThreshold))
			}
			writeStrategy = policy.NewAdaptiveDualWrite(opts...)
		default:
			writeStrategy = policy.NewAdaptiveDualWrite(
				policy.WithAdaptiveDeltaThreshold(100*time.Millisecond),
				policy.WithAdaptiveStrikeThreshold(3),
			)
		}

		// Configure Read Strategy
		// rsCfg := s.config.Settings.Helix.ReadStrategy
		readStrategy = policy.NewStickyRead()

		// Configure Failover Policy
		fpCfg := s.config.Settings.Helix.FailoverPolicy
		switch fpCfg.Type {
		case "circuit":
			failoverPolicy = policy.NewCircuitBreaker(
				policy.WithThreshold(fpCfg.Threshold),
				policy.WithResetTimeout(fpCfg.ResetTimeout),
			)
		case "latency_circuit":
			failoverPolicy = policy.NewLatencyCircuitBreaker(
				policy.WithLatencyThreshold(fpCfg.Threshold),
				policy.WithLatencyResetTimeout(fpCfg.ResetTimeout),
				policy.WithLatencyAbsoluteMax(fpCfg.AbsoluteMax),
			)
		default:
			failoverPolicy = policy.NewActiveFailover()
		}

		// Configure Replayer
		memReplayer = replay.NewMemoryReplayer()
		replayer = memReplayer
	} else {
		// Default configuration
		writeStrategy = policy.NewAdaptiveDualWrite(
			policy.WithAdaptiveDeltaThreshold(100*time.Millisecond),
			policy.WithAdaptiveStrikeThreshold(3),
		)
		readStrategy = policy.NewStickyRead()
		failoverPolicy = policy.NewActiveFailover()
		memReplayer = replay.NewMemoryReplayer()
		replayer = memReplayer
	}

	topo := topology.NewLocal()

	// Define replay execution logic
	executeReplay := func(_ context.Context, payload types.ReplayPayload) error {
		var session cql.Session
		if payload.TargetCluster == types.ClusterA {
			session = sessionA
		} else {
			session = sessionB
		}
		// Note: In a real app, we'd need to handle args properly.
		// ReplayPayload stores args as []interface{}.
		return session.Query(payload.Query, payload.Args...).Exec()
	}

	worker := replay.NewMemoryWorker(memReplayer, executeReplay)

	client, err := helix.NewCQLClient(sessionA, sessionB,
		helix.WithWriteStrategy(writeStrategy),
		helix.WithReadStrategy(readStrategy),
		helix.WithFailoverPolicy(failoverPolicy),
		helix.WithReplayer(replayer),
		helix.WithReplayWorker(worker),
		helix.WithTopologyWatcher(topo),
	)
	if err != nil {
		return err
	}

	tracker := workload.NewWriteTracker()

	s.env = &simtypes.Environment{
		Client:  client,
		ChaosA:  sessionA,
		ChaosB:  sessionB,
		Tracker: tracker,
		Logger:  s.logger,
	}

	// Initialize Schema
	schema := "CREATE TABLE IF NOT EXISTS test_data (id uuid PRIMARY KEY, data blob)"
	if err := s.config.ClusterA.Session.Query(schema).Exec(); err != nil {
		return fmt.Errorf("failed to create schema on A: %w", err)
	}
	if err := s.config.ClusterB.Session.Query(schema).Exec(); err != nil {
		return fmt.Errorf("failed to create schema on B: %w", err)
	}

	return nil
}

func (s *Simulation) teardown() {
	if s.stopWorkload != nil {
		s.stopWorkload()
	}
	if s.env != nil && s.env.Client != nil {
		s.env.Client.Close()
	}
}

func (s *Simulation) generateTraffic(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			id := uuid.New()
			data := make([]byte, 100)
			if _, err := s.rng.Read(data); err != nil {
				s.logger.Error("Failed to generate random data", "error", err)
				continue
			}

			err := s.env.Client.Query("INSERT INTO test_data (id, data) VALUES (?, ?)", id.String(), data).Exec()
			if err == nil || errors.Is(err, types.ErrWriteAsync) {
				s.env.Tracker.TrackWrite(id.String(), time.Now().UnixMicro())
			} else {
				s.logger.Error("Write failed", "error", err)
			}
		}
	}
}

func (s *Simulation) verify() error {
	s.logger.Info("Verifying simulation results...")

	// Reset chaos to ensure clean verification
	resetConfig := chaos.SessionConfig{}
	s.env.ChaosA.SetConfig(resetConfig)
	s.env.ChaosB.SetConfig(resetConfig)

	// Wait for eventual consistency (replay to finish)
	s.logger.Info("Waiting for eventual consistency...")
	time.Sleep(5 * time.Second)

	if err := s.env.Tracker.VerifyConsistency(s.env.ChaosA, s.env.ChaosB); err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}

	s.logger.Info("Verification passed!")

	return nil
}

func (s *Simulation) runPruner(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Prune writes older than 5 minutes
			pruned, err := s.env.Tracker.VerifyAndPrune(s.env.ChaosA, s.env.ChaosB, 5*time.Minute)
			if err != nil {
				s.logger.Error("Pruning failed", "error", err)
			} else {
				s.logger.Info("Pruned old writes", "count", pruned)
			}
		}
	}
}
