package simulation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
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
	htypes "github.com/arloliu/helix/types"
	"github.com/gocql/gocql"
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

// StrategyGroupSetupFunc creates a CQLClient with a specific strategy combination.
// It receives the chaos-wrapped sessions and a fresh metrics collector.
// The returned MemoryReplayer must belong to the returned client.
type StrategyGroupSetupFunc func(sessionA, sessionB cql.Session, mc *testutil.TestMetricsCollector) (*helix.CQLClient, *replay.MemoryReplayer, error)

// StrategyGroup is a set of scenarios that share a specific client configuration.
// The simulation runs all strategy groups sequentially after the main scenario set,
// creating a new CQLClient per group while reusing the same Cassandra sessions.
type StrategyGroup struct {
	Name      string
	SetupFunc StrategyGroupSetupFunc
	Scenarios []simtypes.Scenario
}

// Simulation orchestrates the test execution.
type Simulation struct {
	config         Config
	logger         *slog.Logger
	env            *simtypes.Environment
	scenarios      []simtypes.Scenario
	strategyGroups []StrategyGroup
	stopWorkload   context.CancelFunc
	rng            *rand.Rand
	rngMu          sync.Mutex
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

// RegisterStrategyGroup adds a strategy group to the simulation.
// Strategy groups run after the main scenario set, each with its own CQLClient.
func (s *Simulation) RegisterStrategyGroup(group StrategyGroup) {
	s.strategyGroups = append(s.strategyGroups, group)
}

// Run executes the simulation.
//
// For the "soak" profile, a duration-bounded context is applied and a
// randomized soak loop runs after the initial sequential pass. All other
// profiles run the registered scenarios once and exit.
func (s *Simulation) Run(ctx context.Context) error {
	// Keep the parent (signal-only) context for strategy groups, which run
	// after the soak loop and must not be cancelled by the duration timeout.
	parentCtx := ctx

	// Apply duration bound for soak profile so traffic, pruner, and the
	// soak loop all respect the configured duration.
	if s.config.Profile == "soak" && s.config.Duration > 0 {
		var durationCancel context.CancelFunc
		ctx, durationCancel = context.WithTimeout(ctx, s.config.Duration)
		defer durationCancel()
	}

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

	var scenarioErrors []error

	// Phase 1: Sequential pass — validates baseline correctness.
	for _, scenario := range s.scenarios {
		if ctx.Err() != nil {
			break
		}

		s.logger.Info("--------------------------------------------------")
		s.logger.Info("Running Scenario", "name", scenario.Name())
		s.logger.Info("--------------------------------------------------")

		if err := scenario.Run(ctx, s.env); err != nil {
			s.logger.Error("Scenario failed", "name", scenario.Name(), "error", err)
			scenarioErrors = append(scenarioErrors, fmt.Errorf("%s: %w", scenario.Name(), err))
		} else {
			s.logger.Info("Scenario completed successfully")
		}

		s.resetBetweenScenarios(ctx)
	}

	// Phase 2 (soak only): Randomized loop until duration expires.
	if s.config.Profile == "soak" && ctx.Err() == nil && len(scenarioErrors) == 0 {
		soakErrs := s.runSoakLoop(ctx)
		scenarioErrors = append(scenarioErrors, soakErrs...)
	}

	s.logger.Info("Stopping workload...")
	cancel()
	time.Sleep(1 * time.Second)

	if err := s.verify(); err != nil {
		return err
	}

	// Run strategy groups using the parent context — the duration-bounded
	// context may have expired after the soak loop, but strategy groups
	// should still run as a final validation pass.
	for i := range s.strategyGroups {
		if parentCtx.Err() != nil {
			break
		}
		if errs := s.runStrategyGroup(parentCtx, &s.strategyGroups[i]); len(errs) > 0 {
			scenarioErrors = append(scenarioErrors, errs...)
		}
	}

	if len(scenarioErrors) > 0 {
		return fmt.Errorf("%d scenario(s) failed: %w", len(scenarioErrors), errors.Join(scenarioErrors...))
	}

	return nil
}

// runSoakLoop randomly selects and executes scenarios until the context
// expires (duration timeout or signal). It runs after the sequential pass
// has validated baseline correctness. Context cancellation during a scenario
// is treated as a clean exit, not a failure.
func (s *Simulation) runSoakLoop(ctx context.Context) []error {
	if len(s.scenarios) == 0 {
		return nil
	}

	s.logger.Info("==================================================")
	s.logger.Info("Entering soak loop — randomly selecting scenarios",
		"scenarioPool", len(s.scenarios),
		"remaining", time.Until(s.soakDeadline(ctx)),
	)
	s.logger.Info("==================================================")

	var soakErrors []error
	iteration := 0

	for ctx.Err() == nil {
		iteration++

		scenario := s.scenarios[s.randIntn(len(s.scenarios))]

		s.logger.Info("--------------------------------------------------")
		s.logger.Info("Soak iteration",
			"iteration", iteration,
			"scenario", scenario.Name(),
			"remaining", time.Until(s.soakDeadline(ctx)),
		)
		s.logger.Info("--------------------------------------------------")

		if err := scenario.Run(ctx, s.env); err != nil {
			// Context cancellation is a clean exit, not a failure.
			if ctx.Err() != nil {
				break
			}
			s.logger.Error("Soak scenario failed",
				"iteration", iteration,
				"scenario", scenario.Name(),
				"error", err,
			)
			soakErrors = append(soakErrors, fmt.Errorf("soak #%d %s: %w", iteration, scenario.Name(), err))
		} else {
			s.logger.Info("Soak scenario completed", "iteration", iteration)
		}

		s.resetBetweenScenarios(ctx)
	}

	s.logger.Info("Soak loop finished",
		"totalIterations", iteration,
		"failures", len(soakErrors),
	)

	return soakErrors
}

// soakDeadline returns the context deadline or a zero time if none is set.
func (s *Simulation) soakDeadline(ctx context.Context) time.Time {
	if dl, ok := ctx.Deadline(); ok {
		return dl
	}
	return time.Time{}
}

// randIntn returns a random int in [0, n) using the simulation's shared RNG.
func (s *Simulation) randIntn(n int) int {
	s.rngMu.Lock()
	defer s.rngMu.Unlock()
	return s.rng.Intn(n)
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
		var replayerOpts []replay.MemoryReplayerOption
		if s.config.Settings.Helix.Replay.QueueSize > 0 {
			replayerOpts = append(replayerOpts, replay.WithQueueCapacity(s.config.Settings.Helix.Replay.QueueSize))
		}
		memReplayer = replay.NewMemoryReplayer(replayerOpts...)
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
	mc := testutil.NewTestMetricsCollector()

	client, err := helix.NewCQLClient(sessionA, sessionB,
		helix.WithWriteStrategy(writeStrategy),
		helix.WithReadStrategy(readStrategy),
		helix.WithFailoverPolicy(failoverPolicy),
		helix.WithReplayer(replayer),
		helix.WithTopologyWatcher(topo),
		helix.WithMetrics(mc),
	)
	if err != nil {
		return err
	}

	// Wire replay worker to the client's default executor so it honors batch payloads.
	workerOpts := []replay.WorkerOption{
		replay.WithWorkerMetrics(mc),
		replay.WithWorkerLogger(slogLogger{s.logger}),
	}
	if s.config.Settings != nil && s.config.Settings.Helix.Replay.RetryPolicy == "retained" {
		workerOpts = append(workerOpts, replay.WithRetryPolicy(replay.RetryWhileRetained))
	}
	worker := replay.NewMemoryWorker(memReplayer, client.DefaultExecuteFunc(), workerOpts...)
	if err := worker.Start(); err != nil {
		client.Close()
		return err
	}
	client.Config().ReplayWorker = worker

	tracker := workload.NewWriteTracker()

	s.env = &simtypes.Environment{
		Client:      client,
		ChaosA:      sessionA,
		ChaosB:      sessionB,
		Tracker:     tracker,
		Stats:       workload.NewWorkloadStats(),
		Logger:      s.logger,
		Metrics:     mc,
		MemReplayer: memReplayer,
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
	interval := 10 * time.Millisecond
	workers := 1
	payloadSize := 100
	readRatio := 0.2
	batchRatio := 0.1

	if s.config.Settings != nil {
		if s.config.Settings.Workload.Interval > 0 {
			interval = s.config.Settings.Workload.Interval
		}
		if s.config.Settings.Workload.Workers > 0 {
			workers = s.config.Settings.Workload.Workers
		}
		if s.config.Settings.Workload.PayloadSize > 0 {
			payloadSize = s.config.Settings.Workload.PayloadSize
		}
		if s.config.Settings.Workload.ReadRatio > 0 {
			readRatio = s.config.Settings.Workload.ReadRatio
		}
		if s.config.Settings.Workload.BatchRatio > 0 {
			batchRatio = s.config.Settings.Workload.BatchRatio
		}
	}

	for i := 0; i < workers; i++ {
		go s.trafficWorker(ctx, interval, payloadSize, readRatio, batchRatio)
	}

	<-ctx.Done()
}

func (s *Simulation) trafficWorker(ctx context.Context, interval time.Duration, payloadSize int, readRatio, batchRatio float64) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.rngMu.Lock()
			roll := s.rng.Float64()
			s.rngMu.Unlock()

			tracked := s.env.Tracker.Count()

			switch {
			case roll < readRatio && tracked > 0:
				// Read a previously written key
				s.doRead(ctx)
			case roll < readRatio+batchRatio:
				// Batch write
				s.doBatchWrite(ctx, payloadSize)
			default:
				// Single write
				s.doWrite(ctx, payloadSize)
			}
		}
	}
}

func (s *Simulation) doWrite(_ context.Context, payloadSize int) {
	id := gocql.TimeUUID()
	data := make([]byte, payloadSize)

	s.rngMu.Lock()
	_, err := s.rng.Read(data)
	s.rngMu.Unlock()
	if err != nil {
		s.logger.Error("Failed to generate random data", "error", err)
		return
	}

	err = s.env.Client.Query("INSERT INTO test_data (id, data) VALUES (?, ?)", id, data).Exec()
	s.classifyWriteError(err, id)
}

func (s *Simulation) doBatchWrite(_ context.Context, payloadSize int) {
	ids := [3]gocql.UUID{gocql.TimeUUID(), gocql.TimeUUID(), gocql.TimeUUID()}
	data := make([]byte, payloadSize)

	s.rngMu.Lock()
	_, err := s.rng.Read(data)
	s.rngMu.Unlock()
	if err != nil {
		s.logger.Error("Failed to generate random data for batch", "error", err)
		return
	}

	batch := s.env.Client.Batch(helix.LoggedBatch)
	for _, id := range ids {
		batch = batch.Query("INSERT INTO test_data (id, data) VALUES (?, ?)", id, data)
	}
	err = batch.Exec()

	if err == nil || errors.Is(err, htypes.ErrWriteAsync) {
		tsNs := time.Now().UnixNano()
		for _, id := range ids {
			s.env.Tracker.TrackWrite(id, tsNs)
		}
		s.env.Stats.WriteOK.Add(1)
	} else {
		s.classifyWriteError(err, gocql.UUID{})
	}
}

func (s *Simulation) doRead(_ context.Context) {
	key := s.env.Tracker.RandomKey()
	if key == (gocql.UUID{}) {
		return
	}

	var id gocql.UUID
	err := s.env.Client.Query("SELECT id FROM test_data WHERE id = ?", key).Scan(&id)
	if err == nil {
		s.env.Stats.ReadOK.Add(1)
	} else {
		s.env.Stats.ReadFailed.Add(1)
	}
}

func (s *Simulation) classifyWriteError(err error, id gocql.UUID) {
	switch {
	case err == nil:
		if id != (gocql.UUID{}) {
			s.env.Tracker.TrackWrite(id, time.Now().UnixNano())
		}
		s.env.Stats.WriteOK.Add(1)
	case errors.Is(err, htypes.ErrWriteAsync):
		if id != (gocql.UUID{}) {
			s.env.Tracker.TrackWrite(id, time.Now().UnixNano())
		}
		s.env.Stats.WriteAsync.Add(1)
	case errors.Is(err, htypes.ErrWriteDropped):
		s.env.Stats.WriteDropped.Add(1)
	default:
		var dce *htypes.DualClusterError
		if errors.As(err, &dce) {
			// Both clusters failed — neither accepted the write and no replay
			// is enqueued (CQLClient returns DualClusterError before the replay
			// path). Do NOT track: the key will never exist in either cluster.
			s.env.Stats.DualClusterErr.Add(1)
		} else {
			s.env.Stats.WriteFailed.Add(1)
			s.logger.Error("Write failed", "error", err)
		}
	}
}

// runStrategyGroup creates a new CQLClient for the group, runs its scenarios,
// verifies consistency, then closes the client. The Cassandra containers and
// chaos sessions are shared; only the Helix strategy configuration differs.
func (s *Simulation) runStrategyGroup(ctx context.Context, group *StrategyGroup) []error {
	s.logger.Info("==================================================")
	s.logger.Info("Running Strategy Group", "name", group.Name)
	s.logger.Info("==================================================")

	// Wait for replay queue from previous group to drain before switching clients.
	if s.env.MemReplayer != nil {
		_ = waitUntilCtx(ctx, 30*time.Second, func() bool {
			return s.env.MemReplayer.Len() == 0
		})
	}

	// Truncate BEFORE closing the client — the session must still be open.
	if err := s.config.ClusterA.Session.Query("TRUNCATE test_data").Exec(); err != nil {
		s.logger.Warn("Failed to truncate Cluster A", "error", err)
	}
	if err := s.config.ClusterB.Session.Query("TRUNCATE test_data").Exec(); err != nil {
		s.logger.Warn("Failed to truncate Cluster B", "error", err)
	}
	s.env.Tracker = workload.NewWriteTracker()
	s.env.Stats = workload.NewWorkloadStats()

	// Close the previous client after truncation.
	// chaos.Session.Close() is a no-op, so the underlying gocql sessions remain
	// open and the chaos sessions can be reused by the new client below.
	if s.env.Client != nil {
		s.env.Client.Close()
	}

	// Create fresh metrics collector and new client.
	mc := testutil.NewTestMetricsCollector()
	client, memReplayer, err := group.SetupFunc(s.env.ChaosA, s.env.ChaosB, mc)
	if err != nil {
		return []error{fmt.Errorf("strategy group %s setup failed: %w", group.Name, err)}
	}
	s.env.Client = client
	s.env.Metrics = mc
	s.env.MemReplayer = memReplayer

	// Start a dedicated workload for this group so that scenarios have live traffic
	// to observe. Wait for enough writes to accumulate before running scenarios.
	groupWorkloadCtx, stopGroupWorkload := context.WithCancel(ctx)
	go s.generateTraffic(groupWorkloadCtx)
	_ = waitUntilCtx(ctx, 30*time.Second, func() bool {
		return s.env.Tracker.Count() >= 50
	})

	var groupErrors []error
	for _, scenario := range group.Scenarios {
		if ctx.Err() != nil {
			break
		}
		s.logger.Info("--------------------------------------------------")
		s.logger.Info("Running Scenario", "name", scenario.Name(), "group", group.Name)
		s.logger.Info("--------------------------------------------------")

		if err := scenario.Run(ctx, s.env); err != nil {
			s.logger.Error("Scenario failed", "name", scenario.Name(), "group", group.Name, "error", err)
			groupErrors = append(groupErrors, fmt.Errorf("%s/%s: %w", group.Name, scenario.Name(), err))
		} else {
			s.logger.Info("Scenario completed successfully")
		}
		s.resetBetweenScenarios(ctx)
	}

	// Stop the group workload before verify.
	stopGroupWorkload()
	time.Sleep(500 * time.Millisecond)

	// Verify consistency for this group.
	if err := s.verify(); err != nil {
		groupErrors = append(groupErrors, fmt.Errorf("strategy group %s verify: %w", group.Name, err))
	}

	return groupErrors
}

// waitUntilCtx is waitUntil without importing the scenarios package.
func waitUntilCtx(ctx context.Context, timeout time.Duration, condition func() bool) error {
	if condition() {
		return nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return context.DeadlineExceeded
		case <-ticker.C:
			if condition() {
				return nil
			}
		}
	}
}

// resetBetweenScenarios clears chaos state and waits for both clusters to stabilise
// before the next scenario starts. It resets chaos configs, operation counters, and
// the metrics collector, then waits for 5 consecutive successful writes to prove
// both clusters are healthy.
func (s *Simulation) resetBetweenScenarios(ctx context.Context) {
	if s.env == nil {
		return
	}
	s.env.ChaosA.SetConfig(chaos.SessionConfig{})
	s.env.ChaosB.SetConfig(chaos.SessionConfig{})
	s.env.ChaosA.ResetCounters()
	s.env.ChaosB.ResetCounters()
	s.env.Metrics.Reset()

	// Reset write strategy state so degraded flags from a previous scenario
	// don't bleed into the next one.
	if adw, ok := s.env.Client.Config().WriteStrategy.(*policy.AdaptiveDualWrite); ok {
		adw.Reset()
	}

	// Drain the replay queue so the next scenario starts with a clean slate.
	// Without this, residual entries from the previous scenario accumulate
	// across soak iterations and cause false failures in drain/saturation checks.
	if s.env.MemReplayer != nil {
		_ = waitUntilCtx(ctx, 30*time.Second, func() bool {
			return s.env.MemReplayer.Len() == 0
		})
	}

	// Stability gate: wait until 5 consecutive writes succeed, up to 10 seconds.
	consecutive := 0
	deadline := time.Now().Add(10 * time.Second)
	for consecutive < 5 && time.Now().Before(deadline) && ctx.Err() == nil {
		id := gocql.TimeUUID()
		err := s.env.Client.Query("INSERT INTO test_data (id, data) VALUES (?, ?)", id, []byte{0}).Exec()
		if err == nil || errors.Is(err, htypes.ErrWriteAsync) {
			consecutive++
		} else {
			consecutive = 0
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (s *Simulation) verify() error {
	s.logger.Info("Verifying simulation results...")

	// Reset chaos to ensure clean verification
	resetConfig := chaos.SessionConfig{}
	s.env.ChaosA.SetConfig(resetConfig)
	s.env.ChaosB.SetConfig(resetConfig)

	// Drain the replay queue before checking consistency. Without this,
	// writes that were enqueued for replay during the last scenario may
	// not have been applied yet, causing false consistency failures.
	if s.env.MemReplayer != nil {
		_ = waitUntilCtx(context.Background(), 60*time.Second, func() bool {
			return s.env.MemReplayer.Len() == 0
		})
	}

	// Every payload the worker gave up on is a row that will be missing
	// below; report the counts so the failure is attributable.
	s.logger.Info("Replay drop totals before verification",
		"droppedA", s.env.Metrics.GetReplayDropped(htypes.ClusterA),
		"droppedB", s.env.Metrics.GetReplayDropped(htypes.ClusterB),
	)

	// Wait for eventual consistency (replay to finish).
	// Use condition-based waiting rather than a fixed sleep to reduce flakiness.
	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := s.env.Tracker.VerifyConsistency(s.env.ChaosA, s.env.ChaosB); err == nil {
			break
		} else if time.Now().After(deadline) {
			return fmt.Errorf("verification failed: %w", err)
		}

		time.Sleep(500 * time.Millisecond)
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

// slogLogger adapts *slog.Logger to types.Logger so the replay worker can
// share the simulation's logger and its drop decisions land in the run log.
type slogLogger struct{ *slog.Logger }

// Fatal logs the message at error level and aborts the run.
func (l slogLogger) Fatal(msg string, keysAndValues ...any) {
	l.Error(msg, keysAndValues...)
	panic(msg)
}
