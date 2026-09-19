package config_test

import (
	"os"
	"testing"
	"time"

	"github.com/arloliu/helix/test/simulation/config"
)

// writeTempFile creates a temporary YAML file with the given content and returns its path.
// The caller is responsible for removing it (or rely on t.Cleanup).
func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp("", "helix-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close temp file: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(f.Name()) })

	return f.Name()
}

// Load with an empty YAML file must fill in all documented defaults.
func TestLoad_DefaultsOnEmptyYAML(t *testing.T) {
	path := writeTempFile(t, "{}\n")

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Simulation.Duration != 5*time.Minute {
		t.Errorf("Duration = %v, want 5m", cfg.Simulation.Duration)
	}
	if cfg.Simulation.ConsoleInterval != 10*time.Second {
		t.Errorf("ConsoleInterval = %v, want 10s", cfg.Simulation.ConsoleInterval)
	}
	if cfg.Workload.Workers != 1 {
		t.Errorf("Workers = %d, want 1", cfg.Workload.Workers)
	}
	if cfg.Workload.Interval != 10*time.Millisecond {
		t.Errorf("Interval = %v, want 10ms", cfg.Workload.Interval)
	}
	if cfg.Workload.PayloadSize != 100 {
		t.Errorf("PayloadSize = %d, want 100", cfg.Workload.PayloadSize)
	}
}

// Load with fully-specified YAML must not overwrite any explicit value.
func TestLoad_ExplicitValuesPreserved(t *testing.T) {
	yaml := `
simulation:
  duration: 2h
  seed: 12345
  console_interval: 30s

workload:
  workers: 4
  interval: 5ms
  payload_size: 512
  read_ratio: 0.3
  batch_ratio: 0.2

helix:
  write_strategy:
    delta_threshold: 200ms
    strike_threshold: 5
  failover_policy:
    type: circuit
    threshold: 5
    reset_timeout: 1m
    absolute_max: 2s
  replay:
    queue_size: 5000
    retry_policy: bounded
`
	path := writeTempFile(t, yaml)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Simulation.Duration != 2*time.Hour {
		t.Errorf("Duration = %v, want 2h", cfg.Simulation.Duration)
	}
	if cfg.Simulation.Seed != 12345 {
		t.Errorf("Seed = %d, want 12345", cfg.Simulation.Seed)
	}
	if cfg.Simulation.ConsoleInterval != 30*time.Second {
		t.Errorf("ConsoleInterval = %v, want 30s", cfg.Simulation.ConsoleInterval)
	}
	if cfg.Workload.Workers != 4 {
		t.Errorf("Workers = %d, want 4", cfg.Workload.Workers)
	}
	if cfg.Workload.Interval != 5*time.Millisecond {
		t.Errorf("Interval = %v, want 5ms", cfg.Workload.Interval)
	}
	if cfg.Workload.PayloadSize != 512 {
		t.Errorf("PayloadSize = %d, want 512", cfg.Workload.PayloadSize)
	}
	if cfg.Workload.ReadRatio != 0.3 {
		t.Errorf("ReadRatio = %v, want 0.3", cfg.Workload.ReadRatio)
	}
	if cfg.Workload.BatchRatio != 0.2 {
		t.Errorf("BatchRatio = %v, want 0.2", cfg.Workload.BatchRatio)
	}
	if cfg.Helix.WriteStrategy.DeltaThreshold != 200*time.Millisecond {
		t.Errorf("DeltaThreshold = %v, want 200ms", cfg.Helix.WriteStrategy.DeltaThreshold)
	}
	if cfg.Helix.WriteStrategy.StrikeThreshold != 5 {
		t.Errorf("StrikeThreshold = %d, want 5", cfg.Helix.WriteStrategy.StrikeThreshold)
	}
	if cfg.Helix.FailoverPolicy.Type != "circuit" {
		t.Errorf("FailoverPolicy.Type = %q, want circuit", cfg.Helix.FailoverPolicy.Type)
	}
	if cfg.Helix.FailoverPolicy.Threshold != 5 {
		t.Errorf("FailoverPolicy.Threshold = %d, want 5", cfg.Helix.FailoverPolicy.Threshold)
	}
	if cfg.Helix.Replay.QueueSize != 5000 {
		t.Errorf("Replay.QueueSize = %d, want 5000", cfg.Helix.Replay.QueueSize)
	}
	if cfg.Helix.FailoverPolicy.ResetTimeout != time.Minute {
		t.Errorf("FailoverPolicy.ResetTimeout = %v, want 1m", cfg.Helix.FailoverPolicy.ResetTimeout)
	}
	if cfg.Helix.FailoverPolicy.AbsoluteMax != 2*time.Second {
		t.Errorf("FailoverPolicy.AbsoluteMax = %v, want 2s", cfg.Helix.FailoverPolicy.AbsoluteMax)
	}
	if cfg.Helix.Replay.RetryPolicy != "bounded" {
		t.Errorf("Replay.RetryPolicy = %q, want bounded", cfg.Helix.Replay.RetryPolicy)
	}
}

// Partial YAML must get defaults only for missing fields.
func TestLoad_PartialYAMLMixesDefaultsAndExplicit(t *testing.T) {
	yaml := `
workload:
  workers: 8
`
	path := writeTempFile(t, yaml)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Explicit value preserved.
	if cfg.Workload.Workers != 8 {
		t.Errorf("Workers = %d, want 8", cfg.Workload.Workers)
	}
	// Defaults filled in for missing fields.
	if cfg.Workload.Interval != 10*time.Millisecond {
		t.Errorf("Interval = %v, want 10ms (default)", cfg.Workload.Interval)
	}
	if cfg.Simulation.Duration != 5*time.Minute {
		t.Errorf("Duration = %v, want 5m (default)", cfg.Simulation.Duration)
	}
}

// Workers set to 0 must be replaced by the default of 1.
func TestLoad_WorkersZeroGetsDefault(t *testing.T) {
	path := writeTempFile(t, "workload:\n  workers: 0\n")

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Workload.Workers != 1 {
		t.Errorf("Workers = %d, want 1 (default for zero)", cfg.Workload.Workers)
	}
}

// Load must return an error for a non-existent path.
func TestLoad_NonExistentPath(t *testing.T) {
	_, err := config.Load("/nonexistent/path/to/config.yaml")
	if err == nil {
		t.Fatal("Load() expected error for non-existent file, got nil")
	}
}

// Load must return an error for malformed YAML.
func TestLoad_MalformedYAML(t *testing.T) {
	path := writeTempFile(t, "simulation: {duration: [broken\n")

	_, err := config.Load(path)
	if err == nil {
		t.Fatal("Load() expected error for malformed YAML, got nil")
	}
}
