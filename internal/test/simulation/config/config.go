package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the simulation configuration
type Config struct {
	Simulation SimulationConfig `yaml:"simulation"`
	Helix      HelixConfig      `yaml:"helix"`
	Workload   WorkloadConfig   `yaml:"workload"`
}

type SimulationConfig struct {
	Duration time.Duration `yaml:"duration"`
	Seed     int64         `yaml:"seed"`
}

type HelixConfig struct {
	WriteStrategy  WriteStrategyConfig  `yaml:"write_strategy"`
	FailoverPolicy FailoverPolicyConfig `yaml:"failover_policy"`
	Replay         ReplayConfig         `yaml:"replay"`
}

// WriteStrategyConfig tunes the main client's AdaptiveDualWrite.
// The write strategy itself is not configurable:
// strategy groups in cmd/main.go choose their own strategies.
type WriteStrategyConfig struct {
	DeltaThreshold  time.Duration `yaml:"delta_threshold"`
	StrikeThreshold int           `yaml:"strike_threshold"`
}

type FailoverPolicyConfig struct {
	Type         string        `yaml:"type"` // active | circuit | latency_circuit
	Threshold    int           `yaml:"threshold"`
	ResetTimeout time.Duration `yaml:"reset_timeout"`
	AbsoluteMax  time.Duration `yaml:"absolute_max"`
}

type ReplayConfig struct {
	QueueSize   int    `yaml:"queue_size"`
	RetryPolicy string `yaml:"retry_policy"` // retained (default) | bounded
}

type WorkloadConfig struct {
	Workers     int           `yaml:"workers"`
	Interval    time.Duration `yaml:"interval"`
	PayloadSize int           `yaml:"payload_size"`
	ReadRatio   float64       `yaml:"read_ratio"`  // Fraction of ops that are reads (0.0-1.0, default 0.2)
	BatchRatio  float64       `yaml:"batch_ratio"` // Fraction of writes that use LOGGED BATCH (0.0-1.0, default 0.1)
}

// Load reads configuration from a YAML file
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Set defaults if needed
	if cfg.Simulation.Duration == 0 {
		cfg.Simulation.Duration = 5 * time.Minute
	}
	if cfg.Workload.Workers <= 0 {
		cfg.Workload.Workers = 1
	}
	if cfg.Workload.Interval <= 0 {
		cfg.Workload.Interval = 10 * time.Millisecond
	}
	if cfg.Workload.PayloadSize <= 0 {
		cfg.Workload.PayloadSize = 100
	}

	return &cfg, nil
}
