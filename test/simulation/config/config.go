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
	Clusters   ClustersConfig   `yaml:"clusters"`
	Helix      HelixConfig      `yaml:"helix"`
}

type SimulationConfig struct {
	Duration        time.Duration `yaml:"duration"`
	Seed            int64         `yaml:"seed"`
	ReportDir       string        `yaml:"report_dir"`
	ConsoleInterval time.Duration `yaml:"console_interval"`
}

type ClustersConfig struct {
	Type     string  `yaml:"type"` // scylladb | cassandra
	MemoryMB int     `yaml:"memory_mb"`
	CPULimit float64 `yaml:"cpu_limit"`
}

type HelixConfig struct {
	WriteStrategy  WriteStrategyConfig  `yaml:"write_strategy"`
	ReadStrategy   ReadStrategyConfig   `yaml:"read_strategy"`
	FailoverPolicy FailoverPolicyConfig `yaml:"failover_policy"`
	Replay         ReplayConfig         `yaml:"replay"`
}

type WriteStrategyConfig struct {
	Type              string        `yaml:"type"` // adaptive | concurrent | single
	DeltaThreshold    time.Duration `yaml:"delta_threshold"`
	AbsoluteMax       time.Duration `yaml:"absolute_max"`
	StrikeThreshold   int           `yaml:"strike_threshold"`
	RecoveryThreshold int           `yaml:"recovery_threshold"`
	FireForgetTimeout time.Duration `yaml:"fire_forget_timeout"`
	FireForgetLimit   int           `yaml:"fire_forget_limit"`
}

type ReadStrategyConfig struct {
	Type      string        `yaml:"type"` // sticky | primary_only
	Cooldown  time.Duration `yaml:"cooldown"`
	Preferred string        `yaml:"preferred"` // A | B | random
}

type FailoverPolicyConfig struct {
	Type         string        `yaml:"type"` // active | circuit | latency_circuit
	Threshold    int           `yaml:"threshold"`
	ResetTimeout time.Duration `yaml:"reset_timeout"`
	AbsoluteMax  time.Duration `yaml:"absolute_max"`
}

type ReplayConfig struct {
	Type      string `yaml:"type"` // memory | nats
	QueueSize int    `yaml:"queue_size"`
	NATSURL   string `yaml:"nats_url"`
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
	if cfg.Simulation.ConsoleInterval == 0 {
		cfg.Simulation.ConsoleInterval = 10 * time.Second
	}
	if cfg.Simulation.ReportDir == "" {
		cfg.Simulation.ReportDir = "./reports"
	}

	return &cfg, nil
}
