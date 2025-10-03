package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration wraps time.Duration to support YAML unmarshalling from strings.
type Duration struct {
	time.Duration
}

// UnmarshalYAML parses duration strings like "5s" or "1m".
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	if value == nil {
		return fmt.Errorf("duration value node is nil")
	}
	var raw string
	if err := value.Decode(&raw); err != nil {
		return fmt.Errorf("decode duration: %w", err)
	}
	if raw == "" {
		d.Duration = 0
		return nil
	}
	dur, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", raw, err)
	}
	d.Duration = dur
	return nil
}

// MarshalYAML renders the duration as a string.
func (d Duration) MarshalYAML() (interface{}, error) {
	return d.Duration.String(), nil
}

// RemoteRegisterConfig describes how to interact with a remote Modbus endpoint.
type RemoteRegisterConfig struct {
	Address      string            `yaml:"address"`
	UnitID       uint8             `yaml:"unit_id"`
	Type         string            `yaml:"type"`
	Register     uint16            `yaml:"register"`
	Interval     Duration          `yaml:"interval"`
	PushOnUpdate bool              `yaml:"push_on_update"`
	Metadata     map[string]string `yaml:"metadata,omitempty"`
}

// BaseRegisterConfig contains the shared configuration for a register.
type BaseRegisterConfig struct {
	Name       string                `yaml:"name"`
	Address    uint16                `yaml:"address"`
	Expression string                `yaml:"expression,omitempty"`
	Remote     *RemoteRegisterConfig `yaml:"remote,omitempty"`
}

// CoilConfig describes a coil register.
type CoilConfig struct {
	BaseRegisterConfig `yaml:",inline"`
	InitialValue       bool `yaml:"initial_value"`
}

// DiscreteInputConfig describes a discrete input register.
type DiscreteInputConfig struct {
	BaseRegisterConfig `yaml:",inline"`
	InitialValue       bool `yaml:"initial_value"`
}

// HoldingRegisterConfig describes a holding register.
type HoldingRegisterConfig struct {
	BaseRegisterConfig `yaml:",inline"`
	InitialValue       uint16 `yaml:"initial_value"`
}

// InputRegisterConfig describes an input register.
type InputRegisterConfig struct {
	BaseRegisterConfig `yaml:",inline"`
	InitialValue       uint16 `yaml:"initial_value"`
}

// LokiConfig configures optional Loki integration for logging.
type LokiConfig struct {
	Enabled bool              `yaml:"enabled"`
	URL     string            `yaml:"url"`
	Labels  map[string]string `yaml:"labels"`
}

// LoggingConfig encapsulates runtime logging options.
type LoggingConfig struct {
	Level string     `yaml:"level"`
	Loki  LokiConfig `yaml:"loki"`
}

// Config is the root configuration structure for the service.
type Config struct {
	ListenAddress string                  `yaml:"listen_address"`
	LoopDuration  Duration                `yaml:"loop_duration"`
	Logging       LoggingConfig           `yaml:"logging"`
	Coils         []CoilConfig            `yaml:"coils"`
	Discrete      []DiscreteInputConfig   `yaml:"discrete_inputs"`
	Holding       []HoldingRegisterConfig `yaml:"holding_registers"`
	Input         []InputRegisterConfig   `yaml:"input_registers"`
}

// Load reads and decodes the configuration file from disk.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	return &cfg, nil
}

// LoopInterval returns the configured loop interval or a default value.
func (c *Config) LoopInterval() time.Duration {
	if c == nil || c.LoopDuration.Duration <= 0 {
		return time.Second
	}
	return c.LoopDuration.Duration
}
