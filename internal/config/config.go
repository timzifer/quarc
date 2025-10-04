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

// ValueKind describes the primitive type stored inside a cell.
type ValueKind string

const (
	// ValueKindNumber represents floating point numbers.
	ValueKindNumber ValueKind = "number"
	// ValueKindBool represents boolean values.
	ValueKindBool ValueKind = "bool"
	// ValueKindString represents plain UTF-8 strings.
	ValueKindString ValueKind = "string"
)

// EndpointConfig describes how to reach a Modbus slave.
type EndpointConfig struct {
	Address string   `yaml:"address"`
	UnitID  uint8    `yaml:"unit_id"`
	Timeout Duration `yaml:"timeout,omitempty"`
}

// CellConfig configures a local memory cell.
type CellConfig struct {
	ID       string    `yaml:"id"`
	Type     ValueKind `yaml:"type"`
	Unit     string    `yaml:"unit,omitempty"`
	TTL      Duration  `yaml:"ttl,omitempty"`
	Scale    float64   `yaml:"scale,omitempty"`
	Metadata yaml.Node `yaml:"metadata,omitempty"`
}

// ReadSignalConfig maps a portion of a Modbus read block into a cell.
type ReadSignalConfig struct {
	Cell       string    `yaml:"cell"`
	Offset     uint16    `yaml:"offset"`
	Bit        *uint8    `yaml:"bit,omitempty"`
	Type       ValueKind `yaml:"type"`
	Scale      float64   `yaml:"scale,omitempty"`
	Endianness string    `yaml:"endianness,omitempty"`
	Signed     bool      `yaml:"signed,omitempty"`
}

// ReadGroupConfig describes a block read.
type ReadGroupConfig struct {
	ID       string             `yaml:"id"`
	Endpoint EndpointConfig     `yaml:"endpoint"`
	Function string             `yaml:"function"`
	Start    uint16             `yaml:"start"`
	Length   uint16             `yaml:"length"`
	TTL      Duration           `yaml:"ttl"`
	Signals  []ReadSignalConfig `yaml:"signals"`
}

// WriteTargetConfig describes how a cell is pushed to Modbus.
type WriteTargetConfig struct {
	ID         string         `yaml:"id"`
	Cell       string         `yaml:"cell"`
	Endpoint   EndpointConfig `yaml:"endpoint"`
	Function   string         `yaml:"function"`
	Address    uint16         `yaml:"address"`
	Scale      float64        `yaml:"scale,omitempty"`
	Endianness string         `yaml:"endianness,omitempty"`
	Signed     bool           `yaml:"signed,omitempty"`
	Deadband   float64        `yaml:"deadband,omitempty"`
	RateLimit  Duration       `yaml:"rate_limit,omitempty"`
	Priority   int            `yaml:"priority,omitempty"`
}

// DependencyConfig describes a dependency for a logic block.
type DependencyConfig struct {
	Cell string    `yaml:"cell"`
	Type ValueKind `yaml:"type"`
}

// LogicBlockConfig describes a single logic evaluation block.
type LogicBlockConfig struct {
	ID           string             `yaml:"id"`
	Target       string             `yaml:"target"`
	Dependencies []DependencyConfig `yaml:"dependencies"`
	Normal       string             `yaml:"normal"`
	Fallback     string             `yaml:"fallback"`
	Metadata     yaml.Node          `yaml:"metadata,omitempty"`
}

// GlobalPolicies configure optional behaviours shared by the controller.
type GlobalPolicies struct {
	RetryBackoff   Duration `yaml:"retry_backoff,omitempty"`
	RetryMax       int      `yaml:"retry_max,omitempty"`
	ReadbackVerify bool     `yaml:"readback_verify,omitempty"`
	WatchdogCell   string   `yaml:"watchdog_cell,omitempty"`
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
	Cycle    Duration            `yaml:"cycle"`
	Logging  LoggingConfig       `yaml:"logging"`
	Cells    []CellConfig        `yaml:"cells"`
	Reads    []ReadGroupConfig   `yaml:"reads"`
	Writes   []WriteTargetConfig `yaml:"writes"`
	Logic    []LogicBlockConfig  `yaml:"logic"`
	Policies GlobalPolicies      `yaml:"policies"`
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

// CycleInterval returns the configured controller cycle duration.
func (c *Config) CycleInterval() time.Duration {
	if c == nil || c.Cycle.Duration <= 0 {
		return 500 * time.Millisecond
	}
	return c.Cycle.Duration
}
