package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	ID       string      `yaml:"id"`
	Type     ValueKind   `yaml:"type"`
	Unit     string      `yaml:"unit,omitempty"`
	TTL      Duration    `yaml:"ttl,omitempty"`
	Scale    float64     `yaml:"scale,omitempty"`
	Constant interface{} `yaml:"constant,omitempty"`
	Metadata yaml.Node   `yaml:"metadata,omitempty"`
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
	Disable  bool               `yaml:"disable,omitempty"`
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
	Disable    bool           `yaml:"disable,omitempty"`
}

// ProgramSignalConfig maps a program signal onto a cell.
type ProgramSignalConfig struct {
	ID       string      `yaml:"id"`
	Cell     string      `yaml:"cell"`
	Type     ValueKind   `yaml:"type,omitempty"`
	Optional bool        `yaml:"optional,omitempty"`
	Default  interface{} `yaml:"default,omitempty"`
}

// ProgramConfig describes a reusable processing module.
type ProgramConfig struct {
	ID       string                 `yaml:"id"`
	Type     string                 `yaml:"type"`
	Inputs   []ProgramSignalConfig  `yaml:"inputs,omitempty"`
	Outputs  []ProgramSignalConfig  `yaml:"outputs,omitempty"`
	Settings map[string]interface{} `yaml:"settings,omitempty"`
	Metadata yaml.Node              `yaml:"metadata,omitempty"`
}

// DependencyConfig describes a dependency for a logic block.
type DependencyConfig struct {
	Cell string    `yaml:"cell"`
	Type ValueKind `yaml:"type"`
	// Threshold defines the minimum delta that should trigger a logic re-evaluation for numeric values.
	Threshold float64 `yaml:"threshold,omitempty"`
}

// LogicBlockConfig describes a single logic evaluation block.
type LogicBlockConfig struct {
	ID           string             `yaml:"id"`
	Target       string             `yaml:"target"`
	Dependencies []DependencyConfig `yaml:"dependencies"`
	Expression   string             `yaml:"expression"`
	Valid        string             `yaml:"valid"`
	Quality      string             `yaml:"quality"`
	Metadata     yaml.Node          `yaml:"metadata,omitempty"`
}

// HelperFunctionConfig defines a standalone helper function that can be used from logic expressions.
type HelperFunctionConfig struct {
	Name       string   `yaml:"name"`
	Arguments  []string `yaml:"arguments"`
	Expression string   `yaml:"expression"`
}

// DSLConfig configures expression language extensions.
type DSLConfig struct {
	Helpers []HelperFunctionConfig `yaml:"helpers,omitempty"`
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
	Level  string     `yaml:"level"`
	Format string     `yaml:"format,omitempty"`
	Loki   LokiConfig `yaml:"loki"`
}

// WorkerSlots configures the concurrency for each pipeline stage.
type WorkerSlots struct {
	Read    int `yaml:"read,omitempty"`
	Program int `yaml:"program,omitempty"`
	Execute int `yaml:"execute,omitempty"`
	Write   int `yaml:"write,omitempty"`
}

// ServerCellConfig maps a cell onto an input register address exposed via the embedded Modbus server.
type ServerCellConfig struct {
	Cell    string  `yaml:"cell"`
	Address uint16  `yaml:"address"`
	Scale   float64 `yaml:"scale,omitempty"`
	Signed  bool    `yaml:"signed,omitempty"`
}

// ServerConfig configures the optional embedded Modbus server.
type ServerConfig struct {
	Enabled bool               `yaml:"enabled"`
	Listen  string             `yaml:"listen"`
	UnitID  uint8              `yaml:"unit_id,omitempty"`
	Cells   []ServerCellConfig `yaml:"cells"`
}

// Config is the root configuration structure for the service.
type Config struct {
	Cycle    Duration               `yaml:"cycle"`
	Logging  LoggingConfig          `yaml:"logging"`
	Modules  []string               `yaml:"modules"`
	Workers  WorkerSlots            `yaml:"workers,omitempty"`
	Programs []ProgramConfig        `yaml:"programs,omitempty"`
	Cells    []CellConfig           `yaml:"cells"`
	Reads    []ReadGroupConfig      `yaml:"reads"`
	Writes   []WriteTargetConfig    `yaml:"writes"`
	Logic    []LogicBlockConfig     `yaml:"logic"`
	DSL      DSLConfig              `yaml:"dsl"`
	Helpers  []HelperFunctionConfig `yaml:"helpers,omitempty"`
	Policies GlobalPolicies         `yaml:"policies"`
	Server   ServerConfig           `yaml:"server"`
}

// Load reads and decodes the configuration file from disk.
func Load(path string) (*Config, error) {
	if path == "" {
		return nil, errors.New("config path must not be empty")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve config path: %w", err)
	}

	info, err := os.Stat(abs)
	if err != nil {
		return nil, fmt.Errorf("stat config path: %w", err)
	}

	visited := make(map[string]struct{})
	if info.IsDir() {
		return loadDir(abs, visited)
	}
	return loadFile(abs, visited)
}

// CycleInterval returns the configured controller cycle duration.
func (c *Config) CycleInterval() time.Duration {
	if c == nil || c.Cycle.Duration <= 0 {
		return 500 * time.Millisecond
	}
	return c.Cycle.Duration
}

func loadFile(path string, visited map[string]struct{}) (*Config, error) {
	if _, ok := visited[path]; ok {
		return nil, fmt.Errorf("config include cycle detected at %s", path)
	}
	visited[path] = struct{}{}
	defer delete(visited, path)

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config %s: %w", path, err)
	}

	modules := cfg.Modules
	cfg.Modules = nil

	baseDir := filepath.Dir(path)
	for _, module := range modules {
		if module == "" {
			continue
		}
		modulePath := module
		if !filepath.IsAbs(modulePath) {
			modulePath = filepath.Join(baseDir, module)
		}

		info, err := os.Stat(modulePath)
		if err != nil {
			return nil, fmt.Errorf("load module %s: %w", module, err)
		}

		var modCfg *Config
		if info.IsDir() {
			modCfg, err = loadDir(modulePath, visited)
		} else {
			modCfg, err = loadFile(modulePath, visited)
		}
		if err != nil {
			return nil, fmt.Errorf("load module %s: %w", module, err)
		}
		mergeConfig(&cfg, modCfg)
	}

	return &cfg, nil
}

func loadDir(path string, visited map[string]struct{}) (*Config, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("read config dir %s: %w", path, err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	result := &Config{}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		ext := strings.ToLower(filepath.Ext(name))
		if ext != ".yaml" && ext != ".yml" {
			continue
		}
		subPath := filepath.Join(path, name)
		cfg, err := loadFile(subPath, visited)
		if err != nil {
			return nil, err
		}
		mergeConfig(result, cfg)
	}
	return result, nil
}

func mergeConfig(dst, src *Config) {
	if dst == nil || src == nil {
		return
	}

	if src.Cycle.Duration != 0 {
		dst.Cycle = src.Cycle
	}
	if src.Logging.Level != "" {
		dst.Logging.Level = src.Logging.Level
	}
	if src.Logging.Format != "" {
		dst.Logging.Format = src.Logging.Format
	}
	if src.Logging.Loki.Enabled || src.Logging.Loki.URL != "" || len(src.Logging.Loki.Labels) > 0 {
		dst.Logging.Loki = src.Logging.Loki
	}
	if src.Workers != (WorkerSlots{}) {
		dst.Workers = src.Workers
	}
	if len(src.DSL.Helpers) > 0 {
		dst.DSL.Helpers = append(dst.DSL.Helpers, src.DSL.Helpers...)
	}
	if len(src.Helpers) > 0 {
		dst.Helpers = append(dst.Helpers, src.Helpers...)
	}
	if src.Policies != (GlobalPolicies{}) {
		dst.Policies = src.Policies
	}
	if src.Server.Enabled || src.Server.Listen != "" || src.Server.UnitID != 0 || len(src.Server.Cells) > 0 {
		dst.Server = src.Server
	}

	dst.Programs = append(dst.Programs, src.Programs...)
	dst.Cells = append(dst.Cells, src.Cells...)
	dst.Reads = append(dst.Reads, src.Reads...)
	dst.Writes = append(dst.Writes, src.Writes...)
	dst.Logic = append(dst.Logic, src.Logic...)
}
