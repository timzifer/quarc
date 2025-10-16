package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"cuelang.org/go/cue"
	"cuelang.org/go/cue/load"
)

// Duration wraps time.Duration to support decoding from configuration files.
type Duration struct {
	time.Duration
}

// UnmarshalJSON parses duration strings like "5s" or "1m" when decoding via JSON.
func (d *Duration) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		return errors.New("duration value is empty")
	}
	if string(data) == "null" {
		d.Duration = 0
		return nil
	}
	var raw string
	if err := json.Unmarshal(data, &raw); err == nil {
		raw = strings.TrimSpace(raw)
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
	var ns int64
	if err := json.Unmarshal(data, &ns); err == nil {
		d.Duration = time.Duration(ns)
		return nil
	}
	return fmt.Errorf("unsupported duration encoding: %s", string(data))
}

// MarshalJSON renders the duration as a string when encoding to JSON.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}

// ValueKind describes the primitive type stored inside a cell.
type ValueKind string

const (
	// ValueKindNumber represents floating point numbers.
	ValueKindNumber ValueKind = "number"
	// ValueKindFloat represents floating point numbers (alias for number).
	ValueKindFloat ValueKind = "float"
	// ValueKindInteger represents signed integer values.
	ValueKindInteger ValueKind = "integer"
	// ValueKindDecimal represents arbitrary precision decimal numbers.
	ValueKindDecimal ValueKind = "decimal"
	// ValueKindBool represents boolean values.
	ValueKindBool ValueKind = "bool"
	// ValueKindString represents plain UTF-8 strings.
	ValueKindString ValueKind = "string"
	// ValueKindDate represents calendar date values.
	ValueKindDate ValueKind = "date"
)

var allowedSignalAggregators = map[string]struct{}{
	"last":         {},
	"sum":          {},
	"mean":         {},
	"min":          {},
	"max":          {},
	"count":        {},
	"queue_length": {},
}

// EndpointConfig describes how to reach a Modbus slave.
type EndpointConfig struct {
	Address string   `json:"address"`
	UnitID  uint8    `json:"unit_id"`
	Timeout Duration `json:"timeout,omitempty"`
	Driver  string   `json:"driver,omitempty"`
}

// ConnectionPoolingHints exposes optional pooling parameters for reusable connections.
type ConnectionPoolingHints struct {
	MaxIdle     int      `json:"max_idle,omitempty"`
	MaxActive   int      `json:"max_active,omitempty"`
	MaxLifetime Duration `json:"max_lifetime,omitempty"`
}

// IOConnectionConfig describes a reusable transport connection shared between reads and writes.
type IOConnectionConfig struct {
	ID             string                 `json:"id"`
	Driver         string                 `json:"driver,omitempty"`
	Endpoint       EndpointConfig         `json:"endpoint"`
	DriverSettings json.RawMessage        `json:"driver_settings,omitempty"`
	Pooling        ConnectionPoolingHints `json:"pooling,omitempty"`
	Source         ModuleReference        `json:"-"`
}

// ModuleReference captures metadata about the configuration source that defined an entry.
type ModuleReference struct {
	File        string `json:"file,omitempty"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	Package     string `json:"package,omitempty"`
}

// CellConfig configures a local memory cell.
type CellConfig struct {
	ID          string          `json:"id"`
	Type        ValueKind       `json:"type"`
	Unit        string          `json:"unit,omitempty"`
	TTL         Duration        `json:"ttl,omitempty"`
	Scale       float64         `json:"scale,omitempty"`
	Constant    interface{}     `json:"constant,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
	Name        string          `json:"name,omitempty"`
	Description string          `json:"description,omitempty"`
	Source      ModuleReference `json:"-"`
}

// ReadSignalConfig maps a portion of a Modbus read block into a cell.
type ReadSignalConfig struct {
	Cell         string                        `json:"cell"`
	Offset       uint16                        `json:"offset"`
	Bit          *uint8                        `json:"bit,omitempty"`
	Type         ValueKind                     `json:"type"`
	Scale        float64                       `json:"scale,omitempty"`
	Endianness   string                        `json:"endianness,omitempty"`
	Signed       bool                          `json:"signed,omitempty"`
	Aggregation  string                        `json:"aggregation,omitempty"`
	BufferSize   int                           `json:"buffer_size,omitempty"`
	Buffer       *SignalBufferConfig           `json:"buffer,omitempty"`
	Aggregations []ReadSignalAggregationConfig `json:"aggregations,omitempty"`
}

// SignalBufferConfig controls buffering behaviour for a read signal.
type SignalBufferConfig struct {
	Capacity   *int   `json:"capacity,omitempty"`
	Aggregator string `json:"aggregator,omitempty"`
	OnOverflow string `json:"on_overflow,omitempty"`
}

// ReadSignalAggregationConfig configures an aggregate output for a buffered signal.
type ReadSignalAggregationConfig struct {
	Cell       string `json:"cell"`
	Aggregator string `json:"aggregator,omitempty"`
	Quality    string `json:"quality,omitempty"`
	OnOverflow string `json:"on_overflow,omitempty"`
}

// ReadGroupConfig describes a block read.
type ReadGroupConfig struct {
	ID             string              `json:"id"`
	Connection     string              `json:"connection,omitempty"`
	Endpoint       EndpointConfig      `json:"endpoint"`
	Function       string              `json:"function"`
	Start          uint16              `json:"start"`
	Length         uint16              `json:"length"`
	TTL            Duration            `json:"ttl"`
	Signals        []ReadSignalConfig  `json:"signals"`
	Disable        bool                `json:"disable,omitempty"`
	CAN            *CANReadGroupConfig `json:"can,omitempty"`
	DriverSettings json.RawMessage     `json:"driver_settings,omitempty"`
	Source         ModuleReference     `json:"-"`
}

// CANReadGroupConfig describes how CAN frames should be ingested from a byte stream.
type CANReadGroupConfig struct {
	Protocol    string                  `json:"protocol,omitempty"`
	Mode        string                  `json:"mode,omitempty"`
	DBC         string                  `json:"dbc"`
	BufferSize  int                     `json:"buffer_size,omitempty"`
	ReadTimeout Duration                `json:"read_timeout,omitempty"`
	Frames      []CANFrameBindingConfig `json:"frames"`
}

// CANFrameBindingConfig maps a CAN message to one or more cell updates.
type CANFrameBindingConfig struct {
	Message  string                   `json:"message,omitempty"`
	FrameID  string                   `json:"frame_id,omitempty"`
	Extended *bool                    `json:"extended,omitempty"`
	Channel  *uint8                   `json:"channel,omitempty"`
	Signals  []CANSignalBindingConfig `json:"signals"`
}

// CANSignalBindingConfig maps a decoded CAN signal to a cell.
type CANSignalBindingConfig struct {
	Name    string   `json:"name"`
	Cell    string   `json:"cell"`
	Scale   *float64 `json:"scale,omitempty"`
	Offset  *float64 `json:"offset,omitempty"`
	Quality *float64 `json:"quality,omitempty"`
}

// WriteTargetConfig describes how a cell is pushed to Modbus.
type WriteTargetConfig struct {
	ID             string          `json:"id"`
	Cell           string          `json:"cell"`
	Connection     string          `json:"connection,omitempty"`
	Endpoint       EndpointConfig  `json:"endpoint"`
	Function       string          `json:"function"`
	Address        uint16          `json:"address"`
	Scale          float64         `json:"scale,omitempty"`
	Endianness     string          `json:"endianness,omitempty"`
	Signed         bool            `json:"signed,omitempty"`
	Deadband       float64         `json:"deadband,omitempty"`
	RateLimit      Duration        `json:"rate_limit,omitempty"`
	Priority       int             `json:"priority,omitempty"`
	Disable        bool            `json:"disable,omitempty"`
	DriverSettings json.RawMessage `json:"driver_settings,omitempty"`
	Source         ModuleReference `json:"-"`
}

// ProgramSignalConfig maps a program signal onto a cell.
type ProgramSignalConfig struct {
	ID       string      `json:"id"`
	Cell     string      `json:"cell"`
	Type     ValueKind   `json:"type,omitempty"`
	Optional bool        `json:"optional,omitempty"`
	Default  interface{} `json:"default,omitempty"`
}

// ProgramConfig describes a reusable processing module.
type ProgramConfig struct {
	ID       string                 `json:"id"`
	Type     string                 `json:"type"`
	Inputs   []ProgramSignalConfig  `json:"inputs,omitempty"`
	Outputs  []ProgramSignalConfig  `json:"outputs,omitempty"`
	Settings map[string]interface{} `json:"settings,omitempty"`
	Metadata json.RawMessage        `json:"metadata,omitempty"`
	Source   ModuleReference        `json:"-"`
}

// HeatmapCooldownConfig controls how long activity stays highlighted in the live view heatmap.
type HeatmapCooldownConfig struct {
	Cells    int `json:"cells,omitempty"`
	Logic    int `json:"logic,omitempty"`
	Programs int `json:"programs,omitempty"`
}

// HeatmapColorConfig customises the palette used in the live view heatmap.
type HeatmapColorConfig struct {
	Background string `json:"background,omitempty"`
	Border     string `json:"border,omitempty"`
	Logic      string `json:"logic,omitempty"`
	Program    string `json:"program,omitempty"`
	Read       string `json:"read,omitempty"`
	Stale      string `json:"stale,omitempty"`
	Write      string `json:"write,omitempty"`
}

// HeatmapConfig configures the optional live view heatmap.
type HeatmapConfig struct {
	Cooldown HeatmapCooldownConfig `json:"cooldown,omitempty"`
	Colors   HeatmapColorConfig    `json:"colors,omitempty"`
}

// LiveViewConfig exposes runtime controls for the live view endpoints.
type LiveViewConfig struct {
	Heatmap HeatmapConfig `json:"heatmap,omitempty"`
}

// DependencyConfig describes a dependency for a logic block.
type DependencyConfig struct {
	Cell string    `json:"cell"`
	Type ValueKind `json:"type"`
	// Threshold defines the minimum delta that should trigger a logic re-evaluation for numeric values.
	Threshold float64 `json:"threshold,omitempty"`
}

// LogicBlockConfig describes a single logic evaluation block.
type LogicBlockConfig struct {
	ID           string             `json:"id"`
	Target       string             `json:"target"`
	Dependencies []DependencyConfig `json:"dependencies"`
	Expression   string             `json:"expression"`
	Valid        string             `json:"valid"`
	Quality      string             `json:"quality"`
	Metadata     json.RawMessage    `json:"metadata,omitempty"`
	Source       ModuleReference    `json:"-"`
}

// HelperFunctionConfig defines a standalone helper function that can be used from logic expressions.
type HelperFunctionConfig struct {
	Name       string   `json:"name"`
	Arguments  []string `json:"arguments"`
	Expression string   `json:"expression"`
}

// DSLConfig configures expression language extensions.
type DSLConfig struct {
	Helpers []HelperFunctionConfig `json:"helpers,omitempty"`
}

// GlobalPolicies configure optional behaviours shared by the controller.
type GlobalPolicies struct {
	RetryBackoff   Duration `json:"retry_backoff,omitempty"`
	RetryMax       int      `json:"retry_max,omitempty"`
	ReadbackVerify bool     `json:"readback_verify,omitempty"`
	WatchdogCell   string   `json:"watchdog_cell,omitempty"`
}

// LokiConfig configures optional Loki integration for logging.
type LokiConfig struct {
	Enabled bool              `json:"enabled"`
	URL     string            `json:"url"`
	Labels  map[string]string `json:"labels"`
}

// LoggingConfig encapsulates runtime logging options.
type LoggingConfig struct {
	Level  string     `json:"level"`
	Format string     `json:"format,omitempty"`
	Loki   LokiConfig `json:"loki"`
}

// TelemetryConfig configures runtime telemetry exporters.
type TelemetryConfig struct {
	Enabled  bool   `json:"enabled"`
	Provider string `json:"provider,omitempty"`
}

// WorkerSlots configures the concurrency for each pipeline stage.
type WorkerSlots struct {
	Read    int `json:"read,omitempty"`
	Program int `json:"program,omitempty"`
	Execute int `json:"execute,omitempty"`
	Write   int `json:"write,omitempty"`
}

// ServerCellConfig maps a cell onto an input register address exposed via the embedded Modbus server.
//
// Deprecated: The embedded Modbus server has been removed. This structure is kept
// only so older configurations continue to decode but it is ignored by the
// service.
type ServerCellConfig struct {
	Cell    string  `json:"cell"`
	Address uint16  `json:"address"`
	Scale   float64 `json:"scale,omitempty"`
	Signed  bool    `json:"signed,omitempty"`
}

// ServerConfig configures the optional embedded Modbus server.
//
// Deprecated: The embedded Modbus server has been removed. The configuration is
// ignored and retained solely for schema compatibility.
type ServerConfig struct {
	Enabled bool               `json:"enabled"`
	Listen  string             `json:"listen"`
	UnitID  uint8              `json:"unit_id,omitempty"`
	Cells   []ServerCellConfig `json:"cells"`
}

// Config is the root configuration structure for the service.
type Config struct {
	Name        string                 `json:"name,omitempty"`
	Description string                 `json:"description,omitempty"`
	Cycle       Duration               `json:"cycle"`
	Logging     LoggingConfig          `json:"logging"`
	LiveView    LiveViewConfig         `json:"live_view,omitempty"`
	Telemetry   TelemetryConfig        `json:"telemetry"`
	Workers     WorkerSlots            `json:"workers,omitempty"`
	Programs    []ProgramConfig        `json:"programs,omitempty"`
	Connections []IOConnectionConfig   `json:"connections,omitempty"`
	Cells       []CellConfig           `json:"cells"`
	Reads       []ReadGroupConfig      `json:"reads"`
	Writes      []WriteTargetConfig    `json:"writes"`
	Logic       []LogicBlockConfig     `json:"logic"`
	DSL         DSLConfig              `json:"dsl"`
	Helpers     []HelperFunctionConfig `json:"helpers,omitempty"`
	Policies    GlobalPolicies         `json:"policies"`
	// Deprecated: the embedded Modbus server has been removed. The field is
	// ignored when loading the configuration and will be dropped in a
	// future release.
	Server    ServerConfig    `json:"server"`
	HotReload bool            `json:"hot_reload,omitempty"`
	Source    ModuleReference `json:"-"`
}

// Load reads and decodes the configuration file from disk.
func Load(path string) (*Config, error) {
	if strings.TrimSpace(path) == "" {
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

	var inst *cue.Instance
	if info.IsDir() {
		inst, err = buildInstance(abs, ".")
	} else {
		inst, err = buildInstance(filepath.Dir(abs), filepath.Base(abs))
	}
	if err != nil {
		return nil, err
	}
	return decodeInstance(abs, inst)
}

func buildInstance(dir, target string) (*cue.Instance, error) {
	cfg := &load.Config{Dir: dir, AcceptLegacyModules: true}
	if overlays := ResolveOverlays(dir); len(overlays) > 0 {
		cfg.Overlay = overlays
	}
	instances := load.Instances([]string{target}, cfg)
	if len(instances) == 0 {
		return nil, fmt.Errorf("no CUE packages found in %s", dir)
	}
	var runtime cue.Runtime
	inst, err := runtime.Build(instances[0])
	if err != nil {
		return nil, fmt.Errorf("build CUE instance: %w", err)
	}
	if err := inst.Err; err != nil {
		return nil, fmt.Errorf("invalid CUE instance: %w", err)
	}
	return inst, nil
}

func decodeInstance(path string, inst *cue.Instance) (*Config, error) {
	value := inst.Value()
	configVal := value.LookupPath(cue.MakePath(cue.Str("config")))
	if !configVal.Exists() {
		configVal = value
	}
	pkgName := strings.TrimSpace(inst.PkgName)
	if pkgField := configVal.LookupPath(cue.MakePath(cue.Str("package"))); pkgField.Exists() {
		if str, err := pkgField.String(); err == nil {
			if trimmed := strings.TrimSpace(str); trimmed != "" {
				pkgName = trimmed
			}
		} else {
			return nil, fmt.Errorf("read package name: %w", err)
		}
	}
	pkgPrefix := computePackagePrefix(inst, pkgName)
	data, err := configVal.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("encode config to JSON: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	normalizeSignalBuffers(&cfg)

	moduleNamespace := strings.TrimSpace(pkgPrefix)
	if moduleNamespace != "" {
		for i := range cfg.Connections {
			if strings.TrimSpace(cfg.Connections[i].Source.Package) == "" {
				cfg.Connections[i].Source.Package = moduleNamespace
			}
		}
		for i := range cfg.Cells {
			if strings.TrimSpace(cfg.Cells[i].Source.Package) == "" {
				cfg.Cells[i].Source.Package = moduleNamespace
			}
		}
		for i := range cfg.Programs {
			if strings.TrimSpace(cfg.Programs[i].Source.Package) == "" {
				cfg.Programs[i].Source.Package = moduleNamespace
			}
		}
		for i := range cfg.Reads {
			if strings.TrimSpace(cfg.Reads[i].Source.Package) == "" {
				cfg.Reads[i].Source.Package = moduleNamespace
			}
		}
		for i := range cfg.Writes {
			if strings.TrimSpace(cfg.Writes[i].Source.Package) == "" {
				cfg.Writes[i].Source.Package = moduleNamespace
			}
		}
		for i := range cfg.Logic {
			if strings.TrimSpace(cfg.Logic[i].Source.Package) == "" {
				cfg.Logic[i].Source.Package = moduleNamespace
			}
		}
	}

	meta := ModuleReference{File: path, Name: cfg.Name, Description: cfg.Description, Package: pkgPrefix}
	cfg.setSource(meta)
	if err := validateSignalBuffers(&cfg); err != nil {
		return nil, err
	}
	qualifyConfig(&cfg, pkgPrefix)
	if err := applyConnectionDefaults(&cfg); err != nil {
		return nil, err
	}
	if err := validateConfigIdentifiers(&cfg); err != nil {
		return nil, err
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

func normalizeSignalBuffers(cfg *Config) {
	if cfg == nil {
		return
	}
	for i := range cfg.Reads {
		for j := range cfg.Reads[i].Signals {
			signal := &cfg.Reads[i].Signals[j]
			if signal.Buffer != nil {
				if signal.Buffer.Capacity != nil {
					signal.BufferSize = *signal.Buffer.Capacity
				}
				signal.Buffer.Aggregator = strings.TrimSpace(signal.Buffer.Aggregator)
				signal.Buffer.OnOverflow = strings.TrimSpace(signal.Buffer.OnOverflow)
			}
			aggName := strings.TrimSpace(signal.Aggregation)
			if aggName != "" {
				signal.Aggregation = aggName
			}
			if signal.Buffer == nil {
				var capPtr *int
				if signal.BufferSize > 0 {
					cap := signal.BufferSize
					capPtr = &cap
				}
				if aggName != "" || capPtr != nil {
					signal.Buffer = &SignalBufferConfig{Capacity: capPtr, Aggregator: aggName}
				}
			}
			if signal.Buffer != nil {
				if signal.Buffer.Aggregator != "" {
					aggName = signal.Buffer.Aggregator
				}
				if signal.Buffer.Capacity == nil && signal.BufferSize > 0 {
					cap := signal.BufferSize
					signal.Buffer.Capacity = &cap
				}
			}
			if len(signal.Aggregations) == 0 {
				target := strings.TrimSpace(signal.Cell)
				aggregation := ReadSignalAggregationConfig{
					Cell:       target,
					Aggregator: aggName,
				}
				if signal.Buffer != nil {
					aggregation.OnOverflow = signal.Buffer.OnOverflow
				}
				signal.Aggregations = append(signal.Aggregations, aggregation)
			}
			for k := range signal.Aggregations {
				agg := &signal.Aggregations[k]
				agg.Cell = strings.TrimSpace(agg.Cell)
				if agg.Cell == "" {
					agg.Cell = strings.TrimSpace(signal.Cell)
				}
				agg.Aggregator = strings.TrimSpace(agg.Aggregator)
				if agg.Aggregator == "" {
					agg.Aggregator = aggName
				}
				agg.Quality = strings.TrimSpace(agg.Quality)
				agg.OnOverflow = strings.TrimSpace(agg.OnOverflow)
				if agg.OnOverflow == "" && signal.Buffer != nil {
					agg.OnOverflow = signal.Buffer.OnOverflow
				}
			}
			if len(signal.Aggregations) > 0 {
				agg := signal.Aggregations[0]
				if agg.Aggregator != "" {
					signal.Aggregation = agg.Aggregator
					if signal.Buffer != nil && signal.Buffer.Aggregator == "" {
						signal.Buffer.Aggregator = agg.Aggregator
					}
				}
			}
		}
	}
}

func validateSignalBuffers(cfg *Config) error {
	if cfg == nil {
		return nil
	}
	for _, read := range cfg.Reads {
		for _, signal := range read.Signals {
			if len(signal.Aggregations) == 0 {
				return fmt.Errorf("read group %s signal %s: at least one aggregation is required", read.ID, signal.Cell)
			}
			for _, aggCfg := range signal.Aggregations {
				if aggCfg.Cell == "" {
					return fmt.Errorf("read group %s signal %s: aggregation missing cell", read.ID, signal.Cell)
				}
				if agg := strings.TrimSpace(aggCfg.Aggregator); agg != "" {
					if !isAllowedSignalAggregator(agg) {
						return fmt.Errorf("read group %s signal %s aggregation %s: unsupported aggregation %q", read.ID, signal.Cell, aggCfg.Cell, agg)
					}
				}
			}
			if agg := strings.TrimSpace(signal.Aggregation); agg != "" {
				if !isAllowedSignalAggregator(agg) {
					return fmt.Errorf("read group %s signal %s: unsupported aggregation %q", read.ID, signal.Cell, agg)
				}
			}
			if signal.Buffer != nil {
				if signal.Buffer.Capacity != nil {
					if *signal.Buffer.Capacity <= 0 {
						return fmt.Errorf("read group %s signal %s: buffer capacity must be positive", read.ID, signal.Cell)
					}
				}
				if agg := strings.TrimSpace(signal.Buffer.Aggregator); agg != "" {
					if !isAllowedSignalAggregator(agg) {
						return fmt.Errorf("read group %s signal %s: unsupported buffer aggregator %q", read.ID, signal.Cell, agg)
					}
				}
			}
			if signal.BufferSize < 0 {
				return fmt.Errorf("read group %s signal %s: buffer size must not be negative", read.ID, signal.Cell)
			}
		}
	}
	return nil
}

func isAllowedSignalAggregator(value string) bool {
	if value == "" {
		return true
	}
	_, ok := allowedSignalAggregators[value]
	return ok
}

func applyConnectionDefaults(cfg *Config) error {
	if cfg == nil {
		return nil
	}
	connections := make(map[string]*IOConnectionConfig, len(cfg.Connections))
	for i := range cfg.Connections {
		conn := &cfg.Connections[i]
		conn.ID = strings.TrimSpace(conn.ID)
		if conn.ID == "" {
			return errors.New("connection identifier must not be empty")
		}
		if _, exists := connections[conn.ID]; exists {
			return fmt.Errorf("duplicate connection %s", conn.ID)
		}
		conn.Driver = strings.TrimSpace(conn.Driver)
		conn.Endpoint.Driver = strings.TrimSpace(conn.Endpoint.Driver)
		conn.Endpoint.Address = strings.TrimSpace(conn.Endpoint.Address)
		if conn.Driver == "" {
			conn.Driver = conn.Endpoint.Driver
		}
		if conn.Driver == "" {
			return fmt.Errorf("connection %s: driver must not be empty", conn.ID)
		}
		if conn.Endpoint.Driver == "" {
			conn.Endpoint.Driver = conn.Driver
		}
		connections[conn.ID] = conn
	}

	cloneSettings := func(base json.RawMessage) json.RawMessage {
		if len(base) == 0 {
			return nil
		}
		cloned := make([]byte, len(base))
		copy(cloned, base)
		return cloned
	}

	mergeEndpoint := func(target *EndpointConfig, base EndpointConfig) {
		if target == nil {
			return
		}
		merged := base
		if addr := strings.TrimSpace(target.Address); addr != "" {
			merged.Address = addr
		}
		if target.UnitID != 0 || base.UnitID == 0 {
			merged.UnitID = target.UnitID
		}
		if target.Timeout.Duration != 0 || base.Timeout.Duration == 0 {
			merged.Timeout = target.Timeout
		}
		if drv := strings.TrimSpace(target.Driver); drv != "" {
			merged.Driver = drv
		}
		*target = merged
	}

	for i := range cfg.Reads {
		read := &cfg.Reads[i]
		read.Connection = strings.TrimSpace(read.Connection)
		read.Endpoint.Driver = strings.TrimSpace(read.Endpoint.Driver)
		read.Endpoint.Address = strings.TrimSpace(read.Endpoint.Address)
		if read.Connection == "" {
			continue
		}
		conn, ok := connections[read.Connection]
		if !ok {
			return fmt.Errorf("read group %s: unknown connection %s", read.ID, read.Connection)
		}
		mergeEndpoint(&read.Endpoint, conn.Endpoint)
		if read.Endpoint.Driver == "" {
			read.Endpoint.Driver = conn.Driver
		}
		if read.Endpoint.Driver != conn.Driver {
			return fmt.Errorf("read group %s: connection %s uses driver %s but read requests %s", read.ID, read.Connection, conn.Driver, read.Endpoint.Driver)
		}
		if len(read.DriverSettings) == 0 && len(conn.DriverSettings) > 0 {
			read.DriverSettings = cloneSettings(conn.DriverSettings)
		}
	}

	for i := range cfg.Writes {
		write := &cfg.Writes[i]
		write.Connection = strings.TrimSpace(write.Connection)
		write.Endpoint.Driver = strings.TrimSpace(write.Endpoint.Driver)
		write.Endpoint.Address = strings.TrimSpace(write.Endpoint.Address)
		if write.Connection == "" {
			continue
		}
		conn, ok := connections[write.Connection]
		if !ok {
			return fmt.Errorf("write target %s: unknown connection %s", write.ID, write.Connection)
		}
		mergeEndpoint(&write.Endpoint, conn.Endpoint)
		if write.Endpoint.Driver == "" {
			write.Endpoint.Driver = conn.Driver
		}
		if write.Endpoint.Driver != conn.Driver {
			return fmt.Errorf("write target %s: connection %s uses driver %s but write requests %s", write.ID, write.Connection, conn.Driver, write.Endpoint.Driver)
		}
		if len(write.DriverSettings) == 0 && len(conn.DriverSettings) > 0 {
			write.DriverSettings = cloneSettings(conn.DriverSettings)
		}
	}
	return nil
}

func qualifyConfig(cfg *Config, pkg string) {
	if cfg == nil {
		return
	}
	root := strings.TrimSpace(pkg)
	qualifyID := func(namespace, id string) string {
		return qualifyIdentifier(namespace, id)
	}
	qualifyRef := func(namespace, value string) string {
		return qualifyReference(namespace, value)
	}
	for i := range cfg.Connections {
		namespace := combineNamespaces(root, cfg.Connections[i].Source.Package)
		cfg.Connections[i].ID = qualifyID(namespace, cfg.Connections[i].ID)
	}
	for i := range cfg.Cells {
		namespace := combineNamespaces(root, cfg.Cells[i].Source.Package)
		cfg.Cells[i].ID = qualifyID(namespace, cfg.Cells[i].ID)
	}
	for i := range cfg.Programs {
		namespace := combineNamespaces(root, cfg.Programs[i].Source.Package)
		cfg.Programs[i].ID = qualifyID(namespace, cfg.Programs[i].ID)
		for j := range cfg.Programs[i].Inputs {
			cfg.Programs[i].Inputs[j].Cell = qualifyRef(namespace, cfg.Programs[i].Inputs[j].Cell)
		}
		for j := range cfg.Programs[i].Outputs {
			cfg.Programs[i].Outputs[j].Cell = qualifyRef(namespace, cfg.Programs[i].Outputs[j].Cell)
		}
	}
	for i := range cfg.Reads {
		namespace := combineNamespaces(root, cfg.Reads[i].Source.Package)
		cfg.Reads[i].ID = qualifyID(namespace, cfg.Reads[i].ID)
		cfg.Reads[i].Connection = qualifyRef(namespace, cfg.Reads[i].Connection)
		for j := range cfg.Reads[i].Signals {
			cfg.Reads[i].Signals[j].Cell = qualifyRef(namespace, cfg.Reads[i].Signals[j].Cell)
			for k := range cfg.Reads[i].Signals[j].Aggregations {
				cfg.Reads[i].Signals[j].Aggregations[k].Cell = qualifyRef(namespace, cfg.Reads[i].Signals[j].Aggregations[k].Cell)
				cfg.Reads[i].Signals[j].Aggregations[k].Quality = qualifyRef(namespace, cfg.Reads[i].Signals[j].Aggregations[k].Quality)
			}
		}
		if cfg.Reads[i].CAN != nil {
			for j := range cfg.Reads[i].CAN.Frames {
				for k := range cfg.Reads[i].CAN.Frames[j].Signals {
					cfg.Reads[i].CAN.Frames[j].Signals[k].Cell = qualifyRef(namespace, cfg.Reads[i].CAN.Frames[j].Signals[k].Cell)
				}
			}
		}
	}
	for i := range cfg.Writes {
		namespace := combineNamespaces(root, cfg.Writes[i].Source.Package)
		cfg.Writes[i].ID = qualifyID(namespace, cfg.Writes[i].ID)
		cfg.Writes[i].Cell = qualifyRef(namespace, cfg.Writes[i].Cell)
		cfg.Writes[i].Connection = qualifyRef(namespace, cfg.Writes[i].Connection)
	}
	for i := range cfg.Logic {
		namespace := combineNamespaces(root, cfg.Logic[i].Source.Package)
		cfg.Logic[i].ID = qualifyID(namespace, cfg.Logic[i].ID)
		cfg.Logic[i].Target = qualifyRef(namespace, cfg.Logic[i].Target)
		for j := range cfg.Logic[i].Dependencies {
			cfg.Logic[i].Dependencies[j].Cell = qualifyRef(namespace, cfg.Logic[i].Dependencies[j].Cell)
		}
	}
	serverNamespace := combineNamespaces(root, cfg.Source.Package)
	for i := range cfg.Server.Cells {
		cfg.Server.Cells[i].Cell = qualifyRef(serverNamespace, cfg.Server.Cells[i].Cell)
	}
	cfg.Policies.WatchdogCell = qualifyRef(serverNamespace, cfg.Policies.WatchdogCell)
}

func combineNamespaces(root, module string) string {
	root = strings.TrimSpace(root)
	module = strings.TrimSpace(module)
	switch {
	case root == "" && module == "":
		return ""
	case root == "":
		return module
	case module == "":
		return root
	case module == root || strings.HasPrefix(module, root+"."):
		return module
	default:
		return strings.TrimSuffix(root, ".") + "." + strings.Trim(module, ".")
	}
}

func qualifyIdentifier(pkg, id string) string {
	trimmed := strings.TrimSpace(id)
	if trimmed == "" {
		return trimmed
	}
	if pkg == "" || strings.Contains(trimmed, ":") {
		return trimmed
	}
	return pkg + ":" + trimmed
}

func qualifyReference(pkg, value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return trimmed
	}
	if strings.Contains(trimmed, ":") || pkg == "" {
		return trimmed
	}
	return pkg + ":" + trimmed
}

func validateConfigIdentifiers(cfg *Config) error {
	if cfg == nil {
		return nil
	}
	for _, conn := range cfg.Connections {
		if err := ensureIdentifier(conn.ID, "connection"); err != nil {
			return err
		}
	}
	for _, cell := range cfg.Cells {
		if err := ensureIdentifier(cell.ID, "cell"); err != nil {
			return err
		}
	}
	for _, program := range cfg.Programs {
		if err := ensureIdentifier(program.ID, "program"); err != nil {
			return err
		}
	}
	for _, logic := range cfg.Logic {
		if err := ensureIdentifier(logic.ID, "logic block"); err != nil {
			return err
		}
	}
	for _, read := range cfg.Reads {
		if err := ensureIdentifier(read.ID, "read group"); err != nil {
			return err
		}
	}
	for _, write := range cfg.Writes {
		if err := ensureIdentifier(write.ID, "write target"); err != nil {
			return err
		}
	}
	return nil
}

func ensureIdentifier(value, kind string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fmt.Errorf("%s identifier must not be empty", kind)
	}
	if strings.Count(trimmed, ":") != 1 {
		return fmt.Errorf("%s %q must be qualified as <package>:<local>", kind, trimmed)
	}
	parts := strings.SplitN(trimmed, ":", 2)
	pkgPart := parts[0]
	local := parts[1]
	if pkgPart == "" || local == "" {
		return fmt.Errorf("%s %q must be qualified as <package>:<local>", kind, trimmed)
	}
	if err := validateIdentifierSegments(pkgPart, "package segment", kind, trimmed); err != nil {
		return err
	}
	if strings.Contains(local, ":") {
		return fmt.Errorf("%s %q has an unexpected colon in the local identifier", kind, trimmed)
	}
	if err := validateIdentifierSegments(local, "local segment", kind, trimmed); err != nil {
		return err
	}
	return nil
}

func validateIdentifierSegments(value, segmentKind, kind, full string) error {
	for _, part := range strings.Split(value, ".") {
		if part == "" {
			return fmt.Errorf("%s %q contains an empty %s", kind, full, segmentKind)
		}
		for idx, r := range part {
			if idx == 0 && unicode.IsDigit(r) {
				return fmt.Errorf("%s %q must not start with a digit", kind, full)
			}
			if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_') {
				return fmt.Errorf("%s %q contains invalid character %q", kind, full, r)
			}
		}
	}
	return nil
}

func computePackagePrefix(inst *cue.Instance, explicit string) string {
	trimmed := strings.TrimSpace(explicit)
	if trimmed != "" {
		return trimmed
	}
	if inst == nil {
		return ""
	}
	if path := strings.Trim(strings.TrimSpace(inst.ImportPath), "/"); path != "" {
		replacer := strings.NewReplacer("/", ".", ":", ".")
		if converted := replacer.Replace(path); strings.TrimSpace(converted) != "" {
			return strings.Trim(converted, ".")
		}
	}
	if name := strings.TrimSpace(inst.PkgName); name != "" {
		return name
	}
	return ""
}

func (c *Config) setSource(meta ModuleReference) {
	if c == nil {
		return
	}
	if meta.File == "" {
		meta.File = c.Source.File
	}
	if meta.Name == "" {
		meta.Name = c.Name
	}
	if meta.Description == "" {
		meta.Description = c.Description
	}
	c.Source = meta
	c.applySource(meta)
}

func (c *Config) applySource(meta ModuleReference) {
	if c == nil {
		return
	}
	for i := range c.Connections {
		c.Connections[i].Source = mergeInitialSource(c.Connections[i].Source, meta)
	}
	for i := range c.Cells {
		c.Cells[i].Source = mergeInitialSource(c.Cells[i].Source, meta)
	}
	for i := range c.Programs {
		c.Programs[i].Source = mergeInitialSource(c.Programs[i].Source, meta)
	}
	for i := range c.Reads {
		c.Reads[i].Source = mergeInitialSource(c.Reads[i].Source, meta)
	}
	for i := range c.Writes {
		c.Writes[i].Source = mergeInitialSource(c.Writes[i].Source, meta)
	}
	for i := range c.Logic {
		c.Logic[i].Source = mergeInitialSource(c.Logic[i].Source, meta)
	}
}

func mergeInitialSource(child, meta ModuleReference) ModuleReference {
	if child.File == "" && meta.File != "" {
		child.File = meta.File
	}
	if child.Name == "" && meta.Name != "" {
		child.Name = meta.Name
	}
	if child.Description == "" && meta.Description != "" {
		child.Description = meta.Description
	}
	if child.Package == "" && meta.Package != "" {
		child.Package = meta.Package
	}
	return child
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
