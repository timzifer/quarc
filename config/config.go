package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"

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

// EndpointConfig describes how to reach a Modbus slave.
type EndpointConfig struct {
	Address string   `yaml:"address"`
	UnitID  uint8    `yaml:"unit_id"`
	Timeout Duration `yaml:"timeout,omitempty"`
	Driver  string   `yaml:"driver,omitempty"`
}

// ModuleReference captures metadata about the configuration source that defined an entry.
type ModuleReference struct {
	File        string `json:"file,omitempty"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	Package     string `json:"package,omitempty"`
}

// ModuleInclude describes a referenced configuration module.
type ModuleInclude struct {
	Path        string
	Name        string
	Description string
}

// UnmarshalYAML allows module includes to be declared either as scalar strings or structured objects.
func (m *ModuleInclude) UnmarshalYAML(value *yaml.Node) error {
	if value == nil {
		return errors.New("module include node is nil")
	}
	switch value.Kind {
	case yaml.ScalarNode:
		var path string
		if err := value.Decode(&path); err != nil {
			return fmt.Errorf("decode module path: %w", err)
		}
		m.Path = strings.TrimSpace(path)
		return nil
	case yaml.MappingNode:
		type rawModule struct {
			Path        string `yaml:"path"`
			Name        string `yaml:"name"`
			Description string `yaml:"description"`
		}
		var raw rawModule
		if err := value.Decode(&raw); err != nil {
			return fmt.Errorf("decode module include: %w", err)
		}
		if raw.Path == "" {
			return errors.New("module include missing path")
		}
		m.Path = raw.Path
		m.Name = raw.Name
		m.Description = raw.Description
		return nil
	default:
		return fmt.Errorf("unsupported module include node kind %d", value.Kind)
	}
}

// CellConfig configures a local memory cell.
type CellConfig struct {
	ID          string          `yaml:"id"`
	Type        ValueKind       `yaml:"type"`
	Unit        string          `yaml:"unit,omitempty"`
	TTL         Duration        `yaml:"ttl,omitempty"`
	Scale       float64         `yaml:"scale,omitempty"`
	Constant    interface{}     `yaml:"constant,omitempty"`
	Metadata    yaml.Node       `yaml:"metadata,omitempty"`
	Name        string          `yaml:"name,omitempty"`
	Description string          `yaml:"description,omitempty"`
	Source      ModuleReference `yaml:"-"`
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
	ID             string              `yaml:"id"`
	Endpoint       EndpointConfig      `yaml:"endpoint"`
	Function       string              `yaml:"function"`
	Start          uint16              `yaml:"start"`
	Length         uint16              `yaml:"length"`
	TTL            Duration            `yaml:"ttl"`
	Signals        []ReadSignalConfig  `yaml:"signals"`
	Disable        bool                `yaml:"disable,omitempty"`
	CAN            *CANReadGroupConfig `yaml:"can,omitempty"`
	DriverSettings *yaml.Node          `yaml:"driver_settings,omitempty"`
	Source         ModuleReference     `yaml:"-"`
}

// CANReadGroupConfig describes how CAN frames should be ingested from a byte stream.
type CANReadGroupConfig struct {
	Protocol    string                  `yaml:"protocol,omitempty"`
	Mode        string                  `yaml:"mode,omitempty"`
	DBC         string                  `yaml:"dbc"`
	BufferSize  int                     `yaml:"buffer_size,omitempty"`
	ReadTimeout Duration                `yaml:"read_timeout,omitempty"`
	Frames      []CANFrameBindingConfig `yaml:"frames"`
}

// CANFrameBindingConfig maps a CAN message to one or more cell updates.
type CANFrameBindingConfig struct {
	Message  string                   `yaml:"message,omitempty"`
	FrameID  string                   `yaml:"frame_id,omitempty"`
	Extended *bool                    `yaml:"extended,omitempty"`
	Channel  *uint8                   `yaml:"channel,omitempty"`
	Signals  []CANSignalBindingConfig `yaml:"signals"`
}

// CANSignalBindingConfig maps a decoded CAN signal to a cell.
type CANSignalBindingConfig struct {
	Name    string   `yaml:"name"`
	Cell    string   `yaml:"cell"`
	Scale   *float64 `yaml:"scale,omitempty"`
	Offset  *float64 `yaml:"offset,omitempty"`
	Quality *float64 `yaml:"quality,omitempty"`
}

// WriteTargetConfig describes how a cell is pushed to Modbus.
type WriteTargetConfig struct {
	ID             string          `yaml:"id"`
	Cell           string          `yaml:"cell"`
	Endpoint       EndpointConfig  `yaml:"endpoint"`
	Function       string          `yaml:"function"`
	Address        uint16          `yaml:"address"`
	Scale          float64         `yaml:"scale,omitempty"`
	Endianness     string          `yaml:"endianness,omitempty"`
	Signed         bool            `yaml:"signed,omitempty"`
	Deadband       float64         `yaml:"deadband,omitempty"`
	RateLimit      Duration        `yaml:"rate_limit,omitempty"`
	Priority       int             `yaml:"priority,omitempty"`
	Disable        bool            `yaml:"disable,omitempty"`
	DriverSettings *yaml.Node      `yaml:"driver_settings,omitempty"`
	Source         ModuleReference `yaml:"-"`
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
	Source   ModuleReference        `yaml:"-"`
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
	Source       ModuleReference    `yaml:"-"`
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

// TelemetryConfig configures runtime telemetry exporters.
type TelemetryConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Provider string `yaml:"provider,omitempty"`
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
	Name        string                 `yaml:"name,omitempty"`
	Description string                 `yaml:"description,omitempty"`
	Cycle       Duration               `yaml:"cycle"`
	Logging     LoggingConfig          `yaml:"logging"`
	Telemetry   TelemetryConfig        `yaml:"telemetry"`
	Modules     []ModuleInclude        `yaml:"modules"`
	Workers     WorkerSlots            `yaml:"workers,omitempty"`
	Programs    []ProgramConfig        `yaml:"programs,omitempty"`
	Cells       []CellConfig           `yaml:"cells"`
	Reads       []ReadGroupConfig      `yaml:"reads"`
	Writes      []WriteTargetConfig    `yaml:"writes"`
	Logic       []LogicBlockConfig     `yaml:"logic"`
	DSL         DSLConfig              `yaml:"dsl"`
	Helpers     []HelperFunctionConfig `yaml:"helpers,omitempty"`
	Policies    GlobalPolicies         `yaml:"policies"`
	Server      ServerConfig           `yaml:"server"`
	HotReload   bool                   `yaml:"hot_reload,omitempty"`
	Source      ModuleReference        `yaml:"-"`
}

type moduleContext struct {
	packagePath []string
	templates   map[string]*yaml.Node
	values      map[string]*yaml.Node
}

type moduleResult struct {
	cfg         *Config
	packageName string
	packagePath []string
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
	ctx := moduleContext{
		templates: make(map[string]*yaml.Node),
		values:    make(map[string]*yaml.Node),
	}

	var result *moduleResult
	if info.IsDir() {
		result, err = loadDir(abs, visited, ctx)
	} else {
		result, err = loadFile(abs, visited, ctx)
	}
	if err != nil {
		return nil, err
	}
	if result == nil {
		return &Config{}, nil
	}
	return result.cfg, nil
}

// CycleInterval returns the configured controller cycle duration.
func (c *Config) CycleInterval() time.Duration {
	if c == nil || c.Cycle.Duration <= 0 {
		return 500 * time.Millisecond
	}
	return c.Cycle.Duration
}

func loadFile(path string, visited map[string]struct{}, ctx moduleContext) (*moduleResult, error) {
	if _, ok := visited[path]; ok {
		return nil, fmt.Errorf("config include cycle detected at %s", path)
	}
	visited[path] = struct{}{}
	defer delete(visited, path)

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	prefix, err := renderTemplateInjection(ctx.templates)
	if err != nil {
		return nil, fmt.Errorf("prepare templates for %s: %w", path, err)
	}
	if len(prefix) > 0 {
		combined := make([]byte, 0, len(prefix)+len(raw))
		combined = append(combined, prefix...)
		combined = append(combined, raw...)
		raw = combined
	}

	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("unmarshal config %s: %w", path, err)
	}
	if len(document.Content) == 0 || document.Content[0] == nil {
		return nil, fmt.Errorf("config %s is empty", path)
	}

	root := document.Content[0]
	if root.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("config %s: top-level YAML document must be a mapping", path)
	}

	pkgName, err := extractPackageName(root)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if pkgName == "" {
		return nil, fmt.Errorf("%s: package name is required", path)
	}
	if err := ensureIdentifier(pkgName, "package"); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	fullPath := append([]string{}, ctx.packagePath...)
	if len(fullPath) == 0 {
		fullPath = append(fullPath, pkgName)
	} else {
		expected := fullPath[len(fullPath)-1]
		if expected != pkgName {
			return nil, fmt.Errorf("package mismatch: expected %q, got %q", expected, pkgName)
		}
	}
	packagePath := joinPackagePath(fullPath)

	valuesMap := copyValueMap(ctx.values)
	if err := loadValuesIntoMap(root, filepath.Dir(path), valuesMap); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	templates := collectTemplateDefinitions(root)
	aggregatedTemplates := mergeTemplateMaps(ctx.templates, templates)

	if err := resolveValueTags(root, valuesMap); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	var cfg Config
	if err := root.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config %s: %w", path, err)
	}

	cfg.setSource(ModuleReference{File: path, Name: cfg.Name, Description: cfg.Description, Package: packagePath})

	modules := cfg.Modules
	cfg.Modules = nil

	baseDir := filepath.Dir(path)
	for _, module := range modules {
		if module.Path == "" {
			continue
		}
		modulePath := module.Path
		if !filepath.IsAbs(modulePath) {
			modulePath = filepath.Join(baseDir, module.Path)
		}

		info, err := os.Stat(modulePath)
		if err != nil {
			return nil, fmt.Errorf("load module %s: %w", module.Path, err)
		}

		childPackagePath, err := computeChildPackagePath(baseDir, modulePath, fullPath, info.IsDir())
		if err != nil {
			return nil, fmt.Errorf("load module %s: %w", module.Path, err)
		}

		childCtx := moduleContext{
			packagePath: childPackagePath,
			templates:   copyTemplateMap(aggregatedTemplates),
			values:      copyValueMap(valuesMap),
		}

		var result *moduleResult
		if info.IsDir() {
			result, err = loadDir(modulePath, visited, childCtx)
		} else {
			result, err = loadFile(modulePath, visited, childCtx)
		}
		if err != nil {
			return nil, fmt.Errorf("load module %s: %w", module.Path, err)
		}
		if result == nil || result.cfg == nil {
			continue
		}
		if len(childPackagePath) > 0 && !packagePathIsDescendant(result.packagePath, childPackagePath) {
			return nil, fmt.Errorf("module %s declares package %s outside parent package %s", module.Path, joinPackagePath(result.packagePath), packagePath)
		}
		override := ModuleReference{
			Name:        firstNonEmpty(module.Name, result.cfg.Source.Name),
			Description: firstNonEmpty(module.Description, result.cfg.Source.Description),
		}
		result.cfg.applyModuleMetadata(override)
		mergeConfig(&cfg, result.cfg)
	}

	if err := validateConfigIdentifiers(&cfg); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	return &moduleResult{cfg: &cfg, packageName: pkgName, packagePath: fullPath}, nil
}

func loadDir(path string, visited map[string]struct{}, ctx moduleContext) (*moduleResult, error) {
	if _, ok := visited[path]; ok {
		return nil, fmt.Errorf("config include cycle detected at %s", path)
	}
	visited[path] = struct{}{}
	defer delete(visited, path)

	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("read config dir %s: %w", path, err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	result := &Config{}
	result.setSource(ModuleReference{File: path, Package: joinPackagePath(ctx.packagePath)})

	var dirPackage []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if isValuesFile(name) {
			continue
		}
		ext := strings.ToLower(filepath.Ext(name))
		if ext != ".yaml" && ext != ".yml" {
			continue
		}
		subPath := filepath.Join(path, name)
		res, err := loadFile(subPath, visited, ctx)
		if err != nil {
			return nil, err
		}
		if res == nil || res.cfg == nil {
			continue
		}
		if len(dirPackage) == 0 {
			dirPackage = append([]string(nil), res.packagePath...)
		} else if !equalPackagePath(dirPackage, res.packagePath) {
			return nil, fmt.Errorf("%s: inconsistent package declarations (%s vs %s)", path, joinPackagePath(dirPackage), joinPackagePath(res.packagePath))
		}
		mergeConfig(result, res.cfg)
	}

	return &moduleResult{cfg: result, packagePath: dirPackage, packageName: lastPackageSegment(dirPackage)}, nil
}

func extractPackageName(root *yaml.Node) (string, error) {
	if root == nil {
		return "", nil
	}
	var pkg string
	for i := 0; i+1 < len(root.Content); i += 2 {
		key := root.Content[i]
		if key == nil || key.Kind != yaml.ScalarNode {
			continue
		}
		if strings.TrimSpace(key.Value) != "package" {
			continue
		}
		value := root.Content[i+1]
		if value == nil {
			continue
		}
		if err := value.Decode(&pkg); err != nil {
			return "", fmt.Errorf("invalid package declaration: %w", err)
		}
	}
	return strings.TrimSpace(pkg), nil
}

func ensureIdentifier(value, kind string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fmt.Errorf("%s identifier must not be empty", kind)
	}
	if strings.Contains(trimmed, ".") {
		return fmt.Errorf("%s %q must not contain '.'", kind, trimmed)
	}
	for idx, r := range trimmed {
		if idx == 0 && unicode.IsDigit(r) {
			return fmt.Errorf("%s %q must not start with a digit", kind, trimmed)
		}
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_') {
			return fmt.Errorf("%s %q contains invalid character %q", kind, trimmed, r)
		}
	}
	return nil
}

func copyValueMap(src map[string]*yaml.Node) map[string]*yaml.Node {
	if len(src) == 0 {
		return make(map[string]*yaml.Node)
	}
	dst := make(map[string]*yaml.Node, len(src))
	for key, node := range src {
		dst[key] = cloneNode(node)
	}
	return dst
}

func copyTemplateMap(src map[string]*yaml.Node) map[string]*yaml.Node {
	if len(src) == 0 {
		return make(map[string]*yaml.Node)
	}
	dst := make(map[string]*yaml.Node, len(src))
	for key, node := range src {
		dst[key] = cloneNode(node)
	}
	return dst
}

func mergeTemplateMaps(parent, current map[string]*yaml.Node) map[string]*yaml.Node {
	merged := copyTemplateMap(parent)
	for key, node := range current {
		merged[key] = cloneNode(node)
	}
	return merged
}

func cloneNode(n *yaml.Node) *yaml.Node {
	if n == nil {
		return nil
	}
	clone := *n
	if len(n.Content) > 0 {
		clone.Content = make([]*yaml.Node, len(n.Content))
		for i, child := range n.Content {
			clone.Content[i] = cloneNode(child)
		}
	}
	if n.Alias != nil {
		clone.Alias = cloneNode(n.Alias)
	}
	return &clone
}

func renderTemplateInjection(templates map[string]*yaml.Node) ([]byte, error) {
	if len(templates) == 0 {
		return nil, nil
	}
	names := make([]string, 0, len(templates))
	for name := range templates {
		names = append(names, name)
	}
	sort.Strings(names)

	mapping := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	for _, name := range names {
		key := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: name}
		value := cloneNode(templates[name])
		if value != nil && value.Anchor == "" {
			value.Anchor = name
		}
		mapping.Content = append(mapping.Content, key, value)
	}

	root := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	root.Content = append(root.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "__template_inject__"},
		mapping,
	)

	document := &yaml.Node{Kind: yaml.DocumentNode, Content: []*yaml.Node{root}}
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(document); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func collectTemplateDefinitions(root *yaml.Node) map[string]*yaml.Node {
	templates := make(map[string]*yaml.Node)
	if root == nil || root.Kind != yaml.MappingNode {
		return templates
	}
	for i := 0; i+1 < len(root.Content); i += 2 {
		key := root.Content[i]
		if key == nil || key.Kind != yaml.ScalarNode || strings.TrimSpace(key.Value) != "template" {
			continue
		}
		value := root.Content[i+1]
		if value == nil || value.Kind != yaml.MappingNode {
			continue
		}
		for j := 0; j+1 < len(value.Content); j += 2 {
			nameNode := value.Content[j]
			if nameNode == nil || nameNode.Kind != yaml.ScalarNode {
				continue
			}
			name := strings.TrimSpace(nameNode.Value)
			if name == "" {
				continue
			}
			tpl := cloneNode(value.Content[j+1])
			if tpl != nil && tpl.Anchor == "" {
				tpl.Anchor = name
			}
			templates[name] = tpl
		}
	}
	return templates
}

func loadValuesIntoMap(root *yaml.Node, baseDir string, values map[string]*yaml.Node) error {
	if root == nil || root.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(root.Content); i += 2 {
		key := root.Content[i]
		if key == nil || key.Kind != yaml.ScalarNode || strings.TrimSpace(key.Value) != "values" {
			continue
		}
		seq := root.Content[i+1]
		if seq == nil {
			continue
		}
		if seq.Kind != yaml.SequenceNode {
			return fmt.Errorf("values block must be a sequence")
		}
		for _, item := range seq.Content {
			if item == nil {
				continue
			}
			switch item.Kind {
			case yaml.ScalarNode:
				var ref string
				if err := item.Decode(&ref); err != nil {
					return fmt.Errorf("decode values entry: %w", err)
				}
				ref = strings.TrimSpace(ref)
				if ref == "" {
					continue
				}
				path := ref
				if !filepath.IsAbs(path) {
					path = filepath.Join(baseDir, ref)
				}
				loaded, err := loadValueFile(path)
				if err != nil {
					return fmt.Errorf("load values %s: %w", ref, err)
				}
				for name, node := range loaded {
					values[name] = node
				}
			case yaml.MappingNode:
				inline := extractValuesFromMapping(item)
				for name, node := range inline {
					values[name] = node
				}
			default:
				return fmt.Errorf("values entry at line %d must be a string path or mapping", item.Line)
			}
		}
	}
	return nil
}

func isValuesFile(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasSuffix(lower, ".values.yaml") || strings.HasSuffix(lower, ".values.yml")
}

func loadValueFile(path string) (map[string]*yaml.Node, error) {
	if !isValuesFile(filepath.Base(path)) {
		return nil, fmt.Errorf("values file %s must end with .values.yaml or .values.yml", path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		return nil, err
	}
	if len(document.Content) == 0 || document.Content[0] == nil {
		return nil, fmt.Errorf("values file %s is empty", path)
	}
	root := document.Content[0]
	if root.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("values file %s must contain a mapping", path)
	}
	return extractValuesFromMapping(root), nil
}

func extractValuesFromMapping(node *yaml.Node) map[string]*yaml.Node {
	result := make(map[string]*yaml.Node)
	if node == nil || node.Kind != yaml.MappingNode {
		return result
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		keyNode := node.Content[i]
		if keyNode == nil || keyNode.Kind != yaml.ScalarNode {
			continue
		}
		name := strings.TrimSpace(keyNode.Value)
		if name == "" {
			continue
		}
		result[name] = cloneNode(node.Content[i+1])
	}
	return result
}

func resolveValueTags(node *yaml.Node, values map[string]*yaml.Node) error {
	if node == nil {
		return nil
	}
	switch node.Kind {
	case yaml.DocumentNode, yaml.SequenceNode, yaml.MappingNode:
		for _, child := range node.Content {
			if err := resolveValueTags(child, values); err != nil {
				return err
			}
		}
	case yaml.ScalarNode:
		if node.Tag == "" || strings.HasPrefix(node.Tag, "!!") {
			return nil
		}
		if !strings.HasPrefix(node.Tag, "!") {
			return nil
		}
		key := strings.TrimPrefix(node.Tag, "!")
		if key == "" {
			return fmt.Errorf("invalid value reference at line %d", node.Line)
		}
		value, ok := values[key]
		if !ok {
			return fmt.Errorf("unknown value reference %q at line %d", key, node.Line)
		}
		replacement := cloneNode(value)
		if replacement == nil {
			return fmt.Errorf("value %q resolved to nil", key)
		}
		node.Kind = replacement.Kind
		node.Style = replacement.Style
		node.Tag = replacement.Tag
		node.Value = replacement.Value
		node.Anchor = replacement.Anchor
		node.Alias = replacement.Alias
		node.Content = make([]*yaml.Node, len(replacement.Content))
		for i := range replacement.Content {
			node.Content[i] = cloneNode(replacement.Content[i])
		}
	}
	return nil
}

func computeChildPackagePath(baseDir, modulePath string, parent []string, isDir bool) ([]string, error) {
	rel, err := filepath.Rel(baseDir, modulePath)
	if err != nil {
		return nil, err
	}
	rel = filepath.Clean(rel)
	if strings.HasPrefix(rel, "..") {
		return nil, fmt.Errorf("module path %s escapes base directory", modulePath)
	}
	segments := make([]string, 0)
	if rel != "." {
		parts := strings.Split(rel, string(os.PathSeparator))
		for _, part := range parts {
			if part == "" || part == "." {
				continue
			}
			segments = append(segments, part)
		}
	}
	if !isDir && len(segments) > 0 {
		segments = segments[:len(segments)-1]
	}
	child := append([]string{}, parent...)
	child = append(child, segments...)
	return child, nil
}

func packagePathIsDescendant(child, parent []string) bool {
	if len(child) < len(parent) {
		return false
	}
	for i := range parent {
		if child[i] != parent[i] {
			return false
		}
	}
	return true
}

func equalPackagePath(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func joinPackagePath(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, ".")
}

func lastPackageSegment(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

func validateConfigIdentifiers(cfg *Config) error {
	if cfg == nil {
		return nil
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
	if src.Telemetry.Enabled || src.Telemetry.Provider != "" {
		dst.Telemetry = src.Telemetry
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
	if src.HotReload {
		dst.HotReload = true
	}

	dst.Programs = append(dst.Programs, src.Programs...)
	dst.Cells = append(dst.Cells, src.Cells...)
	dst.Reads = append(dst.Reads, src.Reads...)
	dst.Writes = append(dst.Writes, src.Writes...)
	dst.Logic = append(dst.Logic, src.Logic...)
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

func (c *Config) applyModuleMetadata(meta ModuleReference) {
	if c == nil {
		return
	}
	c.Source = mergeModuleOverride(c.Source, meta)
	for i := range c.Cells {
		c.Cells[i].Source = mergeModuleOverride(c.Cells[i].Source, meta)
	}
	for i := range c.Programs {
		c.Programs[i].Source = mergeModuleOverride(c.Programs[i].Source, meta)
	}
	for i := range c.Reads {
		c.Reads[i].Source = mergeModuleOverride(c.Reads[i].Source, meta)
	}
	for i := range c.Writes {
		c.Writes[i].Source = mergeModuleOverride(c.Writes[i].Source, meta)
	}
	for i := range c.Logic {
		c.Logic[i].Source = mergeModuleOverride(c.Logic[i].Source, meta)
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

func mergeModuleOverride(base, override ModuleReference) ModuleReference {
	if override.File != "" {
		base.File = override.File
	}
	if override.Name != "" {
		base.Name = override.Name
	}
	if override.Description != "" {
		base.Description = override.Description
	}
	if override.Package != "" {
		base.Package = override.Package
	}
	return base
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
