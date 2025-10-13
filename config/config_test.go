package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadModules(t *testing.T) {
	dir := t.TempDir()
	mainPath := filepath.Join(dir, "config.yaml")
	modulePath := filepath.Join(dir, "module.yaml")

	if err := os.WriteFile(modulePath, []byte(`package: main
cells:
  - id: extra
    type: bool
`), 0o600); err != nil {
		t.Fatalf("write module: %v", err)
	}

	content := `package: main
cycle: 1s
modules:
  - module.yaml
cells:
  - id: base
    type: number
`
	if err := os.WriteFile(mainPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write main: %v", err)
	}

	cfg, err := Load(mainPath)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if len(cfg.Cells) != 2 {
		t.Fatalf("expected 2 cells, got %d", len(cfg.Cells))
	}
}

func TestLoadPrograms(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	content := `package: main
programs:
  - id: ramp1
    type: ramp
    inputs:
      - id: target
        cell: c1
    outputs:
      - id: value
        cell: c2
    settings:
      rate: 1.5
cells:
  - id: c1
    type: number
  - id: c2
    type: number
`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if len(cfg.Programs) != 1 {
		t.Fatalf("expected 1 program, got %d", len(cfg.Programs))
	}
	prog := cfg.Programs[0]
	if prog.ID != "ramp1" || prog.Type != "ramp" {
		t.Fatalf("unexpected program config: %+v", prog)
	}
	if prog.Settings["rate"].(float64) != 1.5 {
		t.Fatalf("expected rate 1.5, got %v", prog.Settings["rate"])
	}
}

func TestLoadDirectory(t *testing.T) {
	dir := t.TempDir()

	fileA := filepath.Join(dir, "00-base.yaml")
	if err := os.WriteFile(fileA, []byte(`package: plant
logging:
  level: debug
cells:
  - id: a
    type: number
`), 0o600); err != nil {
		t.Fatalf("write base: %v", err)
	}

	fileB := filepath.Join(dir, "10-extra.yaml")
	if err := os.WriteFile(fileB, []byte(`package: plant
reads:
  - id: read1
    endpoint:
      address: "localhost:502"
      unit_id: 1
    function: holding
    start: 0
    length: 1
    ttl: 1s
    signals:
      - cell: a
        offset: 0
        type: number
`), 0o600); err != nil {
		t.Fatalf("write extra: %v", err)
	}

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("load dir: %v", err)
	}

	if len(cfg.Cells) != 1 {
		t.Fatalf("expected 1 cell, got %d", len(cfg.Cells))
	}
	if len(cfg.Reads) != 1 {
		t.Fatalf("expected 1 read, got %d", len(cfg.Reads))
	}
	if cfg.Logging.Level != "debug" {
		t.Fatalf("expected logging level debug, got %s", cfg.Logging.Level)
	}
}

func TestModuleMetadataPropagation(t *testing.T) {
	dir := t.TempDir()

	mainPath := filepath.Join(dir, "config.yaml")
	modulePath := filepath.Join(dir, "module.yaml")

	moduleContent := `package: root
cells:
  - id: extra
    type: bool
    name: Extra Flag
    description: Provided by module
`
	if err := os.WriteFile(modulePath, []byte(moduleContent), 0o600); err != nil {
		t.Fatalf("write module: %v", err)
	}

	mainContent := `package: root
name: Root Config
description: Root description
cycle: 1s
modules:
  - path: module.yaml
    name: Extra Module
    description: Additional cells
cells:
  - id: base
    type: number
    name: Base Cell
    description: Defined in root
`
	if err := os.WriteFile(mainPath, []byte(mainContent), 0o600); err != nil {
		t.Fatalf("write main: %v", err)
	}

	cfg, err := Load(mainPath)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if len(cfg.Cells) != 2 {
		t.Fatalf("expected 2 cells, got %d", len(cfg.Cells))
	}

	cells := make(map[string]CellConfig)
	for _, cell := range cfg.Cells {
		cells[cell.ID] = cell
	}

	base, ok := cells["base"]
	if !ok {
		t.Fatalf("missing base cell")
	}
	if base.Name != "Base Cell" {
		t.Fatalf("expected base cell name, got %q", base.Name)
	}
	if base.Source.Name != "Root Config" {
		t.Fatalf("expected root module name, got %q", base.Source.Name)
	}
	if base.Source.Description != "Root description" {
		t.Fatalf("expected root module description, got %q", base.Source.Description)
	}
	if !strings.HasSuffix(base.Source.File, "config.yaml") {
		t.Fatalf("expected base cell file to be config.yaml, got %q", base.Source.File)
	}

	extra, ok := cells["extra"]
	if !ok {
		t.Fatalf("missing extra cell")
	}
	if extra.Name != "Extra Flag" {
		t.Fatalf("expected extra cell name, got %q", extra.Name)
	}
	if extra.Source.Name != "Extra Module" {
		t.Fatalf("expected module name, got %q", extra.Source.Name)
	}
	if extra.Source.Description != "Additional cells" {
		t.Fatalf("expected module description, got %q", extra.Source.Description)
	}
	if !strings.HasSuffix(extra.Source.File, "module.yaml") {
		t.Fatalf("expected extra cell file to be module.yaml, got %q", extra.Source.File)
	}
}

func TestTemplateInheritance(t *testing.T) {
	dir := t.TempDir()

	modulePath := filepath.Join(dir, "module.yaml")
	if err := os.WriteFile(modulePath, []byte(`package: root
cells:
  - <<: *number_cell
    id: module_cell
`), 0o600); err != nil {
		t.Fatalf("write module: %v", err)
	}

	mainPath := filepath.Join(dir, "config.yaml")
	content := `package: root
cycle: 1s
template:
  number_cell: &number_cell
    type: number
cells:
  - <<: *number_cell
    id: root_cell
modules:
  - module.yaml
`
	if err := os.WriteFile(mainPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write main: %v", err)
	}

	cfg, err := Load(mainPath)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if len(cfg.Cells) != 2 {
		t.Fatalf("expected 2 cells, got %d", len(cfg.Cells))
	}
	for _, cell := range cfg.Cells {
		if cell.Type != ValueKindNumber {
			t.Fatalf("expected number cell, got %s", cell.Type)
		}
	}
}

func TestDirectoryIncludeSkipsValuesFiles(t *testing.T) {
	dir := t.TempDir()

	moduleDir := filepath.Join(dir, "modules")
	if err := os.MkdirAll(moduleDir, 0o755); err != nil {
		t.Fatalf("mkdir modules: %v", err)
	}

	modulePath := filepath.Join(moduleDir, "module.yaml")
	if err := os.WriteFile(modulePath, []byte(`package: modules
cells:
  - id: included_cell
    type: number
`), 0o600); err != nil {
		t.Fatalf("write module: %v", err)
	}

	valuesPath := filepath.Join(moduleDir, "secrets.values.yaml")
	if err := os.WriteFile(valuesPath, []byte("secret: hunter2\n"), 0o600); err != nil {
		t.Fatalf("write values: %v", err)
	}

	configPath := filepath.Join(dir, "config.yaml")
	content := `package: root
cycle: 1s
cells: []
reads: []
writes: []
logic: []
values: []
modules:
  - modules
`
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if len(cfg.Cells) != 1 {
		t.Fatalf("expected 1 cell from module, got %d", len(cfg.Cells))
	}
	if cfg.Cells[0].ID != "included_cell" {
		t.Fatalf("unexpected cell id %q", cfg.Cells[0].ID)
	}
}

func TestValuesSubstitution(t *testing.T) {
	dir := t.TempDir()

	valuesPath := filepath.Join(dir, "secrets.values.yaml")
	if err := os.WriteFile(valuesPath, []byte("secret: hunter2\n"), 0o600); err != nil {
		t.Fatalf("write values: %v", err)
	}

	configPath := filepath.Join(dir, "config.yaml")
	content := `package: vault
values:
  - secrets.values.yaml
cells:
  - id: secret_value
    type: string
    constant: !secret
`
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.Cells) != 1 {
		t.Fatalf("expected 1 cell, got %d", len(cfg.Cells))
	}
	if cfg.Cells[0].Constant != "hunter2" {
		t.Fatalf("expected constant hunter2, got %#v", cfg.Cells[0].Constant)
	}
}

func TestValuesFileMustUseSuffix(t *testing.T) {
	dir := t.TempDir()

	valuesPath := filepath.Join(dir, "secrets.yaml")
	if err := os.WriteFile(valuesPath, []byte("secret: hunter2\n"), 0o600); err != nil {
		t.Fatalf("write values: %v", err)
	}

	configPath := filepath.Join(dir, "config.yaml")
	content := `package: vault
values:
  - secrets.yaml
cells:
  - id: secret_value
    type: string
    constant: !secret
`
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, err := Load(configPath); err == nil {
		t.Fatalf("expected error for invalid values file suffix")
	}
}

func TestIdentifierWithDotRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `package: invalid
cells:
  - id: bad.name
    type: number
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, err := Load(path); err == nil {
		t.Fatalf("expected error for dotted identifier")
	}
}
