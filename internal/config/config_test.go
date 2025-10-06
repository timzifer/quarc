package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadModules(t *testing.T) {
        dir := t.TempDir()
        mainPath := filepath.Join(dir, "config.yaml")
        modulePath := filepath.Join(dir, "module.yaml")

	if err := os.WriteFile(modulePath, []byte(`cells:
  - id: extra
    type: bool
`), 0o600); err != nil {
		t.Fatalf("write module: %v", err)
	}

	content := `cycle: 1s
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

        content := `programs:
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
	if err := os.WriteFile(fileA, []byte(`logging:
  level: debug
cells:
  - id: a
    type: number
`), 0o600); err != nil {
		t.Fatalf("write base: %v", err)
	}

	fileB := filepath.Join(dir, "10-extra.yaml")
	if err := os.WriteFile(fileB, []byte(`reads:
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
