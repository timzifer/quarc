package config

import (
	"os"
	"path/filepath"
	"testing"
)

const baseSections = `
    logging: {
        level: "info"
        loki: {
            enabled: false
            url: ""
            labels: {}
        }
    }
    telemetry: {
        enabled: false
    }
    workers: {}
    helpers: []
    dsl: {}
    live_view: {}
    policies: {}
    server: {
        enabled: false
        listen: ""
        cells: []
    }
    hot_reload: false
`

func TestLoadCUEConfigNamespacing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.cue")

	content := `package plant

config: {
    package: "plant.core"
    name: "Core"
    description: "Core configuration"
    cycle: "1s"
` + baseSections + `
    reads: [
        {
            id: "sensors"
            endpoint: {
                address: "localhost:502"
                unit_id: 1
                timeout: "1s"
            }
            function: "holding"
            start: 0
            length: 1
            ttl: "1s"
            signals: [
                {
                    cell: "temperature"
                    offset: 0
                    type: "number"
                },
            ]
        },
    ]
    cells: [
        {
            id: "temperature"
            type: "number"
        },
    ]
}
`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if len(cfg.Cells) != 1 {
		t.Fatalf("expected 1 cell, got %d", len(cfg.Cells))
	}
	if cfg.Cells[0].ID != "plant.core.temperature" {
		t.Fatalf("expected namespaced cell id, got %q", cfg.Cells[0].ID)
	}
	if len(cfg.Reads) != 1 {
		t.Fatalf("expected 1 read, got %d", len(cfg.Reads))
	}
	if cfg.Reads[0].Signals[0].Cell != "plant.core.temperature" {
		t.Fatalf("expected signal to reference namespaced cell, got %q", cfg.Reads[0].Signals[0].Cell)
	}
	if cfg.Source.Package != "plant.core" {
		t.Fatalf("expected source package to be plant.core, got %q", cfg.Source.Package)
	}
}

func TestQualifiedReferencesRemain(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.cue")

	content := `package plant

config: {
    package: "plant.core"
    cycle: "1s"
` + baseSections + `
    cells: [
        {
            id: "temperature"
            type: "number"
        },
    ]
    reads: []
    writes: [
        {
            id: "publish"
            cell: "other.package.temperature"
            endpoint: {
                address: "localhost:502"
                unit_id: 1
            }
            function: "holding"
            address: 1
            ttl: "1s"
        },
    ]
}
`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if len(cfg.Writes) != 1 {
		t.Fatalf("expected 1 write, got %d", len(cfg.Writes))
	}
	write := cfg.Writes[0]
	if write.ID != "plant.core.publish" {
		t.Fatalf("expected namespaced write id, got %q", write.ID)
	}
	if write.Cell != "other.package.temperature" {
		t.Fatalf("expected qualified reference to remain unchanged, got %q", write.Cell)
	}
}

func TestTemplateExpansion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.cue")

	content := `package templ

template: {
    cell: {
        id: string
        unit?: string
        type: "number"
    }
    read: {
        id: string
        endpoint: {
            address: string
            unit_id: int
        }
        function: string
        start: int
        length: int
        ttl: "1s"
        signals: [
            {
                cell: string
                offset: int
                type: "number"
            },
        ]
    }
}

config: {
    package: "templ.app"
    cycle: "1s"
` + baseSections + `
    cells: [
        template.cell & {
            id: "pressure"
            unit: "bar"
        },
    ]
    reads: [
        template.read & {
            id: "sensor"
            endpoint: {
                address: "localhost:502"
                unit_id: 1
            }
            function: "holding"
            start: 0
            length: 1
            signals: [
                {
                    cell: "pressure"
                    offset: 0
                    type: "number"
                },
            ]
        },
    ]
    writes: []
}
`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if len(cfg.Cells) != 1 {
		t.Fatalf("expected 1 cell, got %d", len(cfg.Cells))
	}
	cell := cfg.Cells[0]
	if cell.ID != "templ.app.pressure" {
		t.Fatalf("expected namespaced id, got %q", cell.ID)
	}
	if cell.Unit != "bar" {
		t.Fatalf("expected unit from template override, got %q", cell.Unit)
	}
	if len(cfg.Reads) != 1 {
		t.Fatalf("expected 1 read, got %d", len(cfg.Reads))
	}
	if cfg.Reads[0].Signals[0].Cell != "templ.app.pressure" {
		t.Fatalf("expected read signal to reference namespaced cell, got %q", cfg.Reads[0].Signals[0].Cell)
	}
}

func TestLoadConfigWithOverlay(t *testing.T) {
	resetOverlaysForTest()
	t.Cleanup(resetOverlaysForTest)

	dir := t.TempDir()
	overlayContent := `package overlay

config: {
    package: "overlay.test"
    cycle: "1s"
` + baseSections + `
    cells: [{
        id: "virtual"
        type: "number"
    }]
    reads: []
    writes: []
}
`

	if err := RegisterOverlayString("config.cue", overlayContent); err != nil {
		t.Fatalf("register overlay: %v", err)
	}

	path := filepath.Join(dir, "config.cue")
	content := `package overlay

config: {
    package: "overlay.test"
    cycle: "1s"
` + baseSections + `
    cells: []
    reads: []
    writes: []
}
`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.Cells) != 1 {
		t.Fatalf("expected 1 cell, got %d", len(cfg.Cells))
	}
	cell := cfg.Cells[0]
	if cell.ID != "overlay.test.virtual" {
		t.Fatalf("expected namespaced cell from overlay, got %q", cell.ID)
	}
}

func TestDirectoryMerge(t *testing.T) {
	dir := t.TempDir()

	mainPath := filepath.Join(dir, "main.cue")
	extraPath := filepath.Join(dir, "extra.cue")

	mainContent := `package plant

config: {
    package: "plant.core"
    cycle: "1s"
` + baseSections + `
    cells: [
        {
            id: "temperature"
            type: "number"
        },
    ]
    writes: []
}
`

	extraContent := `package plant

config: config & {
    reads: [
        {
            id: "sensor"
            endpoint: {
                address: "localhost:502"
                unit_id: 1
            }
            function: "holding"
            start: 0
            length: 1
            ttl: "1s"
            signals: [
                {
                    cell: "temperature"
                    offset: 0
                    type: "number"
                },
            ]
        },
    ]
}
`

	if err := os.WriteFile(mainPath, []byte(mainContent), 0o600); err != nil {
		t.Fatalf("write main: %v", err)
	}
	if err := os.WriteFile(extraPath, []byte(extraContent), 0o600); err != nil {
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
	if cfg.Reads[0].Signals[0].Cell != "plant.core.temperature" {
		t.Fatalf("expected read to reference namespaced cell, got %q", cfg.Reads[0].Signals[0].Cell)
	}
}
