package processor

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"cuelang.org/go/cue/load"

	"github.com/timzifer/quarc/config"
	"github.com/timzifer/quarc/programs"
)

type stubProgram struct {
	id string
}

func (s stubProgram) ID() string { return s.id }

func (s stubProgram) Execute(programs.Context, []programs.Signal) ([]programs.Signal, error) {
	return nil, nil
}

func TestWithProgramRegistersOverlaysBeforeLoad(t *testing.T) {
	config.ResetOverlaysForTest()
	t.Cleanup(config.ResetOverlaysForTest)

	overlay := config.OverlayDescriptor{
		Path: "virtual_overlay.cue",
		Source: load.FromString(`package overlaytest

#overlayCells: [{
    id: "virtual"
    type: "number"
}]
`),
	}

	factory := func(instanceID string, _ map[string]interface{}) (programs.Program, error) {
		return stubProgram{id: instanceID}, nil
	}

	def := ProgramDefinition{ID: "stub", Factory: factory}

	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "main.cue")
	content := `package overlaytest

config: {
    package: "overlay.test"
    cycle: "1s"
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
    cells: #overlayCells
    reads: []
    writes: []
    programs: []
    logic: []
}
`
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	proc, err := New(context.Background(), WithProgram(def, overlay), WithConfigPath(configDir, nil))
	if err != nil {
		t.Fatalf("processor new: %v", err)
	}
	t.Cleanup(proc.Close)

	if proc.config == nil {
		t.Fatal("expected processor config to be loaded")
	}
	if len(proc.config.Cells) != 1 {
		t.Fatalf("expected 1 cell, got %d", len(proc.config.Cells))
	}
	if proc.config.Cells[0].ID != "overlay.test.virtual" {
		t.Fatalf("unexpected cell id %q", proc.config.Cells[0].ID)
	}
}
