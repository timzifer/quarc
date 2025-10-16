package processor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cuelang.org/go/cue/load"

	"github.com/timzifer/quarc/config"
	"github.com/timzifer/quarc/programs"
	"github.com/timzifer/quarc/serviceio"
)

type stubProgram struct {
	id string
}

func (s stubProgram) ID() string { return s.id }

func (s stubProgram) Execute(programs.Context, []programs.Signal) ([]programs.Signal, error) {
	return nil, nil
}

func stubReaderFactory(config.ReadGroupConfig, serviceio.ReaderDependencies) (serviceio.ReadGroup, error) {
	return nil, nil
}

func stubWriterFactory(config.WriteTargetConfig, serviceio.WriterDependencies) (serviceio.Writer, error) {
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
	if proc.config.Cells[0].ID != "overlay.test:virtual" {
		t.Fatalf("unexpected cell id %q", proc.config.Cells[0].ID)
	}
}

func TestRegisterProgramsValid(t *testing.T) {
	defs := []ProgramDefinition{{
		ID: t.Name(),
		Factory: func(instanceID string, _ map[string]interface{}) (programs.Program, error) {
			return stubProgram{id: instanceID}, nil
		},
	}}
	if err := registerPrograms(defs); err != nil {
		t.Fatalf("registerPrograms: %v", err)
	}
}

func TestRegisterProgramsRejectsEmptyID(t *testing.T) {
	defs := []ProgramDefinition{{
		ID: "",
		Factory: func(instanceID string, _ map[string]interface{}) (programs.Program, error) {
			return stubProgram{id: instanceID}, nil
		},
	}}
	if err := registerPrograms(defs); err == nil {
		t.Fatal("expected error for empty program id")
	}
}

func TestRegisterProgramsRejectsNilFactory(t *testing.T) {
	defs := []ProgramDefinition{{
		ID:      t.Name(),
		Factory: nil,
	}}
	if err := registerPrograms(defs); err == nil {
		t.Fatal("expected error for nil factory")
	}
}

func TestRegisterProgramsDetectsDuplicates(t *testing.T) {
	id := t.Name()
	factory := func(instanceID string, _ map[string]interface{}) (programs.Program, error) {
		return stubProgram{id: instanceID}, nil
	}
	defs := []ProgramDefinition{{ID: id, Factory: factory}}
	if err := registerPrograms(defs); err != nil {
		t.Fatalf("first registration failed: %v", err)
	}
	if err := registerPrograms(defs); err == nil {
		t.Fatal("expected error for duplicate registration")
	}
}

func TestBuildServiceOptions(t *testing.T) {
	defs := []IOServiceDefinition{
		{Driver: "alpha", Reader: stubReaderFactory},
		{Driver: "beta", Writer: stubWriterFactory},
		{Driver: "gamma", Reader: stubReaderFactory, Writer: stubWriterFactory},
		{Driver: "", Reader: stubReaderFactory, Writer: stubWriterFactory},
	}
	opts := buildServiceOptions(defs)
	if len(opts) != 4 {
		t.Fatalf("expected 4 service options, got %d", len(opts))
	}
	for idx, opt := range opts {
		if opt == nil {
			t.Fatalf("option at index %d is nil", idx)
		}
	}
	if buildServiceOptions(nil) != nil {
		t.Fatal("expected nil result for empty definitions")
	}
}

func TestListenAddress(t *testing.T) {
	if got := listenAddress("localhost", 0); got != "localhost" {
		t.Fatalf("unexpected listen address: %q", got)
	}
	if got := listenAddress("::1", 8080); got != "[::1]:8080" {
		t.Fatalf("unexpected ipv6 listen address: %q", got)
	}
}

func TestDrainReloadRequests(t *testing.T) {
	proc := &Processor{}
	errSent := errors.New("boom")
	done1 := make(chan error, 1)
	done2 := make(chan error, 1)
	ch := make(chan reloadRequest, 2)
	ch <- reloadRequest{done: done1}
	ch <- reloadRequest{done: done2}

	proc.drainReloadRequests(ch, errSent)

	select {
	case got := <-done1:
		if got != errSent {
			t.Fatalf("unexpected error for first request: %v", got)
		}
	default:
		t.Fatal("first request not drained")
	}

	select {
	case got := <-done2:
		if got != errSent {
			t.Fatalf("unexpected error for second request: %v", got)
		}
	default:
		t.Fatal("second request not drained")
	}

	if len(ch) != 0 {
		t.Fatalf("expected reload channel to be empty, got %d", len(ch))
	}

	proc.drainReloadRequests(nil, errSent) // ensure no panic
}

func TestTickChannel(t *testing.T) {
	if tickChannel(nil) != nil {
		t.Fatal("expected nil channel for nil ticker")
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	if tickChannel(ticker) != ticker.C {
		t.Fatal("expected ticker channel to be returned")
	}
}
