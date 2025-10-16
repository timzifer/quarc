package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
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
	if cfg.Cells[0].ID != "plant.core:temperature" {
		t.Fatalf("expected namespaced cell id, got %q", cfg.Cells[0].ID)
	}
	if len(cfg.Reads) != 1 {
		t.Fatalf("expected 1 read, got %d", len(cfg.Reads))
	}
	if cfg.Reads[0].Signals[0].Cell != "plant.core:temperature" {
		t.Fatalf("expected signal to reference namespaced cell, got %q", cfg.Reads[0].Signals[0].Cell)
	}
	if cfg.Source.Package != "plant.core" {
		t.Fatalf("expected source package to be plant.core, got %q", cfg.Source.Package)
	}
}

func TestNestedPackageQualification(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.cue")

	content := `package plant

config: {
    package: "plant.heating.underfloor"
    cycle: "1s"
` + baseSections + `
    cells: [
        {
            id: "flow"
            type: "number"
        },
    ]
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
	if cfg.Cells[0].ID != "plant.heating.underfloor:flow" {
		t.Fatalf("expected nested package id, got %q", cfg.Cells[0].ID)
	}
	if cfg.Source.Package != "plant.heating.underfloor" {
		t.Fatalf("expected source package to match nested package, got %q", cfg.Source.Package)
	}
}

func TestModulePackageQualifiedWithinRoot(t *testing.T) {
	dir := t.TempDir()

	mainPath := filepath.Join(dir, "main.cue")
	modulePath := filepath.Join(dir, "underfloor.cue")

	mainContent := `package homebase

config: {
    cycle: "1s"
` + baseSections + `
}
`

	moduleContent := `package homebase

config: config & {
    package: "heating.underfloor"
    connections: [{
        id: "underfloor_bus"
        driver: "modbus"
        endpoint: {
            address: "127.0.0.1:502"
            unit_id: 1
            driver: "modbus"
        }
    }]
    cells: [{
        id: "floor_temp"
        type: "number"
    }]
    reads: [{
        id: "supply"
        connection: "underfloor_bus"
        endpoint: {
            address: "127.0.0.1:502"
            unit_id: 1
            driver: "modbus"
        }
        function: "holding"
        start: 0
        length: 1
        ttl: "1s"
        signals: [{
            cell: "floor_temp"
            offset: 0
            type: "number"
        }]
    }]
}`

	if err := os.WriteFile(mainPath, []byte(mainContent), 0o600); err != nil {
		t.Fatalf("write main config: %v", err)
	}
	if err := os.WriteFile(modulePath, []byte(moduleContent), 0o600); err != nil {
		t.Fatalf("write module config: %v", err)
	}

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("load directory: %v", err)
	}

	if len(cfg.Connections) != 1 {
		t.Fatalf("expected 1 connection, got %d", len(cfg.Connections))
	}
	if len(cfg.Cells) != 1 {
		t.Fatalf("expected 1 cell, got %d", len(cfg.Cells))
	}
	if len(cfg.Reads) != 1 {
		t.Fatalf("expected 1 read, got %d", len(cfg.Reads))
	}

	expectedNamespace := "homebase.heating.underfloor"

	if got := cfg.Connections[0].ID; got != expectedNamespace+":underfloor_bus" {
		t.Fatalf("expected connection id %q, got %q", expectedNamespace+":underfloor_bus", got)
	}
	if got := cfg.Cells[0].ID; got != expectedNamespace+":floor_temp" {
		t.Fatalf("expected cell id %q, got %q", expectedNamespace+":floor_temp", got)
	}
	if got := cfg.Reads[0].ID; got != expectedNamespace+":supply" {
		t.Fatalf("expected read id %q, got %q", expectedNamespace+":supply", got)
	}
	if got := cfg.Reads[0].Connection; got != expectedNamespace+":underfloor_bus" {
		t.Fatalf("expected read connection %q, got %q", expectedNamespace+":underfloor_bus", got)
	}
	if got := cfg.Reads[0].Signals[0].Cell; got != expectedNamespace+":floor_temp" {
		t.Fatalf("expected read signal cell %q, got %q", expectedNamespace+":floor_temp", got)
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
            cell: "other.package:temperature"
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
	if write.ID != "plant.core:publish" {
		t.Fatalf("expected namespaced write id, got %q", write.ID)
	}
	if write.Cell != "other.package:temperature" {
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
	if cell.ID != "templ.app:pressure" {
		t.Fatalf("expected namespaced id, got %q", cell.ID)
	}
	if cell.Unit != "bar" {
		t.Fatalf("expected unit from template override, got %q", cell.Unit)
	}
	if len(cfg.Reads) != 1 {
		t.Fatalf("expected 1 read, got %d", len(cfg.Reads))
	}
	if cfg.Reads[0].Signals[0].Cell != "templ.app:pressure" {
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
	if cell.ID != "overlay.test:virtual" {
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
	if cfg.Reads[0].Signals[0].Cell != "plant.core:temperature" {
		t.Fatalf("expected read to reference namespaced cell, got %q", cfg.Reads[0].Signals[0].Cell)
	}
}

func TestLoadSignalBufferConfig(t *testing.T) {
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
                    buffer: {
                        capacity: 8
                        aggregator: "sum"
                    }
                    aggregations: [
                        {
                            cell: "temperature"
                            aggregator: "sum"
                        },
                    ]
                },
            ]
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

	if len(cfg.Reads) != 1 {
		t.Fatalf("expected 1 read, got %d", len(cfg.Reads))
	}
	signals := cfg.Reads[0].Signals
	if len(signals) != 1 {
		t.Fatalf("expected 1 signal, got %d", len(signals))
	}
	signal := signals[0]
	if signal.Aggregation != "sum" {
		t.Fatalf("expected aggregation sum, got %q", signal.Aggregation)
	}
	if len(signal.Aggregations) != 1 {
		t.Fatalf("expected 1 aggregation, got %d", len(signal.Aggregations))
	}
	if agg := signal.Aggregations[0]; agg.Cell != "plant.core:temperature" || agg.Aggregator != "sum" {
		t.Fatalf("unexpected aggregation config: %#v", agg)
	}
	if signal.BufferSize != 8 {
		t.Fatalf("expected buffer size 8, got %d", signal.BufferSize)
	}
	if signal.Buffer == nil {
		t.Fatalf("expected buffer config to be populated")
	}
	if signal.Buffer.Capacity == nil || *signal.Buffer.Capacity != 8 {
		t.Fatalf("expected buffer capacity to be 8, got %#v", signal.Buffer.Capacity)
	}
	if signal.Buffer.Aggregator != "sum" {
		t.Fatalf("expected buffer aggregator sum, got %q", signal.Buffer.Aggregator)
	}
}

func TestLoadRejectsInvalidSignalBufferAggregator(t *testing.T) {
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
                    buffer: {
                        capacity: 4
                        aggregator: "invalid"
                    }
                    aggregations: [
                        {
                            cell: "temperature"
                            aggregator: "invalid"
                        },
                    ]
                },
            ]
        },
    ]
}
`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, err := Load(path); err == nil {
		t.Fatal("expected load to fail due to invalid buffer aggregator")
	}
}

func TestLoadRejectsIdentifiersWithStrayColons(t *testing.T) {
	cases := map[string]string{
		"leading":  ":temperature",
		"trailing": "temperature:",
		"multi":    "plant.core:temperature:extra",
	}

	for name, cellID := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "config.cue")

			content := fmt.Sprintf(`package plant

config: {
    package: "plant.core"
    cycle: "1s"
%s
    cells: [
        {
            id: "%s"
            type: "number"
        },
    ]
    reads: []
    writes: []
}
`, baseSections, cellID)

			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}

			if _, err := Load(path); err == nil {
				t.Fatalf("expected load to fail for id %q", cellID)
			}
		})
	}
}

func TestLoadRejectsNonPositiveSignalBufferCapacity(t *testing.T) {
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
                    buffer: {
                        capacity: 0
                    }
                    aggregations: [
                        {
                            cell: "temperature"
                            aggregator: "sum"
                        },
                    ]
                },
            ]
        },
    ]
}
`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, err := Load(path); err == nil {
		t.Fatal("expected load to fail due to non-positive buffer capacity")
	}
}

func TestLoadLiveViewHeatmapConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.cue")

	content := fmt.Sprintf(`package plant

config: {
    package: "plant.core"
    cycle: "1s"
%s
    live_view: {
        heatmap: {
            cooldown: {
                cells: 9
                logic: 7
                programs: 5
            }
            colors: {
                read: "#00ff00"
                write: "#ff0000"
                stale: "#999999"
                logic: "#0000ff"
                program: "#ff00ff"
                background: "#111111"
                border: "#222222"
            }
        }
    }
    cells: []
    reads: []
    writes: []
}
`, baseSections)

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	heatmap := cfg.LiveView.Heatmap
	if heatmap.Cooldown.Cells != 9 || heatmap.Cooldown.Logic != 7 || heatmap.Cooldown.Programs != 5 {
		t.Fatalf("unexpected cooldown config: %#v", heatmap.Cooldown)
	}
	if heatmap.Colors.Read != "#00ff00" || heatmap.Colors.Write != "#ff0000" || heatmap.Colors.Stale != "#999999" {
		t.Fatalf("unexpected heatmap colours: %#v", heatmap.Colors)
	}
	if heatmap.Colors.Logic != "#0000ff" || heatmap.Colors.Program != "#ff00ff" {
		t.Fatalf("unexpected logic/program colours: %#v", heatmap.Colors)
	}
	if heatmap.Colors.Background != "#111111" || heatmap.Colors.Border != "#222222" {
		t.Fatalf("unexpected frame colours: %#v", heatmap.Colors)
	}
}

func TestApplyConnectionDefaults(t *testing.T) {
	sharedSettings := json.RawMessage(`{"shared":true}`)
	cfg := &Config{
		Connections: []IOConnectionConfig{{
			ID:             "plant:shared",
			Driver:         "stub",
			Endpoint:       EndpointConfig{Address: "127.0.0.1:502", UnitID: 1, Timeout: Duration{Duration: time.Second}},
			DriverSettings: sharedSettings,
		}},
		Reads: []ReadGroupConfig{
			{
				ID:         "plant:read-default",
				Connection: "plant:shared",
				Endpoint:   EndpointConfig{},
			},
			{
				ID:         "plant:read-override",
				Connection: "plant:shared",
				Endpoint:   EndpointConfig{Address: "192.0.2.10:1502"},
			},
			{
				ID:       "plant:inline",
				Endpoint: EndpointConfig{Address: "192.0.2.20:1502", Driver: "stub"},
			},
		},
		Writes: []WriteTargetConfig{
			{
				ID:         "plant:write-default",
				Cell:       "plant:cell",
				Connection: "plant:shared",
				Endpoint:   EndpointConfig{},
			},
			{
				ID:             "plant:write-custom",
				Cell:           "plant:cell",
				Connection:     "plant:shared",
				Endpoint:       EndpointConfig{},
				DriverSettings: json.RawMessage(`{"local":true}`),
			},
		},
	}

	if err := applyConnectionDefaults(cfg); err != nil {
		t.Fatalf("apply connection defaults: %v", err)
	}

	if cfg.Connections[0].Endpoint.Driver != "stub" {
		t.Fatalf("expected connection driver to be normalised, got %q", cfg.Connections[0].Endpoint.Driver)
	}

	if cfg.Reads[0].Endpoint.Address != "127.0.0.1:502" || cfg.Reads[0].Endpoint.Driver != "stub" {
		t.Fatalf("expected read defaults to apply, got %+v", cfg.Reads[0].Endpoint)
	}
	if cfg.Reads[0].Endpoint.Timeout.Duration != time.Second {
		t.Fatalf("expected read timeout to inherit, got %v", cfg.Reads[0].Endpoint.Timeout.Duration)
	}
	if len(cfg.Reads[0].DriverSettings) == 0 {
		t.Fatalf("expected read driver settings to inherit")
	}
	if len(cfg.Reads[0].DriverSettings) > 0 && len(cfg.Connections[0].DriverSettings) > 0 {
		if &cfg.Reads[0].DriverSettings[0] == &cfg.Connections[0].DriverSettings[0] {
			t.Fatalf("expected driver settings to be cloned, not shared")
		}
	}

	if cfg.Reads[1].Endpoint.Address != "192.0.2.10:1502" {
		t.Fatalf("expected read override address to be preserved, got %q", cfg.Reads[1].Endpoint.Address)
	}
	if cfg.Reads[1].Endpoint.Driver != "stub" {
		t.Fatalf("expected read override driver to match connection, got %q", cfg.Reads[1].Endpoint.Driver)
	}

	if cfg.Reads[2].Endpoint.Address != "192.0.2.20:1502" {
		t.Fatalf("expected inline endpoint to remain unchanged, got %q", cfg.Reads[2].Endpoint.Address)
	}

	if cfg.Writes[0].Endpoint.Address != "127.0.0.1:502" || cfg.Writes[0].Endpoint.Driver != "stub" {
		t.Fatalf("expected write defaults to apply, got %+v", cfg.Writes[0].Endpoint)
	}
	if len(cfg.Writes[0].DriverSettings) == 0 {
		t.Fatalf("expected write driver settings to inherit")
	}
	if len(cfg.Writes[0].DriverSettings) > 0 && len(cfg.Connections[0].DriverSettings) > 0 {
		if &cfg.Writes[0].DriverSettings[0] == &cfg.Connections[0].DriverSettings[0] {
			t.Fatalf("expected write driver settings to be cloned")
		}
	}
	if string(cfg.Writes[1].DriverSettings) != `{"local":true}` {
		t.Fatalf("expected write-specific driver settings to remain, got %s", string(cfg.Writes[1].DriverSettings))
	}
}

func TestApplyConnectionDefaultsUnknownConnection(t *testing.T) {
	cfg := &Config{
		Reads: []ReadGroupConfig{{
			ID:         "plant:missing",
			Connection: "plant:unknown",
			Endpoint:   EndpointConfig{Driver: "stub"},
		}},
	}
	if err := applyConnectionDefaults(cfg); err == nil {
		t.Fatalf("expected error for missing connection")
	}
}
