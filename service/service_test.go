package service

import (
        "context"
        "encoding/binary"
        "fmt"
        "io"
        "strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/timzifer/quarc/config"
	"github.com/timzifer/quarc/runtime/readers"
	"github.com/timzifer/quarc/runtime/state"
	"github.com/timzifer/quarc/runtime/writers"
)

type fakeClient struct {
	readHoldingFn func(address, quantity uint16) ([]byte, error)
	writeCoilFn   func(address, value uint16) ([]byte, error)
	writes        []uint16
}

func (f *fakeClient) ReadCoils(address, quantity uint16) ([]byte, error) {
	return nil, nil
}

func (f *fakeClient) ReadDiscreteInputs(address, quantity uint16) ([]byte, error) {
	return nil, nil
}

func (f *fakeClient) ReadHoldingRegisters(address, quantity uint16) ([]byte, error) {
	if f.readHoldingFn == nil {
		return nil, nil
	}
	return f.readHoldingFn(address, quantity)
}

func (f *fakeClient) ReadInputRegisters(address, quantity uint16) ([]byte, error) {
	return nil, nil
}

func (f *fakeClient) WriteSingleCoil(address, value uint16) ([]byte, error) {
	if f.writeCoilFn != nil {
		return f.writeCoilFn(address, value)
	}
	f.writes = append(f.writes, value)
	return nil, nil
}

func (f *fakeClient) WriteSingleRegister(address, value uint16) ([]byte, error) {
	return nil, nil
}

func (f *fakeClient) Close() error { return nil }

type countingClient struct {
	fakeClient
	closed int
}

func (c *countingClient) Close() error {
	c.closed++
	return nil
}

const testDriverName = "stub"

type testClient interface {
	ReadHoldingRegisters(address, quantity uint16) ([]byte, error)
	WriteSingleCoil(address, value uint16) ([]byte, error)
	Close() error
}

type testClientFactory func(cfg config.EndpointConfig) (testClient, error)

func stubOptions(factory testClientFactory) []Option {
	return []Option{
		WithReaderFactory(testDriverName, newStubReaderFactory(factory)),
		WithWriterFactory(testDriverName, newStubWriterFactory(factory)),
	}
}

func newStubReaderFactory(factory testClientFactory) readers.ReaderFactory {
	return func(cfg config.ReadGroupConfig, deps readers.ReaderDependencies) (readers.ReadGroup, error) {
		if factory == nil {
			return nil, fmt.Errorf("read group %s: no client factory provided", cfg.ID)
		}
		if len(cfg.Signals) == 0 {
			return nil, fmt.Errorf("read group %s: no signals configured", cfg.ID)
		}
		cellID := cfg.Signals[0].Cell
		if cellID == "" {
			return nil, fmt.Errorf("read group %s: signal missing cell", cfg.ID)
		}
		cell, err := deps.Cells.Get(cellID)
		if err != nil {
			return nil, err
		}
		client, err := factory(cfg.Endpoint)
		if err != nil {
			return nil, err
		}
		return &stubReadGroup{
			id:       cfg.ID,
			client:   client,
			cell:     cell,
			cellID:   cellID,
			function: cfg.Function,
			start:    cfg.Start,
			length:   cfg.Length,
			source:   cfg.Source,
		}, nil
	}
}

func newStubWriterFactory(factory testClientFactory) writers.WriterFactory {
	return func(cfg config.WriteTargetConfig, deps writers.WriterDependencies) (writers.Writer, error) {
		if factory == nil {
			return nil, fmt.Errorf("write target %s: no client factory provided", cfg.ID)
		}
		if cfg.Cell == "" {
			return nil, fmt.Errorf("write target %s: missing cell", cfg.ID)
		}
		cell, err := deps.Cells.Get(cfg.Cell)
		if err != nil {
			return nil, err
		}
		client, err := factory(cfg.Endpoint)
		if err != nil {
			return nil, err
		}
		return &stubWriter{
			id:      cfg.ID,
			client:  client,
			cell:    cell,
			cellID:  cfg.Cell,
			address: cfg.Address,
			source:  cfg.Source,
		}, nil
	}
}

type stubReadGroup struct {
	id       string
	client   testClient
	cell     state.Cell
	cellID   string
	function string
	start    uint16
	length   uint16
	source   config.ModuleReference

	disabled     atomic.Bool
	lastRun      time.Time
	lastAttempt  time.Time
	lastDuration time.Duration
}

func (g *stubReadGroup) ID() string { return g.id }

func (g *stubReadGroup) Due(time.Time) bool {
	return !g.disabled.Load()
}

func (g *stubReadGroup) Perform(now time.Time, logger zerolog.Logger) int {
	if g.disabled.Load() {
		return 0
	}
	g.lastAttempt = now
	start := time.Now()
	data, err := g.client.ReadHoldingRegisters(g.start, g.length)
	g.lastDuration = time.Since(start)
	if err != nil {
		g.cell.MarkInvalid(now, "stub.read", err.Error())
		logger.Error().Err(err).Str("group", g.id).Msg("stub read failed")
		return 1
	}
	if len(data) < 2 {
		g.cell.MarkInvalid(now, "stub.read", "insufficient data")
		logger.Error().Str("group", g.id).Msg("stub read produced insufficient data")
		return 1
	}
	value := binary.BigEndian.Uint16(data[:2])
	if err := g.cell.SetValue(float64(value), now, nil); err != nil {
		logger.Error().Err(err).Str("group", g.id).Msg("stub read set value failed")
		return 1
	}
	g.lastRun = now
	return 0
}

func (g *stubReadGroup) SetDisabled(disabled bool) {
	g.disabled.Store(disabled)
}

func (g *stubReadGroup) Status() readers.ReadGroupStatus {
	return readers.ReadGroupStatus{
		ID:           g.id,
		Function:     g.function,
		Start:        g.start,
		Length:       g.length,
		Disabled:     g.disabled.Load(),
		LastRun:      g.lastRun,
		LastDuration: g.lastDuration,
		Source:       g.source,
	}
}

func (g *stubReadGroup) Close() {
	_ = g.client.Close()
}

type stubWriter struct {
	id      string
	client  testClient
	cell    state.Cell
	cellID  string
	address uint16
	source  config.ModuleReference

	disabled  atomic.Bool
	lastValue bool
	haveLast  bool
}

func (w *stubWriter) ID() string { return w.id }

func (w *stubWriter) Commit(now time.Time, logger zerolog.Logger) int {
	if w.disabled.Load() {
		return 0
	}
	value, ok := w.cell.CurrentValue()
	if !ok {
		return 0
	}
	boolValue, ok := value.(bool)
	if !ok {
		logger.Error().Str("writer", w.id).Msg("stub writer expected bool value")
		return 1
	}
	if w.haveLast && w.lastValue == boolValue {
		return 0
	}
	raw := uint16(0)
	if boolValue {
		raw = 1
	}
	if _, err := w.client.WriteSingleCoil(w.address, raw); err != nil {
		logger.Error().Err(err).Str("writer", w.id).Msg("stub writer failed")
		return 1
	}
	w.lastValue = boolValue
	w.haveLast = true
	return 0
}

func (w *stubWriter) SetDisabled(disabled bool) {
	w.disabled.Store(disabled)
}

func (w *stubWriter) Status() writers.WriteTargetStatus {
	return writers.WriteTargetStatus{
		ID:       w.id,
		Cell:     w.cellID,
		Address:  w.address,
		Disabled: w.disabled.Load(),
		Source:   w.source,
	}
}

func (w *stubWriter) Close() {
	_ = w.client.Close()
}

func TestDeterministicCycle(t *testing.T) {
	client := &fakeClient{}
	client.readHoldingFn = func(address, quantity uint16) ([]byte, error) {
		return []byte{0x00, 0xFA}, nil // 250
	}
	client.writeCoilFn = func(address, value uint16) ([]byte, error) {
		client.writes = append(client.writes, value)
		return nil, nil
	}

	factory := func(config.EndpointConfig) (testClient, error) {
		return client, nil
	}

	cfg := &config.Config{
		Cycle: config.Duration{Duration: time.Millisecond},
		Cells: []config.CellConfig{
			{ID: "raw_temp", Type: config.ValueKindNumber},
			{ID: "deg_c", Type: config.ValueKindNumber},
			{ID: "alarm", Type: config.ValueKindBool},
		},
		Reads: []config.ReadGroupConfig{
			{
				ID:       "temperature",
				Endpoint: config.EndpointConfig{Address: "ignored:502", UnitID: 1, Driver: testDriverName},
				Function: "holding",
				Start:    0,
				Length:   1,
				TTL:      config.Duration{},
				Signals: []config.ReadSignalConfig{{
					Cell:   "raw_temp",
					Offset: 0,
					Type:   config.ValueKindNumber,
				}},
			},
		},
		Logic: []config.LogicBlockConfig{
			{
				ID:         "deg_c_calc",
				Target:     "deg_c",
				Expression: `valid("raw_temp") ? value("raw_temp") * 0.1 : 0`,
				Valid:      `valid("raw_temp") ? true : {"valid": false, "code": "logic.dependency", "message": "raw temperature unavailable"}`,
				Quality:    `valid("raw_temp") ? 1 : 0`,
			},
			{
				ID:         "alarm_logic",
				Target:     "alarm",
				Expression: `valid("deg_c") ? value("deg_c") > 20 : false`,
				Valid:      `valid("deg_c")`,
				Quality:    `valid("deg_c") ? 1 : 0`,
			},
		},
		Writes: []config.WriteTargetConfig{
			{
				ID:       "write_alarm",
				Cell:     "alarm",
				Endpoint: config.EndpointConfig{Address: "ignored:502", UnitID: 1, Driver: testDriverName},
				Function: "coil",
				Address:  10,
			},
		},
	}

	logger := zerolog.New(io.Discard)
	svc, err := New(cfg, logger, stubOptions(factory)...)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	if err := svc.IterateOnce(context.Background(), time.Now()); err != nil {
		t.Fatalf("iterate once: %v", err)
	}
	if metrics := svc.Metrics(); metrics.LastEvalErrors != 0 {
		t.Fatalf("unexpected eval errors: %+v", metrics)
	}

	snap := svc.CellSnapshot()
	if cell := snap["raw_temp"]; cell == nil || !cell.Valid {
		t.Fatalf("raw_temp missing or invalid: %+v", cell)
	}
	if cell := snap["deg_c"]; cell == nil || !cell.Valid {
		t.Fatalf("deg_c missing or invalid: %+v", cell)
	} else if got, ok := cell.Value.(float64); !ok || got != 25 {
		t.Fatalf("expected deg_c=25, got %v", cell.Value)
	}
	if cell := snap["alarm"]; cell == nil || !cell.Valid {
		t.Fatalf("alarm missing or invalid: %+v", cell)
	} else if got, ok := cell.Value.(bool); !ok || !got {
		t.Fatalf("expected alarm true, got %v", cell.Value)
	}
	if len(client.writes) != 1 {
		t.Fatalf("expected single write, got %d", len(client.writes))
	}

	if err := svc.IterateOnce(context.Background(), time.Now()); err != nil {
		t.Fatalf("second iterate: %v", err)
	}
	if len(client.writes) != 1 {
		t.Fatalf("expected write-on-change to avoid duplicate writes")
	}
}

func TestProgramsExecuteBeforeLogic(t *testing.T) {
	cfg := &config.Config{
		Cycle: config.Duration{Duration: time.Millisecond},
		Cells: []config.CellConfig{
			{ID: "target", Type: config.ValueKindNumber, Constant: 10.0},
			{ID: "processed", Type: config.ValueKindNumber},
			{ID: "result", Type: config.ValueKindNumber},
		},
		Programs: []config.ProgramConfig{
			{
				ID:   "ramp_test",
				Type: "ramp",
				Inputs: []config.ProgramSignalConfig{{
					ID:   "target",
					Cell: "target",
				}},
				Outputs: []config.ProgramSignalConfig{{
					ID:   "value",
					Cell: "processed",
				}},
				Settings: map[string]interface{}{"rate": 100.0},
			},
		},
		Logic: []config.LogicBlockConfig{
			{
				ID:         "logic",
				Target:     "result",
				Expression: `value("processed") * 2`,
			},
		},
	}

	logger := zerolog.New(io.Discard)
	svc, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	if err := svc.IterateOnce(context.Background(), time.Now()); err != nil {
		t.Fatalf("iterate: %v", err)
	}

	metrics := svc.Metrics()
	if metrics.LastProgramErrors != 0 {
		t.Fatalf("unexpected program errors: %+v", metrics)
	}

	snap := svc.CellSnapshot()
	if cell := snap["processed"]; cell == nil || !cell.Valid {
		t.Fatalf("processed cell invalid: %+v", cell)
	} else if got, ok := cell.Value.(float64); !ok || got != 10 {
		t.Fatalf("expected processed=10, got %v", cell.Value)
	}
	if cell := snap["result"]; cell == nil || !cell.Valid {
		t.Fatalf("result cell invalid: %+v", cell)
	} else if got, ok := cell.Value.(float64); !ok || got != 20 {
		t.Fatalf("expected result=20, got %v", cell.Value)
	}
}

func TestExpressionHandlesInvalidDependency(t *testing.T) {
	cfg := &config.Config{
		Cycle: config.Duration{Duration: time.Millisecond},
		Cells: []config.CellConfig{
			{ID: "input_a", Type: config.ValueKindNumber},
			{ID: "output_b", Type: config.ValueKindNumber},
		},
		Logic: []config.LogicBlockConfig{
			{
				ID:         "compute",
				Target:     "output_b",
				Expression: `valid("input_a") ? value("input_a") * 2 : 42`,
				Valid:      "true",
				Quality:    `valid("input_a") ? 1 : 0.2`,
			},
		},
	}

	logger := zerolog.New(io.Discard)
	svc, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	if err := svc.IterateOnce(context.Background(), time.Now()); err != nil {
		t.Fatalf("iterate: %v", err)
	}

	snap := svc.CellSnapshot()
	val, ok := snap["output_b"].Value.(float64)
	if !ok {
		t.Fatalf("expected numeric value")
	}
	if val != 42 {
		t.Fatalf("expected fallback value 42, got %v", val)
	}
	if !snap["output_b"].Valid {
		t.Fatalf("fallback should mark cell valid")
	}
}

func TestCycleDetection(t *testing.T) {
	cfg := &config.Config{
		Cells: []config.CellConfig{
			{ID: "a", Type: config.ValueKindNumber},
			{ID: "b", Type: config.ValueKindNumber},
		},
		Logic: []config.LogicBlockConfig{
			{
				ID:           "block_a",
				Target:       "a",
				Dependencies: []config.DependencyConfig{{Cell: "b", Type: config.ValueKindNumber}},
				Expression:   "value(\\\"b\\\")",
			},
			{
				ID:           "block_b",
				Target:       "b",
				Dependencies: []config.DependencyConfig{{Cell: "a", Type: config.ValueKindNumber}},
				Expression:   "value(\\\"a\\\")",
			},
		},
	}

	logger := zerolog.New(io.Discard)
	if _, err := New(cfg, logger); err == nil {
		t.Fatalf("expected cycle detection error")
	}
}


func TestServiceManualCellOperations(t *testing.T) {
        cfg := &config.Config{
                Cycle: config.Duration{Duration: time.Millisecond},
                Cells: []config.CellConfig{{ID: "manual", Type: config.ValueKindNumber}},
        }
	logger := zerolog.New(io.Discard)
	svc, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	if err := svc.SetCellValue("manual", 42); err != nil {
		t.Fatalf("set cell value: %v", err)
	}

	state, err := svc.InspectCell("manual")
	if err != nil {
		t.Fatalf("inspect cell: %v", err)
	}
	if !state.Valid {
		t.Fatalf("expected cell to be valid")
	}
	if state.UpdatedAt == nil || state.UpdatedAt.IsZero() {
		t.Fatalf("expected updated timestamp to be set")
	}
	if got, ok := state.Value.(float64); !ok || got != 42 {
		t.Fatalf("expected value 42, got %v", state.Value)
	}
	if state.Diagnosis != nil {
		t.Fatalf("unexpected diagnosis: %+v", state.Diagnosis)
	}

        if err := svc.InvalidateCell("manual", "manual.override", "forced invalid"); err != nil {
                t.Fatalf("invalidate cell: %v", err)
        }

        state, err = svc.InspectCell("manual")
	if err != nil {
		t.Fatalf("inspect cell after invalidate: %v", err)
	}
	if state.Valid {
		t.Fatalf("expected cell to be invalid")
	}
	if state.Diagnosis == nil || state.Diagnosis.Code != "manual.override" {
		t.Fatalf("expected diagnosis with code manual.override, got %+v", state.Diagnosis)
	}
	if state.Diagnosis.Message != "forced invalid" {
		t.Fatalf("unexpected diagnosis message: %v", state.Diagnosis.Message)
        }
}

func TestServerConfigIgnored(t *testing.T) {
        cfg := &config.Config{
                Cells: []config.CellConfig{{ID: "value", Type: config.ValueKindNumber}},
                Server: config.ServerConfig{
                        Enabled: true,
                        Listen:  "127.0.0.1:0",
                        UnitID:  1,
                        Cells: []config.ServerCellConfig{{Cell: "value", Address: 0}},
                },
        }
        logger := zerolog.New(io.Discard)

        if err := Validate(cfg, logger); err != nil {
                t.Fatalf("validate config with legacy server: %v", err)
        }

        svc, err := New(cfg, logger)
        if err != nil {
                t.Fatalf("new service: %v", err)
        }
        defer svc.Close()

        if addr := svc.ServerAddress(); addr != "" {
                t.Fatalf("expected legacy server address to be empty, got %q", addr)
        }

        if err := svc.SetCellValue("value", 123.4); err != nil {
                t.Fatalf("set cell value: %v", err)
        }
        if err := svc.InvalidateCell("value", "legacy.server", "ignored"); err != nil {
                t.Fatalf("invalidate cell: %v", err)
        }
}

func TestServiceClosesRemoteClients(t *testing.T) {
	cfg := &config.Config{
		Cycle: config.Duration{Duration: time.Millisecond},
		Cells: []config.CellConfig{
			{ID: "input", Type: config.ValueKindNumber},
			{ID: "output", Type: config.ValueKindBool},
		},
		Reads: []config.ReadGroupConfig{{
			ID:       "rg",
			Endpoint: config.EndpointConfig{Address: "ignored:1", UnitID: 1, Driver: testDriverName},
			Function: "holding",
			Start:    0,
			Length:   1,
			Signals: []config.ReadSignalConfig{{
				Cell:   "input",
				Offset: 0,
				Type:   config.ValueKindNumber,
			}},
		}},
		Logic: []config.LogicBlockConfig{{
			ID:         "logic",
			Target:     "output",
			Expression: `valid("input") ? value("input") > 0 : false`,
		}},
		Writes: []config.WriteTargetConfig{{
			ID:       "wt",
			Cell:     "output",
			Endpoint: config.EndpointConfig{Address: "ignored:1", UnitID: 1, Driver: testDriverName},
			Function: "coil",
			Address:  0,
		}},
	}

	clients := make([]*countingClient, 0, 2)
	factory := func(config.EndpointConfig) (testClient, error) {
		client := &countingClient{}
		client.readHoldingFn = func(address, quantity uint16) ([]byte, error) {
			return []byte{0x00, 0x01}, nil
		}
		client.writeCoilFn = func(address, value uint16) ([]byte, error) {
			return nil, nil
		}
		clients = append(clients, client)
		return client, nil
	}

	logger := zerolog.New(io.Discard)
	svc, err := New(cfg, logger, stubOptions(factory)...)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	if err := svc.IterateOnce(context.Background(), time.Now()); err != nil {
		t.Fatalf("iterate: %v", err)
	}

	if err := svc.Close(); err != nil {
		t.Fatalf("close service: %v", err)
	}

	if len(clients) == 0 {
		t.Fatalf("expected clients to be created")
	}
	for idx, client := range clients {
		if client.closed == 0 {
			t.Fatalf("expected client %d to be closed", idx)
		}
	}
}


func TestNewProgramBindingsDetectsDuplicateOutputs(t *testing.T) {
	logger := zerolog.New(io.Discard)
	makeCells := func() *cellStore {
		t.Helper()
		cells, err := newCellStore([]config.CellConfig{
			{ID: "target", Type: config.ValueKindNumber},
			{ID: "output", Type: config.ValueKindNumber},
		})
		if err != nil {
			t.Fatalf("create cell store: %v", err)
		}
		return cells
	}
	testCases := []struct {
		name    string
		cfgs    []config.ProgramConfig
		wantErr string
	}{
		{
			name: "duplicate within program",
			cfgs: []config.ProgramConfig{
				{
					ID:   "prog1",
					Type: "ramp",
					Inputs: []config.ProgramSignalConfig{{
						ID:   "target",
						Cell: "target",
					}},
					Outputs: []config.ProgramSignalConfig{
						{ID: "value", Cell: "output"},
						{ID: "secondary", Cell: "output"},
					},
				},
			},
			wantErr: "bound multiple times",
		},
		{
			name: "duplicate across programs",
			cfgs: []config.ProgramConfig{
				{
					ID:   "prog1",
					Type: "ramp",
					Inputs: []config.ProgramSignalConfig{{
						ID:   "target",
						Cell: "target",
					}},
					Outputs: []config.ProgramSignalConfig{{
						ID:   "value",
						Cell: "output",
					}},
				},
				{
					ID:   "prog2",
					Type: "ramp",
					Inputs: []config.ProgramSignalConfig{{
						ID:   "target",
						Cell: "target",
					}},
					Outputs: []config.ProgramSignalConfig{{
						ID:   "value",
						Cell: "output",
					}},
				},
			},
			wantErr: "already bound by program prog1",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cells := makeCells()
			_, err := newProgramBindings(tc.cfgs, cells, logger, nil)
			if err == nil {
				t.Fatalf("expected error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("unexpected error %q", err.Error())
			}
		})
	}
}

func TestValidateFailsOnDuplicateProgramOutputs(t *testing.T) {
	cfg := &config.Config{
		Cycle: config.Duration{Duration: time.Millisecond},
		Cells: []config.CellConfig{
			{ID: "target", Type: config.ValueKindNumber, Constant: 1},
			{ID: "output", Type: config.ValueKindNumber},
		},
		Programs: []config.ProgramConfig{
			{
				ID:   "prog1",
				Type: "ramp",
				Inputs: []config.ProgramSignalConfig{{
					ID:   "target",
					Cell: "target",
				}},
				Outputs: []config.ProgramSignalConfig{{
					ID:   "value",
					Cell: "output",
				}},
			},
			{
				ID:   "prog2",
				Type: "ramp",
				Inputs: []config.ProgramSignalConfig{{
					ID:   "target",
					Cell: "target",
				}},
				Outputs: []config.ProgramSignalConfig{{
					ID:   "value",
					Cell: "output",
				}},
			},
		},
	}

	logger := zerolog.New(io.Discard)
	err := Validate(cfg, logger)
	if err == nil {
		t.Fatalf("expected duplicate output validation error")
	}
	if !strings.Contains(err.Error(), "already bound by program prog1") {
		t.Fatalf("unexpected error: %v", err)
	}
}
