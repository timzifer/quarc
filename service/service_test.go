package service

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/timzifer/quarc/config"
	"github.com/timzifer/quarc/runtime/connections"
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

type stubReadPlan struct {
	Function string  `json:"function"`
	Start    *uint16 `json:"start"`
	Length   *uint16 `json:"length"`
}

func stubDriverSettings(function string, start, length uint16) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"function":%q,"start":%d,"length":%d}`, function, start, length))
}

func extractStubPlan(cfg config.ReadGroupConfig) (string, uint16, uint16, error) {
	var function string
	var functionSet bool
	var start uint16
	var startSet bool
	var length uint16
	var lengthSet bool

	decode := func(raw json.RawMessage) error {
		if len(raw) == 0 {
			return nil
		}
		var plan stubReadPlan
		if err := json.Unmarshal(raw, &plan); err != nil {
			return err
		}
		if plan.Function != "" {
			function = plan.Function
			functionSet = true
		}
		if plan.Start != nil {
			start = *plan.Start
			startSet = true
		}
		if plan.Length != nil {
			length = *plan.Length
			lengthSet = true
		}
		return nil
	}

	if err := decode(cfg.Driver.Settings); err != nil {
		return "", 0, 0, fmt.Errorf("decode driver settings: %w", err)
	}
	if err := decode(cfg.DriverMetadata); err != nil {
		return "", 0, 0, fmt.Errorf("decode driver metadata: %w", err)
	}
	if !functionSet && cfg.LegacyFunction != "" {
		function = cfg.LegacyFunction
		functionSet = true
	}
	if !startSet && cfg.LegacyStart != nil {
		start = *cfg.LegacyStart
		startSet = true
	}
	if !lengthSet && cfg.LegacyLength != nil {
		length = *cfg.LegacyLength
		lengthSet = true
	}
	if !functionSet {
		return "", 0, 0, errors.New("missing function")
	}
	if !startSet {
		return "", 0, 0, errors.New("missing start")
	}
	if !lengthSet {
		return "", 0, 0, errors.New("missing length")
	}
	return function, start, length, nil
}

type testClientFactory func(cfg config.EndpointConfig) (testClient, error)

func stubOptions(factory testClientFactory) []Option {
	return []Option{
		WithReaderFactory(testDriverName, newStubReaderFactory(factory)),
		WithWriterFactory(testDriverName, newStubWriterFactory(factory)),
		WithConnectionFactory(testDriverName, newStubConnectionFactory(factory)),
	}
}

func newStubConnectionFactory(factory testClientFactory) connections.Factory {
	return func(cfg config.IOConnectionConfig) (connections.Handle, error) {
		if factory == nil {
			return nil, fmt.Errorf("connection %s: no client factory provided", cfg.ID)
		}
		client, err := factory(cfg.Endpoint)
		if err != nil {
			return nil, err
		}
		return client, nil
	}
}

func newStubReaderFactory(factory testClientFactory) readers.ReaderFactory {
	return func(cfg config.ReadGroupConfig, deps readers.ReaderDependencies) (readers.ReadGroup, error) {
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
		if deps.Buffers == nil {
			return nil, fmt.Errorf("read group %s: buffer store not initialised", cfg.ID)
		}
		buffer, err := deps.Buffers.Get(cellID)
		if err != nil {
			return nil, err
		}
		var (
			client     testClient
			ownsClient = true
		)
		if cfg.Connection != "" {
			if deps.Connections == nil {
				return nil, fmt.Errorf("read group %s: connection provider missing", cfg.ID)
			}
			handle, err := deps.Connections.Connection(cfg.Connection)
			if err != nil {
				return nil, err
			}
			var ok bool
			client, ok = handle.(testClient)
			if !ok {
				return nil, fmt.Errorf("read group %s: connection %s has incompatible handle", cfg.ID, cfg.Connection)
			}
			ownsClient = false
		} else {
			if factory == nil {
				return nil, fmt.Errorf("read group %s: no client factory provided", cfg.ID)
			}
			var err error
			client, err = factory(cfg.Endpoint)
			if err != nil {
				return nil, err
			}
		}
		function, start, length, err := extractStubPlan(cfg)
		if err != nil {
			return nil, fmt.Errorf("read group %s: %w", cfg.ID, err)
		}
		metadata := map[string]interface{}{
			"function": function,
			"start":    start,
			"length":   length,
		}
		driverName := strings.TrimSpace(cfg.Driver.Name)
		return &stubReadGroup{
			id:         cfg.ID,
			client:     client,
			ownsClient: ownsClient,
			cell:       cell,
			buffer:     buffer,
			cellID:     cellID,
			driver:     driverName,
			function:   function,
			start:      start,
			length:     length,
			metadata:   metadata,
			source:     cfg.Source,
		}, nil
	}
}

func newStubWriterFactory(factory testClientFactory) writers.WriterFactory {
	return func(cfg config.WriteTargetConfig, deps writers.WriterDependencies) (writers.Writer, error) {
		if cfg.Cell == "" {
			return nil, fmt.Errorf("write target %s: missing cell", cfg.ID)
		}
		cell, err := deps.Cells.Get(cfg.Cell)
		if err != nil {
			return nil, err
		}
		var (
			client     testClient
			ownsClient = true
		)
		if cfg.Connection != "" {
			if deps.Connections == nil {
				return nil, fmt.Errorf("write target %s: connection provider missing", cfg.ID)
			}
			handle, err := deps.Connections.Connection(cfg.Connection)
			if err != nil {
				return nil, err
			}
			var ok bool
			client, ok = handle.(testClient)
			if !ok {
				return nil, fmt.Errorf("write target %s: connection %s has incompatible handle", cfg.ID, cfg.Connection)
			}
			ownsClient = false
		} else {
			if factory == nil {
				return nil, fmt.Errorf("write target %s: no client factory provided", cfg.ID)
			}
			var err error
			client, err = factory(cfg.Endpoint)
			if err != nil {
				return nil, err
			}
		}
		return &stubWriter{
			id:         cfg.ID,
			client:     client,
			ownsClient: ownsClient,
			cell:       cell,
			cellID:     cfg.Cell,
			address:    cfg.Address,
			source:     cfg.Source,
		}, nil
	}
}

type stubReadGroup struct {
	id         string
	client     testClient
	ownsClient bool
	cell       state.Cell
	buffer     *readers.SignalBuffer
	cellID     string
	driver     string
	function   string
	start      uint16
	length     uint16
	metadata   map[string]interface{}
	source     config.ModuleReference

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
	if g.buffer != nil {
		if err := g.buffer.Push(now, float64(value), nil); err != nil {
			if errors.Is(err, readers.ErrSignalBufferOverflow) {
				logger.Warn().Str("group", g.id).Str("cell", g.cellID).Msg("stub read buffer overflow")
			} else {
				logger.Error().Err(err).Str("group", g.id).Msg("stub read buffer push failed")
				return 1
			}
		}
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
		Driver:       g.driver,
		Disabled:     g.disabled.Load(),
		LastRun:      g.lastRun,
		LastDuration: g.lastDuration,
		Source:       g.source,
		Metadata:     g.metadata,
	}
}

func (g *stubReadGroup) Close() {
	if g.ownsClient {
		_ = g.client.Close()
	}
}

type stubWriter struct {
	id         string
	client     testClient
	ownsClient bool
	cell       state.Cell
	cellID     string
	address    uint16
	source     config.ModuleReference

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
	if w.ownsClient {
		_ = w.client.Close()
	}
}

type bufferDropRecord struct {
	group  string
	buffer string
	count  uint64
}

type recordingCollector struct {
	dropped   []bufferDropRecord
	occupancy map[string]int
}

func (recordingCollector) IncHotReload(string) {}

func (c *recordingCollector) IncBufferDropped(group, buffer string, count uint64) {
	if c == nil || count == 0 {
		return
	}
	c.dropped = append(c.dropped, bufferDropRecord{group: group, buffer: buffer, count: count})
}

func (c *recordingCollector) SetBufferOccupancy(group, buffer string, occupancy int) {
	if c == nil {
		return
	}
	if c.occupancy == nil {
		c.occupancy = make(map[string]int)
	}
	c.occupancy[group+":"+buffer] = occupancy
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
				Driver:   config.DriverConfig{Name: testDriverName, Settings: stubDriverSettings("holding", 0, 1)},
				Endpoint: config.EndpointConfig{Address: "ignored:502", UnitID: 1},
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
				Driver:   config.DriverConfig{Name: testDriverName},
				Endpoint: config.EndpointConfig{Address: "ignored:502", UnitID: 1},
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
			Cells:   []config.ServerCellConfig{{Cell: "value", Address: 0}},
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
			Driver:   config.DriverConfig{Name: testDriverName, Settings: stubDriverSettings("holding", 0, 1)},
			Endpoint: config.EndpointConfig{Address: "ignored:1", UnitID: 1},
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
			Driver:   config.DriverConfig{Name: testDriverName},
			Endpoint: config.EndpointConfig{Address: "ignored:1", UnitID: 1},
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

func TestServiceBufferStatusAndTelemetry(t *testing.T) {
	cfg := &config.Config{
		Cycle: config.Duration{Duration: time.Millisecond},
		Cells: []config.CellConfig{{ID: "input", Type: config.ValueKindNumber}},
		Reads: []config.ReadGroupConfig{{
			ID:       "rg",
			Driver:   config.DriverConfig{Name: testDriverName, Settings: stubDriverSettings("holding", 0, 1)},
			Endpoint: config.EndpointConfig{Address: "ignored:1", UnitID: 1},
			Signals: []config.ReadSignalConfig{{
				Cell:       "input",
				Offset:     0,
				Type:       config.ValueKindNumber,
				BufferSize: 2,
			}},
		}},
	}

	factory := func(config.EndpointConfig) (testClient, error) {
		client := &fakeClient{}
		client.readHoldingFn = func(address, quantity uint16) ([]byte, error) {
			return []byte{0x00, 0x01}, nil
		}
		return client, nil
	}

	logger := zerolog.New(io.Discard)
	svc, err := New(cfg, logger, stubOptions(factory)...)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	collector := &recordingCollector{}
	svc.SetTelemetry(collector)

	buffer, err := svc.buffers.Get("input")
	if err != nil {
		t.Fatalf("get buffer: %v", err)
	}
	now := time.Now()
	if err := buffer.Push(now, 1.0, nil); err != nil {
		t.Fatalf("push 1: %v", err)
	}
	if err := buffer.Push(now.Add(time.Millisecond), 2.0, nil); err != nil {
		t.Fatalf("push 2: %v", err)
	}
	thirdTs := now.Add(2 * time.Millisecond)
	if err := buffer.Push(thirdTs, 3.0, nil); !errors.Is(err, readers.ErrSignalBufferOverflow) {
		t.Fatalf("expected overflow error, got %v", err)
	}

	if svc.flushBufferPhase(now.Add(3*time.Millisecond)) == 0 {
		t.Fatalf("expected flush to report overflow")
	}

	statuses := svc.ReadStatuses()
	if len(statuses) != 1 {
		t.Fatalf("expected single read status, got %d", len(statuses))
	}
	bufStatus, ok := statuses[0].Buffers["input"]
	if !ok {
		t.Fatalf("buffer status for input missing")
	}
	if bufStatus.Buffered != 2 {
		t.Fatalf("expected buffered count 2, got %d", bufStatus.Buffered)
	}
	if bufStatus.Dropped != 1 {
		t.Fatalf("expected dropped count 1, got %d", bufStatus.Dropped)
	}
	aggStatus, ok := bufStatus.Aggregations["input"]
	if !ok {
		t.Fatalf("expected aggregation status for input")
	}
	if aggStatus.LastAggregate.Count != 2 {
		t.Fatalf("expected aggregate count 2, got %d", aggStatus.LastAggregate.Count)
	}
	if !aggStatus.LastAggregate.Overflow {
		t.Fatalf("expected aggregate overflow flag")
	}
	if aggStatus.LastAggregate.Timestamp.IsZero() || !aggStatus.LastAggregate.Timestamp.Equal(thirdTs) {
		t.Fatalf("unexpected aggregate timestamp %v", aggStatus.LastAggregate.Timestamp)
	}
	if value, ok := aggStatus.LastAggregate.Value.(float64); !ok || value != 3.0 {
		t.Fatalf("expected aggregate value 3.0, got %v", aggStatus.LastAggregate.Value)
	}

	if len(collector.dropped) != 1 {
		t.Fatalf("expected telemetry drop record, got %d", len(collector.dropped))
	}
	drop := collector.dropped[0]
	if drop.group != "rg" || drop.buffer != "input" || drop.count != 1 {
		t.Fatalf("unexpected drop record: %+v", drop)
	}
	if collector.occupancy["rg:input"] != 2 {
		t.Fatalf("expected occupancy gauge 2, got %d", collector.occupancy["rg:input"])
	}
}

func TestServiceFlushBufferPhaseAppliesAggregations(t *testing.T) {
	cfg := &config.Config{
		Cycle: config.Duration{Duration: time.Millisecond},
		Cells: []config.CellConfig{
			{ID: "input", Type: config.ValueKindNumber},
			{ID: "aggregate", Type: config.ValueKindNumber},
			{ID: "quality", Type: config.ValueKindNumber},
		},
		Reads: []config.ReadGroupConfig{{
			ID:       "rg",
			Driver:   config.DriverConfig{Name: testDriverName, Settings: stubDriverSettings("holding", 0, 1)},
			Endpoint: config.EndpointConfig{Address: "ignored:1", UnitID: 1},
			Signals: []config.ReadSignalConfig{{
				Cell:       "input",
				Offset:     0,
				Type:       config.ValueKindNumber,
				BufferSize: 4,
				Aggregations: []config.ReadSignalAggregationConfig{{
					Cell:       "aggregate",
					Aggregator: "sum",
					Quality:    "quality",
				}},
			}},
		}},
	}

	factory := func(config.EndpointConfig) (testClient, error) {
		return &fakeClient{}, nil
	}

	logger := zerolog.New(io.Discard)
	svc, err := New(cfg, logger, stubOptions(factory)...)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	buffer, err := svc.buffers.Get("input")
	if err != nil {
		t.Fatalf("get buffer: %v", err)
	}
	now := time.Now()
	q1 := 0.25
	if err := buffer.Push(now, 1.5, &q1); err != nil {
		t.Fatalf("push 1: %v", err)
	}
	q2 := 0.75
	if err := buffer.Push(now.Add(time.Millisecond), 2.5, &q2); err != nil {
		t.Fatalf("push 2: %v", err)
	}

	if errors := svc.flushBufferPhase(now.Add(2 * time.Millisecond)); errors != 0 {
		t.Fatalf("expected no flush errors, got %d", errors)
	}

	aggCell, err := svc.cells.mustGet("aggregate")
	if err != nil {
		t.Fatalf("lookup aggregate cell: %v", err)
	}
	if value, ok := aggCell.CurrentValue(); !ok {
		t.Fatalf("expected aggregate value to be set")
	} else if got, ok := value.(float64); !ok || got != 4.0 {
		t.Fatalf("expected aggregate value 4.0, got %v", value)
	}
	state, err := svc.cells.state("aggregate")
	if err != nil {
		t.Fatalf("aggregate state: %v", err)
	}
	if state.Diagnosis != nil {
		t.Fatalf("unexpected aggregate diagnosis: %+v", state.Diagnosis)
	}

	qualityCell, err := svc.cells.mustGet("quality")
	if err != nil {
		t.Fatalf("lookup quality cell: %v", err)
	}
	if value, ok := qualityCell.CurrentValue(); !ok {
		t.Fatalf("expected quality value to be set")
	} else if got, ok := value.(float64); !ok || got != q2 {
		t.Fatalf("expected quality value %.2f, got %v", q2, value)
	}
}

func TestServiceFlushBufferPhaseHandlesOverflowPolicies(t *testing.T) {
	cfg := &config.Config{
		Cycle: config.Duration{Duration: time.Millisecond},
		Cells: []config.CellConfig{
			{ID: "input", Type: config.ValueKindNumber},
			{ID: "overflow", Type: config.ValueKindNumber},
			{ID: "ignored", Type: config.ValueKindNumber},
		},
		Reads: []config.ReadGroupConfig{{
			ID:       "rg",
			Driver:   config.DriverConfig{Name: testDriverName, Settings: stubDriverSettings("holding", 0, 1)},
			Endpoint: config.EndpointConfig{Address: "ignored:1", UnitID: 1},
			Signals: []config.ReadSignalConfig{{
				Cell:       "input",
				Offset:     0,
				Type:       config.ValueKindNumber,
				BufferSize: 2,
				Aggregations: []config.ReadSignalAggregationConfig{
					{Cell: "overflow", Aggregator: "last"},
					{Cell: "ignored", Aggregator: "last", OnOverflow: "ignore"},
				},
			}},
		}},
	}

	factory := func(config.EndpointConfig) (testClient, error) {
		return &fakeClient{}, nil
	}

	logger := zerolog.New(io.Discard)
	svc, err := New(cfg, logger, stubOptions(factory)...)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	buffer, err := svc.buffers.Get("input")
	if err != nil {
		t.Fatalf("get buffer: %v", err)
	}
	now := time.Now()
	if err := buffer.Push(now, 1.0, nil); err != nil {
		t.Fatalf("push 1: %v", err)
	}
	if err := buffer.Push(now.Add(time.Millisecond), 2.0, nil); err != nil {
		t.Fatalf("push 2: %v", err)
	}
	if err := buffer.Push(now.Add(2*time.Millisecond), 3.0, nil); !errors.Is(err, readers.ErrSignalBufferOverflow) {
		t.Fatalf("expected overflow error, got %v", err)
	}

	if errors := svc.flushBufferPhase(now.Add(3 * time.Millisecond)); errors != 1 {
		t.Fatalf("expected single flush error, got %d", errors)
	}

	overflowCell, err := svc.cells.mustGet("overflow")
	if err != nil {
		t.Fatalf("lookup overflow cell: %v", err)
	}
	if value, ok := overflowCell.CurrentValue(); ok {
		t.Fatalf("expected overflow cell to be invalid, got value %v", value)
	}
	overflowState, err := svc.cells.state("overflow")
	if err != nil {
		t.Fatalf("overflow state: %v", err)
	}
	if overflowState.Diagnosis == nil || overflowState.Diagnosis.Code != diagCodeBufferOverflow {
		t.Fatalf("expected buffer overflow diagnosis, got %+v", overflowState.Diagnosis)
	}

	ignoredCell, err := svc.cells.mustGet("ignored")
	if err != nil {
		t.Fatalf("lookup ignored cell: %v", err)
	}
	if value, ok := ignoredCell.CurrentValue(); !ok {
		t.Fatalf("expected ignored cell to have value")
	} else if got, ok := value.(float64); !ok || got != 3.0 {
		t.Fatalf("expected ignored cell value 3.0, got %v", value)
	}
	ignoredState, err := svc.cells.state("ignored")
	if err != nil {
		t.Fatalf("ignored state: %v", err)
	}
	if ignoredState.Diagnosis != nil {
		t.Fatalf("expected ignored cell to remain valid, got diagnosis %+v", ignoredState.Diagnosis)
	}
}

func TestServiceFlushBufferPhaseAggregationErrorsMarkCellsInvalid(t *testing.T) {
	cfg := &config.Config{
		Cycle: config.Duration{Duration: time.Millisecond},
		Cells: []config.CellConfig{
			{ID: "input", Type: config.ValueKindString},
			{ID: "sum", Type: config.ValueKindNumber},
			{ID: "last", Type: config.ValueKindString},
		},
		Reads: []config.ReadGroupConfig{{
			ID:       "rg",
			Driver:   config.DriverConfig{Name: testDriverName, Settings: stubDriverSettings("holding", 0, 1)},
			Endpoint: config.EndpointConfig{Address: "ignored:1", UnitID: 1},
			Signals: []config.ReadSignalConfig{{
				Cell:       "input",
				Offset:     0,
				Type:       config.ValueKindString,
				BufferSize: 2,
				Aggregations: []config.ReadSignalAggregationConfig{
					{Cell: "sum", Aggregator: "sum"},
					{Cell: "last", Aggregator: "last"},
				},
			}},
		}},
	}

	factory := func(config.EndpointConfig) (testClient, error) {
		return &fakeClient{}, nil
	}

	logger := zerolog.New(io.Discard)
	svc, err := New(cfg, logger, stubOptions(factory)...)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	buffer, err := svc.buffers.Get("input")
	if err != nil {
		t.Fatalf("get buffer: %v", err)
	}
	now := time.Now()
	if err := buffer.Push(now, "not-a-number", nil); err != nil {
		t.Fatalf("push: %v", err)
	}

	if errors := svc.flushBufferPhase(now.Add(time.Millisecond)); errors != 1 {
		t.Fatalf("expected single flush error, got %d", errors)
	}

	sumCell, err := svc.cells.mustGet("sum")
	if err != nil {
		t.Fatalf("lookup sum cell: %v", err)
	}
	if value, ok := sumCell.CurrentValue(); ok {
		t.Fatalf("expected sum cell to be invalid, got value %v", value)
	}
	sumState, err := svc.cells.state("sum")
	if err != nil {
		t.Fatalf("sum state: %v", err)
	}
	if sumState.Diagnosis == nil || sumState.Diagnosis.Code != diagCodeAggregationError {
		t.Fatalf("expected aggregation error diagnosis, got %+v", sumState.Diagnosis)
	}

	lastCell, err := svc.cells.mustGet("last")
	if err != nil {
		t.Fatalf("lookup last cell: %v", err)
	}
	if value, ok := lastCell.CurrentValue(); !ok {
		t.Fatalf("expected last cell to have value")
	} else if got, ok := value.(string); !ok || got != "not-a-number" {
		t.Fatalf("expected last cell to retain value, got %v", value)
	}
	lastState, err := svc.cells.state("last")
	if err != nil {
		t.Fatalf("last state: %v", err)
	}
	if lastState.Diagnosis != nil {
		t.Fatalf("expected last cell to remain valid, got diagnosis %+v", lastState.Diagnosis)
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

func TestServiceStateResponseContainsSplitIdentifiers(t *testing.T) {
	_, _, _, ts := newLiveViewTestServer(t)

	resp, err := ts.Client().Get(ts.URL + "/api/state")
	if err != nil {
		t.Fatalf("request state: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	var payload struct {
		Cells []struct {
			ID      string `json:"id"`
			Package string `json:"package"`
			LocalID string `json:"local_id"`
		} `json:"cells"`
		Reads []struct {
			ID      string `json:"id"`
			Package string `json:"package"`
			LocalID string `json:"local_id"`
		} `json:"reads"`
		Writes []struct {
			ID      string `json:"id"`
			Package string `json:"package"`
			LocalID string `json:"local_id"`
		} `json:"writes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode state payload: %v", err)
	}

	if len(payload.Cells) == 0 {
		t.Fatal("expected cell payload")
	}
	if payload.Cells[0].Package == "" || payload.Cells[0].LocalID == "" {
		t.Fatalf("expected split identifiers for cells, got %+v", payload.Cells[0])
	}

	if len(payload.Reads) == 0 {
		t.Fatal("expected read payload")
	}
	if payload.Reads[0].Package == "" || payload.Reads[0].LocalID == "" {
		t.Fatalf("expected split identifiers for reads, got %+v", payload.Reads[0])
	}

	if len(payload.Writes) == 0 {
		t.Fatal("expected write payload")
	}
	if payload.Writes[0].Package == "" || payload.Writes[0].LocalID == "" {
		t.Fatalf("expected split identifiers for writes, got %+v", payload.Writes[0])
	}
}

func TestServiceSharedConnections(t *testing.T) {
	cfg := &config.Config{
		Cycle: config.Duration{Duration: time.Millisecond},
		Connections: []config.IOConnectionConfig{{
			ID:     "stub_bus",
			Driver: config.DriverConfig{Name: testDriverName},
			Endpoint: config.EndpointConfig{
				Address: "ignored:1",
			},
		}},
		Cells: []config.CellConfig{
			{ID: "input", Type: config.ValueKindNumber},
			{ID: "output", Type: config.ValueKindBool},
		},
		Reads: []config.ReadGroupConfig{{
			ID:         "rg",
			Connection: "stub_bus",
			Driver:     config.DriverConfig{Name: testDriverName, Settings: stubDriverSettings("holding", 0, 1)},
			Signals: []config.ReadSignalConfig{{
				Cell:   "input",
				Offset: 0,
				Type:   config.ValueKindNumber,
			}},
		}},
		Writes: []config.WriteTargetConfig{{
			ID:         "wt",
			Cell:       "output",
			Connection: "stub_bus",
			Driver:     config.DriverConfig{Name: testDriverName},
			Function:   "coil",
			Address:    0,
		}},
	}

	client := &countingClient{}
	client.readHoldingFn = func(address, quantity uint16) ([]byte, error) {
		return []byte{0x00, 0x01}, nil
	}
	client.writeCoilFn = func(address, value uint16) ([]byte, error) {
		return nil, nil
	}

	var created int
	factory := func(config.EndpointConfig) (testClient, error) {
		created++
		return client, nil
	}

	logger := zerolog.New(io.Discard)
	svc, err := New(cfg, logger, stubOptions(factory)...)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	if created != 1 {
		t.Fatalf("expected one shared connection, got %d", created)
	}

	if err := svc.IterateOnce(context.Background(), time.Now()); err != nil {
		t.Fatalf("iterate: %v", err)
	}

	if err := svc.Close(); err != nil {
		t.Fatalf("close service: %v", err)
	}

	if client.closed != 1 {
		t.Fatalf("expected shared connection to be closed once, got %d", client.closed)
	}
}
