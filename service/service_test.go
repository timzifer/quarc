package service

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/timzifer/modbus_processor/config"
	modbusdrv "github.com/timzifer/modbus_processor/drivers/modbus"
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

func modbusOptions(factory modbusdrv.ClientFactory) []Option {
	return []Option{
		WithReaderFactory("modbus", modbusdrv.NewReaderFactory(factory)),
		WithWriterFactory("modbus", modbusdrv.NewWriterFactory(factory)),
	}
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

	factory := func(config.EndpointConfig) (modbusdrv.Client, error) {
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
				Endpoint: config.EndpointConfig{Address: "ignored:502", UnitID: 1},
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
				Endpoint: config.EndpointConfig{Address: "ignored:502", UnitID: 1},
				Function: "coil",
				Address:  10,
			},
		},
	}

	logger := zerolog.New(io.Discard)
	svc, err := New(cfg, logger, modbusOptions(factory)...)
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

func TestServiceCloseShutsDownServerConnections(t *testing.T) {
	cfg := &config.Config{
		Cycle: config.Duration{Duration: time.Millisecond},
		Cells: []config.CellConfig{{ID: "foo", Type: config.ValueKindNumber}},
		Server: config.ServerConfig{
			Enabled: true,
			Listen:  "127.0.0.1:0",
			Cells: []config.ServerCellConfig{{
				Cell:    "foo",
				Address: 0,
			}},
		},
	}

	logger := zerolog.New(io.Discard)
	svc, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	addr := svc.ServerAddress()
	if addr == "" {
		t.Fatalf("expected server address")
	}

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial modbus server: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := svc.Close(); err != nil {
			t.Errorf("service close: %v", err)
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("service close timed out")
	}

	_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatalf("expected connection to be closed")
	}
	_ = conn.Close()
}

func TestModbusServerExposesCells(t *testing.T) {
	cfg := &config.Config{
		Cycle: config.Duration{Duration: time.Millisecond},
		Cells: []config.CellConfig{
			{ID: "number", Type: config.ValueKindNumber},
			{ID: "flag", Type: config.ValueKindBool},
		},
		Server: config.ServerConfig{
			Enabled: true,
			Listen:  "127.0.0.1:0",
			UnitID:  1,
			Cells: []config.ServerCellConfig{
				{Cell: "number", Address: 0, Scale: 0.1},
				{Cell: "flag", Address: 1},
			},
		},
	}

	logger := zerolog.New(io.Discard)
	svc, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	numberCell, err := svc.cells.mustGet("number")
	if err != nil {
		t.Fatalf("get number cell: %v", err)
	}
	if err := numberCell.setValue(float64(12.3), time.Now(), nil); err != nil {
		t.Fatalf("set number value: %v", err)
	}
	flagCell, err := svc.cells.mustGet("flag")
	if err != nil {
		t.Fatalf("get flag cell: %v", err)
	}
	if err := flagCell.setValue(true, time.Now(), nil); err != nil {
		t.Fatalf("set flag value: %v", err)
	}

	if err := svc.IterateOnce(context.Background(), time.Now()); err != nil {
		t.Fatalf("iterate: %v", err)
	}

	addr := svc.ServerAddress()
	if addr == "" {
		t.Fatalf("expected server address")
	}
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial server: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	request := make([]byte, 12)
	binary.BigEndian.PutUint16(request[0:2], 1)
	binary.BigEndian.PutUint16(request[2:4], 0)
	binary.BigEndian.PutUint16(request[4:6], 6)
	request[6] = 1
	request[7] = 0x04
	binary.BigEndian.PutUint16(request[8:10], 0)
	binary.BigEndian.PutUint16(request[10:12], 2)

	if _, err := conn.Write(request); err != nil {
		t.Fatalf("write request: %v", err)
	}

	header := make([]byte, 7)
	if _, err := io.ReadFull(conn, header); err != nil {
		t.Fatalf("read header: %v", err)
	}
	if header[6] != 1 {
		t.Fatalf("unexpected unit id: %d", header[6])
	}
	length := int(binary.BigEndian.Uint16(header[4:6]))
	if length <= 1 {
		t.Fatalf("invalid length: %d", length)
	}
	pdu := make([]byte, length-1)
	if _, err := io.ReadFull(conn, pdu); err != nil {
		t.Fatalf("read body: %v", err)
	}
	if pdu[0] != 0x04 {
		t.Fatalf("unexpected function code: %d", pdu[0])
	}
	if pdu[1] != 4 {
		t.Fatalf("unexpected byte count: %d", pdu[1])
	}
	if len(pdu) != 6 {
		t.Fatalf("unexpected pdu length: %d", len(pdu))
	}
	numberValue := binary.BigEndian.Uint16(pdu[2:4])
	flagValue := binary.BigEndian.Uint16(pdu[4:6])
	if numberValue != 123 {
		t.Fatalf("expected number register 123, got %d", numberValue)
	}
	if flagValue != 1 {
		t.Fatalf("expected flag register 1, got %d", flagValue)
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

func TestServiceClosesRemoteClients(t *testing.T) {
	cfg := &config.Config{
		Cycle: config.Duration{Duration: time.Millisecond},
		Cells: []config.CellConfig{
			{ID: "input", Type: config.ValueKindNumber},
			{ID: "output", Type: config.ValueKindBool},
		},
		Reads: []config.ReadGroupConfig{{
			ID:       "rg",
			Endpoint: config.EndpointConfig{Address: "ignored:1", UnitID: 1},
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
			Endpoint: config.EndpointConfig{Address: "ignored:1", UnitID: 1},
			Function: "coil",
			Address:  0,
		}},
	}

	clients := make([]*countingClient, 0, 2)
	factory := func(config.EndpointConfig) (modbusdrv.Client, error) {
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
	svc, err := New(cfg, logger, modbusOptions(factory)...)
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

func TestSetCellValueUpdatesServerImmediately(t *testing.T) {
	cfg := &config.Config{
		Cells: []config.CellConfig{{ID: "number", Type: config.ValueKindNumber}},
		Server: config.ServerConfig{
			Enabled: true,
			Listen:  "127.0.0.1:0",
			UnitID:  1,
			Cells: []config.ServerCellConfig{{
				Cell:    "number",
				Address: 0,
				Scale:   0.1,
			}},
		},
	}
	logger := zerolog.New(io.Discard)
	svc, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	if err := svc.SetCellValue("number", 12.3); err != nil {
		t.Fatalf("set cell value: %v", err)
	}

	addr := svc.ServerAddress()
	if addr == "" {
		t.Fatalf("expected server address")
	}

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial server: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	request := make([]byte, 12)
	binary.BigEndian.PutUint16(request[0:2], 1)
	binary.BigEndian.PutUint16(request[2:4], 0)
	binary.BigEndian.PutUint16(request[4:6], 6)
	request[6] = 1
	request[7] = 0x04
	binary.BigEndian.PutUint16(request[8:10], 0)
	binary.BigEndian.PutUint16(request[10:12], 1)

	if _, err := conn.Write(request); err != nil {
		t.Fatalf("write request: %v", err)
	}

	header := make([]byte, 7)
	if _, err := io.ReadFull(conn, header); err != nil {
		t.Fatalf("read header: %v", err)
	}
	length := int(binary.BigEndian.Uint16(header[4:6]))
	if length <= 1 {
		t.Fatalf("invalid length: %d", length)
	}
	pdu := make([]byte, length-1)
	if _, err := io.ReadFull(conn, pdu); err != nil {
		t.Fatalf("read body: %v", err)
	}
	if pdu[0] != 0x04 {
		t.Fatalf("unexpected function code: %d", pdu[0])
	}
	if pdu[1] != 2 {
		t.Fatalf("unexpected byte count: %d", pdu[1])
	}
	value := binary.BigEndian.Uint16(pdu[2:4])
	if value != 123 {
		t.Fatalf("expected register value 123, got %d", value)
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
