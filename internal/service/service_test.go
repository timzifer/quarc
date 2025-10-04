package service

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"modbus_processor/internal/config"
	"modbus_processor/internal/remote"
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

func TestDeterministicCycle(t *testing.T) {
	client := &fakeClient{}
	client.readHoldingFn = func(address, quantity uint16) ([]byte, error) {
		return []byte{0x00, 0xFA}, nil // 250
	}
	client.writeCoilFn = func(address, value uint16) ([]byte, error) {
		client.writes = append(client.writes, value)
		return nil, nil
	}

	factory := func(config.EndpointConfig) (remote.Client, error) {
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
				ID:           "deg_c_calc",
				Target:       "deg_c",
				Dependencies: []config.DependencyConfig{{Cell: "raw_temp", Type: config.ValueKindNumber}},
				Normal:       "success(value(\"raw_temp\") * 0.1)",
				Fallback:     "success(0)",
			},
			{
				ID:           "alarm_logic",
				Target:       "alarm",
				Dependencies: []config.DependencyConfig{{Cell: "deg_c", Type: config.ValueKindNumber}},
				Normal:       "success(value(\"deg_c\") > 20)",
				Fallback:     "success(false)",
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
	svc, err := New(cfg, logger, factory)
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

func TestFallbackExecutedOnInvalidDependency(t *testing.T) {
	cfg := &config.Config{
		Cycle: config.Duration{Duration: time.Millisecond},
		Cells: []config.CellConfig{
			{ID: "input_a", Type: config.ValueKindNumber},
			{ID: "output_b", Type: config.ValueKindNumber},
		},
		Logic: []config.LogicBlockConfig{
			{
				ID:           "compute",
				Target:       "output_b",
				Dependencies: []config.DependencyConfig{{Cell: "input_a", Type: config.ValueKindNumber}},
				Normal:       "success(value(\"input_a\") * 2)",
				Fallback:     "success(valid(\"input_a\") ? value(\"input_a\") : 42)",
			},
		},
	}

	logger := zerolog.New(io.Discard)
	svc, err := New(cfg, logger, func(config.EndpointConfig) (remote.Client, error) {
		return &fakeClient{}, nil
	})
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
				Normal:       "success(value(\"b\"))",
			},
			{
				ID:           "block_b",
				Target:       "b",
				Dependencies: []config.DependencyConfig{{Cell: "a", Type: config.ValueKindNumber}},
				Normal:       "success(value(\"a\"))",
			},
		},
	}

	logger := zerolog.New(io.Discard)
	if _, err := New(cfg, logger, func(config.EndpointConfig) (remote.Client, error) {
		return &fakeClient{}, nil
	}); err == nil {
		t.Fatalf("expected cycle detection error")
	}
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
	svc, err := New(cfg, logger, func(config.EndpointConfig) (remote.Client, error) {
		return &fakeClient{}, nil
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	numberCell, err := svc.cells.mustGet("number")
	if err != nil {
		t.Fatalf("get number cell: %v", err)
	}
	if err := numberCell.setValue(float64(12.3), time.Now()); err != nil {
		t.Fatalf("set number value: %v", err)
	}
	flagCell, err := svc.cells.mustGet("flag")
	if err != nil {
		t.Fatalf("get flag cell: %v", err)
	}
	if err := flagCell.setValue(true, time.Now()); err != nil {
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
