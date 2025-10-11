package canstream

import (
	"testing"

	"github.com/timzifer/modbus_processor/config"
	"go.einride.tech/can/pkg/dbc"
)

func TestExtractSignalBitsIntel(t *testing.T) {
	signal := &dbc.SignalDef{
		Name:        "Speed",
		StartBit:    0,
		Size:        16,
		IsBigEndian: false,
		IsSigned:    false,
		Factor:      0.1,
		Offset:      0,
	}
	frame := [8]byte{0x10, 0x27, 0, 0, 0, 0, 0, 0}
	raw, signed, err := extractSignalBits(signal, frame[:])
	if err != nil {
		t.Fatalf("extractSignalBits: %v", err)
	}
	if raw != 0x2710 {
		t.Fatalf("expected raw 0x2710, got 0x%X", raw)
	}
	if signed != 0x2710 {
		t.Fatalf("expected signed 0x2710, got 0x%X", signed)
	}
	binding := signalBinding{
		signal:     signal,
		cellConfig: config.CellConfig{Type: config.ValueKindNumber},
		factor:     signal.Factor,
	}
	value, err := binding.decode(frame[:])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if value.(float64) != 0x2710*0.1 {
		t.Fatalf("unexpected decoded value %v", value)
	}
}

func TestExtractSignalBitsMotorola(t *testing.T) {
	signal := &dbc.SignalDef{
		Name:        "Temperature",
		StartBit:    7,
		Size:        12,
		IsBigEndian: true,
		IsSigned:    true,
		Factor:      0.5,
		Offset:      -40,
	}
	frame := [8]byte{0x0F, 0xF0, 0, 0, 0, 0, 0, 0}
	raw, signed, err := extractSignalBits(signal, frame[:])
	if err != nil {
		t.Fatalf("extractSignalBits: %v", err)
	}
	if raw != 0xF00 {
		t.Fatalf("expected raw 0xF00, got 0x%X", raw)
	}
	if signed != -256 {
		t.Fatalf("expected signed -256, got %d", signed)
	}
	binding := signalBinding{
		signal:     signal,
		cellConfig: config.CellConfig{Type: config.ValueKindInteger},
		factor:     signal.Factor,
		offset:     signal.Offset,
	}
	value, err := binding.decode(frame[:])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, ok := value.(int64)
	if !ok {
		t.Fatalf("expected integer value, got %T", value)
	}
	if got != -168 {
		t.Fatalf("unexpected converted value %d", got)
	}
}

func TestDecodeFrame(t *testing.T) {
	payload := []byte{0x88, 0x12, 0x34, 0x56, 0x78, 1, 2, 3, 4, 5, 6, 7, 8}
	frame, consumed, err := decodeFrame(payload)
	if err != nil {
		t.Fatalf("decodeFrame: %v", err)
	}
	if consumed != 13 {
		t.Fatalf("unexpected consumed bytes %d", consumed)
	}
	if !frame.extended {
		t.Fatalf("expected extended frame")
	}
	if frame.id != 0x12345678&0x1FFFFFFF {
		t.Fatalf("unexpected id 0x%X", frame.id)
	}
	if frame.dlc != 8 {
		t.Fatalf("unexpected dlc %d", frame.dlc)
	}
}
