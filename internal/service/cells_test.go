package service

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"modbus_processor/internal/config"
)

func TestNewCellStoreConstantValue(t *testing.T) {
	store, err := newCellStore([]config.CellConfig{{
		ID:       "const_number",
		Type:     config.ValueKindNumber,
		Constant: 42,
	}})
	if err != nil {
		t.Fatalf("newCellStore: %v", err)
	}

	cell, err := store.mustGet("const_number")
	if err != nil {
		t.Fatalf("mustGet: %v", err)
	}
	if !cell.valid {
		t.Fatalf("expected constant cell to be valid")
	}
	if got, ok := cell.value.(float64); !ok || got != 42 {
		t.Fatalf("expected constant value 42, got %v", cell.value)
	}
}

func TestNewCellStoreConstantTypeMismatch(t *testing.T) {
	_, err := newCellStore([]config.CellConfig{{
		ID:       "const_invalid",
		Type:     config.ValueKindBool,
		Constant: "nope",
	}})
	if err == nil {
		t.Fatalf("expected error for mismatched constant type")
	}
}

func TestConvertValueExtendedTypes(t *testing.T) {
	t.Helper()
	val, err := convertValue(config.ValueKindInteger, int32(10))
	if err != nil {
		t.Fatalf("convert integer: %v", err)
	}
	if got := val.(int64); got != 10 {
		t.Fatalf("expected 10, got %d", got)
	}

	decVal, err := convertValue(config.ValueKindDecimal, "12.345")
	if err != nil {
		t.Fatalf("convert decimal: %v", err)
	}
	dec := decVal.(decimal.Decimal)
	if dec.String() != "12.345" {
		t.Fatalf("expected decimal 12.345, got %s", dec.String())
	}

	dateVal, err := convertValue(config.ValueKindDate, "2024-01-02")
	if err != nil {
		t.Fatalf("convert date: %v", err)
	}
	expected := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	if !dateVal.(time.Time).Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, dateVal)
	}
}

func TestCellStoreSnapshotReuse(t *testing.T) {
	store, err := newCellStore([]config.CellConfig{{ID: "value", Type: config.ValueKindInteger}})
	if err != nil {
		t.Fatalf("newCellStore: %v", err)
	}
	cell, err := store.mustGet("value")
	if err != nil {
		t.Fatalf("mustGet: %v", err)
	}
	if err := cell.setValue(1, time.Now(), nil); err != nil {
		t.Fatalf("setValue: %v", err)
	}

	initial := store.snapshot()
	ptr := initial["value"]
	if ptr == nil {
		t.Fatalf("expected snapshot entry")
	}

	if err := cell.setValue(2, time.Now(), nil); err != nil {
		t.Fatalf("setValue: %v", err)
	}

	reused := store.snapshotInto(initial)
	if reused["value"] != ptr {
		t.Fatalf("expected snapshot value pointer to be reused")
	}
	if got, ok := reused["value"].Value.(int64); !ok || got != 2 {
		t.Fatalf("expected updated value 2, got %v", reused["value"].Value)
	}
}
