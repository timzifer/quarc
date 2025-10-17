package modbus

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/timzifer/quarc/config"
)

func TestResolveReadGroupAppliesJSONOverrides(t *testing.T) {
	cfg := config.ReadGroupConfig{
		ID: "group1",
		Driver: config.DriverConfig{Name: "modbus", Settings: []byte(`{
                        "function": "input",
                        "start": 16,
                        "length": 32,
                        "max_gap_size": 4
                }`)},
	}

	resolved, plan, err := resolveReadGroup(cfg)
	if err != nil {
		t.Fatalf("resolveReadGroup: %v", err)
	}
	if plan.Function != "input" {
		t.Fatalf("unexpected function: got %q want %q", plan.Function, "input")
	}
	if plan.Start != 16 {
		t.Fatalf("unexpected start: got %d want %d", plan.Start, 16)
	}
	if plan.Length != 32 {
		t.Fatalf("unexpected length: got %d want %d", plan.Length, 32)
	}
	if plan.Legacy {
		t.Fatal("expected plan to be marked non-legacy")
	}
	if len(resolved.DriverMetadata) == 0 {
		t.Fatal("expected driver metadata to be populated")
	}
	if plan.MaxGapSize != 4 {
		t.Fatalf("unexpected max gap size: got %d want %d", plan.MaxGapSize, 4)
	}
}

func TestResolveReadGroupInvalidJSON(t *testing.T) {
	cfg := config.ReadGroupConfig{
		ID:     "group1",
		Driver: config.DriverConfig{Name: "modbus", Settings: []byte("not-json")},
	}

	if _, _, err := resolveReadGroup(cfg); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestResolveReadGroupUsesLegacyFields(t *testing.T) {
	start := uint16(2)
	length := uint16(4)
	cfg := config.ReadGroupConfig{
		ID:             "group1",
		LegacyFunction: "holding",
		LegacyStart:    &start,
		LegacyLength:   &length,
	}

	resolved, plan, err := resolveReadGroup(cfg)
	if err != nil {
		t.Fatalf("resolveReadGroup: %v", err)
	}
	if !plan.Legacy {
		t.Fatal("expected plan to be marked legacy")
	}
	if plan.Function != "holding" {
		t.Fatalf("unexpected function: got %q want %q", plan.Function, "holding")
	}
	if plan.Start != start {
		t.Fatalf("unexpected start: got %d want %d", plan.Start, start)
	}
	if plan.Length != length {
		t.Fatalf("unexpected length: got %d want %d", plan.Length, length)
	}
	if plan.MaxGapSize != length {
		t.Fatalf("unexpected max gap size: got %d want %d", plan.MaxGapSize, length)
	}
	if len(resolved.DriverMetadata) == 0 {
		t.Fatal("expected driver metadata to be populated for legacy plan")
	}
	var metadata map[string]interface{}
	if err := json.Unmarshal(resolved.DriverMetadata, &metadata); err != nil {
		t.Fatalf("decode driver metadata: %v", err)
	}
	if legacy, ok := metadata["legacy"].(bool); !ok || !legacy {
		t.Fatalf("expected legacy flag in metadata, got %#v", metadata["legacy"])
	}
}

func TestResolveReadGroupDefaultsMaxGapToLength(t *testing.T) {
	cfg := config.ReadGroupConfig{
		ID: "group1",
		Driver: config.DriverConfig{Name: "modbus", Settings: []byte(`{
                        "function": "holding",
                        "start": 0,
                        "length": 6
                }`)},
	}

	_, plan, err := resolveReadGroup(cfg)
	if err != nil {
		t.Fatalf("resolveReadGroup: %v", err)
	}
	if plan.MaxGapSize != 6 {
		t.Fatalf("unexpected max gap size default: got %d want %d", plan.MaxGapSize, 6)
	}
}

func TestResolveWriteTargetAppliesJSONOverrides(t *testing.T) {
	cfg := config.WriteTargetConfig{
		ID:       "target1",
		Function: "holding",
		Address:  10,
		Scale:    1,
		Driver: config.DriverConfig{Name: "modbus", Settings: []byte(`{
                        "function": "coil",
                        "address": 42,
                        "endianness": "be",
                        "signed": true,
                        "deadband": 0.5,
                        "rate_limit": "5s",
                        "scale": 2.5
                }`)},
	}

	resolved, err := resolveWriteTarget(cfg)
	if err != nil {
		t.Fatalf("resolveWriteTarget: %v", err)
	}
	if resolved.Function != "coil" {
		t.Fatalf("unexpected function: got %q want %q", resolved.Function, "coil")
	}
	if resolved.Address != 42 {
		t.Fatalf("unexpected address: got %d want %d", resolved.Address, 42)
	}
	if resolved.Endianness != "be" {
		t.Fatalf("unexpected endianness: got %q want %q", resolved.Endianness, "be")
	}
	if !resolved.Signed {
		t.Fatal("expected signed override to be true")
	}
	if resolved.Deadband != 0.5 {
		t.Fatalf("unexpected deadband: got %v want %v", resolved.Deadband, 0.5)
	}
	if resolved.RateLimit.Duration != 5*time.Second {
		t.Fatalf("unexpected rate limit: got %s want %s", resolved.RateLimit.Duration, 5*time.Second)
	}
	if resolved.Scale != 2.5 {
		t.Fatalf("unexpected scale: got %v want %v", resolved.Scale, 2.5)
	}
}

func TestResolveWriteTargetInvalidJSON(t *testing.T) {
	cfg := config.WriteTargetConfig{
		ID:       "target1",
		Function: "holding",
		Address:  10,
		Driver:   config.DriverConfig{Name: "modbus", Settings: []byte("{")},
	}

	if _, err := resolveWriteTarget(cfg); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}
