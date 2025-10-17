package modbus

import (
	"testing"
	"time"

	"github.com/timzifer/quarc/config"
)

func TestResolveReadGroupAppliesJSONOverrides(t *testing.T) {
	cfg := config.ReadGroupConfig{
		ID:       "group1",
		Function: "holding",
		Start:    1,
		Length:   2,
		Driver: config.DriverConfig{Name: "modbus", Settings: []byte(`{
                        "function": "input",
                        "start": 16,
                        "length": 32
                }`)},
	}

	resolved, err := resolveReadGroup(cfg)
	if err != nil {
		t.Fatalf("resolveReadGroup: %v", err)
	}
	if resolved.Function != "input" {
		t.Fatalf("unexpected function: got %q want %q", resolved.Function, "input")
	}
	if resolved.Start != 16 {
		t.Fatalf("unexpected start: got %d want %d", resolved.Start, 16)
	}
	if resolved.Length != 32 {
		t.Fatalf("unexpected length: got %d want %d", resolved.Length, 32)
	}
}

func TestResolveReadGroupInvalidJSON(t *testing.T) {
	cfg := config.ReadGroupConfig{
		ID:       "group1",
		Function: "holding",
		Start:    1,
		Length:   2,
		Driver:   config.DriverConfig{Name: "modbus", Settings: []byte("not-json")},
	}

	if _, err := resolveReadGroup(cfg); err == nil {
		t.Fatal("expected error for invalid JSON")
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
