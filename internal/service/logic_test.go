package service

import (
	"strings"
	"testing"

	"modbus_processor/internal/config"
)

func TestPrepareLogicBlockAutoDependencies(t *testing.T) {
	cells, err := newCellStore([]config.CellConfig{
		{ID: "a", Type: config.ValueKindNumber},
		{ID: "b", Type: config.ValueKindNumber},
		{ID: "target", Type: config.ValueKindNumber},
	})
	if err != nil {
		t.Fatalf("newCellStore: %v", err)
	}

	cfg := config.LogicBlockConfig{
		ID:       "example",
		Target:   "target",
		Normal:   "success(value(\"a\") + value(\"b\"))",
		Fallback: "success(valid(\"a\") ? value(\"a\") : 0)",
	}

	block, meta, err := prepareLogicBlock(cfg, cells, 0)
	if err != nil {
		t.Fatalf("prepareLogicBlock: %v", err)
	}
	if block == nil {
		t.Fatalf("block must not be nil")
	}

	if got, want := block.normalDependencyIDs, []string{"a", "b"}; !equalStringSlices(got, want) {
		t.Fatalf("normal dependencies = %v, want %v", got, want)
	}
	if got, want := block.fallbackDependencyIDs, []string{"a"}; !equalStringSlices(got, want) {
		t.Fatalf("fallback dependencies = %v, want %v", got, want)
	}

	depA := meta["a"]
	if depA == nil || !depA.normal || !depA.fallback {
		t.Fatalf("dependency metadata for 'a' missing normal/fallback flags: %+v", depA)
	}
	depB := meta["b"]
	if depB == nil || !depB.normal {
		t.Fatalf("dependency metadata for 'b' missing normal flag: %+v", depB)
	}
}

func TestPrepareLogicBlockMissingFallbackDependency(t *testing.T) {
	cells, err := newCellStore([]config.CellConfig{
		{ID: "input", Type: config.ValueKindNumber},
		{ID: "target", Type: config.ValueKindNumber},
	})
	if err != nil {
		t.Fatalf("newCellStore: %v", err)
	}

	cfg := config.LogicBlockConfig{
		ID:       "example",
		Target:   "target",
		Normal:   "success(value(\"input\"))",
		Fallback: "success(value(\"missing\"))",
	}

	_, _, err = prepareLogicBlock(cfg, cells, 0)
	if err == nil {
		t.Fatalf("expected error for missing fallback dependency")
	}
	if !strings.Contains(err.Error(), "fallback dependency") {
		t.Fatalf("expected fallback dependency error, got %v", err)
	}
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
