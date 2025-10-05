package service

import (
	"strings"
	"testing"
	"time"

	"modbus_processor/internal/config"
)

func TestPrepareLogicBlockAutoDependencies(t *testing.T) {
	dsl, err := newDSLEngine(config.DSLConfig{})
	if err != nil {
		t.Fatalf("newDSLEngine: %v", err)
	}
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

	block, meta, err := prepareLogicBlock(cfg, cells, dsl, 0)
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

func TestPrepareLogicBlockIgnoresLocalVariables(t *testing.T) {
	dsl, err := newDSLEngine(config.DSLConfig{})
	if err != nil {
		t.Fatalf("newDSLEngine: %v", err)
	}
	cells, err := newCellStore([]config.CellConfig{
		{ID: "return_underfloor_heating_temperature", Type: config.ValueKindNumber},
		{ID: "raw_flow_sensor_underfloor_heating", Type: config.ValueKindNumber},
		{ID: "target", Type: config.ValueKindNumber},
	})
	if err != nil {
		t.Fatalf("newCellStore: %v", err)
	}

	expression := `
let temperature = return_underfloor_heating_temperature;
let flow_sensor_current = value("raw_flow_sensor_underfloor_heating") / 1000 - 4;
success(flow_sensor_current - (0.8 * 1) + 0.8)
`

	cfg := config.LogicBlockConfig{
		ID:     "flow_sensor_underfloor_heating",
		Target: "target",
		Normal: expression,
	}

	block, _, err := prepareLogicBlock(cfg, cells, dsl, 0)
	if err != nil {
		t.Fatalf("prepareLogicBlock: %v", err)
	}

	want := []string{"raw_flow_sensor_underfloor_heating", "return_underfloor_heating_temperature"}
	if got := block.normalDependencyIDs; !equalStringSlices(got, want) {
		t.Fatalf("normal dependencies = %v, want %v", got, want)
	}
}

func TestPrepareLogicBlockMissingFallbackDependency(t *testing.T) {
	dsl, err := newDSLEngine(config.DSLConfig{})
	if err != nil {
		t.Fatalf("newDSLEngine: %v", err)
	}
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

	_, _, err = prepareLogicBlock(cfg, cells, dsl, 0)
	if err == nil {
		t.Fatalf("expected error for missing fallback dependency")
	}
	if !strings.Contains(err.Error(), "fallback dependency") {
		t.Fatalf("expected fallback dependency error, got %v", err)
	}
}

func TestHelperFunctionUsage(t *testing.T) {
	allowIf := true
	dsl, err := newDSLEngine(config.DSLConfig{
		AllowIfBlocks: &allowIf,
		Helpers: []config.HelperFunctionConfig{
			{
				Name:      "water_viscosity",
				Arguments: []string{"temperature"},
				Expression: `if temperature < 0 {
        fail("helper.temperature", "below freezing")
} else if temperature > 100 {
        fail("helper.temperature", "too hot")
} else {
        temperature * 0.001
}`,
			},
		},
	})
	if err != nil {
		t.Fatalf("newDSLEngine: %v", err)
	}

	cells, err := newCellStore([]config.CellConfig{
		{ID: "temperature", Type: config.ValueKindNumber},
		{ID: "result", Type: config.ValueKindNumber},
	})
	if err != nil {
		t.Fatalf("newCellStore: %v", err)
	}

	cfg := config.LogicBlockConfig{
		ID:       "viscosity",
		Target:   "result",
		Normal:   "success(water_viscosity(value(\"temperature\")))",
		Fallback: "success(0)",
	}

	block, _, err := prepareLogicBlock(cfg, cells, dsl, 0)
	if err != nil {
		t.Fatalf("prepareLogicBlock: %v", err)
	}

	if got := block.normalDependencyIDs; len(got) != 1 || got[0] != "temperature" {
		t.Fatalf("unexpected normal dependencies: %v", got)
	}

	now := time.Now()
	temp, err := cells.mustGet("temperature")
	if err != nil {
		t.Fatalf("mustGet temperature: %v", err)
	}
	if err := temp.setValue(float64(20), now); err != nil {
		t.Fatalf("setValue: %v", err)
	}
	snapshot := cells.snapshot()
	result := block.runProgram(now, snapshot, block.normal, false)
	if !result.success {
		t.Fatalf("expected success, got %+v", result)
	}
	if got, ok := result.value.(float64); !ok || got != 0.02 {
		t.Fatalf("unexpected helper result: %v (%T)", result.value, result.value)
	}

	if err := temp.setValue(float64(-5), now); err != nil {
		t.Fatalf("setValue: %v", err)
	}
	snapshot = cells.snapshot()
	failure := block.runProgram(now, snapshot, block.normal, false)
	if failure.success {
		t.Fatalf("expected failure result, got success")
	}
	if failure.diag == nil || failure.diag.Code != "helper.temperature" {
		t.Fatalf("expected helper failure, got %+v", failure.diag)
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
