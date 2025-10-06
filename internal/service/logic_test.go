package service

import (
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"modbus_processor/internal/config"
)

func TestPrepareLogicBlockAutoDependencies(t *testing.T) {
	dsl, err := newDSLEngine(config.DSLConfig{}, nil, zerolog.Nop())
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
		ID:         "example",
		Target:     "target",
		Expression: "value(\"a\") + value(\"b\")",
		Valid:      "valid(\"a\") && valid(\"b\")",
		Quality:    "valid(\"a\") && valid(\"b\") ? 0.75 : 0",
	}

	block, meta, err := prepareLogicBlock(cfg, cells, dsl, 0)
	if err != nil {
		t.Fatalf("prepareLogicBlock: %v", err)
	}
	if block == nil {
		t.Fatalf("block must not be nil")
	}

	if got, want := block.expressionDependencyIDs, []string{"a", "b"}; !equalStringSlices(got, want) {
		t.Fatalf("expression dependencies = %v, want %v", got, want)
	}
	if got, want := block.validDependencyIDs, []string{"a", "b"}; !equalStringSlices(got, want) {
		t.Fatalf("valid dependencies = %v, want %v", got, want)
	}
	if got, want := block.qualityDependencyIDs, []string{"a", "b"}; !equalStringSlices(got, want) {
		t.Fatalf("quality dependencies = %v, want %v", got, want)
	}

	depA := meta["a"]
	if depA == nil || !depA.expression || !depA.valid || !depA.quality {
		t.Fatalf("dependency metadata for 'a' missing flags: %+v", depA)
	}
	depB := meta["b"]
	if depB == nil || !depB.expression || !depB.valid || !depB.quality {
		t.Fatalf("dependency metadata for 'b' missing flags: %+v", depB)
	}
}

func TestPrepareLogicBlockIgnoresLocalVariables(t *testing.T) {
	dsl, err := newDSLEngine(config.DSLConfig{}, nil, zerolog.Nop())
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
flow_sensor_current - (0.8 * 1) + 0.8
`

	cfg := config.LogicBlockConfig{
		ID:         "flow_sensor_underfloor_heating",
		Target:     "target",
		Expression: expression,
	}

	block, _, err := prepareLogicBlock(cfg, cells, dsl, 0)
	if err != nil {
		t.Fatalf("prepareLogicBlock: %v", err)
	}

	want := []string{"raw_flow_sensor_underfloor_heating", "return_underfloor_heating_temperature"}
	if got := block.expressionDependencyIDs; !equalStringSlices(got, want) {
		t.Fatalf("expression dependencies = %v, want %v", got, want)
	}
}

func TestPrepareLogicBlockMissingValidateDependency(t *testing.T) {
	dsl, err := newDSLEngine(config.DSLConfig{}, nil, zerolog.Nop())
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
		ID:         "example",
		Target:     "target",
		Expression: "value(\"input\")",
		Valid:      "cell(\"missing\")",
	}

	_, _, err = prepareLogicBlock(cfg, cells, dsl, 0)
	if err == nil {
		t.Fatalf("expected error for missing validate dependency")
	}
	if !strings.Contains(err.Error(), "valid dependency") {
		t.Fatalf("expected valid dependency error, got %v", err)
	}
}

func TestHelperFunctionUsage(t *testing.T) {
	dsl, err := newDSLEngine(config.DSLConfig{}, []config.HelperFunctionConfig{
		{
			Name:       "water_viscosity",
			Arguments:  []string{"temperature"},
			Expression: `temperature <= 0 ? 0 : temperature * 0.001`,
		},
	}, zerolog.Nop())
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
		ID:         "viscosity",
		Target:     "result",
		Expression: "water_viscosity(value(\"temperature\"))",
		Valid:      "valid(\"temperature\")",
		Quality:    "valid(\"temperature\") ? 0.9 : 0",
	}

	block, _, err := prepareLogicBlock(cfg, cells, dsl, 0)
	if err != nil {
		t.Fatalf("prepareLogicBlock: %v", err)
	}

	if got := block.expressionDependencyIDs; len(got) != 1 || got[0] != "temperature" {
		t.Fatalf("unexpected expression dependencies: %v", got)
	}

	now := time.Now()
	temp, err := cells.mustGet("temperature")
	if err != nil {
		t.Fatalf("mustGet temperature: %v", err)
	}
	if err := temp.setValue(float64(20), now, nil); err != nil {
		t.Fatalf("setValue: %v", err)
	}
	snapshot := cells.snapshot()
	result, exprErr := block.runExpression(snapshot)
	if exprErr != nil {
		t.Fatalf("runExpression: %v", exprErr)
	}
	if got, ok := result.(float64); !ok || got != 0.02 {
		t.Fatalf("unexpected helper result: %v (%T)", result, result)
	}

	decision, valErr := block.runValidation(snapshot, result, nil)
	if valErr != nil {
		t.Fatalf("runValidation: %v", valErr)
	}
	if !decision.valid {
		t.Fatalf("expected validator to mark result valid: %+v", decision)
	}
	if decision.quality == nil || *decision.quality != 0.9 {
		t.Fatalf("unexpected quality: %v", decision.quality)
	}

	// Make the dependency invalid to trigger an evaluation error and ensure the validator sees it.
	temp.markInvalid(now, "test.invalid", "forced")
	snapshot = cells.snapshot()
	_, exprErr = block.runExpression(snapshot)
	if exprErr == nil {
		t.Fatalf("expected expression error when dependency invalid")
	}
	decision, valErr = block.runValidation(snapshot, nil, exprErr)
	if valErr != nil {
		t.Fatalf("runValidation with error: %v", valErr)
	}
	if decision.valid {
		t.Fatalf("expected invalid decision when expression errored")
	}
}

func TestValidationExpressionValueFunctionAlias(t *testing.T) {
	dsl, err := newDSLEngine(config.DSLConfig{}, nil, zerolog.Nop())
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
		ID:         "alias",
		Target:     "target",
		Expression: `value("input")`,
		Valid:      `let t = value("input"); t > 0`,
	}

	block, _, err := prepareLogicBlock(cfg, cells, dsl, 0)
	if err != nil {
		t.Fatalf("prepareLogicBlock: %v", err)
	}

	now := time.Now()
	input, err := cells.mustGet("input")
	if err != nil {
		t.Fatalf("mustGet: %v", err)
	}
	if err := input.setValue(float64(10), now, nil); err != nil {
		t.Fatalf("setValue: %v", err)
	}

	snapshot := cells.snapshot()
	result, exprErr := block.runExpression(snapshot)
	if exprErr != nil {
		t.Fatalf("runExpression: %v", exprErr)
	}
	if got, ok := result.(float64); !ok || got != 10 {
		t.Fatalf("unexpected expression result: %v", result)
	}

	decision, valErr := block.runValidation(snapshot, result, nil)
	if valErr != nil {
		t.Fatalf("runValidation: %v", valErr)
	}
	if !decision.valid {
		t.Fatalf("expected validator to accept value: %+v", decision)
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
