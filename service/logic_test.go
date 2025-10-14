package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/timzifer/quarc/config"
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

	block, meta, err := prepareLogicBlock(cfg, cells, dsl, 0, nil)
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

	block, _, err := prepareLogicBlock(cfg, cells, dsl, 0, nil)
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

	_, _, err = prepareLogicBlock(cfg, cells, dsl, 0, nil)
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

	block, _, err := prepareLogicBlock(cfg, cells, dsl, 0, nil)
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

func TestParseDSLLogLevel(t *testing.T) {
	cases := []struct {
		name      string
		input     interface{}
		wantLevel zerolog.Level
		ok        bool
	}{
		{"trace", "trace", zerolog.TraceLevel, true},
		{"debug", "DEBUG", zerolog.DebugLevel, true},
		{"info", "info", zerolog.InfoLevel, true},
		{"warn", "Warn", zerolog.WarnLevel, true},
		{"warning", "warning", zerolog.WarnLevel, true},
		{"error", "error", zerolog.ErrorLevel, true},
		{"err", "err", zerolog.ErrorLevel, true},
		{"unknown", "unknown", zerolog.InfoLevel, false},
		{"non-string", 123, zerolog.InfoLevel, false},
		{"slice", []string{"debug"}, zerolog.InfoLevel, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			level, ok := parseDSLLogLevel(tc.input)
			if level != tc.wantLevel || ok != tc.ok {
				t.Fatalf("parseDSLLogLevel(%v) = (%v, %v), want (%v, %v)", tc.input, level, ok, tc.wantLevel, tc.ok)
			}
		})
	}
}

func TestDSLContextLogLevels(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf).Level(zerolog.TraceLevel)
	ctx := &dslContext{
		logger:         &logger,
		originKind:     "block",
		originID:       "heater",
		expressionKind: "expression",
		expression:     "value(\"input\")",
	}

	ctx.log("debug", "heating", 42)
	ctx.log("WARN", "overheat")
	ctx.log("custom message", true)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 log entries, got %d", len(lines))
	}

	decode := func(t *testing.T, line string) map[string]interface{} {
		t.Helper()
		entry := make(map[string]interface{})
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("decode log entry: %v", err)
		}
		return entry
	}

	first := decode(t, lines[0])
	if got := first["level"]; got != "debug" {
		t.Fatalf("expected debug level, got %v", got)
	}
	if got := first["message"]; got != "heating" {
		t.Fatalf("unexpected first message: %v", got)
	}
	if got := first["value"]; got != float64(42) { // JSON numbers decode as float64
		t.Fatalf("unexpected logged value: %v", got)
	}

	second := decode(t, lines[1])
	if got := second["level"]; got != "warn" {
		t.Fatalf("expected warn level, got %v", got)
	}
	if got := second["message"]; got != "overheat" {
		t.Fatalf("unexpected second message: %v", got)
	}

	third := decode(t, lines[2])
	if got := third["level"]; got != "info" {
		t.Fatalf("expected info level fallback, got %v", got)
	}
	if got := third["message"]; got != "custom message" {
		t.Fatalf("unexpected third message: %v", got)
	}
	if value, ok := third["value"]; !ok || value.(bool) != true {
		t.Fatalf("expected bool value in third entry, got %v", value)
	}

	for idx, entry := range []map[string]interface{}{first, second, third} {
		if entry["origin_kind"] != "block" {
			t.Fatalf("entry %d missing origin_kind decoration: %v", idx, entry)
		}
		if entry["origin_id"] != "heater" {
			t.Fatalf("entry %d missing origin_id decoration: %v", idx, entry)
		}
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

	block, _, err := prepareLogicBlock(cfg, cells, dsl, 0, nil)
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

func TestNormalizeEvaluationError(t *testing.T) {
	baseErr := evaluationError{Code: "logic.custom", Message: "details"}

	tests := []struct {
		name string
		err  error
		want error
	}{
		{name: "nil", err: nil, want: nil},
		{name: "evaluation_error", err: baseErr, want: baseErr},
		{name: "wrapped_evaluation_error", err: fmt.Errorf("wrap: %w", baseErr), want: baseErr},
		{name: "generic_error", err: errors.New("boom"), want: evaluationError{Code: "logic.runtime_error", Message: "boom"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeEvaluationError(tc.err)
			if tc.want == nil {
				if got != nil {
					t.Fatalf("normalizeEvaluationError(%v) = %v, want nil", tc.err, got)
				}
				return
			}

			var eval evaluationError
			if !errors.As(tc.want, &eval) {
				eval = tc.want.(evaluationError)
			}

			gotEval, ok := got.(evaluationError)
			if !ok {
				t.Fatalf("normalizeEvaluationError(%v) = %T, want evaluationError", tc.err, got)
			}
			if gotEval.Code != eval.Code || gotEval.Message != eval.Message {
				t.Fatalf("normalizeEvaluationError(%v) = %+v, want %+v", tc.err, gotEval, eval)
			}
		})
	}
}

func TestDefaultValidation(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		res := defaultValidation(nil)
		if !res.valid {
			t.Fatalf("expected valid result")
		}
		if res.quality == nil || *res.quality != 1 {
			t.Fatalf("expected full quality, got %+v", res.quality)
		}
	})

	t.Run("evaluation_error", func(t *testing.T) {
		eval := evaluationError{Code: "logic.invalid", Message: "bad"}
		res := defaultValidation(eval)
		if res.valid || res.code != eval.Code || res.message != eval.Message {
			t.Fatalf("unexpected result: %+v", res)
		}
	})

	t.Run("generic_error", func(t *testing.T) {
		err := errors.New("boom")
		res := defaultValidation(err)
		if res.valid || res.code != "logic.error" || res.message != err.Error() {
			t.Fatalf("unexpected result: %+v", res)
		}
	})
}

func TestBuildErrorInfo(t *testing.T) {
	if info := buildErrorInfo(nil); info != nil {
		t.Fatalf("expected nil for nil error, got %v", info)
	}

	eval := evaluationError{Code: "logic.bad", Message: "fail"}
	info := buildErrorInfo(eval)
	if info["message"] != eval.Message {
		t.Fatalf("expected message %q, got %v", eval.Message, info["message"])
	}
	if info["code"] != eval.Code {
		t.Fatalf("expected code %q, got %v", eval.Code, info["code"])
	}

	noCode := evaluationError{Message: "just message"}
	info = buildErrorInfo(noCode)
	if _, ok := info["code"]; ok {
		t.Fatalf("did not expect code in %+v", info)
	}

	generic := errors.New("boom")
	info = buildErrorInfo(generic)
	if info["message"] != generic.Error() {
		t.Fatalf("expected message %q, got %v", generic.Error(), info["message"])
	}
	if _, ok := info["code"]; ok {
		t.Fatalf("did not expect code for generic error")
	}
}

func TestApplyValidationOutcome(t *testing.T) {
	base := validationResult{valid: false}

	res := applyValidationOutcome(base, true)
	if !res.valid {
		t.Fatalf("expected valid=true, got %+v", res)
	}

	res = applyValidationOutcome(base, "no")
	if res.valid {
		t.Fatalf("expected valid=false for \"no\", got %+v", res)
	}

	unchanged := applyValidationOutcome(base, 123)
	if unchanged.valid != base.valid {
		t.Fatalf("expected unchanged result, got %+v", unchanged)
	}

	if got := applyValidationOutcome(base, nil); got != base {
		t.Fatalf("expected base unchanged for nil outcome")
	}
}

func TestApplyValidationMap(t *testing.T) {
	base := validationResult{valid: true, quality: floatPtr(0.5)}

	values := map[string]interface{}{
		"valid":   "false",
		"quality": "0.75",
		"code":    42,
		"message": "problem",
		"ignored": "value",
	}

	res := applyValidationMap(base, values)
	if res.valid {
		t.Fatalf("expected valid=false after map, got %+v", res)
	}
	if res.quality == nil || *res.quality != 0.75 {
		t.Fatalf("expected quality=0.75, got %+v", res.quality)
	}
	if res.code != "42" {
		t.Fatalf("expected code \"42\", got %q", res.code)
	}
	if res.message != "problem" {
		t.Fatalf("expected message \"problem\", got %q", res.message)
	}

	invalid := applyValidationMap(base, map[string]interface{}{"quality": "not a number"})
	if invalid.quality == nil || *invalid.quality != *base.quality {
		t.Fatalf("expected quality unchanged, got %+v", invalid.quality)
	}
}

func TestToBool(t *testing.T) {
	cases := []struct {
		name  string
		input interface{}
		want  bool
		ok    bool
	}{
		{name: "bool_true", input: true, want: true, ok: true},
		{name: "bool_false", input: false, want: false, ok: true},
		{name: "string_yes", input: "Yes", want: true, ok: true},
		{name: "string_zero", input: "0", want: false, ok: true},
		{name: "unknown", input: 1, want: false, ok: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := toBool(tc.input)
			if got != tc.want || ok != tc.ok {
				t.Fatalf("toBool(%v) = (%v, %v), want (%v, %v)", tc.input, got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestToFloat(t *testing.T) {
	cases := []struct {
		name  string
		input interface{}
		want  float64
		ok    bool
	}{
		{name: "float64", input: 1.25, want: 1.25, ok: true},
		{name: "int", input: int32(3), want: 3, ok: true},
		{name: "uint", input: uint16(7), want: 7, ok: true},
		{name: "string", input: " 2.5 ", want: 2.5, ok: true},
		{name: "empty_string", input: " ", want: 0, ok: false},
		{name: "invalid", input: struct{}{}, want: 0, ok: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := toFloat(tc.input)
			if math.Abs(got-tc.want) > 1e-9 || ok != tc.ok {
				t.Fatalf("toFloat(%v) = (%v, %v), want (%v, %v)", tc.input, got, ok, tc.want, tc.ok)
			}
		})
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
