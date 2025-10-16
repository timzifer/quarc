package programs

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/timzifer/quarc/config"
)

const rampOverlayBaseSections = `
    logging: {
        level: "info"
        loki: {
            enabled: false
            url: ""
            labels: {}
        }
    }
    telemetry: {
        enabled: false
    }
    workers: {}
    helpers: []
    dsl: {}
    live_view: {}
    policies: {}
    server: {
        enabled: false
        listen: ""
        cells: []
    }
    hot_reload: false
`

func mustExecute(t *testing.T, prog Program, ctx Context, signals []Signal) []Signal {
	t.Helper()
	out, err := prog.Execute(ctx, signals)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	return out
}

func TestRampOverlayIntegration(t *testing.T) {
	config.ResetOverlaysForTest()
	t.Cleanup(config.ResetOverlaysForTest)

	dir := t.TempDir()
	path := filepath.Join(dir, "ramp_module.cue")
	content := `package overlaytest

import (
    module "quarc.dev/quarc/module"
    ramp "quarc.dev/quarc/programs/ramp"
)

instance: module.#Module & ramp.#Ramp & {
    config: {
        package: "overlay.test"
        cycle: "1s"
` + rampOverlayBaseSections + `
        reads: []
        writes: []
        logic: []
        cells: [
            { id: "target", type: "number" },
            { id: "output", type: "number" },
        ]
        programs: [{
            id: "ramp1"
            type: "ramp"
            inputs: [{
                id: "target"
                cell: "target"
            }]
            outputs: [{
                id: "value"
                cell: "output"
            }]
            settings: {
                rate: 1.5
                initial: 0.5
                allow_hold: true
            }
        }]
    }
}

config: instance.config
`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write overlay config: %v", err)
	}

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load overlay config: %v", err)
	}

	if len(cfg.Cells) != 2 {
		t.Fatalf("expected 2 cells, got %d", len(cfg.Cells))
	}

	seenCells := map[string]bool{
		"overlay.test:target": false,
		"overlay.test:output": false,
	}
	for _, cell := range cfg.Cells {
		if _, ok := seenCells[cell.ID]; ok {
			seenCells[cell.ID] = true
		}
	}
	for id, seen := range seenCells {
		if !seen {
			t.Fatalf("expected cell %s to be present", id)
		}
	}

	if len(cfg.Programs) != 1 {
		t.Fatalf("expected 1 program, got %d", len(cfg.Programs))
	}
	prog := cfg.Programs[0]
	if prog.Type != "ramp" {
		t.Fatalf("expected program type ramp, got %s", prog.Type)
	}
	if len(prog.Outputs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(prog.Outputs))
	}
	if prog.Outputs[0].Cell != "overlay.test:output" {
		t.Fatalf("expected output cell overlay.test:output, got %s", prog.Outputs[0].Cell)
	}
	if len(prog.Inputs) != 1 {
		t.Fatalf("expected 1 input, got %d", len(prog.Inputs))
	}
	if prog.Inputs[0].Cell != "overlay.test:target" {
		t.Fatalf("expected input cell overlay.test:target, got %s", prog.Inputs[0].Cell)
	}

	if rate, ok := prog.Settings["rate"].(float64); !ok || rate != 1.5 {
		t.Fatalf("expected rate 1.5, got %v", prog.Settings["rate"])
	}
	if initial, ok := prog.Settings["initial"].(float64); !ok || initial != 0.5 {
		t.Fatalf("expected initial 0.5, got %v", prog.Settings["initial"])
	}
	if allowHold, ok := prog.Settings["allow_hold"].(bool); !ok || !allowHold {
		t.Fatalf("expected allow_hold true, got %v", prog.Settings["allow_hold"])
	}
}

func TestPIDProgram(t *testing.T) {
	prog, err := Instantiate("pid", "pid1", map[string]interface{}{"kp": 2.0, "ki": 1.0})
	if err != nil {
		t.Fatalf("instantiate pid: %v", err)
	}
	ctx := Context{Now: time.Now(), Delta: time.Second}
	inputs := []Signal{NewNumberSignal("setpoint", 10), NewNumberSignal("process", 8)}
	outputs := mustExecute(t, prog, ctx, inputs)
	if len(outputs) != 1 {
		t.Fatalf("expected single output, got %d", len(outputs))
	}
	if outputs[0].ID != "output" || !outputs[0].Valid {
		t.Fatalf("unexpected output: %+v", outputs[0])
	}
	if outputs[0].Value.(float64) <= 0 {
		t.Fatalf("expected positive control value, got %v", outputs[0].Value)
	}
}

func TestRampProgram(t *testing.T) {
	prog, err := Instantiate("ramp", "r1", map[string]interface{}{"rate": 1.0})
	if err != nil {
		t.Fatalf("instantiate ramp: %v", err)
	}
	ctx := Context{Now: time.Now(), Delta: time.Second}
	inputs := []Signal{NewNumberSignal("target", 5)}
	outputs := mustExecute(t, prog, ctx, inputs)
	if outputs[0].Value.(float64) != 5 {
		t.Fatalf("expected immediate value 5, got %v", outputs[0].Value)
	}
	ctx.Delta = 500 * time.Millisecond
	inputs[0].Value = 7.0
	outputs = mustExecute(t, prog, ctx, inputs)
	if outputs[0].Value.(float64) >= 7 {
		t.Fatalf("expected ramp limited output, got %v", outputs[0].Value)
	}
}

func TestSlewPrograms(t *testing.T) {
	sym, err := Instantiate("slew", "s1", map[string]interface{}{"rate": 2.0})
	if err != nil {
		t.Fatalf("instantiate slew: %v", err)
	}
	asym, err := Instantiate("slew_asym", "s2", map[string]interface{}{"rise_rate": 2.0, "fall_rate": 1.0})
	if err != nil {
		t.Fatalf("instantiate asym: %v", err)
	}
	ctx := Context{Now: time.Now(), Delta: time.Second}
	mustExecute(t, sym, ctx, []Signal{NewNumberSignal("input", 0)})
	out := mustExecute(t, sym, ctx, []Signal{NewNumberSignal("input", 10)})
	if out[0].Value.(float64) > 2.0+1e-9 {
		t.Fatalf("slew exceeded rate: %v", out[0].Value)
	}
	mustExecute(t, asym, ctx, []Signal{NewNumberSignal("input", 10)})
	ctx.Delta = time.Second
	out = mustExecute(t, asym, ctx, []Signal{NewNumberSignal("input", 0)})
	if out[0].Value.(float64) < 8.9 {
		t.Fatalf("asym down rate expected slow change, got %v", out[0].Value)
	}
}

func TestTimerPrograms(t *testing.T) {
	ton, err := Instantiate("ton", "ton", map[string]interface{}{"preset": "1s"})
	if err != nil {
		t.Fatalf("ton instantiate: %v", err)
	}
	ctx := Context{Now: time.Now(), Delta: 500 * time.Millisecond}
	out := mustExecute(t, ton, ctx, []Signal{NewBoolSignal("in", true)})
	if out[0].Value.(bool) {
		t.Fatalf("TON should be false before preset")
	}
	ctx.Now = ctx.Now.Add(500 * time.Millisecond)
	out = mustExecute(t, ton, ctx, []Signal{NewBoolSignal("in", true)})
	if !out[0].Value.(bool) {
		t.Fatalf("TON should be true after preset")
	}

	tof, err := Instantiate("tof", "tof", map[string]interface{}{"preset": "1s"})
	if err != nil {
		t.Fatalf("tof instantiate: %v", err)
	}
	ctx = Context{Now: time.Now(), Delta: 500 * time.Millisecond}
	mustExecute(t, tof, ctx, []Signal{NewBoolSignal("in", true)})
	ctx.Now = ctx.Now.Add(500 * time.Millisecond)
	out = mustExecute(t, tof, ctx, []Signal{NewBoolSignal("in", false)})
	if !out[0].Value.(bool) {
		t.Fatalf("TOF should hold true during delay")
	}
	ctx.Now = ctx.Now.Add(500 * time.Millisecond)
	out = mustExecute(t, tof, ctx, []Signal{NewBoolSignal("in", false)})
	if out[0].Value.(bool) {
		t.Fatalf("TOF should be false after delay")
	}

	tp, err := Instantiate("tp", "tp", map[string]interface{}{"preset": "1s"})
	if err != nil {
		t.Fatalf("tp instantiate: %v", err)
	}
	ctx = Context{Now: time.Now(), Delta: 100 * time.Millisecond}
	out = mustExecute(t, tp, ctx, []Signal{NewBoolSignal("in", true)})
	if !out[0].Value.(bool) {
		t.Fatalf("TP should start pulse")
	}
	for i := 0; i < 10; i++ {
		ctx.Now = ctx.Now.Add(100 * time.Millisecond)
		out = mustExecute(t, tp, ctx, []Signal{NewBoolSignal("in", false)})
	}
	if out[0].Value.(bool) {
		t.Fatalf("TP should finish pulse")
	}
}

func TestCounterPrograms(t *testing.T) {
	ctu, err := Instantiate("ctu", "cu", map[string]interface{}{"preset": 2.0})
	if err != nil {
		t.Fatalf("ctu: %v", err)
	}
	ctx := Context{Now: time.Now(), Delta: time.Second}
	out := mustExecute(t, ctu, ctx, []Signal{NewBoolSignal("cu", true)})
	if out[0].Value.(float64) != 1 {
		t.Fatalf("expected count 1, got %v", out[0].Value)
	}
	mustExecute(t, ctu, ctx, []Signal{NewBoolSignal("cu", false)})
	out = mustExecute(t, ctu, ctx, []Signal{NewBoolSignal("cu", true)})
	if !out[1].Value.(bool) {
		t.Fatalf("expected done after reaching preset")
	}

	ctd, err := Instantiate("ctd", "cd", map[string]interface{}{"initial": 3.0, "preset": 0.0})
	if err != nil {
		t.Fatalf("ctd: %v", err)
	}
	mustExecute(t, ctd, ctx, []Signal{NewBoolSignal("cd", true)})
	mustExecute(t, ctd, ctx, []Signal{NewBoolSignal("cd", false)})
	out = mustExecute(t, ctd, ctx, []Signal{NewBoolSignal("cd", true)})
	if out[0].Value.(float64) >= 3 {
		t.Fatalf("expected counter to decrement")
	}
}

func TestHysteresisProgram(t *testing.T) {
	prog, err := Instantiate("hysteresis", "h1", map[string]interface{}{"low": 2.0, "high": 4.0})
	if err != nil {
		t.Fatalf("hysteresis: %v", err)
	}
	ctx := Context{Now: time.Now(), Delta: time.Second}
	out := mustExecute(t, prog, ctx, []Signal{NewNumberSignal("value", 5)})
	if !out[0].Value.(bool) {
		t.Fatalf("expected state true above high")
	}
	out = mustExecute(t, prog, ctx, []Signal{NewNumberSignal("value", 3)})
	if !out[0].Value.(bool) {
		t.Fatalf("state should stay true inside band")
	}
	out = mustExecute(t, prog, ctx, []Signal{NewNumberSignal("value", 1)})
	if out[0].Value.(bool) {
		t.Fatalf("state should reset below low")
	}
}

func TestFilterPrograms(t *testing.T) {
	lp, err := Instantiate("lowpass", "lp1", map[string]interface{}{"alpha": 0.5})
	if err != nil {
		t.Fatalf("lowpass: %v", err)
	}
	ctx := Context{Now: time.Now(), Delta: time.Second}
	mustExecute(t, lp, ctx, []Signal{NewNumberSignal("input", 0)})
	out := mustExecute(t, lp, ctx, []Signal{NewNumberSignal("input", 10)})
	if out[0].Value.(float64) != 5 {
		t.Fatalf("expected intermediate value 5, got %v", out[0].Value)
	}

	ma, err := Instantiate("moving_average", "ma", map[string]interface{}{"window": 3.0})
	if err != nil {
		t.Fatalf("moving average: %v", err)
	}
	ctx.Delta = time.Second
	mustExecute(t, ma, ctx, []Signal{NewNumberSignal("input", 1)})
	mustExecute(t, ma, ctx, []Signal{NewNumberSignal("input", 2)})
	out = mustExecute(t, ma, ctx, []Signal{NewNumberSignal("input", 3)})
	if out[0].Value.(float64) != 2 {
		t.Fatalf("expected average 2, got %v", out[0].Value)
	}

	median, err := Instantiate("median", "med", map[string]interface{}{"window": 3.0})
	if err != nil {
		t.Fatalf("median: %v", err)
	}
	mustExecute(t, median, ctx, []Signal{NewNumberSignal("input", 1)})
	mustExecute(t, median, ctx, []Signal{NewNumberSignal("input", 100)})
	out = mustExecute(t, median, ctx, []Signal{NewNumberSignal("input", 2)})
	if out[0].Value.(float64) != 2 {
		t.Fatalf("expected median 2, got %v", out[0].Value)
	}
}

func TestSelectionPrograms(t *testing.T) {
	maxProg, err := Instantiate("max", "max", map[string]interface{}{"inputs": []string{"a", "b"}})
	if err != nil {
		t.Fatalf("max: %v", err)
	}
	ctx := Context{Now: time.Now(), Delta: time.Second}
	out := mustExecute(t, maxProg, ctx, []Signal{NewNumberSignal("a", 1), NewNumberSignal("b", 5)})
	if out[0].Value.(float64) != 5 {
		t.Fatalf("max expected 5, got %v", out[0].Value)
	}

	priorityProg, err := Instantiate("priority", "prio", map[string]interface{}{"inputs": []string{"p1", "p2"}})
	if err != nil {
		t.Fatalf("priority: %v", err)
	}
	out = mustExecute(t, priorityProg, ctx, []Signal{InvalidSignal("p1", config.ValueKindNumber), NewNumberSignal("p2", 3)})
	if out[0].Value.(float64) != 3 {
		t.Fatalf("priority expected second input, got %v", out[0].Value)
	}

	muxProg, err := Instantiate("mux", "mux", map[string]interface{}{"inputs": []string{"in0", "in1"}})
	if err != nil {
		t.Fatalf("mux: %v", err)
	}
	out = mustExecute(t, muxProg, ctx, []Signal{NewNumberSignal("selector", 1), NewNumberSignal("in0", 2), NewNumberSignal("in1", 4)})
	if out[0].Value.(float64) != 4 {
		t.Fatalf("mux expected selected input, got %v", out[0].Value)
	}
}

func TestFlipFlopPrograms(t *testing.T) {
	sr, err := Instantiate("sr", "sr", nil)
	if err != nil {
		t.Fatalf("sr: %v", err)
	}
	ctx := Context{Now: time.Now(), Delta: time.Second}
	out := mustExecute(t, sr, ctx, []Signal{NewBoolSignal("set", true)})
	if !out[0].Value.(bool) {
		t.Fatalf("SR should set")
	}
	out = mustExecute(t, sr, ctx, []Signal{NewBoolSignal("reset", true)})
	if out[0].Value.(bool) {
		t.Fatalf("SR should reset")
	}

	toggle, err := Instantiate("toggle", "tog", nil)
	if err != nil {
		t.Fatalf("toggle: %v", err)
	}
	out = mustExecute(t, toggle, ctx, []Signal{NewBoolSignal("toggle", true)})
	if !out[0].Value.(bool) {
		t.Fatalf("toggle should flip on rising edge")
	}
	mustExecute(t, toggle, ctx, []Signal{NewBoolSignal("toggle", false)})
	out = mustExecute(t, toggle, ctx, []Signal{NewBoolSignal("toggle", true)})
	if out[0].Value.(bool) {
		t.Fatalf("toggle should flip back")
	}
}

func TestConsumptionPrograms(t *testing.T) {
	runtimeProg, err := Instantiate("operating_hours", "rt", nil)
	if err != nil {
		t.Fatalf("runtime: %v", err)
	}
	ctx := Context{Now: time.Now(), Delta: time.Hour}
	out := mustExecute(t, runtimeProg, ctx, []Signal{NewBoolSignal("run", true)})
	if math.Abs(out[0].Value.(float64)-1) > 1e-6 {
		t.Fatalf("runtime expected 1 hour, got %v", out[0].Value)
	}

	energyProg, err := Instantiate("energy_meter", "em", nil)
	if err != nil {
		t.Fatalf("energy: %v", err)
	}
	ctx.Delta = time.Hour
	out = mustExecute(t, energyProg, ctx, []Signal{NewNumberSignal("power", 2)})
	if math.Abs(out[0].Value.(float64)-2) > 1e-6 {
		t.Fatalf("energy expected 2 kWh, got %v", out[0].Value)
	}
}

func TestSequencerProgram(t *testing.T) {
	prog, err := Instantiate("sequencer", "seq", map[string]interface{}{"steps": []string{"s1", "s2"}})
	if err != nil {
		t.Fatalf("sequencer: %v", err)
	}
	ctx := Context{Now: time.Now(), Delta: time.Second}
	out := mustExecute(t, prog, ctx, nil)
	if out[0].Value.(string) != "s1" {
		t.Fatalf("expected first step, got %v", out[0].Value)
	}
	out = mustExecute(t, prog, ctx, []Signal{NewBoolSignal("advance", true)})
	if out[0].Value.(string) != "s2" {
		t.Fatalf("expected second step, got %v", out[0].Value)
	}
}

func TestAlarmProgram(t *testing.T) {
	prog, err := Instantiate("alarm", "al", map[string]interface{}{"high": 10.0})
	if err != nil {
		t.Fatalf("alarm: %v", err)
	}
	ctx := Context{Now: time.Now(), Delta: time.Second}
	out := mustExecute(t, prog, ctx, []Signal{NewNumberSignal("value", 5)})
	if out[0].Value.(bool) {
		t.Fatalf("alarm should be false below threshold")
	}
	out = mustExecute(t, prog, ctx, []Signal{NewNumberSignal("value", 12)})
	if !out[0].Value.(bool) {
		t.Fatalf("alarm should trigger above threshold")
	}
}
