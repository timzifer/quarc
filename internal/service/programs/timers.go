package programs

import "modbus_processor/internal/config"

type timerMode int

const (
	tonMode timerMode = iota
	tofMode
	tpMode
)

type timerProgram struct {
	id        string
	mode      timerMode
	preset    float64
	elapsed   float64
	state     bool
	inputPrev bool
	hasPrev   bool
}

func newTimerProgram(instanceID string, settings map[string]interface{}, mode timerMode) (Program, error) {
	presetDur, err := getDurationSetting(settings, "preset", 0)
	if err != nil {
		return nil, err
	}
	presetSeconds := presetDur.Seconds()
	if presetSeconds < 0 {
		presetSeconds = 0
	}
	return &timerProgram{id: instanceID, mode: mode, preset: presetSeconds}, nil
}

func newTONProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	return newTimerProgram(instanceID, settings, tonMode)
}

func newTOFProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	return newTimerProgram(instanceID, settings, tofMode)
}

func newTPProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	return newTimerProgram(instanceID, settings, tpMode)
}

func (t *timerProgram) ID() string { return t.id }

func (t *timerProgram) Execute(ctx Context, incoming []Signal) ([]Signal, error) {
	signals := MapSignals(incoming)
	inSig, ok := signals["in"]
	if !ok {
		inSig, ok = signals["input"]
	}
	if !ok {
		return []Signal{InvalidSignal("q", config.ValueKindBool), InvalidSignal("elapsed", config.ValueKindNumber)}, nil
	}
	input, okInput := inSig.AsBool()
	if !okInput {
		return []Signal{InvalidSignal("q", config.ValueKindBool), InvalidSignal("elapsed", config.ValueKindNumber)}, nil
	}
	if resetSig, ok := signals["reset"]; ok {
		if reset, okReset := resetSig.AsBool(); okReset && reset {
			t.elapsed = 0
			t.state = false
			t.hasPrev = false
			return []Signal{NewBoolSignal("q", false), NewNumberSignal("elapsed", 0)}, nil
		}
	}
	dt := ctx.Delta.Seconds()
	if dt <= 0 {
		dt = 0
	}
	switch t.mode {
	case tonMode:
		if input {
			t.elapsed += dt
			if t.elapsed >= t.preset {
				t.state = true
			}
		} else {
			t.elapsed = 0
			t.state = false
		}
	case tofMode:
		if input {
			t.state = true
			t.elapsed = 0
		} else if t.state {
			t.elapsed += dt
			if t.elapsed >= t.preset {
				t.state = false
				t.elapsed = t.preset
			}
		} else {
			t.elapsed = 0
		}
	case tpMode:
		rising := !t.hasPrev || (!t.inputPrev && input)
		t.inputPrev = input
		t.hasPrev = true
		if rising {
			t.state = true
			t.elapsed = 0
		}
		if t.state {
			t.elapsed += dt
			if t.elapsed >= t.preset {
				t.state = false
				t.elapsed = t.preset
			}
		} else {
			t.elapsed = 0
		}
	}
	t.inputPrev = input
	t.hasPrev = true
	return []Signal{NewBoolSignal("q", t.state), NewNumberSignal("elapsed", t.elapsed)}, nil
}

func init() {
	RegisterProgram("ton", newTONProgram)
	RegisterProgram("tof", newTOFProgram)
	RegisterProgram("tp", newTPProgram)
}
