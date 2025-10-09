package programs

import "github.com/timzifer/modbus_processor/internal/config"

type flipFlopMode int

const (
	modeSR flipFlopMode = iota
	modeRS
	modeToggle
)

type flipFlopProgram struct {
	id         string
	mode       flipFlopMode
	state      bool
	hasState   bool
	initial    bool
	hasInit    bool
	prevToggle bool
}

func newFlipFlopProgram(instanceID string, settings map[string]interface{}, mode flipFlopMode) (Program, error) {
	init, err := getBoolSetting(settings, "initial", false)
	if err != nil {
		return nil, err
	}
	prog := &flipFlopProgram{id: instanceID, mode: mode, initial: init}
	if settings != nil && settings["initial"] != nil {
		prog.state = init
		prog.hasState = true
		prog.hasInit = true
	}
	return prog, nil
}

func newSRProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	return newFlipFlopProgram(instanceID, settings, modeSR)
}

func newRSProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	return newFlipFlopProgram(instanceID, settings, modeRS)
}

func newToggleProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	return newFlipFlopProgram(instanceID, settings, modeToggle)
}

func (f *flipFlopProgram) ID() string { return f.id }

func (f *flipFlopProgram) Execute(ctx Context, incoming []Signal) ([]Signal, error) {
	signals := MapSignals(incoming)
	if resetSig, ok := signals["reset"]; ok {
		if reset, okBool := resetSig.AsBool(); okBool && reset {
			f.state = false
			f.hasState = true
			return []Signal{NewBoolSignal("q", f.state)}, nil
		}
	}
	switch f.mode {
	case modeSR:
		if setSig, ok := signals["set"]; ok {
			if set, okBool := setSig.AsBool(); okBool && set {
				f.state = true
				f.hasState = true
			}
		}
		if resetSig, ok := signals["reset"]; ok {
			if rst, okBool := resetSig.AsBool(); okBool && rst {
				f.state = false
				f.hasState = true
			}
		}
	case modeRS:
		if resetSig, ok := signals["reset"]; ok {
			if rst, okBool := resetSig.AsBool(); okBool && rst {
				f.state = false
				f.hasState = true
			}
		}
		if setSig, ok := signals["set"]; ok {
			if set, okBool := setSig.AsBool(); okBool && set {
				f.state = true
				f.hasState = true
			}
		}
	case modeToggle:
		toggleSig, ok := signals["toggle"]
		if !ok {
			return []Signal{InvalidSignal("q", config.ValueKindBool)}, nil
		}
		if toggle, okBool := toggleSig.AsBool(); okBool {
			if toggle && !f.prevToggle {
				f.state = !f.state
			}
			f.prevToggle = toggle
			f.hasState = true
		} else {
			return []Signal{InvalidSignal("q", config.ValueKindBool)}, nil
		}
	}
	if !f.hasState {
		f.state = f.initial
		f.hasState = true
	}
	return []Signal{NewBoolSignal("q", f.state)}, nil
}

func init() {
	RegisterProgram("sr", newSRProgram)
	RegisterProgram("rs", newRSProgram)
	RegisterProgram("toggle", newToggleProgram)
}
