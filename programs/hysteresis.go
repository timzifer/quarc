package programs

import (
	"github.com/timzifer/quarc/config"
)

type hysteresisProgram struct {
	id       string
	low      float64
	high     float64
	state    bool
	hasState bool
	initial  bool
	hasInit  bool
}

func newHysteresisProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	low, err := getFloat(settings, "low", 0)
	if err != nil {
		return nil, err
	}
	high, err := getFloat(settings, "high", 0)
	if err != nil {
		return nil, err
	}
	init, err := getBoolSetting(settings, "initial", false)
	if err != nil {
		return nil, err
	}
	prog := &hysteresisProgram{id: instanceID, low: low, high: high, initial: init}
	if settings != nil && settings["initial"] != nil {
		prog.state = init
		prog.hasState = true
		prog.hasInit = true
	}
	return prog, nil
}

func (h *hysteresisProgram) ID() string { return h.id }

func (h *hysteresisProgram) Execute(ctx Context, incoming []Signal) ([]Signal, error) {
	signals := MapSignals(incoming)
	valueSig, ok := signals["value"]
	if !ok {
		valueSig, ok = signals["input"]
	}
	if !ok {
		return []Signal{InvalidSignal("state", config.ValueKindBool)}, nil
	}
	value, okValue := valueSig.AsNumber()
	if !okValue {
		return []Signal{InvalidSignal("state", config.ValueKindBool)}, nil
	}
	if !h.hasState {
		if h.hasInit {
			h.state = h.initial
		} else {
			h.state = value >= h.high
		}
		h.hasState = true
	}
	if value >= h.high {
		h.state = true
	} else if value <= h.low {
		h.state = false
	}
	return []Signal{NewBoolSignal("state", h.state)}, nil
}

func init() {
	RegisterProgram("hysteresis", newHysteresisProgram)
}
