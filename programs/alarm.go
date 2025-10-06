package programs

import (
	"math"

	"modbus_processor/internal/config"
)

type alarmProgram struct {
	id      string
	high    float64
	low     float64
	useHigh bool
	useLow  bool
	latch   bool
	latched bool
}

func newAlarmProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	high, err := getFloat(settings, "high", math.NaN())
	if err != nil {
		return nil, err
	}
	low, err := getFloat(settings, "low", math.NaN())
	if err != nil {
		return nil, err
	}
	latch, err := getBoolSetting(settings, "latch", false)
	if err != nil {
		return nil, err
	}
	prog := &alarmProgram{id: instanceID, high: high, low: low, latch: latch}
	if !math.IsNaN(high) {
		prog.useHigh = true
	}
	if !math.IsNaN(low) {
		prog.useLow = true
	}
	return prog, nil
}

func (a *alarmProgram) ID() string { return a.id }

func (a *alarmProgram) Execute(ctx Context, incoming []Signal) ([]Signal, error) {
	signals := MapSignals(incoming)
	if resetSig, ok := signals["reset"]; ok {
		if reset, okBool := resetSig.AsBool(); okBool && reset {
			a.latched = false
		}
	}
	valueSig, ok := signals["value"]
	if !ok {
		return []Signal{InvalidSignal("alarm", config.ValueKindBool)}, nil
	}
	value, okVal := valueSig.AsNumber()
	if !okVal {
		return []Signal{InvalidSignal("alarm", config.ValueKindBool)}, nil
	}
	active := false
	if a.useHigh && value >= a.high {
		active = true
	}
	if a.useLow && value <= a.low {
		active = true
	}
	if a.latch {
		if active {
			a.latched = true
		}
		active = a.latched
	}
	return []Signal{NewBoolSignal("alarm", active)}, nil
}

func init() {
	RegisterProgram("alarm", newAlarmProgram)
}
