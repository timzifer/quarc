package programs

import (
	"github.com/timzifer/quarc/config"
)

type rampProgram struct {
	id        string
	rate      float64
	initial   float64
	hasInit   bool
	value     float64
	hasValue  bool
	allowHold bool
}

func newRampProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	rate, err := getFloat(settings, "rate", 0)
	if err != nil {
		return nil, err
	}
	initValue, err := getFloat(settings, "initial", 0)
	if err != nil {
		return nil, err
	}
	hold, err := getBoolSetting(settings, "allow_hold", false)
	if err != nil {
		return nil, err
	}
	prog := &rampProgram{id: instanceID, rate: rate, initial: initValue, hasInit: settings != nil && settings["initial"] != nil, allowHold: hold}
	return prog, nil
}

func (r *rampProgram) ID() string { return r.id }

func (r *rampProgram) Execute(ctx Context, incoming []Signal) ([]Signal, error) {
	signals := MapSignals(incoming)
	targetSig, ok := signals["target"]
	if !ok {
		targetSig, ok = signals["setpoint"]
	}
	if !ok {
		return []Signal{InvalidSignal("value", config.ValueKindNumber)}, nil
	}
	target, okTarget := targetSig.AsNumber()
	if !okTarget {
		return []Signal{InvalidSignal("value", config.ValueKindNumber)}, nil
	}
	if !r.hasValue {
		if r.hasInit {
			r.value = r.initial
		} else {
			r.value = target
		}
		r.hasValue = true
	}
	if holdSig, ok := signals["hold"]; ok && r.allowHold {
		if hold, okHold := holdSig.AsBool(); okHold && hold {
			return []Signal{NewNumberSignal("value", r.value)}, nil
		}
	}
	if resetSig, ok := signals["reset"]; ok {
		if reset, okReset := resetSig.AsBool(); okReset && reset {
			r.value = target
			return []Signal{NewNumberSignal("value", r.value)}, nil
		}
	}
	if r.rate <= 0 {
		r.value = target
		return []Signal{NewNumberSignal("value", r.value)}, nil
	}
	dt := ctx.Delta.Seconds()
	if dt <= 0 {
		dt = 1
	}
	maxStep := r.rate * dt
	delta := target - r.value
	if delta > maxStep {
		delta = maxStep
	} else if delta < -maxStep {
		delta = -maxStep
	}
	r.value += delta
	return []Signal{NewNumberSignal("value", r.value)}, nil
}

func init() {
	RegisterProgram("ramp", newRampProgram)
}
