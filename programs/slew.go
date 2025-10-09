package programs

import "github.com/timzifer/modbus_processor/internal/config"

type slewProgram struct {
	id       string
	rise     float64
	fall     float64
	value    float64
	hasValue bool
}

func newSlewProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	rate, err := getFloat(settings, "rate", 0)
	if err != nil {
		return nil, err
	}
	initValue, err := getFloat(settings, "initial", 0)
	if err != nil {
		return nil, err
	}
	prog := &slewProgram{id: instanceID, rise: rate, fall: rate}
	if settings != nil && settings["initial"] != nil {
		prog.value = initValue
		prog.hasValue = true
	}
	return prog, nil
}

func newAsymSlewProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	rise, err := getFloat(settings, "rise_rate", 0)
	if err != nil {
		return nil, err
	}
	fall, err := getFloat(settings, "fall_rate", 0)
	if err != nil {
		return nil, err
	}
	initValue, err := getFloat(settings, "initial", 0)
	if err != nil {
		return nil, err
	}
	prog := &slewProgram{id: instanceID, rise: rise, fall: fall}
	if settings != nil && settings["initial"] != nil {
		prog.value = initValue
		prog.hasValue = true
	}
	return prog, nil
}

func (s *slewProgram) ID() string { return s.id }

func (s *slewProgram) Execute(ctx Context, incoming []Signal) ([]Signal, error) {
	signals := MapSignals(incoming)
	inputSig, ok := signals["input"]
	if !ok {
		inputSig, ok = signals["target"]
	}
	if !ok {
		return []Signal{InvalidSignal("output", config.ValueKindNumber)}, nil
	}
	input, okVal := inputSig.AsNumber()
	if !okVal {
		return []Signal{InvalidSignal("output", config.ValueKindNumber)}, nil
	}
	if !s.hasValue {
		s.value = input
		s.hasValue = true
	}
	dt := ctx.Delta.Seconds()
	if dt <= 0 {
		dt = 1
	}
	riseStep := s.rise * dt
	fallStep := s.fall * dt
	delta := input - s.value
	if delta > riseStep {
		delta = riseStep
	}
	if delta < -fallStep {
		delta = -fallStep
	}
	s.value += delta
	return []Signal{NewNumberSignal("output", s.value)}, nil
}

func init() {
	RegisterProgram("slew", newSlewProgram)
	RegisterProgram("slew_asym", newAsymSlewProgram)
}
