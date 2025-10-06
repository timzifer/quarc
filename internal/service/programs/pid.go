package programs

import (
	"math"

	"modbus_processor/internal/config"
)

type pidProgram struct {
	id           string
	kp           float64
	ki           float64
	kd           float64
	bias         float64
	min          float64
	max          float64
	useMin       bool
	useMax       bool
	integMin     float64
	integMax     float64
	limitInteg   bool
	integral     float64
	prevError    float64
	hasPrevError bool
}

func newPIDProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	kp, err := getFloat(settings, "kp", 0)
	if err != nil {
		return nil, err
	}
	ki, err := getFloat(settings, "ki", 0)
	if err != nil {
		return nil, err
	}
	kd, err := getFloat(settings, "kd", 0)
	if err != nil {
		return nil, err
	}
	bias, err := getFloat(settings, "bias", 0)
	if err != nil {
		return nil, err
	}
	min, err := getFloat(settings, "min", math.NaN())
	if err != nil {
		return nil, err
	}
	max, err := getFloat(settings, "max", math.NaN())
	if err != nil {
		return nil, err
	}
	integMin, err := getFloat(settings, "integrator_min", math.NaN())
	if err != nil {
		return nil, err
	}
	integMax, err := getFloat(settings, "integrator_max", math.NaN())
	if err != nil {
		return nil, err
	}
	prog := &pidProgram{
		id:   instanceID,
		kp:   kp,
		ki:   ki,
		kd:   kd,
		bias: bias,
	}
	if !math.IsNaN(min) {
		prog.min = min
		prog.useMin = true
	}
	if !math.IsNaN(max) {
		prog.max = max
		prog.useMax = true
	}
	if !math.IsNaN(integMin) {
		prog.integMin = integMin
		prog.limitInteg = true
	}
	if !math.IsNaN(integMax) {
		prog.integMax = integMax
		prog.limitInteg = true
	}
	return prog, nil
}

func (p *pidProgram) ID() string { return p.id }

func (p *pidProgram) Execute(ctx Context, incoming []Signal) ([]Signal, error) {
	signals := MapSignals(incoming)
	spSig, ok := signals["setpoint"]
	if !ok {
		spSig, ok = signals["sp"]
	}
	pvSig, okPV := signals["process"]
	if !okPV {
		pvSig, okPV = signals["measurement"]
	}
	if !ok || !okPV {
		return []Signal{InvalidSignal("output", config.ValueKindNumber)}, nil
	}
	sp, spOK := spSig.AsNumber()
	pv, pvOK := pvSig.AsNumber()
	if !spOK || !pvOK {
		return []Signal{InvalidSignal("output", config.ValueKindNumber)}, nil
	}
	dt := ctx.Delta.Seconds()
	if dt <= 0 {
		dt = 1
	}
	err := sp - pv
	pTerm := p.kp * err
	if p.ki != 0 {
		p.integral += p.ki * err * dt
		if p.limitInteg {
			if p.integral < p.integMin {
				p.integral = p.integMin
			}
			if p.integral > p.integMax {
				p.integral = p.integMax
			}
		}
	}
	dTerm := 0.0
	if p.kd != 0 && p.hasPrevError {
		dTerm = p.kd * (err - p.prevError) / dt
	}
	output := p.bias + pTerm + p.integral + dTerm
	if p.useMin && output < p.min {
		output = p.min
	}
	if p.useMax && output > p.max {
		output = p.max
	}
	p.prevError = err
	p.hasPrevError = true
	return []Signal{NewNumberSignal("output", output)}, nil
}

func init() {
	RegisterProgram("pid", newPIDProgram)
}
