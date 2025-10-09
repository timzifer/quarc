package programs

import "github.com/timzifer/modbus_processor/internal/config"

type runtimeProgram struct {
	id    string
	total float64
}

type energyProgram struct {
	id    string
	total float64
}

func newRuntimeProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	return &runtimeProgram{id: instanceID}, nil
}

func newEnergyProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	return &energyProgram{id: instanceID}, nil
}

func (r *runtimeProgram) ID() string { return r.id }

func (r *runtimeProgram) Execute(ctx Context, incoming []Signal) ([]Signal, error) {
	signals := MapSignals(incoming)
	runSig, ok := signals["run"]
	if !ok {
		return []Signal{InvalidSignal("hours", config.ValueKindNumber)}, nil
	}
	run, okRun := runSig.AsBool()
	if !okRun {
		return []Signal{InvalidSignal("hours", config.ValueKindNumber)}, nil
	}
	if run {
		r.total += ctx.Delta.Hours()
	}
	return []Signal{NewNumberSignal("hours", r.total)}, nil
}

func (e *energyProgram) ID() string { return e.id }

func (e *energyProgram) Execute(ctx Context, incoming []Signal) ([]Signal, error) {
	signals := MapSignals(incoming)
	powerSig, ok := signals["power"]
	if !ok {
		return []Signal{InvalidSignal("energy", config.ValueKindNumber)}, nil
	}
	power, okPower := powerSig.AsNumber()
	if !okPower {
		return []Signal{InvalidSignal("energy", config.ValueKindNumber)}, nil
	}
	if enableSig, ok := signals["enable"]; ok {
		if enable, okBool := enableSig.AsBool(); okBool && !enable {
			return []Signal{NewNumberSignal("energy", e.total)}, nil
		}
	}
	e.total += power * ctx.Delta.Hours()
	return []Signal{NewNumberSignal("energy", e.total)}, nil
}

func init() {
	RegisterProgram("operating_hours", newRuntimeProgram)
	RegisterProgram("energy_meter", newEnergyProgram)
}
