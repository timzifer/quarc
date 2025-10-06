package programs

import (
	"sort"

	"modbus_processor/internal/config"
)

type lowPassProgram struct {
	id       string
	alpha    float64
	tau      float64
	useAlpha bool
	value    float64
	hasValue bool
}

type movingAverageProgram struct {
	id     string
	window int
	values []float64
	sum    float64
}

type medianProgram struct {
	id     string
	window int
	values []float64
}

func newLowPassProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	alpha, err := getFloat(settings, "alpha", 0)
	if err != nil {
		return nil, err
	}
	tau, err := getFloat(settings, "tau", 0)
	if err != nil {
		return nil, err
	}
	prog := &lowPassProgram{id: instanceID, alpha: alpha, tau: tau}
	if settings != nil && settings["alpha"] != nil {
		prog.useAlpha = true
	}
	return prog, nil
}

func newMovingAverageProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	window, err := getFloat(settings, "window", 5)
	if err != nil {
		return nil, err
	}
	if window < 1 {
		window = 1
	}
	return &movingAverageProgram{id: instanceID, window: int(window)}, nil
}

func newMedianProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	window, err := getFloat(settings, "window", 5)
	if err != nil {
		return nil, err
	}
	if window < 1 {
		window = 1
	}
	if int(window)%2 == 0 {
		window++
	}
	return &medianProgram{id: instanceID, window: int(window)}, nil
}

func (l *lowPassProgram) ID() string { return l.id }

func (l *lowPassProgram) Execute(ctx Context, incoming []Signal) ([]Signal, error) {
	signals := MapSignals(incoming)
	inputSig, ok := signals["input"]
	if !ok {
		return []Signal{InvalidSignal("output", config.ValueKindNumber)}, nil
	}
	input, okVal := inputSig.AsNumber()
	if !okVal {
		return []Signal{InvalidSignal("output", config.ValueKindNumber)}, nil
	}
	if !l.hasValue {
		l.value = input
		l.hasValue = true
		return []Signal{NewNumberSignal("output", l.value)}, nil
	}
	alpha := l.alpha
	if !l.useAlpha {
		dt := ctx.Delta.Seconds()
		if dt <= 0 {
			dt = 0
		}
		if l.tau <= 0 {
			alpha = 1
		} else {
			alpha = dt / (l.tau + dt)
		}
	}
	if alpha < 0 {
		alpha = 0
	}
	if alpha > 1 {
		alpha = 1
	}
	l.value = l.value + alpha*(input-l.value)
	return []Signal{NewNumberSignal("output", l.value)}, nil
}

func (m *movingAverageProgram) ID() string { return m.id }

func (m *movingAverageProgram) Execute(ctx Context, incoming []Signal) ([]Signal, error) {
	signals := MapSignals(incoming)
	inputSig, ok := signals["input"]
	if !ok {
		return []Signal{InvalidSignal("output", config.ValueKindNumber)}, nil
	}
	input, okVal := inputSig.AsNumber()
	if !okVal {
		return []Signal{InvalidSignal("output", config.ValueKindNumber)}, nil
	}
	m.values = append(m.values, input)
	m.sum += input
	if len(m.values) > m.window {
		m.sum -= m.values[0]
		m.values = m.values[1:]
	}
	avg := m.sum / float64(len(m.values))
	return []Signal{NewNumberSignal("output", avg)}, nil
}

func (m *medianProgram) ID() string { return m.id }

func (m *medianProgram) Execute(ctx Context, incoming []Signal) ([]Signal, error) {
	signals := MapSignals(incoming)
	inputSig, ok := signals["input"]
	if !ok {
		return []Signal{InvalidSignal("output", config.ValueKindNumber)}, nil
	}
	input, okVal := inputSig.AsNumber()
	if !okVal {
		return []Signal{InvalidSignal("output", config.ValueKindNumber)}, nil
	}
	m.values = append(m.values, input)
	if len(m.values) > m.window {
		m.values = m.values[1:]
	}
	sorted := append([]float64(nil), m.values...)
	sort.Float64s(sorted)
	median := sorted[len(sorted)/2]
	return []Signal{NewNumberSignal("output", median)}, nil
}

func init() {
	RegisterProgram("lowpass", newLowPassProgram)
	RegisterProgram("moving_average", newMovingAverageProgram)
	RegisterProgram("median", newMedianProgram)
}
