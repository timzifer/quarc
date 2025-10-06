package programs

import (
	"math"
	"sort"

	"modbus_processor/internal/config"
)

type selectionMode int

const (
	modeMax selectionMode = iota
	modeMin
	modePriority
	modeMux
)

type selectionProgram struct {
	id     string
	mode   selectionMode
	inputs []string
}

func newSelectionProgram(instanceID string, settings map[string]interface{}, mode selectionMode) (Program, error) {
	inputNames, err := asStringSlice(settings["inputs"])
	if err != nil {
		return nil, err
	}
	return &selectionProgram{id: instanceID, mode: mode, inputs: inputNames}, nil
}

func newMaxProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	return newSelectionProgram(instanceID, settings, modeMax)
}

func newMinProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	return newSelectionProgram(instanceID, settings, modeMin)
}

func newPriorityProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	return newSelectionProgram(instanceID, settings, modePriority)
}

func newMuxProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	return newSelectionProgram(instanceID, settings, modeMux)
}

func (s *selectionProgram) ID() string { return s.id }

func (s *selectionProgram) Execute(ctx Context, incoming []Signal) ([]Signal, error) {
	signals := MapSignals(incoming)
	inputs := s.inputs
	if len(inputs) == 0 {
		inputs = make([]string, 0, len(signals))
		for name := range signals {
			if name == "selector" {
				continue
			}
			inputs = append(inputs, name)
		}
		sort.Strings(inputs)
	}
	switch s.mode {
	case modeMax:
		return s.executeMax(signals, inputs)
	case modeMin:
		return s.executeMin(signals, inputs)
	case modePriority:
		return s.executePriority(signals, inputs)
	case modeMux:
		return s.executeMux(signals, inputs)
	default:
		return []Signal{InvalidSignal("value", config.ValueKindNumber)}, nil
	}
}

func (s *selectionProgram) executeMax(signals map[string]Signal, inputs []string) ([]Signal, error) {
	best := math.Inf(-1)
	found := false
	for _, name := range inputs {
		sig, ok := signals[name]
		if !ok {
			continue
		}
		val, okVal := sig.AsNumber()
		if !okVal {
			continue
		}
		if !found || val > best {
			best = val
			found = true
		}
	}
	if !found {
		return []Signal{InvalidSignal("value", config.ValueKindNumber)}, nil
	}
	return []Signal{NewNumberSignal("value", best)}, nil
}

func (s *selectionProgram) executeMin(signals map[string]Signal, inputs []string) ([]Signal, error) {
	best := math.Inf(1)
	found := false
	for _, name := range inputs {
		sig, ok := signals[name]
		if !ok {
			continue
		}
		val, okVal := sig.AsNumber()
		if !okVal {
			continue
		}
		if !found || val < best {
			best = val
			found = true
		}
	}
	if !found {
		return []Signal{InvalidSignal("value", config.ValueKindNumber)}, nil
	}
	return []Signal{NewNumberSignal("value", best)}, nil
}

func (s *selectionProgram) executePriority(signals map[string]Signal, inputs []string) ([]Signal, error) {
	for _, name := range inputs {
		sig, ok := signals[name]
		if !ok || !sig.Valid {
			continue
		}
		return []Signal{Signal{ID: "value", Kind: sig.Kind, Value: sig.Value, Valid: true}}, nil
	}
	return []Signal{InvalidSignal("value", config.ValueKindNumber)}, nil
}

func (s *selectionProgram) executeMux(signals map[string]Signal, inputs []string) ([]Signal, error) {
	selectorSig, ok := signals["selector"]
	if !ok {
		return []Signal{InvalidSignal("value", config.ValueKindNumber)}, nil
	}
	idxFloat, okIdx := selectorSig.AsNumber()
	if !okIdx {
		return []Signal{InvalidSignal("value", config.ValueKindNumber)}, nil
	}
	idx := int(idxFloat)
	if idx < 0 || idx >= len(inputs) {
		return []Signal{InvalidSignal("value", config.ValueKindNumber)}, nil
	}
	sig, ok := signals[inputs[idx]]
	if !ok || !sig.Valid {
		return []Signal{InvalidSignal("value", config.ValueKindNumber)}, nil
	}
	return []Signal{Signal{ID: "value", Kind: sig.Kind, Value: sig.Value, Valid: true}}, nil
}

func init() {
	RegisterProgram("max", newMaxProgram)
	RegisterProgram("min", newMinProgram)
	RegisterProgram("priority", newPriorityProgram)
	RegisterProgram("mux", newMuxProgram)
}
