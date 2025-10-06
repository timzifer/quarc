package programs

import "fmt"

type sequencerProgram struct {
	id          string
	steps       []string
	index       int
	wrap        bool
	prevAdvance bool
}

func newSequencerProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	steps, err := asStringSlice(settings["steps"])
	if err != nil {
		return nil, err
	}
	if len(steps) == 0 {
		return nil, fmt.Errorf("sequencer requires at least one step")
	}
	wrap, err := getBoolSetting(settings, "wrap", true)
	if err != nil {
		return nil, err
	}
	return &sequencerProgram{id: instanceID, steps: steps, wrap: wrap}, nil
}

func (s *sequencerProgram) ID() string { return s.id }

func (s *sequencerProgram) Execute(ctx Context, incoming []Signal) ([]Signal, error) {
	signals := MapSignals(incoming)
	if resetSig, ok := signals["reset"]; ok {
		if reset, okBool := resetSig.AsBool(); okBool && reset {
			s.index = 0
			s.prevAdvance = false
		}
	}
	if advSig, ok := signals["advance"]; ok {
		if adv, okBool := advSig.AsBool(); okBool {
			if adv && !s.prevAdvance {
				s.index++
				if s.index >= len(s.steps) {
					if s.wrap {
						s.index = 0
					} else {
						s.index = len(s.steps) - 1
					}
				}
			}
			s.prevAdvance = adv
		}
	}
	done := s.index == len(s.steps)-1
	outputs := []Signal{
		NewStringSignal("active", s.steps[s.index]),
		NewNumberSignal("step", float64(s.index)),
		NewBoolSignal("done", done),
	}
	return outputs, nil
}

func init() {
	RegisterProgram("sequencer", newSequencerProgram)
}
