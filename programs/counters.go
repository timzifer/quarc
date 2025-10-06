package programs

type counterMode int

const (
	counterUp counterMode = iota
	counterDown
	counterUpDown
)

type counterProgram struct {
	id        string
	mode      counterMode
	value     float64
	min       float64
	max       float64
	preset    float64
	useMin    bool
	useMax    bool
	usePreset bool
	prevCU    bool
	prevCD    bool
}

func newCounterProgram(instanceID string, settings map[string]interface{}, mode counterMode) (Program, error) {
	min, err := getFloat(settings, "min", 0)
	if err != nil {
		return nil, err
	}
	max, err := getFloat(settings, "max", 0)
	if err != nil {
		return nil, err
	}
	preset, err := getFloat(settings, "preset", 0)
	if err != nil {
		return nil, err
	}
	initial, err := getFloat(settings, "initial", 0)
	if err != nil {
		return nil, err
	}
	prog := &counterProgram{id: instanceID, mode: mode, value: initial}
	if settings != nil {
		if settings["min"] != nil {
			prog.min = min
			prog.useMin = true
		}
		if settings["max"] != nil {
			prog.max = max
			prog.useMax = true
		}
		if settings["preset"] != nil {
			prog.preset = preset
			prog.usePreset = true
		}
	}
	return prog, nil
}

func newCTUProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	return newCounterProgram(instanceID, settings, counterUp)
}

func newCTDProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	return newCounterProgram(instanceID, settings, counterDown)
}

func newCTUDProgram(instanceID string, settings map[string]interface{}) (Program, error) {
	return newCounterProgram(instanceID, settings, counterUpDown)
}

func (c *counterProgram) ID() string { return c.id }

func (c *counterProgram) Execute(ctx Context, incoming []Signal) ([]Signal, error) {
	signals := MapSignals(incoming)
	if resetSig, ok := signals["reset"]; ok {
		if reset, okReset := resetSig.AsBool(); okReset && reset {
			if initialSig, okInit := signals["initial"]; okInit {
				if val, okVal := initialSig.AsNumber(); okVal {
					c.value = val
				}
			}
			return []Signal{NewNumberSignal("value", c.value), NewBoolSignal("done", c.done())}, nil
		}
	}
	if c.mode == counterUp || c.mode == counterUpDown {
		if cuSig, ok := signals["cu"]; ok {
			if cu, okBool := cuSig.AsBool(); okBool {
				if cu && !c.prevCU {
					c.value++
				}
				c.prevCU = cu
			}
		}
	}
	if c.mode == counterDown || c.mode == counterUpDown {
		if cdSig, ok := signals["cd"]; ok {
			if cd, okBool := cdSig.AsBool(); okBool {
				if cd && !c.prevCD {
					c.value--
				}
				c.prevCD = cd
			}
		}
	}
	if loadSig, ok := signals["load"]; ok {
		if load, okBool := loadSig.AsBool(); okBool && load {
			if valSig, okValSig := signals["load_value"]; okValSig {
				if val, okVal := valSig.AsNumber(); okVal {
					c.value = val
				}
			}
		}
	}
	if c.useMin && c.value < c.min {
		c.value = c.min
	}
	if c.useMax && c.value > c.max {
		c.value = c.max
	}
	return []Signal{NewNumberSignal("value", c.value), NewBoolSignal("done", c.done())}, nil
}

func (c *counterProgram) done() bool {
	if !c.usePreset {
		return false
	}
	switch c.mode {
	case counterDown:
		return c.value <= c.preset
	default:
		return c.value >= c.preset
	}
}

func init() {
	RegisterProgram("ctu", newCTUProgram)
	RegisterProgram("ctd", newCTDProgram)
	RegisterProgram("ctud", newCTUDProgram)
}
