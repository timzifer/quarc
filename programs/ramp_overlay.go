package programs

import "github.com/timzifer/quarc/config"

const rampOverlayPath = "cue.mod/pkg/quarc.dev/quarc/programs/ramp/ramp.cue"

const rampOverlayContent = `package ramp

import module "quarc.dev/quarc/module"

#Ramp: module.#Module & {
    config: {
        cells?: [...module.#Cell]
        programs: [...#RampProgram]
        ...
    }
}

#RampProgram: module.#Program & {
    type: "ramp"
    outputs: [...#RampOutput]
    settings?: #RampSettings
    ...
}

#RampOutput: module.#ProgramSignal & {
    id: "value"
    ...
}

#RampSettings: {
    rate?: number
    initial?: number
    allow_hold?: bool
    ...
}
`

func init() {
	config.RegisterDefaultOverlay(func() error {
		return config.RegisterOverlayString(rampOverlayPath, rampOverlayContent)
	})
}
