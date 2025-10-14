package config

const moduleModulePath = "cue.mod/module.cue"
const moduleOverlayPath = "cue.mod/pkg/quarc.dev/quarc/module/module.cue"

const moduleModuleContent = `module: "quarc.dev/quarc"
language: {
    version: "v0.8.0"
}
`

const moduleOverlayContent = `package module

#Module: {
    config: #Config
    ...
}

#Config: {
    package?: string
    name?: string
    description?: string
    cycle?: string
    logging?: _
    telemetry?: _
    live_view?: _
    helpers?: [...]
    workers?: _
    dsl?: _
    policies?: _
    server?: _
    hot_reload?: bool
    programs?: [...#Program]
    cells?: [...#Cell]
    reads?: [..._]
    writes?: [..._]
    logic?: [..._]
    ...
}

#Program: {
    id: string
    type: string
    inputs?: [...#ProgramSignal]
    outputs?: [...#ProgramSignal]
    settings?: _
    metadata?: _
    ...
}

#ProgramSignal: {
    id: string
    cell: string
    type?: string
    default?: _
    metadata?: _
    ...
}

//#Cell provides a reusable schema for cell declarations.
#Cell: {
    id: string
    type: string
    unit?: string
    ttl?: string
    scale?: number
    signed?: bool
    metadata?: _
    ...
}
`

func init() {
	RegisterDefaultOverlay(func() error {
		if err := RegisterOverlayString(moduleModulePath, moduleModuleContent); err != nil {
			return err
		}
		return RegisterOverlayString(moduleOverlayPath, moduleOverlayContent)
	})
}
