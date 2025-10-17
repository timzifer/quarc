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
    reads?: [...#Read]
    writes?: [...#Write]
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

#Read: {
    id: string
    endpoint: _
    ttl?: string
    signals: [...#ReadSignal]
    disable?: bool
    can?: _
    specification?: _
    driver?: {
        name?: string
        settings?: specification
        ...
    }
    metadata?: _
    driver_metadata?: _
    // Deprecated fields retained for backward compatibility.
    function?: string
    start?: int
    length?: int
    ...
}

#Write: {
    id: string
    cell: string
    connection?: string
    endpoint: _
    priority?: int
    disable?: bool
    specification?: _
    driver?: {
        name?: string
        settings?: specification
        ...
    }
    metadata?: _
    // Deprecated fields retained for backward compatibility.
    function?: string
    address?: int
    endianness?: string
    signed?: bool
    deadband?: number
    rate_limit?: string
    scale?: number
    ...
}

#ReadSignal: {
    cell: string
    offset: int
    type: string
    bit?: int
    scale?: number
    endianness?: string
    signed?: bool
    aggregation?: string
    buffer_size?: int
    buffer?: #SignalBuffer
    aggregations?: [...#SignalAggregation]
    metadata?: _
    ...
}

#SignalBuffer: {
    capacity?: int
    aggregator?: *"last" | "sum" | "mean" | "min" | "max" | "count" | "queue_length"
    on_overflow?: string
    ...
}

#SignalAggregation: {
    cell: string
    aggregator?: *"last" | "sum" | "mean" | "min" | "max" | "count" | "queue_length"
    quality?: string
    on_overflow?: string
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
