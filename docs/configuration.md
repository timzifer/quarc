# Configuration

QUARC uses [CUE](https://cuelang.org) to describe installations. Each configuration package exports a top-level `config` value matching `config.Config`. The package namespace is derived from the CUE package path (with directory separators rewritten as dots) and can be extended with the optional `config.package` suffix.

Unqualified identifiers such as `temperature` are automatically rewritten to `<package path>:<local>` when loaded, ensuring short names remain unambiguous. Fully qualified references that already include a colon are left untouched.

## Top-level sections

Key sections mirror the runtime layout:

* `cycle` – Global cycle duration.
* `programs` – Reusable control modules with typed input/output bindings executed between the read and logic phases.
* `cells` – Definitions of all local memory cells.
* `connections` – Reusable IO connection definitions describing shared transports (driver, endpoint, optional pooling hints and `driver_settings`). Reads and writes reference these by identifier via their `connection` field.
* `reads` – Block reads that reference a driver either directly via their `endpoint.driver` or by binding to a shared `connection`. Drivers ingest protocol-specific payloads (Modbus, CAN, OPC-UA, mock data, …) and translate them into cell updates. Inline endpoint blocks may override connection defaults (e.g. timeouts).
* `logic` – Logic blocks with an expression AST, optional `valid`/`quality` expressions, and a target cell. Dependencies are derived automatically from the expressions.
* `writes` – Write targets referencing a driver identifier, either inline or through a shared `connection`. Driver-specific `driver_settings` values are forwarded untouched so transports can expose their own tuning knobs.
* `logging` / `policies` – Runtime logging setup and optional global policies (retry behaviour, watchdog, readback, etc.). `logging.format` controls the stdout renderer (`json` by default, `text` for human-friendly console output).
* `telemetry` – Enables metrics exporters such as Prometheus.
* `live_view` – Configures the optional live view web UI and heatmap.

## Example

```cue
package plant

config: {
    package: "plant.core"
    cycle: "1s"
    logging: {
        level: "info"
        loki: {
            enabled: false
            url: ""
            labels: {
                app: "plant.core" // overrides the default {app: "quarc"}
            }
        }
    }
    telemetry: {
        enabled: false
    }
    connections: [
        {
            id: "io_bus"
            driver: "modbus"
            endpoint: {
                address: "192.168.10.10:502"
                unit_id: 1
            }
        },
    ]
    cells: [
        {
            id: "temperature"
            type: "number"
        },
    ]
    reads: [
        {
            id: "temperature_sensor"
            connection: "io_bus"
            endpoint: {
                timeout: "1s"
            }
            function: "holding"
            start: 0
            length: 1
            ttl: "1s"
            signals: [
                {
                    cell: "temperature"
                    offset: 0
                    type: "number"
                    scale: 0.1
                },
            ]
        },
    ]
    logic: [
        {
            id: "heater_control"
            target: "heater_command"
            expression: """
                !value("heater_enabled") ? false :
                value("alarm_state") ? false :
                value("pid_output") >= 60 ? true :
                value("pid_output") <= 40 ? false :
                heater_command
            """
            valid: """
                error == nil ? true : {"valid": false, "code": error_code, "message": error_message}
            """
            quality: """
                error == nil ? 1 : 0.95
            """
        },
    ]
    writes: []
}
```

## Templates and reuse

CUE definitions replace legacy YAML templates and allow type-safe reuse:

```cue
template: {
    cell: {
        id: string
        type: "number"
        unit?: string
    }
}

config: {
    cells: [
        template.cell & { id: "supply_pressure", unit: "bar" },
        template.cell & { id: "return_pressure", unit: "bar" },
    ]
}
```

## Splitting configuration files

Place additional `.cue` files in the same directory or import reusable packages. CUE automatically unifies files that share a package name, allowing large installations to be organised without custom include directives. Namespaces follow the package hierarchy, giving each bundle a natural, collision-free prefix.

## Signal buffers

Each read signal can declare an optional `buffer` block that accumulates values between flushes, as well as an `aggregations` list describing how buffered samples collapse into outputs. `capacity` controls how many samples are retained while each aggregation selects a target cell and strategy (`last`, `sum`, `mean`, `min`, `max`, `count`, `queue_length`). Optional `quality` and `on_overflow` fields allow forwarding aggregated quality or suppressing overflow diagnostics (set `"ignore"`). Buffers default to a capacity of one and a single `last` aggregation when omitted.

```cue
signals: [
    {
        cell: "temperature_sum"
        offset: 0
        type: "number"
        buffer: {
            capacity: 60
        }
        aggregations: [
            {
                cell: "temperature_sum"
                aggregator: "sum"
            },
        ]
    },
    {
        cell: "temperature_mean"
        offset: 0
        type: "number"
        buffer: {
            capacity: 60
        }
        aggregations: [
            {
                cell: "temperature_mean"
                aggregator: "mean"
            },
        ]
    },
]
```

See [`config.example.cue`](../config.example.cue) for a full configuration that demonstrates reads, programs, writes, telemetry, and live view settings.
