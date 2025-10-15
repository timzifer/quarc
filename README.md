![QUARC Gopher Mascot](docs/assets/quarc-gopher.svg)

# QUARC (Quadriphasic Automation Runtime Controller)

![Coverage](https://gopherbadger.herokuapp.com/badge/github.com/timzifer/quarc.svg)

A deterministic, cyclic automation runtime that executes four strictly separated phases – **READ**, **PROGRAM**, **EVAL** and **COMMIT** – on a fixed schedule. The controller stores typed in-memory cells, lets reusable control programs derive new signals, evaluates expression-based logic with optional validation expressions and finally commits changes back to configured outputs using a write-on-change strategy.

QUARC is transport agnostic: protocol support is provided by pluggable drivers and the core runtime no longer embeds a Modbus/TCP server. Deployments can therefore run without any physical inputs or outputs and hook up only the interfaces they explicitly register.

## Architecture

* **Cells** – Typed storage units (`number`, `bool`, `string`) with a binary validity flag. Optional diagnostics (error code, message, timestamp) are attached when reads, evaluations or writes fail. Only the validity flag is considered during logic execution.
* **Read phase** – Groups driver reads by source/function/range. Each group has an individual TTL; groups that expired during the current cycle are re-polled. Raw bytes (or driver specific payloads) are marshalled into typed cell values using the configured endianness, signedness and scaling. Read failures only invalidate the affected cells – the rest of the cycle continues.
* **Program phase** – Executes reusable control modules that consume existing cell snapshots and emit additional signals. Programs are instantiated from the registry, receive cycle timing information and may produce diagnostics when mandatory outputs are missing or invalid.
* **Eval phase** – Runs expression ASTs in a deterministic order derived from the declared dependency graph (topological sort with configuration order as tie breaker). Every block operates on a consistent snapshot of the cell state. Optional `valid`/`quality` expressions inspect the computed result (and potential evaluation errors) to decide validity, diagnostics and a quality value for the target cell.
* **Commit phase** – Writes only the cells that changed beyond the configured deadband/rate limits. Writes are ordered by priority. Marshalling is performed in reverse (typed value → protocol payload). Failures raise diagnostics but never abort the cycle.

The cycle duration is monotonic (default `500ms` if unspecified). Metrics record last cycle duration, counts of read/eval/write errors and total cycles executed.

## Continuous integration

The [Go coverage workflow](.github/workflows/coverage.yml) runs `go test ./... -coverprofile=coverage.out`, publishes the coverage profile as a build artifact and reports the aggregated coverage percentage in the job summary.

## Runtime packages

Interfaces used by the scheduler now live under the `runtime` namespace:

* `runtime/state` – cell abstractions (`Cell`, `CellStore`).
* `runtime/readers` – reader interfaces, status structures, and factories.
* `runtime/writers` – writer interfaces, status structures, and factories.

The legacy `serviceio` package still exports deprecated type aliases for backwards compatibility but will be removed in a future release. Update existing integrations to import the runtime packages directly.

### Runtime overrides

Besides deterministic scheduling, the service exposes helper methods that make the controller easier to embed into a supervisory application:

* `SetCellValue(id, value)` – manually override a cell with type-checked data. Connected HMIs or supervisory systems see the change on the next publish by their respective drivers.
* `InvalidateCell(id, code, message)` – mark a cell invalid while attaching a diagnostic entry that is exported to logic blocks and snapshots.
* `InspectCell(id)` – obtain a structured view of the current value, validity, diagnostic metadata and last update timestamp.

These helpers allow HMIs to implement “force” functionality or provide manual fallback values during commissioning, mirroring typical features of a soft PLC.

## Drivers

Protocol implementations are packaged as standalone Go modules under the [`drivers/`](drivers) directory. The core service does not link against fieldbus, PLC or SCADA dependencies by default – applications register the drivers they need via `service.WithReaderFactory` / `service.WithWriterFactory` options when constructing a `service.Service` instance. Every endpoint must declare the driver it expects so the service can select the matching factories.

For convenience the [`drivers/bundle`](drivers/bundle) module exposes helper functions that install the built-in drivers:

```go
svc, err := service.New(cfg, logger, bundle.Options(modbus.NewTCPClientFactory())...)
```

Custom drivers can be registered by providing factories for a new driver identifier:

```go
svc, err := service.New(cfg, logger,
        service.WithReaderFactory("my-driver", myReaderFactory),
        service.WithWriterFactory("my-driver", myWriterFactory),
)
```

Driver modules receive the raw `driver_settings` YAML node from the configuration (see below) so that protocol-specific options can be deserialised without polluting the core schema.

## Expression DSL

Expressions are compiled with [`expr`](https://github.com/expr-lang/expr). The following helpers are available:

| Function | Description |
|----------|-------------|
| `value(id)` | Returns the typed value of the referenced cell. If the cell is missing or invalid, expression evaluation fails with a diagnostic error. |
| `cell(id)` | Alias for `value(id)` that is also available inside validation expressions. |
| `valid(id)` | Returns `true` if the referenced cell exists and is valid. |
| `dump(value)` | Logs the provided value at debug level and returns it unchanged. Useful for inspecting intermediate results. |
| `log(level?, message?, values...)` | Emits a log entry annotated with the current block/helper context. When the first argument is a recognised level (`trace`, `debug`, `info`, `warn`, `error`) it controls the severity; otherwise it is treated as the message. Remaining arguments are logged as `value`/`values`. |

Validation expressions (`valid` / `quality`) receive additional helpers: `value` (the computed result), `error` (a map with `code`/`message` when the evaluation failed) and `error_raw`/`error_code`/`error_message` for convenience alongside the regular DSL helpers (`cell`, `valid`, `dump`, `log`).

Expression ASTs only execute when all declared dependencies exist and are valid. Helper functions follow the same contract and return their result directly; any runtime error bubbles up to the caller.

## Configuration

Configuration is expressed in [CUE](https://cuelang.org). Each configuration package exports a top-level `config` value matching `config.Config`. The optional `package` field determines the namespace applied to identifiers. Any `id` or cell reference without a dot is automatically qualified with this package path, so short names stay unambiguous. References that already contain a dot are treated as fully qualified names and are left untouched.

Key sections mirror the previous layout:

* `cycle` – Global cycle time.
* `programs` – Reusable control modules with typed input/output bindings that execute between the read and logic phases.
* `cells` – Definitions of all local memory cells.
* `reads` – Block reads referencing a `driver` via the endpoint. Drivers ingest protocol specific payloads (Modbus, CAN, OPC-UA, mock data, …) and translate them into cell updates.
* `logic` – Logic blocks with an expression AST, optional `valid`/`quality` expressions and a target cell. Dependencies are automatically discovered from all expressions.
* `writes` – Write targets referencing a driver identifier. Driver specific `driver_settings` values are forwarded untouched so transports can expose their own tuning knobs.
* `logging` / `policies` – Runtime logging setup and optional global policies (retry behaviour, watchdog, readback, etc.). `logging.format` controls the stdout renderer (`json` by default, `text` for human friendly console output).
### Example snippet

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
            labels: {}
        }
    }
    telemetry: {
        enabled: false
    }
    cells: [
        {
            id: "temperature"
            type: "number"
        },
    ]
    reads: [
        {
            id: "temperature_sensor"
            endpoint: {
                address: "192.168.10.10:502"
                unit_id: 1
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

## Live View & Telemetrie

Die Live-View-Oberfläche stellt eine leichtgewichtige Web-UI bereit, mit der sich Zyklen, Zellen, Lese-/Schreibpuffer sowie Programmausführungen in Echtzeit nachvollziehen lassen. Neben Tabellenansichten bietet sie eine Heatmap, die zuletzt aktive Zellen, Logik-Blöcke und Programme hervorhebt und so beim Debugging sowie bei der Inbetriebnahme unterstützt.

**Aktivierung:**

* CLI: `quarc --live-view --live-view-listen=:18080` startet den Server auf Port `18080`. Ohne `--live-view` bleibt die Oberfläche deaktiviert.
* Embedding/API: Anwendungen, die den Prozessor programmatisch starten, aktivieren den Server über `processor.WithLiveView(host, port)`.

**Konfiguration:**

Die Gestaltung der Heatmap erfolgt über den `live_view`-Abschnitt der CUE-Konfiguration. Das [Beispiel `config.example.cue`](config.example.cue) zeigt den Standardaufbau mit `heatmap.cooldown`- und `heatmap.colors`-Feldern. `cooldown` definiert, wie viele Zyklen Aktivitäten sichtbar bleiben (`cells`, `logic`, `programs`), bevor ein Tile in den `stale`-Zustand zurückfällt. Die Farbpalette legt hervor, wie einzelne Heatmap-Bereiche dargestellt werden:

```cue
live_view: {
    heatmap: {
        cooldown: {
            cells: 12
            logic: 12
            programs: 12
        }
        colors: {
            read: "#4caf50"
            write: "#ef5350"
            stale: "#bdbdbd"
            logic: "#29b6f6"
            program: "#ab47bc"
            background: "#f5f5f5"
            border: "#424242"
        }
    }
}
```

Die Farben werden 1:1 in der Oberfläche verwendet: Neu geschriebene Zellen nutzen `write`, kürzlich gelesene `read`, inaktive Kacheln `stale`. Für Logik- und Programmkacheln kommen `logic` bzw. `program` zum Einsatz, während Rahmen- und Hintergrundfarben (`border`, `background`) das UI-Theming bestimmen.

**Telemetrie:**

Die Telemetrie wird über den `telemetry`-Block der Konfiguration aktiviert. Ist `enabled` gesetzt und kein eigener Collector registriert, erzeugt QUARC automatisch einen Prometheus-Collector (`provider: "prometheus"` oder leer). Das Beispiel oben setzt standardmäßig `enabled: false`; um Metriken auszugeben, genügt:

```cue
telemetry: {
    enabled: true
    provider: "prometheus"
}
```

Der Collector registriert folgende Metriken:

* `quarc_config_hot_reload_total` – Anzahl der erfolgreichen Konfigurations-Neuladevorgänge pro Datei.
* `quarc_read_buffer_dropped_total` – Summe verwerfener Samples pro Lesegruppe und Puffer (z. B. bei Überläufen).
* `quarc_read_buffer_occupancy` – Gauge, das die Auslastung jedes Lesepuffers beim letzten Flush meldet.

Eigene Collector-Implementierungen lassen sich per `processor.WithTelemetry(...)` übergeben; andernfalls fällt der Dienst auf einen No-Op-Collector zurück, wenn `enabled` deaktiviert oder ein unbekannter Provider hinterlegt ist.

Short identifiers such as `temperature` are materialised as `plant.core.temperature` once loaded. To refer to a cell from another package, use its fully qualified name directly.

### Templates and reuse

CUE definitions replace the previous YAML templates. For example:

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

### Splitting configuration files

Place additional `.cue` files in the same directory or import reusable packages. CUE automatically unifies all files that share a package name, so you can organise large installations across multiple files without custom include directives. Namespaces follow the package hierarchy, giving each bundle a natural, collision-free prefix.

See [`config.example.cue`](config.example.cue) for a complete configuration including reads, programs and writes.

#### Signal buffers

Each read signal can declare an optional `buffer` block that accumulates values between flushes as well as an `aggregations` list describing how buffered samples should be collapsed. The `capacity` controls how many samples are retained while each aggregation chooses a target cell and strategy (`last`, `sum`, `mean`, `min`, `max`, `count`, `queue_length`). Optional `quality` and `on_overflow` fields allow forwarding the aggregated quality to a dedicated cell or suppressing overflow diagnoses (use `"ignore"`). Buffers default to a capacity of one and a single `last` aggregation when omitted.

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

### Reusable programs

Programs encapsulate common control algorithms such as PID regulators, ramp generators, slew limiters, timers, counters, filters, selection logic, latches, runtime/energy tracking, sequencers and alarm supervision. Each entry in the `programs` list declares:

* `id` – Unique instance identifier referenced by diagnostics.
* `type` – Program factory key (e.g. `pid`, `ramp`, `slew_asym`).
* `inputs` / `outputs` – Mappings from program signal names to cell IDs. Optional signals can define defaults and type overrides.
* `settings` – Arbitrary key/value map forwarded to the program factory for tuning parameters.

Programs run before logic evaluation and write directly into the associated cells, making their results available to downstream logic blocks and driver backed outputs. See [`config.example.cue`](config.example.cue) for a full configuration that demonstrates the bundled programs and how they are wired.

## Running

1. Build/install the binary:
   ```bash
   go build ./cmd/...
   ```
2. Start the processor with your configuration:
   ```bash
   ./quarc --config path/to/config.cue
   ```

   Use `--config-check` to produce a detailed logic validation report without starting the service, or `--healthcheck` to perform a lightweight configuration validation suitable for Docker health probes.

The service logs with [zerolog](https://github.com/rs/zerolog) and can optionally stream logs to Loki when configured.

## Release process

Releases tag the root module **and** the driver modules so that consumers can pin compatible versions. When creating a new version, create matching tags for:

* `github.com/timzifer/quarc`
* `github.com/timzifer/quarc/drivers/modbus`
* `github.com/timzifer/quarc/drivers/canstream`
* `github.com/timzifer/quarc/drivers/bundle`

This ensures that downstream users embedding the drivers can resolve consistent module versions.

## Testing

Run the full test suite with:

```bash
go test ./...
```
