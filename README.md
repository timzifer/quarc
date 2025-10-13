# Modbus Processor

A deterministic, cyclic Modbus controller that executes four strictly separated phases – **READ**, **PROGRAM**, **EVAL** and **COMMIT** – on a fixed schedule. The controller ingests Modbus registers in blocks, stores them in typed in-memory cells, lets reusable control programs derive new signals, evaluates expression-based logic with optional validation expressions and finally commits changes back to Modbus targets using a write-on-change strategy.

## Architecture

* **Cells** – Typed storage units (`number`, `bool`, `string`) with a binary validity flag. Optional diagnostics (error code, message, timestamp) are attached when reads, evaluations or writes fail. Only the validity flag is considered during logic execution.
* **Read phase** – Groups Modbus reads by slave/function/range. Each group has an individual TTL; groups that expired during the current cycle are re-polled. Raw bytes are marshalled into typed cell values using the configured endianness, signedness and scaling. Read failures only invalidate the affected cells – the rest of the cycle continues.
* **Program phase** – Executes reusable control modules that consume existing cell snapshots and emit additional signals. Programs are instantiated from the registry, receive cycle timing information and may produce diagnostics when mandatory outputs are missing or invalid.
* **Eval phase** – Runs expression ASTs in a deterministic order derived from the declared dependency graph (topological sort with configuration order as tie breaker). Every block operates on a consistent snapshot of the cell state. Optional `valid`/`quality` expressions inspect the computed result (and potential evaluation errors) to decide validity, diagnostics and a quality value for the target cell.
* **Commit phase** – Writes only the cells that changed beyond the configured deadband/rate limits. Writes are ordered by priority. Marshalling is performed in reverse (typed value → bytes). Failures raise diagnostics but never abort the cycle.

The cycle duration is monotonic (default `500ms` if unspecified). Metrics record last cycle duration, counts of read/eval/write errors and total cycles executed.

## Runtime packages

Interfaces used by the scheduler now live under the `runtime` namespace:

* `runtime/state` – cell abstractions (`Cell`, `CellStore`).
* `runtime/readers` – reader interfaces, status structures, and factories.
* `runtime/writers` – writer interfaces, status structures, and factories.

The legacy `serviceio` package still exports deprecated type aliases for
backwards compatibility but will be removed in a future release. Update existing
integrations to import the runtime packages directly.

### Runtime overrides

Besides deterministic scheduling, the service exposes helper methods that make the controller easier to embed into a supervisory application:

* `SetCellValue(id, value)` – manually override a cell with type-checked data. The Modbus server is refreshed immediately so connected HMIs or SCADA clients see the change without waiting for the next cycle.
* `InvalidateCell(id, code, message)` – mark a cell invalid while attaching a diagnostic entry that is exported to logic blocks and snapshots.
* `InspectCell(id)` – obtain a structured view of the current value, validity, diagnostic metadata and last update timestamp.

These helpers allow HMIs to implement “force” functionality or provide manual fallback values during commissioning, mirroring typical features of a soft PLC.

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

Configuration is provided as YAML (see [`config.example.yaml`](config.example.yaml)). Key sections:

* `cycle` – Global cycle time.
* `modules` – Optional list of additional YAML snippets to load (relative to the parent file). The loader also accepts directories, processing all `*.yaml`/`*.yml` files in lexical order, which makes `config.d`-style setups trivial. Files ending in `.values.yaml`/`.values.yml` are treated as value bundles and skipped automatically when a whole directory is imported.
* `programs` – Reusable control modules with typed input/output bindings that execute between the read and logic phases.
* `cells` – Definitions of all local memory cells.
* `reads` – Modbus block reads (slave endpoint, function, address range, TTL and signal mapping into cells). A dedicated `can` driver ingests UDP/TCP byte streams from CAN↔Ethernet controllers and decodes frames according to an external DBC file.
* `logic` – Logic blocks with an expression AST, optional `valid`/`quality` expressions and a target cell. Dependencies are automatically discovered from all expressions.
* `writes` – Modbus write targets with deadband, rate limit and priority options.
* `logging` / `policies` – Runtime logging setup and optional global policies (retry behaviour, watchdog, readback, etc.). `logging.format` controls the stdout renderer (`json` by default, `text` for human friendly console output).
* `server` – Configuration for the embedded Modbus/TCP server that exposes cells as input registers.

### Example snippet

```yaml
logic:
  - id: heater_control
    target: heater_command
    expression: |
      !value("heater_enabled") ? false :
      value("alarm_state") ? false :
      value("pid_output") >= 60 ? true :
      value("pid_output") <= 40 ? false :
      heater_command
    valid: |
      error == nil ? true : {"valid": false, "code": error_code, "message": error_message}
    quality: |
      error == nil ? 1 : 0.95
```

See the full [`config.example.yaml`](config.example.yaml) for a complete configuration including reads, programs and writes. The configuration loads every module in [`example-config.d`](example-config.d) to showcase each built-in program with runnable input/output bindings.

#### Read configuration examples

```yaml
reads:
  - id: temperature_sensor
    endpoint:
      address: "192.168.10.10:502"
      unit_id: 1
    function: holding
    start: 0
    length: 1
    ttl: 1s
    signals:
      - cell: raw_temperature
        offset: 0
        type: number
        scale: 0.1
  - id: drivetrain_can
    endpoint:
      address: "192.168.10.50:20108"
      driver: can
    ttl: 0s
    can:
      protocol: udp
      dbc: configs/drivetrain.dbc
      frames:
        - message: MotorStatus
          signals:
            - name: SpeedKph
              cell: motor_speed
            - name: WheelError
              cell: motor_alarm
```

### Reusable programs

Programs encapsulate common control algorithms such as PID regulators, ramp generators, slew limiters, timers, counters, filters, selection logic, latches, runtime/energy tracking, sequencers and alarm supervision. Each entry in the `programs` list declares:

* `id` – Unique instance identifier referenced by diagnostics.
* `type` – Program factory key (e.g. `pid`, `ramp`, `slew_asym`).
* `inputs` / `outputs` – Mappings from program signal names to cell IDs. Optional signals can define defaults and type overrides.
* `settings` – Arbitrary key/value map forwarded to the program factory for tuning parameters.

Programs run before logic evaluation and write directly into the associated cells, making their results available to downstream logic blocks, Modbus writes and the embedded server. Refer to the files in [`example-config.d`](example-config.d) for concrete YAML fragments covering every bundled program.

### Splitting configuration files

Use the `modules` directive to include additional YAML documents from the main configuration. Paths are resolved relative to the parent file, and may reference either files or directories. When a directory is supplied (for example `config.d`), every `.yaml` / `.yml` file is merged in lexical order, allowing you to organise large installations across multiple files without manual concatenation. Files that end in `.values.yaml` / `.values.yml` are skipped automatically – they are only loaded when explicitly referenced from the `values` section.

Values bundles referenced from `values:` **must** use the `.values.yaml` or `.values.yml` suffix so the loader can distinguish them from ordinary modules. Inline mappings remain supported for small pieces of data.

### Schema validation

Machine-readable JSON Schemas are available under [`config/config.schema.json`](config/config.schema.json) and [`config/values.schema.json`](config/values.schema.json). They can be used with editors or CI tooling to validate configuration fragments and value bundles. The configuration schema focuses on structural validation while keeping `additionalProperties` enabled so custom metadata blocks remain possible.

### Embedded Modbus server

When `server.enabled` is `true`, the processor starts an integrated Modbus/TCP server. Cells mapped in `server.cells` are exposed as input registers, making the current controller state available to external systems. Numeric cells are scaled according to the provided `scale` factor and can be marked as signed, while boolean cells are exported as 0/1 values.

Trace level logging now provides detailed insights into each READ/PROGRAM/EVAL/COMMIT step, including the Modbus server update cycle.

## Running

1. Build/install the binary:
   ```bash
   go build ./cmd/...
   ```
2. Start the processor with your configuration:
   ```bash
   ./modbus_processor --config path/to/config.yaml
   ```

   Use `--config-check` to produce a detailed logic validation report without starting the service, or `--healthcheck` to perform a lightweight configuration validation suitable for Docker health probes.

The service logs with [zerolog](https://github.com/rs/zerolog) and can optionally stream logs to Loki when configured.

## Testing

Run the full test suite with:

```bash
go test ./...
```
