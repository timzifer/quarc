# Modbus Processor

A deterministic, cyclic Modbus controller that executes three strictly separated phases – **READ**, **EVAL** and **COMMIT** – on a fixed schedule. The controller ingests Modbus registers in blocks, stores them in typed in-memory cells, evaluates expression-based logic (normal and fallback ASTs) and finally commits changes back to Modbus targets using a write-on-change strategy.

## Architecture

* **Cells** – Typed storage units (`number`, `bool`, `string`) with a binary validity flag. Optional diagnostics (error code, message, timestamp) are attached when reads, evaluations or writes fail. Only the validity flag is considered during logic execution.
* **Read phase** – Groups Modbus reads by slave/function/range. Each group has an individual TTL; groups that expired during the current cycle are re-polled. Raw bytes are marshalled into typed cell values using the configured endianness, signedness and scaling. Read failures only invalidate the affected cells – the rest of the cycle continues.
* **Eval phase** – Runs normal ASTs in a deterministic order derived from the declared dependency graph (topological sort with configuration order as tie breaker). Every block operates on a consistent snapshot of the cell state. When dependencies are invalid or the normal AST explicitly triggers `fallback()`, the fallback AST is executed. Logic blocks emit exactly one target cell value via `success(value)`.
* **Commit phase** – Writes only the cells that changed beyond the configured deadband/rate limits. Writes are ordered by priority. Marshalling is performed in reverse (typed value → bytes). Failures raise diagnostics but never abort the cycle.

The cycle duration is monotonic (default `500ms` if unspecified). Metrics record last cycle duration, counts of read/eval/write errors and total cycles executed.

## Expression DSL

Expressions are compiled with [`expr`](https://github.com/expr-lang/expr). The following helpers are available:

| Function | Description |
|----------|-------------|
| `value(id)` | Returns the typed value of the referenced cell. If the cell is missing or invalid, the current AST switches to the fallback path. |
| `success(v)` | Finalises the current block with the provided result. The controller converts the value into the target cell type. |
| `fail(code?, msg?)` | Marks the target cell invalid, storing the optional diagnostic code/message. |
| `fallback()` | Immediately switches to the fallback AST. |
| `valid(id)` | *(fallback AST only)* Returns `true` if the referenced cell exists and is valid. |
| `dump(value)` | Logs the provided value at debug level and returns it unchanged. Useful for inspecting intermediate results. |
| `log(message?, values...)` | Emits an info level log entry annotated with the current block/helper context. The first argument can be a message string followed by optional values. |

The normal AST only executes when all declared dependencies exist and are valid. Fallback ASTs typically provide defaults or soft-fail behaviour. Helper functions follow the same contract and must call `success(...)` to return a value; completing without a `success` signal is treated as an error.

## Configuration

Configuration is provided as YAML (see [`config.example.yaml`](config.example.yaml)). Key sections:

* `cycle` – Global cycle time.
* `modules` – Optional list of additional YAML snippets to load (relative to the parent file). The loader also accepts directories, processing all `*.yaml`/`*.yml` files in lexical order, which makes `config.d`-style setups trivial.
* `cells` – Definitions of all local memory cells.
* `reads` – Modbus block reads (slave endpoint, function, address range, TTL and signal mapping into cells).
* `logic` – Logic blocks with normal/fallback ASTs and a target cell. Dependencies are automatically discovered from the expressions.
* `writes` – Modbus write targets with deadband, rate limit and priority options.
* `logging` / `policies` – Runtime logging setup and optional global policies (retry behaviour, watchdog, readback, etc.). `logging.format` controls the stdout renderer (`json` by default, `text` for human friendly console output).
* `server` – Configuration for the embedded Modbus/TCP server that exposes cells as input registers.

### Example snippet

```yaml
logic:
  - id: heater_control
    target: heater_command
    normal: |
      success(
        !value("heater_enabled") ? false :
        value("temperature_c") < 18 ? true :
        value("temperature_c") > 21 ? false :
        fallback()
      )
    fallback: |
      success(valid("temperature_c") ? false : fail("logic.dependency", "temperature unavailable"))
```

See the full [`config.example.yaml`](config.example.yaml) for a complete configuration including reads and writes.

### Splitting configuration files

Use the `modules` directive to include additional YAML documents from the main configuration. Paths are resolved relative to the parent file, and may reference either files or directories. When a directory is supplied (for example `config.d`), every `.yaml` / `.yml` file is merged in lexical order, allowing you to organise large installations across multiple files without manual concatenation.

### Embedded Modbus server

When `server.enabled` is `true`, the processor starts an integrated Modbus/TCP server. Cells mapped in `server.cells` are exposed as input registers, making the current controller state available to external systems. Numeric cells are scaled according to the provided `scale` factor and can be marked as signed, while boolean cells are exported as 0/1 values.

Trace level logging now provides detailed insights into each READ/EVAL/COMMIT step, including the Modbus server update cycle.

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
