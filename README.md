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

The normal AST only executes when all declared dependencies exist and are valid. Fallback ASTs typically provide defaults or soft-fail behaviour.

## Configuration

Configuration is provided as YAML (see [`config.example.yaml`](config.example.yaml)). Key sections:

* `cycle` – Global cycle time.
* `cells` – Definitions of all local memory cells.
* `reads` – Modbus block reads (slave endpoint, function, address range, TTL and signal mapping into cells).
* `logic` – Logic blocks with dependency declarations, normal/fallback ASTs and target cell.
* `writes` – Modbus write targets with deadband, rate limit and priority options.
* `logging` / `policies` – Runtime logging setup and optional global policies (retry behaviour, watchdog, readback, etc.).

### Example snippet

```yaml
logic:
  - id: heater_control
    target: heater_command
    dependencies:
      - cell: temperature_c
        type: number
      - cell: heater_enabled
        type: bool
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

## Running

1. Build/install the binary:
   ```bash
   go build ./cmd/...
   ```
2. Start the processor with your configuration:
   ```bash
   ./modbus_processor --config path/to/config.yaml
   ```

The service logs with [zerolog](https://github.com/rs/zerolog) and can optionally stream logs to Loki when configured.

## Testing

Run the full test suite with:

```bash
go test ./...
```
