# Runtime Overview

QUARC follows a deterministic execution cycle built around four isolated stages that run on a fixed schedule: **READ**, **PROGRAM**, **EVAL**, and **COMMIT**. Each cycle operates on typed in-memory cells and records diagnostics when any phase fails. The runtime keeps the cycle duration monotonic (default `500ms`) and publishes metrics such as cycle time, read/eval/write error counts, and total cycles executed.

## Cell model

Cells store typed values (`number`, `bool`, `string`) along with a validity flag. Diagnostics (error code, message, timestamp) are attached whenever reads, evaluations, or writes fail. Logic execution only considers the validity flag, allowing diagnostics to provide context without disrupting downstream calculations.

## Phases

### Read phase

Driver reads are grouped by source, function, and range. Each group tracks its own TTL; expired groups are re-polled in the current cycle. Drivers translate protocol payloads into typed cell values using the configured endianness, signedness, and scaling. Read failures invalidate only the affected cells—other stages continue to run.

### Program phase

Reusable control modules run between the read and logic phases. They receive the current cell snapshot and cycle timing information, emit derived signals, and may report diagnostics if required outputs are missing or invalid. Programs are instantiated from the registry based on their configured type.

### Eval phase

Expression blocks execute in a deterministic order derived from their dependency graph (topological sort with configuration order as a tie breaker). Each block sees a consistent snapshot of the cell state. Optional `valid` and `quality` expressions inspect the computed result and any evaluation errors to determine validity, diagnostics, and quality for the target cell.

### Commit phase

Only cells whose values changed beyond configured deadbands or rate limits are written back to drivers. Writes are ordered by priority and payload marshalling performs the inverse translation (typed value → protocol payload). Write failures raise diagnostics but do not abort the cycle.

## Runtime packages

Interfaces used by the scheduler live under the `runtime` namespace:

* `runtime/state` – cell abstractions (`Cell`, `CellStore`).
* `runtime/readers` – reader interfaces, status structures, and factories.
* `runtime/writers` – writer interfaces, status structures, and factories.

The legacy `serviceio` package still exports deprecated type aliases for backwards compatibility but will be removed in a future release. New integrations should import the runtime packages directly.

## Runtime overrides

The service exposes helper methods that make the controller easier to embed into supervisory applications:

* `SetCellValue(id, value)` – overrides a cell with type-checked data. The change appears on the next publish by any connected drivers.
* `InvalidateCell(id, code, message)` – marks a cell invalid while attaching diagnostics available to logic blocks and snapshots.
* `InspectCell(id)` – returns a structured view of the current value, validity, diagnostic metadata, and last update timestamp.

These helpers enable HMIs to implement manual "force" capabilities or provide fallback values during commissioning, mirroring typical soft-PLC behaviour.
