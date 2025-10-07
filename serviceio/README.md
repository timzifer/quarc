# Service I/O Interfaces

The `serviceio` package defines the interfaces that connect the scheduler with
cell storage, read groups, and write targets. These abstractions allow the core
service to orchestrate Modbus communication without depending on concrete
protocol implementations.

## Interfaces

### `Cell`
Maintains the current value, validity, and quality metadata for a logical signal.
Implementations must be concurrency-safe, because both readers and writers may
update cell state simultaneously.

### `CellStore`
Lookup facility used by the scheduler to resolve cell identifiers. Returning an
error for unknown cells keeps misconfigured modules from silently failing.

### `ReadGroup`
Represents a scheduled poll of a contiguous Modbus address range. Read groups
decide when they are due, execute reads, and expose diagnostic status.

### `Writer`
Flushes pending values back to Modbus targets. Writers are called on a fixed
schedule and should handle transient failures by retrying or reporting status.

### Factories
`ReaderFactory` and `WriterFactory` receive configuration objects along with the
dependencies they need to construct concrete instances.

## Usage Example

```go
readGroup, err := myReaderFactory(cfg, serviceio.ReaderDependencies{Cells: store})
if err != nil {
    return err
}
if readGroup.Due(time.Now()) {
    readGroup.Perform(time.Now(), logger)
}
```

## Implementation Notes

* Keep `Due` and `Commit` implementations lightweight; they run frequently on
  the scheduler's hot path.
* Always update status metadata (`NextRun`, `LastDuration`, etc.) so operators
  can diagnose communication issues.

## Common Pitfalls

* Failing to mark a read group as disabled when configuration requires it will
  cause unnecessary Modbus traffic.
* `Cell` implementations that don't deep copy mutable values risk data races
  when referenced from multiple goroutines.
