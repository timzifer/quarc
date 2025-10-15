# Service I/O Interfaces (Deprecated)

The `serviceio` package now only provides type aliases that point to the
runtime packages introduced in `runtime/state`, `runtime/readers`, and
`runtime/writers`. The aliases exist to support downstream consumers during the
migration window and will be removed in a future release.

New development should import the runtime packages directly:

* `runtime/state` provides `Cell` and `CellStore` implementations.
* `runtime/readers` defines `ReadGroup` and the associated factory helpers.
* `runtime/writers` defines `Writer` types and dependencies.

## Migration Guide

```go
import (
    "github.com/timzifer/quarc/runtime/readers"
    "github.com/timzifer/quarc/runtime/state"
)

group, err := myReaderFactory(cfg, readers.ReaderDependencies{Cells: store})
if err != nil {
    return err
}
cell, err := store.Get("sensor") // state.Cell implementation
```

Existing code that continues to import `serviceio` will keep compiling, but
please update imports to the runtime packages to benefit from future
improvements.
