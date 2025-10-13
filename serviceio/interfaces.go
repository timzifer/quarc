package serviceio

import (
	readers "github.com/timzifer/modbus_processor/runtime/readers"
	state "github.com/timzifer/modbus_processor/runtime/state"
	writers "github.com/timzifer/modbus_processor/runtime/writers"
)

// Deprecated: use runtime/state.Cell.
type Cell = state.Cell

// Deprecated: use runtime/state.CellStore.
type CellStore = state.CellStore

// Deprecated: use runtime/readers.ReadGroup.
type ReadGroup = readers.ReadGroup

// Deprecated: use runtime/readers.ReadGroupStatus.
type ReadGroupStatus = readers.ReadGroupStatus

// Deprecated: use runtime/readers.ReaderDependencies.
type ReaderDependencies = readers.ReaderDependencies

// Deprecated: use runtime/readers.ReaderFactory.
type ReaderFactory = readers.ReaderFactory

// Deprecated: use runtime/writers.Writer.
type Writer = writers.Writer

// Deprecated: use runtime/writers.WriteTargetStatus.
type WriteTargetStatus = writers.WriteTargetStatus

// Deprecated: use runtime/writers.WriterDependencies.
type WriterDependencies = writers.WriterDependencies

// Deprecated: use runtime/writers.WriterFactory.
type WriterFactory = writers.WriterFactory
