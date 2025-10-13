package writers

import (
	"time"

	"github.com/rs/zerolog"

	"github.com/timzifer/modbus_processor/config"
	"github.com/timzifer/modbus_processor/runtime/state"
)

// Writer represents a write target that periodically flushes values to a remote
// Modbus endpoint.
//
// Writers are invoked by the scheduler on a regular cadence. Implementations
// should be resilient against transient failures and must handle repeated calls
// to Commit as well as Close.
type Writer interface {
	ID() string
	Commit(now time.Time, logger zerolog.Logger) int
	SetDisabled(disabled bool)
	Status() WriteTargetStatus
	Close()
}

type WriteTargetStatus struct {
	ID           string
	Cell         string
	Function     string
	Address      uint16
	Disabled     bool
	LastWrite    time.Time
	LastAttempt  time.Time
	LastDuration time.Duration
	Source       config.ModuleReference
}

type WriterDependencies struct {
	Cells state.CellStore
}

// WriterFactory constructs a Writer using the provided configuration and
// dependencies.
//
// Similar to ReaderFactory, the writer factory pattern enables pluggable write
// backends while keeping the service core agnostic of protocol details.
type WriterFactory func(cfg config.WriteTargetConfig, deps WriterDependencies) (Writer, error)
