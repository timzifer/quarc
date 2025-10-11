package serviceio

import (
	"time"

	"github.com/rs/zerolog"
	"github.com/timzifer/modbus_processor/config"
)

// Cell models a single logical value managed by the service runtime.
//
// Implementations are expected to persist the supplied values, track their
// validity and timestamp, and expose the current value to other parts of the
// system. Implementations must be safe for concurrent use by multiple
// goroutines because the service routinely updates and inspects cell state from
// different workers.
type Cell interface {
	Config() config.CellConfig
	SetValue(value interface{}, ts time.Time, quality *float64) error
	MarkInvalid(ts time.Time, code, message string)
	CurrentValue() (interface{}, bool)
}

// CellStore provides access to cells by their identifier.
//
// Implementations should return an error when the requested cell is unknown and
// may implement additional caching or synchronization depending on the backing
// storage.
type CellStore interface {
	Get(id string) (Cell, error)
}

type ReadGroupStatus struct {
	ID           string
	Function     string
	Start        uint16
	Length       uint16
	Disabled     bool
	NextRun      time.Time
	LastRun      time.Time
	LastDuration time.Duration
	Source       config.ModuleReference
}

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

// ReadGroup encapsulates a scheduled polling task for a contiguous Modbus data
// block.
//
// A read group is responsible for determining when it is due for execution,
// performing the actual read, and exposing diagnostic status information to the
// rest of the service.
type ReadGroup interface {
	ID() string
	Due(now time.Time) bool
	Perform(now time.Time, logger zerolog.Logger) int
	SetDisabled(disabled bool)
	Status() ReadGroupStatus
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

type ReaderDependencies struct {
	Cells CellStore
}

type WriterDependencies struct {
	Cells CellStore
}

// ReaderFactory constructs a ReadGroup using the provided configuration and
// dependencies.
//
// Factories allow different protocol implementations to be wired into the
// service without coupling the scheduler to concrete types.
type ReaderFactory func(cfg config.ReadGroupConfig, deps ReaderDependencies) (ReadGroup, error)

// WriterFactory constructs a Writer using the provided configuration and
// dependencies.
//
// Similar to ReaderFactory, the writer factory pattern enables pluggable write
// backends while keeping the service core agnostic of protocol details.
type WriterFactory func(cfg config.WriteTargetConfig, deps WriterDependencies) (Writer, error)
