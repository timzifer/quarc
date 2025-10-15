package readers

import (
	"time"

	"github.com/rs/zerolog"

	"github.com/timzifer/quarc/config"
	"github.com/timzifer/quarc/runtime/activity"
	"github.com/timzifer/quarc/runtime/state"
)

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
	Buffers      map[string]SignalBufferStatus
}

// SignalBufferStatus captures diagnostic information about a buffered signal.
type SignalBufferStatus struct {
	Buffered     int
	Dropped      uint64
	Overflow     bool
	Aggregations map[string]SignalAggregationStatus
}

type ReaderDependencies struct {
	Cells    state.CellStore
	Activity activity.Tracker
	Buffers  *SignalBufferStore
}

// ReaderFactory constructs a ReadGroup using the provided configuration and
// dependencies.
//
// Factories allow different protocol implementations to be wired into the
// service without coupling the scheduler to concrete types.
type ReaderFactory func(cfg config.ReadGroupConfig, deps ReaderDependencies) (ReadGroup, error)
