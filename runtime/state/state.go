package state

import (
	"time"

	"github.com/timzifer/quarc/config"
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
