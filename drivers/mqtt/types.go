package mqtt

import "time"

// CellUpdate describes a value observed in a QUARC cell.
type CellUpdate struct {
	Value     any
	Timestamp time.Time
}
