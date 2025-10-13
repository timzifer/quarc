package activity

import "time"

// Tracker captures runtime activity for heatmap visualisations.
//
// Components may choose to implement only the methods they need; callers must
// guard against a nil tracker. The interface is intentionally broad so the
// service can provide a single implementation without leaking internal types.
type Tracker interface {
	RecordCellRead(cellID, origin string, ts time.Time)
	RecordCellWrite(cellID, origin, kind string, ts time.Time)
	RecordLogic(blockID string, ts time.Time, success bool)
	RecordProgram(programID string, ts time.Time, success bool)
}
