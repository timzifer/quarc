package service

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/timzifer/quarc/config"
)

func TestNewHeatmapSettingsOverrides(t *testing.T) {
	settings := newHeatmapSettings(config.HeatmapConfig{
		Cooldown: config.HeatmapCooldownConfig{
			Cells:    3,
			Logic:    4,
			Programs: 5,
		},
		Colors: config.HeatmapColorConfig{
			Background: "#111111",
			Border:     "#222222",
			Logic:      "#333333",
			Program:    "#444444",
			Read:       "#555555",
			Stale:      "#666666",
			Write:      "#777777",
		},
	})

	cfg := settings.config
	if cfg.Cooldown.Cells != 3 || cfg.Cooldown.Logic != 4 || cfg.Cooldown.Programs != 5 {
		t.Fatalf("unexpected cooldown config: %+v", cfg.Cooldown)
	}
	if cfg.Colors.Background != "#111111" || cfg.Colors.Border != "#222222" {
		t.Fatalf("unexpected color overrides: %+v", cfg.Colors)
	}
	if cfg.Colors.Read != "#555555" || cfg.Colors.Write != "#777777" {
		t.Fatalf("unexpected read/write colors: %+v", cfg.Colors)
	}
}

func TestActivityTrackerRecordCellAccess(t *testing.T) {
	var cycle atomic.Uint64
	cycle.Store(42)
	tracker := newActivityTracker(&cycle, defaultHeatmapSettings())

	tsRead := time.Unix(10, 0)
	tracker.RecordCellRead("cell-a", "reader", tsRead)

	tsWrite := tsRead.Add(time.Second)
	tracker.RecordCellWrite("cell-a", "writer", "manual", tsWrite)

	tracker.mu.RLock()
	entry := tracker.cells["cell-a"]
	tracker.mu.RUnlock()

	if entry == nil {
		t.Fatalf("expected cell entry to exist")
	}
	if entry.readCount != 1 || entry.writeCount != 1 {
		t.Fatalf("unexpected read/write counts: %#v", entry)
	}
	if entry.lastReadOrigin != "reader" || entry.lastWriteOrigin != "writer" {
		t.Fatalf("unexpected origins: %#v", entry)
	}
	if entry.lastAccessKind != "write" {
		t.Fatalf("expected last access kind write, got %q", entry.lastAccessKind)
	}
	if entry.lastAccessCycle != 42 {
		t.Fatalf("expected last access cycle 42, got %d", entry.lastAccessCycle)
	}
	if !entry.lastWriteTime.Equal(tsWrite) {
		t.Fatalf("unexpected last write time: %v", entry.lastWriteTime)
	}
}

func TestActivityTrackerRecordLogicAndProgram(t *testing.T) {
	var cycle atomic.Uint64
	cycle.Store(7)
	tracker := newActivityTracker(&cycle, defaultHeatmapSettings())

	ts := time.Unix(25, 0)
	tracker.RecordLogic("logic-1", ts, true)
	tracker.RecordLogic("logic-1", ts.Add(time.Second), false)
	tracker.RecordProgram("program-1", ts, false)

	tracker.mu.RLock()
	logicEntry := tracker.logic["logic-1"]
	programEntry := tracker.programs["program-1"]
	tracker.mu.RUnlock()

	if logicEntry == nil || logicEntry.count != 2 || logicEntry.errors != 1 {
		t.Fatalf("unexpected logic entry: %#v", logicEntry)
	}
	if programEntry == nil || programEntry.count != 1 || programEntry.errors != 1 {
		t.Fatalf("unexpected program entry: %#v", programEntry)
	}
	if logicEntry.lastCycle != 7 || programEntry.lastCycle != 7 {
		t.Fatalf("expected last cycle 7 for both entries")
	}
}

func TestActivityTrackerSnapshotIncludesState(t *testing.T) {
	var cycle atomic.Uint64
	cycle.Store(3)
	tracker := newActivityTracker(&cycle, newHeatmapSettings(config.HeatmapConfig{
		Cooldown: config.HeatmapCooldownConfig{Cells: 9},
	}))

	ts := time.Unix(100, 0)
	tracker.RecordCellRead("cell-1", "reader", ts)
	tracker.RecordLogic("logic-1", ts, true)
	tracker.RecordProgram("program-1", ts, false)

	cells := []CellState{{ID: "cell-1"}}
	logicBlocks := []logicBlockState{{ID: "logic-1", Target: "cell-1", Source: config.ModuleReference{Name: "logic"}}}
	programs := []programState{{ID: "program-1", Type: "custom", Outputs: []string{"cell-1"}, Source: config.ModuleReference{Name: "prog"}}}

	snapshot := tracker.Snapshot(cells, logicBlocks, programs)

	if len(snapshot.Cells) != 1 || len(snapshot.Logic) != 1 || len(snapshot.Programs) != 1 {
		t.Fatalf("unexpected snapshot sizes: %#v", snapshot)
	}
	if snapshot.Cells[0].LastAccessKind != "read" || snapshot.Cells[0].ReadCount != 1 {
		t.Fatalf("unexpected cell snapshot: %#v", snapshot.Cells[0])
	}
	if snapshot.Logic[0].Calls != 1 || snapshot.Logic[0].Errors != 0 {
		t.Fatalf("unexpected logic snapshot: %#v", snapshot.Logic[0])
	}
	if snapshot.Programs[0].Errors != 1 {
		t.Fatalf("expected program errors to be 1: %#v", snapshot.Programs[0])
	}
	if snapshot.Config.Cooldown.Cells != 9 {
		t.Fatalf("expected cooldown override to propagate, got %d", snapshot.Config.Cooldown.Cells)
	}
}

func TestActivityTrackerSnapshotNilTracker(t *testing.T) {
	var tracker *activityTracker
	cells := []CellState{{ID: "cell"}}
	logicBlocks := []logicBlockState{{ID: "logic"}}
	programs := []programState{{ID: "program"}}

	snapshot := tracker.Snapshot(cells, logicBlocks, programs)
	if len(snapshot.Cells) != 0 || len(snapshot.Logic) != 0 || len(snapshot.Programs) != 0 {
		t.Fatalf("expected snapshot to be empty for nil tracker: %#v", snapshot)
	}
	if snapshot.Config.Cooldown.Cells == 0 {
		t.Fatalf("expected default cooldown to be non-zero")
	}
}
