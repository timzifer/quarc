package service

import (
	"testing"
	"time"

	"github.com/timzifer/quarc/config"
)

func TestSnapshotSwitchIsolation(t *testing.T) {
	store, err := newCellStore([]config.CellConfig{{ID: "value", Type: config.ValueKindInteger}})
	if err != nil {
		t.Fatalf("newCellStore: %v", err)
	}
	cell, err := store.mustGet("value")
	if err != nil {
		t.Fatalf("mustGet: %v", err)
	}
	if err := cell.setValue(1, time.Now(), nil); err != nil {
		t.Fatalf("setValue: %v", err)
	}

	sw := newSnapshotSwitch(1)

	snap1 := sw.Capture(store)
	if got, ok := snap1["value"].Value.(int64); !ok || got != 1 {
		t.Fatalf("expected snapshot 1 to contain value 1, got %v", snap1["value"].Value)
	}

	if err := cell.setValue(2, time.Now(), nil); err != nil {
		t.Fatalf("setValue: %v", err)
	}

	snap2 := sw.Capture(store)
	if got, ok := snap2["value"].Value.(int64); !ok || got != 2 {
		t.Fatalf("expected snapshot 2 to contain value 2, got %v", snap2["value"].Value)
	}

	if snap1["value"] == snap2["value"] {
		t.Fatalf("expected distinct buffers for consecutive captures")
	}

	if got, ok := snap1["value"].Value.(int64); !ok || got != 1 {
		t.Fatalf("expected first snapshot to remain unchanged, got %v", snap1["value"].Value)
	}
}

func TestSnapshotSwitchReusesBuffers(t *testing.T) {
	store, err := newCellStore([]config.CellConfig{{ID: "value", Type: config.ValueKindInteger}})
	if err != nil {
		t.Fatalf("newCellStore: %v", err)
	}
	cell, err := store.mustGet("value")
	if err != nil {
		t.Fatalf("mustGet: %v", err)
	}

	if err := cell.setValue(1, time.Now(), nil); err != nil {
		t.Fatalf("setValue: %v", err)
	}

	sw := newSnapshotSwitch(1)
	snap1 := sw.Capture(store)

	if err := cell.setValue(2, time.Now(), nil); err != nil {
		t.Fatalf("setValue: %v", err)
	}
	snap2 := sw.Capture(store)

	if err := cell.setValue(3, time.Now(), nil); err != nil {
		t.Fatalf("setValue: %v", err)
	}
	snap3 := sw.Capture(store)

	if snap3["value"] != snap1["value"] {
		t.Fatalf("expected buffer reuse after two captures")
	}
	if got, ok := snap3["value"].Value.(int64); !ok || got != 3 {
		t.Fatalf("expected latest value 3, got %v", snap3["value"].Value)
	}

	if got, ok := snap2["value"].Value.(int64); !ok || got != 2 {
		t.Fatalf("expected middle snapshot to remain at 2, got %v", snap2["value"].Value)
	}
}
