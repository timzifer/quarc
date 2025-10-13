package service

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/timzifer/modbus_processor/config"
	"github.com/timzifer/modbus_processor/runtime/activity"
)

const (
	defaultCooldownCells    = 12
	defaultCooldownLogic    = 8
	defaultCooldownPrograms = 8
)

var _ activity.Tracker = (*activityTracker)(nil)

type heatmapColors struct {
	Background string `json:"background"`
	Border     string `json:"border"`
	Logic      string `json:"logic"`
	Program    string `json:"program"`
	Read       string `json:"read"`
	Stale      string `json:"stale"`
	Write      string `json:"write"`
}

type heatmapCooldown struct {
	Cells    uint64 `json:"cells"`
	Logic    uint64 `json:"logic"`
	Programs uint64 `json:"programs"`
}

type heatmapConfigPayload struct {
	Colors   heatmapColors   `json:"colors"`
	Cooldown heatmapCooldown `json:"cooldown"`
}

type heatmapSettings struct {
	config heatmapConfigPayload
}

func defaultHeatmapSettings() heatmapSettings {
	return heatmapSettings{config: heatmapConfigPayload{
		Colors: heatmapColors{
			Background: "#f5f5f5",
			Border:     "#424242",
			Logic:      "#29b6f6",
			Program:    "#ab47bc",
			Read:       "#4caf50",
			Stale:      "#bdbdbd",
			Write:      "#ef5350",
		},
		Cooldown: heatmapCooldown{
			Cells:    defaultCooldownCells,
			Logic:    defaultCooldownLogic,
			Programs: defaultCooldownPrograms,
		},
	}}
}

func newHeatmapSettings(cfg config.HeatmapConfig) heatmapSettings {
	settings := defaultHeatmapSettings()
	if cfg.Cooldown.Cells > 0 {
		settings.config.Cooldown.Cells = uint64(cfg.Cooldown.Cells)
	}
	if cfg.Cooldown.Logic > 0 {
		settings.config.Cooldown.Logic = uint64(cfg.Cooldown.Logic)
	}
	if cfg.Cooldown.Programs > 0 {
		settings.config.Cooldown.Programs = uint64(cfg.Cooldown.Programs)
	}
	override := cfg.Colors
	if override.Background != "" {
		settings.config.Colors.Background = override.Background
	}
	if override.Border != "" {
		settings.config.Colors.Border = override.Border
	}
	if override.Logic != "" {
		settings.config.Colors.Logic = override.Logic
	}
	if override.Program != "" {
		settings.config.Colors.Program = override.Program
	}
	if override.Read != "" {
		settings.config.Colors.Read = override.Read
	}
	if override.Stale != "" {
		settings.config.Colors.Stale = override.Stale
	}
	if override.Write != "" {
		settings.config.Colors.Write = override.Write
	}
	return settings
}

type cellActivity struct {
	lastReadTime    time.Time
	lastReadCycle   uint64
	readCount       uint64
	lastReadOrigin  string
	lastWriteTime   time.Time
	lastWriteCycle  uint64
	writeCount      uint64
	lastWriteOrigin string
	lastWriteKind   string
	lastAccessKind  string
	lastAccessTime  time.Time
	lastAccessCycle uint64
}

type unitActivity struct {
	lastTime  time.Time
	lastCycle uint64
	count     uint64
	errors    uint64
}

type activityTracker struct {
	cycle    *atomic.Uint64
	mu       sync.RWMutex
	cells    map[string]*cellActivity
	logic    map[string]*unitActivity
	programs map[string]*unitActivity
	settings heatmapSettings
}

func newActivityTracker(counter *atomic.Uint64, settings heatmapSettings) *activityTracker {
	return &activityTracker{
		cycle:    counter,
		cells:    make(map[string]*cellActivity),
		logic:    make(map[string]*unitActivity),
		programs: make(map[string]*unitActivity),
		settings: settings,
	}
}

func (t *activityTracker) currentCycle() uint64 {
	if t == nil || t.cycle == nil {
		return 0
	}
	return t.cycle.Load()
}

func (t *activityTracker) RecordCellRead(cellID, origin string, ts time.Time) {
	if t == nil || cellID == "" {
		return
	}
	cycle := t.currentCycle()
	t.mu.Lock()
	entry, ok := t.cells[cellID]
	if !ok {
		entry = &cellActivity{}
		t.cells[cellID] = entry
	}
	entry.readCount++
	entry.lastReadCycle = cycle
	entry.lastReadTime = ts
	entry.lastReadOrigin = origin
	entry.lastAccessKind = "read"
	entry.lastAccessCycle = cycle
	entry.lastAccessTime = ts
	t.mu.Unlock()
}

func (t *activityTracker) RecordCellWrite(cellID, origin, kind string, ts time.Time) {
	if t == nil || cellID == "" {
		return
	}
	cycle := t.currentCycle()
	t.mu.Lock()
	entry, ok := t.cells[cellID]
	if !ok {
		entry = &cellActivity{}
		t.cells[cellID] = entry
	}
	entry.writeCount++
	entry.lastWriteCycle = cycle
	entry.lastWriteTime = ts
	entry.lastWriteOrigin = origin
	entry.lastWriteKind = kind
	entry.lastAccessKind = "write"
	entry.lastAccessCycle = cycle
	entry.lastAccessTime = ts
	t.mu.Unlock()
}

func (t *activityTracker) recordUnitActivity(store map[string]*unitActivity, id string, ts time.Time, success bool) {
	if t == nil || id == "" {
		return
	}
	cycle := t.currentCycle()
	entry, ok := store[id]
	if !ok {
		entry = &unitActivity{}
		store[id] = entry
	}
	entry.count++
	entry.lastCycle = cycle
	entry.lastTime = ts
	if !success {
		entry.errors++
	}
}

func (t *activityTracker) RecordLogic(blockID string, ts time.Time, success bool) {
	if t == nil || blockID == "" {
		return
	}
	t.mu.Lock()
	t.recordUnitActivity(t.logic, blockID, ts, success)
	t.mu.Unlock()
}

func (t *activityTracker) RecordProgram(programID string, ts time.Time, success bool) {
	if t == nil || programID == "" {
		return
	}
	t.mu.Lock()
	t.recordUnitActivity(t.programs, programID, ts, success)
	t.mu.Unlock()
}

type heatmapCellTile struct {
	ID              string     `json:"id"`
	LastAccessKind  string     `json:"last_access_kind,omitempty"`
	LastAccessCycle uint64     `json:"last_access_cycle,omitempty"`
	LastAccessTime  *time.Time `json:"last_access_time,omitempty"`
	LastReadCycle   uint64     `json:"last_read_cycle,omitempty"`
	LastReadTime    *time.Time `json:"last_read_time,omitempty"`
	ReadCount       uint64     `json:"read_count,omitempty"`
	LastReadOrigin  string     `json:"last_read_origin,omitempty"`
	LastWriteCycle  uint64     `json:"last_write_cycle,omitempty"`
	LastWriteTime   *time.Time `json:"last_write_time,omitempty"`
	WriteCount      uint64     `json:"write_count,omitempty"`
	LastWriteKind   string     `json:"last_write_kind,omitempty"`
	LastWriteOrigin string     `json:"last_write_origin,omitempty"`
}

type heatmapLogicTile struct {
	ID        string                 `json:"id"`
	Target    string                 `json:"target,omitempty"`
	Calls     uint64                 `json:"calls,omitempty"`
	Errors    uint64                 `json:"errors,omitempty"`
	LastCycle uint64                 `json:"last_cycle,omitempty"`
	LastTime  *time.Time             `json:"last_time,omitempty"`
	Source    config.ModuleReference `json:"source,omitempty"`
}

type heatmapProgramTile struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type,omitempty"`
	Outputs   []string               `json:"outputs,omitempty"`
	Calls     uint64                 `json:"calls,omitempty"`
	Errors    uint64                 `json:"errors,omitempty"`
	LastCycle uint64                 `json:"last_cycle,omitempty"`
	LastTime  *time.Time             `json:"last_time,omitempty"`
	Source    config.ModuleReference `json:"source,omitempty"`
}

type heatmapState struct {
	Config   heatmapConfigPayload `json:"config"`
	Cells    []heatmapCellTile    `json:"cells"`
	Logic    []heatmapLogicTile   `json:"logic"`
	Programs []heatmapProgramTile `json:"programs"`
}

func optionalTime(ts time.Time) *time.Time {
	if ts.IsZero() {
		return nil
	}
	copy := ts
	return &copy
}

func (t *activityTracker) Snapshot(cells []CellState, logic []logicBlockState, programs []programState) heatmapState {
	settings := defaultHeatmapSettings()
	tracker := t
	if tracker == nil {
		return heatmapState{Config: settings.config}
	}

	tracker.mu.RLock()
	defer tracker.mu.RUnlock()

	cellTiles := make([]heatmapCellTile, 0, len(cells))
	for _, cell := range cells {
		entry := tracker.cells[cell.ID]
		tile := heatmapCellTile{ID: cell.ID}
		if entry != nil {
			tile.LastAccessKind = entry.lastAccessKind
			tile.LastAccessCycle = entry.lastAccessCycle
			tile.LastAccessTime = optionalTime(entry.lastAccessTime)
			tile.LastReadCycle = entry.lastReadCycle
			tile.LastReadTime = optionalTime(entry.lastReadTime)
			tile.ReadCount = entry.readCount
			tile.LastReadOrigin = entry.lastReadOrigin
			tile.LastWriteCycle = entry.lastWriteCycle
			tile.LastWriteTime = optionalTime(entry.lastWriteTime)
			tile.WriteCount = entry.writeCount
			tile.LastWriteOrigin = entry.lastWriteOrigin
			tile.LastWriteKind = entry.lastWriteKind
		}
		cellTiles = append(cellTiles, tile)
	}

	logicTiles := make([]heatmapLogicTile, 0, len(logic))
	for _, block := range logic {
		entry := tracker.logic[block.ID]
		tile := heatmapLogicTile{ID: block.ID, Target: block.Target, Source: block.Source}
		if entry != nil {
			tile.Calls = entry.count
			tile.Errors = entry.errors
			tile.LastCycle = entry.lastCycle
			tile.LastTime = optionalTime(entry.lastTime)
		}
		logicTiles = append(logicTiles, tile)
	}

	programTiles := make([]heatmapProgramTile, 0, len(programs))
	for _, program := range programs {
		entry := tracker.programs[program.ID]
		outputs := append([]string(nil), program.Outputs...)
		tile := heatmapProgramTile{ID: program.ID, Type: program.Type, Outputs: outputs, Source: program.Source}
		if entry != nil {
			tile.Calls = entry.count
			tile.Errors = entry.errors
			tile.LastCycle = entry.lastCycle
			tile.LastTime = optionalTime(entry.lastTime)
		}
		programTiles = append(programTiles, tile)
	}

	return heatmapState{
		Config:   tracker.settings.config,
		Cells:    cellTiles,
		Logic:    logicTiles,
		Programs: programTiles,
	}
}

type programState struct {
	ID      string
	Type    string
	Outputs []string
	Source  config.ModuleReference
}

func (s *Service) heatmapSnapshot(cells []CellState, logic []logicBlockState, programs []programState) heatmapState {
	if s == nil || s.activity == nil {
		settings := defaultHeatmapSettings()
		return heatmapState{Config: settings.config}
	}
	return s.activity.Snapshot(cells, logic, programs)
}
