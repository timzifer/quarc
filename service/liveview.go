package service

import (
	"bytes"
	"context"
	"encoding/json"
	"html/template"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/timzifer/quarc/config"
	"github.com/timzifer/quarc/runtime/readers"
	"github.com/timzifer/quarc/runtime/writers"
)

type liveViewServer struct {
	logger  zerolog.Logger
	service *Service
	server  *http.Server
	ln      net.Listener
}

type liveStateResponse struct {
	Cells   []liveCell        `json:"cells"`
	Control controlStatus     `json:"control"`
	Metrics metrics           `json:"metrics"`
	Logic   []liveLogicBlock  `json:"logic"`
	System  systemInfo        `json:"system"`
	Reads   []liveReadGroup   `json:"reads"`
	Writes  []liveWriteTarget `json:"writes"`
	Heatmap heatmapState      `json:"heatmap"`
}

type liveCell struct {
	ID          string                 `json:"id"`
	Package     string                 `json:"package,omitempty"`
	LocalID     string                 `json:"local_id,omitempty"`
	Name        string                 `json:"name,omitempty"`
	Description string                 `json:"description,omitempty"`
	Kind        string                 `json:"kind"`
	Value       interface{}            `json:"value"`
	Valid       bool                   `json:"valid"`
	Quality     *float64               `json:"quality"`
	Diagnosis   *CellDiagnosis         `json:"diagnosis,omitempty"`
	UpdatedAt   *time.Time             `json:"updated_at"`
	Source      config.ModuleReference `json:"source,omitempty"`
}

type liveLogicBlock struct {
	ID      string                 `json:"id"`
	Package string                 `json:"package,omitempty"`
	LocalID string                 `json:"local_id,omitempty"`
	Target  string                 `json:"target"`
	Calls   uint64                 `json:"calls"`
	Skipped uint64                 `json:"skipped"`
	AvgMS   float64                `json:"avg_ms"`
	LastMS  float64                `json:"last_ms"`
	Source  config.ModuleReference `json:"source,omitempty"`
}

type systemInfo struct {
	Goroutines int          `json:"goroutines"`
	Workers    []liveWorker `json:"workers"`
}

type liveWorker struct {
	Kind       string `json:"kind"`
	Configured int    `json:"configured"`
	Active     int    `json:"active"`
}

type liveReadGroup struct {
	ID             string                      `json:"id"`
	Package        string                      `json:"package,omitempty"`
	LocalID        string                      `json:"local_id,omitempty"`
	Driver         string                      `json:"driver,omitempty"`
	Disabled       bool                        `json:"disabled"`
	NextRun        *time.Time                  `json:"next_run,omitempty"`
	LastRun        *time.Time                  `json:"last_run,omitempty"`
	LastDurationMS float64                     `json:"last_duration_ms"`
	Source         config.ModuleReference      `json:"source,omitempty"`
	Buffers        map[string]liveSignalBuffer `json:"buffers,omitempty"`
	Metadata       map[string]interface{}      `json:"metadata,omitempty"`
}

type liveSignalAggregate struct {
	Value     interface{} `json:"value,omitempty"`
	Timestamp *time.Time  `json:"timestamp,omitempty"`
	Quality   *float64    `json:"quality,omitempty"`
	Count     int         `json:"count"`
	Overflow  bool        `json:"overflow"`
}

type liveSignalBuffer struct {
	Buffered     int                                    `json:"buffered"`
	Dropped      uint64                                 `json:"dropped"`
	Overflow     bool                                   `json:"overflow"`
	Aggregations map[string]liveSignalAggregationStatus `json:"aggregations,omitempty"`
}

type liveSignalAggregationStatus struct {
	Aggregator  string               `json:"aggregator,omitempty"`
	QualityCell string               `json:"quality_cell,omitempty"`
	OnOverflow  string               `json:"on_overflow,omitempty"`
	Last        *liveSignalAggregate `json:"last,omitempty"`
	Error       string               `json:"error,omitempty"`
}

type liveWriteTarget struct {
	ID             string                 `json:"id"`
	Package        string                 `json:"package,omitempty"`
	LocalID        string                 `json:"local_id,omitempty"`
	Cell           string                 `json:"cell"`
	Function       string                 `json:"function"`
	Address        uint16                 `json:"address"`
	Disabled       bool                   `json:"disabled"`
	LastWrite      *time.Time             `json:"last_write,omitempty"`
	LastAttempt    *time.Time             `json:"last_attempt,omitempty"`
	LastDurationMS float64                `json:"last_duration_ms"`
	Source         config.ModuleReference `json:"source,omitempty"`
}

type cellUpdateRequest struct {
	Value   *json.RawMessage `json:"value"`
	Quality *json.RawMessage `json:"quality"`
	Valid   *bool            `json:"valid"`
}

type toggleRequest struct {
	Disabled *bool `json:"disabled"`
}

func durationToMillis(d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(d) / float64(time.Millisecond)
}

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func isJSONNull(raw *json.RawMessage) bool {
	if raw == nil {
		return true
	}
	trimmed := bytes.TrimSpace(*raw)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null"))
}

func splitIdentifier(id string, source config.ModuleReference) (string, string) {
	pkg := strings.TrimSpace(source.Package)
	local := id
	if pkg != "" {
		prefix := pkg + ":"
		if strings.HasPrefix(id, prefix) {
			trimmed := strings.TrimPrefix(id, prefix)
			if trimmed != "" {
				local = trimmed
			}
		}
		return pkg, local
	}
	if idx := strings.Index(id, ":"); idx >= 0 && idx < len(id)-1 {
		pkg = id[:idx]
		local = id[idx+1:]
	}
	return pkg, local
}

func lessByPackageAndLocal(apkg, alocal, bpkg, blocal string) bool {
	if apkg == bpkg {
		return alocal < blocal
	}
	return apkg < bpkg
}

func toLiveCell(state CellState) liveCell {
	pkg, local := splitIdentifier(state.ID, state.Source)
	return liveCell{
		ID:          state.ID,
		Package:     pkg,
		LocalID:     local,
		Name:        state.Name,
		Description: state.Description,
		Kind:        string(state.Kind),
		Value:       state.Value,
		Valid:       state.Valid,
		Quality:     state.Quality,
		Diagnosis:   state.Diagnosis,
		UpdatedAt:   state.UpdatedAt,
		Source:      state.Source,
	}
}

func toLiveSignalAggregate(agg readers.SignalAggregate) *liveSignalAggregate {
	if agg.Count == 0 && agg.Value == nil && agg.Timestamp.IsZero() && agg.Quality == nil && !agg.Overflow {
		return nil
	}
	return &liveSignalAggregate{
		Value:     agg.Value,
		Timestamp: timePtr(agg.Timestamp),
		Quality:   agg.Quality,
		Count:     agg.Count,
		Overflow:  agg.Overflow,
	}
}

func toLiveReadGroup(status readers.ReadGroupStatus) liveReadGroup {
	pkg, local := splitIdentifier(status.ID, status.Source)
	group := liveReadGroup{
		ID:             status.ID,
		Package:        pkg,
		LocalID:        local,
		Driver:         status.Driver,
		Disabled:       status.Disabled,
		NextRun:        timePtr(status.NextRun),
		LastRun:        timePtr(status.LastRun),
		LastDurationMS: durationToMillis(status.LastDuration),
		Source:         status.Source,
	}
	if len(status.Metadata) > 0 {
		group.Metadata = status.Metadata
	}
	if len(status.Buffers) > 0 {
		buffers := make(map[string]liveSignalBuffer, len(status.Buffers))
		for signalID, buf := range status.Buffers {
			aggs := make(map[string]liveSignalAggregationStatus, len(buf.Aggregations))
			for cellID, agg := range buf.Aggregations {
				aggs[cellID] = liveSignalAggregationStatus{
					Aggregator:  string(agg.Aggregator),
					QualityCell: agg.QualityCell,
					OnOverflow:  agg.OnOverflow,
					Last:        toLiveSignalAggregate(agg.LastAggregate),
					Error:       agg.Error,
				}
			}
			buffers[signalID] = liveSignalBuffer{
				Buffered:     buf.Buffered,
				Dropped:      buf.Dropped,
				Overflow:     buf.Overflow,
				Aggregations: aggs,
			}
		}
		group.Buffers = buffers
	}
	return group
}

func toLiveWriteTarget(status writers.WriteTargetStatus) liveWriteTarget {
	pkg, local := splitIdentifier(status.ID, status.Source)
	return liveWriteTarget{
		ID:             status.ID,
		Package:        pkg,
		LocalID:        local,
		Cell:           status.Cell,
		Function:       status.Function,
		Address:        status.Address,
		Disabled:       status.Disabled,
		LastWrite:      timePtr(status.LastWrite),
		LastAttempt:    timePtr(status.LastAttempt),
		LastDurationMS: durationToMillis(status.LastDuration),
		Source:         status.Source,
	}
}

type controlRequest struct {
	Action     string `json:"action"`
	DurationMS *int64 `json:"duration_ms,omitempty"`
}

func newLiveViewServer(listen string, svc *Service, logger zerolog.Logger) (*liveViewServer, error) {
	mux := http.NewServeMux()
	server := &liveViewServer{logger: logger, service: svc}
	mux.HandleFunc("/", server.handleIndex)
	mux.HandleFunc("/api/state", server.handleState)
	mux.HandleFunc("/api/control", server.handleControl)
	mux.HandleFunc("/api/cells/", server.handleCellUpdate)
	mux.HandleFunc("/api/reads/", server.handleReadControl)
	mux.HandleFunc("/api/writes/", server.handleWriteControl)

	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}
	srv := &http.Server{Handler: mux}
	server.server = srv
	server.ln = ln

	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("live view server stopped")
		}
	}()

	logger.Info().Str("listen", ln.Addr().String()).Msg("live view started")
	return server, nil
}

func (s *liveViewServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := liveViewTemplate.Execute(w, nil); err != nil {
		s.logger.Error().Err(err).Msg("render live view page")
	}
}

func (s *liveViewServer) handleState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	states := s.service.cells.states()
	cells := make([]liveCell, 0, len(states))
	for _, state := range states {
		cells = append(cells, toLiveCell(state))
	}
	sort.Slice(cells, func(i, j int) bool {
		return lessByPackageAndLocal(cells[i].Package, cells[i].LocalID, cells[j].Package, cells[j].LocalID)
	})
	logicStates := s.service.LogicStates()
	logic := make([]liveLogicBlock, 0, len(logicStates))
	for _, entry := range logicStates {
		pkg, local := splitIdentifier(entry.ID, entry.Source)
		logic = append(logic, liveLogicBlock{
			ID:      entry.ID,
			Package: pkg,
			LocalID: local,
			Target:  entry.Target,
			Calls:   entry.Metrics.Calls,
			Skipped: entry.Metrics.Skipped,
			AvgMS:   durationToMillis(entry.Metrics.Average),
			LastMS:  durationToMillis(entry.Metrics.Last),
			Source:  entry.Source,
		})
	}
	sort.Slice(logic, func(i, j int) bool {
		return lessByPackageAndLocal(logic[i].Package, logic[i].LocalID, logic[j].Package, logic[j].LocalID)
	})
	readStatuses := s.service.ReadStatuses()
	reads := make([]liveReadGroup, 0, len(readStatuses))
	for _, status := range readStatuses {
		reads = append(reads, toLiveReadGroup(status))
	}
	sort.Slice(reads, func(i, j int) bool {
		return lessByPackageAndLocal(reads[i].Package, reads[i].LocalID, reads[j].Package, reads[j].LocalID)
	})
	writeStatuses := s.service.WriteStatuses()
	writes := make([]liveWriteTarget, 0, len(writeStatuses))
	for _, status := range writeStatuses {
		writes = append(writes, toLiveWriteTarget(status))
	}
	sort.Slice(writes, func(i, j int) bool {
		return lessByPackageAndLocal(writes[i].Package, writes[i].LocalID, writes[j].Package, writes[j].LocalID)
	})
	sys := s.service.SystemLoad()
	workers := make([]liveWorker, 0, len(sys.Workers))
	for _, worker := range sys.Workers {
		workers = append(workers, liveWorker{Kind: worker.Kind, Configured: worker.Configured, Active: worker.Active})
	}
	system := systemInfo{Goroutines: sys.Goroutines, Workers: workers}
	programStates := s.service.ProgramStates()
	heatmap := s.service.heatmapSnapshot(states, logicStates, programStates)
	resp := liveStateResponse{
		Cells:   cells,
		Control: s.service.controller.Status(),
		Metrics: s.service.Metrics(),
		Logic:   logic,
		System:  system,
		Reads:   reads,
		Writes:  writes,
		Heatmap: heatmap,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error().Err(err).Msg("encode live view state")
	}
}

func (s *liveViewServer) handleControl(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var req controlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	switch req.Action {
	case "run":
		s.service.controller.SetMode(controlModeRun)
	case "pause":
		s.service.controller.SetMode(controlModePause)
	case "step":
		s.service.controller.Step()
	case "speed":
		if req.DurationMS == nil {
			http.Error(w, "duration required", http.StatusBadRequest)
			return
		}
		duration := time.Duration(*req.DurationMS) * time.Millisecond
		s.service.setCycleInterval(duration)
	default:
		http.Error(w, "unknown action", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(s.service.controller.Status()); err != nil {
		s.logger.Error().Err(err).Msg("encode control status")
	}
}

func (s *liveViewServer) handleCellUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/cells/")
	if id == "" || strings.Contains(id, "/") {
		http.NotFound(w, r)
		return
	}
	defer r.Body.Close()
	var req cellUpdateRequest
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	var value interface{}
	hasValue := false
	if req.Value != nil && !isJSONNull(req.Value) {
		if err := json.Unmarshal(*req.Value, &value); err != nil {
			http.Error(w, "invalid value", http.StatusBadRequest)
			return
		}
		hasValue = true
	}
	var quality *float64
	qualitySet := false
	if req.Quality != nil {
		qualitySet = true
		if !isJSONNull(req.Quality) {
			var q float64
			if err := json.Unmarshal(*req.Quality, &q); err != nil {
				http.Error(w, "invalid quality", http.StatusBadRequest)
				return
			}
			quality = &q
		}
	}
	state, err := s.service.UpdateCell(id, value, hasValue, quality, qualitySet, req.Valid)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(toLiveCell(state)); err != nil {
		s.logger.Error().Err(err).Msg("encode updated cell")
	}
}

func (s *liveViewServer) handleReadControl(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/reads/")
	if id == "" || strings.Contains(id, "/") {
		http.NotFound(w, r)
		return
	}
	defer r.Body.Close()
	var req toggleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if req.Disabled == nil {
		http.Error(w, "disabled flag required", http.StatusBadRequest)
		return
	}
	status, err := s.service.SetReadGroupDisabled(id, *req.Disabled)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	resp := toLiveReadGroup(status)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error().Err(err).Str("read", id).Msg("encode read toggle response")
	}
}

func (s *liveViewServer) handleWriteControl(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/writes/")
	if id == "" || strings.Contains(id, "/") {
		http.NotFound(w, r)
		return
	}
	defer r.Body.Close()
	var req toggleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if req.Disabled == nil {
		http.Error(w, "disabled flag required", http.StatusBadRequest)
		return
	}
	status, err := s.service.SetWriteTargetDisabled(id, *req.Disabled)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	resp := toLiveWriteTarget(status)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error().Err(err).Str("write", id).Msg("encode write toggle response")
	}
}

func (s *liveViewServer) close() {
	if s == nil || s.server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.server.Shutdown(ctx); err != nil && err != context.Canceled {
		s.logger.Error().Err(err).Msg("shutdown live view")
	}
}

var liveViewTemplate = template.Must(template.New("liveview").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>QUARC Live View</title>
<style>
body { font-family: Arial, sans-serif; margin: 2rem; background: #f7f7f7; color: #222; }
h1 { margin-bottom: 1rem; }
.controls { display: flex; flex-wrap: wrap; gap: 0.5rem; align-items: center; margin-bottom: 1rem; }
.controls button { padding: 0.5rem 1rem; border: none; border-radius: 4px; background: #1976d2; color: #fff; cursor: pointer; }
.controls button.pause { background: #c62828; }
.controls button.step { background: #2e7d32; }
.controls button:disabled { opacity: 0.5; cursor: not-allowed; }
.controls label { display: flex; align-items: center; gap: 0.5rem; }
#durationValue { font-weight: bold; }
.status-indicator { margin-left: auto; font-weight: 600; color: #1976d2; }
.status-indicator.paused { color: #c62828; }
.metrics, .system { margin-bottom: 1rem; font-size: 0.9rem; }
.view-toggle { display: flex; flex-wrap: wrap; gap: 0.5rem; margin-bottom: 0.75rem; }
.view-toggle button { padding: 0.45rem 1rem; border: none; border-radius: 4px; background: #e0e0e0; color: #333; cursor: pointer; transition: background 0.2s ease-in-out; }
.view-toggle button.active { background: #1976d2; color: #fff; }
.view-toggle button:focus { outline: 2px solid rgba(25, 118, 210, 0.4); outline-offset: 2px; }
.view { display: none; }
.view.active { display: block; }
#tableView.view { margin-top: 1.5rem; }
.tabs { display: flex; flex-wrap: wrap; gap: 0.5rem; margin-bottom: 0.75rem; }
.tabs button { padding: 0.4rem 0.8rem; border: none; border-radius: 4px; background: #e0e0e0; cursor: pointer; }
.tabs button.active { background: #424242; color: #fff; }
.tab-content { display: none; }
.tab-content.active { display: block; }
.heatmap-section { margin-bottom: 1.5rem; }
.heatmap-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(110px, 1fr)); gap: 0.75rem; }
.heatmap-tile { position: relative; display: flex; align-items: center; justify-content: center; border: 1px solid #424242; border-radius: 6px; min-height: 96px; aspect-ratio: 1 / 1; padding: 0.25rem; font-weight: 600; text-align: center; background: #bdbdbd; color: #222; transition: transform 0.15s ease-in-out, box-shadow 0.2s ease-in-out; }
.heatmap-tile:hover { transform: translateY(-2px); box-shadow: 0 6px 16px rgba(0,0,0,0.18); }
.heatmap-tile .tile-label { display: block; width: 100%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.heatmap-empty { color: #666; font-size: 0.85rem; grid-column: 1 / -1; }
.heatmap-tooltip { position: fixed; z-index: 1000; max-width: 320px; padding: 0.5rem 0.75rem; background: #fff; color: #222; border: 1px solid #424242; border-radius: 6px; box-shadow: 0 12px 30px rgba(0,0,0,0.25); pointer-events: none; font-size: 0.85rem; line-height: 1.35; }
.heatmap-tooltip .tooltip-title { font-weight: 600; margin-bottom: 0.35rem; font-size: 0.95rem; }
.heatmap-tooltip .tooltip-metric { margin: 0.12rem 0; }
.heatmap-tooltip strong { font-weight: 600; }
table { width: 100%; border-collapse: collapse; background: #fff; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 1rem; }
thead { background: #e0e0e0; }
th, td { padding: 0.5rem; border: 1px solid #ccc; text-align: left; vertical-align: middle; }
tr.invalid { background: #ffebee; }
tr.invalid td { color: #b71c1c; }
.cell-input { width: 100%; box-sizing: border-box; padding: 0.25rem; }
.cell-actions { display: flex; gap: 0.5rem; }
.cell-actions button { padding: 0.35rem 0.8rem; border: none; border-radius: 4px; background: #1976d2; color: #fff; cursor: pointer; }
.cell-actions button:disabled { opacity: 0.5; cursor: not-allowed; }
.diag { font-size: 0.85rem; color: #555; }
.section-title { margin: 0.5rem 0; }
.system table { width: auto; min-width: 260px; border-collapse: collapse; margin-top: 0.5rem; }
.system table th, .system table td { border: 1px solid #ddd; padding: 0.35rem 0.6rem; }
.badge-disabled { display: inline-block; padding: 0.2rem 0.4rem; border-radius: 3px; background: #c62828; color: #fff; font-size: 0.75rem; }
</style>
</head>
<body>
<h1>QUARC Live View</h1>
<div class="controls">
<button id="runBtn">Run</button>
<button id="pauseBtn" class="pause">Pause</button>
<button id="stepBtn" class="step">Step</button>
<label for="speedRange">Tick duration: <span id="durationValue"></span></label>
<input type="range" id="speedRange" min="50" max="5000" step="50" value="500">
<span id="controllerStatus" class="status-indicator"></span>
</div>
<div class="metrics" id="metrics"></div>
<div class="system" id="systemLoad"></div>
<div class="view-toggle" id="viewToggle">
<button data-mode="heatmap" class="active">Heatmap</button>
<button data-mode="table">Tabellen</button>
</div>
<div id="heatmapView" class="view active">
<div class="heatmap-section">
<h2 class="section-title">Speicher Heatmap</h2>
<div class="heatmap-grid" id="heatmapCells"></div>
</div>
<div class="heatmap-section">
<h2 class="section-title">Logik-Blöcke</h2>
<div class="heatmap-grid" id="heatmapLogic"></div>
</div>
<div class="heatmap-section">
<h2 class="section-title">Programme</h2>
<div class="heatmap-grid" id="heatmapPrograms"></div>
</div>
<div id="heatmapTooltip" class="heatmap-tooltip" hidden></div>
</div>
<div id="tableView" class="view">
<div class="tabs" id="tabs">
<button data-tab="cells" class="active">Speicherzellen</button>
<button data-tab="logic">Logik-Blöcke</button>
<button data-tab="io">Reads &amp; Writes</button>
</div>
<div class="tab-content active" id="tab-cells">
<table id="cells">
<thead>
<tr><th>Package</th><th>ID</th><th>Typ</th><th>Wert</th><th>Gültig</th><th>Quality</th><th>Aktualisiert</th><th>Diagnose</th><th>Aktionen</th></tr>
</thead>
<tbody></tbody>
</table>
</div>
<div class="tab-content" id="tab-logic">
<table id="logic">
<thead>
<tr><th>Package</th><th>ID</th><th>Ziel</th><th>Aufrufe</th><th>Übersprungen</th><th>Ø Dauer (ms)</th><th>Letzte Dauer (ms)</th></tr>
</thead>
<tbody></tbody>
</table>
</div>
<div class="tab-content" id="tab-io">
<h2 class="section-title">Reads</h2>
<table id="reads">
<thead>
<tr><th>Package</th><th>ID</th><th>Funktion</th><th>Range</th><th>Status</th><th>Nächster Lauf</th><th>Letzte Dauer (ms)</th><th>Puffer</th><th>Aktionen</th></tr>
</thead>
<tbody></tbody>
</table>
<h2 class="section-title">Writes</h2>
<table id="writes">
<thead>
<tr><th>Package</th><th>ID</th><th>Cell</th><th>Funktion</th><th>Status</th><th>Letzter Schreibvorgang</th><th>Letzte Dauer (ms)</th><th>Aktionen</th></tr>
</thead>
<tbody></tbody>
</table>
</div>
</div>
<script>
const runBtn = document.getElementById('runBtn');
const pauseBtn = document.getElementById('pauseBtn');
const stepBtn = document.getElementById('stepBtn');
const speedRange = document.getElementById('speedRange');
const durationValue = document.getElementById('durationValue');
const metricsBox = document.getElementById('metrics');
const systemBox = document.getElementById('systemLoad');
const statusBox = document.getElementById('controllerStatus');
const tabs = document.getElementById('tabs');
const tabContents = document.querySelectorAll('.tab-content');
const cellsTable = document.getElementById('cells');
const cellsBody = cellsTable.querySelector('tbody');
const logicBody = document.querySelector('#logic tbody');
const readsBody = document.querySelector('#reads tbody');
const writesBody = document.querySelector('#writes tbody');
const viewToggle = document.getElementById('viewToggle');
const heatmapView = document.getElementById('heatmapView');
const tableView = document.getElementById('tableView');
const heatmapCellsGrid = document.getElementById('heatmapCells');
const heatmapLogicGrid = document.getElementById('heatmapLogic');
const heatmapProgramsGrid = document.getElementById('heatmapPrograms');
const heatmapTooltip = document.getElementById('heatmapTooltip');
let isPaused = false;
let refreshTimer = null;
let activeView = 'heatmap';

function startUpdateLoop() {
  if (refreshTimer === null) {
    refreshTimer = setInterval(fetchState, 1000);
  }
}

function stopUpdateLoop() {
  if (refreshTimer !== null) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
}

function updateDurationLabel(ms) {
  durationValue.textContent = ms + ' ms';
  if (Number(speedRange.value) !== Number(ms)) {
    speedRange.value = ms;
  }
}

function setActiveTab(name) {
  tabs.querySelectorAll('button[data-tab]').forEach(function(btn) {
    const active = btn.dataset.tab === name;
    btn.classList.toggle('active', active);
  });
  tabContents.forEach(function(content) {
    const active = content.id === 'tab-' + name;
    content.classList.toggle('active', active);
  });
}

tabs.addEventListener('click', function(event) {
  const btn = event.target.closest('button[data-tab]');
  if (!btn) {
    return;
  }
  setActiveTab(btn.dataset.tab);
});

function setActiveView(mode) {
  if (mode !== 'heatmap' && mode !== 'table') {
    return;
  }
  activeView = mode;
  if (viewToggle) {
    viewToggle.querySelectorAll('button[data-mode]').forEach(function(btn) {
      btn.classList.toggle('active', btn.dataset.mode === mode);
    });
  }
  if (heatmapView) {
    heatmapView.classList.toggle('active', mode === 'heatmap');
  }
  if (tableView) {
    tableView.classList.toggle('active', mode === 'table');
  }
  if (mode !== 'heatmap') {
    hideHeatmapTooltip();
  }
}

if (viewToggle) {
  viewToggle.addEventListener('click', function(event) {
    const btn = event.target.closest('button[data-mode]');
    if (!btn) {
      return;
    }
    setActiveView(btn.dataset.mode);
  });
}

setActiveView('heatmap');

function formatTimestamp(value) {
  if (!value) {
    return '';
  }
  const date = new Date(value);
  if (isNaN(date.getTime())) {
    return '';
  }
  return date.toLocaleString();
}

function formatNumber(value) {
  if (typeof value !== 'number' || Number.isNaN(value)) {
    return '0.00';
  }
  return value.toFixed(2);
}

function escapeHtml(value) {
  if (value === null || value === undefined) {
    return '';
  }
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function stringifyValue(value) {
  if (value === null || value === undefined) {
    return '—';
  }
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch (error) {
      return String(value);
    }
  }
  return String(value);
}

function renderReadBuffers(buffers) {
  if (!buffers || typeof buffers !== 'object') {
    return '—';
  }
  const signals = Object.keys(buffers);
  if (signals.length === 0) {
    return '—';
  }
  signals.sort();
  return signals.map(function(signal) {
    const status = buffers[signal] || {};
    const buffered = typeof status.buffered === 'number' ? status.buffered : 0;
    const dropped = typeof status.dropped === 'number' ? status.dropped : 0;
    let html = '<div class="buffer-status"><div><strong>' + escapeHtml(signal) + '</strong>: ' + escapeHtml('Buffered=' + buffered + ', Dropped=' + dropped);
    if (status.overflow) {
      html += ' <span class="buffer-overflow">(Overflow)</span>';
    }
    html += '</div>';
    const aggregations = status.aggregations || {};
    const aggIds = Object.keys(aggregations);
    aggIds.sort();
    aggIds.forEach(function(cell) {
      const agg = aggregations[cell] || {};
      const labels = [];
      labels.push('<strong>' + escapeHtml(cell) + '</strong>');
      if (agg.aggregator) {
        labels.push('(' + escapeHtml(agg.aggregator) + ')');
      }
      const parts = [];
      if (agg.last) {
        const last = agg.last;
        parts.push('Wert=' + stringifyValue(last.value));
        const count = typeof last.count === 'number' ? last.count : 0;
        parts.push('Anzahl=' + count);
        if (last.quality !== null && last.quality !== undefined) {
          parts.push('Qualität=' + last.quality);
        }
        if (last.timestamp) {
          const ts = formatTimestamp(last.timestamp);
          if (ts) {
            parts.push('Zeit=' + ts);
          }
        }
        if (last.overflow || status.overflow) {
          parts.push('Overflow');
        }
      }
      if (agg.error) {
        parts.push('Fehler=' + agg.error);
      }
      if (agg.quality_cell) {
        parts.push('Qualität in ' + agg.quality_cell);
      }
      if (agg.on_overflow && agg.on_overflow.toLowerCase() === 'ignore') {
        parts.push('Overflow=ignorieren');
      }
      html += '<div class="buffer-details">' + labels.join(' ') + ': ' + parts.map(function(part) { return escapeHtml(String(part)); }).join(', ') + '</div>';
    });
    html += '</div>';
    return html;
  }).join('');
}

function formatAccessKind(kind) {
  if (kind === 'read') {
    return 'Lesen';
  }
  if (kind === 'write') {
    return 'Schreiben';
  }
  return '—';
}

function formatWriteKind(kind) {
  switch (kind) {
    case 'logic':
      return 'Logik';
    case 'logic_invalid':
      return 'Logik (Fehler)';
    case 'program':
      return 'Programm';
    case 'program_invalid':
      return 'Programm (Fehler)';
    case 'manual':
      return 'Manuell';
    default:
      return '';
  }
}

function formatSource(source) {
  if (!source) {
    return '';
  }
  const parts = [];
  if (source.package) {
    parts.push(source.package);
  }
  if (source.file) {
    parts.push(source.file);
  }
  if (source.name) {
    parts.push(source.name);
  }
  if (source.description) {
    parts.push(source.description);
  }
  return parts.join(' · ');
}

function hexToRgb(hex) {
  if (typeof hex !== 'string') {
    return null;
  }
  let value = hex.trim();
  if (value[0] === '#') {
    value = value.slice(1);
  }
  if (value.length === 3) {
    value = value.split('').map(function(ch) { return ch + ch; }).join('');
  }
  if (value.length !== 6) {
    return null;
  }
  const num = parseInt(value, 16);
  if (Number.isNaN(num)) {
    return null;
  }
  return {
    r: (num >> 16) & 255,
    g: (num >> 8) & 255,
    b: num & 255
  };
}

function mixToColor(baseHex, staleHex, ratio) {
  const from = hexToRgb(staleHex) || { r: 189, g: 189, b: 189 };
  const to = hexToRgb(baseHex) || from;
  const clamped = Math.max(0, Math.min(1, Number.isFinite(ratio) ? ratio : 0));
  const r = Math.round(from.r + (to.r - from.r) * clamped);
  const g = Math.round(from.g + (to.g - from.g) * clamped);
  const b = Math.round(from.b + (to.b - from.b) * clamped);
  return { css: 'rgb(' + r + ', ' + g + ', ' + b + ')', r: r, g: g, b: b };
}

function getTextColor(rgb) {
  if (!rgb) {
    return '#222';
  }
  const brightness = (rgb.r * 299 + rgb.g * 587 + rgb.b * 114) / 1000;
  return brightness > 160 ? '#1a1a1a' : '#ffffff';
}

function computeIntensity(lastCycle, cooldown, cycleCount) {
  if (!lastCycle || typeof lastCycle !== 'number' || lastCycle <= 0) {
    return 0;
  }
  if (typeof cooldown !== 'number' || cooldown <= 0) {
    return 1;
  }
  const delta = cycleCount - lastCycle;
  if (delta <= 0) {
    return 1;
  }
  if (delta >= cooldown) {
    return 0;
  }
  return 1 - (delta / cooldown);
}

function computeCellColor(tile, colors, cooldown, cycleCount) {
  const staleColor = colors.stale || '#bdbdbd';
  let baseColor = staleColor;
  let lastCycle = 0;
  if (tile) {
    if (tile.last_access_kind === 'write') {
      baseColor = colors.write || '#ef5350';
      lastCycle = tile.last_access_cycle || tile.last_write_cycle || 0;
    } else if (tile.last_access_kind === 'read') {
      baseColor = colors.read || '#4caf50';
      lastCycle = tile.last_access_cycle || tile.last_read_cycle || 0;
    } else if (tile.last_write_cycle) {
      baseColor = colors.write || '#ef5350';
      lastCycle = tile.last_write_cycle;
    } else if (tile.last_read_cycle) {
      baseColor = colors.read || '#4caf50';
      lastCycle = tile.last_read_cycle;
    }
  }
  const intensity = computeIntensity(lastCycle, cooldown, cycleCount);
  const mixed = mixToColor(baseColor, staleColor, intensity);
  return {
    css: mixed.css,
    textColor: getTextColor(mixed),
    borderColor: colors.border || '#424242'
  };
}

function computeLogicColor(tile, colors, cooldown, cycleCount) {
  const baseColor = colors.logic || '#29b6f6';
  const staleColor = colors.stale || '#bdbdbd';
  const lastCycle = tile && typeof tile.last_cycle === 'number' ? tile.last_cycle : 0;
  const intensity = computeIntensity(lastCycle, cooldown, cycleCount);
  const mixed = mixToColor(baseColor, staleColor, intensity);
  return {
    css: mixed.css,
    textColor: getTextColor(mixed),
    borderColor: colors.border || '#424242'
  };
}

function computeProgramColor(tile, colors, cooldown, cycleCount) {
  const baseColor = colors.program || '#ab47bc';
  const staleColor = colors.stale || '#bdbdbd';
  const lastCycle = tile && typeof tile.last_cycle === 'number' ? tile.last_cycle : 0;
  const intensity = computeIntensity(lastCycle, cooldown, cycleCount);
  const mixed = mixToColor(baseColor, staleColor, intensity);
  return {
    css: mixed.css,
    textColor: getTextColor(mixed),
    borderColor: colors.border || '#424242'
  };
}

function attachHeatmapTooltip(element, html) {
  if (!element) {
    return;
  }
  element.addEventListener('mouseenter', function(event) {
    if (!html) {
      return;
    }
    showHeatmapTooltip(html, event);
  });
  element.addEventListener('mousemove', positionHeatmapTooltip);
  element.addEventListener('mouseleave', hideHeatmapTooltip);
}

function showHeatmapTooltip(html, event) {
  if (!heatmapTooltip || activeView !== 'heatmap') {
    return;
  }
  heatmapTooltip.innerHTML = html;
  heatmapTooltip.removeAttribute('hidden');
  positionHeatmapTooltip(event);
}

function positionHeatmapTooltip(event) {
  if (!heatmapTooltip || heatmapTooltip.hasAttribute('hidden')) {
    return;
  }
  const offset = 16;
  const width = heatmapTooltip.offsetWidth;
  const height = heatmapTooltip.offsetHeight;
  let left = event.clientX + offset;
  let top = event.clientY + offset;
  if (left + width > window.innerWidth) {
    left = event.clientX - width - offset;
  }
  if (top + height > window.innerHeight) {
    top = event.clientY - height - offset;
  }
  heatmapTooltip.style.left = Math.max(8, left) + 'px';
  heatmapTooltip.style.top = Math.max(8, top) + 'px';
}

function hideHeatmapTooltip() {
  if (heatmapTooltip) {
    heatmapTooltip.setAttribute('hidden', '');
  }
}

function renderHeatmapTiles(options) {
  const tiles = Array.isArray(options.tiles) ? options.tiles : [];
  const container = options.container;
  if (!container) {
    return;
  }
  container.innerHTML = '';
  if (tiles.length === 0) {
    const empty = document.createElement('div');
    empty.className = 'heatmap-empty';
    empty.textContent = options.fallback || 'Keine Daten verfügbar.';
    container.appendChild(empty);
    return;
  }
  tiles.forEach(function(tile) {
    const el = document.createElement('div');
    el.className = 'heatmap-tile';
    const colorInfo = options.getColor ? options.getColor(tile) : null;
    if (colorInfo && colorInfo.css) {
      el.style.backgroundColor = colorInfo.css;
      el.style.color = colorInfo.textColor || '#222';
      el.style.borderColor = colorInfo.borderColor || options.borderColor || '#424242';
    } else {
      el.style.backgroundColor = '#bdbdbd';
      el.style.borderColor = options.borderColor || '#424242';
    }
    const label = document.createElement('span');
    label.className = 'tile-label';
    label.textContent = options.getLabel ? String(options.getLabel(tile) || '') : '';
    el.appendChild(label);
    const tooltipHtml = options.getTooltip ? options.getTooltip(tile) : '';
    if (tooltipHtml) {
      attachHeatmapTooltip(el, tooltipHtml);
    }
    container.appendChild(el);
  });
}

function renderCellTooltip(tile, cell) {
  const parts = [];
  const cellId = tile && tile.id ? tile.id : '';
  const title = cell && cell.name ? cell.name : cellId;
  if (title) {
    parts.push('<div class="tooltip-title">' + escapeHtml(title) + '</div>');
  }
  if (cell && cell.name && cellId) {
    parts.push('<div class="tooltip-metric"><strong>ID:</strong> ' + escapeHtml(cellId) + '</div>');
  }
  if (cell && cell.description) {
    parts.push('<div class="tooltip-metric"><strong>Beschreibung:</strong> ' + escapeHtml(cell.description) + '</div>');
  }
  if (cell && cell.kind) {
    parts.push('<div class="tooltip-metric"><strong>Typ:</strong> ' + escapeHtml(cell.kind) + '</div>');
  }
  if (cell) {
    parts.push('<div class="tooltip-metric"><strong>Wert:</strong> ' + escapeHtml(stringifyValue(cell.value)) + '</div>');
    parts.push('<div class="tooltip-metric"><strong>Gültig:</strong> ' + (cell.valid ? 'Ja' : 'Nein') + '</div>');
    parts.push('<div class="tooltip-metric"><strong>Quality:</strong> ' + (cell.quality !== null && cell.quality !== undefined ? escapeHtml(cell.quality) : '—') + '</div>');
    parts.push('<div class="tooltip-metric"><strong>Aktualisiert:</strong> ' + (formatTimestamp(cell.updated_at) || '—') + '</div>');
  }
  if (tile) {
    if (tile.last_access_kind) {
      const accessLabel = formatAccessKind(tile.last_access_kind);
      const accessTime = formatTimestamp(tile.last_access_time) || '—';
      parts.push('<div class="tooltip-metric"><strong>Letzter Zugriff:</strong> ' + escapeHtml(accessLabel) + ' · ' + accessTime + '</div>');
    }
    if (tile.last_read_cycle) {
      const origin = tile.last_read_origin ? ' (' + escapeHtml(tile.last_read_origin) + ')' : '';
      parts.push('<div class="tooltip-metric"><strong>Letzte Lesung:</strong> ' + (formatTimestamp(tile.last_read_time) || '—') + origin + '</div>');
    }
    if (tile.read_count) {
      parts.push('<div class="tooltip-metric"><strong>Lesungen gesamt:</strong> ' + tile.read_count + '</div>');
    }
    if (tile.last_write_cycle) {
      const writeKind = formatWriteKind(tile.last_write_kind);
      const writeOrigin = tile.last_write_origin ? ' (' + escapeHtml(tile.last_write_origin) + ')' : '';
      const writeLabel = writeKind ? writeKind + writeOrigin : writeOrigin;
      parts.push('<div class="tooltip-metric"><strong>Letzter Schreibzugriff:</strong> ' + (formatTimestamp(tile.last_write_time) || '—') + (writeLabel ? ' · ' + writeLabel : '') + '</div>');
    }
    if (tile.write_count) {
      parts.push('<div class="tooltip-metric"><strong>Schreibzugriffe gesamt:</strong> ' + tile.write_count + '</div>');
    }
  }
  return parts.join('');
}

function renderLogicTooltip(tile) {
  if (!tile) {
    return '';
  }
  const parts = [];
  const id = tile.id || '';
  if (id) {
    parts.push('<div class="tooltip-title">' + escapeHtml(id) + '</div>');
  }
  if (tile.target) {
    parts.push('<div class="tooltip-metric"><strong>Ziel:</strong> ' + escapeHtml(tile.target) + '</div>');
  }
  parts.push('<div class="tooltip-metric"><strong>Aufrufe:</strong> ' + (tile.calls || 0) + '</div>');
  parts.push('<div class="tooltip-metric"><strong>Fehler:</strong> ' + (tile.errors || 0) + '</div>');
  if (tile.last_time) {
    parts.push('<div class="tooltip-metric"><strong>Letzte Ausführung:</strong> ' + (formatTimestamp(tile.last_time) || '—') + '</div>');
  }
  const sourceText = formatSource(tile.source);
  if (sourceText) {
    parts.push('<div class="tooltip-metric"><strong>Quelle:</strong> ' + escapeHtml(sourceText) + '</div>');
  }
  return parts.join('');
}

function renderProgramTooltip(tile) {
  if (!tile) {
    return '';
  }
  const parts = [];
  const id = tile.id || '';
  if (id) {
    parts.push('<div class="tooltip-title">' + escapeHtml(id) + '</div>');
  }
  if (tile.type) {
    parts.push('<div class="tooltip-metric"><strong>Typ:</strong> ' + escapeHtml(tile.type) + '</div>');
  }
  if (Array.isArray(tile.outputs) && tile.outputs.length > 0) {
    parts.push('<div class="tooltip-metric"><strong>Ausgänge:</strong> ' + escapeHtml(tile.outputs.join(', ')) + '</div>');
  }
  parts.push('<div class="tooltip-metric"><strong>Aufrufe:</strong> ' + (tile.calls || 0) + '</div>');
  parts.push('<div class="tooltip-metric"><strong>Fehler:</strong> ' + (tile.errors || 0) + '</div>');
  if (tile.last_time) {
    parts.push('<div class="tooltip-metric"><strong>Letzte Ausführung:</strong> ' + (formatTimestamp(tile.last_time) || '—') + '</div>');
  }
  const sourceText = formatSource(tile.source);
  if (sourceText) {
    parts.push('<div class="tooltip-metric"><strong>Quelle:</strong> ' + escapeHtml(sourceText) + '</div>');
  }
  return parts.join('');
}

function renderHeatmap(state) {
  if (!heatmapCellsGrid || !heatmapLogicGrid || !heatmapProgramsGrid) {
    return;
  }
  hideHeatmapTooltip();
  const heatmap = state && state.heatmap ? state.heatmap : null;
  const config = heatmap && heatmap.config ? heatmap.config : {};
  const colors = config.colors || {};
  const cooldown = config.cooldown || {};
  const cycleCount = state && state.metrics && typeof state.metrics.CycleCount === 'number' ? state.metrics.CycleCount : 0;
  if (heatmapView) {
    heatmapView.style.backgroundColor = colors.background || '';
  }
  if (heatmapTooltip) {
    heatmapTooltip.style.borderColor = colors.border || '#424242';
  }
  const cellsById = new Map();
  if (state && Array.isArray(state.cells)) {
    state.cells.forEach(function(cell) {
      if (cell && cell.id) {
        cellsById.set(cell.id, cell);
      }
    });
  }
  renderHeatmapTiles({
    tiles: heatmap && Array.isArray(heatmap.cells) ? heatmap.cells : [],
    container: heatmapCellsGrid,
    fallback: 'Keine Speicherzellen konfiguriert.',
    getLabel: function(tile) { return tile && tile.id ? tile.id : '—'; },
    getColor: function(tile) { return computeCellColor(tile, colors, cooldown.cells, cycleCount); },
    getTooltip: function(tile) { return renderCellTooltip(tile, cellsById.get(tile && tile.id)); },
    borderColor: colors.border
  });
  renderHeatmapTiles({
    tiles: heatmap && Array.isArray(heatmap.logic) ? heatmap.logic : [],
    container: heatmapLogicGrid,
    fallback: 'Keine Logik-Blöcke konfiguriert.',
    getLabel: function(tile) { return tile && tile.id ? tile.id : '—'; },
    getColor: function(tile) { return computeLogicColor(tile, colors, cooldown.logic, cycleCount); },
    getTooltip: renderLogicTooltip,
    borderColor: colors.border
  });
  renderHeatmapTiles({
    tiles: heatmap && Array.isArray(heatmap.programs) ? heatmap.programs : [],
    container: heatmapProgramsGrid,
    fallback: 'Keine Programme konfiguriert.',
    getLabel: function(tile) { return tile && tile.id ? tile.id : '—'; },
    getColor: function(tile) { return computeProgramColor(tile, colors, cooldown.programs, cycleCount); },
    getTooltip: renderProgramTooltip,
    borderColor: colors.border
  });
}

function renderMetrics(metrics) {
  if (!metrics) {
    metricsBox.innerHTML = '';
    return;
  }
  const durationMs = metrics.LastDuration ? formatNumber(metrics.LastDuration / 1e6) : '0.00';
  metricsBox.innerHTML = ''
    + '<div>Zyklen: ' + (metrics.CycleCount || 0) + '</div>'
    + '<div>Letzte Dauer: ' + durationMs + ' ms</div>'
    + '<div>Fehler (Read/Program/Logic/Write): ' + (metrics.LastReadErrors || 0) + ' / ' + (metrics.LastProgramErrors || 0) + ' / ' + (metrics.LastEvalErrors || 0) + ' / ' + (metrics.LastWriteErrors || 0) + '</div>';
}

function renderSystem(system) {
  if (!system) {
    systemBox.innerHTML = '';
    return;
  }
  let html = '<div>Goroutines: ' + (system.goroutines || 0) + '</div>';
  if (Array.isArray(system.workers) && system.workers.length > 0) {
    let rows = '';
    system.workers.forEach(function(worker) {
      rows += '<tr><td>' + worker.kind + '</td><td>' + worker.configured + '</td><td>' + worker.active + '</td></tr>';
    });
    html += '<table><thead><tr><th>Worker</th><th>Konfiguriert</th><th>Aktiv</th></tr></thead><tbody>' + rows + '</tbody></table>';
  }
  systemBox.innerHTML = html;
}

function createValueInput(cell, editable) {
  let element;
  let original = '';
  if (cell.kind === 'bool') {
    element = document.createElement('select');
    element.className = 'cell-input cell-value';
    ['', 'true', 'false'].forEach(function(value) {
      const option = document.createElement('option');
      option.value = value;
      option.textContent = value === '' ? '—' : value;
      element.appendChild(option);
    });
    if (cell.valid && typeof cell.value === 'boolean') {
      original = String(cell.value);
      element.value = original;
    } else {
      element.value = '';
    }
  } else {
    element = document.createElement('input');
    element.className = 'cell-input cell-value';
    if (cell.kind === 'number') {
      element.type = 'number';
      element.step = 'any';
    } else {
      element.type = 'text';
    }
    if (cell.valid && cell.value !== undefined && cell.value !== null) {
      original = String(cell.value);
      element.value = original;
    } else {
      element.value = '';
    }
  }
  element.disabled = !editable;
  return { element: element, original: original };
}

function renderCells(cells, editable) {
  cellsBody.innerHTML = '';
  (cells || []).forEach(function(cell) {
    const tr = document.createElement('tr');
    if (!cell.valid) {
      tr.classList.add('invalid');
    }
    tr.dataset.id = cell.id;
    tr.dataset.kind = cell.kind;
    const valueInfo = createValueInput(cell, editable);
    tr.dataset.valueOriginal = valueInfo.original || '';
    const qualityInput = document.createElement('input');
    qualityInput.type = 'number';
    qualityInput.step = 'any';
    qualityInput.className = 'cell-input cell-quality';
    if (cell.quality !== null && cell.quality !== undefined) {
      const q = Number(cell.quality);
      tr.dataset.qualityOriginal = String(q);
      qualityInput.value = String(q);
    } else {
      tr.dataset.qualityOriginal = '';
      qualityInput.value = '';
    }
    qualityInput.disabled = !editable;
    const validInput = document.createElement('input');
    validInput.type = 'checkbox';
    validInput.className = 'cell-valid';
    validInput.checked = !!cell.valid;
    validInput.disabled = !editable;
    tr.dataset.valid = cell.valid ? 'true' : 'false';
    const actions = document.createElement('div');
    actions.className = 'cell-actions';
    const saveBtn = document.createElement('button');
    saveBtn.type = 'button';
    saveBtn.textContent = 'Speichern';
    saveBtn.dataset.action = 'save-cell';
    saveBtn.disabled = !editable;
    actions.appendChild(saveBtn);
    const diagText = cell.diagnosis ? ((cell.diagnosis.code || '') + ' ' + (cell.diagnosis.message || '')).trim() : '';
    let rowHtml = '';
    rowHtml += '<td>' + (cell.package || '') + '</td>';
    rowHtml += '<td>' + (cell.local_id || cell.id) + '</td>';
    rowHtml += '<td>' + cell.kind + '</td>';
    rowHtml += '<td></td>';
    rowHtml += '<td></td>';
    rowHtml += '<td></td>';
    rowHtml += '<td>' + formatTimestamp(cell.updated_at) + '</td>';
    rowHtml += '<td class="diag">' + diagText + '</td>';
    rowHtml += '<td></td>';
    tr.innerHTML = rowHtml;
    tr.children[3].appendChild(valueInfo.element);
    tr.children[4].appendChild(validInput);
    tr.children[5].appendChild(qualityInput);
    tr.children[8].appendChild(actions);
    cellsBody.appendChild(tr);
  });
}

function renderLogic(entries) {
  logicBody.innerHTML = '';
  (entries || []).forEach(function(entry) {
    const tr = document.createElement('tr');
    let rowHtml = '';
    rowHtml += '<td>' + (entry.package || '') + '</td>';
    rowHtml += '<td>' + (entry.local_id || entry.id) + '</td>';
    rowHtml += '<td>' + (entry.target || '') + '</td>';
    rowHtml += '<td>' + (entry.calls || 0) + '</td>';
    rowHtml += '<td>' + (entry.skipped || 0) + '</td>';
    rowHtml += '<td>' + formatNumber(entry.avg_ms) + '</td>';
    rowHtml += '<td>' + formatNumber(entry.last_ms) + '</td>';
    tr.innerHTML = rowHtml;
    logicBody.appendChild(tr);
  });
}

function renderReads(reads) {
  readsBody.innerHTML = '';
  (reads || []).forEach(function(read) {
    const tr = document.createElement('tr');
    const range = typeof read.length === 'number' ? (read.start + ' (+' + read.length + ')') : String(read.start || 0);
    const status = read.disabled ? '<span class="badge-disabled">Deaktiviert</span>' : 'Aktiv';
    let rowHtml = '';
    rowHtml += '<td>' + (read.package || '') + '</td>';
    rowHtml += '<td>' + (read.local_id || read.id) + '</td>';
    rowHtml += '<td>' + read.function + '</td>';
    rowHtml += '<td>' + range + '</td>';
    rowHtml += '<td>' + status + '</td>';
    rowHtml += '<td>' + formatTimestamp(read.next_run) + '</td>';
    rowHtml += '<td>' + formatNumber(read.last_duration_ms) + '</td>';
    rowHtml += '<td>' + renderReadBuffers(read.buffers) + '</td>';
    rowHtml += '<td><button type="button" data-action="toggle-read" data-id="' + read.id + '" data-disabled="' + (read.disabled ? 'true' : 'false') + '">' + (read.disabled ? 'Aktivieren' : 'Pausieren') + '</button></td>';
    tr.innerHTML = rowHtml;
    readsBody.appendChild(tr);
  });
}

function renderWrites(writes) {
  writesBody.innerHTML = '';
  (writes || []).forEach(function(write) {
    const tr = document.createElement('tr');
    const status = write.disabled ? '<span class="badge-disabled">Deaktiviert</span>' : 'Aktiv';
    const timestamp = formatTimestamp(write.last_write || write.last_attempt);
    let rowHtml = '';
    rowHtml += '<td>' + (write.package || '') + '</td>';
    rowHtml += '<td>' + (write.local_id || write.id) + '</td>';
    rowHtml += '<td>' + write.cell + '</td>';
    rowHtml += '<td>' + write.function + '</td>';
    rowHtml += '<td>' + status + '</td>';
    rowHtml += '<td>' + timestamp + '</td>';
    rowHtml += '<td>' + formatNumber(write.last_duration_ms) + '</td>';
    rowHtml += '<td><button type="button" data-action="toggle-write" data-id="' + write.id + '" data-disabled="' + (write.disabled ? 'true' : 'false') + '">' + (write.disabled ? 'Aktivieren' : 'Pausieren') + '</button></td>';
    tr.innerHTML = rowHtml;
    writesBody.appendChild(tr);
  });
}

function updateControllerStatus(control) {
  if (!control) {
    statusBox.textContent = '';
    statusBox.classList.remove('paused');
    isPaused = false;
    startUpdateLoop();
    return false;
  }
  const paused = control.mode === 'pause';
  statusBox.textContent = paused ? 'Pause' : 'Run';
  statusBox.className = 'status-indicator' + (paused ? ' paused' : '');
  isPaused = paused;
  if (paused) {
    stopUpdateLoop();
  } else {
    startUpdateLoop();
  }
  return paused;
}

function fetchState() {
  fetch('/api/state')
    .then(function(resp) { return resp.json(); })
    .then(function(data) {
      if (data.control && typeof data.control.interval_ms === 'number') {
        updateDurationLabel(data.control.interval_ms);
      }
      const paused = updateControllerStatus(data.control);
      renderCells(data.cells || [], paused);
      renderMetrics(data.metrics);
      renderHeatmap(data);
      renderSystem(data.system);
      renderLogic(data.logic);
      renderReads(data.reads);
      renderWrites(data.writes);
    })
    .catch(function(err) { console.error('state error', err); });
}

function postControl(action, payload) {
  const body = Object.assign({ action: action }, payload || {});
  return fetch('/api/control', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  }).then(function(resp) {
    if (!resp.ok) {
      return resp.text().then(function(text) { throw new Error(text || 'request failed'); });
    }
    return resp.json();
  }).then(function(status) {
    if (status && typeof status.interval_ms === 'number') {
      updateDurationLabel(status.interval_ms);
    }
    updateControllerStatus(status);
    return status;
  });
}

runBtn.addEventListener('click', function() {
  postControl('run').then(fetchState).catch(function(err) { console.error('control error', err); });
});
pauseBtn.addEventListener('click', function() {
  postControl('pause').then(fetchState).catch(function(err) { console.error('control error', err); });
});
stepBtn.addEventListener('click', function() {
  postControl('step').then(fetchState).catch(function(err) { console.error('control error', err); });
});

speedRange.addEventListener('input', function() {
  updateDurationLabel(speedRange.value);
});
speedRange.addEventListener('change', function() {
  postControl('speed', { duration_ms: Number(speedRange.value) }).catch(function(err) { console.error('control error', err); });
});

cellsTable.addEventListener('click', function(event) {
  const button = event.target.closest('button[data-action="save-cell"]');
  if (!button) {
    return;
  }
  const row = button.closest('tr');
  if (!row) {
    return;
  }
  submitCellUpdate(row, button);
});

function submitCellUpdate(row, button) {
  const id = row.dataset.id;
  const kind = row.dataset.kind;
  const valueInput = row.querySelector('.cell-value');
  const qualityInput = row.querySelector('.cell-quality');
  const validInput = row.querySelector('.cell-valid');
  if (!id || !valueInput || !qualityInput || !validInput) {
    return;
  }
  const payload = {};
  let changed = false;
  const originalValid = row.dataset.valid === 'true';
  if (validInput.checked !== originalValid) {
    payload.valid = validInput.checked;
    changed = true;
  }
  const originalValue = row.dataset.valueOriginal || '';
  if (kind === 'bool') {
    const value = valueInput.value;
    if (value !== originalValue) {
      if (value !== '') {
        payload.value = value === 'true';
        changed = true;
      }
    }
  } else {
    const value = valueInput.value;
    if (value !== originalValue) {
      if (value !== '') {
        if (kind === 'number') {
          const num = Number(value);
          if (Number.isNaN(num)) {
            alert('Ungültiger Zahlenwert');
            return;
          }
          payload.value = num;
        } else {
          payload.value = value;
        }
        changed = true;
      }
    }
  }
  const originalQuality = row.dataset.qualityOriginal || '';
  const qualityValue = qualityInput.value;
  if (qualityValue !== originalQuality) {
    if (qualityValue === '') {
      payload.quality = null;
    } else {
      const q = Number(qualityValue);
      if (Number.isNaN(q)) {
        alert('Ungültige Quality');
        return;
      }
      payload.quality = q;
    }
    changed = true;
  }
  if (!changed) {
    return;
  }
  button.disabled = true;
  fetch('/api/cells/' + encodeURIComponent(id), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  }).then(function(resp) {
    if (!resp.ok) {
      return resp.text().then(function(text) { throw new Error(text || 'request failed'); });
    }
    return resp.json();
  }).then(function() {
    fetchState();
  }).catch(function(err) {
    console.error('cell update error', err);
    alert(err.message || 'Aktualisierung fehlgeschlagen');
  }).finally(function() {
    button.disabled = !isPaused;
  });
}

function toggleEndpoint(prefix, id, disabled, button) {
  button.disabled = true;
  fetch(prefix + '/' + encodeURIComponent(id), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ disabled: disabled })
  }).then(function(resp) {
    if (!resp.ok) {
      return resp.text().then(function(text) { throw new Error(text || 'request failed'); });
    }
    return resp.json();
  }).then(function() {
    fetchState();
  }).catch(function(err) {
    console.error('toggle error', err);
    alert(err.message || 'Aktion fehlgeschlagen');
  }).finally(function() {
    button.disabled = false;
  });
}

document.getElementById('reads').addEventListener('click', function(event) {
  const button = event.target.closest('button[data-action="toggle-read"]');
  if (!button) {
    return;
  }
  const id = button.dataset.id;
  const disabled = button.dataset.disabled === 'true';
  toggleEndpoint('/api/reads', id, !disabled, button);
});

document.getElementById('writes').addEventListener('click', function(event) {
  const button = event.target.closest('button[data-action="toggle-write"]');
  if (!button) {
    return;
  }
  const id = button.dataset.id;
  const disabled = button.dataset.disabled === 'true';
  toggleEndpoint('/api/writes', id, !disabled, button);
});

updateDurationLabel(speedRange.value);
startUpdateLoop();
fetchState();
</script>
</body>
</html>`))
