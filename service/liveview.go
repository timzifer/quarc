package service

import (
	"bytes"
	"context"
	"encoding/json"
	"html/template"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/timzifer/modbus_processor/config"
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
}

type liveCell struct {
	ID          string                 `json:"id"`
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
	ID             string                 `json:"id"`
	Function       string                 `json:"function"`
	Start          uint16                 `json:"start"`
	Length         uint16                 `json:"length"`
	Disabled       bool                   `json:"disabled"`
	NextRun        *time.Time             `json:"next_run,omitempty"`
	LastRun        *time.Time             `json:"last_run,omitempty"`
	LastDurationMS float64                `json:"last_duration_ms"`
	Source         config.ModuleReference `json:"source,omitempty"`
}

type liveWriteTarget struct {
	ID             string                 `json:"id"`
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

func toLiveCell(state CellState) liveCell {
	return liveCell{
		ID:          state.ID,
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
	logicStates := s.service.LogicStates()
	logic := make([]liveLogicBlock, 0, len(logicStates))
	for _, entry := range logicStates {
		logic = append(logic, liveLogicBlock{
			ID:      entry.ID,
			Target:  entry.Target,
			Calls:   entry.Metrics.Calls,
			Skipped: entry.Metrics.Skipped,
			AvgMS:   durationToMillis(entry.Metrics.Average),
			LastMS:  durationToMillis(entry.Metrics.Last),
			Source:  entry.Source,
		})
	}
	readStatuses := s.service.ReadStatuses()
	reads := make([]liveReadGroup, 0, len(readStatuses))
	for _, status := range readStatuses {
		reads = append(reads, liveReadGroup{
			ID:             status.ID,
			Function:       status.Function,
			Start:          status.Start,
			Length:         status.Length,
			Disabled:       status.Disabled,
			NextRun:        timePtr(status.NextRun),
			LastRun:        timePtr(status.LastRun),
			LastDurationMS: durationToMillis(status.LastDuration),
			Source:         status.Source,
		})
	}
	writeStatuses := s.service.WriteStatuses()
	writes := make([]liveWriteTarget, 0, len(writeStatuses))
	for _, status := range writeStatuses {
		writes = append(writes, liveWriteTarget{
			ID:             status.ID,
			Cell:           status.Cell,
			Function:       status.Function,
			Address:        status.Address,
			Disabled:       status.Disabled,
			LastWrite:      timePtr(status.LastWrite),
			LastAttempt:    timePtr(status.LastAttempt),
			LastDurationMS: durationToMillis(status.LastDuration),
			Source:         status.Source,
		})
	}
	sys := s.service.SystemLoad()
	workers := make([]liveWorker, 0, len(sys.Workers))
	for _, worker := range sys.Workers {
		workers = append(workers, liveWorker{Kind: worker.Kind, Configured: worker.Configured, Active: worker.Active})
	}
	system := systemInfo{Goroutines: sys.Goroutines, Workers: workers}
	resp := liveStateResponse{
		Cells:   cells,
		Control: s.service.controller.Status(),
		Metrics: s.service.Metrics(),
		Logic:   logic,
		System:  system,
		Reads:   reads,
		Writes:  writes,
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
	resp := liveReadGroup{
		ID:             status.ID,
		Function:       status.Function,
		Start:          status.Start,
		Length:         status.Length,
		Disabled:       status.Disabled,
		NextRun:        timePtr(status.NextRun),
		LastRun:        timePtr(status.LastRun),
		LastDurationMS: durationToMillis(status.LastDuration),
	}
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
	resp := liveWriteTarget{
		ID:             status.ID,
		Cell:           status.Cell,
		Function:       status.Function,
		Address:        status.Address,
		Disabled:       status.Disabled,
		LastWrite:      timePtr(status.LastWrite),
		LastAttempt:    timePtr(status.LastAttempt),
		LastDurationMS: durationToMillis(status.LastDuration),
	}
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
<title>Modbus Processor Live View</title>
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
.tabs { display: flex; flex-wrap: wrap; gap: 0.5rem; margin-bottom: 0.75rem; }
.tabs button { padding: 0.4rem 0.8rem; border: none; border-radius: 4px; background: #e0e0e0; cursor: pointer; }
.tabs button.active { background: #424242; color: #fff; }
.tab-content { display: none; }
.tab-content.active { display: block; }
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
<h1>Modbus Processor Live View</h1>
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
<div class="tabs" id="tabs">
<button data-tab="cells" class="active">Speicherzellen</button>
<button data-tab="logic">Logik-Blöcke</button>
<button data-tab="io">Reads &amp; Writes</button>
</div>
<div class="tab-content active" id="tab-cells">
<table id="cells">
<thead>
<tr><th>ID</th><th>Typ</th><th>Wert</th><th>Gültig</th><th>Quality</th><th>Aktualisiert</th><th>Diagnose</th><th>Aktionen</th></tr>
</thead>
<tbody></tbody>
</table>
</div>
<div class="tab-content" id="tab-logic">
<table id="logic">
<thead>
<tr><th>ID</th><th>Ziel</th><th>Aufrufe</th><th>Übersprungen</th><th>Ø Dauer (ms)</th><th>Letzte Dauer (ms)</th></tr>
</thead>
<tbody></tbody>
</table>
</div>
<div class="tab-content" id="tab-io">
<h2 class="section-title">Reads</h2>
<table id="reads">
<thead>
<tr><th>ID</th><th>Funktion</th><th>Range</th><th>Status</th><th>Nächster Lauf</th><th>Letzte Dauer (ms)</th><th>Aktionen</th></tr>
</thead>
<tbody></tbody>
</table>
<h2 class="section-title">Writes</h2>
<table id="writes">
<thead>
<tr><th>ID</th><th>Cell</th><th>Funktion</th><th>Status</th><th>Letzter Schreibvorgang</th><th>Letzte Dauer (ms)</th><th>Aktionen</th></tr>
</thead>
<tbody></tbody>
</table>
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
let isPaused = false;
let refreshTimer = null;

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
    rowHtml += '<td>' + cell.id + '</td>';
    rowHtml += '<td>' + cell.kind + '</td>';
    rowHtml += '<td></td>';
    rowHtml += '<td></td>';
    rowHtml += '<td></td>';
    rowHtml += '<td>' + formatTimestamp(cell.updated_at) + '</td>';
    rowHtml += '<td class="diag">' + diagText + '</td>';
    rowHtml += '<td></td>';
    tr.innerHTML = rowHtml;
    tr.children[2].appendChild(valueInfo.element);
    tr.children[3].appendChild(validInput);
    tr.children[4].appendChild(qualityInput);
    tr.children[7].appendChild(actions);
    cellsBody.appendChild(tr);
  });
}

function renderLogic(entries) {
  logicBody.innerHTML = '';
  (entries || []).forEach(function(entry) {
    const tr = document.createElement('tr');
    let rowHtml = '';
    rowHtml += '<td>' + entry.id + '</td>';
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
    rowHtml += '<td>' + read.id + '</td>';
    rowHtml += '<td>' + read.function + '</td>';
    rowHtml += '<td>' + range + '</td>';
    rowHtml += '<td>' + status + '</td>';
    rowHtml += '<td>' + formatTimestamp(read.next_run) + '</td>';
    rowHtml += '<td>' + formatNumber(read.last_duration_ms) + '</td>';
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
    rowHtml += '<td>' + write.id + '</td>';
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
