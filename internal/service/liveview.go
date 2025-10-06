package service

import (
	"context"
	"encoding/json"
	"html/template"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

type liveViewServer struct {
	logger  zerolog.Logger
	service *Service
	server  *http.Server
	ln      net.Listener
}

type liveStateResponse struct {
	Cells   []liveCell         `json:"cells"`
	Control controlStatus      `json:"control"`
	Metrics metrics            `json:"metrics"`
	Load    loadSnapshot       `json:"load"`
	Logic   []logicBlockState  `json:"logic"`
	Reads   []readGroupState   `json:"reads"`
	Writes  []writeTargetState `json:"writes"`
}

type liveCell struct {
	ID        string         `json:"id"`
	Kind      string         `json:"kind"`
	Value     interface{}    `json:"value"`
	Valid     bool           `json:"valid"`
	Quality   *float64       `json:"quality"`
	Diagnosis *CellDiagnosis `json:"diagnosis,omitempty"`
	UpdatedAt time.Time      `json:"updated_at"`
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
	mux.HandleFunc("/api/cell", server.handleCell)
	mux.HandleFunc("/api/io", server.handleIOControl)

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
		cells = append(cells, liveCell{
			ID:        state.ID,
			Kind:      string(state.Kind),
			Value:     state.Value,
			Valid:     state.Valid,
			Quality:   state.Quality,
			Diagnosis: state.Diagnosis,
			UpdatedAt: state.UpdatedAt,
		})
	}
	resp := liveStateResponse{
		Cells:   cells,
		Control: s.service.controller.Status(),
		Metrics: s.service.Metrics(),
		Load:    s.service.Load(),
		Logic:   s.service.LogicStates(),
		Reads:   s.service.ReadStates(),
		Writes:  s.service.WriteStates(),
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

type cellUpdateRequest struct {
	ID      string      `json:"id"`
	Value   interface{} `json:"value"`
	Quality *float64    `json:"quality"`
	Valid   *bool       `json:"valid"`
}

func (s *liveViewServer) handleCell(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var req cellUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	state, err := s.service.UpdateCell(req.ID, req.Value, req.Quality, req.Valid)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(state); err != nil {
		s.logger.Error().Err(err).Msg("encode cell state")
	}
}

type ioControlRequest struct {
	Kind     string `json:"kind"`
	ID       string `json:"id"`
	Disabled bool   `json:"disabled"`
}

type ioControlResponse struct {
	Reads  []readGroupState   `json:"reads"`
	Writes []writeTargetState `json:"writes"`
}

func (s *liveViewServer) handleIOControl(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var req ioControlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	var err error
	switch strings.ToLower(req.Kind) {
	case "read", "reads":
		err = s.service.SetReadDisabled(req.ID, req.Disabled)
	case "write", "writes":
		err = s.service.SetWriteDisabled(req.ID, req.Disabled)
	default:
		http.Error(w, "unknown kind", http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := ioControlResponse{
		Reads:  s.service.ReadStates(),
		Writes: s.service.WriteStates(),
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error().Err(err).Msg("encode io control response")
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
.controls button:hover { opacity: 0.9; }
.controls label { display: flex; align-items: center; gap: 0.5rem; }
#durationValue { font-weight: bold; }
table { width: 100%; border-collapse: collapse; background: #fff; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
thead { background: #e0e0e0; }
th, td { padding: 0.5rem; border: 1px solid #ccc; text-align: left; }
tr.invalid { background: #ffebee; }
tr.invalid td { color: #b71c1c; }
.diag { font-size: 0.85rem; color: #555; }
.metrics { margin-bottom: 1rem; font-size: 0.9rem; }
.tabs { display: flex; gap: 0.5rem; margin-bottom: 1rem; }
.tab-btn { padding: 0.5rem 1rem; border: none; border-radius: 4px; background: #e0e0e0; cursor: pointer; }
.tab-btn.active { background: #1976d2; color: #fff; }
.tab-content { display: none; }
.tab-content.active { display: block; }
.io-section { margin-bottom: 1.5rem; }
#logic, #reads, #writes { margin-top: 0.5rem; }
.cell-editor { width: 100%; box-sizing: border-box; padding: 0.25rem; }
.apply-cell { padding: 0.25rem 0.75rem; border: none; border-radius: 4px; background: #3949ab; color: #fff; cursor: pointer; }
.apply-cell:disabled { opacity: 0.5; cursor: not-allowed; }
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
</div>
<div class="metrics" id="metrics"></div>
<div class="tabs">
<button class="tab-btn active" data-tab="cells">Cells</button>
<button class="tab-btn" data-tab="logic">Logic</button>
<button class="tab-btn" data-tab="io">I/O</button>
</div>
<div id="cellsTab" class="tab-content active">
<table id="cells">
<thead>
<tr><th>ID</th><th>Type</th><th>Value</th><th>Valid</th><th>Quality</th><th>Updated</th><th>Diagnosis</th><th>Actions</th></tr>
</thead>
<tbody></tbody>
</table>
</div>
<div id="logicTab" class="tab-content">
<table id="logic">
<thead>
<tr><th>ID</th><th>Target</th><th>Calls</th><th>Skipped</th><th>Avg Duration (ms)</th></tr>
</thead>
<tbody></tbody>
</table>
</div>
<div id="ioTab" class="tab-content">
<div class="io-section">
<h2>Read Groups</h2>
<table id="reads" data-kind="read">
<thead>
<tr><th>ID</th><th>Function</th><th>Range</th><th>Disabled</th><th>Toggle</th></tr>
</thead>
<tbody></tbody>
</table>
</div>
<div class="io-section">
<h2>Write Targets</h2>
<table id="writes" data-kind="write">
<thead>
<tr><th>ID</th><th>Cell</th><th>Function</th><th>Address</th><th>Disabled</th><th>Toggle</th></tr>
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
const cellsBody = document.querySelector('#cells tbody');
const logicBody = document.querySelector('#logic tbody');
const readsBody = document.querySelector('#reads tbody');
const writesBody = document.querySelector('#writes tbody');
const metricsBox = document.getElementById('metrics');
const tabButtons = document.querySelectorAll('.tab-btn');
const tabContents = document.querySelectorAll('.tab-content');
let currentControl = null;

tabButtons.forEach(btn => {
  btn.addEventListener('click', () => setActiveTab(btn.dataset.tab));
});

function setActiveTab(name) {
  tabButtons.forEach(btn => btn.classList.toggle('active', btn.dataset.tab === name));
  tabContents.forEach(panel => panel.classList.toggle('active', panel.id === name + 'Tab'));
}

function isPaused() {
  return currentControl && currentControl.mode === 'pause';
}

function updateDurationLabel(ms) {
  durationValue.textContent = ms + ' ms';
  if (Number(speedRange.value) !== Number(ms)) {
    speedRange.value = ms;
  }
}

function renderMetrics(metrics, load, control) {
  if (!metrics) {
    metricsBox.textContent = '';
    return;
  }
  const cycleCount = metrics.cycle_count || 0;
  const lastDuration = typeof metrics.last_duration_ms === 'number'
    ? metrics.last_duration_ms.toFixed(2) + ' ms'
    : (metrics.last_duration || '0');
  const mode = control && control.mode ? control.mode : 'unknown';
  const parts = [];
  parts.push('Mode: ' + mode);
  parts.push('Cycle count: ' + cycleCount);
  parts.push('Last duration: ' + lastDuration);
  parts.push('Errors - read: ' + (metrics.last_read_errors || 0) + ', program: ' + (metrics.last_program_errors || 0) + ', logic: ' + (metrics.last_eval_errors || 0) + ', write: ' + (metrics.last_write_errors || 0));
  if (load) {
    const worker = load.worker_config || {};
    parts.push('Worker slots - read: ' + (worker.read || 0) + ', program: ' + (worker.program || 0) + ', logic: ' + (worker.execute || 0) + ', write: ' + (worker.write || 0));
    const reads = load.reads || {};
    const programs = load.programs || {};
    const logic = load.logic || {};
    const writes = load.writes || {};
    const logicAvg = typeof logic.avg_duration_ms === 'number' ? logic.avg_duration_ms.toFixed(2) + ' ms' : '0.00 ms';
    parts.push('Reads active: ' + (reads.active || 0) + '/' + (reads.total || 0) + ' (disabled ' + (reads.disabled || 0) + ')');
    parts.push('Programs active: ' + (programs.active || 0) + '/' + (programs.total || 0));
    parts.push('Logic executed: ' + (logic.evaluated || 0) + '/' + (logic.total || 0) + ' (skipped ' + (logic.skipped || 0) + ', avg ' + logicAvg + ')');
    parts.push('Writes active: ' + (writes.active || 0) + '/' + (writes.total || 0) + ' (disabled ' + (writes.disabled || 0) + ')');
    parts.push('Goroutines: ' + (load.goroutines || 0));
  }
  metricsBox.innerHTML = parts.map(part => '<div>' + part + '</div>').join('');
}

function renderCells(cells) {
  cellsBody.innerHTML = '';
  cells.forEach(cell => {
    const tr = document.createElement('tr');
    tr.dataset.id = cell.id;
    tr.dataset.kind = cell.kind;
    if (!cell.valid) {
      tr.classList.add('invalid');
    }
    const diagText = cell.diagnosis ? ((cell.diagnosis.code || '') + ' ' + (cell.diagnosis.message || '')).trim() : '';
    const updated = cell.updated_at ? new Date(cell.updated_at).toLocaleString() : '';

    const valueCell = document.createElement('td');
    valueCell.appendChild(createValueEditor(cell));

    const validCell = document.createElement('td');
    validCell.appendChild(createValidEditor(cell));

    const qualityCell = document.createElement('td');
    qualityCell.appendChild(createQualityEditor(cell));

    const applyCell = document.createElement('td');
    const applyBtn = document.createElement('button');
    applyBtn.textContent = 'Apply';
    applyBtn.className = 'apply-cell';
    applyCell.appendChild(applyBtn);

    tr.innerHTML = '<td>' + cell.id + '</td><td>' + cell.kind + '</td>';
    tr.appendChild(valueCell);
    tr.appendChild(validCell);
    tr.appendChild(qualityCell);
    const updatedCell = document.createElement('td');
    updatedCell.textContent = updated || '—';
    tr.appendChild(updatedCell);
    const diagCell = document.createElement('td');
    diagCell.className = 'diag';
    diagCell.textContent = diagText || '—';
    tr.appendChild(diagCell);
    tr.appendChild(applyCell);
    cellsBody.appendChild(tr);
  });
}

function createValueEditor(cell) {
  let input;
  const value = cell.value;
  switch (String(cell.kind).toLowerCase()) {
    case 'bool':
      input = document.createElement('select');
      ['true', 'false'].forEach(optionValue => {
        const option = document.createElement('option');
        option.value = optionValue;
        option.textContent = optionValue;
        if (String(value) === optionValue || (value === true && optionValue === 'true') || (value === false && optionValue === 'false')) {
          option.selected = true;
        }
        input.appendChild(option);
      });
      break;
    case 'number':
      input = document.createElement('input');
      input.type = 'number';
      input.step = 'any';
      if (value !== null && value !== undefined) {
        input.value = value;
      }
      break;
    default:
      input = document.createElement('input');
      input.type = 'text';
      if (value !== null && value !== undefined) {
        input.value = value;
      }
      break;
  }
  input.classList.add('cell-editor', 'cell-value');
  input.disabled = !isPaused();
  return input;
}

function createQualityEditor(cell) {
  const input = document.createElement('input');
  input.type = 'number';
  input.step = 'any';
  if (cell.quality !== null && cell.quality !== undefined) {
    input.value = cell.quality;
  }
  input.placeholder = '—';
  input.classList.add('cell-editor', 'cell-quality');
  input.disabled = !isPaused();
  return input;
}

function createValidEditor(cell) {
  const input = document.createElement('input');
  input.type = 'checkbox';
  input.checked = !!cell.valid;
  input.classList.add('cell-editor', 'cell-valid');
  input.disabled = !isPaused();
  return input;
}

function renderLogic(blocks) {
  logicBody.innerHTML = '';
  blocks.forEach(block => {
    const tr = document.createElement('tr');
    const avg = typeof block.avg_duration_ms === 'number' ? block.avg_duration_ms.toFixed(2) : '0.00';
    tr.innerHTML = '<td>' + block.id + '</td><td>' + block.target + '</td><td>' + block.calls + '</td><td>' + block.skips + '</td><td>' + avg + '</td>';
    logicBody.appendChild(tr);
  });
}

function renderIO(reads, writes) {
  readsBody.innerHTML = '';
  reads.forEach(group => {
    const tr = document.createElement('tr');
    tr.dataset.id = group.id;
    const range = group.start + ' / ' + group.length;
    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.className = 'io-toggle';
    checkbox.checked = !!group.disabled;
    tr.innerHTML = '<td>' + group.id + '</td><td>' + group.function + '</td><td>' + range + '</td><td>' + (group.disabled ? 'Yes' : 'No') + '</td>';
    const toggleCell = document.createElement('td');
    toggleCell.appendChild(checkbox);
    tr.appendChild(toggleCell);
    readsBody.appendChild(tr);
  });

  writesBody.innerHTML = '';
  writes.forEach(target => {
    const tr = document.createElement('tr');
    tr.dataset.id = target.id;
    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.className = 'io-toggle';
    checkbox.checked = !!target.disabled;
    tr.innerHTML = '<td>' + target.id + '</td><td>' + target.cell + '</td><td>' + target.function + '</td><td>' + target.address + '</td><td>' + (target.disabled ? 'Yes' : 'No') + '</td>';
    const toggleCell = document.createElement('td');
    toggleCell.appendChild(checkbox);
    tr.appendChild(toggleCell);
    writesBody.appendChild(tr);
  });
}

function updateEditingState() {
  const paused = isPaused();
  document.querySelectorAll('.cell-editor').forEach(el => {
    el.disabled = !paused;
  });
  document.querySelectorAll('.apply-cell').forEach(btn => {
    btn.disabled = !paused;
  });
}

function submitCellUpdate(row, button) {
  if (!row) {
    return;
  }
  const id = row.dataset.id;
  const kind = (row.dataset.kind || '').toLowerCase();
  const valueInput = row.querySelector('.cell-value');
  const qualityInput = row.querySelector('.cell-quality');
  const validInput = row.querySelector('.cell-valid');
  let value = null;
  if (valueInput) {
    if (kind === 'bool') {
      value = valueInput.value === 'true';
    } else if (kind === 'number') {
      const parsed = parseFloat(valueInput.value);
      if (!Number.isNaN(parsed)) {
        value = parsed;
      }
    } else {
      value = valueInput.value;
    }
  }
  let quality = null;
  if (qualityInput && qualityInput.value !== '') {
    const parsedQuality = parseFloat(qualityInput.value);
    if (!Number.isNaN(parsedQuality)) {
      quality = parsedQuality;
    }
  }
  const valid = validInput ? validInput.checked : true;
  const payload = { id: id, value: valid ? value : null, quality: quality, valid: valid };
  if (button) {
    button.disabled = true;
  }
  fetch('/api/cell', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  }).then(resp => {
    if (!resp.ok) {
      return resp.text().then(text => { throw new Error(text || 'request failed'); });
    }
    return resp.json();
  }).then(() => {
    fetchState();
  }).catch(err => {
    console.error('cell update error', err);
    alert('Cell update failed: ' + err.message);
    if (button) {
      button.disabled = false;
    }
  });
}

function submitIO(kind, id, disabled, checkbox) {
  if (!id) {
    return;
  }
  checkbox.disabled = true;
  fetch('/api/io', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ kind: kind, id: id, disabled: disabled })
  }).then(resp => {
    if (!resp.ok) {
      return resp.text().then(text => { throw new Error(text || 'request failed'); });
    }
    return resp.json();
  }).then(data => {
    renderIO(data.reads || [], data.writes || []);
  }).catch(err => {
    console.error('io control error', err);
    alert('Update failed: ' + err.message);
    checkbox.checked = !disabled;
  }).finally(() => {
    checkbox.disabled = false;
  });
}

function fetchState() {
  fetch('/api/state').then(resp => resp.json()).then(data => {
    currentControl = data.control || null;
    renderCells(data.cells || []);
    renderLogic(data.logic || []);
    renderIO(data.reads || [], data.writes || []);
    if (data.control && typeof data.control.interval_ms === 'number') {
      updateDurationLabel(data.control.interval_ms);
    }
    renderMetrics(data.metrics, data.load, data.control);
    updateEditingState();
  }).catch(err => console.error('state error', err));
}

function postControl(action, payload) {
  const body = Object.assign({ action }, payload || {});
  return fetch('/api/control', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  }).then(resp => {
    if (!resp.ok) {
      return resp.text().then(text => { throw new Error(text || 'request failed'); });
    }
    return resp.json();
  }).then(status => {
    if (status && typeof status.interval_ms === 'number') {
      updateDurationLabel(status.interval_ms);
    }
    return status;
  });
}

runBtn.addEventListener('click', () => postControl('run').catch(err => console.error('control error', err)));
pauseBtn.addEventListener('click', () => postControl('pause').catch(err => console.error('control error', err)));
stepBtn.addEventListener('click', () => postControl('step').catch(err => console.error('control error', err)));
speedRange.addEventListener('input', () => updateDurationLabel(speedRange.value));
speedRange.addEventListener('change', () => postControl('speed', { duration_ms: Number(speedRange.value) }).catch(err => console.error('control error', err)));

fetchState();
setInterval(fetchState, 1000);

cellsBody.addEventListener('click', event => {
  if (event.target.classList.contains('apply-cell')) {
    const row = event.target.closest('tr');
    submitCellUpdate(row, event.target);
  }
});

[readsBody, writesBody].forEach(body => {
  body.addEventListener('change', event => {
    if (event.target.classList.contains('io-toggle')) {
      const row = event.target.closest('tr');
      const table = event.target.closest('table');
      const kind = table ? table.dataset.kind : '';
      submitIO(kind, row ? row.dataset.id : '', event.target.checked, event.target);
    }
  });
});
</script>
</body>
</html>`))
