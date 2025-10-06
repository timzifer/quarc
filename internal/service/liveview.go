package service

import (
	"context"
	"encoding/json"
	"html/template"
	"net"
	"net/http"
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
	Cells   []liveCell    `json:"cells"`
	Control controlStatus `json:"control"`
	Metrics metrics       `json:"metrics"`
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
<table id="cells">
<thead>
<tr><th>ID</th><th>Type</th><th>Value</th><th>Valid</th><th>Quality</th><th>Updated</th><th>Diagnosis</th></tr>
</thead>
<tbody></tbody>
</table>
<script>
const runBtn = document.getElementById('runBtn');
const pauseBtn = document.getElementById('pauseBtn');
const stepBtn = document.getElementById('stepBtn');
const speedRange = document.getElementById('speedRange');
const durationValue = document.getElementById('durationValue');
const cellsBody = document.querySelector('#cells tbody');
const metricsBox = document.getElementById('metrics');

function updateDurationLabel(ms) {
  durationValue.textContent = ms + ' ms';
  if (Number(speedRange.value) !== Number(ms)) {
    speedRange.value = ms;
  }
}

function renderMetrics(metrics) {
  if (!metrics) {
    metricsBox.textContent = '';
    return;
  }
  const durationMs = metrics.LastDuration ? (metrics.LastDuration / 1e6).toFixed(2) + ' ms' : '0 ms';
  metricsBox.textContent = 'Cycle count: ' + (metrics.CycleCount || 0) + ' | Last duration: ' + durationMs;
}

function renderCells(cells) {
  cellsBody.innerHTML = '';
  cells.forEach(cell => {
    const tr = document.createElement('tr');
    if (!cell.valid) {
      tr.classList.add('invalid');
    }
    const diagText = cell.diagnosis ? ((cell.diagnosis.code || '') + ' ' + (cell.diagnosis.message || '')).trim() : '';
    const qualityText = cell.quality === null ? '—' : Number(cell.quality).toFixed(3);
    const valueText = cell.valid && cell.value !== null && cell.value !== undefined ? cell.value : '—';
    const updated = cell.updated_at ? new Date(cell.updated_at).toLocaleString() : '';
    tr.innerHTML = '<td>' + cell.id + '</td><td>' + cell.kind + '</td><td>' + valueText + '</td><td>' + (cell.valid ? 'Yes' : 'No') + '</td><td>' + qualityText + '</td><td>' + updated + '</td><td class="diag">' + diagText + '</td>';
    cellsBody.appendChild(tr);
  });
}

function fetchState() {
  fetch('/api/state').then(resp => resp.json()).then(data => {
    renderCells(data.cells || []);
    if (data.control && typeof data.control.interval_ms === 'number') {
      updateDurationLabel(data.control.interval_ms);
    }
    renderMetrics(data.metrics);
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
</script>
</body>
</html>`))
