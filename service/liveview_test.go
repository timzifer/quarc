package service

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/timzifer/quarc/config"
	"github.com/timzifer/quarc/runtime/readers"
	"github.com/timzifer/quarc/runtime/writers"
)

type liveViewReadGroupStub struct {
	status   readers.ReadGroupStatus
	disabled bool
}

func (g *liveViewReadGroupStub) ID() string { return g.status.ID }

func (g *liveViewReadGroupStub) Due(time.Time) bool { return false }

func (g *liveViewReadGroupStub) Perform(time.Time, zerolog.Logger) int { return 0 }

func (g *liveViewReadGroupStub) SetDisabled(disabled bool) {
	g.disabled = disabled
	g.status.Disabled = disabled
}

func (g *liveViewReadGroupStub) Status() readers.ReadGroupStatus {
	g.status.Disabled = g.disabled
	return g.status
}

func (g *liveViewReadGroupStub) Close() {}

type liveViewWriterStub struct {
	status   writers.WriteTargetStatus
	disabled bool
}

func (w *liveViewWriterStub) ID() string { return w.status.ID }

func (w *liveViewWriterStub) Commit(time.Time, zerolog.Logger) int { return 0 }

func (w *liveViewWriterStub) SetDisabled(disabled bool) {
	w.disabled = disabled
	w.status.Disabled = disabled
}

func (w *liveViewWriterStub) Status() writers.WriteTargetStatus {
	w.status.Disabled = w.disabled
	return w.status
}

func (w *liveViewWriterStub) Close() {}

func newLiveViewTestServer(t *testing.T) (*Service, *liveViewReadGroupStub, *liveViewWriterStub, *httptest.Server) {
	t.Helper()

	ctrl := newCycleController(200 * time.Millisecond)
	ctrl.SetMode(controlModePause)

	readStub := &liveViewReadGroupStub{status: readers.ReadGroupStatus{
		ID:           "telemetry:read-1",
		Driver:       "modbus",
		Disabled:     false,
		NextRun:      time.Now().Add(100 * time.Millisecond),
		LastRun:      time.Now(),
		LastDuration: 5 * time.Millisecond,
		Source:       config.ModuleReference{Package: "telemetry"},
		Metadata: map[string]interface{}{
			"function": "holding_registers",
			"start":    uint16(1),
			"length":   uint16(2),
		},
	}}

	writeStub := &liveViewWriterStub{status: writers.WriteTargetStatus{
		ID:           "io:write-1",
		Cell:         "demo:temp",
		Function:     "holding_registers",
		Address:      8,
		Disabled:     false,
		LastAttempt:  time.Now().Add(-2 * time.Second),
		LastWrite:    time.Now().Add(-1 * time.Second),
		LastDuration: 3 * time.Millisecond,
		Source:       config.ModuleReference{Package: "io"},
	}}

	store := &cellStore{cells: map[string]*cell{
		"demo:temp": {
			cfg:   config.CellConfig{ID: "demo:temp", Type: config.ValueKindFloat, Name: "Temperature", Source: config.ModuleReference{Package: "demo"}},
			value: 21.5,
			valid: true,
		},
	}}

	svc := &Service{
		cells:        store,
		controller:   ctrl,
		reads:        []readers.ReadGroup{readStub},
		writes:       []writers.Writer{writeStub},
		metrics:      metrics{CycleCount: 4, LastDuration: 12 * time.Millisecond},
		load:         newSystemLoad(newWorkerSlots(config.WorkerSlots{Read: 1, Program: 1, Execute: 1, Write: 1})),
		bufferGroups: map[string][]string{"telemetry:read-1": {"demo:temp"}},
	}
	svc.load.read.active.Store(1)
	svc.load.write.active.Store(1)

	server := &liveViewServer{logger: zerolog.New(io.Discard), service: svc}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/state", server.handleState)
	mux.HandleFunc("/api/control", server.handleControl)
	mux.HandleFunc("/api/cells/", server.handleCellUpdate)
	mux.HandleFunc("/api/reads/", server.handleReadControl)
	mux.HandleFunc("/api/writes/", server.handleWriteControl)

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return svc, readStub, writeStub, ts
}

func TestLiveViewStateEndpoint(t *testing.T) {
	_, _, _, ts := newLiveViewTestServer(t)

	resp, err := ts.Client().Get(ts.URL + "/api/state")
	if err != nil {
		t.Fatalf("request state: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	var payload struct {
		Cells []struct {
			ID      string `json:"id"`
			Package string `json:"package"`
			LocalID string `json:"local_id"`
			Kind    string `json:"kind"`
			Valid   bool   `json:"valid"`
		} `json:"cells"`
		Control struct {
			Mode string `json:"mode"`
		} `json:"control"`
		Reads []struct {
			ID       string `json:"id"`
			Package  string `json:"package"`
			LocalID  string `json:"local_id"`
			Disabled bool   `json:"disabled"`
		} `json:"reads"`
		Writes []struct {
			ID       string `json:"id"`
			Package  string `json:"package"`
			LocalID  string `json:"local_id"`
			Disabled bool   `json:"disabled"`
		} `json:"writes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode state payload: %v", err)
	}

	if len(payload.Cells) == 0 {
		t.Fatal("expected at least one cell in payload")
	}
	if payload.Cells[0].ID != "demo:temp" {
		t.Fatalf("expected cell id 'demo:temp', got %s", payload.Cells[0].ID)
	}
	if payload.Cells[0].Package != "demo" {
		t.Fatalf("expected cell package 'demo', got %s", payload.Cells[0].Package)
	}
	if payload.Cells[0].LocalID != "temp" {
		t.Fatalf("expected cell local_id 'temp', got %s", payload.Cells[0].LocalID)
	}
	if payload.Control.Mode != string(controlModePause) {
		t.Fatalf("expected control mode pause, got %s", payload.Control.Mode)
	}
	if len(payload.Reads) != 1 || payload.Reads[0].ID != "telemetry:read-1" {
		t.Fatalf("expected read snapshot for telemetry:read-1, got %+v", payload.Reads)
	}
	if payload.Reads[0].Package != "telemetry" {
		t.Fatalf("expected read package 'telemetry', got %s", payload.Reads[0].Package)
	}
	if payload.Reads[0].LocalID != "read-1" {
		t.Fatalf("expected read local_id 'read-1', got %s", payload.Reads[0].LocalID)
	}
	if len(payload.Writes) != 1 || payload.Writes[0].ID != "io:write-1" {
		t.Fatalf("expected write snapshot for io:write-1, got %+v", payload.Writes)
	}
	if payload.Writes[0].Package != "io" {
		t.Fatalf("expected write package 'io', got %s", payload.Writes[0].Package)
	}
	if payload.Writes[0].LocalID != "write-1" {
		t.Fatalf("expected write local_id 'write-1', got %s", payload.Writes[0].LocalID)
	}
}

func TestLiveViewCellUpdate(t *testing.T) {
	svc, _, _, ts := newLiveViewTestServer(t)

	body := bytes.NewBufferString(`{"value": 42, "valid": true}`)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/cells/demo:temp", body)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("perform cell update: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	var cellResp struct {
		ID    string      `json:"id"`
		Value interface{} `json:"value"`
		Valid bool        `json:"valid"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&cellResp); err != nil {
		t.Fatalf("decode cell response: %v", err)
	}

	if cellResp.ID != "demo:temp" {
		t.Fatalf("expected demo:temp cell, got %s", cellResp.ID)
	}
	if !cellResp.Valid {
		t.Fatal("expected cell to remain valid")
	}

	state, err := svc.cells.state("demo:temp")
	if err != nil {
		t.Fatalf("fetch updated state: %v", err)
	}
	if state.Value == nil {
		t.Fatal("expected updated value to be stored")
	}
}

func TestLiveViewCellUpdateUnknownCell(t *testing.T) {
	_, _, _, ts := newLiveViewTestServer(t)

	body := bytes.NewBufferString(`{"value": 13}`)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/cells/unknown", body)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("perform cell update: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for unknown cell, got %d", resp.StatusCode)
	}
}

func TestLiveViewReadToggle(t *testing.T) {
	_, readStub, _, ts := newLiveViewTestServer(t)

	body := bytes.NewBufferString(`{"disabled": true}`)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/reads/telemetry:read-1", body)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("perform read toggle: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	if !readStub.disabled {
		t.Fatal("expected read group to be disabled")
	}

	var toggleResp struct {
		ID       string `json:"id"`
		Disabled bool   `json:"disabled"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&toggleResp); err != nil {
		t.Fatalf("decode toggle response: %v", err)
	}
	if !toggleResp.Disabled {
		t.Fatal("expected disabled flag in response")
	}
}

func TestLiveViewReadToggleNotFound(t *testing.T) {
	_, _, _, ts := newLiveViewTestServer(t)

	body := bytes.NewBufferString(`{"disabled": true}`)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/reads/missing", body)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("perform read toggle: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for missing read group, got %d", resp.StatusCode)
	}
}

func TestLiveViewWriteToggle(t *testing.T) {
	_, _, writeStub, ts := newLiveViewTestServer(t)

	body := bytes.NewBufferString(`{"disabled": true}`)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/writes/io:write-1", body)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("perform write toggle: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	if !writeStub.disabled {
		t.Fatal("expected write target to be disabled")
	}

	var toggleResp struct {
		ID       string `json:"id"`
		Disabled bool   `json:"disabled"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&toggleResp); err != nil {
		t.Fatalf("decode toggle response: %v", err)
	}
	if !toggleResp.Disabled {
		t.Fatal("expected disabled flag in response")
	}
}

func TestLiveViewWriteToggleNotFound(t *testing.T) {
	_, _, _, ts := newLiveViewTestServer(t)

	body := bytes.NewBufferString(`{"disabled": true}`)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/writes/missing", body)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("perform write toggle: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for missing write target, got %d", resp.StatusCode)
	}
}
