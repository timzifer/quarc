package random

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/timzifer/quarc/config"
	"github.com/timzifer/quarc/runtime/readers"
	"github.com/timzifer/quarc/runtime/state"
)

type fakeCell struct {
	id   string
	kind config.ValueKind

	mu       sync.Mutex
	value    interface{}
	quality  *float64
	invalid  bool
	diagCode string
	diagMsg  string
}

func newFakeCell(id string, kind config.ValueKind) *fakeCell {
	return &fakeCell{id: id, kind: kind}
}

func (c *fakeCell) Config() config.CellConfig {
	return config.CellConfig{ID: c.id, Type: c.kind}
}

func (c *fakeCell) SetValue(value interface{}, _ time.Time, quality *float64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value = value
	c.quality = cloneQuality(quality)
	c.invalid = false
	c.diagCode = ""
	c.diagMsg = ""
	return nil
}

func (c *fakeCell) MarkInvalid(_ time.Time, code, message string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value = nil
	c.quality = nil
	c.invalid = true
	c.diagCode = code
	c.diagMsg = message
}

func (c *fakeCell) CurrentValue() (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value, !c.invalid
}

func (c *fakeCell) diagnostics() (string, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.diagCode, c.diagMsg
}

func (c *fakeCell) snapshot() (interface{}, *float64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value, cloneQuality(c.quality), !c.invalid
}

type fakeCellStore struct {
	cells map[string]state.Cell
}

func newFakeCellStore(cells ...*fakeCell) *fakeCellStore {
	store := &fakeCellStore{cells: make(map[string]state.Cell, len(cells))}
	for _, cell := range cells {
		store.cells[cell.id] = cell
	}
	return store
}

func (s *fakeCellStore) Get(id string) (state.Cell, error) {
	cell, ok := s.cells[id]
	if !ok {
		return nil, fmt.Errorf("unknown cell %s", id)
	}
	return cell, nil
}

func ptrFloat64(v float64) *float64 { return &v }

func ptrInt64(v int64) *int64 { return &v }

func TestRandomReadFactoryProducesValues(t *testing.T) {
	tempCell := newFakeCell("temperature", config.ValueKindNumber)
	boolCell := newFakeCell("switch", config.ValueKindBool)
	store := newFakeCellStore(tempCell, boolCell)
	buffers := readers.NewSignalBufferStore()
	for _, id := range []string{"temperature", "switch"} {
		_, err := buffers.Configure(id, 4, []readers.SignalBufferAggregationConfig{{Cell: id}})
		require.NoError(t, err)
	}

	settings := Settings{
		Source: "pseudo",
		Seed:   ptrInt64(42),
		Defaults: SignalSettings{
			Quality: ptrFloat64(0.9),
		},
		Signals: map[string]SignalSettings{
			"temperature": {Min: ptrFloat64(20), Max: ptrFloat64(25)},
			"switch":      {TrueProbability: ptrFloat64(1)},
		},
	}
	rawSettings, err := json.Marshal(settings)
	require.NoError(t, err)

	cfg := config.ReadGroupConfig{
		ID:  "random.read",
		TTL: config.Duration{Duration: time.Second},
		Signals: []config.ReadSignalConfig{
			{Cell: "temperature", Type: config.ValueKindNumber},
			{Cell: "switch", Type: config.ValueKindBool},
		},
		DriverSettings: rawSettings,
	}

	deps := readers.ReaderDependencies{Cells: store, Buffers: buffers}
	group, err := NewReadFactory()(cfg, deps)
	require.NoError(t, err)

	logger := zerolog.New(io.Discard)
	now := time.Now()
	require.Equal(t, 0, group.Perform(now, logger))

	tempValue, tempQuality, tempValid := tempCell.snapshot()
	require.True(t, tempValid)
	require.IsType(t, float64(0), tempValue)
	require.GreaterOrEqual(t, tempValue.(float64), 20.0)
	require.LessOrEqual(t, tempValue.(float64), 25.0)
	require.NotNil(t, tempQuality)
	require.InDelta(t, 0.9, *tempQuality, 1e-9)

	boolValue, _, boolValid := boolCell.snapshot()
	require.True(t, boolValid)
	require.Equal(t, true, boolValue)

	status := group.Status()
	require.Equal(t, "random.read", status.ID)
	require.Equal(t, "random", status.Function)
	require.True(t, status.NextRun.After(now))
	require.NotZero(t, status.LastDuration)
	require.Contains(t, status.Buffers, "temperature")

	require.False(t, group.Due(now.Add(200*time.Millisecond)))
	require.True(t, group.Due(now.Add(1200*time.Millisecond)))

	buf, err := buffers.Get("temperature")
	require.NoError(t, err)
	flush, err := buf.Flush()
	require.NoError(t, err)
	require.Equal(t, 1, flush.Buffered)
}

func TestRandomReadFactoryUnknownSource(t *testing.T) {
	cell := newFakeCell("value", config.ValueKindInteger)
	store := newFakeCellStore(cell)
	buffers := readers.NewSignalBufferStore()
	_, err := buffers.Configure("value", 1, []readers.SignalBufferAggregationConfig{{Cell: "value"}})
	require.NoError(t, err)

	raw, err := json.Marshal(Settings{Source: "mystery"})
	require.NoError(t, err)

	cfg := config.ReadGroupConfig{
		ID: "mystery", TTL: config.Duration{Duration: time.Second},
		Signals:        []config.ReadSignalConfig{{Cell: "value", Type: config.ValueKindInteger}},
		DriverSettings: raw,
	}
	deps := readers.ReaderDependencies{Cells: store, Buffers: buffers}
	_, err = NewReadFactory()(cfg, deps)
	require.Error(t, err)
}

func TestRandomReadGroupUnsupportedType(t *testing.T) {
	cell := newFakeCell("date", config.ValueKindDate)
	store := newFakeCellStore(cell)
	buffers := readers.NewSignalBufferStore()
	_, err := buffers.Configure("date", 1, []readers.SignalBufferAggregationConfig{{Cell: "date"}})
	require.NoError(t, err)

	cfg := config.ReadGroupConfig{
		ID:      "date-group",
		TTL:     config.Duration{Duration: time.Second},
		Signals: []config.ReadSignalConfig{{Cell: "date", Type: config.ValueKindDate}},
	}
	deps := readers.ReaderDependencies{Cells: store, Buffers: buffers}
	group, err := NewReadFactory()(cfg, deps)
	require.NoError(t, err)

	logger := zerolog.New(io.Discard)
	now := time.Now()
	require.Equal(t, 1, group.Perform(now, logger))

	_, _, valid := cell.snapshot()
	require.False(t, valid)
	code, msg := cell.diagnostics()
	require.Equal(t, "random.generate", code)
	require.Contains(t, msg, "unsupported")
}
