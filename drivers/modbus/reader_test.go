package modbus

import (
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/timzifer/quarc/config"
	runtimeReaders "github.com/timzifer/quarc/runtime/readers"
	"github.com/timzifer/quarc/runtime/state"
)

type testReadCell struct {
	mu      sync.Mutex
	cfg     config.CellConfig
	value   interface{}
	invalid bool
}

func newTestReadCell(id string, kind config.ValueKind) *testReadCell {
	return &testReadCell{cfg: config.CellConfig{ID: id, Type: kind}}
}

func (c *testReadCell) Config() config.CellConfig {
	return c.cfg
}

func (c *testReadCell) SetValue(value interface{}, _ time.Time, _ *float64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value = value
	c.invalid = false
	return nil
}

func (c *testReadCell) MarkInvalid(_ time.Time, _ string, _ string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.invalid = true
	c.value = nil
}

func (c *testReadCell) CurrentValue() (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value, !c.invalid
}

type testCellStore struct {
	cells map[string]state.Cell
}

func newTestCellStore(cells ...*testReadCell) *testCellStore {
	store := &testCellStore{cells: make(map[string]state.Cell, len(cells))}
	for _, cell := range cells {
		store.cells[cell.cfg.ID] = cell
	}
	return store
}

func (s *testCellStore) Get(id string) (state.Cell, error) {
	cell, ok := s.cells[id]
	if !ok {
		return nil, fmt.Errorf("unknown cell %s", id)
	}
	return cell, nil
}

type readInvocation struct {
	start  uint16
	length uint16
}

type testClient struct {
	mu         sync.Mutex
	responses  map[readInvocation][]byte
	calls      []readInvocation
	closeCalls int
}

func newTestClient(responses map[readInvocation][]byte) *testClient {
	return &testClient{responses: responses}
}

func (c *testClient) recordCall(start, length uint16) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	call := readInvocation{start: start, length: length}
	c.calls = append(c.calls, call)
	payload, ok := c.responses[call]
	if !ok {
		return nil, fmt.Errorf("unexpected read %v", call)
	}
	return payload, nil
}

func (c *testClient) ReadCoils(address, quantity uint16) ([]byte, error) {
	return c.recordCall(address, quantity)
}

func (c *testClient) ReadDiscreteInputs(address, quantity uint16) ([]byte, error) {
	return c.recordCall(address, quantity)
}

func (c *testClient) ReadHoldingRegisters(address, quantity uint16) ([]byte, error) {
	return c.recordCall(address, quantity)
}

func (c *testClient) ReadInputRegisters(address, quantity uint16) ([]byte, error) {
	return c.recordCall(address, quantity)
}

func (c *testClient) WriteSingleCoil(uint16, uint16) ([]byte, error) {
	return nil, fmt.Errorf("unexpected write")
}

func (c *testClient) WriteSingleRegister(uint16, uint16) ([]byte, error) {
	return nil, fmt.Errorf("unexpected write")
}

func (c *testClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeCalls++
	return nil
}

func TestReadGroupPlansSegmentsAndReadsIndividually(t *testing.T) {
	cellA := newTestReadCell("cell.a", config.ValueKindInteger)
	cellB := newTestReadCell("cell.b", config.ValueKindInteger)
	cellC := newTestReadCell("cell.c", config.ValueKindInteger)
	store := newTestCellStore(cellA, cellB, cellC)

	responses := map[readInvocation][]byte{
		{start: 0, length: 1}: {0x00, 0x02},
		{start: 4, length: 1}: {0x00, 0x04},
		{start: 9, length: 1}: {0x00, 0x08},
	}
	client := newTestClient(responses)
	factory := func(config.EndpointConfig) (Client, error) {
		return client, nil
	}

	cfg := config.ReadGroupConfig{
		ID:  "segmented",
		TTL: config.Duration{Duration: time.Second},
		Signals: []config.ReadSignalConfig{
			{Cell: "cell.a", Offset: 0, Type: config.ValueKindInteger},
			{Cell: "cell.b", Offset: 4, Type: config.ValueKindInteger},
			{Cell: "cell.c", Offset: 9, Type: config.ValueKindInteger},
		},
		Driver: config.DriverConfig{Name: "modbus", Settings: []byte(`{
                        "function": "holding",
                        "start": 0,
                        "length": 16,
                        "max_gap_size": 1
                }`)},
	}

	deps := runtimeReaders.ReaderDependencies{Cells: store}
	group, err := NewReaderFactory(factory)(cfg, deps)
	require.NoError(t, err)
	t.Cleanup(func() {
		group.Close()
	})

	rg, ok := group.(*readGroup)
	require.True(t, ok)
	require.Len(t, rg.requests, 3)
	require.Equal(t, uint16(0), rg.requests[0].BaseOffset)
	require.Equal(t, uint16(0), rg.requests[0].Start)
	require.Equal(t, uint16(1), rg.requests[0].Length)
	require.Equal(t, uint16(4), rg.requests[1].BaseOffset)
	require.Equal(t, uint16(4), rg.requests[1].Start)
	require.Equal(t, uint16(1), rg.requests[1].Length)
	require.Equal(t, uint16(9), rg.requests[2].BaseOffset)
	require.Equal(t, uint16(9), rg.requests[2].Start)
	require.Equal(t, uint16(1), rg.requests[2].Length)

	status := rg.Status()
	require.Equal(t, 3, status.Metadata["request_count"])
	require.Equal(t, uint16(1), status.Metadata["max_gap_size"])

	logger := zerolog.New(io.Discard)
	now := time.Now()
	require.Equal(t, 0, group.Perform(now, logger))

	require.Equal(t, []readInvocation{{start: 0, length: 1}, {start: 4, length: 1}, {start: 9, length: 1}}, client.calls)

	valueA, validA := cellA.CurrentValue()
	require.True(t, validA)
	require.Equal(t, int64(2), valueA)
	valueB, validB := cellB.CurrentValue()
	require.True(t, validB)
	require.Equal(t, int64(4), valueB)
	valueC, validC := cellC.CurrentValue()
	require.True(t, validC)
	require.Equal(t, int64(8), valueC)
}
