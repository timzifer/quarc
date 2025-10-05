package service

import (
	"fmt"
	"math"
	"sync"
	"time"

	"modbus_processor/internal/config"
)

type diagnosis struct {
	Code      string
	Message   string
	Timestamp time.Time
}

type cell struct {
	cfg    config.CellConfig
	mu     sync.RWMutex
	value  interface{}
	valid  bool
	diag   *diagnosis
	update time.Time
}

type snapshotValue struct {
	Value interface{}
	Valid bool
	Kind  config.ValueKind
}

// CellDiagnosis is a public representation of a diagnostic entry associated with a cell.
type CellDiagnosis struct {
	Code      string
	Message   string
	Timestamp time.Time
}

// CellState exposes the current state of a cell for external inspection.
type CellState struct {
	ID        string
	Kind      config.ValueKind
	Value     interface{}
	Valid     bool
	Diagnosis *CellDiagnosis
	UpdatedAt time.Time
}

type cellStore struct {
	mu    sync.RWMutex
	cells map[string]*cell
}

func newCellStore(cfgs []config.CellConfig) (*cellStore, error) {
	store := &cellStore{cells: make(map[string]*cell, len(cfgs))}
	for _, cfg := range cfgs {
		if cfg.ID == "" {
			return nil, fmt.Errorf("cell id must not be empty")
		}
		if _, ok := store.cells[cfg.ID]; ok {
			return nil, fmt.Errorf("duplicate cell id %q", cfg.ID)
		}
		if cfg.Type == "" {
			return nil, fmt.Errorf("cell %s missing type", cfg.ID)
		}
		c := &cell{cfg: cfg, valid: false}
		if cfg.Constant != nil {
			if err := c.setValue(cfg.Constant, time.Time{}); err != nil {
				return nil, fmt.Errorf("cell %s constant: %w", cfg.ID, err)
			}
		}
		store.cells[cfg.ID] = c
	}
	return store, nil
}

func (s *cellStore) mustGet(id string) (*cell, error) {
	if id == "" {
		return nil, fmt.Errorf("cell id must not be empty")
	}
	s.mu.RLock()
	c, ok := s.cells[id]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown cell %q", id)
	}
	return c, nil
}

func (s *cellStore) snapshot() map[string]*snapshotValue {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := make(map[string]*snapshotValue, len(s.cells))
	for id, c := range s.cells {
		snap[id] = c.asSnapshotValue()
	}
	return snap
}

func (c *cell) setValue(value interface{}, ts time.Time) error {
	converted, err := convertValue(c.cfg.Type, value)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.value = converted
	c.valid = true
	c.diag = nil
	c.update = ts
	c.mu.Unlock()
	return nil
}

func (c *cell) markInvalid(ts time.Time, code, message string) {
	c.mu.Lock()
	c.value = nil
	c.valid = false
	c.diag = &diagnosis{Code: code, Message: message, Timestamp: ts}
	c.update = ts
	c.mu.Unlock()
}

func (c *cell) asSnapshotValue() *snapshotValue {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return &snapshotValue{Value: cloneValue(c.value), Valid: c.valid, Kind: c.cfg.Type}
}

func convertValue(kind config.ValueKind, value interface{}) (interface{}, error) {
	switch kind {
	case config.ValueKindBool:
		switch v := value.(type) {
		case bool:
			return v, nil
		case int:
			return v != 0, nil
		case int8:
			return v != 0, nil
		case int16:
			return v != 0, nil
		case int32:
			return v != 0, nil
		case int64:
			return v != 0, nil
		case uint8:
			return v != 0, nil
		case float64:
			return v != 0, nil
		default:
			return nil, fmt.Errorf("expected bool-compatible value, got %T", value)
		}
	case config.ValueKindNumber:
		switch v := value.(type) {
		case float64:
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return nil, fmt.Errorf("invalid float value %v", v)
			}
			return v, nil
		case float32:
			return float64(v), nil
		case int:
			return float64(v), nil
		case int8:
			return float64(v), nil
		case int16:
			return float64(v), nil
		case int32:
			return float64(v), nil
		case int64:
			return float64(v), nil
		case uint16:
			return float64(v), nil
		case uint8:
			return float64(v), nil
		case uint32:
			return float64(v), nil
		case uint64:
			return float64(v), nil
		case bool:
			if v {
				return float64(1), nil
			}
			return float64(0), nil
		default:
			return nil, fmt.Errorf("expected number-compatible value, got %T", value)
		}
	case config.ValueKindString:
		switch v := value.(type) {
		case string:
			return v, nil
		default:
			return nil, fmt.Errorf("expected string value, got %T", value)
		}
	default:
		return nil, fmt.Errorf("unsupported value kind %q", kind)
	}
}

func cloneValue(value interface{}) interface{} {
	switch v := value.(type) {
	case string:
		return string([]byte(v))
	default:
		return v
	}
}

func (s *cellStore) ids() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.cells))
	for id := range s.cells {
		out = append(out, id)
	}
	return out
}

func (c *cell) currentValue() (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.valid {
		return nil, false
	}
	return cloneValue(c.value), true
}

func (c *cell) state() CellState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var diag *CellDiagnosis
	if c.diag != nil {
		diag = &CellDiagnosis{Code: c.diag.Code, Message: c.diag.Message, Timestamp: c.diag.Timestamp}
	}
	return CellState{
		ID:        c.cfg.ID,
		Kind:      c.cfg.Type,
		Value:     cloneValue(c.value),
		Valid:     c.valid,
		Diagnosis: diag,
		UpdatedAt: c.update,
	}
}

func (s *cellStore) state(id string) (CellState, error) {
	cell, err := s.mustGet(id)
	if err != nil {
		return CellState{}, err
	}
	return cell.state(), nil
}
