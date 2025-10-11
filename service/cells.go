package service

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"github.com/timzifer/modbus_processor/config"

	serviceio "github.com/timzifer/modbus_processor/serviceio"
)

type diagnosis struct {
	Code      string
	Message   string
	Timestamp time.Time
}

type cell struct {
	cfg     config.CellConfig
	mu      sync.RWMutex
	value   interface{}
	valid   bool
	quality *float64
	diag    *diagnosis
	update  time.Time
}

type manualCellUpdate struct {
	value      interface{}
	hasValue   bool
	quality    *float64
	qualitySet bool
	valid      *bool
}

type snapshotValue struct {
	Value   interface{}
	Valid   bool
	Kind    config.ValueKind
	Quality *float64
}

// CellDiagnosis is a public representation of a diagnostic entry associated with a cell.
type CellDiagnosis struct {
	Code      string
	Message   string
	Timestamp time.Time
}

// CellState exposes the current state of a cell for external inspection.
type CellState struct {
	ID          string
	Name        string
	Description string
	Kind        config.ValueKind
	Value       interface{}
	Valid       bool
	Quality     *float64
	Diagnosis   *CellDiagnosis
	UpdatedAt   *time.Time
	Source      config.ModuleReference
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
			if err := c.setValue(cfg.Constant, time.Time{}, nil); err != nil {
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

func (s *cellStore) Get(id string) (serviceio.Cell, error) {
	return s.mustGet(id)
}

func (s *cellStore) snapshot() map[string]*snapshotValue {
	return s.snapshotInto(nil)
}

func (s *cellStore) snapshotInto(buf map[string]*snapshotValue) map[string]*snapshotValue {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if buf == nil {
		buf = make(map[string]*snapshotValue, len(s.cells))
	}

	if len(buf) > len(s.cells) {
		for id := range buf {
			if _, exists := s.cells[id]; !exists {
				delete(buf, id)
			}
		}
	}

	for id, c := range s.cells {
		buf[id] = c.writeSnapshot(buf[id])
	}
	return buf
}

func (c *cell) setValue(value interface{}, ts time.Time, quality *float64) error {
	converted, err := convertValue(c.cfg.Type, value)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.value = converted
	c.valid = true
	c.quality = cloneQuality(quality)
	c.diag = nil
	c.update = ts
	c.mu.Unlock()
	return nil
}

func (c *cell) markInvalid(ts time.Time, code, message string) {
	c.mu.Lock()
	c.value = nil
	c.valid = false
	c.quality = nil
	c.diag = &diagnosis{Code: code, Message: message, Timestamp: ts}
	c.update = ts
	c.mu.Unlock()
}

func (c *cell) Config() config.CellConfig {
	return c.cfg
}

func (c *cell) SetValue(value interface{}, ts time.Time, quality *float64) error {
	return c.setValue(value, ts, quality)
}

func (c *cell) MarkInvalid(ts time.Time, code, message string) {
	c.markInvalid(ts, code, message)
}

func (c *cell) CurrentValue() (interface{}, bool) {
	return c.currentValue()
}

func (c *cell) applyManualUpdate(ts time.Time, update manualCellUpdate) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if update.valid != nil && !*update.valid {
		c.value = nil
		c.valid = false
		c.quality = nil
		c.diag = nil
		c.update = ts
		return nil
	}

	if update.hasValue {
		converted, err := convertValue(c.cfg.Type, update.value)
		if err != nil {
			return err
		}
		c.value = converted
		c.valid = true
		c.diag = nil
	} else if update.valid != nil && *update.valid && !c.valid {
		return fmt.Errorf("cell %s requires a value to mark it valid", c.cfg.ID)
	}

	if update.qualitySet {
		c.quality = cloneQuality(update.quality)
	} else if update.hasValue {
		c.quality = cloneQuality(c.quality)
	}

	if update.hasValue || update.qualitySet || (update.valid != nil && *update.valid) {
		c.update = ts
	}
	return nil
}

func (c *cell) asSnapshotValue() *snapshotValue {
	return c.writeSnapshot(nil)
}

func (c *cell) writeSnapshot(dst *snapshotValue) *snapshotValue {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if dst == nil {
		dst = &snapshotValue{}
	}
	dst.Value = cloneValue(c.value)
	dst.Valid = c.valid
	dst.Kind = c.cfg.Type
	dst.Quality = cloneQuality(c.quality)
	return dst
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
	case config.ValueKindNumber, config.ValueKindFloat:
		return convertFloatValue(value)
	case config.ValueKindInteger:
		return convertIntegerValue(value)
	case config.ValueKindDecimal:
		return convertDecimalValue(value)
	case config.ValueKindString:
		switch v := value.(type) {
		case string:
			return v, nil
		default:
			return nil, fmt.Errorf("expected string value, got %T", value)
		}
	case config.ValueKindDate:
		return convertDateValue(value)
	default:
		return nil, fmt.Errorf("unsupported value kind %q", kind)
	}
}

func convertFloatValue(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, fmt.Errorf("invalid float value %v", v)
		}
		return v, nil
	case float32:
		return convertFloatValue(float64(v))
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
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("parse float from string: %w", err)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("expected number-compatible value, got %T", value)
	}
}

func convertIntegerValue(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("value %d overflows int64", v)
		}
		return int64(v), nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, fmt.Errorf("invalid float value %v", v)
		}
		return int64(v), nil
	case float32:
		return int64(v), nil
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse integer from string: %w", err)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("expected integer-compatible value, got %T", value)
	}
}

func convertDecimalValue(value interface{}) (decimal.Decimal, error) {
	switch v := value.(type) {
	case decimal.Decimal:
		return v, nil
	case *decimal.Decimal:
		if v == nil {
			return decimal.Zero, fmt.Errorf("decimal pointer is nil")
		}
		return *v, nil
	case int:
		return decimal.NewFromInt(int64(v)), nil
	case int8:
		return decimal.NewFromInt(int64(v)), nil
	case int16:
		return decimal.NewFromInt(int64(v)), nil
	case int32:
		return decimal.NewFromInt(int64(v)), nil
	case int64:
		return decimal.NewFromInt(v), nil
	case uint:
		return decimal.NewFromInt(int64(v)), nil
	case uint8:
		return decimal.NewFromInt(int64(v)), nil
	case uint16:
		return decimal.NewFromInt(int64(v)), nil
	case uint32:
		return decimal.NewFromInt(int64(v)), nil
	case uint64:
		if v > math.MaxInt64 {
			return decimal.Zero, fmt.Errorf("value %d overflows supported range", v)
		}
		return decimal.NewFromInt(int64(v)), nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return decimal.Zero, fmt.Errorf("invalid float value %v", v)
		}
		return decimal.RequireFromString(strconv.FormatFloat(v, 'f', -1, 64)), nil
	case float32:
		return decimal.RequireFromString(strconv.FormatFloat(float64(v), 'f', -1, 32)), nil
	case string:
		dec, err := decimal.NewFromString(v)
		if err != nil {
			return decimal.Zero, fmt.Errorf("parse decimal from string: %w", err)
		}
		return dec, nil
	default:
		return decimal.Zero, fmt.Errorf("expected decimal-compatible value, got %T", value)
	}
}

func convertDateValue(value interface{}) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		if v == "" {
			return time.Time{}, fmt.Errorf("date string is empty")
		}
		layouts := []string{time.RFC3339, "2006-01-02", time.RFC3339Nano}
		for _, layout := range layouts {
			parsed, err := time.Parse(layout, v)
			if err == nil {
				return parsed, nil
			}
		}
		return time.Time{}, fmt.Errorf("parse date value %q: unsupported format", v)
	default:
		return time.Time{}, fmt.Errorf("expected date-compatible value, got %T", value)
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

func cloneQuality(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
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
	var updated *time.Time
	if !c.update.IsZero() {
		ts := c.update
		updated = &ts
	}
	return CellState{
		ID:          c.cfg.ID,
		Name:        c.cfg.Name,
		Description: c.cfg.Description,
		Kind:        c.cfg.Type,
		Value:       cloneValue(c.value),
		Valid:       c.valid,
		Quality:     cloneQuality(c.quality),
		Diagnosis:   diag,
		UpdatedAt:   updated,
		Source:      c.cfg.Source,
	}
}

func (s *cellStore) state(id string) (CellState, error) {
	cell, err := s.mustGet(id)
	if err != nil {
		return CellState{}, err
	}
	return cell.state(), nil
}

func (s *cellStore) updateManual(id string, ts time.Time, update manualCellUpdate) (CellState, error) {
	cell, err := s.mustGet(id)
	if err != nil {
		return CellState{}, err
	}
	if err := cell.applyManualUpdate(ts, update); err != nil {
		return CellState{}, err
	}
	return cell.state(), nil
}

func (s *cellStore) states() []CellState {
	ids := s.ids()
	sort.Strings(ids)
	out := make([]CellState, 0, len(ids))
	for _, id := range ids {
		state, err := s.state(id)
		if err != nil {
			continue
		}
		out = append(out, state)
	}
	return out
}
