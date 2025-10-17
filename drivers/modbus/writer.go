package modbus

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/shopspring/decimal"
	"github.com/timzifer/quarc/config"

	"github.com/timzifer/quarc/runtime/state"
	runtimeWriters "github.com/timzifer/quarc/runtime/writers"
)

type writeTarget struct {
	cfg           config.WriteTargetConfig
	cell          state.Cell
	clientFactory ClientFactory
	client        Client
	lastValue     interface{}
	lastWrite     time.Time
	mu            sync.RWMutex
	disabled      atomic.Bool
	lastDuration  time.Duration
	lastAttempt   time.Time
}

// NewWriterFactory builds a Modbus write target factory.
func NewWriterFactory(factory ClientFactory) runtimeWriters.WriterFactory {
	if factory == nil {
		factory = NewTCPClientFactory()
	}
	return func(cfg config.WriteTargetConfig, deps runtimeWriters.WriterDependencies) (runtimeWriters.Writer, error) {
		return newWriteTarget(cfg, deps, factory)
	}
}

func newWriteTarget(cfg config.WriteTargetConfig, deps runtimeWriters.WriterDependencies, factory ClientFactory) (runtimeWriters.Writer, error) {
	if deps.Cells == nil {
		return nil, fmt.Errorf("write target %s: missing cell store", cfg.ID)
	}
	if cfg.ID == "" {
		return nil, fmt.Errorf("write target id must not be empty")
	}
	resolved, err := resolveWriteTarget(cfg)
	if err != nil {
		return nil, err
	}
	if resolved.Function == "" {
		return nil, fmt.Errorf("write target %s missing function", resolved.ID)
	}
	if resolved.Cell == "" {
		return nil, fmt.Errorf("write target %s missing cell", resolved.ID)
	}
	cell, err := deps.Cells.Get(resolved.Cell)
	if err != nil {
		return nil, fmt.Errorf("write target %s: %w", resolved.ID, err)
	}
	target := &writeTarget{cfg: resolved, cell: cell, clientFactory: factory}
	target.disabled.Store(resolved.Disable)
	return target, nil
}

func (t *writeTarget) ID() string {
	return t.cfg.ID
}

func (t *writeTarget) Commit(now time.Time, logger zerolog.Logger) int {
	if t.disabled.Load() {
		return 0
	}
	errors := 0
	logger.Trace().Str("target", t.cfg.ID).Msg("write target evaluation started")
	if t.cell == nil {
		return 0
	}
	current, valid := t.cell.CurrentValue()
	if !valid {
		logger.Trace().Str("target", t.cfg.ID).Msg("skipping invalid cell")
		return 0
	}

	lastWrite := t.lastWriteTime()
	if limit := t.cfg.RateLimit.Duration; limit > 0 && !lastWrite.IsZero() {
		if now.Before(lastWrite.Add(limit)) {
			logger.Trace().Str("target", t.cfg.ID).Dur("rate_limit", limit).Msg("skipping due to rate limit")
			return 0
		}
	}

	if !t.shouldWrite(current) {
		logger.Trace().Str("target", t.cfg.ID).Msg("no significant change detected")
		return 0
	}

	if t.client == nil {
		logger.Trace().Str("target", t.cfg.ID).Msg("creating modbus write client")
	}
	client, err := t.ensureClient()
	if err != nil {
		t.closeClient()
		logger.Error().Err(err).Str("target", t.cfg.ID).Msg("write client unavailable")
		t.recordAttempt(now, 0)
		return 1
	}

	start := time.Now()
	if err := t.performWrite(client, current); err != nil {
		t.closeClient()
		logger.Error().Err(err).Str("target", t.cfg.ID).Msg("modbus write failed")
		t.recordAttempt(now, time.Since(start))
		return 1
	}

	duration := time.Since(start)
	t.updateAfterWrite(current, now, duration)
	logger.Trace().Str("target", t.cfg.ID).Time("timestamp", now).Dur("duration", duration).Msg("write target committed")
	return errors
}

func (t *writeTarget) shouldWrite(value interface{}) bool {
	t.mu.RLock()
	lastValue := cloneValue(t.lastValue)
	t.mu.RUnlock()
	if lastValue == nil {
		return true
	}
	cellCfg := t.cell.Config()
	switch cellCfg.Type {
	case config.ValueKindBool, config.ValueKindString, config.ValueKindDate:
		return value != lastValue
	case config.ValueKindNumber, config.ValueKindFloat:
		current, ok := toFloat(value)
		if !ok {
			return true
		}
		previous, ok := toFloat(lastValue)
		if !ok {
			return true
		}
		deadband := t.cfg.Deadband
		if deadband < 0 {
			deadband = 0
		}
		return math.Abs(current-previous) > deadband
	case config.ValueKindInteger:
		current, ok := toInt(value)
		if !ok {
			return true
		}
		previous, ok := toInt(lastValue)
		if !ok {
			return true
		}
		deadband := t.cfg.Deadband
		if deadband < 0 {
			deadband = 0
		}
		return math.Abs(float64(current-previous)) > deadband
	case config.ValueKindDecimal:
		current, ok := toDecimalValue(value)
		if !ok {
			return true
		}
		previous, ok := toDecimalValue(lastValue)
		if !ok {
			return true
		}
		threshold := decimal.NewFromFloat(t.cfg.Deadband)
		if threshold.Sign() < 0 {
			threshold = decimal.Zero
		}
		diff := current.Sub(previous).Abs()
		return diff.Cmp(threshold) > 0
	default:
		return true
	}
}

func (t *writeTarget) lastWriteTime() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastWrite
}

func (t *writeTarget) updateAfterWrite(value interface{}, ts time.Time, duration time.Duration) {
	t.mu.Lock()
	t.lastValue = cloneValue(value)
	t.lastWrite = ts
	t.lastAttempt = ts
	t.lastDuration = duration
	t.mu.Unlock()
}

func (t *writeTarget) recordAttempt(ts time.Time, duration time.Duration) {
	t.mu.Lock()
	t.lastAttempt = ts
	t.lastDuration = duration
	t.mu.Unlock()
}

func (t *writeTarget) ensureClient() (Client, error) {
	if t.client != nil {
		return t.client, nil
	}
	if t.clientFactory == nil {
		return nil, fmt.Errorf("no client factory configured")
	}
	client, err := t.clientFactory(t.cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	t.client = client
	return client, nil
}

func (t *writeTarget) performWrite(client Client, value interface{}) error {
	switch strings.ToLower(t.cfg.Function) {
	case "coil", "coils":
		boolVal, ok := value.(bool)
		if !ok {
			return fmt.Errorf("expected bool value for coil write, got %T", value)
		}
		var modbusVal uint16
		if boolVal {
			modbusVal = 0xFF00
		}
		_, err := client.WriteSingleCoil(t.cfg.Address, modbusVal)
		return err
	case "holding", "holding_register", "holding_registers":
		number, ok := toFloat(value)
		if !ok {
			return fmt.Errorf("expected numeric value for register write, got %T", value)
		}
		scale := t.cfg.Scale
		if scale == 0 {
			scale = 1
		}
		raw := number / scale
		var value16 uint16
		if t.cfg.Signed {
			intVal := int(math.Round(raw))
			if intVal < math.MinInt16 || intVal > math.MaxInt16 {
				return fmt.Errorf("value %v out of range for int16", number)
			}
			value16 = uint16(intVal & 0xFFFF)
		} else {
			uintVal := uint32(math.Round(raw))
			if uintVal > math.MaxUint16 {
				return fmt.Errorf("value %v out of range for uint16", number)
			}
			value16 = uint16(uintVal)
		}
		if strings.ToLower(t.cfg.Endianness) == "little" || strings.ToLower(t.cfg.Endianness) == "little_endian" {
			value16 = (value16>>8)&0x00FF | (value16<<8)&0xFF00
		}
		_, err := client.WriteSingleRegister(t.cfg.Address, value16)
		return err
	default:
		return fmt.Errorf("unsupported write function %q", t.cfg.Function)
	}
}

func (t *writeTarget) closeClient() {
	if t.client == nil {
		return
	}
	_ = t.client.Close()
	t.client = nil
}

func (t *writeTarget) SetDisabled(disabled bool) {
	t.disabled.Store(disabled)
	if disabled {
		t.mu.Lock()
		t.lastAttempt = time.Time{}
		t.lastDuration = 0
		t.mu.Unlock()
	}
}

func (t *writeTarget) Status() runtimeWriters.WriteTargetStatus {
	t.mu.RLock()
	lastWrite := t.lastWrite
	lastAttempt := t.lastAttempt
	lastDuration := t.lastDuration
	t.mu.RUnlock()
	return runtimeWriters.WriteTargetStatus{
		ID:           t.cfg.ID,
		Cell:         t.cfg.Cell,
		Function:     t.cfg.Function,
		Address:      t.cfg.Address,
		Disabled:     t.disabled.Load(),
		LastWrite:    lastWrite,
		LastAttempt:  lastAttempt,
		LastDuration: lastDuration,
		Source:       t.cfg.Source,
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

func (t *writeTarget) Close() {
	t.closeClient()
}

func toFloat(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, false
		}
		return v, true
	case float32:
		return toFloat(float64(v))
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case decimal.Decimal:
		f, exact := v.Float64()
		if !exact {
			return f, true
		}
		return f, true
	case *decimal.Decimal:
		if v == nil {
			return 0, false
		}
		return toFloat(*v)
	default:
		return 0, false
	}
}

func toInt(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		if v > math.MaxInt64 {
			return 0, false
		}
		return int64(v), true
	default:
		return 0, false
	}
}

func toDecimalValue(value interface{}) (decimal.Decimal, bool) {
	switch v := value.(type) {
	case decimal.Decimal:
		return v, true
	case *decimal.Decimal:
		if v == nil {
			return decimal.Zero, false
		}
		return *v, true
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return decimal.Zero, false
		}
		return decimal.RequireFromString(fmt.Sprintf("%g", v)), true
	case float32:
		return decimal.RequireFromString(fmt.Sprintf("%g", v)), true
	case int:
		return decimal.NewFromInt(int64(v)), true
	case int8:
		return decimal.NewFromInt(int64(v)), true
	case int16:
		return decimal.NewFromInt(int64(v)), true
	case int32:
		return decimal.NewFromInt(int64(v)), true
	case int64:
		return decimal.NewFromInt(v), true
	case uint:
		return decimal.NewFromInt(int64(v)), true
	case uint8:
		return decimal.NewFromInt(int64(v)), true
	case uint16:
		return decimal.NewFromInt(int64(v)), true
	case uint32:
		return decimal.NewFromInt(int64(v)), true
	case uint64:
		if v > math.MaxInt64 {
			return decimal.Zero, false
		}
		return decimal.NewFromInt(int64(v)), true
	default:
		return decimal.Zero, false
	}
}
