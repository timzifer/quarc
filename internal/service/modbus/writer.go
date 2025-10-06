package modbus

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"modbus_processor/internal/config"
	"modbus_processor/internal/remote"
	serviceio "modbus_processor/internal/service/io"
)

type writeTarget struct {
	cfg           config.WriteTargetConfig
	cell          serviceio.Cell
	clientFactory remote.ClientFactory
	client        remote.Client
	lastValue     interface{}
	lastWrite     time.Time
	mu            sync.RWMutex
	disabled      atomic.Bool
	lastDuration  time.Duration
	lastAttempt   time.Time
}

// NewWriterFactory builds a Modbus write target factory.
func NewWriterFactory(factory remote.ClientFactory) serviceio.WriterFactory {
	if factory == nil {
		factory = remote.NewTCPClientFactory()
	}
	return func(cfg config.WriteTargetConfig, deps serviceio.WriterDependencies) (serviceio.Writer, error) {
		return newWriteTarget(cfg, deps, factory)
	}
}

func newWriteTarget(cfg config.WriteTargetConfig, deps serviceio.WriterDependencies, factory remote.ClientFactory) (serviceio.Writer, error) {
	if deps.Cells == nil {
		return nil, fmt.Errorf("write target %s: missing cell store", cfg.ID)
	}
	if cfg.ID == "" {
		return nil, fmt.Errorf("write target id must not be empty")
	}
	if cfg.Function == "" {
		return nil, fmt.Errorf("write target %s missing function", cfg.ID)
	}
	cell, err := deps.Cells.Get(cfg.Cell)
	if err != nil {
		return nil, fmt.Errorf("write target %s: %w", cfg.ID, err)
	}
	target := &writeTarget{cfg: cfg, cell: cell, clientFactory: factory}
	target.disabled.Store(cfg.Disable)
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
	case config.ValueKindBool:
		return value != lastValue
	case config.ValueKindString:
		return value != lastValue
	case config.ValueKindNumber:
		current := value.(float64)
		previous, ok := lastValue.(float64)
		if !ok {
			return true
		}
		deadband := t.cfg.Deadband
		if deadband < 0 {
			deadband = 0
		}
		return math.Abs(current-previous) > deadband
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

func (t *writeTarget) ensureClient() (remote.Client, error) {
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

func (t *writeTarget) performWrite(client remote.Client, value interface{}) error {
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
		number, ok := value.(float64)
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

func (t *writeTarget) Status() serviceio.WriteTargetStatus {
	t.mu.RLock()
	lastWrite := t.lastWrite
	lastAttempt := t.lastAttempt
	lastDuration := t.lastDuration
	t.mu.RUnlock()
	return serviceio.WriteTargetStatus{
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
