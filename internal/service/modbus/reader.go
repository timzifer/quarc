package modbus

import (
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/shopspring/decimal"

	"modbus_processor/internal/config"
	"modbus_processor/remote"
	serviceio "modbus_processor/serviceio"
)

type readSignal struct {
	cfg        config.ReadSignalConfig
	cell       serviceio.Cell
	cellConfig config.CellConfig
}

type readGroup struct {
	cfg           config.ReadGroupConfig
	signals       []readSignal
	next          time.Time
	clientFactory remote.ClientFactory
	client        remote.Client
	mu            sync.RWMutex
	disabled      atomic.Bool
	lastRun       time.Time
	lastDuration  time.Duration
}

// NewReaderFactory builds a Modbus read group factory.
func NewReaderFactory(factory remote.ClientFactory) serviceio.ReaderFactory {
	if factory == nil {
		factory = remote.NewTCPClientFactory()
	}
	return func(cfg config.ReadGroupConfig, deps serviceio.ReaderDependencies) (serviceio.ReadGroup, error) {
		return newReadGroup(cfg, deps, factory)
	}
}

func newReadGroup(cfg config.ReadGroupConfig, deps serviceio.ReaderDependencies, factory remote.ClientFactory) (serviceio.ReadGroup, error) {
	if deps.Cells == nil {
		return nil, fmt.Errorf("read group %s: missing cell store", cfg.ID)
	}
	if cfg.ID == "" {
		return nil, fmt.Errorf("read group id must not be empty")
	}
	if cfg.Length == 0 {
		return nil, fmt.Errorf("read group %s length must be >0", cfg.ID)
	}
	if cfg.Function == "" {
		return nil, fmt.Errorf("read group %s missing function", cfg.ID)
	}
	group := &readGroup{cfg: cfg, clientFactory: factory}
	group.disabled.Store(cfg.Disable)
	for _, sigCfg := range cfg.Signals {
		cell, err := deps.Cells.Get(sigCfg.Cell)
		if err != nil {
			return nil, fmt.Errorf("read group %s: %w", cfg.ID, err)
		}
		cellCfg := cell.Config()
		if sigCfg.Type == "" {
			sigCfg.Type = cellCfg.Type
		}
		if cellCfg.Type != sigCfg.Type {
			return nil, fmt.Errorf("read group %s signal %s type mismatch (cell %s is %s, signal is %s)", cfg.ID, sigCfg.Cell, sigCfg.Cell, cellCfg.Type, sigCfg.Type)
		}
		group.signals = append(group.signals, readSignal{cfg: sigCfg, cell: cell, cellConfig: cellCfg})
	}
	return group, nil
}

func (g *readGroup) ID() string {
	return g.cfg.ID
}

func (g *readGroup) Due(now time.Time) bool {
	if g.disabled.Load() {
		return false
	}
	ttl := g.cfg.TTL.Duration
	if ttl <= 0 {
		return true
	}
	g.mu.RLock()
	next := g.next
	g.mu.RUnlock()
	if next.IsZero() {
		return true
	}
	return !now.Before(next)
}

func (g *readGroup) Perform(now time.Time, logger zerolog.Logger) int {
	if g.disabled.Load() {
		return 0
	}
	errors := 0
	ttl := g.cfg.TTL.Duration
	if ttl <= 0 {
		ttl = 0
	}
	g.mu.Lock()
	g.next = now.Add(ttl)
	g.mu.Unlock()

	if len(g.signals) == 0 {
		return 0
	}

	start := time.Now()
	logger.Trace().Str("group", g.cfg.ID).Str("function", strings.ToLower(g.cfg.Function)).Uint16("start", g.cfg.Start).Uint16("length", g.cfg.Length).Msg("read group scheduled")

	if g.client == nil {
		logger.Trace().Str("group", g.cfg.ID).Msg("creating modbus read client")
	}
	client, err := g.ensureClient()
	if err != nil {
		g.invalidateAll(now, "read.connect", err.Error())
		g.closeClient()
		logger.Error().Err(err).Str("group", g.cfg.ID).Msg("read group connection failed")
		return 1
	}

	raw, err := g.read(client)
	if err != nil {
		g.invalidateAll(now, "read.error", err.Error())
		g.closeClient()
		logger.Error().Err(err).Str("group", g.cfg.ID).Msg("modbus read failed")
		return 1
	}

	for _, sig := range g.signals {
		if err := sig.apply(raw, g.cfg.Function, now); err != nil {
			sig.cell.MarkInvalid(now, "read.marshal", err.Error())
			logger.Error().Err(err).Str("group", g.cfg.ID).Str("cell", sig.cfg.Cell).Msg("signal decode failed")
			errors++
		}
	}
	duration := time.Since(start)
	g.mu.Lock()
	g.lastRun = now
	g.lastDuration = duration
	g.mu.Unlock()
	logger.Trace().Str("group", g.cfg.ID).Int("signals", len(g.signals)).Dur("duration", duration).Msg("read group completed")
	return errors
}

func (g *readGroup) ensureClient() (remote.Client, error) {
	if g.client != nil {
		return g.client, nil
	}
	if g.clientFactory == nil {
		return nil, fmt.Errorf("no client factory configured")
	}
	client, err := g.clientFactory(g.cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	g.client = client
	return client, nil
}

func (g *readGroup) read(client remote.Client) ([]byte, error) {
	switch strings.ToLower(g.cfg.Function) {
	case "coil", "coils":
		return client.ReadCoils(g.cfg.Start, g.cfg.Length)
	case "discrete", "discrete_input", "discrete_inputs":
		return client.ReadDiscreteInputs(g.cfg.Start, g.cfg.Length)
	case "holding", "holding_register", "holding_registers":
		return client.ReadHoldingRegisters(g.cfg.Start, g.cfg.Length)
	case "input", "input_register", "input_registers":
		return client.ReadInputRegisters(g.cfg.Start, g.cfg.Length)
	default:
		return nil, fmt.Errorf("unsupported function %q", g.cfg.Function)
	}
}

func (g *readGroup) invalidateAll(ts time.Time, code, message string) {
	for _, sig := range g.signals {
		sig.cell.MarkInvalid(ts, code, message)
	}
}

func (g *readGroup) SetDisabled(disabled bool) {
	g.disabled.Store(disabled)
	if disabled {
		g.mu.Lock()
		g.next = time.Time{}
		g.mu.Unlock()
	}
}

func (g *readGroup) Status() serviceio.ReadGroupStatus {
	g.mu.RLock()
	next := g.next
	lastRun := g.lastRun
	lastDuration := g.lastDuration
	g.mu.RUnlock()
	return serviceio.ReadGroupStatus{
		ID:           g.cfg.ID,
		Function:     g.cfg.Function,
		Start:        g.cfg.Start,
		Length:       g.cfg.Length,
		Disabled:     g.disabled.Load(),
		NextRun:      next,
		LastRun:      lastRun,
		LastDuration: lastDuration,
		Source:       g.cfg.Source,
	}
}

func (g *readGroup) closeClient() {
	if g.client == nil {
		return
	}
	_ = g.client.Close()
	g.client = nil
}

func (s *readSignal) apply(raw []byte, function string, ts time.Time) error {
	if len(raw) == 0 {
		return fmt.Errorf("no data returned")
	}
	switch s.cfg.Type {
	case config.ValueKindBool:
		value, err := s.boolValue(raw, function)
		if err != nil {
			return err
		}
		return s.cell.SetValue(value, ts, nil)
	case config.ValueKindNumber, config.ValueKindFloat:
		value, err := s.numberValue(raw, function)
		if err != nil {
			return err
		}
		return s.cell.SetValue(value, ts, nil)
	case config.ValueKindInteger:
		value, err := s.integerValue(raw, function)
		if err != nil {
			return err
		}
		return s.cell.SetValue(value, ts, nil)
	case config.ValueKindDecimal:
		value, err := s.decimalValue(raw, function)
		if err != nil {
			return err
		}
		return s.cell.SetValue(value, ts, nil)
	case config.ValueKindString:
		return fmt.Errorf("string decoding from modbus not implemented")
	case config.ValueKindDate:
		return fmt.Errorf("date decoding from modbus not implemented")
	default:
		return fmt.Errorf("unsupported value kind %q", s.cfg.Type)
	}
}

func (s *readSignal) boolValue(raw []byte, function string) (bool, error) {
	switch strings.ToLower(function) {
	case "coil", "coils", "discrete", "discrete_input", "discrete_inputs":
		bitIndex := int(s.cfg.Offset)
		byteIndex := bitIndex / 8
		bit := uint(bitIndex % 8)
		if byteIndex >= len(raw) {
			return false, fmt.Errorf("offset %d out of range", s.cfg.Offset)
		}
		val := (raw[byteIndex] >> bit) & 0x01
		return val == 1, nil
	case "holding", "holding_register", "holding_registers", "input", "input_register", "input_registers":
		word, err := s.readWord(raw)
		if err != nil {
			return false, err
		}
		if s.cfg.Bit != nil {
			if *s.cfg.Bit >= 16 {
				return false, fmt.Errorf("bit index %d out of range", *s.cfg.Bit)
			}
			bit := uint16(1) << *s.cfg.Bit
			return (word & bit) != 0, nil
		}
		return word != 0, nil
	default:
		return false, fmt.Errorf("unsupported function %q", function)
	}
}

func (s *readSignal) numberValue(raw []byte, function string) (float64, error) {
	switch strings.ToLower(function) {
	case "holding", "holding_register", "holding_registers", "input", "input_register", "input_registers":
		word, err := s.readWord(raw)
		if err != nil {
			return 0, err
		}
		scale := s.cfg.Scale
		if scale == 0 {
			scale = 1
		}
		if s.cfg.Signed {
			return float64(int16(word)) * scale, nil
		}
		return float64(word) * scale, nil
	default:
		return 0, fmt.Errorf("unsupported numeric function %q", function)
	}
}

func (s *readSignal) integerValue(raw []byte, function string) (int64, error) {
	switch strings.ToLower(function) {
	case "holding", "holding_register", "holding_registers", "input", "input_register", "input_registers":
		word, err := s.readWord(raw)
		if err != nil {
			return 0, err
		}
		if s.cfg.Signed {
			return int64(int16(word)), nil
		}
		return int64(word), nil
	default:
		return 0, fmt.Errorf("unsupported integer function %q", function)
	}
}

func (s *readSignal) decimalValue(raw []byte, function string) (decimal.Decimal, error) {
	switch strings.ToLower(function) {
	case "holding", "holding_register", "holding_registers", "input", "input_register", "input_registers":
		word, err := s.readWord(raw)
		if err != nil {
			return decimal.Zero, err
		}
		scale := s.cfg.Scale
		if scale == 0 {
			scale = 1
		}
		base := decimal.NewFromInt(int64(word))
		if s.cfg.Signed {
			base = decimal.NewFromInt(int64(int16(word)))
		}
		scaled := base.Mul(decimal.RequireFromString(fmt.Sprintf("%g", scale)))
		return scaled, nil
	default:
		return decimal.Zero, fmt.Errorf("unsupported decimal function %q", function)
	}
}

func (s *readSignal) readWord(raw []byte) (uint16, error) {
	offset := int(s.cfg.Offset) * 2
	if offset+1 >= len(raw) {
		return 0, fmt.Errorf("offset %d out of range", s.cfg.Offset)
	}
	switch strings.ToLower(s.cfg.Endianness) {
	case "little", "little_endian":
		return binary.LittleEndian.Uint16(raw[offset:]), nil
	default:
		return binary.BigEndian.Uint16(raw[offset:]), nil
	}
}

func (g *readGroup) Close() {
	g.closeClient()
}
