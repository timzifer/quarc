package service

import (
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"modbus_processor/internal/config"
	"modbus_processor/internal/remote"
)

type readSignal struct {
	cfg  config.ReadSignalConfig
	cell *cell
}

type readGroup struct {
	cfg          config.ReadGroupConfig
	signals      []readSignal
	next         time.Time
	client       remote.Client
	mu           sync.RWMutex
	disabled     atomic.Bool
	lastRun      time.Time
	lastDuration time.Duration
}

func newReadGroups(cfgs []config.ReadGroupConfig, cells *cellStore) ([]*readGroup, error) {
	groups := make([]*readGroup, 0, len(cfgs))
	for _, cfg := range cfgs {
		if cfg.ID == "" {
			return nil, fmt.Errorf("read group id must not be empty")
		}
		if cfg.Length == 0 {
			return nil, fmt.Errorf("read group %s length must be >0", cfg.ID)
		}
		if cfg.Function == "" {
			return nil, fmt.Errorf("read group %s missing function", cfg.ID)
		}
		group := &readGroup{cfg: cfg}
		group.disabled.Store(cfg.Disable)
		for _, sigCfg := range cfg.Signals {
			cell, err := cells.mustGet(sigCfg.Cell)
			if err != nil {
				return nil, fmt.Errorf("read group %s: %w", cfg.ID, err)
			}
			if sigCfg.Type == "" {
				sigCfg.Type = cell.cfg.Type
			}
			if cell.cfg.Type != sigCfg.Type {
				return nil, fmt.Errorf("read group %s signal %s type mismatch (cell %s is %s, signal is %s)", cfg.ID, sigCfg.Cell, sigCfg.Cell, cell.cfg.Type, sigCfg.Type)
			}
			group.signals = append(group.signals, readSignal{cfg: sigCfg, cell: cell})
		}
		groups = append(groups, group)
	}
	return groups, nil
}

func (g *readGroup) due(now time.Time) bool {
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

func (g *readGroup) perform(now time.Time, factory remote.ClientFactory, logger zerolog.Logger) int {
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
	client, err := g.ensureClient(factory)
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
			sig.cell.markInvalid(now, "read.marshal", err.Error())
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

func (g *readGroup) ensureClient(factory remote.ClientFactory) (remote.Client, error) {
	if g.client != nil {
		return g.client, nil
	}
	if factory == nil {
		return nil, fmt.Errorf("no client factory configured")
	}
	client, err := factory(g.cfg.Endpoint)
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
		sig.cell.markInvalid(ts, code, message)
	}
}

func (g *readGroup) setDisabled(disabled bool) {
	g.disabled.Store(disabled)
	if disabled {
		g.mu.Lock()
		g.next = time.Time{}
		g.mu.Unlock()
	}
}

func (g *readGroup) status() readGroupStatus {
	g.mu.RLock()
	next := g.next
	lastRun := g.lastRun
	lastDuration := g.lastDuration
	g.mu.RUnlock()
	return readGroupStatus{
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
		return s.cell.setValue(value, ts, nil)
	case config.ValueKindNumber:
		value, err := s.numberValue(raw, function)
		if err != nil {
			return err
		}
		return s.cell.setValue(value, ts, nil)
	case config.ValueKindString:
		return fmt.Errorf("string decoding from modbus not implemented")
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
