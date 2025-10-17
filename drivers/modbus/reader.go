package modbus

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/shopspring/decimal"
	"github.com/timzifer/quarc/config"

	"github.com/timzifer/quarc/runtime/activity"
	runtimeReaders "github.com/timzifer/quarc/runtime/readers"
	"github.com/timzifer/quarc/runtime/state"
)

type readSignal struct {
	cfg        config.ReadSignalConfig
	cell       state.Cell
	cellConfig config.CellConfig
}

type readGroup struct {
	cfg           config.ReadGroupConfig
	plan          readGroupPlan
	signals       []readSignal
	requests      []plannedRequest
	signalPlan    []int
	next          time.Time
	clientFactory ClientFactory
	client        Client
	mu            sync.RWMutex
	disabled      atomic.Bool
	lastRun       time.Time
	lastDuration  time.Duration
	activity      activity.Tracker
	legacyOnce    sync.Once
}

type plannedRequest struct {
	Start      uint16
	Length     uint16
	BaseOffset uint16
	Signals    []int
}

func (g *readGroup) planRequests() error {
	if len(g.signals) == 0 {
		g.requests = nil
		g.signalPlan = nil
		return nil
	}
	type signalRef struct {
		index  int
		offset uint16
	}
	refs := make([]signalRef, len(g.signals))
	for i := range g.signals {
		refs[i] = signalRef{index: i, offset: g.signals[i].cfg.Offset}
	}
	sort.SliceStable(refs, func(i, j int) bool {
		if refs[i].offset == refs[j].offset {
			return refs[i].index < refs[j].index
		}
		return refs[i].offset < refs[j].offset
	})
	maxGap := int(g.plan.MaxGapSize)
	length := int(g.plan.Length)
	segments := make([]plannedRequest, 0)
	signalPlan := make([]int, len(g.signals))
	for i := range signalPlan {
		signalPlan[i] = -1
	}
	type segment struct {
		start   int
		end     int
		signals []int
	}
	var current segment
	for i, ref := range refs {
		if int(ref.offset) >= length {
			return fmt.Errorf("read group %s: signal offset %d exceeds length %d", g.cfg.ID, ref.offset, g.plan.Length)
		}
		if i == 0 {
			current = segment{start: int(ref.offset), end: int(ref.offset), signals: []int{ref.index}}
			continue
		}
		if ref.offset < uint16(current.start) {
			refOffset := int(ref.offset)
			current.signals = append(current.signals, ref.index)
			if refOffset < current.start {
				current.start = refOffset
			}
			continue
		}
		if int(ref.offset) > current.end {
			gap := int(ref.offset) - current.end - 1
			if gap > maxGap {
				segments = append(segments, plannedRequest{})
				segIndex := len(segments) - 1
				segments[segIndex] = plannedRequest{
					Start:      g.plan.Start + uint16(current.start),
					Length:     uint16(current.end-current.start) + 1,
					BaseOffset: uint16(current.start),
					Signals:    append([]int(nil), current.signals...),
				}
				for _, idx := range current.signals {
					signalPlan[idx] = segIndex
				}
				current = segment{start: int(ref.offset), end: int(ref.offset), signals: []int{ref.index}}
				continue
			}
			current.end = int(ref.offset)
		}
		current.signals = append(current.signals, ref.index)
	}
	if len(current.signals) > 0 {
		segments = append(segments, plannedRequest{
			Start:      g.plan.Start + uint16(current.start),
			Length:     uint16(current.end-current.start) + 1,
			BaseOffset: uint16(current.start),
			Signals:    append([]int(nil), current.signals...),
		})
		segIndex := len(segments) - 1
		for _, idx := range current.signals {
			signalPlan[idx] = segIndex
		}
	}
	for i, seg := range segments {
		absStart := int(g.plan.Start) + int(seg.BaseOffset)
		if absStart > 0xFFFF {
			return fmt.Errorf("read group %s: planned start %d exceeds modbus address space", g.cfg.ID, absStart)
		}
		if int(seg.BaseOffset)+int(seg.Length) > length {
			return fmt.Errorf("read group %s: planned segment exceeds configured length", g.cfg.ID)
		}
		segments[i].Start = uint16(absStart)
	}
	g.requests = segments
	g.signalPlan = signalPlan
	return nil
}

// NewReaderFactory builds a Modbus read group factory.
func NewReaderFactory(factory ClientFactory) runtimeReaders.ReaderFactory {
	if factory == nil {
		factory = NewTCPClientFactory()
	}
	return func(cfg config.ReadGroupConfig, deps runtimeReaders.ReaderDependencies) (runtimeReaders.ReadGroup, error) {
		return newReadGroup(cfg, deps, factory)
	}
}

func newReadGroup(cfg config.ReadGroupConfig, deps runtimeReaders.ReaderDependencies, factory ClientFactory) (runtimeReaders.ReadGroup, error) {
	if deps.Cells == nil {
		return nil, fmt.Errorf("read group %s: missing cell store", cfg.ID)
	}
	if cfg.ID == "" {
		return nil, fmt.Errorf("read group id must not be empty")
	}
	resolved, plan, err := resolveReadGroup(cfg)
	if err != nil {
		return nil, err
	}
	if plan.Length == 0 {
		return nil, fmt.Errorf("read group %s length must be >0", resolved.ID)
	}
	if plan.Function == "" {
		return nil, fmt.Errorf("read group %s missing function", resolved.ID)
	}
	group := &readGroup{cfg: resolved, plan: plan, clientFactory: factory, activity: deps.Activity}
	group.disabled.Store(resolved.Disable)
	for _, sigCfg := range resolved.Signals {
		cell, err := deps.Cells.Get(sigCfg.Cell)
		if err != nil {
			return nil, fmt.Errorf("read group %s: %w", resolved.ID, err)
		}
		cellCfg := cell.Config()
		if sigCfg.Type == "" {
			sigCfg.Type = cellCfg.Type
		}
		if cellCfg.Type != sigCfg.Type {
			return nil, fmt.Errorf("read group %s signal %s type mismatch (cell %s is %s, signal is %s)", resolved.ID, sigCfg.Cell, sigCfg.Cell, cellCfg.Type, sigCfg.Type)
		}
		group.signals = append(group.signals, readSignal{cfg: sigCfg, cell: cell, cellConfig: cellCfg})
	}
	if err := group.planRequests(); err != nil {
		return nil, err
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
	if g.plan.Legacy {
		g.legacyOnce.Do(func() {
			logger.Warn().Str("group", g.cfg.ID).Msg("modbus read group using deprecated function/start/length fields; move settings into driver.settings")
		})
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
	logger.Trace().Str("group", g.cfg.ID).Str("function", strings.ToLower(g.plan.Function)).Uint16("start", g.plan.Start).Uint16("length", g.plan.Length).Uint16("max_gap_size", g.plan.MaxGapSize).Int("requests", len(g.requests)).Msg("read group scheduled")

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

	for i, sig := range g.signals {
		reqIndex := -1
		if len(g.signalPlan) > 0 {
			reqIndex = g.signalPlan[i]
		}
		if reqIndex < 0 || reqIndex >= len(raw) {
			sig.cell.MarkInvalid(now, "read.plan", "signal not mapped to request payload")
			logger.Error().Str("group", g.cfg.ID).Str("cell", sig.cfg.Cell).Msg("signal not mapped to request")
			errors++
			continue
		}
		segment := g.requests[reqIndex]
		if err := sig.apply(raw[reqIndex], g.plan.Function, now, segment.BaseOffset); err != nil {
			sig.cell.MarkInvalid(now, "read.marshal", err.Error())
			logger.Error().Err(err).Str("group", g.cfg.ID).Str("cell", sig.cfg.Cell).Msg("signal decode failed")
			errors++
			continue
		}
		g.recordRead(sig.cfg.Cell, now)
	}
	duration := time.Since(start)
	g.mu.Lock()
	g.lastRun = now
	g.lastDuration = duration
	g.mu.Unlock()
	logger.Trace().Str("group", g.cfg.ID).Int("signals", len(g.signals)).Dur("duration", duration).Msg("read group completed")
	return errors
}

func (g *readGroup) ensureClient() (Client, error) {
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

func (g *readGroup) read(client Client) ([][]byte, error) {
	if len(g.requests) == 0 {
		return nil, nil
	}
	segments := make([][]byte, len(g.requests))
	for i, req := range g.requests {
		var (
			payload []byte
			err     error
		)
		switch strings.ToLower(g.plan.Function) {
		case "coil", "coils":
			payload, err = client.ReadCoils(req.Start, req.Length)
		case "discrete", "discrete_input", "discrete_inputs":
			payload, err = client.ReadDiscreteInputs(req.Start, req.Length)
		case "holding", "holding_register", "holding_registers":
			payload, err = client.ReadHoldingRegisters(req.Start, req.Length)
		case "input", "input_register", "input_registers":
			payload, err = client.ReadInputRegisters(req.Start, req.Length)
		default:
			return nil, fmt.Errorf("unsupported function %q", g.plan.Function)
		}
		if err != nil {
			return nil, err
		}
		segments[i] = payload
	}
	return segments, nil
}

func (g *readGroup) invalidateAll(ts time.Time, code, message string) {
	for _, sig := range g.signals {
		sig.cell.MarkInvalid(ts, code, message)
	}
}

func (g *readGroup) recordRead(cellID string, ts time.Time) {
	if g == nil || g.activity == nil {
		return
	}
	g.activity.RecordCellRead(cellID, g.cfg.ID, ts)
}

func (g *readGroup) SetDisabled(disabled bool) {
	g.disabled.Store(disabled)
	if disabled {
		g.mu.Lock()
		g.next = time.Time{}
		g.mu.Unlock()
	}
}

func (g *readGroup) Status() runtimeReaders.ReadGroupStatus {
	g.mu.RLock()
	next := g.next
	lastRun := g.lastRun
	lastDuration := g.lastDuration
	g.mu.RUnlock()
	metadata := map[string]interface{}{
		"function":      g.plan.Function,
		"start":         g.plan.Start,
		"length":        g.plan.Length,
		"max_gap_size":  g.plan.MaxGapSize,
		"request_count": len(g.requests),
	}
	if g.plan.Legacy {
		metadata["legacy"] = true
	}
	driverName := g.cfg.Driver.Name
	if driverName == "" {
		driverName = "modbus"
	}
	return runtimeReaders.ReadGroupStatus{
		ID:           g.cfg.ID,
		Driver:       driverName,
		Disabled:     g.disabled.Load(),
		NextRun:      next,
		LastRun:      lastRun,
		LastDuration: lastDuration,
		Source:       g.cfg.Source,
		Metadata:     metadata,
	}
}

func (g *readGroup) closeClient() {
	if g.client == nil {
		return
	}
	_ = g.client.Close()
	g.client = nil
}

func (s *readSignal) apply(raw []byte, function string, ts time.Time, baseOffset uint16) error {
	if len(raw) == 0 {
		return fmt.Errorf("no data returned")
	}
	switch s.cfg.Type {
	case config.ValueKindBool:
		value, err := s.boolValue(raw, function, baseOffset)
		if err != nil {
			return err
		}
		return s.cell.SetValue(value, ts, nil)
	case config.ValueKindNumber, config.ValueKindFloat:
		value, err := s.numberValue(raw, function, baseOffset)
		if err != nil {
			return err
		}
		return s.cell.SetValue(value, ts, nil)
	case config.ValueKindInteger:
		value, err := s.integerValue(raw, function, baseOffset)
		if err != nil {
			return err
		}
		return s.cell.SetValue(value, ts, nil)
	case config.ValueKindDecimal:
		value, err := s.decimalValue(raw, function, baseOffset)
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

func (s *readSignal) boolValue(raw []byte, function string, baseOffset uint16) (bool, error) {
	switch strings.ToLower(function) {
	case "coil", "coils", "discrete", "discrete_input", "discrete_inputs":
		if s.cfg.Offset < baseOffset {
			return false, fmt.Errorf("offset %d before segment base %d", s.cfg.Offset, baseOffset)
		}
		bitIndex := int(s.cfg.Offset - baseOffset)
		byteIndex := bitIndex / 8
		bit := uint(bitIndex % 8)
		if byteIndex >= len(raw) {
			return false, fmt.Errorf("offset %d out of range", s.cfg.Offset)
		}
		val := (raw[byteIndex] >> bit) & 0x01
		return val == 1, nil
	case "holding", "holding_register", "holding_registers", "input", "input_register", "input_registers":
		word, err := s.readWord(raw, baseOffset)
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

func (s *readSignal) numberValue(raw []byte, function string, baseOffset uint16) (float64, error) {
	switch strings.ToLower(function) {
	case "holding", "holding_register", "holding_registers", "input", "input_register", "input_registers":
		word, err := s.readWord(raw, baseOffset)
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

func (s *readSignal) integerValue(raw []byte, function string, baseOffset uint16) (int64, error) {
	switch strings.ToLower(function) {
	case "holding", "holding_register", "holding_registers", "input", "input_register", "input_registers":
		word, err := s.readWord(raw, baseOffset)
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

func (s *readSignal) decimalValue(raw []byte, function string, baseOffset uint16) (decimal.Decimal, error) {
	switch strings.ToLower(function) {
	case "holding", "holding_register", "holding_registers", "input", "input_register", "input_registers":
		word, err := s.readWord(raw, baseOffset)
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

func (s *readSignal) readWord(raw []byte, baseOffset uint16) (uint16, error) {
	if s.cfg.Offset < baseOffset {
		return 0, fmt.Errorf("offset %d before segment base %d", s.cfg.Offset, baseOffset)
	}
	offset := int(s.cfg.Offset-baseOffset) * 2
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
