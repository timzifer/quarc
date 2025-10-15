package random

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/shopspring/decimal"

	"github.com/timzifer/quarc/config"
	"github.com/timzifer/quarc/runtime/readers"
	"github.com/timzifer/quarc/runtime/state"
)

// NewReadFactory returns a readers.ReaderFactory that produces random values within
// configurable ranges. The factory consumes the read group's driver_settings node
// to determine the random source and per-signal bounds.
func NewReadFactory() readers.ReaderFactory {
	return func(cfg config.ReadGroupConfig, deps readers.ReaderDependencies) (readers.ReadGroup, error) {
		if cfg.ID == "" {
			return nil, errors.New("read group id must not be empty")
		}
		if len(cfg.Signals) == 0 {
			return nil, fmt.Errorf("read group %s: no signals configured", cfg.ID)
		}
		if deps.Cells == nil {
			return nil, fmt.Errorf("read group %s: dependencies missing cell store", cfg.ID)
		}
		settings, err := parseSettings(cfg.DriverSettings)
		if err != nil {
			return nil, fmt.Errorf("read group %s: %w", cfg.ID, err)
		}
		source, err := newRandomSource(settings.Source, settings.Seed)
		if err != nil {
			return nil, fmt.Errorf("read group %s: %w", cfg.ID, err)
		}
		signals := make([]randomSignal, 0, len(cfg.Signals))
		bufferStatus := make(map[string]readers.SignalBufferStatus)
		for _, signalCfg := range cfg.Signals {
			if signalCfg.Cell == "" {
				return nil, fmt.Errorf("read group %s: signal missing cell", cfg.ID)
			}
			cell, err := deps.Cells.Get(signalCfg.Cell)
			if err != nil {
				return nil, err
			}
			resolved, err := settings.resolve(signalCfg.Cell)
			if err != nil {
				return nil, fmt.Errorf("read group %s: %w", cfg.ID, err)
			}
			var buffer *readers.SignalBuffer
			if deps.Buffers != nil {
				buffer, err = deps.Buffers.Get(signalCfg.Cell)
				if err != nil {
					return nil, err
				}
				bufferStatus[signalCfg.Cell] = buffer.Status()
			}
			signals = append(signals, randomSignal{
				cellID:   signalCfg.Cell,
				cell:     cell,
				buffer:   buffer,
				kind:     signalCfg.Type,
				settings: resolved,
			})
		}
		function := cfg.Function
		if function == "" {
			function = "random"
		}
		group := &randomReadGroup{
			id:           cfg.ID,
			function:     function,
			start:        cfg.Start,
			length:       cfg.Length,
			source:       cfg.Source,
			interval:     cfg.TTL.Duration,
			generator:    source,
			signals:      signals,
			bufferStatus: bufferStatus,
		}
		return group, nil
	}
}

type randomSignal struct {
	cellID   string
	cell     state.Cell
	buffer   *readers.SignalBuffer
	kind     config.ValueKind
	settings resolvedSignalSettings
}

func (s randomSignal) generate(src randomSource) (interface{}, *float64, error) {
	switch s.kind {
	case config.ValueKindNumber, config.ValueKindFloat:
		value, err := randomFloatInRange(src, s.settings.floatMin, s.settings.floatMax)
		if err != nil {
			return nil, nil, err
		}
		return value, cloneQuality(s.settings.quality), nil
	case config.ValueKindInteger:
		value, err := randomIntInRange(src, s.settings.intMin, s.settings.intMax)
		if err != nil {
			return nil, nil, err
		}
		return value, cloneQuality(s.settings.quality), nil
	case config.ValueKindBool:
		value, err := randomBool(src, s.settings.boolProbability)
		if err != nil {
			return nil, nil, err
		}
		return value, cloneQuality(s.settings.quality), nil
	case config.ValueKindString:
		value, err := randomString(src, s.settings.stringLength, s.settings.alphabet)
		if err != nil {
			return nil, nil, err
		}
		return value, cloneQuality(s.settings.quality), nil
	case config.ValueKindDecimal:
		value, err := randomFloatInRange(src, s.settings.floatMin, s.settings.floatMax)
		if err != nil {
			return nil, nil, err
		}
		return decimal.NewFromFloat(value), cloneQuality(s.settings.quality), nil
	default:
		return nil, nil, fmt.Errorf("unsupported value kind %s", s.kind)
	}
}

type randomReadGroup struct {
	id       string
	function string
	start    uint16
	length   uint16
	source   config.ModuleReference

	interval  time.Duration
	generator randomSource

	signals []randomSignal

	disabled atomic.Bool

	mu           sync.RWMutex
	nextRun      time.Time
	lastRun      time.Time
	lastDuration time.Duration
	bufferStatus map[string]readers.SignalBufferStatus
}

func (g *randomReadGroup) ID() string { return g.id }

func (g *randomReadGroup) Due(now time.Time) bool {
	if g.disabled.Load() {
		return false
	}
	if g.interval <= 0 {
		return true
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.nextRun.IsZero() {
		return true
	}
	return !now.Before(g.nextRun)
}

func (g *randomReadGroup) Perform(now time.Time, logger zerolog.Logger) int {
	if g.disabled.Load() {
		return 0
	}
	start := time.Now()
	errCount := 0
	for _, signal := range g.signals {
		value, quality, err := signal.generate(g.generator)
		if err != nil {
			errCount++
			signal.cell.MarkInvalid(now, "random.generate", err.Error())
			logger.Error().Err(err).Str("group", g.id).Str("cell", signal.cellID).Msg("random reader failed")
			continue
		}
		if err := signal.cell.SetValue(value, now, quality); err != nil {
			errCount++
			logger.Error().Err(err).Str("group", g.id).Str("cell", signal.cellID).Msg("random reader update failed")
			continue
		}
		if signal.buffer != nil {
			if err := signal.buffer.Push(now, value, quality); err != nil {
				if errors.Is(err, readers.ErrSignalBufferOverflow) {
					logger.Warn().Str("group", g.id).Str("cell", signal.cellID).Msg("random reader buffer overflow")
				} else {
					errCount++
					logger.Error().Err(err).Str("group", g.id).Str("cell", signal.cellID).Msg("random reader buffer push failed")
				}
			}
			g.mu.Lock()
			g.bufferStatus[signal.cellID] = signal.buffer.Status()
			g.mu.Unlock()
		}
	}
	duration := time.Since(start)
	if duration == 0 {
		// Ensure the reported duration is non-zero even if the work completed
		// within the clock's resolution to avoid downstream status checks
		// seeing a zero-length execution window.
		duration = time.Nanosecond
	}
	g.mu.Lock()
	if g.interval > 0 {
		g.nextRun = now.Add(g.interval)
	} else {
		g.nextRun = time.Time{}
	}
	g.lastRun = now
	g.lastDuration = duration
	g.mu.Unlock()
	return errCount
}

func (g *randomReadGroup) SetDisabled(disabled bool) {
	g.disabled.Store(disabled)
}

func (g *randomReadGroup) Status() readers.ReadGroupStatus {
	g.mu.RLock()
	defer g.mu.RUnlock()
	buffers := make(map[string]readers.SignalBufferStatus, len(g.bufferStatus))
	for id, status := range g.bufferStatus {
		buffers[id] = status
	}
	return readers.ReadGroupStatus{
		ID:           g.id,
		Function:     g.function,
		Start:        g.start,
		Length:       g.length,
		Disabled:     g.disabled.Load(),
		NextRun:      g.nextRun,
		LastRun:      g.lastRun,
		LastDuration: g.lastDuration,
		Source:       g.source,
		Buffers:      buffers,
	}
}

func (g *randomReadGroup) Close() {}
