package canstream

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/timzifer/modbus_processor/config"
	runtimeReaders "github.com/timzifer/modbus_processor/runtime/readers"
	"github.com/timzifer/modbus_processor/runtime/state"

	"go.einride.tech/can/pkg/dbc"
)

type frameKey struct {
	id       uint32
	extended bool
}

type frame struct {
	id       uint32
	extended bool
	channel  uint8
	remote   bool
	dlc      uint8
	data     [8]byte
}

type signalBinding struct {
	signal     *dbc.SignalDef
	cell       state.Cell
	cellConfig config.CellConfig
	factor     float64
	offset     float64
	quality    *float64
}

type frameBinding struct {
	channel *uint8
	signals []signalBinding
}

type readGroup struct {
	cfg          config.ReadGroupConfig
	canCfg       *config.CANReadGroupConfig
	next         time.Time
	disabled     atomic.Bool
	mu           sync.Mutex
	conn         net.Conn
	buffer       []byte
	bindings     map[frameKey][]frameBinding
	allCells     []state.Cell
	readTimeout  time.Duration
	bufferSize   int
	lastRun      time.Time
	lastDuration time.Duration
}

// NewReaderFactory creates a CAN stream reader factory.
func NewReaderFactory() runtimeReaders.ReaderFactory {
	return func(cfg config.ReadGroupConfig, deps runtimeReaders.ReaderDependencies) (runtimeReaders.ReadGroup, error) {
		return newReadGroup(cfg, deps)
	}
}

func newReadGroup(cfg config.ReadGroupConfig, deps runtimeReaders.ReaderDependencies) (runtimeReaders.ReadGroup, error) {
	if cfg.ID == "" {
		return nil, fmt.Errorf("read group id must not be empty")
	}
	if deps.Cells == nil {
		return nil, fmt.Errorf("read group %s: missing cell store", cfg.ID)
	}
	canCfg, err := resolveCANConfig(cfg)
	if err != nil {
		return nil, err
	}
	if len(canCfg.Frames) == 0 {
		return nil, fmt.Errorf("read group %s: no CAN frames configured", cfg.ID)
	}
	if strings.TrimSpace(canCfg.DBC) == "" {
		return nil, fmt.Errorf("read group %s: dbc path must not be empty", cfg.ID)
	}

	filePath := resolveRelativePath(cfg.Source.File, canCfg.DBC)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read group %s: read dbc %s: %w", cfg.ID, filePath, err)
	}
	parser := dbc.NewParser(filePath, data)
	if err := parser.Parse(); err != nil {
		return nil, fmt.Errorf("read group %s: parse dbc: %w", cfg.ID, err)
	}
	messages := indexMessages(parser.Defs())
	if len(messages.byID) == 0 {
		return nil, fmt.Errorf("read group %s: dbc contains no message definitions", cfg.ID)
	}

	bindings := make(map[frameKey][]frameBinding)
	cellSet := make(map[string]state.Cell)
	for _, frameCfg := range canCfg.Frames {
		msgDef, key, err := resolveMessage(frameCfg, messages)
		if err != nil {
			return nil, fmt.Errorf("read group %s: %w", cfg.ID, err)
		}
		frameBind := frameBinding{}
		if frameCfg.Channel != nil {
			ch := new(uint8)
			*ch = *frameCfg.Channel
			frameBind.channel = ch
		}
		for _, sigCfg := range frameCfg.Signals {
			if sigCfg.Cell == "" {
				return nil, fmt.Errorf("read group %s: frame %s signal %s missing cell", cfg.ID, describeFrame(frameCfg), sigCfg.Name)
			}
			cell, err := deps.Cells.Get(sigCfg.Cell)
			if err != nil {
				return nil, fmt.Errorf("read group %s: frame %s signal %s: %w", cfg.ID, describeFrame(frameCfg), sigCfg.Name, err)
			}
			signalDef, err := findSignal(msgDef, sigCfg.Name)
			if err != nil {
				return nil, fmt.Errorf("read group %s: frame %s: %w", cfg.ID, describeFrame(frameCfg), err)
			}
			binding := signalBinding{
				signal:     signalDef,
				cell:       cell,
				cellConfig: cell.Config(),
				factor:     signalDef.Factor,
				offset:     signalDef.Offset,
				quality:    cloneQuality(sigCfg.Quality),
			}
			if sigCfg.Scale != nil {
				binding.factor = *sigCfg.Scale
			}
			if sigCfg.Offset != nil {
				binding.offset = *sigCfg.Offset
			}
			frameBind.signals = append(frameBind.signals, binding)
			cellSet[sigCfg.Cell] = cell
		}
		if len(frameBind.signals) == 0 {
			return nil, fmt.Errorf("read group %s: frame %s has no signal bindings", cfg.ID, describeFrame(frameCfg))
		}
		bindings[key] = append(bindings[key], frameBind)
	}

	allCells := make([]state.Cell, 0, len(cellSet))
	for _, cell := range cellSet {
		allCells = append(allCells, cell)
	}

	group := &readGroup{
		cfg:         cfg,
		canCfg:      canCfg,
		bindings:    bindings,
		allCells:    allCells,
		bufferSize:  canCfg.BufferSize,
		readTimeout: canCfg.ReadTimeout.Duration,
	}
	group.disabled.Store(cfg.Disable)
	if group.bufferSize <= 0 {
		group.bufferSize = 2048
	}
	if group.readTimeout <= 0 {
		group.readTimeout = 50 * time.Millisecond
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
	g.mu.Lock()
	next := g.next
	g.mu.Unlock()
	if next.IsZero() {
		return true
	}
	return !now.Before(next)
}

func (g *readGroup) Perform(now time.Time, logger zerolog.Logger) int {
	if g.disabled.Load() {
		return 0
	}
	ttl := g.cfg.TTL.Duration
	if ttl > 0 {
		g.mu.Lock()
		g.next = now.Add(ttl)
		g.mu.Unlock()
	}
	start := time.Now()
	errors := g.readOnce(now, logger)
	duration := time.Since(start)
	g.mu.Lock()
	g.lastRun = now
	g.lastDuration = duration
	g.mu.Unlock()
	return errors
}

func (g *readGroup) readOnce(now time.Time, logger zerolog.Logger) int {
	conn, err := g.ensureConnection()
	if err != nil {
		logger.Error().Err(err).Str("group", g.cfg.ID).Msg("can stream connection failed")
		g.invalidateAll(now, "can.connect", err.Error())
		return 1
	}
	tmp := make([]byte, g.bufferSize)
	totalErrors := 0
	for {
		if deadlineErr := conn.SetReadDeadline(time.Now().Add(g.readTimeout)); deadlineErr != nil {
			logger.Debug().Err(deadlineErr).Str("group", g.cfg.ID).Msg("set read deadline failed")
		}
		n, err := conn.Read(tmp)
		if n > 0 {
			g.buffer = append(g.buffer, tmp[:n]...)
			totalErrors += g.drainBuffer(now, logger)
		}
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				break
			}
			totalErrors++
			logger.Error().Err(err).Str("group", g.cfg.ID).Msg("can stream read failed")
			g.invalidateAll(now, "can.read", err.Error())
			g.closeConnection()
			break
		}
		if n == 0 {
			break
		}
	}
	return totalErrors
}

func (g *readGroup) drainBuffer(now time.Time, logger zerolog.Logger) int {
	errors := 0
	offset := 0
	for offset < len(g.buffer) {
		frame, consumed, err := decodeFrame(g.buffer[offset:])
		if err != nil {
			errors++
			logger.Error().Err(err).Str("group", g.cfg.ID).Msg("decode CAN frame failed")
			offset++
			continue
		}
		if consumed == 0 {
			break
		}
		offset += consumed
		if frame.remote {
			continue
		}
		errors += g.applyFrame(now, frame, logger)
	}
	if offset > 0 {
		copy(g.buffer, g.buffer[offset:])
		g.buffer = g.buffer[:len(g.buffer)-offset]
	}
	return errors
}

func (g *readGroup) applyFrame(now time.Time, frm *frame, logger zerolog.Logger) int {
	key := frameKey{id: frm.id, extended: frm.extended}
	bindings := g.bindings[key]
	if len(bindings) == 0 {
		return 0
	}
	payload := frm.data[:]
	errors := 0
	for _, binding := range bindings {
		if binding.channel != nil && frm.channel != *binding.channel {
			continue
		}
		for _, sig := range binding.signals {
			value, err := sig.decode(payload)
			if err != nil {
				errors++
				sig.cell.MarkInvalid(now, "can.decode", err.Error())
				logger.Error().Err(err).Str("group", g.cfg.ID).Str("cell", sig.cellConfig.ID).Msg("decode CAN signal failed")
				continue
			}
			if err := sig.cell.SetValue(value, now, sig.quality); err != nil {
				errors++
				sig.cell.MarkInvalid(now, "can.apply", err.Error())
				logger.Error().Err(err).Str("group", g.cfg.ID).Str("cell", sig.cellConfig.ID).Msg("apply CAN value failed")
				continue
			}
		}
	}
	return errors
}

func (g *readGroup) ensureConnection() (net.Conn, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.conn != nil {
		return g.conn, nil
	}
	protocol := strings.ToLower(strings.TrimSpace(g.canCfg.Protocol))
	if protocol == "" {
		protocol = "udp"
	}
	mode := strings.ToLower(strings.TrimSpace(g.canCfg.Mode))
	if mode != "" && mode != "dial" {
		return nil, fmt.Errorf("unsupported CAN mode %q", g.canCfg.Mode)
	}
	address := strings.TrimSpace(g.cfg.Endpoint.Address)
	if address == "" {
		return nil, errors.New("endpoint address is required")
	}
	timeout := g.cfg.Endpoint.Timeout.Duration
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	switch protocol {
	case "udp":
		remote, err := net.ResolveUDPAddr("udp", address)
		if err != nil {
			return nil, fmt.Errorf("resolve udp address %s: %w", address, err)
		}
		conn, err := net.DialUDP("udp", nil, remote)
		if err != nil {
			return nil, fmt.Errorf("dial udp %s: %w", address, err)
		}
		g.conn = conn
	case "tcp":
		dialer := &net.Dialer{Timeout: timeout}
		conn, err := dialer.Dial("tcp", address)
		if err != nil {
			return nil, fmt.Errorf("dial tcp %s: %w", address, err)
		}
		g.conn = conn
	default:
		return nil, fmt.Errorf("unsupported CAN protocol %q", protocol)
	}
	return g.conn, nil
}

func (g *readGroup) SetDisabled(disabled bool) {
	g.disabled.Store(disabled)
	if disabled {
		g.mu.Lock()
		g.next = time.Time{}
		g.closeConnectionLocked()
		g.mu.Unlock()
	}
}

func (g *readGroup) Status() runtimeReaders.ReadGroupStatus {
	g.mu.Lock()
	defer g.mu.Unlock()
	return runtimeReaders.ReadGroupStatus{
		ID:           g.cfg.ID,
		Function:     "can",
		Disabled:     g.disabled.Load(),
		NextRun:      g.next,
		LastRun:      g.lastRun,
		LastDuration: g.lastDuration,
		Source:       g.cfg.Source,
	}
}

func (g *readGroup) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closeConnectionLocked()
	g.buffer = nil
}

func (g *readGroup) closeConnection() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closeConnectionLocked()
}

func (g *readGroup) closeConnectionLocked() {
	if g.conn != nil {
		_ = g.conn.Close()
		g.conn = nil
	}
}

func (g *readGroup) invalidateAll(ts time.Time, code, message string) {
	for _, cell := range g.allCells {
		cell.MarkInvalid(ts, code, message)
	}
}

func decodeFrame(buf []byte) (*frame, int, error) {
	if len(buf) < 13 {
		return nil, 0, nil
	}
	ctrl := buf[0]
	dlc := ctrl & 0x0F
	if dlc > 8 {
		return nil, 0, fmt.Errorf("invalid dlc %d", dlc)
	}
	extended := ctrl&0x80 != 0
	remote := ctrl&0x40 != 0
	channel := uint8((ctrl >> 5) & 0x01)
	id := binary.BigEndian.Uint32(buf[1:5])
	if extended {
		id &= 0x1FFFFFFF
	} else {
		id &= 0x7FF
	}
	var data [8]byte
	copy(data[:], buf[5:13])
	return &frame{
		id:       id,
		extended: extended,
		channel:  channel,
		remote:   remote,
		dlc:      dlc,
		data:     data,
	}, 13, nil
}

func (s signalBinding) decode(data []byte) (interface{}, error) {
	raw, signed, err := extractSignalBits(s.signal, data)
	if err != nil {
		return nil, err
	}
	base := float64(raw)
	if s.signal.IsSigned {
		base = float64(signed)
	}
	value := base*s.factor + s.offset
	switch s.cellConfig.Type {
	case config.ValueKindBool:
		return raw != 0, nil
	case config.ValueKindInteger:
		return int64(math.Round(value)), nil
	case config.ValueKindString:
		return fmt.Sprintf("%v", value), nil
	default:
		return value, nil
	}
}

func extractSignalBits(signal *dbc.SignalDef, data []byte) (uint64, int64, error) {
	if signal == nil {
		return 0, 0, errors.New("signal definition is nil")
	}
	if signal.Size == 0 {
		return 0, 0, fmt.Errorf("signal %s size is zero", signal.Name)
	}
	if signal.Size > 64 {
		return 0, 0, fmt.Errorf("signal %s size %d exceeds 64 bits", signal.Name, signal.Size)
	}
	if len(data) < 8 {
		return 0, 0, fmt.Errorf("signal %s requires 8 bytes, have %d", signal.Name, len(data))
	}
	var raw uint64
	if signal.IsBigEndian {
		raw = readMotorolaBits(signal.StartBit, signal.Size, data)
	} else {
		raw = readIntelBits(signal.StartBit, signal.Size, data)
	}
	var signed int64
	if signal.IsSigned {
		signed = signExtend(raw, signal.Size)
	} else {
		signed = int64(raw)
	}
	return raw, signed, nil
}

func readIntelBits(start, size uint64, data []byte) uint64 {
	var value uint64
	for i := uint64(0); i < size; i++ {
		bitIndex := start + i
		byteIndex := bitIndex / 8
		bit := bitIndex % 8
		if byteIndex >= uint64(len(data)) {
			break
		}
		if data[byteIndex]&(1<<bit) != 0 {
			value |= 1 << i
		}
	}
	return value
}

func readMotorolaBits(start, size uint64, data []byte) uint64 {
	var value uint64
	bitIndex := start
	for i := uint64(0); i < size; i++ {
		byteIndex := bitIndex / 8
		bit := 7 - (bitIndex % 8)
		if byteIndex >= uint64(len(data)) {
			break
		}
		if data[byteIndex]&(1<<bit) != 0 {
			value |= 1 << (size - 1 - i)
		}
		if bitIndex%8 == 0 {
			bitIndex = ((bitIndex/8)+1)*8 + 7
		} else {
			bitIndex--
		}
	}
	return value
}

func signExtend(value uint64, size uint64) int64 {
	if size == 0 {
		return 0
	}
	signBit := uint64(1) << (size - 1)
	if value&signBit == 0 {
		return int64(value)
	}
	mask := ^uint64(0) << size
	return int64(value | mask)
}

type messageIndex struct {
	byID   map[frameKey]*dbc.MessageDef
	byName map[string]*dbc.MessageDef
}

func indexMessages(defs []dbc.Def) messageIndex {
	idx := messageIndex{
		byID:   make(map[frameKey]*dbc.MessageDef),
		byName: make(map[string]*dbc.MessageDef),
	}
	for _, def := range defs {
		msg, ok := def.(*dbc.MessageDef)
		if !ok {
			continue
		}
		key := frameKey{id: msg.MessageID.ToCAN(), extended: msg.MessageID.IsExtended()}
		copyDef := *msg
		idx.byID[key] = &copyDef
		idx.byName[strings.ToLower(string(msg.Name))] = &copyDef
	}
	return idx
}

func resolveMessage(cfg config.CANFrameBindingConfig, idx messageIndex) (*dbc.MessageDef, frameKey, error) {
	var msg *dbc.MessageDef
	var ok bool
	if cfg.Message != "" {
		msg, ok = idx.byName[strings.ToLower(cfg.Message)]
		if !ok {
			return nil, frameKey{}, fmt.Errorf("unknown CAN message %s", cfg.Message)
		}
	} else {
		if cfg.FrameID == "" {
			return nil, frameKey{}, fmt.Errorf("frame requires either message or frame_id")
		}
		id, err := parseCANID(cfg.FrameID)
		if err != nil {
			return nil, frameKey{}, err
		}
		key := frameKey{id: id, extended: false}
		if cfg.Extended != nil {
			key.extended = *cfg.Extended
		}
		if msg, ok = idx.byID[key]; !ok {
			if cfg.Extended == nil {
				key.extended = true
				msg, ok = idx.byID[key]
			}
			if !ok {
				return nil, frameKey{}, fmt.Errorf("no message with id 0x%X", id)
			}
		}
	}
	key := frameKey{id: msg.MessageID.ToCAN(), extended: msg.MessageID.IsExtended()}
	return msg, key, nil
}

func findSignal(msg *dbc.MessageDef, name string) (*dbc.SignalDef, error) {
	if msg == nil {
		return nil, errors.New("message definition is nil")
	}
	for i := range msg.Signals {
		signal := msg.Signals[i]
		if strings.EqualFold(string(signal.Name), name) {
			copySignal := signal
			return &copySignal, nil
		}
	}
	return nil, fmt.Errorf("message %s has no signal %s", msg.Name, name)
}

func parseCANID(value string) (uint32, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, fmt.Errorf("empty frame id")
	}
	base := 10
	if strings.HasPrefix(trimmed, "0x") || strings.HasPrefix(trimmed, "0X") {
		base = 16
		trimmed = trimmed[2:]
	}
	parsed, err := strconv.ParseUint(trimmed, base, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid frame id %q: %w", value, err)
	}
	return uint32(parsed), nil
}

func describeFrame(cfg config.CANFrameBindingConfig) string {
	switch {
	case cfg.Message != "":
		return cfg.Message
	case cfg.FrameID != "":
		return cfg.FrameID
	default:
		return "<unnamed>"
	}
}

func resolveRelativePath(base, target string) string {
	if target == "" {
		return target
	}
	if filepath.IsAbs(target) || base == "" {
		return target
	}
	baseDir := filepath.Dir(base)
	return filepath.Join(baseDir, target)
}

func cloneQuality(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}
