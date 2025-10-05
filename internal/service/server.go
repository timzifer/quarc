package service

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"modbus_processor/internal/config"
)

type serverMapping struct {
	cell    *cell
	address uint16
	scale   float64
	signed  bool
}

type modbusServer struct {
	logger    zerolog.Logger
	unitID    uint8
	listener  net.Listener
	stopOnce  sync.Once
	stopCh    chan struct{}
	wg        sync.WaitGroup
	mappings  []serverMapping
	registers []uint16
	mu        sync.RWMutex
	connsMu   sync.Mutex
	conns     map[net.Conn]struct{}
}

func newModbusServer(cfg config.ServerConfig, cells *cellStore, logger zerolog.Logger) (*modbusServer, error) {
	cfg, mappings, err := prepareServer(cfg, cells)
	if err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, fmt.Errorf("listen modbus server on %s: %w", cfg.Listen, err)
	}

	srv := &modbusServer{
		logger:    logger,
		unitID:    cfg.UnitID,
		listener:  listener,
		stopCh:    make(chan struct{}),
		mappings:  mappings,
		registers: make([]uint16, 65536),
		conns:     make(map[net.Conn]struct{}),
	}

	srv.wg.Add(1)
	go srv.acceptLoop()

	logger.Info().Str("listen", listener.Addr().String()).Msg("modbus server started")
	return srv, nil
}

func prepareServer(cfg config.ServerConfig, cells *cellStore) (config.ServerConfig, []serverMapping, error) {
	if cfg.Listen == "" {
		cfg.Listen = ":15020"
	}
	if len(cfg.Cells) == 0 {
		return cfg, nil, fmt.Errorf("modbus server requires at least one cell mapping")
	}

	mappings := make([]serverMapping, 0, len(cfg.Cells))
	used := make(map[uint16]struct{}, len(cfg.Cells))
	for _, cellCfg := range cfg.Cells {
		cell, err := cells.mustGet(cellCfg.Cell)
		if err != nil {
			return cfg, nil, fmt.Errorf("modbus server: %w", err)
		}
		if cell.cfg.Type == config.ValueKindString {
			return cfg, nil, fmt.Errorf("modbus server: cell %s uses unsupported type string", cellCfg.Cell)
		}
		if _, exists := used[cellCfg.Address]; exists {
			return cfg, nil, fmt.Errorf("modbus server: duplicate address %d", cellCfg.Address)
		}
		used[cellCfg.Address] = struct{}{}
		scale := cellCfg.Scale
		if scale == 0 {
			scale = 1
		}
		mappings = append(mappings, serverMapping{
			cell:    cell,
			address: cellCfg.Address,
			scale:   scale,
			signed:  cellCfg.Signed,
		})
	}

	return cfg, mappings, nil
}

func (s *modbusServer) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if isClosedConnError(err) {
				return
			}
			select {
			case <-s.stopCh:
				return
			default:
				s.logger.Error().Err(err).Msg("modbus server accept failed")
			}
			continue
		}
		s.wg.Add(1)
		s.trackConn(conn)
		go s.handleConn(conn)
	}
}

func (s *modbusServer) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	defer s.untrackConn(conn)

	for {
		header := make([]byte, 7)
		if _, err := io.ReadFull(conn, header); err != nil {
			if !isClosedConnError(err) {
				s.logger.Trace().Err(err).Msg("modbus server read header failed")
			}
			return
		}
		length := binary.BigEndian.Uint16(header[4:6])
		if length == 0 {
			s.writeException(conn, header, []byte{header[6]}, 0x03)
			continue
		}
		payload := make([]byte, length)
		payload[0] = header[6]
		if length > 1 {
			if _, err := io.ReadFull(conn, payload[1:]); err != nil {
				if !isClosedConnError(err) {
					s.logger.Trace().Err(err).Msg("modbus server read payload failed")
				}
				return
			}
		}
		if len(payload) < 2 {
			s.writeException(conn, header, payload, 0x01)
			continue
		}
		unitID := payload[0]
		function := payload[1]
		if s.unitID != 0 && unitID != s.unitID {
			// Ignore requests for other unit ids
			continue
		}
		switch function {
		case 0x04:
			if err := s.handleReadInputRegisters(conn, header, payload); err != nil {
				s.logger.Trace().Err(err).Msg("modbus read input registers failed")
			}
		default:
			s.writeException(conn, header, payload, 0x01)
		}
	}
}

func (s *modbusServer) handleReadInputRegisters(conn net.Conn, header, payload []byte) error {
	if len(payload) < 6 {
		s.writeException(conn, header, payload, 0x03)
		return fmt.Errorf("payload too short")
	}
	unitID := payload[0]
	function := payload[1]
	start := binary.BigEndian.Uint16(payload[2:4])
	quantity := binary.BigEndian.Uint16(payload[4:6])
	if quantity == 0 {
		s.writeException(conn, header, payload, 0x03)
		return fmt.Errorf("zero quantity")
	}
	if int(start)+int(quantity) > len(s.registers) {
		s.writeException(conn, header, payload, 0x02)
		return fmt.Errorf("address out of range")
	}

	data := make([]byte, int(quantity)*2)

	s.mu.RLock()
	for i := 0; i < int(quantity); i++ {
		value := s.registers[int(start)+i]
		binary.BigEndian.PutUint16(data[i*2:], value)
	}
	s.mu.RUnlock()

	pdu := make([]byte, 2+len(data))
	pdu[0] = function
	pdu[1] = byte(len(data))
	copy(pdu[2:], data)

	response := make([]byte, 7+len(pdu))
	copy(response[:4], header[:4])
	binary.BigEndian.PutUint16(response[4:6], uint16(len(pdu)+1))
	response[6] = unitID
	copy(response[7:], pdu)

	_, err := conn.Write(response)
	return err
}

func (s *modbusServer) writeException(conn net.Conn, header, payload []byte, code byte) {
	if len(payload) == 0 {
		payload = []byte{0}
	}
	response := make([]byte, 7+3)
	copy(response[:4], header[:4])
	binary.BigEndian.PutUint16(response[4:6], 3)
	unit := byte(0)
	if len(payload) > 0 {
		unit = payload[0]
	}
	response[6] = unit
	function := byte(0)
	if len(payload) > 1 {
		function = payload[1]
	}
	response[7] = function | 0x80
	response[8] = code
	_, _ = conn.Write(response)
}

func (s *modbusServer) refresh(snapshot map[string]*snapshotValue) {
	if s == nil {
		return
	}
	s.mu.Lock()
	for _, mapping := range s.mappings {
		snap := snapshot[mapping.cell.cfg.ID]
		var register uint16
		if snap != nil && snap.Valid {
			switch mapping.cell.cfg.Type {
			case config.ValueKindBool:
				if val, ok := snap.Value.(bool); ok && val {
					register = 1
				}
			case config.ValueKindNumber:
				if val, ok := snap.Value.(float64); ok {
					register = convertNumberForRegister(val, mapping.scale, mapping.signed)
				}
			}
		}
		s.registers[mapping.address] = register
	}
	s.mu.Unlock()
	s.logger.Trace().Int("mappings", len(s.mappings)).Msg("modbus server registers refreshed")
}

func convertNumberForRegister(value float64, scale float64, signed bool) uint16 {
	if scale == 0 {
		scale = 1
	}
	raw := value / scale
	if signed {
		v := int(math.Round(raw))
		if v < math.MinInt16 {
			v = math.MinInt16
		}
		if v > math.MaxInt16 {
			v = math.MaxInt16
		}
		return uint16(int16(v))
	}
	v := int(math.Round(raw))
	if v < 0 {
		v = 0
	}
	if v > math.MaxUint16 {
		v = math.MaxUint16
	}
	return uint16(v)
}

func (s *modbusServer) close() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		close(s.stopCh)
		_ = s.listener.Close()
		s.closeAllConns()
	})
	s.wg.Wait()
	s.logger.Info().Msg("modbus server stopped")
}

func (s *modbusServer) addr() string {
	if s == nil || s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func isClosedConnError(err error) bool {
	if err == nil {
		return false
	}
	if ne, ok := err.(net.Error); ok && !ne.Temporary() {
		return true
	}
	switch err {
	case io.EOF:
		return true
	}
	return false
}

func (s *modbusServer) trackConn(conn net.Conn) {
	if conn == nil {
		return
	}
	s.connsMu.Lock()
	s.conns[conn] = struct{}{}
	s.connsMu.Unlock()
}

func (s *modbusServer) untrackConn(conn net.Conn) {
	if conn == nil {
		return
	}
	s.connsMu.Lock()
	delete(s.conns, conn)
	s.connsMu.Unlock()
}

func (s *modbusServer) closeAllConns() {
	s.connsMu.Lock()
	conns := make([]net.Conn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	s.connsMu.Unlock()

	for _, conn := range conns {
		_ = conn.SetDeadline(time.Now())
		_ = conn.Close()
	}

	s.connsMu.Lock()
	for _, conn := range conns {
		delete(s.conns, conn)
	}
	s.connsMu.Unlock()
}
