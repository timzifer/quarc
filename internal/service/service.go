package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/rs/zerolog"
	"github.com/simonvetter/modbus"

	"modbus_processor/internal/config"
	"modbus_processor/internal/remote"
)

type registerType int

const (
	registerTypeCoil registerType = iota
	registerTypeDiscrete
	registerTypeHolding
	registerTypeInput
)

// Service encapsulates the Modbus processing logic.
type Service struct {
	cfg           *config.Config
	logger        zerolog.Logger
	loopInterval  time.Duration
	clientFactory remote.ClientFactory

	stateMu       sync.RWMutex
	coilState     []bool
	discreteState []bool
	holdingState  []uint16
	inputState    []uint16

	coils    []*boolRegister
	discrete []*boolRegister
	holding  []*wordRegister
	input    []*wordRegister

	server *modbus.ModbusServer
}

type boolRegister struct {
	name       string
	address    uint16
	mu         *sync.RWMutex
	arr        *[]bool
	expression string
	program    *vm.Program
	remote     *remoteBinding
}

type wordRegister struct {
	name       string
	address    uint16
	mu         *sync.RWMutex
	arr        *[]uint16
	expression string
	program    *vm.Program
	remote     *remoteBinding
}

type remoteBinding struct {
	cfg     *config.RemoteRegisterConfig
	regType registerType
	last    time.Time
}

type modbusHandler struct {
	service *Service
}

// New creates a new service from the provided configuration.
func New(cfg *config.Config, logger zerolog.Logger, factory remote.ClientFactory) (*Service, error) {
	if cfg == nil {
		return nil, errors.New("config must not be nil")
	}
	if factory == nil {
		factory = remote.NewTCPClientFactory()
	}

	svc := &Service{
		cfg:           cfg,
		logger:        logger,
		loopInterval:  cfg.LoopInterval(),
		clientFactory: factory,
	}

	var err error
	svc.coilState, svc.coils, err = buildCoils(cfg.Coils, &svc.stateMu)
	if err != nil {
		return nil, err
	}
	for _, reg := range svc.coils {
		reg.arr = &svc.coilState
	}

	svc.discreteState, svc.discrete, err = buildDiscrete(cfg.Discrete, &svc.stateMu)
	if err != nil {
		return nil, err
	}
	for _, reg := range svc.discrete {
		reg.arr = &svc.discreteState
	}

	svc.holdingState, svc.holding, err = buildHolding(cfg.Holding, &svc.stateMu)
	if err != nil {
		return nil, err
	}
	for _, reg := range svc.holding {
		reg.arr = &svc.holdingState
	}

	svc.inputState, svc.input, err = buildInput(cfg.Input, &svc.stateMu)
	if err != nil {
		return nil, err
	}
	for _, reg := range svc.input {
		reg.arr = &svc.inputState
	}

	if err := svc.compileExpressions(); err != nil {
		return nil, err
	}

	return svc, nil
}

func buildCoils(cfgs []config.CoilConfig, mu *sync.RWMutex) ([]bool, []*boolRegister, error) {
	max := maxBoolIndex(len(cfgs), func(i int) uint16 { return cfgs[i].Address })
	arr := make([]bool, max)
	registers := make([]*boolRegister, 0, len(cfgs))
	for _, cfg := range cfgs {
		if int(cfg.Address) >= len(arr) {
			return nil, nil, fmt.Errorf("coil address %d out of range", cfg.Address)
		}
		arr[cfg.Address] = cfg.InitialValue
		reg := &boolRegister{
			name:    cfg.Name,
			address: cfg.Address,
			mu:      mu,
			arr:     &arr,
		}
		if cfg.Remote != nil {
			binding, err := newRemoteBinding(cfg.Remote)
			if err != nil {
				return nil, nil, err
			}
			reg.remote = binding
		}
		registers = append(registers, reg)
	}
	return arr, registers, nil
}

func buildDiscrete(cfgs []config.DiscreteInputConfig, mu *sync.RWMutex) ([]bool, []*boolRegister, error) {
	max := maxBoolIndex(len(cfgs), func(i int) uint16 { return cfgs[i].Address })
	arr := make([]bool, max)
	registers := make([]*boolRegister, 0, len(cfgs))
	for _, cfg := range cfgs {
		if int(cfg.Address) >= len(arr) {
			return nil, nil, fmt.Errorf("discrete input address %d out of range", cfg.Address)
		}
		arr[cfg.Address] = cfg.InitialValue
		reg := &boolRegister{
			name:       cfg.Name,
			address:    cfg.Address,
			mu:         mu,
			arr:        &arr,
			expression: cfg.Expression,
		}
		if cfg.Remote != nil {
			binding, err := newRemoteBinding(cfg.Remote)
			if err != nil {
				return nil, nil, err
			}
			reg.remote = binding
		}
		registers = append(registers, reg)
	}
	return arr, registers, nil
}

func buildHolding(cfgs []config.HoldingRegisterConfig, mu *sync.RWMutex) ([]uint16, []*wordRegister, error) {
	max := maxWordIndex(len(cfgs), func(i int) uint16 { return cfgs[i].Address })
	arr := make([]uint16, max)
	registers := make([]*wordRegister, 0, len(cfgs))
	for _, cfg := range cfgs {
		if int(cfg.Address) >= len(arr) {
			return nil, nil, fmt.Errorf("holding register address %d out of range", cfg.Address)
		}
		arr[cfg.Address] = cfg.InitialValue
		reg := &wordRegister{
			name:    cfg.Name,
			address: cfg.Address,
			mu:      mu,
			arr:     &arr,
		}
		if cfg.Remote != nil {
			binding, err := newRemoteBinding(cfg.Remote)
			if err != nil {
				return nil, nil, err
			}
			reg.remote = binding
		}
		registers = append(registers, reg)
	}
	return arr, registers, nil
}

func buildInput(cfgs []config.InputRegisterConfig, mu *sync.RWMutex) ([]uint16, []*wordRegister, error) {
	max := maxWordIndex(len(cfgs), func(i int) uint16 { return cfgs[i].Address })
	arr := make([]uint16, max)
	registers := make([]*wordRegister, 0, len(cfgs))
	for _, cfg := range cfgs {
		if int(cfg.Address) >= len(arr) {
			return nil, nil, fmt.Errorf("input register address %d out of range", cfg.Address)
		}
		arr[cfg.Address] = cfg.InitialValue
		reg := &wordRegister{
			name:       cfg.Name,
			address:    cfg.Address,
			mu:         mu,
			arr:        &arr,
			expression: cfg.Expression,
		}
		if cfg.Remote != nil {
			binding, err := newRemoteBinding(cfg.Remote)
			if err != nil {
				return nil, nil, err
			}
			reg.remote = binding
		}
		registers = append(registers, reg)
	}
	return arr, registers, nil
}

func maxBoolIndex(length int, address func(i int) uint16) int {
	max := 0
	for i := 0; i < length; i++ {
		candidate := int(address(i)) + 1
		if candidate > max {
			max = candidate
		}
	}
	return max
}

func maxWordIndex(length int, address func(i int) uint16) int {
	max := 0
	for i := 0; i < length; i++ {
		candidate := int(address(i)) + 1
		if candidate > max {
			max = candidate
		}
	}
	return max
}

func newRemoteBinding(cfg *config.RemoteRegisterConfig) (*remoteBinding, error) {
	if cfg == nil {
		return nil, nil
	}
	rt, err := parseRegisterType(cfg.Type)
	if err != nil {
		return nil, err
	}
	return &remoteBinding{cfg: cfg, regType: rt}, nil
}

func parseRegisterType(value string) (registerType, error) {
	switch strings.ToLower(value) {
	case "coil", "coils":
		return registerTypeCoil, nil
	case "discrete", "discrete_input", "discrete_inputs":
		return registerTypeDiscrete, nil
	case "holding", "holding_register", "holding_registers":
		return registerTypeHolding, nil
	case "input", "input_register", "input_registers":
		return registerTypeInput, nil
	default:
		return 0, fmt.Errorf("unknown register type %q", value)
	}
}

func (s *Service) compileExpressions() error {
	env := s.variableTemplate()
	for _, reg := range s.discrete {
		if reg.expression == "" {
			continue
		}
		program, err := expr.Compile(reg.expression, expr.Env(env))
		if err != nil {
			return fmt.Errorf("compile expression for %s: %w", reg.name, err)
		}
		reg.program = program
	}
	for _, reg := range s.input {
		if reg.expression == "" {
			continue
		}
		program, err := expr.Compile(reg.expression, expr.Env(env))
		if err != nil {
			return fmt.Errorf("compile expression for %s: %w", reg.name, err)
		}
		reg.program = program
	}
	return nil
}

func (r *boolRegister) value() bool {
	if r.mu != nil {
		r.mu.RLock()
		defer r.mu.RUnlock()
	}
	return (*r.arr)[r.address]
}

func (r *boolRegister) set(v bool) {
	if r.mu != nil {
		r.mu.Lock()
		defer r.mu.Unlock()
	}
	(*r.arr)[r.address] = v
}

func (r *wordRegister) value() uint16 {
	if r.mu != nil {
		r.mu.RLock()
		defer r.mu.RUnlock()
	}
	return (*r.arr)[r.address]
}

func (r *wordRegister) set(v uint16) {
	if r.mu != nil {
		r.mu.Lock()
		defer r.mu.Unlock()
	}
	(*r.arr)[r.address] = v
}

func (s *Service) variableTemplate() map[string]interface{} {
	env := make(map[string]interface{})
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	for _, reg := range s.coils {
		env[reg.name] = (*reg.arr)[reg.address]
	}
	for _, reg := range s.discrete {
		env[reg.name] = (*reg.arr)[reg.address]
	}
	for _, reg := range s.holding {
		env[reg.name] = (*reg.arr)[reg.address]
	}
	for _, reg := range s.input {
		env[reg.name] = (*reg.arr)[reg.address]
	}
	return env
}

// Run starts the Modbus server and processing loop until the context is cancelled.
func (s *Service) Run(ctx context.Context) error {
	if s.cfg.ListenAddress != "" {
		address := s.cfg.ListenAddress
		if !strings.Contains(address, "://") {
			address = "tcp://" + address
		}
		server, err := modbus.NewServer(&modbus.ServerConfiguration{URL: address}, &modbusHandler{service: s})
		if err != nil {
			return fmt.Errorf("create modbus server: %w", err)
		}
		if err := server.Start(); err != nil {
			return fmt.Errorf("start modbus server: %w", err)
		}
		s.server = server
		s.logger.Info().Str("address", address).Msg("Modbus server listening")
	}

	ticker := time.NewTicker(s.loopInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info().Msg("service stopping")
			if s.server != nil {
				if err := s.server.Stop(); err != nil {
					s.logger.Error().Err(err).Msg("failed to stop modbus server")
				}
			}
			return nil
		case now := <-ticker.C:
			if err := s.IterateOnce(ctx, now); err != nil {
				s.logger.Error().Err(err).Msg("iteration failure")
			}
		}
	}
}

// IterateOnce performs a single loop iteration. Exposed for tests.
func (s *Service) IterateOnce(ctx context.Context, now time.Time) error {
	if err := s.refreshRemotes(now); err != nil {
		return err
	}
	env := s.snapshotVariables()
	if err := s.evaluateDiscrete(env); err != nil {
		return err
	}
	if err := s.evaluateInputs(env); err != nil {
		return err
	}
	if err := s.pushRemotes(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Service) refreshRemotes(now time.Time) error {
	for _, reg := range s.coils {
		if err := s.pullBoolRegister(reg, now); err != nil {
			return err
		}
	}
	for _, reg := range s.discrete {
		if err := s.pullBoolRegister(reg, now); err != nil {
			return err
		}
	}
	for _, reg := range s.holding {
		if err := s.pullWordRegister(reg, now); err != nil {
			return err
		}
	}
	for _, reg := range s.input {
		if err := s.pullWordRegister(reg, now); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) pullBoolRegister(reg *boolRegister, now time.Time) error {
	if reg.remote == nil {
		return nil
	}
	interval := reg.remote.cfg.Interval.Duration
	if interval <= 0 {
		return nil
	}
	if !reg.remote.last.IsZero() && now.Sub(reg.remote.last) < interval {
		return nil
	}
	client, err := s.clientFactory(*reg.remote.cfg)
	if err != nil {
		return err
	}
	defer client.Close()

	var value bool
	switch reg.remote.regType {
	case registerTypeCoil:
		raw, err := client.ReadCoils(reg.remote.cfg.Register, 1)
		if err != nil {
			return fmt.Errorf("read coil %s: %w", reg.name, err)
		}
		if len(raw) == 0 {
			return fmt.Errorf("coil read returned no data")
		}
		value = raw[0]&0x01 == 0x01
	case registerTypeDiscrete:
		raw, err := client.ReadDiscreteInputs(reg.remote.cfg.Register, 1)
		if err != nil {
			return fmt.Errorf("read discrete %s: %w", reg.name, err)
		}
		if len(raw) == 0 {
			return fmt.Errorf("discrete read returned no data")
		}
		value = raw[0]&0x01 == 0x01
	default:
		return fmt.Errorf("unsupported remote type %v for bool register", reg.remote.regType)
	}
	reg.set(value)
	reg.remote.last = now
	return nil
}

func (s *Service) pullWordRegister(reg *wordRegister, now time.Time) error {
	if reg.remote == nil {
		return nil
	}
	interval := reg.remote.cfg.Interval.Duration
	if interval <= 0 {
		return nil
	}
	if !reg.remote.last.IsZero() && now.Sub(reg.remote.last) < interval {
		return nil
	}
	client, err := s.clientFactory(*reg.remote.cfg)
	if err != nil {
		return err
	}
	defer client.Close()

	var value uint16
	switch reg.remote.regType {
	case registerTypeHolding:
		raw, err := client.ReadHoldingRegisters(reg.remote.cfg.Register, 1)
		if err != nil {
			return fmt.Errorf("read holding %s: %w", reg.name, err)
		}
		if len(raw) < 2 {
			return fmt.Errorf("holding read returned no data")
		}
		value = uint16(raw[0])<<8 | uint16(raw[1])
	case registerTypeInput:
		raw, err := client.ReadInputRegisters(reg.remote.cfg.Register, 1)
		if err != nil {
			return fmt.Errorf("read input %s: %w", reg.name, err)
		}
		if len(raw) < 2 {
			return fmt.Errorf("input read returned no data")
		}
		value = uint16(raw[0])<<8 | uint16(raw[1])
	default:
		return fmt.Errorf("unsupported remote type %v for word register", reg.remote.regType)
	}
	reg.set(value)
	reg.remote.last = now
	return nil
}

func (s *Service) snapshotVariables() map[string]interface{} {
	env := make(map[string]interface{})
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	for _, reg := range s.coils {
		env[reg.name] = (*reg.arr)[reg.address]
	}
	for _, reg := range s.discrete {
		env[reg.name] = (*reg.arr)[reg.address]
	}
	for _, reg := range s.holding {
		env[reg.name] = (*reg.arr)[reg.address]
	}
	for _, reg := range s.input {
		env[reg.name] = (*reg.arr)[reg.address]
	}
	return env
}

func (s *Service) evaluateDiscrete(env map[string]interface{}) error {
	for _, reg := range s.discrete {
		if reg.program == nil {
			continue
		}
		result, err := expr.Run(reg.program, env)
		if err != nil {
			return fmt.Errorf("evaluate %s: %w", reg.name, err)
		}
		boolVal, ok := result.(bool)
		if !ok {
			return fmt.Errorf("expression for %s must return bool", reg.name)
		}
		reg.set(boolVal)
		env[reg.name] = boolVal
	}
	return nil
}

func (s *Service) evaluateInputs(env map[string]interface{}) error {
	for _, reg := range s.input {
		if reg.program == nil {
			continue
		}
		result, err := expr.Run(reg.program, env)
		if err != nil {
			return fmt.Errorf("evaluate %s: %w", reg.name, err)
		}
		converted, err := toUint16(result)
		if err != nil {
			return fmt.Errorf("expression for %s: %w", reg.name, err)
		}
		reg.set(converted)
		env[reg.name] = converted
	}
	return nil
}

func toUint16(value interface{}) (uint16, error) {
	switch v := value.(type) {
	case uint16:
		return v, nil
	case uint8:
		return uint16(v), nil
	case int:
		if v < 0 || v > 0xFFFF {
			return 0, fmt.Errorf("value %d out of range", v)
		}
		return uint16(v), nil
	case int64:
		if v < 0 || v > 0xFFFF {
			return 0, fmt.Errorf("value %d out of range", v)
		}
		return uint16(v), nil
	case float64:
		if v < 0 || v > 0xFFFF {
			return 0, fmt.Errorf("value %f out of range", v)
		}
		return uint16(v), nil
	case float32:
		if v < 0 || v > 0xFFFF {
			return 0, fmt.Errorf("value %f out of range", v)
		}
		return uint16(v), nil
	default:
		return 0, fmt.Errorf("unsupported value type %T", value)
	}
}

func (s *Service) pushRemotes(ctx context.Context) error {
	for _, reg := range s.discrete {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.pushBool(reg); err != nil {
			return err
		}
	}
	for _, reg := range s.input {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.pushWord(reg); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) pushBool(reg *boolRegister) error {
	if reg.remote == nil || !reg.remote.cfg.PushOnUpdate {
		return nil
	}
	client, err := s.clientFactory(*reg.remote.cfg)
	if err != nil {
		return err
	}
	defer client.Close()

	value := reg.value()
	switch reg.remote.regType {
	case registerTypeCoil:
		var payload uint16
		if value {
			payload = 0xFF00
		}
		if _, err := client.WriteSingleCoil(reg.remote.cfg.Register, payload); err != nil {
			return fmt.Errorf("write coil %s: %w", reg.name, err)
		}
	case registerTypeHolding:
		var numeric uint16
		if value {
			numeric = 1
		}
		if _, err := client.WriteSingleRegister(reg.remote.cfg.Register, numeric); err != nil {
			return fmt.Errorf("write holding %s: %w", reg.name, err)
		}
	default:
		return fmt.Errorf("unsupported remote type %v for bool push", reg.remote.regType)
	}
	return nil
}

func (s *Service) pushWord(reg *wordRegister) error {
	if reg.remote == nil || !reg.remote.cfg.PushOnUpdate {
		return nil
	}
	client, err := s.clientFactory(*reg.remote.cfg)
	if err != nil {
		return err
	}
	defer client.Close()

	value := reg.value()
	switch reg.remote.regType {
	case registerTypeHolding:
		if _, err := client.WriteSingleRegister(reg.remote.cfg.Register, value); err != nil {
			return fmt.Errorf("write holding %s: %w", reg.name, err)
		}
	case registerTypeCoil:
		payload := uint16(0x0000)
		if value > 0 {
			payload = 0xFF00
		}
		if _, err := client.WriteSingleCoil(reg.remote.cfg.Register, payload); err != nil {
			return fmt.Errorf("write coil %s: %w", reg.name, err)
		}
	default:
		return fmt.Errorf("unsupported remote type %v for word push", reg.remote.regType)
	}
	return nil
}

// ModbusServer exposes the underlying server instance.
func (s *Service) ModbusServer() *modbus.ModbusServer {
	return s.server
}

// CoilsSnapshot returns a copy of the coil state.
func (s *Service) CoilsSnapshot() []bool {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	out := make([]bool, len(s.coilState))
	copy(out, s.coilState)
	return out
}

// DiscreteSnapshot returns a copy of the discrete input state.
func (s *Service) DiscreteSnapshot() []bool {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	out := make([]bool, len(s.discreteState))
	copy(out, s.discreteState)
	return out
}

// HoldingSnapshot returns a copy of the holding register state.
func (s *Service) HoldingSnapshot() []uint16 {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	out := make([]uint16, len(s.holdingState))
	copy(out, s.holdingState)
	return out
}

// InputSnapshot returns a copy of the input register state.
func (s *Service) InputSnapshot() []uint16 {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	out := make([]uint16, len(s.inputState))
	copy(out, s.inputState)
	return out
}

// Handler implementations for the Modbus server.
func (h *modbusHandler) HandleCoils(req *modbus.CoilsRequest) ([]bool, error) {
	svc := h.service
	svc.stateMu.Lock()
	defer svc.stateMu.Unlock()

	end := int(req.Addr) + int(req.Quantity)
	if end > len(svc.coilState) {
		return nil, modbus.ErrIllegalDataAddress
	}
	if req.IsWrite {
		if len(req.Args) < int(req.Quantity) {
			return nil, modbus.ErrIllegalDataValue
		}
		for i := 0; i < int(req.Quantity); i++ {
			svc.coilState[int(req.Addr)+i] = req.Args[i]
		}
		return nil, nil
	}
	res := make([]bool, req.Quantity)
	copy(res, svc.coilState[int(req.Addr):end])
	return res, nil
}

func (h *modbusHandler) HandleDiscreteInputs(req *modbus.DiscreteInputsRequest) ([]bool, error) {
	svc := h.service
	svc.stateMu.RLock()
	defer svc.stateMu.RUnlock()

	end := int(req.Addr) + int(req.Quantity)
	if end > len(svc.discreteState) {
		return nil, modbus.ErrIllegalDataAddress
	}
	res := make([]bool, req.Quantity)
	copy(res, svc.discreteState[int(req.Addr):end])
	return res, nil
}

func (h *modbusHandler) HandleHoldingRegisters(req *modbus.HoldingRegistersRequest) ([]uint16, error) {
	svc := h.service
	svc.stateMu.Lock()
	defer svc.stateMu.Unlock()

	end := int(req.Addr) + int(req.Quantity)
	if end > len(svc.holdingState) {
		return nil, modbus.ErrIllegalDataAddress
	}
	if req.IsWrite {
		if len(req.Args) < int(req.Quantity) {
			return nil, modbus.ErrIllegalDataValue
		}
		for i := 0; i < int(req.Quantity); i++ {
			svc.holdingState[int(req.Addr)+i] = req.Args[i]
		}
		return nil, nil
	}
	res := make([]uint16, req.Quantity)
	copy(res, svc.holdingState[int(req.Addr):end])
	return res, nil
}

func (h *modbusHandler) HandleInputRegisters(req *modbus.InputRegistersRequest) ([]uint16, error) {
	svc := h.service
	svc.stateMu.RLock()
	defer svc.stateMu.RUnlock()

	end := int(req.Addr) + int(req.Quantity)
	if end > len(svc.inputState) {
		return nil, modbus.ErrIllegalDataAddress
	}
	res := make([]uint16, req.Quantity)
	copy(res, svc.inputState[int(req.Addr):end])
	return res, nil
}
