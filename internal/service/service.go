package service

import (
	"context"
	"errors"
	"time"

	"github.com/rs/zerolog"

	"modbus_processor/internal/config"
	"modbus_processor/internal/remote"
)

// Service implements the deterministic three-phase controller.
type Service struct {
	cfg           *config.Config
	logger        zerolog.Logger
	clientFactory remote.ClientFactory

	cells    *cellStore
	reads    []*readGroup
	logic    []*logicBlock
	ordered  []*logicBlock
	writes   []*writeTarget
	server   *modbusServer
	programs []*programBinding

	cycle         time.Duration
	metrics       metrics
	lastCycleTime time.Time
}

type metrics struct {
	CycleCount        uint64
	LastDuration      time.Duration
	LastReadErrors    int
	LastProgramErrors int
	LastEvalErrors    int
	LastWriteErrors   int
}

// New builds a service from configuration and dependencies.
func New(cfg *config.Config, logger zerolog.Logger, factory remote.ClientFactory) (*Service, error) {
	if cfg == nil {
		return nil, errors.New("config must not be nil")
	}
	if factory == nil {
		factory = remote.NewTCPClientFactory()
	}
	dsl, err := newDSLEngine(cfg.DSL, cfg.Helpers, logger)
	if err != nil {
		return nil, err
	}
	cells, err := newCellStore(cfg.Cells)
	if err != nil {
		return nil, err
	}
	reads, err := newReadGroups(cfg.Reads, cells)
	if err != nil {
		return nil, err
	}
	logic, ordered, err := newLogicBlocks(cfg.Logic, cells, dsl, logger)
	if err != nil {
		return nil, err
	}
	programs, err := newProgramBindings(cfg.Programs, cells, logger)
	if err != nil {
		return nil, err
	}
	writes, err := newWriteTargets(cfg.Writes, cells)
	if err != nil {
		return nil, err
	}
	var srv *modbusServer
	if cfg.Server.Enabled {
		serverLogger := logger.With().Str("component", "modbus_server").Logger()
		srv, err = newModbusServer(cfg.Server, cells, serverLogger)
		if err != nil {
			return nil, err
		}
	}

	svc := &Service{
		cfg:           cfg,
		logger:        logger,
		clientFactory: factory,
		cells:         cells,
		reads:         reads,
		logic:         logic,
		ordered:       ordered,
		writes:        writes,
		server:        srv,
		cycle:         cfg.CycleInterval(),
	}
	if len(programs) > 0 {
		svc.programs = programs
	}
	return svc, nil
}

// Validate performs a dry-run validation of the configuration without starting background services.
func Validate(cfg *config.Config, logger zerolog.Logger) error {
	if cfg == nil {
		return errors.New("config must not be nil")
	}

	dsl, err := newDSLEngine(cfg.DSL, cfg.Helpers, logger)
	if err != nil {
		return err
	}
	cells, err := newCellStore(cfg.Cells)
	if err != nil {
		return err
	}
	if _, err := newReadGroups(cfg.Reads, cells); err != nil {
		return err
	}
	if _, _, err := newLogicBlocks(cfg.Logic, cells, dsl, logger); err != nil {
		return err
	}
	if _, err := newProgramBindings(cfg.Programs, cells, logger); err != nil {
		return err
	}
	if _, err := newWriteTargets(cfg.Writes, cells); err != nil {
		return err
	}
	if cfg.Server.Enabled {
		if _, _, err := prepareServer(cfg.Server, cells); err != nil {
			return err
		}
	}
	return nil
}

// Run executes the controller loop until the context is cancelled.
func (s *Service) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.cycle)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			if err := s.IterateOnce(ctx, now); err != nil {
				s.logger.Error().Err(err).Msg("iteration failure")
			}
		}
	}
}

// IterateOnce performs a single deterministic cycle.
func (s *Service) IterateOnce(ctx context.Context, now time.Time) error {
	start := time.Now()
	readErrors := s.readPhase(now)
	programErrors := s.programPhase(now)
	snapshot := s.cells.snapshot()
	evalErrors := s.evalPhase(now, snapshot)
	writeErrors := s.commitPhase(ctx, now)
	s.server.refresh(snapshot)

	s.metrics.CycleCount++
	s.metrics.LastDuration = time.Since(start)
	s.metrics.LastReadErrors = readErrors
	s.metrics.LastProgramErrors = programErrors
	s.metrics.LastEvalErrors = evalErrors
	s.metrics.LastWriteErrors = writeErrors
	s.lastCycleTime = now
	return nil
}

func (s *Service) readPhase(now time.Time) int {
	errors := 0
	for _, group := range s.reads {
		if group.due(now) {
			errors += group.perform(now, s.clientFactory, s.logger)
		}
	}
	return errors
}

func (s *Service) evalPhase(now time.Time, snapshot map[string]*snapshotValue) int {
	errors := 0
	for _, block := range s.ordered {
		errors += block.evaluate(now, snapshot, s.logger)
	}
	return errors
}

func (s *Service) commitPhase(ctx context.Context, now time.Time) int {
	errors := 0
	for _, target := range s.writes {
		if err := ctx.Err(); err != nil {
			return errors
		}
		errors += target.commit(now, s.clientFactory, s.logger)
	}
	return errors
}

// CellSnapshot returns a copy of the current cell values for tests.
func (s *Service) CellSnapshot() map[string]*snapshotValue {
	return s.cells.snapshot()
}

// Metrics returns the last recorded metrics snapshot.
func (s *Service) Metrics() metrics {
	return s.metrics
}

// Close releases all background resources held by the service.
func (s *Service) Close() error {
	if s == nil {
		return nil
	}
	for _, group := range s.reads {
		group.closeClient()
	}
	for _, target := range s.writes {
		target.closeClient()
	}
	if s.server != nil {
		s.server.close()
	}
	return nil
}

// ServerAddress returns the listen address of the embedded Modbus server, if enabled.
func (s *Service) ServerAddress() string {
	if s.server == nil {
		return ""
	}
	return s.server.addr()
}

// SetCellValue applies a manual override to the specified cell.
func (s *Service) SetCellValue(id string, value interface{}) error {
	if s == nil {
		return errors.New("service is nil")
	}
	cell, err := s.cells.mustGet(id)
	if err != nil {
		return err
	}
	now := time.Now()
	if err := cell.setValue(value, now, nil); err != nil {
		return err
	}
	if s.server != nil {
		s.server.refresh(s.cells.snapshot())
	}
	return nil
}

// InvalidateCell marks the specified cell invalid with an optional diagnostic code/message.
func (s *Service) InvalidateCell(id, code, message string) error {
	if s == nil {
		return errors.New("service is nil")
	}
	cell, err := s.cells.mustGet(id)
	if err != nil {
		return err
	}
	now := time.Now()
	cell.markInvalid(now, code, message)
	if s.server != nil {
		s.server.refresh(s.cells.snapshot())
	}
	return nil
}

// InspectCell returns the current state of the requested cell.
func (s *Service) InspectCell(id string) (CellState, error) {
	if s == nil {
		return CellState{}, errors.New("service is nil")
	}
	return s.cells.state(id)
}
