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

	cells   *cellStore
	reads   []*readGroup
	logic   []*logicBlock
	ordered []*logicBlock
	writes  []*writeTarget
	server  *modbusServer

	cycle   time.Duration
	metrics metrics
}

type metrics struct {
	CycleCount      uint64
	LastDuration    time.Duration
	LastReadErrors  int
	LastEvalErrors  int
	LastWriteErrors int
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
	snapshot := s.cells.snapshot()
	evalErrors := s.evalPhase(now, snapshot)
	writeErrors := s.commitPhase(ctx, now)
	s.server.refresh(snapshot)

	s.metrics.CycleCount++
	s.metrics.LastDuration = time.Since(start)
	s.metrics.LastReadErrors = readErrors
	s.metrics.LastEvalErrors = evalErrors
	s.metrics.LastWriteErrors = writeErrors
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
