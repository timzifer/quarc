package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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
	workers  workerSlots

	metrics       metrics
	lastCycleTime time.Time

	controller *cycleController
	cycle      atomic.Int64
	liveView   *liveViewServer
}

type metrics struct {
	CycleCount        uint64
	LastDuration      time.Duration
	LastReadErrors    int
	LastProgramErrors int
	LastEvalErrors    int
	LastWriteErrors   int
}

type workerSlots struct {
	read    int
	program int
	execute int
	write   int
}

func newWorkerSlots(cfg config.WorkerSlots) workerSlots {
	slots := workerSlots{
		read:    sanitizeSlot(cfg.Read),
		program: sanitizeSlot(cfg.Program),
		execute: sanitizeSlot(cfg.Execute),
		write:   sanitizeSlot(cfg.Write),
	}
	return slots
}

func sanitizeSlot(value int) int {
	if value <= 0 {
		return 1
	}
	return value
}

func (w workerSlots) readSlot() int    { return w.read }
func (w workerSlots) programSlot() int { return w.program }
func (w workerSlots) executeSlot() int { return w.execute }
func (w workerSlots) writeSlot() int   { return w.write }

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
		workers:       newWorkerSlots(cfg.Workers),
	}
	svc.controller = newCycleController(cfg.CycleInterval())
	svc.cycle.Store(int64(cfg.CycleInterval()))
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
	for {
		now, err := s.controller.Wait(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
		if err := s.IterateOnce(ctx, now); err != nil {
			s.logger.Error().Err(err).Msg("iteration failure")
		}
	}
}

// IterateOnce performs a single deterministic cycle.
func (s *Service) IterateOnce(ctx context.Context, now time.Time) error {
	start := time.Now()
	readErrors := s.readPhase(now)
	readSnapshot := s.cells.snapshot()
	programErrors := s.programPhase(now, readSnapshot)
	programSnapshot := s.cells.snapshot()
	evalErrors := s.evalPhase(now, programSnapshot)
	executeSnapshot := s.cells.snapshot()
	writeErrors := s.commitPhase(ctx, now)
	if s.server != nil {
		s.server.refresh(executeSnapshot)
	}

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
	due := make([]*readGroup, 0, len(s.reads))
	for _, group := range s.reads {
		if group.due(now) {
			due = append(due, group)
		}
	}
	if len(due) == 0 {
		return 0
	}
	slots := s.workers.readSlot()
	errors, _ := runWorkerPool(context.Background(), slots, due, func(_ context.Context, group *readGroup) int {
		return group.perform(now, s.clientFactory, s.logger)
	})
	return errors
}

func (s *Service) evalPhase(now time.Time, snapshot map[string]*snapshotValue) int {
	return s.evaluateLogic(now, snapshot)
}

func (s *Service) commitPhase(ctx context.Context, now time.Time) int {
	if len(s.writes) == 0 {
		return 0
	}
	slots := s.workers.writeSlot()
	errors, _ := runWorkerPool(ctx, slots, s.writes, func(c context.Context, target *writeTarget) int {
		if err := c.Err(); err != nil {
			return 0
		}
		return target.commit(now, s.clientFactory, s.logger)
	})
	return errors
}

func (s *Service) evaluateLogic(now time.Time, snapshot map[string]*snapshotValue) int {
	count := len(s.ordered)
	if count == 0 {
		return 0
	}
	slots := s.workers.executeSlot()
	if slots <= 1 {
		errors := 0
		for _, block := range s.ordered {
			errors += block.evaluate(now, snapshot, nil, s.logger)
		}
		return errors
	}

	type logicResult struct {
		block  *logicBlock
		errors int
	}

	tasks := make(chan *logicBlock)
	results := make(chan logicResult, slots)
	var wg sync.WaitGroup
	var snapshotMu sync.RWMutex

	worker := func() {
		defer wg.Done()
		for block := range tasks {
			errs := block.evaluate(now, snapshot, &snapshotMu, s.logger)
			results <- logicResult{block: block, errors: errs}
		}
	}

	for i := 0; i < slots; i++ {
		wg.Add(1)
		go worker()
	}

	pending := make(map[*logicBlock]int, count)
	ready := make([]*logicBlock, 0, count)
	for _, block := range s.ordered {
		pending[block] = block.internalDependencies
		if block.internalDependencies == 0 {
			ready = append(ready, block)
		}
	}

	processed := 0
	active := 0
	errors := 0
	for processed < count {
		for active < slots && len(ready) > 0 {
			block := ready[0]
			ready = ready[1:]
			tasks <- block
			active++
		}
		result := <-results
		errors += result.errors
		processed++
		active--
		for _, dep := range result.block.dependents {
			pending[dep]--
			if pending[dep] == 0 {
				ready = append(ready, dep)
			}
		}
	}

	close(tasks)
	wg.Wait()
	close(results)

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

func (s *Service) cycleInterval() time.Duration {
	if s == nil {
		return 0
	}
	if ns := s.cycle.Load(); ns > 0 {
		return time.Duration(ns)
	}
	return 500 * time.Millisecond
}

func (s *Service) setCycleInterval(d time.Duration) {
	if d <= 0 {
		d = 500 * time.Millisecond
	}
	s.cycle.Store(int64(d))
	if s.controller != nil {
		s.controller.SetInterval(d)
	}
}

// EnableLiveView starts the optional live view HTTP server.
func (s *Service) EnableLiveView(listen string) error {
	if s == nil {
		return errors.New("service is nil")
	}
	if s.liveView != nil {
		return errors.New("live view already enabled")
	}
	if listen == "" {
		listen = ":18080"
	}
	if s.controller == nil {
		s.controller = newCycleController(s.cycleInterval())
	} else {
		s.controller.SetInterval(s.cycleInterval())
	}
	logger := s.logger.With().Str("component", "live_view").Logger()
	server, err := newLiveViewServer(listen, s, logger)
	if err != nil {
		return err
	}
	s.liveView = server
	return nil
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
	if s.liveView != nil {
		s.liveView.close()
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
