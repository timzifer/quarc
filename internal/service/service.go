package service

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
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
	load          loadSnapshot
	loadMu        sync.RWMutex
	lastCycleTime time.Time

	controller *cycleController
	cycle      atomic.Int64
	liveView   *liveViewServer
}

type metrics struct {
	CycleCount        uint64        `json:"cycle_count"`
	LastDuration      time.Duration `json:"last_duration"`
	LastDurationMS    float64       `json:"last_duration_ms"`
	LastReadErrors    int           `json:"last_read_errors"`
	LastProgramErrors int           `json:"last_program_errors"`
	LastEvalErrors    int           `json:"last_eval_errors"`
	LastWriteErrors   int           `json:"last_write_errors"`
}

type loadSnapshot struct {
	WorkerConfig workerSlotInfo `json:"worker_config"`
	Reads        phaseLoadInfo  `json:"reads"`
	Programs     phaseLoadInfo  `json:"programs"`
	Logic        logicLoadInfo  `json:"logic"`
	Writes       phaseLoadInfo  `json:"writes"`
	Goroutines   int            `json:"goroutines"`
}

type workerSlotInfo struct {
	Read    int `json:"read"`
	Program int `json:"program"`
	Execute int `json:"execute"`
	Write   int `json:"write"`
}

type phaseLoadInfo struct {
	Total    int `json:"total"`
	Pending  int `json:"pending"`
	Active   int `json:"active"`
	Disabled int `json:"disabled"`
}

type logicLoadInfo struct {
	Total           int     `json:"total"`
	Active          int     `json:"active"`
	Evaluated       int     `json:"evaluated"`
	Skipped         int     `json:"skipped"`
	AverageDuration float64 `json:"avg_duration_ms"`
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

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
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
	cycleLoad := loadSnapshot{
		WorkerConfig: workerSlotInfo{
			Read:    s.workers.readSlot(),
			Program: s.workers.programSlot(),
			Execute: s.workers.executeSlot(),
			Write:   s.workers.writeSlot(),
		},
		Reads:    phaseLoadInfo{Total: len(s.reads)},
		Programs: phaseLoadInfo{Total: len(s.programs)},
		Logic:    logicLoadInfo{Total: len(s.ordered)},
		Writes:   phaseLoadInfo{Total: len(s.writes)},
	}
	readErrors := s.readPhase(now, &cycleLoad)
	readSnapshot := s.cells.snapshot()
	programErrors := s.programPhase(now, readSnapshot, &cycleLoad)
	programSnapshot := s.cells.snapshot()
	evalErrors, logicStats := s.evalPhase(now, programSnapshot)
	executeSnapshot := s.cells.snapshot()
	cycleLoad.Logic.Evaluated = logicStats.Evaluated
	cycleLoad.Logic.Skipped = logicStats.Skipped
	if logicStats.Evaluated > 0 {
		cycleLoad.Logic.Active = minInt(logicStats.Evaluated, cycleLoad.WorkerConfig.Execute)
		cycleLoad.Logic.AverageDuration = float64(logicStats.TotalDuration) / float64(time.Millisecond) / float64(logicStats.Evaluated)
	} else {
		cycleLoad.Logic.Active = 0
		cycleLoad.Logic.AverageDuration = 0
	}
	writeErrors := s.commitPhase(ctx, now, &cycleLoad)
	if s.server != nil {
		s.server.refresh(executeSnapshot)
	}

	s.metrics.CycleCount++
	s.metrics.LastDuration = time.Since(start)
	s.metrics.LastDurationMS = float64(s.metrics.LastDuration) / float64(time.Millisecond)
	s.metrics.LastReadErrors = readErrors
	s.metrics.LastProgramErrors = programErrors
	s.metrics.LastEvalErrors = evalErrors
	s.metrics.LastWriteErrors = writeErrors
	s.lastCycleTime = now
	cycleLoad.Goroutines = runtime.NumGoroutine()
	s.setLoad(cycleLoad)
	return nil
}

func (s *Service) readPhase(now time.Time, load *loadSnapshot) int {
	due := make([]*readGroup, 0, len(s.reads))
	disabled := 0
	for _, group := range s.reads {
		if group.Disabled() {
			disabled++
			continue
		}
		if group.due(now) {
			due = append(due, group)
		}
	}
	slots := s.workers.readSlot()
	if load != nil {
		load.Reads.Total = len(s.reads)
		load.Reads.Disabled = disabled
		load.Reads.Pending = len(due)
		if slots > 0 {
			if len(due) < slots {
				load.Reads.Active = len(due)
			} else {
				load.Reads.Active = slots
			}
		}
	}
	if len(due) == 0 {
		return 0
	}
	errors, _ := runWorkerPool(context.Background(), slots, due, func(_ context.Context, group *readGroup) int {
		return group.perform(now, s.clientFactory, s.logger)
	})
	return errors
}

func (s *Service) evalPhase(now time.Time, snapshot map[string]*snapshotValue) (int, logicCycleStats) {
	return s.evaluateLogic(now, snapshot)
}

func (s *Service) commitPhase(ctx context.Context, now time.Time, load *loadSnapshot) int {
	enabled := make([]*writeTarget, 0, len(s.writes))
	disabled := 0
	for _, target := range s.writes {
		if target.Disabled() {
			disabled++
			continue
		}
		enabled = append(enabled, target)
	}
	slots := s.workers.writeSlot()
	if load != nil {
		load.Writes.Total = len(s.writes)
		load.Writes.Disabled = disabled
		load.Writes.Pending = len(enabled)
		if slots > 0 {
			if len(enabled) < slots {
				load.Writes.Active = len(enabled)
			} else {
				load.Writes.Active = slots
			}
		}
	}
	if len(enabled) == 0 {
		return 0
	}
	errors, _ := runWorkerPool(ctx, slots, enabled, func(c context.Context, target *writeTarget) int {
		if err := c.Err(); err != nil {
			return 0
		}
		return target.commit(now, s.clientFactory, s.logger)
	})
	return errors
}

func (s *Service) evaluateLogic(now time.Time, snapshot map[string]*snapshotValue) (int, logicCycleStats) {
	count := len(s.ordered)
	if count == 0 {
		return 0, logicCycleStats{}
	}
	slots := s.workers.executeSlot()
	if slots <= 1 {
		errors := 0
		stats := logicCycleStats{}
		for _, block := range s.ordered {
			result := block.evaluate(now, snapshot, nil, s.logger)
			errors += result.errors
			if result.skipped {
				stats.Skipped++
			}
			if result.executed {
				stats.Evaluated++
				stats.TotalDuration += result.duration
			}
		}
		return errors, stats
	}

	type logicResult struct {
		block  *logicBlock
		result logicEvaluationResult
	}

	tasks := make(chan *logicBlock)
	results := make(chan logicResult, slots)
	var wg sync.WaitGroup
	var snapshotMu sync.RWMutex

	worker := func() {
		defer wg.Done()
		for block := range tasks {
			eval := block.evaluate(now, snapshot, &snapshotMu, s.logger)
			results <- logicResult{block: block, result: eval}
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
	stats := logicCycleStats{}
	for processed < count {
		for active < slots && len(ready) > 0 {
			block := ready[0]
			ready = ready[1:]
			tasks <- block
			active++
		}
		result := <-results
		errors += result.result.errors
		if result.result.skipped {
			stats.Skipped++
		}
		if result.result.executed {
			stats.Evaluated++
			stats.TotalDuration += result.result.duration
		}
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

	return errors, stats
}

// CellSnapshot returns a copy of the current cell values for tests.
func (s *Service) CellSnapshot() map[string]*snapshotValue {
	return s.cells.snapshot()
}

// Metrics returns the last recorded metrics snapshot.
func (s *Service) Metrics() metrics {
	return s.metrics
}

// ReadStates returns a snapshot of all configured read groups.
func (s *Service) ReadStates() []readGroupState {
	out := make([]readGroupState, 0, len(s.reads))
	for _, group := range s.reads {
		out = append(out, group.state())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// WriteStates returns a snapshot of all configured write targets.
func (s *Service) WriteStates() []writeTargetState {
	out := make([]writeTargetState, 0, len(s.writes))
	for _, target := range s.writes {
		out = append(out, target.state())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// LogicStates returns runtime statistics for all logic blocks.
func (s *Service) LogicStates() []logicBlockState {
	out := make([]logicBlockState, 0, len(s.logic))
	for _, block := range s.logic {
		out = append(out, block.state())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// UpdateCell applies a manual override to a cell when the controller is paused.
func (s *Service) UpdateCell(id string, rawValue interface{}, quality *float64, valid *bool) (CellState, error) {
	if id == "" {
		return CellState{}, errors.New("cell id must not be empty")
	}
	if s.controller == nil {
		return CellState{}, errors.New("controller not initialized")
	}
	if status := s.controller.Status(); status.Mode != controlModePause {
		return CellState{}, errors.New("cells can only be edited while paused")
	}
	cell, err := s.cells.mustGet(id)
	if err != nil {
		return CellState{}, err
	}
	now := time.Now()
	if valid != nil && !*valid {
		cell.markInvalid(now, "manual.invalid", "manually invalidated")
		return cell.state(), nil
	}
	if rawValue == nil {
		if current, ok := cell.currentValue(); ok {
			rawValue = current
		} else {
			return CellState{}, errors.New("value required for valid cells")
		}
	}
	if err := cell.setValue(rawValue, now, quality); err != nil {
		return CellState{}, err
	}
	return cell.state(), nil
}

// SetReadDisabled updates the disabled state of a read group.
func (s *Service) SetReadDisabled(id string, disabled bool) error {
	for _, group := range s.reads {
		if group.cfg.ID == id {
			group.SetDisabled(disabled)
			return nil
		}
	}
	return fmt.Errorf("unknown read group %s", id)
}

// SetWriteDisabled updates the disabled state of a write target.
func (s *Service) SetWriteDisabled(id string, disabled bool) error {
	for _, target := range s.writes {
		if target.cfg.ID == id {
			target.SetDisabled(disabled)
			return nil
		}
	}
	return fmt.Errorf("unknown write target %s", id)
}

// Load returns the last recorded runtime load snapshot.
func (s *Service) Load() loadSnapshot {
	s.loadMu.RLock()
	defer s.loadMu.RUnlock()
	return s.load
}

func (s *Service) setLoad(load loadSnapshot) {
	s.loadMu.Lock()
	s.load = load
	s.loadMu.Unlock()
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
