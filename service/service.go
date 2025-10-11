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
	"github.com/timzifer/modbus_processor/config"
	"github.com/timzifer/modbus_processor/service/canstream"
	modbus2 "github.com/timzifer/modbus_processor/service/modbus"

	"github.com/timzifer/modbus_processor/remote"
	serviceio "github.com/timzifer/modbus_processor/serviceio"
)

// Service implements the deterministic three-phase controller.
type Service struct {
	cfg    *config.Config
	logger zerolog.Logger

	cells     *cellStore
	snapshots *snapshotSwitch
	reads     []serviceio.ReadGroup
	logic     []*logicBlock
	ordered   []*logicBlock
	writes    []serviceio.Writer
	server    *modbusServer
	programs  []*programBinding
	workers   workerSlots

	metrics       metrics
	lastCycleTime time.Time

	controller *cycleController
	cycle      atomic.Int64
	liveView   *liveViewServer
	load       systemLoad
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

type workerLoad struct {
	kind       string
	configured int
	active     atomic.Int64
}

type systemLoad struct {
	read    workerLoad
	program workerLoad
	execute workerLoad
	write   workerLoad
}

type workerLoadSnapshot struct {
	Kind       string
	Configured int
	Active     int
}

type systemLoadSnapshot struct {
	Workers    []workerLoadSnapshot
	Goroutines int
}

type logicBlockState struct {
	ID      string
	Target  string
	Metrics logicMetricsSnapshot
	Source  config.ModuleReference
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

func newSystemLoad(slots workerSlots) systemLoad {
	load := systemLoad{
		read:    workerLoad{kind: "read", configured: slots.readSlot()},
		program: workerLoad{kind: "program", configured: slots.programSlot()},
		execute: workerLoad{kind: "logic", configured: slots.executeSlot()},
		write:   workerLoad{kind: "write", configured: slots.writeSlot()},
	}
	return load
}

func (w *workerLoad) snapshot() workerLoadSnapshot {
	if w == nil {
		return workerLoadSnapshot{}
	}
	return workerLoadSnapshot{
		Kind:       w.kind,
		Configured: w.configured,
		Active:     int(w.active.Load()),
	}
}

func (s systemLoad) snapshot() systemLoadSnapshot {
	workers := []workerLoadSnapshot{
		s.read.snapshot(),
		s.program.snapshot(),
		s.execute.snapshot(),
		s.write.snapshot(),
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].Kind < workers[j].Kind })
	return systemLoadSnapshot{
		Workers:    workers,
		Goroutines: runtime.NumGoroutine(),
	}
}

const (
	defaultDriver = "modbus"
	canDriver     = "can"
)

type Option func(*factoryRegistry)

type factoryRegistry struct {
	readers map[string]serviceio.ReaderFactory
	writers map[string]serviceio.WriterFactory
}

func newFactoryRegistry(factory remote.ClientFactory) factoryRegistry {
	if factory == nil {
		factory = remote.NewTCPClientFactory()
	}
	return factoryRegistry{
		readers: map[string]serviceio.ReaderFactory{
			defaultDriver: modbus2.NewReaderFactory(factory),
			canDriver:     canstream.NewReaderFactory(),
		},
		writers: map[string]serviceio.WriterFactory{
			defaultDriver: modbus2.NewWriterFactory(factory),
		},
	}
}

func applyOptions(reg factoryRegistry, opts []Option) factoryRegistry {
	for _, opt := range opts {
		if opt != nil {
			opt(&reg)
		}
	}
	return reg
}

// WithReaderFactory registers or overrides a reader factory for a driver identifier.
func WithReaderFactory(driver string, factory serviceio.ReaderFactory) Option {
	return func(reg *factoryRegistry) {
		if reg == nil {
			return
		}
		if reg.readers == nil {
			reg.readers = make(map[string]serviceio.ReaderFactory)
		}
		if driver == "" {
			return
		}
		if factory == nil {
			delete(reg.readers, driver)
			return
		}
		reg.readers[driver] = factory
	}
}

// WithWriterFactory registers or overrides a writer factory for a driver identifier.
func WithWriterFactory(driver string, factory serviceio.WriterFactory) Option {
	return func(reg *factoryRegistry) {
		if reg == nil {
			return
		}
		if reg.writers == nil {
			reg.writers = make(map[string]serviceio.WriterFactory)
		}
		if driver == "" {
			return
		}
		if factory == nil {
			delete(reg.writers, driver)
			return
		}
		reg.writers[driver] = factory
	}
}

// New builds a service from configuration and dependencies.
func New(cfg *config.Config, logger zerolog.Logger, factory remote.ClientFactory, opts ...Option) (*Service, error) {
	if cfg == nil {
		return nil, errors.New("config must not be nil")
	}
	registry := applyOptions(newFactoryRegistry(factory), opts)
	dsl, err := newDSLEngine(cfg.DSL, cfg.Helpers, logger)
	if err != nil {
		return nil, err
	}
	cells, err := newCellStore(cfg.Cells)
	if err != nil {
		return nil, err
	}
	reads, err := buildReadGroups(cfg.Reads, serviceio.ReaderDependencies{Cells: cells}, registry.readers)
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
	writes, err := buildWriteTargets(cfg.Writes, serviceio.WriterDependencies{Cells: cells}, registry.writers)
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

	slots := newWorkerSlots(cfg.Workers)
	svc := &Service{
		cfg:       cfg,
		logger:    logger,
		cells:     cells,
		snapshots: newSnapshotSwitch(len(cfg.Cells)),
		reads:     reads,
		logic:     logic,
		ordered:   ordered,
		writes:    writes,
		server:    srv,
		workers:   slots,
	}
	svc.load = newSystemLoad(slots)
	svc.controller = newCycleController(cfg.CycleInterval())
	svc.cycle.Store(int64(cfg.CycleInterval()))
	if len(programs) > 0 {
		svc.programs = programs
	}
	return svc, nil
}

func buildReadGroups(cfgs []config.ReadGroupConfig, deps serviceio.ReaderDependencies, factories map[string]serviceio.ReaderFactory) ([]serviceio.ReadGroup, error) {
	groups := make([]serviceio.ReadGroup, 0, len(cfgs))
	for _, cfg := range cfgs {
		driver := cfg.Endpoint.Driver
		if driver == "" {
			driver = defaultDriver
		}
		factory := factories[driver]
		if factory == nil {
			return nil, fmt.Errorf("read group %s: no reader factory registered for driver %s", cfg.ID, driver)
		}
		group, err := factory(cfg, deps)
		if err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}
	return groups, nil
}

func buildWriteTargets(cfgs []config.WriteTargetConfig, deps serviceio.WriterDependencies, factories map[string]serviceio.WriterFactory) ([]serviceio.Writer, error) {
	sorted := append([]config.WriteTargetConfig(nil), cfgs...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Priority == sorted[j].Priority {
			return sorted[i].ID < sorted[j].ID
		}
		return sorted[i].Priority > sorted[j].Priority
	})
	targets := make([]serviceio.Writer, 0, len(sorted))
	for _, cfg := range sorted {
		driver := cfg.Endpoint.Driver
		if driver == "" {
			driver = defaultDriver
		}
		factory := factories[driver]
		if factory == nil {
			return nil, fmt.Errorf("write target %s: no writer factory registered for driver %s", cfg.ID, driver)
		}
		target, err := factory(cfg, deps)
		if err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	return targets, nil
}

// Validate performs a dry-run validation of the configuration without starting background services.
func Validate(cfg *config.Config, logger zerolog.Logger, opts ...Option) error {
	if cfg == nil {
		return errors.New("config must not be nil")
	}

	registry := applyOptions(newFactoryRegistry(nil), opts)
	dsl, err := newDSLEngine(cfg.DSL, cfg.Helpers, logger)
	if err != nil {
		return err
	}
	cells, err := newCellStore(cfg.Cells)
	if err != nil {
		return err
	}
	if _, err := buildReadGroups(cfg.Reads, serviceio.ReaderDependencies{Cells: cells}, registry.readers); err != nil {
		return err
	}
	if _, _, err := newLogicBlocks(cfg.Logic, cells, dsl, logger); err != nil {
		return err
	}
	if _, err := newProgramBindings(cfg.Programs, cells, logger); err != nil {
		return err
	}
	if _, err := buildWriteTargets(cfg.Writes, serviceio.WriterDependencies{Cells: cells}, registry.writers); err != nil {
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
	var readSnapshot map[string]*snapshotValue
	if s.snapshots != nil {
		readSnapshot = s.snapshots.Capture(s.cells)
	} else {
		readSnapshot = s.cells.snapshot()
	}
	programErrors := s.programPhase(now, readSnapshot)
	var programSnapshot map[string]*snapshotValue
	if s.snapshots != nil {
		programSnapshot = s.snapshots.Capture(s.cells)
	} else {
		programSnapshot = s.cells.snapshot()
	}
	evalErrors := s.evalPhase(now, programSnapshot)
	var executeSnapshot map[string]*snapshotValue
	if s.snapshots != nil {
		executeSnapshot = s.snapshots.Capture(s.cells)
	} else {
		executeSnapshot = s.cells.snapshot()
	}
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
	due := make([]serviceio.ReadGroup, 0, len(s.reads))
	for _, group := range s.reads {
		if group.Due(now) {
			due = append(due, group)
		}
	}
	if len(due) == 0 {
		return 0
	}
	slots := s.workers.readSlot()
	errors, _ := runWorkersWithLoad(context.Background(), &s.load.read, slots, due, func(_ context.Context, group serviceio.ReadGroup) int {
		return group.Perform(now, s.logger)
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
	errors, _ := runWorkersWithLoad(ctx, &s.load.write, slots, s.writes, func(c context.Context, target serviceio.Writer) int {
		if err := c.Err(); err != nil {
			return 0
		}
		return target.Commit(now, s.logger)
	})
	return errors
}

func runWorkersWithLoad[T any](ctx context.Context, load *workerLoad, slots int, items []T, fn func(context.Context, T) int) (int, bool) {
	if load == nil {
		return runWorkerPool(ctx, slots, items, fn)
	}
	wrapped := func(ctx context.Context, item T) int {
		load.active.Add(1)
		defer load.active.Add(-1)
		return fn(ctx, item)
	}
	return runWorkerPool(ctx, slots, items, wrapped)
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
			s.load.execute.active.Add(1)
			errs := block.evaluate(now, snapshot, nil, s.logger)
			s.load.execute.active.Add(-1)
			errors += errs
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

	executeLoad := &s.load.execute
	worker := func() {
		defer wg.Done()
		for block := range tasks {
			executeLoad.active.Add(1)
			errs := block.evaluate(now, snapshot, &snapshotMu, s.logger)
			executeLoad.active.Add(-1)
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

// UpdateCell applies a manual override to a cell when the controller is paused.
func (s *Service) UpdateCell(id string, value interface{}, hasValue bool, quality *float64, qualitySet bool, valid *bool) (CellState, error) {
	if s == nil {
		return CellState{}, errors.New("service is nil")
	}
	if s.controller != nil {
		status := s.controller.Status()
		if status.Mode != controlModePause {
			return CellState{}, errors.New("controller must be paused to edit cells")
		}
	}
	update := manualCellUpdate{value: value, hasValue: hasValue, quality: quality, qualitySet: qualitySet, valid: valid}
	return s.cells.updateManual(id, time.Now(), update)
}

// SetReadGroupDisabled toggles a read group at runtime.
func (s *Service) SetReadGroupDisabled(id string, disabled bool) (serviceio.ReadGroupStatus, error) {
	for _, group := range s.reads {
		if group.ID() == id {
			group.SetDisabled(disabled)
			return group.Status(), nil
		}
	}
	return serviceio.ReadGroupStatus{}, fmt.Errorf("read group %s not found", id)
}

// SetWriteTargetDisabled toggles a write target at runtime.
func (s *Service) SetWriteTargetDisabled(id string, disabled bool) (serviceio.WriteTargetStatus, error) {
	for _, target := range s.writes {
		if target.ID() == id {
			target.SetDisabled(disabled)
			return target.Status(), nil
		}
	}
	return serviceio.WriteTargetStatus{}, fmt.Errorf("write target %s not found", id)
}

// ReadStatuses returns a snapshot of configured read groups.
func (s *Service) ReadStatuses() []serviceio.ReadGroupStatus {
	statuses := make([]serviceio.ReadGroupStatus, 0, len(s.reads))
	for _, group := range s.reads {
		statuses = append(statuses, group.Status())
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].ID < statuses[j].ID })
	return statuses
}

// WriteStatuses returns a snapshot of configured write targets.
func (s *Service) WriteStatuses() []serviceio.WriteTargetStatus {
	statuses := make([]serviceio.WriteTargetStatus, 0, len(s.writes))
	for _, target := range s.writes {
		statuses = append(statuses, target.Status())
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].ID < statuses[j].ID })
	return statuses
}

// LogicStates returns runtime metrics for configured logic blocks.
func (s *Service) LogicStates() []logicBlockState {
	states := make([]logicBlockState, 0, len(s.logic))
	for _, block := range s.logic {
		states = append(states, logicBlockState{ID: block.cfg.ID, Target: block.cfg.Target, Metrics: block.metricsSnapshot(), Source: block.cfg.Source})
	}
	sort.Slice(states, func(i, j int) bool { return states[i].ID < states[j].ID })
	return states
}

// SystemLoad returns a snapshot of current worker utilisation.
func (s *Service) SystemLoad() systemLoadSnapshot {
	if s == nil {
		return systemLoadSnapshot{}
	}
	return s.load.snapshot()
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
		group.Close()
	}
	for _, target := range s.writes {
		target.Close()
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
