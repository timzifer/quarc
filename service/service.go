package service

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/timzifer/quarc/config"
	"github.com/timzifer/quarc/runtime/connections"
	"github.com/timzifer/quarc/runtime/readers"
	"github.com/timzifer/quarc/runtime/state"
	"github.com/timzifer/quarc/runtime/writers"
	"github.com/timzifer/quarc/telemetry"
)

// Service implements the deterministic three-phase controller.
type Service struct {
	cfg    *config.Config
	logger zerolog.Logger

	cells        *cellStore
	snapshots    *snapshotSwitch
	reads        []readers.ReadGroup
	buffers      *readers.SignalBufferStore
	bufferCells  map[string]state.Cell
	bufferStatus map[string]readers.SignalBufferStatus
	bufferGroups map[string][]string
	bufferOwners map[string]string
	logic        []*logicBlock
	ordered      []*logicBlock
	writes       []writers.Writer
	server       *modbusServer
	programs     []*programBinding
	workers      workerSlots

	metrics       metrics
	lastCycleTime time.Time
	telemetry     telemetry.Collector

	connections *connectionManager

	controller *cycleController
	cycle      atomic.Int64
	cycleIndex atomic.Uint64
	liveView   *liveViewServer
	load       systemLoad
	heatmap    heatmapSettings
	activity   *activityTracker
}

const (
	diagCodeBufferOverflow   = "read.buffer_overflow"
	diagCodeAggregationError = "read.aggregation"
)

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

type Option func(*factoryRegistry)

type factoryRegistry struct {
	readers     map[string]readers.ReaderFactory
	writers     map[string]writers.WriterFactory
	connections map[string]connections.Factory
}

func newFactoryRegistry() factoryRegistry {
	return factoryRegistry{
		readers:     make(map[string]readers.ReaderFactory),
		writers:     make(map[string]writers.WriterFactory),
		connections: make(map[string]connections.Factory),
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
func WithReaderFactory(driver string, factory readers.ReaderFactory) Option {
	return func(reg *factoryRegistry) {
		if reg == nil {
			return
		}
		if reg.readers == nil {
			reg.readers = make(map[string]readers.ReaderFactory)
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
func WithWriterFactory(driver string, factory writers.WriterFactory) Option {
	return func(reg *factoryRegistry) {
		if reg == nil {
			return
		}
		if reg.writers == nil {
			reg.writers = make(map[string]writers.WriterFactory)
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

// WithConnectionFactory registers a reusable connection factory for a driver identifier.
func WithConnectionFactory(driver string, factory connections.Factory) Option {
	return func(reg *factoryRegistry) {
		if reg == nil {
			return
		}
		if reg.connections == nil {
			reg.connections = make(map[string]connections.Factory)
		}
		if driver == "" {
			return
		}
		if factory == nil {
			delete(reg.connections, driver)
			return
		}
		reg.connections[driver] = factory
	}
}

// New builds a service from configuration and dependencies.
func New(cfg *config.Config, logger zerolog.Logger, opts ...Option) (*Service, error) {
	if cfg == nil {
		return nil, errors.New("config must not be nil")
	}
	registry := applyOptions(newFactoryRegistry(), opts)
	dsl, err := newDSLEngine(cfg.DSL, cfg.Helpers, logger)
	if err != nil {
		return nil, err
	}
	cells, err := newCellStore(cfg.Cells)
	if err != nil {
		return nil, err
	}
	slots := newWorkerSlots(cfg.Workers)
	svc := &Service{
		cfg:          cfg,
		logger:       logger,
		cells:        cells,
		snapshots:    newSnapshotSwitch(len(cfg.Cells)),
		workers:      slots,
		telemetry:    telemetry.Noop(),
		bufferStatus: make(map[string]readers.SignalBufferStatus),
		bufferGroups: make(map[string][]string),
		bufferOwners: make(map[string]string),
	}
	svc.load = newSystemLoad(slots)
	svc.controller = newCycleController(cfg.CycleInterval())
	svc.cycle.Store(int64(cfg.CycleInterval()))
	svc.heatmap = newHeatmapSettings(cfg.LiveView.Heatmap)
	svc.activity = newActivityTracker(&svc.cycleIndex, svc.heatmap)

	connManager, err := newConnectionManager(cfg.Connections, registry.connections)
	if err != nil {
		return nil, err
	}
	svc.connections = connManager
	cleanupOnErr := func(err error) (*Service, error) {
		if svc.connections != nil {
			_ = svc.connections.Close()
		}
		return nil, err
	}

	bufferStore := readers.NewSignalBufferStore()
	deps := readers.ReaderDependencies{Cells: cells, Activity: svc.activity, Buffers: bufferStore, Connections: connManager}
	reads, bufferCells, bufferGroups, err := buildReadGroups(cfg.Reads, deps, registry.readers)
	if err != nil {
		return cleanupOnErr(err)
	}
	svc.reads = reads
	svc.buffers = bufferStore
	svc.bufferCells = bufferCells
	if len(bufferGroups) > 0 {
		svc.bufferGroups = bufferGroups
		if svc.bufferStatus == nil {
			svc.bufferStatus = make(map[string]readers.SignalBufferStatus, len(bufferCells))
		}
		if svc.bufferOwners == nil {
			svc.bufferOwners = make(map[string]string, len(bufferCells))
		}
		for groupID, cells := range bufferGroups {
			for _, cellID := range cells {
				if cellID == "" {
					continue
				}
				svc.bufferOwners[cellID] = groupID
			}
		}
	}

	logic, ordered, err := newLogicBlocks(cfg.Logic, cells, dsl, logger, svc.activity)
	if err != nil {
		return cleanupOnErr(err)
	}
	svc.logic = logic
	svc.ordered = ordered

	programs, err := newProgramBindings(cfg.Programs, cells, logger, svc.activity)
	if err != nil {
		return cleanupOnErr(err)
	}
	svc.programs = programs

	writes, err := buildWriteTargets(cfg.Writes, writers.WriterDependencies{Cells: cells, Connections: connManager}, registry.writers)
	if err != nil {
		return cleanupOnErr(err)
	}
	svc.writes = writes
	if cfg.Server.Enabled {
		logger.Warn().Str("component", "modbus_server").Msg("embedded Modbus server configuration detected but ignored")
	}
	svc.server = nil
	return svc, nil
}

func buildReadGroups(cfgs []config.ReadGroupConfig, deps readers.ReaderDependencies, factories map[string]readers.ReaderFactory) ([]readers.ReadGroup, map[string]state.Cell, map[string][]string, error) {
	groups := make([]readers.ReadGroup, 0, len(cfgs))
	bufferCells := make(map[string]state.Cell)
	bufferGroups := make(map[string][]string)
	for _, cfg := range cfgs {
		addCell := func(list []string, id string) []string {
			for _, existing := range list {
				if existing == id {
					return list
				}
			}
			return append(list, id)
		}
		for _, signal := range cfg.Signals {
			if signal.Cell == "" {
				return nil, nil, nil, fmt.Errorf("read group %s: signal missing cell", cfg.ID)
			}
			if deps.Cells == nil {
				return nil, nil, nil, errors.New("read group dependencies missing cells store")
			}
			cell, err := deps.Cells.Get(signal.Cell)
			if err != nil {
				return nil, nil, nil, err
			}
			bufferCells[signal.Cell] = cell
			bufferGroups[cfg.ID] = addCell(bufferGroups[cfg.ID], signal.Cell)
			if deps.Buffers != nil {
				size := signal.BufferSize
				if signal.Buffer != nil && signal.Buffer.Capacity != nil {
					size = *signal.Buffer.Capacity
				}
				if size <= 0 {
					size = 1
				}
				aggCfgs := make([]readers.SignalBufferAggregationConfig, 0, len(signal.Aggregations))
				for _, agg := range signal.Aggregations {
					strategy, err := readers.ParseAggregationStrategy(agg.Aggregator)
					if err != nil {
						return nil, nil, nil, fmt.Errorf("read group %s signal %s aggregation %s: %w", cfg.ID, signal.Cell, agg.Cell, err)
					}
					aggCfgs = append(aggCfgs, readers.SignalBufferAggregationConfig{
						Cell:        agg.Cell,
						Strategy:    strategy,
						QualityCell: agg.Quality,
						OnOverflow:  agg.OnOverflow,
					})
					aggCell, err := deps.Cells.Get(agg.Cell)
					if err != nil {
						return nil, nil, nil, err
					}
					bufferCells[agg.Cell] = aggCell
					if quality := agg.Quality; quality != "" {
						qualityCell, err := deps.Cells.Get(quality)
						if err != nil {
							return nil, nil, nil, err
						}
						bufferCells[quality] = qualityCell
					}
				}
				if len(aggCfgs) == 0 {
					aggCfgs = append(aggCfgs, readers.SignalBufferAggregationConfig{Cell: signal.Cell})
				}
				if _, err := deps.Buffers.Configure(signal.Cell, size, aggCfgs); err != nil {
					return nil, nil, nil, fmt.Errorf("read group %s signal %s: %w", cfg.ID, signal.Cell, err)
				}
			}
		}

		driver := cfg.Endpoint.Driver
		if driver == "" {
			return nil, nil, nil, fmt.Errorf("read group %s: endpoint missing driver", cfg.ID)
		}
		if cfg.Connection != "" && deps.Connections == nil {
			return nil, nil, nil, fmt.Errorf("read group %s: connections manager not initialised", cfg.ID)
		}
		factory := factories[driver]
		if factory == nil {
			return nil, nil, nil, fmt.Errorf("read group %s: no reader factory registered for driver %s", cfg.ID, driver)
		}
		group, err := factory(cfg, deps)
		if err != nil {
			return nil, nil, nil, err
		}
		groups = append(groups, group)
	}
	return groups, bufferCells, bufferGroups, nil
}

func buildWriteTargets(cfgs []config.WriteTargetConfig, deps writers.WriterDependencies, factories map[string]writers.WriterFactory) ([]writers.Writer, error) {
	sorted := append([]config.WriteTargetConfig(nil), cfgs...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Priority == sorted[j].Priority {
			return sorted[i].ID < sorted[j].ID
		}
		return sorted[i].Priority > sorted[j].Priority
	})
	targets := make([]writers.Writer, 0, len(sorted))
	for _, cfg := range sorted {
		driver := cfg.Endpoint.Driver
		if driver == "" {
			return nil, fmt.Errorf("write target %s: endpoint missing driver", cfg.ID)
		}
		if cfg.Connection != "" && deps.Connections == nil {
			return nil, fmt.Errorf("write target %s: connections manager not initialised", cfg.ID)
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

	registry := applyOptions(newFactoryRegistry(), opts)
	dsl, err := newDSLEngine(cfg.DSL, cfg.Helpers, logger)
	if err != nil {
		return err
	}
	cells, err := newCellStore(cfg.Cells)
	if err != nil {
		return err
	}
	connManager, err := newConnectionManager(cfg.Connections, registry.connections)
	if err != nil {
		return err
	}
	defer connManager.Close()
	deps := readers.ReaderDependencies{Cells: cells, Buffers: readers.NewSignalBufferStore(), Connections: connManager}
	if _, _, _, err := buildReadGroups(cfg.Reads, deps, registry.readers); err != nil {
		return err
	}
	if _, _, err := newLogicBlocks(cfg.Logic, cells, dsl, logger, nil); err != nil {
		return err
	}
	if _, err := newProgramBindings(cfg.Programs, cells, logger, nil); err != nil {
		return err
	}
	if _, err := buildWriteTargets(cfg.Writes, writers.WriterDependencies{Cells: cells, Connections: connManager}, registry.writers); err != nil {
		return err
	}
	if cfg.Server.Enabled {
		logger.Warn().Str("component", "modbus_server").Msg("embedded Modbus server configuration detected but ignored")
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
	s.cycleIndex.Add(1)
	readErrors := s.readPhase(ctx, now)
	readErrors += s.flushBufferPhase(now)
	var readSnapshot map[string]*snapshotValue
	if s.snapshots != nil {
		readSnapshot = s.snapshots.Capture(s.cells)
	} else {
		readSnapshot = s.cells.snapshot()
	}
	programErrors := s.programPhase(ctx, now, readSnapshot)
	var programSnapshot map[string]*snapshotValue
	if s.snapshots != nil {
		programSnapshot = s.snapshots.Capture(s.cells)
	} else {
		programSnapshot = s.cells.snapshot()
	}
	evalErrors := s.evalPhase(now, programSnapshot)
	if s.snapshots != nil {
		s.snapshots.Capture(s.cells)
	} else {
		s.cells.snapshot()
	}
	writeErrors := s.commitPhase(ctx, now)

	s.metrics.CycleCount++
	s.metrics.LastDuration = time.Since(start)
	s.metrics.LastReadErrors = readErrors
	s.metrics.LastProgramErrors = programErrors
	s.metrics.LastEvalErrors = evalErrors
	s.metrics.LastWriteErrors = writeErrors
	s.lastCycleTime = now
	return nil
}

func (s *Service) readPhase(ctx context.Context, now time.Time) int {
	due := make([]readers.ReadGroup, 0, len(s.reads))
	for _, group := range s.reads {
		if group.Due(now) {
			due = append(due, group)
		}
	}
	if len(due) == 0 {
		return 0
	}
	slots := s.workers.readSlot()
	errors, _ := runWorkersWithLoad(ctx, &s.load.read, slots, due, func(_ context.Context, group readers.ReadGroup) int {
		return group.Perform(now, s.logger)
	})
	return errors
}

func (s *Service) flushBufferPhase(now time.Time) int {
	if s == nil || s.buffers == nil {
		return 0
	}
	errors := 0
	for _, buffer := range s.buffers.All() {
		if buffer == nil {
			continue
		}
		res, err := buffer.Flush()
		bufferStatus := buffer.Status()
		s.recordBufferStatus(buffer.ID(), bufferStatus)
		if err != nil {
			errors++
			continue
		}
		for cellID, agg := range res.Aggregations {
			cell := s.lookupBufferCell(cellID)
			if cell == nil {
				if agg.Err != nil {
					errors++
				}
				continue
			}
			ts := agg.Aggregate.Timestamp
			if ts.IsZero() {
				ts = now
			}
			if !agg.HasValue {
				if res.Overflow || agg.Aggregate.Overflow {
					if !strings.EqualFold(agg.OnOverflow, "ignore") {
						cell.MarkInvalid(ts, diagCodeBufferOverflow, fmt.Sprintf("signal buffer overflow for cell %s", cellID))
						errors++
					}
				}
				continue
			}
			if agg.Err != nil {
				cell.MarkInvalid(ts, diagCodeAggregationError, agg.Err.Error())
				errors++
				continue
			}
			overflow := res.Overflow || agg.Aggregate.Overflow
			if overflow && !strings.EqualFold(agg.OnOverflow, "ignore") {
				cell.MarkInvalid(ts, diagCodeBufferOverflow, fmt.Sprintf("signal buffer overflow for cell %s", cellID))
				errors++
				continue
			}
			if err := cell.SetValue(agg.Aggregate.Value, ts, agg.Aggregate.Quality); err != nil {
				cell.MarkInvalid(ts, diagCodeAggregationError, err.Error())
				errors++
				continue
			}
			if qualityCellID := agg.QualityCell; qualityCellID != "" && agg.Aggregate.Quality != nil {
				qualityCell := s.lookupBufferCell(qualityCellID)
				if qualityCell != nil {
					if err := qualityCell.SetValue(*agg.Aggregate.Quality, ts, nil); err != nil {
						qualityCell.MarkInvalid(ts, diagCodeAggregationError, err.Error())
					}
				}
			}
			if s.activity != nil {
				s.activity.RecordCellRead(cellID, "read", ts)
			}
		}
	}
	return errors
}

func (s *Service) lookupBufferCell(id string) state.Cell {
	if id == "" || s == nil {
		return nil
	}
	if s.bufferCells != nil {
		if cell, ok := s.bufferCells[id]; ok && cell != nil {
			return cell
		}
	}
	if s.cells == nil {
		return nil
	}
	cell, err := s.cells.mustGet(id)
	if err != nil {
		return nil
	}
	if s.bufferCells == nil {
		s.bufferCells = make(map[string]state.Cell)
	}
	s.bufferCells[id] = cell
	return cell
}

func (s *Service) evalPhase(now time.Time, snapshot map[string]*snapshotValue) int {
	return s.evaluateLogic(now, snapshot)
}

func (s *Service) commitPhase(ctx context.Context, now time.Time) int {
	if len(s.writes) == 0 {
		return 0
	}
	slots := s.workers.writeSlot()
	errors, _ := runWorkersWithLoad(ctx, &s.load.write, slots, s.writes, func(c context.Context, target writers.Writer) int {
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

// SetTelemetry configures the collector used for runtime metrics emission.
func (s *Service) SetTelemetry(collector telemetry.Collector) {
	if s == nil {
		return
	}
	if collector == nil {
		collector = telemetry.Noop()
	}
	s.telemetry = collector
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
	ts := time.Now()
	state, err := s.cells.updateManual(id, ts, update)
	if err != nil {
		return CellState{}, err
	}
	if s.activity != nil {
		s.activity.RecordCellWrite(id, "manual", "manual", ts)
	}
	return state, nil
}

func (s *Service) snapshotReadStatus(base readers.ReadGroupStatus) readers.ReadGroupStatus {
	if s == nil {
		return base
	}
	cells := s.bufferGroups[base.ID]
	if len(cells) == 0 {
		return base
	}
	buffers := make(map[string]readers.SignalBufferStatus, len(cells))
	for _, cellID := range cells {
		status, ok := s.bufferStatus[cellID]
		if !ok {
			status = readers.SignalBufferStatus{}
		}
		buffers[cellID] = status
	}
	base.Buffers = buffers
	return base
}

func (s *Service) recordBufferStatus(bufferID string, status readers.SignalBufferStatus) {
	if s == nil || bufferID == "" {
		return
	}
	if s.bufferStatus == nil {
		s.bufferStatus = make(map[string]readers.SignalBufferStatus)
	}
	prev := s.bufferStatus[bufferID]
	s.bufferStatus[bufferID] = status
	groupID := s.bufferOwners[bufferID]
	if s.telemetry != nil {
		if delta := status.Dropped - prev.Dropped; delta > 0 {
			s.telemetry.IncBufferDropped(groupID, bufferID, delta)
		}
		s.telemetry.SetBufferOccupancy(groupID, bufferID, status.Buffered)
	}
}

// SetReadGroupDisabled toggles a read group at runtime.
func (s *Service) SetReadGroupDisabled(id string, disabled bool) (readers.ReadGroupStatus, error) {
	for _, group := range s.reads {
		if group.ID() == id {
			group.SetDisabled(disabled)
			return s.snapshotReadStatus(group.Status()), nil
		}
	}
	return readers.ReadGroupStatus{}, fmt.Errorf("read group %s not found", id)
}

// SetWriteTargetDisabled toggles a write target at runtime.
func (s *Service) SetWriteTargetDisabled(id string, disabled bool) (writers.WriteTargetStatus, error) {
	for _, target := range s.writes {
		if target.ID() == id {
			target.SetDisabled(disabled)
			return target.Status(), nil
		}
	}
	return writers.WriteTargetStatus{}, fmt.Errorf("write target %s not found", id)
}

// ReadStatuses returns a snapshot of configured read groups.
func (s *Service) ReadStatuses() []readers.ReadGroupStatus {
	statuses := make([]readers.ReadGroupStatus, 0, len(s.reads))
	for _, group := range s.reads {
		statuses = append(statuses, s.snapshotReadStatus(group.Status()))
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].ID < statuses[j].ID })
	return statuses
}

// WriteStatuses returns a snapshot of configured write targets.
func (s *Service) WriteStatuses() []writers.WriteTargetStatus {
	statuses := make([]writers.WriteTargetStatus, 0, len(s.writes))
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

// ProgramStates returns a snapshot of configured programs and their bindings.
func (s *Service) ProgramStates() []programState {
	if len(s.programs) == 0 {
		return nil
	}
	states := make([]programState, 0, len(s.programs))
	for _, binding := range s.programs {
		outputs := make([]string, 0, len(binding.outputs))
		for _, out := range binding.outputs {
			if out.cell != nil {
				outputs = append(outputs, out.cell.cfg.ID)
			}
		}
		states = append(states, programState{
			ID:      binding.cfg.ID,
			Type:    binding.cfg.Type,
			Outputs: outputs,
			Source:  binding.cfg.Source,
		})
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
	if s.connections != nil {
		_ = s.connections.Close()
	}
	if s.server != nil {
		// Legacy embedded Modbus server support has been removed, but we
		// keep the shutdown call to guard against third-party extensions
		// that might still register a server implementation.
		s.server.close()
	}
	if s.liveView != nil {
		s.liveView.close()
	}
	return nil
}

// ServerAddress returns the listen address of the embedded Modbus server, if enabled.
//
// Deprecated: The embedded Modbus server has been removed. The method now
// returns an empty string and will disappear in a future release.
func (s *Service) ServerAddress() string {
	if s == nil {
		return ""
	}
	return ""
}

// SetCellValue applies a manual override to the specified cell. Overrides are
// visible to logic immediately and exported once downstream drivers publish
// their next snapshot.
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
	// Legacy Modbus server refreshes are intentionally omitted – overrides are
	// picked up by drivers on their next publish cycle.
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
	// Legacy Modbus server refreshes are intentionally omitted – diagnostics
	// propagate via snapshots that drivers publish on their own cadence.
	return nil
}

// InspectCell returns the current state of the requested cell.
func (s *Service) InspectCell(id string) (CellState, error) {
	if s == nil {
		return CellState{}, errors.New("service is nil")
	}
	return s.cells.state(id)
}
