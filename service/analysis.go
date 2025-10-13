package service

import (
	"fmt"
	"sort"
	"strings"

	"github.com/rs/zerolog"
	"github.com/timzifer/modbus_processor/config"
)

type DependencyReport struct {
	Cell               string
	Type               config.ValueKind
	InExpression       bool
	InValid            bool
	InQuality          bool
	ManuallyConfigured bool
	Resolved           bool
	Source             config.ModuleReference
}

type LogicBlockReport struct {
	ID           string
	Target       string
	TargetType   config.ValueKind
	Expression   string
	Valid        string
	Quality      string
	Dependencies []DependencyReport
	Errors       []string
	Source       config.ModuleReference
}

// CellWriterKind identifies the origin of a cell value.
type CellWriterKind string

const (
	// CellWriterRead indicates a value sourced from a Modbus or CAN read.
	CellWriterRead CellWriterKind = "read"
	// CellWriterProgram indicates a value produced by a configured program.
	CellWriterProgram CellWriterKind = "program"
	// CellWriterLogic indicates a value written by a logic block.
	CellWriterLogic CellWriterKind = "logic"
	// CellWriterConstant indicates a manually configured constant value.
	CellWriterConstant CellWriterKind = "constant"
)

// CellWriterEntry describes a single writer for a cell.
type CellWriterEntry struct {
	Kind      CellWriterKind
	Reference string
	Source    config.ModuleReference
}

// CellWriterReport aggregates the writers for a configured cell.
type CellWriterReport struct {
	Cell    string
	Type    config.ValueKind
	Source  config.ModuleReference
	Writers []CellWriterEntry
}

// LogicAnalysis captures the outcome of the logic inspection, including cell writer information.
type LogicAnalysis struct {
	Blocks         []LogicBlockReport
	CellWriters    []CellWriterReport
	UnwrittenCells []CellWriterReport
}

type cellWriterAccumulator struct {
	report *CellWriterReport
	seen   map[string]struct{}
}

func AnalyzeLogic(cfg *config.Config) (*LogicAnalysis, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config must not be nil")
	}

	dsl, err := newDSLEngine(cfg.DSL, cfg.Helpers, zerolog.Nop())
	if err != nil {
		return nil, err
	}
	cells, err := newCellStore(cfg.Cells)
	if err != nil {
		return nil, err
	}

	reports := make([]LogicBlockReport, 0, len(cfg.Logic))
	for idx, blockCfg := range cfg.Logic {
		report := LogicBlockReport{
			ID:         blockCfg.ID,
			Target:     blockCfg.Target,
			Expression: strings.TrimSpace(blockCfg.Expression),
			Valid:      strings.TrimSpace(blockCfg.Valid),
			Quality:    strings.TrimSpace(blockCfg.Quality),
			Source:     blockCfg.Source,
		}

		block, meta, buildErr := prepareLogicBlock(blockCfg, cells, dsl, idx, nil)
		if block != nil && block.target != nil {
			report.TargetType = block.target.cfg.Type
		}
		if len(meta) > 0 {
			report.Dependencies = buildDependencyReport(meta)
		}
		if buildErr != nil {
			report.Errors = append(report.Errors, buildErr.Error())
		}

		reports = append(reports, report)
	}

	cellWriters := collectCellWriterReports(cfg, reports)
	unwritten := make([]CellWriterReport, 0)
	for _, report := range cellWriters {
		if len(report.Writers) == 0 {
			unwritten = append(unwritten, report)
		}
	}

	return &LogicAnalysis{
		Blocks:         reports,
		CellWriters:    cellWriters,
		UnwrittenCells: unwritten,
	}, nil
}

func buildDependencyReport(meta map[string]*dependencyMeta) []DependencyReport {
	reports := make([]DependencyReport, 0, len(meta))
	for id, entry := range meta {
		if entry == nil {
			continue
		}
		dep := DependencyReport{
			Cell:               id,
			InExpression:       entry.expression,
			InValid:            entry.valid,
			InQuality:          entry.quality,
			ManuallyConfigured: entry.configured,
		}
		if entry.cell != nil {
			dep.Type = entry.cell.cfg.Type
			dep.Resolved = true
			dep.Source = entry.cell.cfg.Source
		}
		reports = append(reports, dep)
	}
	sort.Slice(reports, func(i, j int) bool { return reports[i].Cell < reports[j].Cell })
	return reports
}

func collectCellWriterReports(cfg *config.Config, blocks []LogicBlockReport) []CellWriterReport {
	if cfg == nil {
		return nil
	}

	acc := make(map[string]*cellWriterAccumulator, len(cfg.Cells))
	for _, cellCfg := range cfg.Cells {
		if cellCfg.ID == "" {
			continue
		}
		report := &CellWriterReport{
			Cell:   cellCfg.ID,
			Type:   cellCfg.Type,
			Source: cellCfg.Source,
		}
		entry := &cellWriterAccumulator{report: report, seen: make(map[string]struct{})}
		acc[cellCfg.ID] = entry

		if cellCfg.Constant != nil {
			addCellWriter(entry, CellWriterEntry{Kind: CellWriterConstant, Reference: cellCfg.ID, Source: cellCfg.Source})
		}
	}

	add := func(cellID string, writer CellWriterEntry) {
		entry, ok := acc[cellID]
		if !ok || entry == nil {
			return
		}
		addCellWriter(entry, writer)
	}

	for _, group := range cfg.Reads {
		if group.Disable {
			continue
		}
		writer := CellWriterEntry{Kind: CellWriterRead, Reference: group.ID, Source: group.Source}
		for _, signal := range group.Signals {
			if signal.Cell == "" {
				continue
			}
			add(signal.Cell, writer)
		}
		if group.CAN != nil {
			for _, frame := range group.CAN.Frames {
				for _, signal := range frame.Signals {
					if signal.Cell == "" {
						continue
					}
					add(signal.Cell, writer)
				}
			}
		}
	}

	for _, program := range cfg.Programs {
		writer := CellWriterEntry{Kind: CellWriterProgram, Reference: program.ID, Source: program.Source}
		for _, signal := range program.Outputs {
			if signal.Cell == "" {
				continue
			}
			add(signal.Cell, writer)
		}
	}

	for _, block := range blocks {
		if block.Target == "" {
			continue
		}
		writer := CellWriterEntry{Kind: CellWriterLogic, Reference: block.ID, Source: block.Source}
		add(block.Target, writer)
	}

	reports := make([]CellWriterReport, 0, len(acc))
	for _, entry := range acc {
		if entry == nil || entry.report == nil {
			continue
		}
		sort.Slice(entry.report.Writers, func(i, j int) bool {
			if entry.report.Writers[i].Kind == entry.report.Writers[j].Kind {
				return entry.report.Writers[i].Reference < entry.report.Writers[j].Reference
			}
			return entry.report.Writers[i].Kind < entry.report.Writers[j].Kind
		})
		reports = append(reports, *entry.report)
	}

	sort.Slice(reports, func(i, j int) bool { return reports[i].Cell < reports[j].Cell })
	return reports
}

func addCellWriter(acc *cellWriterAccumulator, writer CellWriterEntry) {
	if acc == nil || acc.report == nil {
		return
	}
	key := string(writer.Kind) + "|" + writer.Reference
	if _, exists := acc.seen[key]; exists {
		return
	}
	acc.seen[key] = struct{}{}
	acc.report.Writers = append(acc.report.Writers, writer)
}
