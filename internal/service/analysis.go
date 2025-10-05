package service

import (
	"fmt"
	"sort"
	"strings"

	"modbus_processor/internal/config"
)

type DependencyReport struct {
	Cell               string
	Type               config.ValueKind
	InNormal           bool
	InFallback         bool
	ManuallyConfigured bool
	Resolved           bool
}

type LogicBlockReport struct {
	ID                 string
	Target             string
	TargetType         config.ValueKind
	NormalExpression   string
	FallbackExpression string
	Dependencies       []DependencyReport
	Errors             []string
}

func AnalyzeLogic(cfg *config.Config) ([]LogicBlockReport, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config must not be nil")
	}

	dsl, err := newDSLEngine(cfg.DSL)
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
			ID:                 blockCfg.ID,
			Target:             blockCfg.Target,
			NormalExpression:   strings.TrimSpace(blockCfg.Normal),
			FallbackExpression: strings.TrimSpace(blockCfg.Fallback),
		}

		block, meta, buildErr := prepareLogicBlock(blockCfg, cells, dsl, idx)
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

	return reports, nil
}

func buildDependencyReport(meta map[string]*dependencyMeta) []DependencyReport {
	reports := make([]DependencyReport, 0, len(meta))
	for id, entry := range meta {
		if entry == nil {
			continue
		}
		dep := DependencyReport{
			Cell:               id,
			InNormal:           entry.normal,
			InFallback:         entry.fallback,
			ManuallyConfigured: entry.configured,
		}
		if entry.cell != nil {
			dep.Type = entry.cell.cfg.Type
			dep.Resolved = true
		}
		reports = append(reports, dep)
	}
	sort.Slice(reports, func(i, j int) bool { return reports[i].Cell < reports[j].Cell })
	return reports
}
