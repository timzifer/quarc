package service

import (
	"testing"

	"github.com/timzifer/quarc/config"
)

func TestAnalyzeLogicCellWriters(t *testing.T) {
	cfg := &config.Config{
		Cells: []config.CellConfig{
			{ID: "read_cell", Type: config.ValueKindNumber},
			{ID: "constant_cell", Type: config.ValueKindBool, Constant: true},
			{ID: "logic_cell", Type: config.ValueKindString},
			{ID: "program_cell", Type: config.ValueKindInteger},
			{ID: "unwritten_cell", Type: config.ValueKindFloat},
		},
		Reads: []config.ReadGroupConfig{
			{
				ID: "rg-1",
				Signals: []config.ReadSignalConfig{
					{Cell: "read_cell", Type: config.ValueKindNumber},
				},
			},
		},
		Programs: []config.ProgramConfig{
			{
				ID: "prog-1",
				Outputs: []config.ProgramSignalConfig{
					{ID: "out", Cell: "program_cell"},
				},
			},
		},
		Logic: []config.LogicBlockConfig{
			{
				ID:     "logic-1",
				Target: "logic_cell",
			},
		},
	}

	analysis, err := AnalyzeLogic(cfg)
	if err != nil {
		t.Fatalf("AnalyzeLogic: %v", err)
	}
	if analysis == nil {
		t.Fatalf("AnalyzeLogic returned nil analysis")
	}

	if len(analysis.Blocks) != 1 {
		t.Fatalf("expected 1 logic block, got %d", len(analysis.Blocks))
	}

	if len(analysis.CellWriters) != len(cfg.Cells) {
		t.Fatalf("expected %d cell writer reports, got %d", len(cfg.Cells), len(analysis.CellWriters))
	}

	writers := make(map[string]CellWriterReport, len(analysis.CellWriters))
	for _, report := range analysis.CellWriters {
		writers[report.Cell] = report
	}

	if entry := writers["read_cell"]; len(entry.Writers) != 1 || entry.Writers[0].Kind != CellWriterRead {
		t.Fatalf("expected read_cell to have a single read writer, got %#v", entry.Writers)
	}

	if entry := writers["constant_cell"]; len(entry.Writers) != 1 || entry.Writers[0].Kind != CellWriterConstant {
		t.Fatalf("expected constant_cell to have a constant writer, got %#v", entry.Writers)
	}

	if entry := writers["logic_cell"]; len(entry.Writers) != 1 || entry.Writers[0].Kind != CellWriterLogic {
		t.Fatalf("expected logic_cell to have a logic writer, got %#v", entry.Writers)
	}

	if entry := writers["program_cell"]; len(entry.Writers) != 1 || entry.Writers[0].Kind != CellWriterProgram {
		t.Fatalf("expected program_cell to have a program writer, got %#v", entry.Writers)
	}

	unwritten := analysis.UnwrittenCells
	if len(unwritten) != 1 {
		t.Fatalf("expected 1 unwritten cell, got %d", len(unwritten))
	}
	if unwritten[0].Cell != "unwritten_cell" {
		t.Fatalf("unexpected unwritten cell: %#v", unwritten[0])
	}
}
