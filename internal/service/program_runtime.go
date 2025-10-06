package service

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"

	"modbus_processor/internal/config"
	"modbus_processor/internal/service/programs"
)

type programInputBinding struct {
	id           string
	cellID       string
	kind         config.ValueKind
	optional     bool
	hasDefault   bool
	defaultValue interface{}
}

type programOutputBinding struct {
	id   string
	cell *cell
	kind config.ValueKind
}

type programBinding struct {
	cfg     config.ProgramConfig
	program programs.Program
	inputs  []programInputBinding
	outputs []programOutputBinding
}

func newProgramBindings(cfgs []config.ProgramConfig, cells *cellStore, logger zerolog.Logger) ([]*programBinding, error) {
	bindings := make([]*programBinding, 0, len(cfgs))
	seen := make(map[string]struct{})
	for _, cfg := range cfgs {
		if cfg.ID == "" {
			return nil, fmt.Errorf("program id must not be empty")
		}
		if cfg.Type == "" {
			return nil, fmt.Errorf("program %s missing type", cfg.ID)
		}
		if _, exists := seen[cfg.ID]; exists {
			return nil, fmt.Errorf("duplicate program id %s", cfg.ID)
		}
		seen[cfg.ID] = struct{}{}

		instance, err := programs.Instantiate(cfg.Type, cfg.ID, cfg.Settings)
		if err != nil {
			return nil, fmt.Errorf("program %s: %w", cfg.ID, err)
		}
		binding := &programBinding{cfg: cfg, program: instance}

		for _, inCfg := range cfg.Inputs {
			if inCfg.Cell == "" {
				return nil, fmt.Errorf("program %s input missing cell", cfg.ID)
			}
			alias := inCfg.ID
			if alias == "" {
				alias = inCfg.Cell
			}
			cell, err := cells.mustGet(inCfg.Cell)
			if err != nil {
				return nil, fmt.Errorf("program %s input %s: %w", cfg.ID, inCfg.Cell, err)
			}
			kind := cell.cfg.Type
			if inCfg.Type != "" {
				kind = inCfg.Type
			}
			entry := programInputBinding{
				id:       alias,
				cellID:   inCfg.Cell,
				kind:     kind,
				optional: inCfg.Optional,
			}
			if inCfg.Default != nil {
				converted, convErr := convertValue(kind, inCfg.Default)
				if convErr != nil {
					return nil, fmt.Errorf("program %s input %s default: %w", cfg.ID, alias, convErr)
				}
				entry.hasDefault = true
				entry.defaultValue = converted
			}
			binding.inputs = append(binding.inputs, entry)
		}

		for _, outCfg := range cfg.Outputs {
			if outCfg.Cell == "" {
				return nil, fmt.Errorf("program %s output missing cell", cfg.ID)
			}
			alias := outCfg.ID
			if alias == "" {
				alias = outCfg.Cell
			}
			cell, err := cells.mustGet(outCfg.Cell)
			if err != nil {
				return nil, fmt.Errorf("program %s output %s: %w", cfg.ID, outCfg.Cell, err)
			}
			kind := cell.cfg.Type
			if outCfg.Type != "" {
				kind = outCfg.Type
			}
			binding.outputs = append(binding.outputs, programOutputBinding{
				id:   alias,
				cell: cell,
				kind: kind,
			})
		}
		bindings = append(bindings, binding)
	}
	if len(bindings) > 0 {
		logger.Debug().Int("count", len(bindings)).Msg("programs registered")
	}
	return bindings, nil
}

func (b *programBinding) prepareInputs(snapshot map[string]*snapshotValue) []programs.Signal {
	if len(b.inputs) == 0 {
		return nil
	}
	signals := make([]programs.Signal, 0, len(b.inputs))
	for _, binding := range b.inputs {
		snap := snapshot[binding.cellID]
		sig := programs.Signal{ID: binding.id, Kind: binding.kind}
		if snap != nil && snap.Valid {
			sig.Valid = true
			sig.Value = snap.Value
		} else if binding.hasDefault {
			sig.Valid = true
			sig.Value = binding.defaultValue
		} else if binding.optional {
			sig.Valid = false
		}
		signals = append(signals, sig)
	}
	return signals
}

func (b *programBinding) applyOutputs(outputs []programs.Signal, now time.Time) int {
	if len(b.outputs) == 0 {
		return 0
	}
	errors := 0
	produced := programs.MapSignals(outputs)
	for _, binding := range b.outputs {
		sig, ok := produced[binding.id]
		if !ok || !sig.Valid {
			binding.cell.markInvalid(now, "program_output_invalid", fmt.Sprintf("program %s missing signal %s", b.cfg.ID, binding.id))
			errors++
			continue
		}
		if err := binding.cell.setValue(sig.Value, now, nil); err != nil {
			binding.cell.markInvalid(now, "program_output_error", fmt.Sprintf("program %s: %v", b.cfg.ID, err))
			errors++
			continue
		}
	}
	return errors
}

func (b *programBinding) invalidateOutputs(now time.Time, err error) {
	message := err.Error()
	for _, binding := range b.outputs {
		binding.cell.markInvalid(now, "program_error", fmt.Sprintf("program %s: %s", b.cfg.ID, message))
	}
}

func (s *Service) delta(now time.Time) time.Duration {
	if s.lastCycleTime.IsZero() {
		return s.cycleInterval()
	}
	delta := now.Sub(s.lastCycleTime)
	if delta <= 0 {
		return s.cycleInterval()
	}
	return delta
}

func (s *Service) programPhase(now time.Time, snapshot map[string]*snapshotValue) int {
	if len(s.programs) == 0 {
		return 0
	}
	ctx := programs.Context{Now: now, Delta: s.delta(now)}
	slots := s.workers.programSlot()
	errors, _ := runWorkersWithLoad(context.Background(), &s.load.program, slots, s.programs, func(_ context.Context, binding *programBinding) int {
		inputs := binding.prepareInputs(snapshot)
		outputs, err := binding.program.Execute(ctx, inputs)
		if err != nil {
			s.logger.Error().Str("program", binding.cfg.ID).Err(err).Msg("program execution failed")
			binding.invalidateOutputs(now, err)
			return 1
		}
		return binding.applyOutputs(outputs, now)
	})
	return errors
}

func (s *Service) registerPrograms(cfgs []config.ProgramConfig) error {
	programs, err := newProgramBindings(cfgs, s.cells, s.logger)
	if err != nil {
		return err
	}
	s.programs = programs
	return nil
}
