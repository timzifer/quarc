package service

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/rs/zerolog"

	"modbus_processor/internal/config"
)

type logicDependency struct {
	cell *cell
	kind config.ValueKind
}

type logicBlock struct {
	cfg      config.LogicBlockConfig
	target   *cell
	deps     []logicDependency
	normal   *vm.Program
	fallback *vm.Program
	order    int
}

type evaluationResult struct {
	success  bool
	fallback bool
	diag     *diagnosis
	value    interface{}
	err      error
}

type successSignal struct {
	value interface{}
}

func (successSignal) Error() string { return "success" }

type failSignal struct {
	code    string
	message string
}

func (failSignal) Error() string { return "fail" }

type fallbackSignal struct{}

func (fallbackSignal) Error() string { return "fallback" }

func newLogicBlocks(cfgs []config.LogicBlockConfig, cells *cellStore) ([]*logicBlock, []*logicBlock, error) {
	blocks := make([]*logicBlock, 0, len(cfgs))
	producers := make(map[string]*logicBlock)

	for idx, cfg := range cfgs {
		if cfg.ID == "" {
			return nil, nil, fmt.Errorf("logic block id must not be empty")
		}
		target, err := cells.mustGet(cfg.Target)
		if err != nil {
			return nil, nil, fmt.Errorf("logic block %s: %w", cfg.ID, err)
		}
		block := &logicBlock{cfg: cfg, target: target, order: idx}
		for _, depCfg := range cfg.Dependencies {
			depCell, err := cells.mustGet(depCfg.Cell)
			if err != nil {
				return nil, nil, fmt.Errorf("logic block %s: %w", cfg.ID, err)
			}
			if depCell.cfg.Type != depCfg.Type {
				return nil, nil, fmt.Errorf("logic block %s dependency %s expects %s but cell is %s", cfg.ID, depCfg.Cell, depCfg.Type, depCell.cfg.Type)
			}
			block.deps = append(block.deps, logicDependency{cell: depCell, kind: depCfg.Type})
		}
		if cfg.Normal != "" {
			program, err := compileExpression(cfg.Normal)
			if err != nil {
				return nil, nil, fmt.Errorf("logic block %s normal: %w", cfg.ID, err)
			}
			block.normal = program
		}
		if cfg.Fallback != "" {
			program, err := compileExpression(cfg.Fallback)
			if err != nil {
				return nil, nil, fmt.Errorf("logic block %s fallback: %w", cfg.ID, err)
			}
			block.fallback = program
		}
		if existing, ok := producers[cfg.Target]; ok {
			return nil, nil, fmt.Errorf("cells %s produced by multiple blocks (%s and %s)", cfg.Target, existing.cfg.ID, cfg.ID)
		}
		producers[cfg.Target] = block
		blocks = append(blocks, block)
	}

	ordered, err := topoSort(blocks, producers)
	if err != nil {
		return nil, nil, err
	}

	return blocks, ordered, nil
}

func compileExpression(exprStr string) (*vm.Program, error) {
	return expr.Compile(exprStr, expr.Env(map[string]interface{}{}), expr.AllowUndefinedVariables())
}

func topoSort(blocks []*logicBlock, producers map[string]*logicBlock) ([]*logicBlock, error) {
	inDegree := make(map[*logicBlock]int, len(blocks))
	edges := make(map[*logicBlock][]*logicBlock, len(blocks))

	for _, block := range blocks {
		for _, dep := range block.deps {
			prod := producers[dep.cell.cfg.ID]
			if prod == nil || prod == block {
				continue
			}
			edges[prod] = append(edges[prod], block)
			inDegree[block]++
		}
	}

	queue := make([]*logicBlock, 0, len(blocks))
	for _, block := range blocks {
		if inDegree[block] == 0 {
			queue = append(queue, block)
		}
	}
	sort.Slice(queue, func(i, j int) bool {
		return queue[i].order < queue[j].order
	})

	ordered := make([]*logicBlock, 0, len(blocks))
	for len(queue) > 0 {
		block := queue[0]
		queue = queue[1:]
		ordered = append(ordered, block)
		for _, succ := range edges[block] {
			inDegree[succ]--
			if inDegree[succ] == 0 {
				queue = append(queue, succ)
			}
		}
		sort.Slice(queue, func(i, j int) bool {
			return queue[i].order < queue[j].order
		})
	}

	if len(ordered) != len(blocks) {
		return nil, fmt.Errorf("logic graph contains a cycle")
	}
	return ordered, nil
}

func (b *logicBlock) evaluate(now time.Time, snapshot map[string]*snapshotValue, logger zerolog.Logger) int {
	errors := 0
	if b.target == nil {
		return 0
	}

	logger.Trace().Str("block", b.cfg.ID).Msg("logic block evaluation started")

	ready := b.dependenciesReady(snapshot)
	var result evaluationResult
	if ready && b.normal != nil {
		result = b.runProgram(now, snapshot, b.normal, false)
		if result.err != nil {
			logger.Error().Err(result.err).Str("block", b.cfg.ID).Msg("normal evaluation failed")
			errors++
		}
		if result.fallback {
			ready = false
		}
	} else {
		ready = false
	}

	if !ready {
		if b.fallback != nil {
			result = b.runProgram(now, snapshot, b.fallback, true)
			if result.err != nil {
				logger.Error().Err(result.err).Str("block", b.cfg.ID).Msg("fallback evaluation failed")
				errors++
			}
		} else {
			result = evaluationResult{success: false, diag: &diagnosis{Code: "logic.fallback_missing", Message: "no fallback available", Timestamp: now}}
			errors++
		}
	}

	if result.success {
		logger.Trace().Str("block", b.cfg.ID).Bool("success", true).Msg("logic block evaluation completed")
		if err := b.target.setValue(result.value, now); err != nil {
			logger.Error().Err(err).Str("block", b.cfg.ID).Msg("assign result")
			b.target.markInvalid(now, "logic.assign", err.Error())
			snapshot[b.target.cfg.ID] = b.target.asSnapshotValue()
			errors++
			return errors
		}
		snapshot[b.target.cfg.ID] = b.target.asSnapshotValue()
		return errors
	}

	if result.diag != nil {
		b.target.markInvalid(now, result.diag.Code, result.diag.Message)
	} else {
		b.target.markInvalid(now, "logic.invalid", "evaluation failed")
		errors++
	}
	snapshot[b.target.cfg.ID] = b.target.asSnapshotValue()
	logger.Trace().Str("block", b.cfg.ID).Bool("success", false).Msg("logic block evaluation completed")
	return errors
}

func (b *logicBlock) dependenciesReady(snapshot map[string]*snapshotValue) bool {
	for _, dep := range b.deps {
		snap := snapshot[dep.cell.cfg.ID]
		if snap == nil || !snap.Valid {
			return false
		}
		if snap.Kind != dep.kind {
			return false
		}
	}
	return true
}

func (b *logicBlock) runProgram(now time.Time, snapshot map[string]*snapshotValue, program *vm.Program, fallback bool) evaluationResult {
	env := make(map[string]interface{}, len(snapshot)+5)
	for id, value := range snapshot {
		if value != nil {
			env[id] = value.Value
		}
	}

	ctx := &dslContext{snapshot: snapshot}
	env["value"] = ctx.value
	env["success"] = ctx.success
	env["fail"] = ctx.fail
	env["fallback"] = ctx.fallback
	if fallback {
		env["valid"] = ctx.valid
	}

	result := evaluationResult{}

	_, err := vm.Run(program, env)
	if err != nil {
		var success successSignal
		if errors.As(err, &success) {
			result.success = true
			result.value = success.value
			return result
		}
		var fail failSignal
		if errors.As(err, &fail) {
			result.diag = &diagnosis{Code: fail.code, Message: fail.message, Timestamp: now}
			return result
		}
		var fb fallbackSignal
		if errors.As(err, &fb) {
			result.fallback = true
			return result
		}
		result.err = err
		return result
	}

	result.diag = &diagnosis{Code: "logic.no_success", Message: "expression completed without success()", Timestamp: now}
	return result
}

type dslContext struct {
	snapshot map[string]*snapshotValue
}

func (c *dslContext) value(id string) interface{} {
	snap := c.snapshot[id]
	if snap == nil || !snap.Valid {
		panic(fallbackSignal{})
	}
	return snap.Value
}

func (c *dslContext) success(v interface{}) interface{} {
	panic(successSignal{value: v})
}

func (c *dslContext) fail(args ...interface{}) interface{} {
	var code, message string
	if len(args) == 1 {
		if s, ok := args[0].(string); ok {
			message = s
		}
	}
	if len(args) >= 2 {
		if s, ok := args[0].(string); ok {
			code = s
		}
		if s, ok := args[1].(string); ok {
			message = s
		}
	}
	panic(failSignal{code: code, message: message})
}

func (c *dslContext) fallback() interface{} {
	panic(fallbackSignal{})
}

func (c *dslContext) valid(id string) bool {
	snap := c.snapshot[id]
	return snap != nil && snap.Valid
}
