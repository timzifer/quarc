package service

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/vm"
	"github.com/rs/zerolog"

	"modbus_processor/internal/config"
)

type logicDependency struct {
	cell *cell
	kind config.ValueKind
}

type logicBlock struct {
	cfg                   config.LogicBlockConfig
	target                *cell
	deps                  []logicDependency
	normal                *vm.Program
	fallback              *vm.Program
	order                 int
	normalDependencyIDs   []string
	fallbackDependencyIDs []string
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

func newLogicBlocks(cfgs []config.LogicBlockConfig, cells *cellStore, logger zerolog.Logger) ([]*logicBlock, []*logicBlock, error) {
	blocks := make([]*logicBlock, 0, len(cfgs))
	producers := make(map[string]*logicBlock)
	logicLogger := logger.With().Str("component", "logic").Logger()

	for idx, cfg := range cfgs {
		block, _, err := prepareLogicBlock(cfg, cells, idx)
		if err != nil {
			return nil, nil, err
		}

		if block.normal != nil {
			logicLogger.Debug().
				Str("block", cfg.ID).
				Str("expression", strings.TrimSpace(cfg.Normal)).
				Strs("dependencies", block.normalDependencyIDs).
				Msg("logic block normal expression")
		}
		if block.fallback != nil {
			logicLogger.Debug().
				Str("block", cfg.ID).
				Str("expression", strings.TrimSpace(cfg.Fallback)).
				Strs("dependencies", block.fallbackDependencyIDs).
				Msg("logic block fallback expression")
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

type dependencyMeta struct {
	normal     bool
	fallback   bool
	configured bool
	cell       *cell
}

func prepareLogicBlock(cfg config.LogicBlockConfig, cells *cellStore, order int) (*logicBlock, map[string]*dependencyMeta, error) {
	block := &logicBlock{cfg: cfg, order: order}
	meta := make(map[string]*dependencyMeta)

	ensure := func(id string) *dependencyMeta {
		if id == "" {
			return nil
		}
		entry, ok := meta[id]
		if !ok {
			entry = &dependencyMeta{}
			meta[id] = entry
		}
		return entry
	}

	if cfg.ID == "" {
		return block, meta, fmt.Errorf("logic block id must not be empty")
	}

	target, err := cells.mustGet(cfg.Target)
	if err != nil {
		return block, meta, fmt.Errorf("logic block %s: %w", cfg.ID, err)
	}
	block.target = target

	normalIDs := make(map[string]struct{})
	fallbackIDs := make(map[string]struct{})

	for _, depCfg := range cfg.Dependencies {
		entry := ensure(depCfg.Cell)
		if entry == nil {
			continue
		}
		entry.normal = true
		entry.configured = true
		depCell, depErr := cells.mustGet(depCfg.Cell)
		if depErr != nil {
			return block, meta, fmt.Errorf("logic block %s dependency %s: %w", cfg.ID, depCfg.Cell, depErr)
		}
		if depCfg.Type != "" && depCell.cfg.Type != depCfg.Type {
			return block, meta, fmt.Errorf("logic block %s dependency %s expects %s but cell is %s", cfg.ID, depCfg.Cell, depCfg.Type, depCell.cfg.Type)
		}
		entry.cell = depCell
		normalIDs[depCfg.Cell] = struct{}{}
	}

	if cfg.Normal != "" {
		program, err := compileExpression(cfg.Normal)
		if err != nil {
			return block, meta, fmt.Errorf("logic block %s normal: %w", cfg.ID, err)
		}
		block.normal = program
		for _, dep := range uniqueDependencies(program, false) {
			entry := ensure(dep)
			if entry == nil {
				continue
			}
			entry.normal = true
			normalIDs[dep] = struct{}{}
		}
	}

	if cfg.Fallback != "" {
		program, err := compileExpression(cfg.Fallback)
		if err != nil {
			return block, meta, fmt.Errorf("logic block %s fallback: %w", cfg.ID, err)
		}
		block.fallback = program
		for _, dep := range uniqueDependencies(program, true) {
			entry := ensure(dep)
			if entry == nil {
				continue
			}
			entry.fallback = true
			fallbackIDs[dep] = struct{}{}
		}
	}

	for id, entry := range meta {
		if entry.cell != nil {
			continue
		}
		depCell, depErr := cells.mustGet(id)
		if depErr != nil {
			switch {
			case entry.normal:
				return block, meta, fmt.Errorf("logic block %s dependency %s: %w", cfg.ID, id, depErr)
			case entry.fallback:
				return block, meta, fmt.Errorf("logic block %s fallback dependency %s: %w", cfg.ID, id, depErr)
			default:
				return block, meta, fmt.Errorf("logic block %s dependency %s: %w", cfg.ID, id, depErr)
			}
		}
		entry.cell = depCell
	}

	block.normalDependencyIDs = sortKeys(normalIDs)
	block.fallbackDependencyIDs = sortKeys(fallbackIDs)

	deps := make([]logicDependency, 0, len(block.normalDependencyIDs))
	for _, id := range block.normalDependencyIDs {
		entry := meta[id]
		if entry == nil || entry.cell == nil {
			continue
		}
		deps = append(deps, logicDependency{cell: entry.cell, kind: entry.cell.cfg.Type})
	}
	block.deps = deps

	return block, meta, nil
}

func sortKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

var reservedIdentifiers = map[string]struct{}{
	"success":  {},
	"fail":     {},
	"fallback": {},
	"value":    {},
	"valid":    {},
}

func uniqueDependencies(program *vm.Program, allowValid bool) []string {
	if program == nil {
		return nil
	}
	node := program.Node()
	collector := &dependencyCollector{allowValid: allowValid}
	collector.walk(node)
	deps := make([]string, 0, len(collector.names))
	for name := range collector.names {
		deps = append(deps, name)
	}
	sort.Strings(deps)
	return deps
}

type dependencyCollector struct {
	allowValid bool
	names      map[string]struct{}
	locals     map[string]int
}

func (c *dependencyCollector) walk(node ast.Node) {
	if node == nil {
		return
	}
	switch n := node.(type) {
	case *ast.NilNode, *ast.IntegerNode, *ast.FloatNode, *ast.BoolNode, *ast.StringNode,
		*ast.ConstantNode, *ast.PointerNode:
		// No dependencies to collect.
	case *ast.IdentifierNode:
		c.addName(n.Value)
	case *ast.UnaryNode:
		c.walk(n.Node)
	case *ast.BinaryNode:
		c.walk(n.Left)
		c.walk(n.Right)
	case *ast.ChainNode:
		c.walk(n.Node)
	case *ast.MemberNode:
		c.walk(n.Node)
		c.walk(n.Property)
	case *ast.SliceNode:
		c.walk(n.Node)
		if n.From != nil {
			c.walk(n.From)
		}
		if n.To != nil {
			c.walk(n.To)
		}
	case *ast.CallNode:
		c.walk(n.Callee)
		for _, arg := range n.Arguments {
			c.walk(arg)
		}
		if ident, ok := n.Callee.(*ast.IdentifierNode); ok {
			switch ident.Value {
			case "value":
				c.collectCallArgs(n)
			case "valid":
				if c.allowValid {
					c.collectCallArgs(n)
				}
			}
		}
	case *ast.BuiltinNode:
		for _, arg := range n.Arguments {
			c.walk(arg)
		}
		if n.Map != nil {
			c.walk(n.Map)
		}
	case *ast.ClosureNode:
		c.walk(n.Node)
	case *ast.VariableDeclaratorNode:
		c.walk(n.Value)
		c.pushLocal(n.Name)
		c.walk(n.Expr)
		c.popLocal(n.Name)
	case *ast.ConditionalNode:
		c.walk(n.Cond)
		c.walk(n.Exp1)
		c.walk(n.Exp2)
	case *ast.ArrayNode:
		for _, child := range n.Nodes {
			c.walk(child)
		}
	case *ast.MapNode:
		for _, pair := range n.Pairs {
			c.walk(pair)
		}
	case *ast.PairNode:
		c.walk(n.Key)
		c.walk(n.Value)
	default:
		// Fallback to the default walker to ensure we don't miss future node types.
		ast.Walk(&node, c)
	}
}

func (c *dependencyCollector) Visit(node *ast.Node) {
	switch n := (*node).(type) {
	case *ast.IdentifierNode:
		c.addName(n.Value)
	case *ast.CallNode:
		if ident, ok := n.Callee.(*ast.IdentifierNode); ok {
			switch ident.Value {
			case "value":
				c.collectCallArgs(n)
			case "valid":
				if c.allowValid {
					c.collectCallArgs(n)
				}
			}
		}
	}
}

func (c *dependencyCollector) addName(name string) {
	if name == "" {
		return
	}
	if _, reserved := reservedIdentifiers[name]; reserved {
		return
	}
	if c.locals != nil {
		if count := c.locals[name]; count > 0 {
			return
		}
	}
	if c.names == nil {
		c.names = make(map[string]struct{})
	}
	c.names[name] = struct{}{}
}

func (c *dependencyCollector) pushLocal(name string) {
	if name == "" {
		return
	}
	if c.locals == nil {
		c.locals = make(map[string]int)
	}
	c.locals[name]++
}

func (c *dependencyCollector) popLocal(name string) {
	if name == "" || c.locals == nil {
		return
	}
	if c.locals[name] <= 1 {
		delete(c.locals, name)
	} else {
		c.locals[name]--
	}
}

func (c *dependencyCollector) collectCallArgs(node *ast.CallNode) {
	for _, arg := range node.Arguments {
		switch a := arg.(type) {
		case *ast.StringNode:
			if a.Value != "" {
				if c.names == nil {
					c.names = make(map[string]struct{})
				}
				c.names[a.Value] = struct{}{}
			}
		case *ast.IdentifierNode:
			c.addName(a.Value)
		}
	}
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
