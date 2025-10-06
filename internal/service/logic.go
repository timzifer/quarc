package service

import (
        "errors"
        "fmt"
        "math"
        "sort"
        "strconv"
        "strings"
        "sync"
        "time"

	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/vm"
	"github.com/rs/zerolog"

	"modbus_processor/internal/config"
)

type logicDependency struct {
        cell *cell
        kind config.ValueKind
        threshold float64
}

type logicBlock struct {
        cfg                     config.LogicBlockConfig
        target                  *cell
        deps                    []logicDependency
        trackedDeps             []logicDependency
        expression              *vm.Program
        validExpr               *vm.Program
        qualityExpr             *vm.Program
        dsl                     *dslEngine
        order                   int
	expressionDependencyIDs []string
	validDependencyIDs      []string
	qualityDependencyIDs    []string
        dependents              []*logicBlock
        internalDependencies    int
        metrics                 logicBlockMetrics
}

type dependencySnapshot struct {
        Valid bool
        Value interface{}
}

type logicBlockMetrics struct {
        mu      sync.Mutex
        calls   uint64
        skipped uint64
        total   time.Duration
        last    time.Duration
        prev    map[string]dependencySnapshot
}

type logicMetricsSnapshot struct {
        Calls          uint64
        Skipped        uint64
        Average        time.Duration
        Last           time.Duration
}

type validationResult struct {
	valid   bool
	quality *float64
	code    string
	message string
}

type evaluationError struct {
	Code    string
	Message string
}

func (e evaluationError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.Code
}

func newLogicBlocks(cfgs []config.LogicBlockConfig, cells *cellStore, dsl *dslEngine, logger zerolog.Logger) ([]*logicBlock, []*logicBlock, error) {
	blocks := make([]*logicBlock, 0, len(cfgs))
	producers := make(map[string]*logicBlock)
	logicLogger := logger.With().Str("component", "logic").Logger()

	for idx, cfg := range cfgs {
		block, _, err := prepareLogicBlock(cfg, cells, dsl, idx)
		if err != nil {
			return nil, nil, err
		}

		if block.expression != nil {
			logicLogger.Debug().
				Str("block", cfg.ID).
				Str("expression", strings.TrimSpace(cfg.Expression)).
				Strs("dependencies", block.expressionDependencyIDs).
				Msg("logic block expression")
		}
		if block.validExpr != nil {
			logicLogger.Debug().
				Str("block", cfg.ID).
				Str("expression", strings.TrimSpace(cfg.Valid)).
				Strs("dependencies", block.validDependencyIDs).
				Msg("logic block valid expression")
		}
		if block.qualityExpr != nil {
			logicLogger.Debug().
				Str("block", cfg.ID).
				Str("expression", strings.TrimSpace(cfg.Quality)).
				Strs("dependencies", block.qualityDependencyIDs).
				Msg("logic block quality expression")
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

	for _, block := range blocks {
		for _, dep := range block.deps {
			if producer, ok := producers[dep.cell.cfg.ID]; ok {
				block.internalDependencies++
				producer.dependents = append(producer.dependents, block)
			}
		}
	}

	return blocks, ordered, nil
}

type dependencyMeta struct {
	expression bool
	valid      bool
	quality    bool
	configured bool
	cell       *cell
}

func prepareLogicBlock(cfg config.LogicBlockConfig, cells *cellStore, dsl *dslEngine, order int) (*logicBlock, map[string]*dependencyMeta, error) {
        block := &logicBlock{cfg: cfg, dsl: dsl, order: order}
        meta := make(map[string]*dependencyMeta)
        thresholds := make(map[string]float64)

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

	expressionIDs := make(map[string]struct{})
	validIDs := make(map[string]struct{})
	qualityIDs := make(map[string]struct{})

        for _, depCfg := range cfg.Dependencies {
                entry := ensure(depCfg.Cell)
                if entry == nil {
                        continue
                }
                entry.expression = true
                entry.configured = true
                thresholds[depCfg.Cell] = depCfg.Threshold
                depCell, depErr := cells.mustGet(depCfg.Cell)
                if depErr != nil {
                        return block, meta, fmt.Errorf("logic block %s dependency %s: %w", cfg.ID, depCfg.Cell, depErr)
                }
                if depCfg.Type != "" && depCell.cfg.Type != depCfg.Type {
			return block, meta, fmt.Errorf("logic block %s dependency %s expects %s but cell is %s", cfg.ID, depCfg.Cell, depCfg.Type, depCell.cfg.Type)
		}
		entry.cell = depCell
		expressionIDs[depCfg.Cell] = struct{}{}
	}

	var helperNames map[string]struct{}
	if dsl != nil {
		helperNames = dsl.helperNames()
	}

	if strings.TrimSpace(cfg.Expression) != "" {
		program, err := dsl.compileExpression(cfg.Expression)
		if err != nil {
			return block, meta, fmt.Errorf("logic block %s expression: %w", cfg.ID, err)
		}
		block.expression = program
		for _, dep := range uniqueDependencies(program, helperNames) {
			entry := ensure(dep)
			if entry == nil {
				continue
			}
			entry.expression = true
			expressionIDs[dep] = struct{}{}
		}
	}

	if strings.TrimSpace(cfg.Valid) != "" {
		program, err := dsl.compileValidationExpression(cfg.Valid)
		if err != nil {
			return block, meta, fmt.Errorf("logic block %s valid: %w", cfg.ID, err)
		}
		block.validExpr = program
		for _, dep := range uniqueDependencies(program, helperNames) {
			entry := ensure(dep)
			if entry == nil {
				continue
			}
			entry.valid = true
			validIDs[dep] = struct{}{}
		}
	}

	if strings.TrimSpace(cfg.Quality) != "" {
		program, err := dsl.compileValidationExpression(cfg.Quality)
		if err != nil {
			return block, meta, fmt.Errorf("logic block %s quality: %w", cfg.ID, err)
		}
		block.qualityExpr = program
		for _, dep := range uniqueDependencies(program, helperNames) {
			entry := ensure(dep)
			if entry == nil {
				continue
			}
			entry.quality = true
			qualityIDs[dep] = struct{}{}
		}
	}

	for id, entry := range meta {
		if entry.cell != nil {
			continue
		}
		depCell, depErr := cells.mustGet(id)
		if depErr != nil {
			switch {
			case entry.expression:
				return block, meta, fmt.Errorf("logic block %s dependency %s: %w", cfg.ID, id, depErr)
			case entry.valid:
				return block, meta, fmt.Errorf("logic block %s valid dependency %s: %w", cfg.ID, id, depErr)
			case entry.quality:
				return block, meta, fmt.Errorf("logic block %s quality dependency %s: %w", cfg.ID, id, depErr)
			default:
				return block, meta, fmt.Errorf("logic block %s dependency %s: %w", cfg.ID, id, depErr)
			}
		}
		entry.cell = depCell
	}

        block.expressionDependencyIDs = sortKeys(expressionIDs)
        block.validDependencyIDs = sortKeys(validIDs)
        block.qualityDependencyIDs = sortKeys(qualityIDs)

        deps := make([]logicDependency, 0, len(block.expressionDependencyIDs))
        for _, id := range block.expressionDependencyIDs {
                entry := meta[id]
                if entry == nil || entry.cell == nil {
                        continue
                }
                deps = append(deps, logicDependency{cell: entry.cell, kind: entry.cell.cfg.Type, threshold: thresholds[id]})
        }
        block.deps = deps

        tracked := make([]logicDependency, 0, len(meta))
        for id, entry := range meta {
                if entry == nil || entry.cell == nil {
                        continue
                }
                if !entry.expression && !entry.valid && !entry.quality {
                        continue
                }
                tracked = append(tracked, logicDependency{cell: entry.cell, kind: entry.cell.cfg.Type, threshold: thresholds[id]})
        }
        sort.Slice(tracked, func(i, j int) bool { return tracked[i].cell.cfg.ID < tracked[j].cell.cfg.ID })
        block.trackedDeps = tracked

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
	"value":         {},
	"valid":         {},
	"dump":          {},
	"log":           {},
	"result":        {},
	"has_error":     {},
	"error":         {},
	"cell":          {},
	"value_fn":      {},
	"error_code":    {},
	"error_message": {},
	"error_raw":     {},
}

func uniqueDependencies(program *vm.Program, helpers map[string]struct{}) []string {
	if program == nil {
		return nil
	}
	node := program.Node()
	collector := &dependencyCollector{helpers: helpers}
	collector.walk(node)
	deps := make([]string, 0, len(collector.names))
	for name := range collector.names {
		deps = append(deps, name)
	}
	sort.Strings(deps)
	return deps
}

type dependencyCollector struct {
	names   map[string]struct{}
	locals  map[string]int
	helpers map[string]struct{}
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
			case "value", "valid", "cell", "value_fn":
				c.collectCallArgs(n)
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
			case "value", "valid", "cell", "value_fn":
				c.collectCallArgs(n)
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
	if c.helpers != nil {
		if _, ok := c.helpers[name]; ok {
			return
		}
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

func (b *logicBlock) evaluate(now time.Time, snapshot map[string]*snapshotValue, mu *sync.RWMutex, logger zerolog.Logger) int {
        errors := 0
        if b.target == nil {
                return 0
        }

        blockLogger := logger.With().Str("block", b.cfg.ID).Logger()
        blockLogger.Trace().Msg("logic block evaluation started")

        view := cloneSnapshotValues(snapshot, mu)
        if !b.shouldEvaluate(view) {
                blockLogger.Trace().Msg("skipping evaluation (inputs unchanged)")
                return 0
        }
        ready := b.dependenciesReady(view)
        start := time.Now()
        defer func() {
                b.metrics.recordCall(time.Since(start))
        }()
        var (
                value   interface{}
                evalErr error
        )

	switch {
	case !ready:
		evalErr = evaluationError{Code: "logic.dependencies", Message: "dependencies invalid"}
	case b.expression == nil:
		evalErr = evaluationError{Code: "logic.expression_missing", Message: "no expression configured"}
	default:
		value, evalErr = b.runExpression(view)
		if evalErr != nil {
			blockLogger.Debug().Err(evalErr).Msg("expression evaluation failed")
		}
	}

	decision, valErr := b.runValidation(view, value, evalErr)
	if valErr != nil {
		blockLogger.Error().Err(valErr).Msg("validate evaluation failed")
		b.target.markInvalid(now, "logic.validate_error", valErr.Error())
		updateSnapshotValue(snapshot, mu, b.target.cfg.ID, b.target.asSnapshotValue())
		errors++
		return errors
	}

	if evalErr != nil && decision.valid {
		blockLogger.Warn().Err(evalErr).Msg("validator ignored expression error")
		decision.valid = false
	}

        if decision.valid {
                if err := b.target.setValue(value, now, decision.quality); err != nil {
                        blockLogger.Error().Err(err).Msg("assign result")
                        b.target.markInvalid(now, "logic.assign", err.Error())
                        updateSnapshotValue(snapshot, mu, b.target.cfg.ID, b.target.asSnapshotValue())
                        errors++
			return errors
		}
		updateSnapshotValue(snapshot, mu, b.target.cfg.ID, b.target.asSnapshotValue())
		blockLogger.Trace().Bool("success", true).Msg("logic block evaluation completed")
		return errors
	}

	diag := decision.diagnosis(now)
	if diag == nil && evalErr != nil {
		diag = evaluationErrorDiagnosis(evalErr, now)
	}
	if diag != nil {
		b.target.markInvalid(now, diag.Code, diag.Message)
	} else {
		b.target.markInvalid(now, "logic.invalid", "evaluation failed")
	}
	updateSnapshotValue(snapshot, mu, b.target.cfg.ID, b.target.asSnapshotValue())
	blockLogger.Trace().Bool("success", false).Msg("logic block evaluation completed")
	errors++
	return errors
}

func (b *logicBlock) dependenciesReady(snapshot map[string]*snapshotValue) bool {
        for _, dep := range b.deps {
                snap := snapshot[dep.cell.cfg.ID]
                if snap == nil {
                        return false
		}
		if snap.Kind != dep.kind {
			return false
		}
	}
	return true
}

func (b *logicBlock) runExpression(snapshot map[string]*snapshotValue) (interface{}, error) {
        if b.expression == nil {
                return nil, evaluationError{Code: "logic.expression_missing", Message: "no expression configured"}
	}
	env := make(map[string]interface{}, len(snapshot)+5)
	for id, value := range snapshot {
		if value != nil {
			env[id] = value.Value
		}
	}

	trimmedExpr := strings.TrimSpace(b.cfg.Expression)
	blockLogger := b.dsl.logger.With().
		Str("block", b.cfg.ID).
		Logger()
	ctx := &dslContext{
		snapshot:       snapshot,
		logger:         &blockLogger,
		originKind:     "logic",
		originID:       b.cfg.ID,
		expression:     trimmedExpr,
		expressionKind: "expression",
	}
	env["value"] = ctx.value
	env["cell"] = ctx.value
	env["value_fn"] = ctx.value
	env["valid"] = ctx.valid
	env["dump"] = ctx.dump
	env["log"] = ctx.log
	if b.dsl != nil {
		b.dsl.injectHelpers(env)
	}

	result, err := vm.Run(b.expression, env)
	if err != nil {
		return nil, normalizeEvaluationError(err)
	}
	return result, nil
}

func (b *logicBlock) shouldEvaluate(snapshot map[string]*snapshotValue) bool {
        if len(b.trackedDeps) == 0 {
                return true
        }
        b.metrics.mu.Lock()
        defer b.metrics.mu.Unlock()
        if b.metrics.prev == nil {
                b.metrics.prev = make(map[string]dependencySnapshot, len(b.trackedDeps))
        }
        changed := false
        for _, dep := range b.trackedDeps {
                id := dep.cell.cfg.ID
                snap := snapshot[id]
                current := dependencySnapshot{}
                if snap != nil {
                        current.Valid = snap.Valid
                        if snap.Valid {
                                switch dep.kind {
                                case config.ValueKindNumber:
                                        current.Value = asFloat64(snap.Value)
                                default:
                                        current.Value = cloneValue(snap.Value)
                                }
                        }
                }
                if !changed {
                        prev, ok := b.metrics.prev[id]
                        if !ok {
                                changed = true
                        } else if current.Valid != prev.Valid {
                                changed = true
                        } else if current.Valid {
                                switch dep.kind {
                                case config.ValueKindNumber:
                                        cur := asFloat64(current.Value)
                                        old := asFloat64(prev.Value)
                                        threshold := dep.threshold
                                        if threshold < 0 {
                                                threshold = 0
                                        }
                                        if math.IsNaN(cur) || math.IsNaN(old) || math.Abs(cur-old) > threshold {
                                                changed = true
                                        }
                                default:
                                        if current.Value != prev.Value {
                                                changed = true
                                        }
                                }
                        }
                }
                b.metrics.prev[id] = current
        }
        if !changed {
                b.metrics.skipped++
                return false
        }
        return true
}

func asFloat64(value interface{}) float64 {
        switch v := value.(type) {
        case float64:
                return v
        case float32:
                return float64(v)
        case int:
                return float64(v)
        case int8:
                return float64(v)
        case int16:
                return float64(v)
        case int32:
                return float64(v)
        case int64:
                return float64(v)
        case uint:
                return float64(v)
        case uint8:
                return float64(v)
        case uint16:
                return float64(v)
        case uint32:
                return float64(v)
        case uint64:
                return float64(v)
        default:
                return math.NaN()
        }
}

func (m *logicBlockMetrics) recordCall(duration time.Duration) {
        m.mu.Lock()
        defer m.mu.Unlock()
        m.calls++
        m.last = duration
        m.total += duration
}

func (b *logicBlock) metricsSnapshot() logicMetricsSnapshot {
        b.metrics.mu.Lock()
        defer b.metrics.mu.Unlock()
        avg := time.Duration(0)
        if b.metrics.calls > 0 {
                avg = b.metrics.total / time.Duration(b.metrics.calls)
        }
        return logicMetricsSnapshot{
                Calls:   b.metrics.calls,
                Skipped: b.metrics.skipped,
                Average: avg,
                Last:    b.metrics.last,
        }
}

func (b *logicBlock) runValidation(snapshot map[string]*snapshotValue, value interface{}, evalErr error) (validationResult, error) {
	decision := defaultValidation(evalErr)

	applyOutcome := func(program *vm.Program, expr, kind string, handler func(interface{}) error) error {
		if program == nil {
			return nil
		}
		env := b.buildValidationEnv(snapshot, value, evalErr, expr, kind)
		outcome, err := vm.Run(program, env)
		if err != nil {
			if evalSignal, ok := asEvaluationError(err); ok {
				decision.valid = false
				decision.code = evalSignal.Code
				decision.message = evalSignal.Message
				return nil
			}
			return err
		}
		if handler != nil {
			return handler(outcome)
		}
		return nil
	}

	if err := applyOutcome(b.validExpr, b.cfg.Valid, "valid", func(outcome interface{}) error {
		decision = applyValidationOutcome(decision, outcome)
		return nil
	}); err != nil {
		return decision, err
	}

	if err := applyOutcome(b.qualityExpr, b.cfg.Quality, "quality", func(outcome interface{}) error {
		if outcome == nil {
			return nil
		}
		if m, ok := outcome.(map[string]interface{}); ok {
			decision = applyValidationMap(decision, m)
			return nil
		}
		if f, ok := toFloat(outcome); ok {
			decision.quality = floatPtr(f)
		}
		return nil
	}); err != nil {
		return decision, err
	}

	return decision, nil
}

func (b *logicBlock) buildValidationEnv(snapshot map[string]*snapshotValue, value interface{}, evalErr error, expr string, kind string) map[string]interface{} {
	env := make(map[string]interface{}, len(snapshot)+9)
	for id, snap := range snapshot {
		if snap != nil {
			env[id] = snap.Value
		}
	}

	trimmedExpr := strings.TrimSpace(expr)
	blockLogger := b.dsl.logger.With().
		Str("block", b.cfg.ID).
		Logger()
	ctx := &dslContext{
		snapshot:       snapshot,
		logger:         &blockLogger,
		originKind:     "logic",
		originID:       b.cfg.ID,
		expression:     trimmedExpr,
		expressionKind: kind,
	}
	env["cell"] = ctx.value
	env["value_fn"] = ctx.value
	env["valid"] = ctx.valid
	env["dump"] = ctx.dump
	env["log"] = ctx.log
	env["value"] = value
	env["result"] = value
	env["has_error"] = evalErr != nil
	env["error"] = buildErrorInfo(evalErr)
	env["error_raw"] = evalErr
	if evalErr != nil {
		env["error_message"] = evalErr.Error()
		if e, ok := evalErr.(evaluationError); ok {
			env["error_code"] = e.Code
		}
	}
	if b.dsl != nil {
		b.dsl.injectHelpers(env)
	}
	return env
}

func cloneSnapshotValues(snapshot map[string]*snapshotValue, mu *sync.RWMutex) map[string]*snapshotValue {
	if snapshot == nil {
		return nil
	}
	if mu != nil {
		mu.RLock()
	}
	clone := make(map[string]*snapshotValue, len(snapshot))
	for id, snap := range snapshot {
		if snap == nil {
			clone[id] = nil
			continue
		}
		copy := &snapshotValue{
			Value:   cloneValue(snap.Value),
			Valid:   snap.Valid,
			Kind:    snap.Kind,
			Quality: cloneQuality(snap.Quality),
		}
		clone[id] = copy
	}
	if mu != nil {
		mu.RUnlock()
	}
	return clone
}

func updateSnapshotValue(snapshot map[string]*snapshotValue, mu *sync.RWMutex, id string, value *snapshotValue) {
	if snapshot == nil {
		return
	}
	if mu != nil {
		mu.Lock()
		snapshot[id] = value
		mu.Unlock()
		return
	}
	snapshot[id] = value
}

func normalizeEvaluationError(err error) error {
	if err == nil {
		return nil
	}
	if eval, ok := asEvaluationError(err); ok {
		return eval
	}
	return evaluationError{Code: "logic.runtime_error", Message: err.Error()}
}

func defaultValidation(evalErr error) validationResult {
	if evalErr == nil {
		return validationResult{valid: true, quality: floatPtr(1)}
	}
	if eval, ok := evalErr.(evaluationError); ok {
		return validationResult{valid: false, code: eval.Code, message: eval.Message}
	}
	return validationResult{valid: false, code: "logic.error", message: evalErr.Error()}
}

func buildErrorInfo(err error) map[string]interface{} {
	if err == nil {
		return nil
	}
	info := map[string]interface{}{"message": err.Error()}
	if eval, ok := err.(evaluationError); ok {
		if eval.Code != "" {
			info["code"] = eval.Code
		}
	}
	return info
}

func asEvaluationError(err error) (evaluationError, bool) {
	if err == nil {
		return evaluationError{}, false
	}
	var eval evaluationError
	if errors.As(err, &eval) {
		return eval, true
	}
	return evaluationError{}, false
}

func applyValidationOutcome(base validationResult, outcome interface{}) validationResult {
	if outcome == nil {
		return base
	}
	if b, ok := toBool(outcome); ok {
		base.valid = b
	}
	return base
}

func applyValidationMap(base validationResult, values map[string]interface{}) validationResult {
	for key, raw := range values {
		switch strings.ToLower(key) {
		case "valid":
			if b, ok := toBool(raw); ok {
				base.valid = b
			}
		case "quality":
			if f, ok := toFloat(raw); ok {
				base.quality = floatPtr(f)
			}
		case "code":
			base.code = fmt.Sprint(raw)
		case "message":
			base.message = fmt.Sprint(raw)
		}
	}
	return base
}

func toBool(value interface{}) (bool, bool) {
	switch v := value.(type) {
	case bool:
		return v, true
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "true", "1", "yes", "ok":
			return true, true
		case "false", "0", "no":
			return false, true
		}
	}
	return false, false
}

func toFloat(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(trimmed, 64)
		if err == nil {
			return f, true
		}
	}
	return 0, false
}

func toInt64(value interface{}) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	default:
		return 0
	}
}

func toUint64(value interface{}) uint64 {
	switch v := value.(type) {
	case uint:
		return uint64(v)
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return v
	default:
		return 0
	}
}

func floatPtr(v float64) *float64 {
	value := v
	return &value
}

func (r validationResult) diagnosis(ts time.Time) *diagnosis {
	if r.valid {
		return nil
	}
	if r.code == "" && r.message == "" {
		return nil
	}
	return &diagnosis{Code: r.code, Message: r.message, Timestamp: ts}
}

func evaluationErrorDiagnosis(err error, ts time.Time) *diagnosis {
	if err == nil {
		return nil
	}
	if eval, ok := err.(evaluationError); ok {
		return &diagnosis{Code: eval.Code, Message: eval.Message, Timestamp: ts}
	}
	return &diagnosis{Code: "logic.error", Message: err.Error(), Timestamp: ts}
}

type dslContext struct {
	snapshot       map[string]*snapshotValue
	logger         *zerolog.Logger
	originKind     string
	originID       string
	expression     string
	expressionKind string
}

func (c *dslContext) value(id string) interface{} {
	snap := c.snapshot[id]
	if snap == nil {
		panic(evaluationError{Code: "logic.dependency_missing", Message: fmt.Sprintf("dependency %s missing", id)})
	}
	if !snap.Valid {
		panic(evaluationError{Code: "logic.dependency_invalid", Message: fmt.Sprintf("dependency %s invalid", id)})
	}
	return snap.Value
}

func (c *dslContext) valid(id string) bool {
	snap := c.snapshot[id]
	return snap != nil && snap.Valid
}

func (c *dslContext) dump(value interface{}) interface{} {
	if c.logger != nil {
		event := c.logger.Debug()
		event = c.decorateEvent(event)
		event.Interface("value", value).Msg("dsl dump")
	}
	return value
}

func (c *dslContext) log(args ...interface{}) interface{} {
	if c.logger == nil {
		return nil
	}
	event := c.logger.Info()
	event = c.decorateEvent(event)
	message := "dsl log"
	if len(args) > 0 {
		if s, ok := args[0].(string); ok {
			message = s
			args = args[1:]
		}
	}
	switch len(args) {
	case 0:
	case 1:
		event.Interface("value", args[0])
	default:
		event.Interface("values", args)
	}
	event.Msg(message)
	return nil
}

func (c *dslContext) decorateEvent(event *zerolog.Event) *zerolog.Event {
	if c.originKind != "" {
		event = event.Str("origin_kind", c.originKind)
	}
	if c.originID != "" {
		event = event.Str("origin_id", c.originID)
	}
	if c.expressionKind != "" {
		event = event.Str("expression_kind", c.expressionKind)
	}
	if c.expression != "" {
		event = event.Str("expression", c.expression)
	}
	return event
}
