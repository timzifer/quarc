package service

import (
	"fmt"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"

	"modbus_processor/internal/config"
)

type dslEngine struct {
	allowIfBlocks bool
	helpers       map[string]*helperFunction
	functions     map[string]func(...interface{}) interface{}
}

type helperFunction struct {
	name      string
	arguments []string
	program   *vm.Program
}

func newDSLEngine(cfg config.DSLConfig) (*dslEngine, error) {
	engine := &dslEngine{
		helpers:   make(map[string]*helperFunction),
		functions: make(map[string]func(...interface{}) interface{}),
	}
	if cfg.AllowIfBlocks != nil {
		engine.allowIfBlocks = *cfg.AllowIfBlocks
	}

	if len(cfg.Helpers) > 0 {
		for _, helperCfg := range cfg.Helpers {
			helper, err := engine.buildHelper(helperCfg)
			if err != nil {
				return nil, err
			}
			if _, exists := engine.helpers[helper.name]; exists {
				return nil, fmt.Errorf("duplicate helper function %q", helper.name)
			}
			engine.helpers[helper.name] = helper
		}
		engine.prepareHelperFunctions()
	}

	return engine, nil
}

func (e *dslEngine) buildHelper(cfg config.HelperFunctionConfig) (*helperFunction, error) {
	name := strings.TrimSpace(cfg.Name)
	if name == "" {
		return nil, fmt.Errorf("helper function name must not be empty")
	}
	if !isValidIdentifier(name) {
		return nil, fmt.Errorf("invalid helper function name %q", name)
	}
	args := make([]string, 0, len(cfg.Arguments))
	seen := make(map[string]struct{})
	for _, raw := range cfg.Arguments {
		arg := strings.TrimSpace(raw)
		if arg == "" {
			return nil, fmt.Errorf("helper %s: argument names must not be empty", name)
		}
		if !isValidIdentifier(arg) {
			return nil, fmt.Errorf("helper %s: invalid argument name %q", name, arg)
		}
		if _, ok := seen[arg]; ok {
			return nil, fmt.Errorf("helper %s: duplicate argument %q", name, arg)
		}
		seen[arg] = struct{}{}
		args = append(args, arg)
	}
	expression := strings.TrimSpace(cfg.Expression)
	if expression == "" {
		return nil, fmt.Errorf("helper %s: expression must not be empty", name)
	}
	processed, err := e.preprocess(expression)
	if err != nil {
		return nil, fmt.Errorf("helper %s: %w", name, err)
	}
	program, err := expr.Compile(processed, expr.Env(map[string]interface{}{}), expr.AllowUndefinedVariables())
	if err != nil {
		return nil, fmt.Errorf("helper %s: compile: %w", name, err)
	}
	return &helperFunction{name: name, arguments: args, program: program}, nil
}

func (e *dslEngine) preprocess(input string) (string, error) {
	if !e.allowIfBlocks {
		return input, nil
	}
	return convertIfBlocks(input)
}

func (e *dslEngine) compileExpression(exprStr string) (*vm.Program, error) {
	processed, err := e.preprocess(strings.TrimSpace(exprStr))
	if err != nil {
		return nil, err
	}
	if processed == "" {
		return nil, nil
	}
	program, err := expr.Compile(processed, expr.Env(map[string]interface{}{}), expr.AllowUndefinedVariables())
	if err != nil {
		return nil, err
	}
	return program, nil
}

func (e *dslEngine) helperNames() map[string]struct{} {
	if len(e.helpers) == 0 {
		return nil
	}
	names := make(map[string]struct{}, len(e.helpers))
	for name := range e.helpers {
		names[name] = struct{}{}
	}
	return names
}

func (e *dslEngine) prepareHelperFunctions() {
	if len(e.helpers) == 0 {
		return
	}
	for name, helper := range e.helpers {
		helper := helper
		e.functions[name] = func(args ...interface{}) interface{} {
			result, err := helper.invoke(e, args...)
			if err != nil {
				panic(err)
			}
			return result
		}
	}
}

func (e *dslEngine) injectHelpers(env map[string]interface{}) {
	for name, fn := range e.functions {
		env[name] = fn
	}
}

func (h *helperFunction) invoke(engine *dslEngine, args ...interface{}) (interface{}, error) {
	if len(args) != len(h.arguments) {
		return nil, fmt.Errorf("helper %s expects %d arguments but got %d", h.name, len(h.arguments), len(args))
	}
	env := make(map[string]interface{}, len(args)+len(engine.functions)+1)
	for idx, name := range h.arguments {
		env[name] = args[idx]
	}
	env["fail"] = helperFail
	for name, fn := range engine.functions {
		env[name] = fn
	}
	result, err := vm.Run(h.program, env)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func helperFail(args ...interface{}) interface{} {
	var ctx dslContext
	ctx.fail(args...)
	return nil
}

func isValidIdentifier(name string) bool {
	if name == "" {
		return false
	}
	for idx, r := range name {
		if idx == 0 && !(r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')) {
			return false
		}
		if idx > 0 {
			if !(r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
				return false
			}
		}
	}
	return true
}
