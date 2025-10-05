package service

import (
	"errors"
	"fmt"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/rs/zerolog"

	"modbus_processor/internal/config"
)

type dslEngine struct {
	allowIfBlocks bool
	helpers       map[string]*helperFunction
	functions     map[string]func(...interface{}) interface{}
	logger        zerolog.Logger
}

type helperFunction struct {
	name       string
	arguments  []string
	program    *vm.Program
	expression string
}

func newDSLEngine(cfg config.DSLConfig, helpers []config.HelperFunctionConfig, logger zerolog.Logger) (*dslEngine, error) {
	engine := &dslEngine{
		helpers:   make(map[string]*helperFunction),
		functions: make(map[string]func(...interface{}) interface{}),
		logger:    logger.With().Str("component", "dsl").Logger(),
	}
	if cfg.AllowIfBlocks != nil {
		engine.allowIfBlocks = *cfg.AllowIfBlocks
	}

	helperCfgs := make([]config.HelperFunctionConfig, 0, len(helpers)+len(cfg.Helpers))
	helperCfgs = append(helperCfgs, helpers...)
	helperCfgs = append(helperCfgs, cfg.Helpers...)

	if len(helperCfgs) > 0 {
		for _, helperCfg := range helperCfgs {
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
	return &helperFunction{name: name, arguments: args, program: program, expression: expression}, nil
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
	helperLogger := engine.logger.With().
		Str("helper", h.name).
		Logger()
	ctx := &dslContext{
		logger:         &helperLogger,
		originKind:     "helper",
		originID:       h.name,
		expression:     h.expression,
		expressionKind: "helper",
	}
	env := make(map[string]interface{}, len(args)+len(engine.functions)+4)
	for idx, name := range h.arguments {
		env[name] = args[idx]
	}
	env["success"] = ctx.success
	env["fail"] = ctx.fail
	env["log"] = ctx.log
	env["dump"] = ctx.dump
	for name, fn := range engine.functions {
		env[name] = fn
	}
	_, err := vm.Run(h.program, env)
	if err != nil {
		var success successSignal
		if errors.As(err, &success) {
			return success.value, nil
		}
		return nil, err
	}
	return nil, fmt.Errorf("helper %s: expression completed without success()", h.name)
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
