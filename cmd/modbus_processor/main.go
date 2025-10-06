package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"modbus_processor/internal/config"
	"modbus_processor/internal/logging"
	"modbus_processor/internal/service"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "Path to configuration file")
	healthcheck := flag.Bool("healthcheck", false, "Run a health check and exit")
	configCheck := flag.Bool("config-check", false, "Validate configuration and exit")
	flag.Parse()

	if *healthcheck {
		if err := executeHealthCheck(*cfgPath); err != nil {
			fmt.Fprintf(os.Stderr, "health check failed: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load configuration")
	}

	if *configCheck {
		exitCode := executeConfigCheck(cfg)
		os.Exit(exitCode)
	}

	logger, cleanup, err := logging.Setup(cfg.Logging)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to setup logger")
	}
	defer cleanup()
	log.Logger = logger

	srv, err := service.New(cfg, logger, nil)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create service")
	}
	defer srv.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := srv.Run(ctx); err != nil {
		logger.Fatal().Err(err).Msg("service stopped with error")
	}
}

func executeHealthCheck(path string) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	return service.Validate(cfg, zerolog.Nop())
}

func executeConfigCheck(cfg *config.Config) int {
	reports, err := service.AnalyzeLogic(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration invalid: %v\n", err)
		return 1
	}

	if len(reports) == 0 {
		fmt.Println("No logic blocks configured.")
		return 0
	}

	exitCode := 0
	for _, report := range reports {
		fmt.Printf("Logic block %q\n", report.ID)
		fmt.Printf("  Target: %s", report.Target)
		if report.TargetType != "" {
			fmt.Printf(" (%s)", report.TargetType)
		}
		fmt.Println()

		printExpression("Expression", report.Expression)
		printExpression("Valid expression", report.Valid)
		printExpression("Quality expression", report.Quality)

		printDependencyGroup("Expression dependencies", report.Dependencies, func(dep service.DependencyReport) bool { return dep.InExpression })
		printDependencyGroup("Valid dependencies", report.Dependencies, func(dep service.DependencyReport) bool { return dep.InValid })
		printDependencyGroup("Quality dependencies", report.Dependencies, func(dep service.DependencyReport) bool { return dep.InQuality })

		if len(report.Errors) > 0 {
			exitCode = 1
			fmt.Println("  Errors:")
			for _, msg := range report.Errors {
				fmt.Printf("    - %s\n", msg)
			}
		} else {
			fmt.Println("  Status: OK")
		}

		fmt.Println()
	}

	if exitCode == 0 {
		fmt.Println("Configuration check completed successfully.")
	} else {
		fmt.Println("Configuration check completed with errors.")
	}
	return exitCode
}

func printExpression(label, expr string) {
	fmt.Printf("  %s:\n", label)
	if expr == "" {
		fmt.Println("    <empty>")
		return
	}
	lines := strings.Split(expr, "\n")
	for _, line := range lines {
		fmt.Printf("    %s\n", strings.TrimRight(line, " \t"))
	}
}

func printDependencyGroup(title string, deps []service.DependencyReport, filter func(service.DependencyReport) bool) {
	fmt.Printf("  %s:\n", title)
	filtered := make([]service.DependencyReport, 0, len(deps))
	for _, dep := range deps {
		if filter(dep) {
			filtered = append(filtered, dep)
		}
	}
	if len(filtered) == 0 {
		fmt.Println("    <none>")
		return
	}
	for _, dep := range filtered {
		typeLabel := "unknown"
		if dep.Resolved && dep.Type != "" {
			typeLabel = string(dep.Type)
		}
		notes := make([]string, 0, 3)
		if dep.ManuallyConfigured {
			notes = append(notes, "configured")
		}
		switch title {
		case "Expression dependencies":
			if dep.InValid {
				notes = append(notes, "also valid")
			}
			if dep.InQuality {
				notes = append(notes, "also quality")
			}
		case "Valid dependencies":
			if dep.InExpression {
				notes = append(notes, "also expression")
			}
			if dep.InQuality {
				notes = append(notes, "also quality")
			}
		case "Quality dependencies":
			if dep.InExpression {
				notes = append(notes, "also expression")
			}
			if dep.InValid {
				notes = append(notes, "also valid")
			}
		}
		if !dep.Resolved {
			notes = append(notes, "unresolved")
		}
		fmt.Printf("    - %s (%s)", dep.Cell, typeLabel)
		if len(notes) > 0 {
			fmt.Printf(" [%s]", strings.Join(notes, ", "))
		}
		fmt.Println()
	}
}
