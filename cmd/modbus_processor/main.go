package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/timzifer/quarc/config"
	service2 "github.com/timzifer/quarc/service"

	"github.com/timzifer/quarc/processor"
)

func main() {
	cfgPath := flag.String("config", "config.cue", "Path to configuration file")
	healthcheck := flag.Bool("healthcheck", false, "Run a health check and exit")
	configCheck := flag.Bool("config-check", false, "Validate configuration and exit")
	liveView := flag.Bool("live-view", false, "Enable live view web interface")
	liveViewListen := flag.String("live-view-listen", ":18080", "Live view listen address")
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

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var reloadFn processor.ReloadFunc
	options := []processor.Option{
		processor.WithConfig(cfg),
		processor.WithConfigPath(*cfgPath, func(fn processor.ReloadFunc) { reloadFn = fn }),
	}

	if *liveView {
		host, port := parseListenAddress(*liveViewListen)
		options = append(options, processor.WithLiveView(host, port))
	}

	proc, err := processor.New(ctx, options...)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create processor")
	}
	defer proc.Close()

	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-hup:
				if reloadFn == nil {
					continue
				}
				if err := reloadFn(ctx); err != nil {
					log.Error().Err(err).Msg("failed to reload configuration")
				}
			}
		}
	}()

	if err := proc.Run(ctx); err != nil {
		if err == context.Canceled {
			return
		}
		log.Fatal().Err(err).Msg("service stopped with error")
	}
}

func executeHealthCheck(path string) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	return service2.Validate(cfg, zerolog.Nop())
}

func executeConfigCheck(cfg *config.Config) int {
	analysis, err := service2.AnalyzeLogic(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration invalid: %v\n", err)
		return 1
	}

	if analysis == nil {
		fmt.Println("No logic blocks configured.")
		return 0
	}

	if len(analysis.Blocks) == 0 {
		fmt.Println("No logic blocks configured.")
		return 0
	}

	exitCode := 0
	for _, report := range analysis.Blocks {
		fmt.Printf("Logic block %q\n", report.ID)
		if module := describeModule(report.Source); module != "" {
			fmt.Printf("  Module: %s\n", module)
		}
		fmt.Printf("  Target: %s", report.Target)
		if report.TargetType != "" {
			fmt.Printf(" (%s)", report.TargetType)
		}
		fmt.Println()

		printExpression("Expression", report.Expression)
		printExpression("Valid expression", report.Valid)
		printExpression("Quality expression", report.Quality)

		printDependencyGroup("Expression dependencies", report.Dependencies, func(dep service2.DependencyReport) bool { return dep.InExpression })
		printDependencyGroup("Valid dependencies", report.Dependencies, func(dep service2.DependencyReport) bool { return dep.InValid })
		printDependencyGroup("Quality dependencies", report.Dependencies, func(dep service2.DependencyReport) bool { return dep.InQuality })

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

	if len(analysis.UnwrittenCells) > 0 {
		fmt.Println("Warnings:")
		for _, cell := range analysis.UnwrittenCells {
			typeLabel := "unknown"
			if cell.Type != "" {
				typeLabel = string(cell.Type)
			}
			notes := make([]string, 0, 2)
			if module := describeModule(cell.Source); module != "" {
				notes = append(notes, fmt.Sprintf("module %s", module))
			}
			fmt.Printf("  - Cell %s (%s) has no writers", cell.Cell, typeLabel)
			if len(notes) > 0 {
				fmt.Printf(" [%s]", strings.Join(notes, ", "))
			}
			fmt.Println()
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

func printDependencyGroup(title string, deps []service2.DependencyReport, filter func(service2.DependencyReport) bool) {
	fmt.Printf("  %s:\n", title)
	filtered := make([]service2.DependencyReport, 0, len(deps))
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
		if module := describeModule(dep.Source); module != "" {
			notes = append(notes, fmt.Sprintf("module %s", module))
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

func parseListenAddress(address string) (string, int) {
	if address == "" {
		return "", 0
	}
	if strings.HasPrefix(address, ":") {
		port, _ := strconv.Atoi(strings.TrimPrefix(address, ":"))
		return "", port
	}
	host, portStr, err := net.SplitHostPort(address)
	if err == nil {
		port, _ := strconv.Atoi(portStr)
		return host, port
	}
	port, _ := strconv.Atoi(address)
	return "", port
}

func describeModule(ref config.ModuleReference) string {
	name := strings.TrimSpace(ref.Name)
	file := strings.TrimSpace(ref.File)
	desc := strings.TrimSpace(ref.Description)

	label := ""
	if name != "" && file != "" {
		label = fmt.Sprintf("%s (%s)", name, file)
	} else if name != "" {
		label = name
	} else if file != "" {
		label = file
	}
	if desc != "" {
		if label != "" {
			label = fmt.Sprintf("%s – %s", label, desc)
		} else {
			label = desc
		}
	}
	return label
}
