package processor

import (
	"fmt"
	"strings"

	"github.com/rs/zerolog"

	"github.com/timzifer/modbus_processor/internal/config"
	"github.com/timzifer/modbus_processor/programs"
	"github.com/timzifer/modbus_processor/serviceio"
	"github.com/timzifer/modbus_processor/telemetry"
)

// WithLogger provides a custom logger instance for the processor.
func WithLogger(logger zerolog.Logger) Option {
	return func(cfg *settings) error {
		if cfg == nil {
			return nil
		}
		cfg.logger = logger
		cfg.customLogger = true
		return nil
	}
}

// WithProgram registers a custom program factory.
func WithProgram(def ProgramDefinition) Option {
	return func(cfg *settings) error {
		if cfg == nil {
			return nil
		}
		cfg.programs = append(cfg.programs, def)
		return nil
	}
}

// WithIOService installs additional reader and writer factories for a driver.
func WithIOService(def IOServiceDefinition) Option {
	return func(cfg *settings) error {
		if cfg == nil {
			return nil
		}
		cfg.ioServices = append(cfg.ioServices, def)
		return nil
	}
}

// WithConfigPath configures the processor to load configuration data from the provided path.
func WithConfigPath(path string, register func(ReloadFunc)) Option {
	return func(cfg *settings) error {
		if cfg == nil {
			return nil
		}
		cfg.configPath = strings.TrimSpace(path)
		cfg.registerReload = register
		return nil
	}
}

// WithConfig supplies an already loaded configuration instance.
func WithConfig(cfgData *config.Config) Option {
	return func(cfg *settings) error {
		if cfg == nil {
			return nil
		}
		cfg.config = cfgData
		return nil
	}
}

// WithLiveView enables the embedded live view server on the specified host and port.
func WithLiveView(host string, port int) Option {
	return func(cfg *settings) error {
		if cfg == nil {
			return nil
		}
		if port < 0 {
			return fmt.Errorf("live view port must be non-negative")
		}
		cfg.enableLiveView = true
		cfg.liveViewHost = host
		cfg.liveViewPort = port
		return nil
	}
}

// WithTelemetry injects a collector instance overriding the default configuration-based behaviour.
func WithTelemetry(collector telemetry.Collector) Option {
	return func(cfg *settings) error {
		if cfg == nil {
			return nil
		}
		if collector == nil {
			collector = telemetry.Noop()
		}
		cfg.telemetry = collector
		cfg.telemetryProvided = true
		return nil
	}
}

// NewProgramDefinition creates a program definition from identifier and factory.
func NewProgramDefinition(id string, factory programs.Factory) ProgramDefinition {
	return ProgramDefinition{ID: id, Factory: factory}
}

// NewIOServiceDefinition creates an IO service definition from the provided factories.
func NewIOServiceDefinition(driver string, reader serviceio.ReaderFactory, writer serviceio.WriterFactory) IOServiceDefinition {
	return IOServiceDefinition{Driver: driver, Reader: reader, Writer: writer}
}
