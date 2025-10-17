package processor

import (
	"fmt"
	"strings"

	"github.com/rs/zerolog"
	"github.com/timzifer/quarc/config"

	"github.com/timzifer/quarc/programs"
	"github.com/timzifer/quarc/service"
	"github.com/timzifer/quarc/serviceio"
	"github.com/timzifer/quarc/telemetry"
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

// WithProgram registers a custom program factory and publishes any associated overlays.
func WithProgram(def ProgramDefinition, overlays ...config.OverlayDescriptor) Option {
	if len(overlays) > 0 {
		def.Overlays = append(def.Overlays, overlays...)
	}
	registered := def.overlaysRegistered
	return func(cfg *settings) error {
		if cfg == nil {
			return nil
		}
		if !registered && len(def.Overlays) > 0 {
			if err := config.RegisterOverlayDescriptors(def.Overlays...); err != nil {
				return err
			}
			registered = true
			def.overlaysRegistered = true
		}
		cfg.programs = append(cfg.programs, def)
		return nil
	}
}

// WithIOService installs additional reader, writer and connection factories for a driver and publishes overlays.
func WithIOService(def IOServiceDefinition, overlays ...config.OverlayDescriptor) Option {
	if len(overlays) > 0 {
		def.Overlays = append(def.Overlays, overlays...)
	}
	registered := def.overlaysRegistered
	return func(cfg *settings) error {
		if cfg == nil {
			return nil
		}
		if !registered && len(def.Overlays) > 0 {
			if err := config.RegisterOverlayDescriptors(def.Overlays...); err != nil {
				return err
			}
			registered = true
			def.overlaysRegistered = true
		}
		cfg.ioServices = append(cfg.ioServices, def)
		return nil
	}
}

// WithServiceOptions appends service options that will be forwarded to service.New.
func WithServiceOptions(opts ...service.Option) Option {
	return func(cfg *settings) error {
		if cfg == nil {
			return nil
		}
		cfg.serviceOptions = append(cfg.serviceOptions, opts...)
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

// NewProgramDefinition creates a program definition from identifier and factory, registering overlays immediately.
func NewProgramDefinition(id string, factory programs.Factory, overlays ...config.OverlayDescriptor) (ProgramDefinition, error) {
	def := ProgramDefinition{ID: id, Factory: factory}
	if len(overlays) > 0 {
		def.Overlays = append(def.Overlays, overlays...)
		if err := config.RegisterOverlayDescriptors(def.Overlays...); err != nil {
			return ProgramDefinition{}, err
		}
		def.overlaysRegistered = true
	}
	return def, nil
}

// NewIOServiceDefinition creates an IO service definition from the provided factories, registering overlays immediately.
func NewIOServiceDefinition(driver string, reader serviceio.ReaderFactory, writer serviceio.WriterFactory, overlays ...config.OverlayDescriptor) (IOServiceDefinition, error) {
	def := IOServiceDefinition{Driver: driver, Reader: reader, Writer: writer}
	if len(overlays) > 0 {
		def.Overlays = append(def.Overlays, overlays...)
		if err := config.RegisterOverlayDescriptors(def.Overlays...); err != nil {
			return IOServiceDefinition{}, err
		}
		def.overlaysRegistered = true
	}
	return def, nil
}
