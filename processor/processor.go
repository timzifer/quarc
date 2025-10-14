package processor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/timzifer/quarc/config"
	"github.com/timzifer/quarc/service"

	"github.com/timzifer/quarc/internal/logging"
	"github.com/timzifer/quarc/internal/reload"
	"github.com/timzifer/quarc/programs"
	"github.com/timzifer/quarc/serviceio"
	"github.com/timzifer/quarc/telemetry"
)

// ReloadFunc represents a function that reloads the processor configuration.
type ReloadFunc func(ctx context.Context) error

// Option configures the processor during construction.
type Option func(*settings) error

// ProgramDefinition describes a program factory that should be registered before startup.
type ProgramDefinition struct {
	ID       string
	Factory  programs.Factory
	Overlays []config.OverlayDescriptor

	overlaysRegistered bool
}

// IOServiceDefinition bundles optional reader and writer factories under a driver identifier.
type IOServiceDefinition struct {
	Driver   string
	Reader   serviceio.ReaderFactory
	Writer   serviceio.WriterFactory
	Overlays []config.OverlayDescriptor

	overlaysRegistered bool
}

type settings struct {
	config            *config.Config
	configPath        string
	registerReload    func(ReloadFunc)
	logger            zerolog.Logger
	customLogger      bool
	telemetry         telemetry.Collector
	telemetryProvided bool
	programs          []ProgramDefinition
	ioServices        []IOServiceDefinition
	serviceOptions    []service.Option
	liveViewHost      string
	liveViewPort      int
	enableLiveView    bool
}

// Processor orchestrates the service lifecycle, including configuration reloads and cleanup.
type Processor struct {
	mu sync.Mutex

	config     *config.Config
	configPath string

	collector telemetry.Collector

	serviceOptions []service.Option

	customLogger bool
	baseLogger   zerolog.Logger

	liveViewEnabled bool
	liveViewAddr    string

	watcher  *reload.Watcher
	reloadCh chan reloadRequest

	current *runtimeState
	running bool
}

type runtimeState struct {
	cfg     *config.Config
	logger  zerolog.Logger
	cleanup func()
	srv     *service.Service
}

type reloadRequest struct {
	done  chan error
	files []string
}

// New constructs a processor with the supplied options.
func New(ctx context.Context, opts ...Option) (*Processor, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}

	cfg := settings{
		logger:    zerolog.Nop(),
		telemetry: telemetry.Noop(),
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	if cfg.config == nil {
		if cfg.configPath == "" {
			return nil, errors.New("configuration path required")
		}
		loaded, err := config.Load(cfg.configPath)
		if err != nil {
			return nil, fmt.Errorf("load configuration: %w", err)
		}
		cfg.config = loaded
	}
	if cfg.config == nil {
		return nil, errors.New("configuration must not be nil")
	}

	if !cfg.telemetryProvided {
		collector, err := newTelemetryCollector(cfg.config.Telemetry)
		if err != nil {
			fmt.Fprintf(os.Stderr, "telemetry disabled: %v\n", err)
			cfg.telemetry = telemetry.Noop()
		} else {
			cfg.telemetry = collector
		}
	}

	if err := registerPrograms(cfg.programs); err != nil {
		return nil, err
	}

	serviceOpts := buildServiceOptions(cfg.ioServices)
	if len(cfg.serviceOptions) > 0 {
		serviceOpts = append(serviceOpts, cfg.serviceOptions...)
	}

	proc := &Processor{
		config:          cfg.config,
		configPath:      cfg.configPath,
		collector:       cfg.telemetry,
		serviceOptions:  serviceOpts,
		customLogger:    cfg.customLogger,
		baseLogger:      cfg.logger,
		liveViewEnabled: cfg.enableLiveView,
		liveViewAddr:    listenAddress(cfg.liveViewHost, cfg.liveViewPort),
	}

	runtime, err := proc.buildRuntime(cfg.config)
	if err != nil {
		return nil, err
	}
	proc.current = runtime

	if cfg.configPath != "" {
		proc.reloadCh = make(chan reloadRequest)
	}

	if err := proc.initWatcher(cfg.config); err != nil {
		runtime.srv.Close()
		runtime.cleanup()
		return nil, err
	}

	if cfg.registerReload != nil {
		cfg.registerReload(proc.Reload)
	}

	return proc, nil
}

// Run executes the processor until the context is cancelled or the service stops with an error.
func (p *Processor) Run(ctx context.Context) error {
	p.mu.Lock()
	if p.current == nil {
		p.mu.Unlock()
		return errors.New("processor not initialized")
	}
	if p.running {
		p.mu.Unlock()
		return errors.New("processor already running")
	}
	p.running = true
	current := p.current
	watcher := p.watcher
	reloadCh := p.reloadCh
	p.mu.Unlock()

	var ticker *time.Ticker
	if watcher != nil {
		ticker = time.NewTicker(time.Second)
		defer ticker.Stop()
	}

	defer func() {
		p.mu.Lock()
		p.running = false
		if p.current == current {
			p.current = nil
		}
		p.mu.Unlock()
	}()

	for {
		runCtx, cancelRun := context.WithCancel(ctx)
		errCh := make(chan error, 1)
		go func(s *service.Service) {
			errCh <- s.Run(runCtx)
		}(current.srv)

		var pending *reloadRequest
		var nextConfig *config.Config

	loop:
		for {
			select {
			case <-ctx.Done():
				cancelRun()
				err := <-errCh
				current.srv.Close()
				current.cleanup()
				if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
					return err
				}
				return ctx.Err()
			case err := <-errCh:
				current.srv.Close()
				current.cleanup()
				return err
			case req := <-reloadCh:
				cfg, err := p.loadConfig()
				if err != nil {
					if req.done != nil {
						req.done <- err
					}
					continue
				}
				if err := service.Validate(cfg, current.logger); err != nil {
					current.logger.Error().Err(err).Msg("reloaded configuration invalid")
					if req.done != nil {
						req.done <- err
					}
					continue
				}
				pending = &req
				nextConfig = cfg
				break loop
			case <-tickChannel(ticker):
				changes, err := watcher.Check()
				if err != nil {
					current.logger.Error().Err(err).Msg("failed to check configuration changes")
					continue
				}
				if len(changes) == 0 {
					continue
				}
				cfg, err := p.loadConfig()
				if err != nil {
					current.logger.Error().Err(err).Msg("failed to reload configuration")
					continue
				}
				if err := service.Validate(cfg, current.logger); err != nil {
					current.logger.Error().Err(err).Msg("reloaded configuration invalid")
					continue
				}
				pending = &reloadRequest{files: changes}
				nextConfig = cfg
				break loop
			}
		}

		cancelRun()
		if err := <-errCh; err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			current.logger.Error().Err(err).Msg("service stopped during reload")
		}
		current.srv.Close()
		current.cleanup()

		runtime, err := p.buildRuntime(nextConfig)
		if err != nil {
			if pending != nil && pending.done != nil {
				pending.done <- err
			}
			return err
		}

		p.mu.Lock()
		p.current = runtime
		current = runtime
		p.config = nextConfig
		if err := p.initWatcher(nextConfig); err != nil {
			current.logger.Error().Err(err).Msg("failed to update configuration watcher")
		} else {
			watcher = p.watcher
		}
		if ticker != nil {
			ticker.Stop()
			ticker = nil
		}
		if watcher != nil {
			ticker = time.NewTicker(time.Second)
		}
		p.mu.Unlock()

		if pending != nil {
			if pending.done != nil {
				pending.done <- nil
			}
			for _, file := range pending.files {
				p.collector.IncHotReload(file)
			}
		}
	}
}

// Reload rebuilds the processor using the latest configuration from disk.
func (p *Processor) Reload(ctx context.Context) error {
	p.mu.Lock()
	running := p.running
	reloadCh := p.reloadCh
	p.mu.Unlock()

	if !running {
		cfg, err := p.loadConfig()
		if err != nil {
			return err
		}
		if err := service.Validate(cfg, zerolog.Nop()); err != nil {
			return err
		}
		return p.swapRuntime(cfg)
	}

	if reloadCh == nil {
		return errors.New("reload not supported without configuration path")
	}

	req := reloadRequest{done: make(chan error, 1)}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case reloadCh <- req:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-req.done:
		return err
	}
}

// Close releases resources managed by the processor.
func (p *Processor) Close() {
	p.mu.Lock()
	current := p.current
	p.current = nil
	p.mu.Unlock()

	if current != nil {
		current.srv.Close()
		current.cleanup()
	}
}

func (p *Processor) swapRuntime(cfg *config.Config) error {
	runtime, err := p.buildRuntime(cfg)
	if err != nil {
		return err
	}

	p.mu.Lock()
	old := p.current
	p.current = runtime
	p.config = cfg
	err = p.initWatcher(cfg)
	p.mu.Unlock()
	if err != nil {
		runtime.srv.Close()
		runtime.cleanup()
		return err
	}

	if old != nil {
		old.srv.Close()
		old.cleanup()
	}
	return nil
}

func (p *Processor) buildRuntime(cfg *config.Config) (*runtimeState, error) {
	if cfg == nil {
		return nil, errors.New("config must not be nil")
	}
	runtime := &runtimeState{cfg: cfg, cleanup: func() {}}
	if p.customLogger {
		runtime.logger = p.baseLogger
	} else {
		logger, cleanup, err := logging.Setup(cfg.Logging)
		if err != nil {
			return nil, err
		}
		runtime.logger = logger
		runtime.cleanup = cleanup
	}
	log.Logger = runtime.logger

	srv, err := service.New(cfg, runtime.logger, p.serviceOptions...)
	if err != nil {
		runtime.cleanup()
		return nil, err
	}
	if p.liveViewEnabled {
		if err := srv.EnableLiveView(p.liveViewAddr); err != nil {
			srv.Close()
			runtime.cleanup()
			return nil, err
		}
	}
	runtime.srv = srv
	return runtime, nil
}

func (p *Processor) loadConfig() (*config.Config, error) {
	if p.configPath == "" {
		return nil, errors.New("configuration path not configured")
	}
	cfg, err := config.Load(p.configPath)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func (p *Processor) initWatcher(cfg *config.Config) error {
	if p.configPath == "" {
		p.watcher = nil
		return nil
	}
	if !cfg.HotReload {
		p.watcher = nil
		return nil
	}
	if p.watcher == nil {
		watcher, err := reload.NewWatcher(p.configPath, cfg)
		if err != nil {
			return err
		}
		p.watcher = watcher
		return nil
	}
	return p.watcher.Update(p.configPath, cfg)
}

func registerPrograms(defs []ProgramDefinition) error {
	if len(defs) == 0 {
		return nil
	}
	existing := make(map[string]struct{})
	for _, id := range programs.RegisteredIDs() {
		existing[id] = struct{}{}
	}
	for _, def := range defs {
		if def.ID == "" {
			return errors.New("program id must not be empty")
		}
		if def.Factory == nil {
			return fmt.Errorf("program %s factory must not be nil", def.ID)
		}
		if _, ok := existing[def.ID]; ok {
			return fmt.Errorf("program %s already registered", def.ID)
		}
		programs.RegisterProgram(def.ID, def.Factory)
		existing[def.ID] = struct{}{}
	}
	return nil
}

func buildServiceOptions(defs []IOServiceDefinition) []service.Option {
	if len(defs) == 0 {
		return nil
	}
	opts := make([]service.Option, 0, len(defs)*2)
	for _, def := range defs {
		if def.Driver == "" {
			continue
		}
		if def.Reader != nil {
			opts = append(opts, service.WithReaderFactory(def.Driver, def.Reader))
		}
		if def.Writer != nil {
			opts = append(opts, service.WithWriterFactory(def.Driver, def.Writer))
		}
	}
	return opts
}

func listenAddress(host string, port int) string {
	if port <= 0 {
		return host
	}
	return net.JoinHostPort(host, strconv.Itoa(port))
}

func tickChannel(t *time.Ticker) <-chan time.Time {
	if t == nil {
		return nil
	}
	return t.C
}
