package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"

	"github.com/timzifer/quarc/config"
	"github.com/timzifer/quarc/runtime/state"
	"github.com/timzifer/quarc/runtime/writers"
)

// NewWriteFactory implements writers.WriterFactory for QUARC.
func NewWriteFactory() writers.WriterFactory {
	return func(cfg config.WriteTargetConfig, deps writers.WriterDependencies) (writers.Writer, error) {
		if cfg.ID == "" {
			return nil, errors.New("write target id must not be empty")
		}
		if deps.Cells == nil {
			return nil, fmt.Errorf("write target %s: missing cell store dependency", cfg.ID)
		}
		if cfg.DriverSettings == nil {
			return nil, fmt.Errorf("write target %s: driver_settings missing", cfg.ID)
		}
		var settings WriteSettings
		if err := json.Unmarshal(cfg.DriverSettings, &settings); err != nil {
			return nil, fmt.Errorf("write target %s: decode driver_settings: %w", cfg.ID, err)
		}
		if err := settings.Validate(cfg.Connection == ""); err != nil {
			return nil, fmt.Errorf("write target %s: %w", cfg.ID, err)
		}

		cell, err := deps.Cells.Get(cfg.Cell)
		if err != nil {
			return nil, fmt.Errorf("write target %s: resolve cell %s: %w", cfg.ID, cfg.Cell, err)
		}

		function := cfg.Function
		if function == "" {
			function = "mqtt"
		}

		haPublisher, err := newHomeAssistantPublisher(settings.HomeAssistant, settings.Connection, settings.Topic, homeAssistantTopicDefaults{
			state: settings.Topic,
		})
		if err != nil {
			return nil, fmt.Errorf("write target %s: %w", cfg.ID, err)
		}

		var shared *sharedConnection
		if cfg.Connection != "" {
			if deps.Connections == nil {
				return nil, fmt.Errorf("write target %s: connection %s requested but connection sharing not configured", cfg.ID, cfg.Connection)
			}
			handle, err := deps.Connections.Connection(cfg.Connection)
			if err != nil {
				return nil, fmt.Errorf("write target %s: %w", cfg.ID, err)
			}
			var ok bool
			shared, ok = handle.(*sharedConnection)
			if !ok {
				return nil, fmt.Errorf("write target %s: connection %s: incompatible handle type %T", cfg.ID, cfg.Connection, handle)
			}
			settings.Connection = shared.Settings()
		}

		writer := &mqttWriter{
			id:       cfg.ID,
			cellID:   cfg.Cell,
			function: function,
			address:  cfg.Address,
			source:   cfg.Source,
			settings: settings,
			cell:     cell,
			logger:   zerolog.Nop(),
			ha:       haPublisher,
		}

		if shared != nil {
			client := shared.Client()
			if client == nil {
				return nil, fmt.Errorf("write target %s: connection %s returned nil client", cfg.ID, cfg.Connection)
			}
			writer.client = client
			writer.sharedConn = shared
		} else {
			client, err := buildClient(settings.Connection, writer.logger, nil)
			if err != nil {
				return nil, fmt.Errorf("write target %s: %w", cfg.ID, err)
			}
			writer.client = client
			writer.ownsClient = true
		}

		return writer, nil
	}
}

type mqttWriter struct {
	id       string
	cellID   string
	function string
	address  uint16
	source   config.ModuleReference

	settings WriteSettings
	cell     state.Cell
	client   mqtt.Client

	sharedConn *sharedConnection
	ownsClient bool

	logger zerolog.Logger

	disabled     atomic.Bool
	lastUpdate   *CellUpdate
	lastAttempt  time.Time
	lastWrite    time.Time
	lastDuration time.Duration

	mu sync.Mutex

	ha *homeAssistantPublisher
}

func (w *mqttWriter) ID() string { return w.id }

func (w *mqttWriter) Commit(now time.Time, logger zerolog.Logger) int {
	if w.disabled.Load() {
		return 0
	}
	w.logger = logger
	w.mu.Lock()
	w.lastAttempt = now
	w.mu.Unlock()

	value, ok := w.cell.CurrentValue()
	if !ok {
		if w.ha != nil {
			w.ha.PublishAvailability(w.client, logger, false)
		}
		return 0
	}

	update := CellUpdate{Value: value, Timestamp: now}
	w.mu.Lock()
	last := w.lastUpdate
	w.mu.Unlock()

	if w.ha != nil {
		w.ha.Ensure(w.client, logger)
	}

	if !ShouldPublish(w.settings.Deadband, w.settings.RateLimit, last, update) {
		if w.ha != nil {
			w.ha.PublishAvailability(w.client, logger, true)
		}
		return 0
	}

	payload, err := EncodePayload(w.settings.ResolveWriterPayload(), value)
	if err != nil {
		logger.Error().Err(err).Str("cell", w.cellID).Msg("mqtt: encode payload failed")
		return 1
	}

	start := time.Now()
	token := w.client.Publish(w.settings.Topic, w.settings.QoSLevel(), w.settings.RetainFlag(), payload)
	if token.Wait() && token.Error() != nil {
		logger.Error().Err(token.Error()).Str("topic", w.settings.Topic).Msg("mqtt: publish failed")
		return 1
	}

	w.mu.Lock()
	w.lastDuration = time.Since(start)
	w.lastWrite = now
	copy := update
	w.lastUpdate = &copy
	w.mu.Unlock()

	if w.ha != nil {
		w.ha.PublishAvailability(w.client, logger, true)
		w.ha.PublishMirroredState(w.client, logger, payload, w.settings.QoSLevel(), w.settings.RetainFlag())
	}

	return 0
}

func (w *mqttWriter) SetDisabled(disabled bool) {
	w.disabled.Store(disabled)
	if w.ha != nil && disabled {
		w.ha.PublishAvailability(w.client, w.logger, false)
	}
}

func (w *mqttWriter) Status() writers.WriteTargetStatus {
	w.mu.Lock()
	defer w.mu.Unlock()
	return writers.WriteTargetStatus{
		ID:           w.id,
		Cell:         w.cellID,
		Function:     w.function,
		Address:      w.address,
		Disabled:     w.disabled.Load(),
		LastWrite:    w.lastWrite,
		LastAttempt:  w.lastAttempt,
		LastDuration: w.lastDuration,
		Source:       w.source,
	}
}

func (w *mqttWriter) Close() {
	if w.ha != nil {
		w.ha.PublishAvailability(w.client, w.logger, false)
	}
	if !w.ownsClient {
		return
	}
	if w.client != nil && w.client.IsConnected() {
		w.client.Disconnect(250)
	}
}
