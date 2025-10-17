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
	"github.com/timzifer/quarc/runtime/readers"
	"github.com/timzifer/quarc/runtime/state"
)

// NewReadFactory implements readers.ReaderFactory for QUARC.
func NewReadFactory() readers.ReaderFactory {
	return func(cfg config.ReadGroupConfig, deps readers.ReaderDependencies) (readers.ReadGroup, error) {
		if cfg.ID == "" {
			return nil, errors.New("read group id must not be empty")
		}
		if deps.Cells == nil {
			return nil, fmt.Errorf("read group %s: missing cell store dependency", cfg.ID)
		}
		if cfg.DriverSettings == nil {
			return nil, fmt.Errorf("read group %s: driver_settings missing", cfg.ID)
		}
		var settings ReadSettings
		if err := json.Unmarshal(cfg.DriverSettings, &settings); err != nil {
			return nil, fmt.Errorf("read group %s: decode driver_settings: %w", cfg.ID, err)
		}
		if err := settings.Validate(cfg.Connection == ""); err != nil {
			return nil, fmt.Errorf("read group %s: %w", cfg.ID, err)
		}

		function := cfg.Function
		if function == "" {
			function = "mqtt"
		}

		var shared *sharedConnection
		if cfg.Connection != "" {
			if deps.Connections == nil {
				return nil, fmt.Errorf("read group %s: connection %s requested but connection sharing not configured", cfg.ID, cfg.Connection)
			}
			handle, err := deps.Connections.Connection(cfg.Connection)
			if err != nil {
				return nil, fmt.Errorf("read group %s: %w", cfg.ID, err)
			}
			var ok bool
			shared, ok = handle.(*sharedConnection)
			if !ok {
				return nil, fmt.Errorf("read group %s: connection %s: incompatible handle type %T", cfg.ID, cfg.Connection, handle)
			}
			settings.Connection = shared.Settings()
		}

		group := &mqttReadGroup{
			id:            cfg.ID,
			function:      function,
			source:        cfg.Source,
			settings:      settings,
			bufferStatus:  make(map[string]readers.SignalBufferStatus),
			subscriptions: make([]*readSubscription, 0, len(settings.Subscriptions)),
			logger:        zerolog.Nop(),
		}

		for _, subCfg := range settings.Subscriptions {
			cell, err := deps.Cells.Get(subCfg.Cell)
			if err != nil {
				return nil, fmt.Errorf("read group %s: resolve cell %s: %w", cfg.ID, subCfg.Cell, err)
			}
			var buffer *readers.SignalBuffer
			if deps.Buffers != nil {
				buffer, err = deps.Buffers.Get(subCfg.Cell)
				if err != nil {
					return nil, err
				}
				if expected := expectedBufferCapacity(settings, subCfg); expected > 0 && buffer.Capacity() != expected {
					return nil, fmt.Errorf("read group %s signal %s: buffer capacity %d does not match expected %d", cfg.ID, subCfg.Cell, buffer.Capacity(), expected)
				}
				group.bufferStatus[subCfg.Cell] = buffer.Status()
			}
			subscription := &readSubscription{
				cellID:     subCfg.Cell,
				topic:      subCfg.Topic,
				qos:        settings.ResolveQoS(subCfg),
				conversion: settings.ResolvePayload(subCfg),
				cell:       cell,
				buffer:     buffer,
			}
			if subCfg.HomeAssistant != nil {
				haPublisher, err := newHomeAssistantPublisher(subCfg.HomeAssistant, settings.Connection, subCfg.Topic, homeAssistantTopicDefaults{
					command: subCfg.Topic,
				})
				if err != nil {
					return nil, fmt.Errorf("read group %s signal %s: %w", cfg.ID, subCfg.Cell, err)
				}
				subscription.ha = haPublisher
				if haPublisher != nil {
					subscription.commandTopic = haPublisher.CommandTopic()
				}
			}
			subscription.handler = group.makeHandler(subscription)
			group.subscriptions = append(group.subscriptions, subscription)
		}

		if shared != nil {
			client := shared.Client()
			if client == nil {
				return nil, fmt.Errorf("read group %s: connection %s returned nil client", cfg.ID, cfg.Connection)
			}
			group.client = client
			group.sharedConn = shared
			group.onConnectCancel = shared.AddOnConnect(group.onConnect)
		} else {
			client, err := buildClient(settings.Connection, group.logger, group.onConnect)
			if err != nil {
				return nil, fmt.Errorf("read group %s: %w", cfg.ID, err)
			}
			group.client = client
			group.ownsClient = true
		}

		return group, nil
	}
}

type readSubscription struct {
	cellID       string
	topic        string
	qos          byte
	conversion   PayloadConversion
	cell         state.Cell
	buffer       *readers.SignalBuffer
	handler      mqtt.MessageHandler
	ha           *homeAssistantPublisher
	commandTopic string
}

type mqttReadGroup struct {
	id       string
	function string
	source   config.ModuleReference

	settings ReadSettings
	client   mqtt.Client

	sharedConn      *sharedConnection
	onConnectCancel func()
	ownsClient      bool

	subscriptions []*readSubscription

	bufferStatus map[string]readers.SignalBufferStatus
	lastMessage  time.Time

	disabled atomic.Bool
	runOnce  atomic.Bool
	logger   zerolog.Logger

	mu sync.RWMutex
}

func (g *mqttReadGroup) ID() string { return g.id }

func (g *mqttReadGroup) Due(now time.Time) bool {
	if g.disabled.Load() {
		return false
	}
	return !g.runOnce.Load()
}

func (g *mqttReadGroup) Perform(now time.Time, logger zerolog.Logger) int {
	if g.disabled.Load() {
		return 0
	}
	g.logger = logger
	g.runOnce.Store(true)
	g.mu.Lock()
	defer g.mu.Unlock()
	g.bufferStatus = g.snapshotBufferStatus()
	return 0
}

func (g *mqttReadGroup) SetDisabled(disabled bool) {
	g.disabled.Store(disabled)
	if g.client == nil || !g.client.IsConnected() {
		return
	}
	for _, sub := range g.subscriptions {
		if sub.ha != nil {
			sub.ha.PublishAvailability(g.client, g.logger, !disabled)
		}
	}
}

func (g *mqttReadGroup) Status() readers.ReadGroupStatus {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return readers.ReadGroupStatus{
		ID:       g.id,
		Function: g.function,
		Disabled: g.disabled.Load(),
		LastRun:  g.lastMessage,
		Source:   g.source,
		Buffers:  cloneBufferStatus(g.bufferStatus),
	}
}

func (g *mqttReadGroup) Close() {
	if g.onConnectCancel != nil {
		g.onConnectCancel()
		g.onConnectCancel = nil
	}
	if g.client == nil {
		return
	}
	if g.client.IsConnected() {
		for _, sub := range g.subscriptions {
			if sub.ha != nil {
				sub.ha.PublishAvailability(g.client, g.logger, false)
			}
		}
	}
	if g.ownsClient && g.client.IsConnected() {
		g.client.Disconnect(250)
	}
}

func (g *mqttReadGroup) onConnect(client mqtt.Client) {
	for _, sub := range g.subscriptions {
		token := client.Subscribe(sub.topic, sub.qos, sub.handler)
		if token.Wait() && token.Error() != nil {
			g.logger.Error().Err(token.Error()).Str("topic", sub.topic).Msg("mqtt: subscribe failed")
		} else {
			g.logger.Info().Str("topic", sub.topic).Msg("mqtt: subscribed")
		}
		if sub.ha != nil {
			if sub.commandTopic != "" && sub.commandTopic != sub.topic {
				token := client.Subscribe(sub.commandTopic, sub.qos, sub.handler)
				if token.Wait() && token.Error() != nil {
					g.logger.Error().Err(token.Error()).Str("topic", sub.commandTopic).Msg("mqtt: subscribe failed")
				} else {
					g.logger.Info().Str("topic", sub.commandTopic).Msg("mqtt: subscribed")
				}
			}
			sub.ha.Ensure(client, g.logger)
			sub.ha.PublishAvailability(client, g.logger, true)
		}
	}
}

func (g *mqttReadGroup) makeHandler(sub *readSubscription) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		if g.disabled.Load() {
			return
		}
		if sub.ha != nil {
			sub.ha.Ensure(client, g.logger)
		}
		value, err := DecodePayload(sub.conversion, msg.Payload())
		if err != nil {
			g.logger.Error().Err(err).Str("topic", msg.Topic()).Msg("mqtt: decode failed")
			return
		}
		ts := time.Now()
		if err := sub.cell.SetValue(value, ts, nil); err != nil {
			g.logger.Error().Err(err).Str("cell", sub.cellID).Msg("mqtt: update failed")
		}
		if sub.buffer != nil {
			if err := sub.buffer.Push(ts, value, nil); err != nil {
				if errors.Is(err, readers.ErrSignalBufferOverflow) {
					g.logger.Warn().Str("cell", sub.cellID).Msg("mqtt: signal buffer overflow")
				} else {
					g.logger.Error().Err(err).Str("cell", sub.cellID).Msg("mqtt: buffer push failed")
				}
			}
		}
		g.mu.Lock()
		g.lastMessage = ts
		if sub.buffer != nil {
			g.bufferStatus[sub.cellID] = sub.buffer.Status()
		}
		g.mu.Unlock()
		if sub.ha != nil {
			sub.ha.PublishAvailability(client, g.logger, true)
			sub.ha.PublishMirroredState(client, g.logger, msg.Payload(), msg.Qos(), msg.Retained())
		}
	}
}

func (g *mqttReadGroup) snapshotBufferStatus() map[string]readers.SignalBufferStatus {
	status := make(map[string]readers.SignalBufferStatus, len(g.subscriptions))
	for _, sub := range g.subscriptions {
		if sub.buffer != nil {
			status[sub.cellID] = sub.buffer.Status()
		}
	}
	return status
}

func cloneBufferStatus(src map[string]readers.SignalBufferStatus) map[string]readers.SignalBufferStatus {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]readers.SignalBufferStatus, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func expectedBufferCapacity(settings ReadSettings, sub ReadSubscription) int {
	if sub.Buffer != nil && sub.Buffer.Capacity != nil {
		return *sub.Buffer.Capacity
	}
	if settings.BufferDefaults != nil && settings.BufferDefaults.Capacity != nil {
		return *settings.BufferDefaults.Capacity
	}
	return 0
}
