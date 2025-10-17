package mqtt

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"

	"github.com/timzifer/quarc/config"
	"github.com/timzifer/quarc/runtime/connections"
)

type sharedConnection struct {
	client   mqtt.Client
	settings ConnectionSettings

	mu       sync.RWMutex
	handlers map[uint64]mqtt.OnConnectHandler
	nextID   atomic.Uint64
}

func newSharedConnection(settings ConnectionSettings) (*sharedConnection, error) {
	handle := &sharedConnection{
		settings: settings,
		handlers: make(map[uint64]mqtt.OnConnectHandler),
	}
	client, err := buildClient(settings, zerolog.Nop(), handle.handleOnConnect)
	if err != nil {
		return nil, err
	}
	handle.client = client
	return handle, nil
}

func (h *sharedConnection) handleOnConnect(client mqtt.Client) {
	h.mu.RLock()
	handlers := make([]mqtt.OnConnectHandler, 0, len(h.handlers))
	for _, handler := range h.handlers {
		if handler != nil {
			handlers = append(handlers, handler)
		}
	}
	h.mu.RUnlock()
	for _, handler := range handlers {
		handler(client)
	}
}

func (h *sharedConnection) AddOnConnect(handler mqtt.OnConnectHandler) func() {
	if handler == nil {
		return func() {}
	}
	id := h.nextID.Add(1) - 1
	h.mu.Lock()
	h.handlers[id] = handler
	h.mu.Unlock()
	if h.client != nil && h.client.IsConnected() {
		handler(h.client)
	}
	return func() {
		h.mu.Lock()
		delete(h.handlers, id)
		h.mu.Unlock()
	}
}

func (h *sharedConnection) Client() mqtt.Client {
	if h == nil {
		return nil
	}
	return h.client
}

func (h *sharedConnection) Settings() ConnectionSettings {
	if h == nil {
		return ConnectionSettings{}
	}
	return h.settings
}

func (h *sharedConnection) Close() error {
	if h == nil {
		return nil
	}
	if h.client != nil && h.client.IsConnected() {
		h.client.Disconnect(250)
	}
	return nil
}

// NewConnectionFactory returns a connection factory that can be registered with
// service.WithConnectionFactory to enable shared MQTT connections across reads
// and writes.
func NewConnectionFactory() connections.Factory {
	return func(cfg config.IOConnectionConfig) (connections.Handle, error) {
		if len(cfg.Driver.Settings) == 0 {
			return nil, fmt.Errorf("mqtt: connection %s: driver settings missing", cfg.ID)
		}
		var settings ConnectionSettings
		if err := json.Unmarshal(cfg.Driver.Settings, &settings); err != nil {
			return nil, fmt.Errorf("mqtt: connection %s: decode driver settings: %w", cfg.ID, err)
		}
		if settings.Broker == "" {
			return nil, fmt.Errorf("mqtt: connection %s: connection.broker is required", cfg.ID)
		}
		return newSharedConnection(settings)
	}
}
