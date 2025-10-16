package service

import (
	"errors"
	"fmt"
	"sync"

	"github.com/timzifer/quarc/config"
	"github.com/timzifer/quarc/runtime/connections"
)

type connectionManager struct {
	mu      sync.RWMutex
	handles map[string]connections.Handle
}

func newConnectionManager(cfgs []config.IOConnectionConfig, factories map[string]connections.Factory) (*connectionManager, error) {
	manager := &connectionManager{handles: make(map[string]connections.Handle, len(cfgs))}
	for _, connCfg := range cfgs {
		if connCfg.ID == "" {
			continue
		}
		factory := factories[connCfg.Driver]
		if factory == nil {
			manager.Close()
			return nil, fmt.Errorf("connection %s: no factory registered for driver %s", connCfg.ID, connCfg.Driver)
		}
		handle, err := factory(connCfg)
		if err != nil {
			manager.Close()
			return nil, fmt.Errorf("connection %s: %w", connCfg.ID, err)
		}
		manager.handles[connCfg.ID] = handle
	}
	return manager, nil
}

func (m *connectionManager) Connection(id string) (connections.Handle, error) {
	if m == nil {
		return nil, fmt.Errorf("connection %s: manager not initialised", id)
	}
	m.mu.RLock()
	handle, ok := m.handles[id]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("connection %s: not found", id)
	}
	return handle, nil
}

func (m *connectionManager) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var errs []error
	for id, handle := range m.handles {
		if handle == nil {
			continue
		}
		if err := handle.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close connection %s: %w", id, err))
		}
	}
	m.handles = nil
	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}
