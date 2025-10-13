package programs

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/timzifer/quarc/config"
)

type Signal struct {
	ID        string
	Kind      config.ValueKind
	Value     interface{}
	Valid     bool
	Timestamp time.Time
}

type Context struct {
	Now   time.Time
	Delta time.Duration
}

// Program defines the contract implemented by runtime control modules.
//
// Programs receive the current execution context and a snapshot of input
// signals, and can emit zero or more output signals. Implementations should be
// deterministic, avoid long blocking operations, and be safe for repeated calls
// with varying input.
type Program interface {
	ID() string
	Execute(ctx Context, incoming []Signal) ([]Signal, error)
}

// Factory creates program instances from configuration data.
//
// Factories are registered under a stable identifier so the service can create
// the required program for each module during startup or configuration reloads.
type Factory func(instanceID string, settings map[string]interface{}) (Program, error)

var (
	registryMu sync.RWMutex
	registry   = make(map[string]Factory)
)

func RegisterProgram(id string, factory Factory) {
	if id == "" {
		panic("program id must not be empty")
	}
	if factory == nil {
		panic("program factory must not be nil")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := registry[id]; exists {
		panic(fmt.Sprintf("program factory for %s already registered", id))
	}
	registry[id] = factory
}

func Instantiate(id, instanceID string, settings map[string]interface{}) (Program, error) {
	registryMu.RLock()
	factory, ok := registry[id]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("program type %s not registered", id)
	}
	return factory(instanceID, settings)
}

func RegisteredIDs() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	ids := make([]string, 0, len(registry))
	for id := range registry {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func MapSignals(signals []Signal) map[string]Signal {
	if len(signals) == 0 {
		return nil
	}
	out := make(map[string]Signal, len(signals))
	for _, sig := range signals {
		out[sig.ID] = sig
	}
	return out
}

func (s Signal) AsNumber() (float64, bool) {
	if !s.Valid {
		return 0, false
	}
	switch v := s.Value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	case int16:
		return float64(v), true
	case int8:
		return float64(v), true
	case uint64:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint8:
		return float64(v), true
	case bool:
		if v {
			return 1, true
		}
		return 0, true
	default:
		return 0, false
	}
}

func (s Signal) AsBool() (bool, bool) {
	if !s.Valid {
		return false, false
	}
	switch v := s.Value.(type) {
	case bool:
		return v, true
	case float64:
		return v != 0, true
	case float32:
		return v != 0, true
	case int:
		return v != 0, true
	case int64:
		return v != 0, true
	case int32:
		return v != 0, true
	case int16:
		return v != 0, true
	case int8:
		return v != 0, true
	case uint8:
		return v != 0, true
	default:
		return false, false
	}
}

func NewNumberSignal(id string, value float64) Signal {
	return Signal{ID: id, Kind: config.ValueKindNumber, Value: value, Valid: true}
}

func NewBoolSignal(id string, value bool) Signal {
	return Signal{ID: id, Kind: config.ValueKindBool, Value: value, Valid: true}
}

func NewStringSignal(id, value string) Signal {
	return Signal{ID: id, Kind: config.ValueKindString, Value: value, Valid: true}
}

func InvalidSignal(id string, kind config.ValueKind) Signal {
	return Signal{ID: id, Kind: kind, Valid: false}
}
