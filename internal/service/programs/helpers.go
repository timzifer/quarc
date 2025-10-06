package programs

import (
	"fmt"
	"time"
)

func getFloat(settings map[string]interface{}, key string, def float64) (float64, error) {
	if settings == nil {
		return def, nil
	}
	raw, ok := settings[key]
	if !ok {
		return def, nil
	}
	switch v := raw.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case string:
		return 0, fmt.Errorf("setting %s must be numeric, got string", key)
	default:
		return 0, fmt.Errorf("setting %s has unsupported type %T", key, raw)
	}
}

func getBoolSetting(settings map[string]interface{}, key string, def bool) (bool, error) {
	if settings == nil {
		return def, nil
	}
	raw, ok := settings[key]
	if !ok {
		return def, nil
	}
	switch v := raw.(type) {
	case bool:
		return v, nil
	case float64:
		return v != 0, nil
	case int:
		return v != 0, nil
	default:
		return false, fmt.Errorf("setting %s has unsupported type %T", key, raw)
	}
}

func getDurationSetting(settings map[string]interface{}, key string, def time.Duration) (time.Duration, error) {
	if settings == nil {
		return def, nil
	}
	raw, ok := settings[key]
	if !ok {
		return def, nil
	}
	switch v := raw.(type) {
	case string:
		dur, err := time.ParseDuration(v)
		if err != nil {
			return 0, fmt.Errorf("parse duration for %s: %w", key, err)
		}
		return dur, nil
	case float64:
		return time.Duration(v * float64(time.Second)), nil
	case int:
		return time.Duration(v) * time.Second, nil
	default:
		return 0, fmt.Errorf("setting %s has unsupported type %T", key, raw)
	}
}

func asStringSlice(raw interface{}) ([]string, error) {
	if raw == nil {
		return nil, nil
	}
	switch v := raw.(type) {
	case []interface{}:
		out := make([]string, len(v))
		for i, item := range v {
			str, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("expected string at index %d, got %T", i, item)
			}
			out[i] = str
		}
		return out, nil
	case []string:
		return v, nil
	default:
		return nil, fmt.Errorf("expected slice of strings, got %T", raw)
	}
}
