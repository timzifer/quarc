package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	errUnsupportedEncoding = errors.New("mqtt: unsupported payload encoding")
	errUnsupportedType     = errors.New("mqtt: unsupported payload value type")
)

// DecodePayload decodes the raw MQTT payload into a Go value using the conversion settings.
func DecodePayload(cfg PayloadConversion, payload []byte) (any, error) {
	switch strings.ToLower(cfg.Encoding) {
	case "json", "":
		return decodeJSON(cfg, payload)
	case "string":
		return string(payload), nil
	case "bytes", "binary":
		return payload, nil
	default:
		return nil, errUnsupportedEncoding
	}
}

func decodeJSON(cfg PayloadConversion, payload []byte) (any, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	var value any
	if err := json.Unmarshal(payload, &value); err != nil {
		// Attempt primitive coercions when the payload is not valid JSON.
		switch strings.ToLower(cfg.ValueType) {
		case "float", "double", "number":
			f, ferr := strconv.ParseFloat(string(payload), 64)
			if ferr != nil {
				return nil, ferr
			}
			return f, nil
		case "int", "integer":
			i, ierr := strconv.ParseInt(string(payload), 10, 64)
			if ierr != nil {
				return nil, ierr
			}
			return i, nil
		case "bool", "boolean":
			b, berr := strconv.ParseBool(strings.TrimSpace(string(payload)))
			if berr != nil {
				return nil, berr
			}
			return b, nil
		case "string", "text":
			return string(payload), nil
		default:
			return nil, fmt.Errorf("%w: %q", errUnsupportedType, cfg.ValueType)
		}
	}

	if cfg.Path != "" {
		current := value
		for _, segment := range strings.Split(cfg.Path, ".") {
			m, ok := current.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("mqtt: path %s not present", cfg.Path)
			}
			current = m[segment]
		}
		value = current
	}

	return coerceType(cfg.ValueType, value)
}

func coerceType(kind string, value any) (any, error) {
	switch strings.ToLower(kind) {
	case "", "any":
		return value, nil
	case "float", "double", "number":
		switch v := value.(type) {
		case float64:
			return v, nil
		case json.Number:
			return v.Float64()
		case string:
			return strconv.ParseFloat(v, 64)
		case int:
			return float64(v), nil
		case int64:
			return float64(v), nil
		}
		return nil, fmt.Errorf("mqtt: cannot convert %T to float", value)
	case "int", "integer":
		switch v := value.(type) {
		case float64:
			return int64(v), nil
		case json.Number:
			return v.Int64()
		case string:
			return strconv.ParseInt(v, 10, 64)
		case int:
			return int64(v), nil
		case int64:
			return v, nil
		}
		return nil, fmt.Errorf("mqtt: cannot convert %T to integer", value)
	case "bool", "boolean":
		switch v := value.(type) {
		case bool:
			return v, nil
		case string:
			return strconv.ParseBool(strings.TrimSpace(v))
		case float64:
			return v != 0, nil
		}
		return nil, fmt.Errorf("mqtt: cannot convert %T to boolean", value)
	case "string", "text":
		return fmt.Sprint(value), nil
	default:
		return nil, fmt.Errorf("mqtt: unknown value type %s", kind)
	}
}

// EncodePayload encodes a Go value to bytes for outbound MQTT messages.
func EncodePayload(cfg PayloadConversion, value any) ([]byte, error) {
	switch strings.ToLower(cfg.Encoding) {
	case "json", "":
		if cfg.ValueType == "string" {
			return json.Marshal(fmt.Sprint(value))
		}
		return json.Marshal(value)
	case "string":
		return []byte(fmt.Sprint(value)), nil
	case "bytes", "binary":
		if b, ok := value.([]byte); ok {
			return b, nil
		}
		return nil, fmt.Errorf("mqtt: value is not []byte, got %T", value)
	default:
		return nil, errUnsupportedEncoding
	}
}

// ShouldPublish evaluates whether a new value should be published based on
// deadband and rate limit constraints.
func ShouldPublish(deadband *Deadband, rateLimit *RateLimit, last *CellUpdate, next CellUpdate) bool {
	if last == nil {
		return true
	}

	if rateLimit != nil && rateLimit.MinInterval.Duration > 0 {
		if next.Timestamp.Sub(last.Timestamp) < rateLimit.MinInterval.Duration {
			return false
		}
	}

	if deadband == nil {
		return true
	}

	lastFloat, lastOK := toFloat(last.Value)
	nextFloat, nextOK := toFloat(next.Value)
	if !lastOK || !nextOK {
		return next.Value != last.Value
	}

	if deadband.Absolute != nil {
		diff := nextFloat - lastFloat
		if diff < 0 {
			diff = -diff
		}
		if diff < *deadband.Absolute {
			return false
		}
	}

	if deadband.Percent != nil && lastFloat != 0 {
		diff := (nextFloat - lastFloat) / lastFloat
		if diff < 0 {
			diff = -diff
		}
		if diff*100 < *deadband.Percent {
			return false
		}
	}

	return true
}

func toFloat(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint64:
		return float64(v), true
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}
