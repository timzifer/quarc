package modbus

import (
	"encoding/json"
	"fmt"

	"github.com/timzifer/quarc/config"
)

type readGroupOverrides struct {
	Function string  `json:"function"`
	Start    *uint16 `json:"start"`
	Length   *uint16 `json:"length"`
}

type writeTargetOverrides struct {
	Function   string           `json:"function"`
	Address    *uint16          `json:"address"`
	Endianness string           `json:"endianness"`
	Signed     *bool            `json:"signed"`
	Deadband   *float64         `json:"deadband"`
	RateLimit  *config.Duration `json:"rate_limit"`
	Scale      *float64         `json:"scale"`
}

func resolveReadGroup(cfg config.ReadGroupConfig) (config.ReadGroupConfig, error) {
	resolved := cfg
	if len(cfg.Driver.Settings) > 0 {
		var overrides readGroupOverrides
		if err := decodeDriverSettings(cfg.Driver.Settings, &overrides); err != nil {
			return config.ReadGroupConfig{}, fmt.Errorf("read group %s: decode driver settings: %w", cfg.ID, err)
		}
		if overrides.Function != "" {
			resolved.Function = overrides.Function
		}
		if overrides.Start != nil {
			resolved.Start = *overrides.Start
		}
		if overrides.Length != nil {
			resolved.Length = *overrides.Length
		}
	}
	return resolved, nil
}

func resolveWriteTarget(cfg config.WriteTargetConfig) (config.WriteTargetConfig, error) {
	resolved := cfg
	if len(cfg.Driver.Settings) > 0 {
		var overrides writeTargetOverrides
		if err := decodeDriverSettings(cfg.Driver.Settings, &overrides); err != nil {
			return config.WriteTargetConfig{}, fmt.Errorf("write target %s: decode driver settings: %w", cfg.ID, err)
		}
		if overrides.Function != "" {
			resolved.Function = overrides.Function
		}
		if overrides.Address != nil {
			resolved.Address = *overrides.Address
		}
		if overrides.Endianness != "" {
			resolved.Endianness = overrides.Endianness
		}
		if overrides.Signed != nil {
			resolved.Signed = *overrides.Signed
		}
		if overrides.Deadband != nil {
			resolved.Deadband = *overrides.Deadband
		}
		if overrides.RateLimit != nil {
			resolved.RateLimit = *overrides.RateLimit
		}
		if overrides.Scale != nil {
			resolved.Scale = *overrides.Scale
		}
	}
	return resolved, nil
}

func decodeDriverSettings(raw json.RawMessage, target any) error {
	if len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, target)
}
