package modbus

import (
	"fmt"

	"github.com/timzifer/modbus_processor/config"
	"gopkg.in/yaml.v3"
)

type readGroupOverrides struct {
	Function string  `yaml:"function"`
	Start    *uint16 `yaml:"start"`
	Length   *uint16 `yaml:"length"`
}

type writeTargetOverrides struct {
	Function   string           `yaml:"function"`
	Address    *uint16          `yaml:"address"`
	Endianness string           `yaml:"endianness"`
	Signed     *bool            `yaml:"signed"`
	Deadband   *float64         `yaml:"deadband"`
	RateLimit  *config.Duration `yaml:"rate_limit"`
	Scale      *float64         `yaml:"scale"`
}

func resolveReadGroup(cfg config.ReadGroupConfig) (config.ReadGroupConfig, error) {
	resolved := cfg
	if cfg.DriverSettings != nil && cfg.DriverSettings.Kind != 0 {
		var overrides readGroupOverrides
		if err := decodeNode(cfg.DriverSettings, &overrides); err != nil {
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
	if cfg.DriverSettings != nil && cfg.DriverSettings.Kind != 0 {
		var overrides writeTargetOverrides
		if err := decodeNode(cfg.DriverSettings, &overrides); err != nil {
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

func decodeNode(node *yaml.Node, target any) error {
	if node == nil {
		return nil
	}
	if node.Kind == 0 {
		return nil
	}
	return node.Decode(target)
}
