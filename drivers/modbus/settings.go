package modbus

import (
	"encoding/json"
	"fmt"

	"github.com/timzifer/quarc/config"
)

type readGroupOverrides struct {
	Function   string  `json:"function"`
	Start      *uint16 `json:"start"`
	Length     *uint16 `json:"length"`
	MaxGapSize *uint16 `json:"max_gap_size"`
	Legacy     bool    `json:"legacy,omitempty"`
}

type readGroupPlan struct {
	Function   string
	Start      uint16
	Length     uint16
	MaxGapSize uint16
	Legacy     bool
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

func resolveReadGroup(cfg config.ReadGroupConfig) (config.ReadGroupConfig, readGroupPlan, error) {
	resolved := cfg
	plan := readGroupPlan{}
	functionSet := false
	startSet := false
	lengthSet := false
	gapSet := false

	if len(cfg.Driver.Settings) > 0 {
		var overrides readGroupOverrides
		if err := decodeDriverSettings(cfg.Driver.Settings, &overrides); err != nil {
			return config.ReadGroupConfig{}, readGroupPlan{}, fmt.Errorf("read group %s: decode driver settings: %w", cfg.ID, err)
		}
		if overrides.Function != "" {
			plan.Function = overrides.Function
			functionSet = true
		}
		if overrides.Start != nil {
			plan.Start = *overrides.Start
			startSet = true
		}
		if overrides.Length != nil {
			plan.Length = *overrides.Length
			lengthSet = true
		}
		if overrides.MaxGapSize != nil {
			plan.MaxGapSize = *overrides.MaxGapSize
			gapSet = true
		}
	}

	if len(cfg.DriverMetadata) > 0 {
		var metadata readGroupOverrides
		if err := json.Unmarshal(cfg.DriverMetadata, &metadata); err != nil {
			return config.ReadGroupConfig{}, readGroupPlan{}, fmt.Errorf("read group %s: decode driver metadata: %w", cfg.ID, err)
		}
		if metadata.Function != "" && !functionSet {
			plan.Function = metadata.Function
			functionSet = true
		}
		if metadata.Start != nil && !startSet {
			plan.Start = *metadata.Start
			startSet = true
		}
		if metadata.Length != nil && !lengthSet {
			plan.Length = *metadata.Length
			lengthSet = true
		}
		if metadata.MaxGapSize != nil && !gapSet {
			plan.MaxGapSize = *metadata.MaxGapSize
			gapSet = true
		}
		if metadata.Legacy {
			plan.Legacy = true
		}
	}

	if !functionSet && cfg.LegacyFunction != "" {
		plan.Function = cfg.LegacyFunction
		functionSet = true
		plan.Legacy = true
	}
	if !startSet && cfg.LegacyStart != nil {
		plan.Start = *cfg.LegacyStart
		startSet = true
		plan.Legacy = true
	}
	if !lengthSet && cfg.LegacyLength != nil {
		plan.Length = *cfg.LegacyLength
		lengthSet = true
		plan.Legacy = true
	}

	if !functionSet {
		return config.ReadGroupConfig{}, readGroupPlan{}, fmt.Errorf("read group %s: driver settings missing function", cfg.ID)
	}
	if !startSet {
		return config.ReadGroupConfig{}, readGroupPlan{}, fmt.Errorf("read group %s: driver settings missing start", cfg.ID)
	}
	if !lengthSet {
		return config.ReadGroupConfig{}, readGroupPlan{}, fmt.Errorf("read group %s: driver settings missing length", cfg.ID)
	}

	metadata := make(map[string]interface{})
	if len(cfg.DriverMetadata) > 0 {
		_ = json.Unmarshal(cfg.DriverMetadata, &metadata)
	}
	if !gapSet {
		plan.MaxGapSize = plan.Length
	}

	metadata["function"] = plan.Function
	metadata["start"] = plan.Start
	metadata["length"] = plan.Length
	metadata["max_gap_size"] = plan.MaxGapSize
	if plan.Legacy {
		metadata["legacy"] = true
	} else {
		delete(metadata, "legacy")
	}
	if raw, err := json.Marshal(metadata); err == nil {
		resolved.DriverMetadata = raw
	}

	return resolved, plan, nil
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
