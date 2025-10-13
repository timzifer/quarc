package canstream

import (
	"fmt"

	"github.com/timzifer/modbus_processor/config"
)

func resolveCANConfig(cfg config.ReadGroupConfig) (*config.CANReadGroupConfig, error) {
	if cfg.DriverSettings != nil && cfg.DriverSettings.Kind != 0 {
		var override config.CANReadGroupConfig
		if err := cfg.DriverSettings.Decode(&override); err != nil {
			return nil, fmt.Errorf("read group %s: decode CAN driver settings: %w", cfg.ID, err)
		}
		return &override, nil
	}
	if cfg.CAN != nil {
		copy := *cfg.CAN
		return &copy, nil
	}
	return nil, fmt.Errorf("read group %s: missing CAN configuration", cfg.ID)
}
