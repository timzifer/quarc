package processor

import (
	"fmt"
	"strings"

	"github.com/timzifer/modbus_processor/internal/config"
	"github.com/timzifer/modbus_processor/telemetry"
)

func newTelemetryCollector(cfg config.TelemetryConfig) (telemetry.Collector, error) {
	if !cfg.Enabled {
		return telemetry.Noop(), nil
	}
	provider := strings.ToLower(strings.TrimSpace(cfg.Provider))
	switch provider {
	case "", "prometheus":
		collector, err := telemetry.NewPrometheusCollector(nil)
		if err != nil {
			return nil, err
		}
		return collector, nil
	default:
		return telemetry.Noop(), fmt.Errorf("unsupported telemetry provider %q", cfg.Provider)
	}
}
