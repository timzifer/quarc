package telemetry

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestNoopCollector(t *testing.T) {
	collector := Noop()
	require.NotNil(t, collector)
	collector.IncHotReload("config.cue")
}

func TestPrometheusCollectorRegistersAndReusesCounter(t *testing.T) {
	hotReloadCounterLock.Lock()
	hotReloadCounter = nil
	hotReloadCounterLock.Unlock()

	reg := prometheus.NewRegistry()
	collector, err := NewPrometheusCollector(reg)
	require.NoError(t, err)
	require.NotNil(t, collector)

	collector.IncHotReload("a.cue")

	metrics, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, metrics, 1)

	metric := metrics[0]
	require.Equal(t, "modbus_processor_config_hot_reload_total", metric.GetName())
	requireCounterValue(t, metric, 1)

	again, err := NewPrometheusCollector(reg)
	require.NoError(t, err)
	require.Same(t, collector.hotReloads, again.hotReloads)

	again.IncHotReload("a.cue")

	metrics, err = reg.Gather()
	require.NoError(t, err)
	requireCounterValue(t, metrics[0], 2)
}

func requireCounterValue(t *testing.T, mf *dto.MetricFamily, value float64) {
	t.Helper()
	require.Len(t, mf.Metric, 1)
	require.NotNil(t, mf.Metric[0].Counter)
	require.Equal(t, value, mf.Metric[0].Counter.GetValue())
}
