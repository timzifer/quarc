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
	collector.IncBufferDropped("group", "cell", 5)
	collector.SetBufferOccupancy("group", "cell", 3)
}

func TestPrometheusCollectorRegistersAndReusesCounter(t *testing.T) {
	hotReloadCounterLock.Lock()
	hotReloadCounter = nil
	hotReloadCounterLock.Unlock()
	bufferDropCounterLock.Lock()
	bufferDropCounter = nil
	bufferDropCounterLock.Unlock()
	bufferOccupancyGaugeLock.Lock()
	bufferOccupancyGauge = nil
	bufferOccupancyGaugeLock.Unlock()

	reg := prometheus.NewRegistry()
	collector, err := NewPrometheusCollector(reg)
	require.NoError(t, err)
	require.NotNil(t, collector)

	collector.IncHotReload("a.cue")
	collector.IncBufferDropped("grp", "cell", 3)
	collector.SetBufferOccupancy("grp", "cell", 4)

	metrics, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, metrics, 3)

	mf := metricsByName(metrics)
	requireCounterValue(t, mf["modbus_processor_config_hot_reload_total"], 1)
	requireCounterValue(t, mf["modbus_processor_read_buffer_dropped_total"], 3)
	requireGaugeValue(t, mf["modbus_processor_read_buffer_occupancy"], 4)

	again, err := NewPrometheusCollector(reg)
	require.NoError(t, err)
	require.Same(t, collector.hotReloads, again.hotReloads)
	require.Same(t, collector.bufferDropped, again.bufferDropped)
	require.Same(t, collector.bufferOccupancy, again.bufferOccupancy)

	again.IncHotReload("a.cue")
	again.IncBufferDropped("grp", "cell", 2)
	again.SetBufferOccupancy("grp", "cell", 6)

	metrics, err = reg.Gather()
	require.NoError(t, err)
	mf = metricsByName(metrics)
	requireCounterValue(t, mf["modbus_processor_config_hot_reload_total"], 2)
	requireCounterValue(t, mf["modbus_processor_read_buffer_dropped_total"], 5)
	requireGaugeValue(t, mf["modbus_processor_read_buffer_occupancy"], 6)
}

func requireCounterValue(t *testing.T, mf *dto.MetricFamily, value float64) {
	t.Helper()
	require.Len(t, mf.Metric, 1)
	require.NotNil(t, mf.Metric[0].Counter)
	require.Equal(t, value, mf.Metric[0].Counter.GetValue())
}

func requireGaugeValue(t *testing.T, mf *dto.MetricFamily, value float64) {
	t.Helper()
	require.Len(t, mf.Metric, 1)
	require.NotNil(t, mf.Metric[0].Gauge)
	require.Equal(t, value, mf.Metric[0].Gauge.GetValue())
}

func metricsByName(metrics []*dto.MetricFamily) map[string]*dto.MetricFamily {
	result := make(map[string]*dto.MetricFamily, len(metrics))
	for _, mf := range metrics {
		if mf == nil {
			continue
		}
		result[mf.GetName()] = mf
	}
	return result
}
