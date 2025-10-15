package telemetry

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Collector captures telemetry events emitted by the runtime.
//
// Implementations may forward metrics to Prometheus, loggers or other
// monitoring systems. They should be inexpensive to call because hooks are
// executed inline with critical paths such as configuration reloads.
type Collector interface {
	IncHotReload(file string)
	IncBufferDropped(group, buffer string, count uint64)
	SetBufferOccupancy(group, buffer string, occupancy int)
}

type noopCollector struct{}

// Noop returns a collector that discards all metrics.
func Noop() Collector {
	return noopCollector{}
}

func (noopCollector) IncHotReload(string)                     {}
func (noopCollector) IncBufferDropped(string, string, uint64) {}
func (noopCollector) SetBufferOccupancy(string, string, int)  {}

// PrometheusCollector exposes telemetry counters via Prometheus.
type PrometheusCollector struct {
	hotReloads      *prometheus.CounterVec
	bufferDropped   *prometheus.CounterVec
	bufferOccupancy *prometheus.GaugeVec
}

var (
	hotReloadCounter         *prometheus.CounterVec
	hotReloadCounterLock     sync.Mutex
	bufferDropCounter        *prometheus.CounterVec
	bufferDropCounterLock    sync.Mutex
	bufferOccupancyGauge     *prometheus.GaugeVec
	bufferOccupancyGaugeLock sync.Mutex
)

// NewPrometheusCollector registers the required metrics with the provided registerer.
func NewPrometheusCollector(reg prometheus.Registerer) (*PrometheusCollector, error) {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	hotReloadCounterLock.Lock()
	if hotReloadCounter == nil {
		counter := prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "modbus_processor_config_hot_reload_total",
			Help: "Number of hot reload operations triggered per configuration source file.",
		}, []string{"file"})
		if err := reg.Register(counter); err != nil {
			if already, ok := err.(prometheus.AlreadyRegisteredError); ok {
				if existing, ok := already.ExistingCollector.(*prometheus.CounterVec); ok {
					hotReloadCounter = existing
				} else {
					return nil, err
				}
			} else {
				return nil, err
			}
		} else {
			hotReloadCounter = counter
		}
	}
	hotReloadCounterLock.Unlock()

	bufferDropCounterLock.Lock()
	if bufferDropCounter == nil {
		counter := prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "modbus_processor_read_buffer_dropped_total",
			Help: "Number of samples dropped from read buffers due to capacity limits.",
		}, []string{"group", "buffer"})
		if err := reg.Register(counter); err != nil {
			if already, ok := err.(prometheus.AlreadyRegisteredError); ok {
				if existing, ok := already.ExistingCollector.(*prometheus.CounterVec); ok {
					bufferDropCounter = existing
				} else {
					bufferDropCounterLock.Unlock()
					return nil, err
				}
			} else {
				bufferDropCounterLock.Unlock()
				return nil, err
			}
		} else {
			bufferDropCounter = counter
		}
	}
	bufferDropCounterLock.Unlock()

	bufferOccupancyGaugeLock.Lock()
	if bufferOccupancyGauge == nil {
		gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "modbus_processor_read_buffer_occupancy",
			Help: "Number of samples observed in read buffers during the last flush.",
		}, []string{"group", "buffer"})
		if err := reg.Register(gauge); err != nil {
			if already, ok := err.(prometheus.AlreadyRegisteredError); ok {
				if existing, ok := already.ExistingCollector.(*prometheus.GaugeVec); ok {
					bufferOccupancyGauge = existing
				} else {
					bufferOccupancyGaugeLock.Unlock()
					return nil, err
				}
			} else {
				bufferOccupancyGaugeLock.Unlock()
				return nil, err
			}
		} else {
			bufferOccupancyGauge = gauge
		}
	}
	bufferOccupancyGaugeLock.Unlock()

	return &PrometheusCollector{
		hotReloads:      hotReloadCounter,
		bufferDropped:   bufferDropCounter,
		bufferOccupancy: bufferOccupancyGauge,
	}, nil
}

// IncHotReload increments the counter for the provided file path.
func (p *PrometheusCollector) IncHotReload(file string) {
	if p == nil || p.hotReloads == nil {
		return
	}
	p.hotReloads.WithLabelValues(file).Inc()
}

// IncBufferDropped records dropped samples for a buffer.
func (p *PrometheusCollector) IncBufferDropped(group, buffer string, count uint64) {
	if p == nil || p.bufferDropped == nil || count == 0 {
		return
	}
	p.bufferDropped.WithLabelValues(group, buffer).Add(float64(count))
}

// SetBufferOccupancy updates the gauge tracking buffered samples per flush.
func (p *PrometheusCollector) SetBufferOccupancy(group, buffer string, occupancy int) {
	if p == nil || p.bufferOccupancy == nil {
		return
	}
	p.bufferOccupancy.WithLabelValues(group, buffer).Set(float64(occupancy))
}
