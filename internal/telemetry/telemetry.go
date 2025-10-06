package telemetry

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Collector captures telemetry events emitted by the runtime.
type Collector interface {
	IncHotReload(file string)
}

type noopCollector struct{}

// Noop returns a collector that discards all metrics.
func Noop() Collector {
	return noopCollector{}
}

func (noopCollector) IncHotReload(string) {}

// PrometheusCollector exposes telemetry counters via Prometheus.
type PrometheusCollector struct {
	hotReloads *prometheus.CounterVec
}

var (
	hotReloadCounter     *prometheus.CounterVec
	hotReloadCounterLock sync.Mutex
)

// NewPrometheusCollector registers the required metrics with the provided registerer.
func NewPrometheusCollector(reg prometheus.Registerer) (*PrometheusCollector, error) {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	hotReloadCounterLock.Lock()
	defer hotReloadCounterLock.Unlock()
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
	return &PrometheusCollector{hotReloads: hotReloadCounter}, nil
}

// IncHotReload increments the counter for the provided file path.
func (p *PrometheusCollector) IncHotReload(file string) {
	if p == nil || p.hotReloads == nil {
		return
	}
	p.hotReloads.WithLabelValues(file).Inc()
}
