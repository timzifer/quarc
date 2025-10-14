# Telemetry Interfaces

The `telemetry` package defines the lightweight `Collector` interface that the
service uses to emit operational metrics.

## Usage

```go
collector, err := telemetry.NewPrometheusCollector(prometheus.DefaultRegisterer)
if err != nil {
    return err
}
collector.IncHotReload("config/main.cue")
```

## Implementation Notes

* `Collector` methods are called inline with runtime operations such as hot
  reload events. Keep implementations fast and avoid blocking on network calls.
* When exposing Prometheus metrics, guard access to shared collectors to prevent
  duplicate registration panics during configuration reloads.

## Common Pitfalls

* Returning a `nil` collector from factory code will panic when the service tries
  to record metrics.
* Forgetting to label counters with the file name obscures which configuration
  source triggered reload activity.
