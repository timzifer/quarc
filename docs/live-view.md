# Live View and Telemetry

The live view offers a lightweight web UI that visualises cycles, cells, read/write buffers, and program executions in real time. Tables are complemented by a heatmap that highlights recently active cells, logic blocks, and programs to simplify debugging and commissioning.

## Enabling the live view

* **CLI** – `quarc --live-view --live-view-listen=:18080` starts the HTTP server on port `18080`. Without `--live-view` the UI remains disabled.
* **Embedded/SDK** – Applications that start the processor programmatically can enable the server with `processor.WithLiveView(host, port)`.

## Heatmap configuration

The `live_view` section in the CUE configuration controls the heatmap. The sample [`config.example.cue`](../config.example.cue) file illustrates the default structure with `heatmap.cooldown` and `heatmap.colors` blocks. `cooldown` defines how many cycles activity remains highlighted for `cells`, `logic`, and `programs` before tiles return to the `stale` state. The colour palette maps logical areas to UI colours:

```cue
live_view: {
    heatmap: {
        cooldown: {
            cells: 12
            logic: 12
            programs: 12
        }
        colors: {
            read: "#4caf50"
            write: "#ef5350"
            stale: "#bdbdbd"
            logic: "#29b6f6"
            program: "#ab47bc"
            background: "#f5f5f5"
            border: "#424242"
        }
    }
}
```

The colours map one-to-one to the UI: freshly written cells use `write`, recently read cells use `read`, inactive tiles revert to `stale`, logic blocks use `logic`, and program tiles use `program`. `border` and `background` control the surrounding frame and canvas colours.

## Telemetry

Enable telemetry via the `telemetry` block. When `enabled` is true and no explicit collector is configured, QUARC registers a Prometheus collector (`provider: "prometheus"` or empty). To export metrics:

```cue
telemetry: {
    enabled: true
    provider: "prometheus"
}
```

The collector publishes the following metrics:

* `quarc_config_hot_reload_total` – Successful configuration hot-reloads per file.
* `quarc_read_buffer_dropped_total` – Total samples dropped per read group and buffer (e.g. due to overflows).
* `quarc_read_buffer_occupancy` – Gauge reporting the occupancy of each read buffer at the last flush.

Custom collectors can be supplied through `processor.WithTelemetry(...)`. When telemetry is disabled or an unknown provider is configured, the service falls back to a no-op collector.
